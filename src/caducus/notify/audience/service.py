"""A host's public audience: anonymous readers who subscribe to its lists by email.

Every answer is ``{"statusCode": int, "body": {...}}`` so a host can hand it to its
routes unchanged. Design: docs/public-audiences.md.
"""

from __future__ import annotations

import hashlib
import logging
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from ..delivery_log import iso, recipient_key_for_address
from ..logs import emit
from ..store import Item, Store, Write
from . import addresses, tokens
from .domains import DomainChecker
from .limits import ConfirmLag, RateTest

LOGGER = logging.getLogger("caducus.notify.audience")

CONFIRMATION_KIND = "audience.confirmation"
SURGE_KIND = "audience.surge"
PROFILE = "PROFILE"
SUPPRESSION = "SUPP"
LIST_INDEX = "byList"
LIST_ATTRIBUTE = "gsiList"
#: Suppressions a person lifts by asking again (a new confirmation or a manage-link opt-in).
LIFTABLE = frozenset(["bounce", "complaint", "unsubscribed", "erased"])
#: Suppressions that block an import: the person asked for their data to go.
BLOCKS_IMPORT = frozenset(["erased"])
MAXIMUM_TEXT = 500
MAXIMUM_USER_AGENT = 300

Answer = Dict[str, Any]


def answer(status_code: int, **body: Any) -> Answer:
    return {"statusCode": status_code, "body": body}


CHECK_EMAIL = answer(202, status="check_email")
INVALID = answer(400, status="invalid")


@dataclass(frozen=True)
class AudienceList:
    id: str
    kind: str
    name: str
    description: str = ""


class LinkBuilder:
    """The host's pages and its one-click endpoint. Tokens go in links; addresses never do."""

    def confirm_page(self, token: str) -> str:
        raise NotImplementedError

    def preferences_page(self, token: str) -> str:
        raise NotImplementedError

    def unsubscribe_page(self, token: str, list_id: Optional[str]) -> str:
        raise NotImplementedError

    def one_click(self, token: str, list_id: Optional[str]) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class AudienceSettings:
    """`confirm_window_seconds` is how long a stranger's address is held without consent and the link works."""

    lists: Tuple[AudienceList, ...]
    consent: str
    confirm_window_seconds: int
    links: LinkBuilder
    product: str
    site: Optional[str] = None
    #: Called with an address when a person lifts a bounce or complaint suppression, so the host can
    #: also clear its provider's own suppression list (SES account suppression). Optional.
    provider_unsuppress: Optional[Callable[[str], None]] = None

    def list_named(self, list_id: str) -> Optional[AudienceList]:
        return next((entry for entry in self.lists if entry.id == list_id), None)


@dataclass(frozen=True)
class AudienceKeys:
    """Both keys come from the host's one recipient-key secret (SSM SecureString)."""

    secret: str
    token: bytes = field(repr=False, default=b"")
    network: bytes = field(repr=False, default=b"")

    @classmethod
    def from_secret(cls, secret: str) -> "AudienceKeys":
        material = secret.encode("utf-8")
        return cls(
            secret=secret,
            token=tokens.hkdf_sha256(material, b"caducus-audience-token"),
            network=tokens.hkdf_sha256(material, b"caducus-audience-network"),
        )

    def recipient(self, address: str) -> str:
        return recipient_key_for_address(address, self.secret)


@dataclass(frozen=True)
class SubscribeRequest:
    address: Any
    lists: Any
    consent: Any
    form_id: Any = ""
    page: Any = ""
    origin: Any = ""
    honeypot: Any = ""
    ip: Any = ""
    user_agent: Any = ""


def _text(value: Any, limit: int = MAXIMUM_TEXT) -> str:
    return value[:limit] if isinstance(value, str) else ""


def _page_path(url: str) -> str:
    """The page without its query or fragment: a confirm or manage page carries a token there."""
    return url.split("?", 1)[0].split("#", 1)[0]


def wording_hash(wording: str) -> str:
    return hashlib.sha256(wording.encode("utf-8")).hexdigest()


def rcpt(key: str) -> str:
    return "RCPT#" + key


class PublicAudience:
    def __init__(
        self,
        store: Store,
        settings: AudienceSettings,
        keys: AudienceKeys,
        domains: DomainChecker,
        clock: Callable[[], datetime],
        emit_event: Callable[[Mapping[str, Any]], Any],
        new_nonce: Callable[[], str] = lambda: secrets.token_urlsafe(12),
    ):
        """`emit_event` takes a Caducus event detail: hosts pass `router.handle_detail`."""
        self._store = store
        self._settings = settings
        self._keys = keys
        self._domains = domains
        self._clock = clock
        self._emit = emit_event
        self._nonce = new_nonce
        self._networks = RateTest(store, clock, "network")
        self._audience = RateTest(store, clock, "audience")
        self._lag = ConfirmLag(store)

    @property
    def settings(self) -> AudienceSettings:
        return self._settings

    # -- reading -----------------------------------------------------------------

    def _now(self) -> float:
        return self._clock().timestamp()

    def _stamp(self) -> str:
        return iso(self._clock())

    def _profile(self, key: str) -> Optional[Item]:
        return self._store.get(rcpt(key), PROFILE)

    def _subscriptions(self, key: str) -> Dict[str, Item]:
        return {row["sk"][4:]: row for row in self._store.query(rcpt(key), "SUB#")}

    def suppression(self, key: str) -> Optional[str]:
        row = self._store.get("SUPP#" + key, SUPPRESSION)
        return row["reason"] if row else None

    def subscribed_lists(self, key: str) -> List[str]:
        return [lid for lid, row in self._subscriptions(key).items() if row["status"] == "subscribed"]

    def receiving(self, key: str, list_ids: Sequence[str]) -> Tuple[bool, str]:
        """May this key get mail for one of `list_ids` now? (False, reason) when not."""
        if self.suppression(key):
            return False, "suppressed"
        profile = self._profile(key)
        if profile is None or profile.get("status") != "confirmed":
            return False, "skipped_preference"
        if not set(self.subscribed_lists(key)) & set(list_ids):
            return False, "skipped_preference"
        return True, ""

    def address_of(self, key: str) -> Optional[str]:
        profile = self._profile(key)
        return profile.get("address") if profile else None

    def subscriber_page(self, list_id: str, cursor: Optional[str]) -> Tuple[List[str], Optional[str]]:
        """One page of the confirmed subscribers of `list_id` (the `byList` index), and the next cursor."""
        rows, nxt = self._store.index_page(LIST_INDEX, LIST_ATTRIBUTE, list_id, cursor)
        return [row["pk"][len("RCPT#"):] for row in rows], nxt

    def manage_token(self, key: str) -> Optional[str]:
        profile = self._profile(key)
        if profile is None:
            return None
        return tokens.issue(self._keys.token, tokens.Claims(key, "manage", profile["generation"]))

    def links_for(self, key: str, list_ids: Sequence[str]) -> Optional[Dict[str, str]]:
        """The footer and header links of one public message to `key`, unsubscribing from the first list it is on."""
        token = self.manage_token(key)
        if token is None:
            return None
        mine = [lid for lid in list_ids if lid in set(self.subscribed_lists(key))]
        list_id = mine[0] if mine else None
        links = self._settings.links
        return {
            "manage": links.preferences_page(token),
            "unsubscribe": links.unsubscribe_page(token, list_id),
            "oneClick": links.one_click(token, list_id),
        }

    # -- writing helpers ---------------------------------------------------------

    def _wording_row(self, wording: str) -> str:
        digest = wording_hash(wording)
        self._store.put_if_absent({"pk": "WORDING#" + digest, "sk": "TEXT", "text": wording})
        return digest

    def _subscription_row(self, key: str, list_id: str, subscribed: bool, indexed: bool) -> Item:
        row: Item = {
            "pk": rcpt(key),
            "sk": "SUB#" + list_id,
            "status": "subscribed" if subscribed else "unsubscribed",
            "at": self._stamp(),
        }
        if subscribed and indexed:
            row[LIST_ATTRIBUTE] = list_id
        return row

    def _save(self, profile: Item, writes: List[Write], seen: Optional[int]) -> bool:
        """Write the profile (conditional on the version read) together with `writes`."""
        put = Write(item=profile, absent=seen is None) if seen is None else Write(item=profile, expect_version=seen)
        return self._store.transact([put] + writes)

    def _lift(self, key: str, address: str, writes: List[Write]) -> bool:
        """Add the deletion of a liftable suppression to `writes`. True when it was a bounce or complaint."""
        reason = self.suppression(key)
        if reason in LIFTABLE:
            writes.append(Write(delete=("SUPP#" + key, SUPPRESSION)))
            if reason in ("bounce", "complaint") and self._settings.provider_unsuppress:
                self._settings.provider_unsuppress(address)
            return True
        return False

    def _set_lists(self, key: str, chosen: Iterable[str], suppressed_manually: bool) -> List[Write]:
        chosen = set(chosen)
        current = self._subscriptions(key)
        writes = []
        for entry in self._settings.lists:
            on = entry.id in chosen
            was = current.get(entry.id, {}).get("status") == "subscribed"
            if on != was or (on and not suppressed_manually and LIST_ATTRIBUTE not in current.get(entry.id, {})):
                writes.append(Write(item=self._subscription_row(key, entry.id, on, not suppressed_manually)))
        return writes

    # -- subscribe ---------------------------------------------------------------

    def _requested(self, raw: Any) -> List[str]:
        if isinstance(raw, str):
            raw = raw.split(",")
        if not isinstance(raw, list):
            return []
        ordered: List[str] = []
        for entry in raw:
            identifier = entry.strip() if isinstance(entry, str) else ""
            if self._settings.list_named(identifier) and identifier not in ordered:
                ordered.append(identifier)
        return ordered

    def subscribe(self, request: SubscribeRequest) -> Answer:
        if request.honeypot:
            emit(LOGGER, "subscribe_ignored", reason="honeypot")
            return CHECK_EMAIL
        address = addresses.normalize(request.address) if isinstance(request.address, str) else ""
        if not addresses.is_valid(address):
            return answer(400, status="invalid_email")
        lists = self._requested(request.lists)
        if not lists:
            return answer(400, status="invalid_lists")
        if request.consent != self._settings.consent:
            return answer(400, status="invalid_consent")
        network = addresses.keyed_hash(self._keys.network, addresses.network_of(_text(request.ip, 64)))
        if self._networks.improbable(network):
            emit(LOGGER, "subscribe_limited", scope="network")
            return answer(429, status="slow_down")
        if not self._domains.accepts_mail(addresses.domain_of(address)):
            return answer(400, status="invalid_email")
        key = self._keys.recipient(address)
        self._networks.record(network)
        if self._audience.improbable("all"):
            self._surge()
        self._audience.record("all")
        reason = self.suppression(key)
        if reason in ("bounce", "complaint", "manual"):
            emit(LOGGER, "subscribe_ignored", reason="suppressed")
            return CHECK_EMAIL
        while True:
            outcome = self._request_confirmation(key, address, lists, request, network)
            if outcome is not None:
                return outcome

    def _request_confirmation(
        self, key: str, address: str, lists: List[str], request: SubscribeRequest, network: str
    ) -> Optional[Answer]:
        """One optimistic attempt. None when another writer changed the profile meanwhile."""
        now = self._now()
        profile = self._profile(key)
        seen = profile.get("version") if profile else None
        if profile and profile.get("status") == "confirmed" and set(lists) <= set(self.subscribed_lists(key)):
            return CHECK_EMAIL
        pending = (profile or {}).get("pending")
        window = self._settings.confirm_window_seconds
        if pending and pending["confirmBy"] > now and now - pending["sentAt"] < self._lag.resend_gap(window):
            emit(LOGGER, "subscribe_limited", scope="address")
            return CHECK_EMAIL
        nonce = self._nonce()
        consent = {
            "source": "form",
            "lists": lists,
            "wordingHash": self._wording_row(self._settings.consent),
            "formId": _text(request.form_id, 100),
            "page": _page_path(_text(request.page)),
            "origin": _text(request.origin, 200),
            "networkHash": network,
            "userAgent": _text(request.user_agent, MAXIMUM_USER_AGENT),
            "requestedAt": iso(datetime.fromtimestamp(now, timezone.utc)),
        }
        updated: Item = dict(profile or {})
        updated.update(
            {
                "pk": rcpt(key),
                "sk": PROFILE,
                "address": address,
                "updatedAt": self._stamp(),
                "pending": {
                    "lists": lists,
                    "nonce": nonce,
                    "sentAt": int(now),
                    "confirmBy": int(now) + window,
                    "consent": consent,
                },
            }
        )
        if profile is None or profile.get("status") == "erased":
            updated.update({"status": "pending", "createdAt": self._stamp(), "generation": self._nonce()})
        if updated["status"] == "pending":
            updated["expiresAt"] = int(now) + window  # never consented: kept no longer than the link works
        if not self._save(updated, [], seen):
            return None
        token = tokens.issue(
            self._keys.token,
            tokens.Claims(key, "confirm", updated["generation"], nonce, int(now) + window),
        )
        names = [self._settings.list_named(lid).name for lid in lists]  # type: ignore[union-attr]
        try:
            self._emit_confirmation(key, nonce, token, names)
        except Exception:
            # Not sent: let the reader try again at once rather than wait out the resend gap.
            self._store.update(rcpt(key), PROFILE, lambda row: dict(row or {}, pending=dict((row or {}).get("pending") or {}, sentAt=0)))
            raise
        emit(LOGGER, "confirmation_requested", lists=",".join(lists))
        return CHECK_EMAIL

    def _emit_confirmation(self, key: str, nonce: str, token: str, names: List[str]) -> None:
        self._emit(
            {
                "schema": 1,
                "eventId": "%s:%s:%s" % (CONFIRMATION_KIND, key, nonce),
                "kind": CONFIRMATION_KIND,
                "product": self._settings.product,
                **({"site": self._settings.site} if self._settings.site else {}),
                "occurredAt": self._stamp(),
                "severity": "info",
                "audience": key,
                "facts": {"title": "Confirm your subscription", "url": self._settings.links.confirm_page(token), "lists": names},
            }
        )

    def _surge(self) -> None:
        hour = int(self._now() // 3600)
        self._emit(
            {
                "schema": 1,
                "eventId": "%s:%s:%d" % (SURGE_KIND, self._settings.site or self._settings.product, hour),
                "kind": SURGE_KIND,
                "product": self._settings.product,
                **({"site": self._settings.site} if self._settings.site else {}),
                "occurredAt": self._stamp(),
                "severity": "warning",
                "facts": {
                    "title": "Unusually many signups this hour",
                    "summary": "Signups this hour are far above this site's usual rate. Confirmations still go out; "
                    "watch the bounce and complaint rates.",
                },
            }
        )

    # -- confirm -----------------------------------------------------------------

    def _claims(self, token: Any, purpose: str) -> Optional[tokens.Claims]:
        try:
            return tokens.verify(self._keys.token, token, purpose, self._now())
        except tokens.InvalidToken:
            return None

    def confirm_state(self, token: Any) -> Answer:
        claims = self._claims(token, "confirm")
        profile = self._profile(claims.key) if claims else None
        pending = (profile or {}).get("pending")
        if not claims or not pending or pending["nonce"] != claims.nonce or profile["generation"] != claims.generation:
            return INVALID
        return answer(200, status="ready", lists=[self._list_view(lid) for lid in pending["lists"]])

    def confirm(self, token: Any, ip: Any = "", user_agent: Any = "") -> Answer:
        claims = self._claims(token, "confirm")
        if claims is None:
            return INVALID
        while True:
            profile = self._profile(claims.key)
            pending = (profile or {}).get("pending")
            if not pending or pending["nonce"] != claims.nonce or profile["generation"] != claims.generation:
                return INVALID
            now = self._now()
            writes: List[Write] = []
            self._lift(claims.key, profile["address"], writes)
            manual = self.suppression(claims.key) == "manual"
            chosen = set(self.subscribed_lists(claims.key)) | set(pending["lists"])
            writes += self._set_lists(claims.key, chosen, manual)
            consent = dict(pending["consent"], confirmedAt=self._stamp())
            consent["confirmUserAgent"] = _text(user_agent, MAXIMUM_USER_AGENT)
            writes.append(Write(item={"pk": rcpt(claims.key), "sk": "CONSENT#%s#%s" % (consent["requestedAt"], claims.nonce), **consent}))
            updated = {k: v for k, v in profile.items() if k not in ("pending", "expiresAt")}
            updated.update({"status": "confirmed", "confirmedAt": self._stamp(), "updatedAt": self._stamp()})
            if self._save(updated, writes, profile.get("version")):
                self._lag.record(now - pending["sentAt"])
                emit(LOGGER, "subscription_confirmed", lists=",".join(pending["lists"]))
                return answer(
                    200,
                    status="confirmed",
                    lists=pending["lists"],
                    manageToken=tokens.issue(self._keys.token, tokens.Claims(claims.key, "manage", profile["generation"])),
                )

    # -- preferences and unsubscribe -------------------------------------------

    def _manage(self, token: Any) -> Tuple[Optional[tokens.Claims], Optional[Item]]:
        claims = self._claims(token, "manage")
        if claims is None:
            return None, None
        profile = self._profile(claims.key)
        if profile is None or profile.get("generation") != claims.generation:
            return claims, None
        return claims, profile

    def _list_view(self, list_id: str) -> Dict[str, Any]:
        entry = self._settings.list_named(list_id)
        return {"id": list_id, "name": entry.name if entry else list_id, "description": entry.description if entry else ""}

    def preferences_state(self, token: Any) -> Answer:
        claims, profile = self._manage(token)
        if profile is None:
            return INVALID
        mine = set(self.subscribed_lists(claims.key))  # type: ignore[union-attr]
        return answer(
            200,
            status="ready",
            email=addresses.mask(profile["address"]),
            lists=[dict(self._list_view(entry.id), subscribed=entry.id in mine) for entry in self._settings.lists],
        )

    def update_preferences(self, token: Any, lists: Any) -> Answer:
        claims, profile = self._manage(token)
        if profile is None:
            return INVALID
        chosen = [lid for lid in (lists if isinstance(lists, list) else []) if self._settings.list_named(lid)]
        if not chosen:
            return self.unsubscribe(token)
        while True:
            profile = self._profile(claims.key)  # type: ignore[union-attr]
            if profile is None:
                return INVALID
            key = claims.key  # type: ignore[union-attr]
            writes: List[Write] = []
            added = set(chosen) - set(self.subscribed_lists(key))
            if added:
                self._lift(key, profile["address"], writes)
                writes.append(
                    Write(
                        item={
                            "pk": rcpt(key),
                            "sk": "CONSENT#%s#manage" % self._stamp(),
                            "source": "manage_link",
                            "lists": sorted(added),
                            "requestedAt": self._stamp(),
                            "confirmedAt": self._stamp(),
                        }
                    )
                )
            writes += self._set_lists(key, chosen, self.suppression(key) == "manual")
            updated = dict(profile, status="confirmed", updatedAt=self._stamp())
            if self._save(updated, writes, profile.get("version")):
                return answer(200, status="saved", lists=chosen)

    def unsubscribe(self, token: Any, list_id: Optional[str] = None) -> Answer:
        """One list, or everything (which also suppresses). Idempotent, and an erased subscriber still succeeds."""
        claims, profile = self._manage(token)
        if claims is None:
            return INVALID
        if profile is None:
            return answer(200, status="unsubscribed", lists=[])
        key = claims.key
        while True:
            profile = self._profile(key)
            if profile is None:
                return answer(200, status="unsubscribed", lists=[])
            mine = self.subscribed_lists(key)
            if list_id and self._settings.list_named(list_id):
                remaining = [lid for lid in mine if lid != list_id]
                if remaining:
                    writes = self._set_lists(key, remaining, self.suppression(key) == "manual")
                    if self._save(dict(profile, updatedAt=self._stamp()), writes, profile.get("version")):
                        emit(LOGGER, "unsubscribed", scope="list")
                        return answer(200, status="unsubscribed", lists=[list_id])
                    continue
            writes = self._set_lists(key, [], True)
            if self.suppression(key) is None:
                writes.append(Write(item={"pk": "SUPP#" + key, "sk": SUPPRESSION, "reason": "unsubscribed", "at": self._stamp()}))
            updated = dict(profile, status="unsubscribed", updatedAt=self._stamp())
            if self._save(updated, writes, profile.get("version")):
                emit(LOGGER, "unsubscribed", scope="all")
                return answer(200, status="unsubscribed", lists=mine)

    # -- provider events ---------------------------------------------------------

    def provider_event(self, key: str, status: str) -> str:
        """From the host's SES event handler: a permanent bounce or a complaint suppresses and unsubscribes."""
        reason = {"bounced": "bounce", "complained": "complaint"}.get(status)
        if reason is None or not key.startswith("ADDR#"):
            return "ignored"
        self._store.put({"pk": "SUPP#" + key, "sk": SUPPRESSION, "reason": reason, "at": self._stamp()})
        while True:
            profile = self._profile(key)
            if profile is None:
                return "suppressed"
            updated = dict(profile, status="unsubscribed", updatedAt=self._stamp())
            if self._save(updated, self._set_lists(key, [], True), profile.get("version")):
                emit(LOGGER, "suppressed", reason=reason)
                return "suppressed"

    # -- erasure, export, import -------------------------------------------------

    def _erase_key(self, key: str) -> int:
        removed = 0
        for row in self._store.query(rcpt(key)):
            self._store.delete(row["pk"], row["sk"])
            removed += 1
        for delivery in self._store.by_recipient(key):
            for prefix in ("DELIVERY#", "CONTENT#", "CLAIM#"):
                self._store.delete(delivery["pk"], prefix + delivery["deliveryId"])
            removed += 1
        self._store.put({"pk": "SUPP#" + key, "sk": SUPPRESSION, "reason": "erased", "at": self._stamp()})
        emit(LOGGER, "erased")
        return removed

    def erase_self(self, token: Any) -> Answer:
        claims, profile = self._manage(token)
        if claims is None:
            return INVALID
        if profile is not None:
            self._erase_key(claims.key)
        return answer(200, status="deleted")

    def erase(self, address: str) -> Answer:
        """Admin only: the host checks the caller's role before calling."""
        normalized = addresses.normalize(address or "")
        if not addresses.is_valid(normalized):
            return answer(400, status="invalid_email")
        return answer(200, status="deleted", removed=self._erase_key(self._keys.recipient(normalized)))

    def export(self, address: str) -> Answer:
        """Admin only: everything held about one address, for an access request."""
        normalized = addresses.normalize(address or "")
        if not addresses.is_valid(normalized):
            return answer(400, status="invalid_email")
        key = self._keys.recipient(normalized)
        rows = self._store.query(rcpt(key))
        profile = next((r for r in rows if r["sk"] == PROFILE), None)
        if profile is None:
            return answer(200, status="not_found")
        consents = []
        for row in rows:
            if row["sk"].startswith("CONSENT#"):
                record = {k: v for k, v in row.items() if k not in ("pk", "sk", "version")}
                if row.get("wordingHash"):
                    record["wording"] = (self._store.get("WORDING#" + row["wordingHash"], "TEXT") or {}).get("text", "")
                consents.append(record)
        drop = ("pk", "sk", "version", "pending", "expiresAt", "generation")
        return answer(
            200,
            status="ready",
            profile={k: v for k, v in profile.items() if k not in drop},
            lists={row["sk"][4:]: row["status"] for row in rows if row["sk"].startswith("SUB#")},
            consent=consents,
            suppression=self.suppression(key),
            deliveries=[
                {k: d.get(k) for k in ("kind", "channel", "status", "reason", "queuedAt", "sentAt", "finalAt") if d.get(k) is not None}
                for d in self._store.by_recipient(key)
            ],
        )

    def import_subscriber(
        self,
        address: str,
        lists: Sequence[str],
        evidence: Mapping[str, Any],
        source: str,
        suppressed: Optional[str] = None,
    ) -> str:
        """Move a confirmed subscriber from another system without a new confirmation. Idempotent.

        Returns ``imported``, ``unchanged`` or ``refused`` (an erased address is never brought back).
        """
        normalized = addresses.normalize(address)
        if not addresses.is_valid(normalized):
            return "refused"
        key = self._keys.recipient(normalized)
        if self.suppression(key) in BLOCKS_IMPORT:
            return "refused"
        chosen = [lid for lid in lists if self._settings.list_named(lid)]
        record = dict(evidence, source="migrated", **{"from": source}, lists=sorted(chosen))
        digest = hashlib.sha256(repr(sorted(record.items())).encode("utf-8")).hexdigest()[:16]
        while True:
            profile = self._profile(key)
            already = profile is not None and profile.get("status") == "confirmed" and set(chosen) <= set(self.subscribed_lists(key))
            consent_key = (rcpt(key), "CONSENT#%s#import-%s" % (evidence.get("confirmedAt", ""), digest))
            if already and self._store.get(*consent_key) and (suppressed is None or self.suppression(key)):
                return "unchanged"
            writes: List[Write] = []
            if suppressed and self.suppression(key) is None:
                writes.append(Write(item={"pk": "SUPP#" + key, "sk": SUPPRESSION, "reason": suppressed, "at": self._stamp()}))
            writes += self._set_lists(key, set(chosen) | set(self.subscribed_lists(key)), bool(suppressed or self.suppression(key)))
            writes.append(Write(item={"pk": consent_key[0], "sk": consent_key[1], **record}))
            updated: Item = dict(profile or {"createdAt": self._stamp(), "generation": self._nonce()})
            updated.update({"pk": rcpt(key), "sk": PROFILE, "address": normalized, "status": "confirmed", "updatedAt": self._stamp()})
            updated.pop("expiresAt", None)
            updated.pop("pending", None)
            if self._save(updated, writes, profile.get("version") if profile else None):
                return "imported"

    def add_alias(self, source: str, old_id: str, address: str, expires_at: int) -> None:
        """Map another system's subscriber id to this key until `expires_at` (DynamoDB TTL deletes it then)."""
        self._store.put(
            {
                "pk": "ALIAS#%s#%s" % (source, old_id),
                "sk": "ALIAS",
                "recipientKey": self._keys.recipient(addresses.normalize(address)),
                "expiresAt": int(expires_at),
            }
        )

    def resolve_alias(self, source: str, old_id: str) -> Optional[str]:
        row = self._store.get("ALIAS#%s#%s" % (source, old_id), "ALIAS")
        if row is None or row["expiresAt"] < self._now():  # TTL deletion can lag; the date is the rule
            return None
        return row["recipientKey"]

    def manage_token_for_alias(self, source: str, old_id: str) -> Optional[str]:
        key = self.resolve_alias(source, old_id)
        return self.manage_token(key) if key else None
