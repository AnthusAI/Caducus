"""The delivery log: what was sent, to whom, on which channel, and what happened.

One row per delivery at `EVENT#<eventId>` / `DELIVERY#<deliveryId>`. Addresses are
never stored here: `recipient_key` is a pseudonym and `destination_mask` is
`r***@example.com`. A status never moves backward, because provider events can
arrive out of order.
"""

from __future__ import annotations

import hashlib
import hmac
import uuid
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional

from .store import Item, Store

# Lowest to highest. Equal rank keeps the first result.
RANK = {
    "queued": 0,
    "sent": 1,
    "delivered": 2,
    "delayed": 3,
    "bounced": 4,
    "complained": 4,
    "rejected": 4,
    "failed": 4,
}
WORST = 4
# Decided outcomes that nothing later may change.
TERMINAL = frozenset(
    ["suppressed", "skipped_preference", "grouped", "refused", "dead_lettered"]
)
STATUSES = tuple(sorted(set(RANK) | TERMINAL))
FINAL = frozenset([s for s, r in RANK.items() if r == WORST] + ["delivered"]) | TERMINAL

DEFAULT_RETENTION_DAYS = 90


def next_status(current: str, new: str) -> str:
    """The status after `new` is reported while the row says `current`."""
    if current in TERMINAL:
        return current
    if new in TERMINAL:
        return current if RANK[current] >= WORST else new
    return new if RANK[new] > RANK[current] else current


def mask_address(address: str) -> str:
    """`reader@example.com` becomes `r***@example.com`."""
    local, _, domain = address.partition("@")
    return "%s***@%s" % (local[:1], domain)


def recipient_key_for_address(address: str, secret: str) -> str:
    """A stable pseudonym for an address: an HMAC, so the log cannot be reversed."""
    digest = hmac.new(
        secret.encode("utf-8"), address.strip().lower().encode("utf-8"), hashlib.sha256
    )
    return "ADDR#" + digest.hexdigest()[:32]


def iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def event_key(event_id: str) -> str:
    return "EVENT#" + event_id


def new_id() -> str:
    return uuid.uuid4().hex


class DeliveryLog:
    def __init__(
        self,
        store: Store,
        clock: Callable[[], datetime],
        retention_days: int = DEFAULT_RETENTION_DAYS,
        make_id: Callable[[], str] = new_id,
    ):
        self._store = store
        self._clock = clock
        self._retention = timedelta(days=retention_days)
        self._make_id = make_id

    def expires_at(self) -> int:
        return int((self._clock() + self._retention).timestamp())

    def create(
        self,
        *,
        event_id: str,
        kind: str,
        klass: str,
        product: str,
        site: Optional[str],
        recipient_key: str,
        channel: str,
        destination_mask: str = "",
        status: str = "queued",
        reason: Optional[str] = None,
        group_id: Optional[str] = None,
        incident_key: Optional[str] = None,
        ack_requested: bool = False,
        delivery_id: Optional[str] = None,
    ) -> Item:
        """Record a delivery, including the ones decided against.

        Pass `delivery_id` to make creation idempotent: a second call with the
        same id returns the row that is already there.
        """
        if status not in RANK and status not in TERMINAL:
            raise ValueError("unknown status %s" % status)
        now = iso(self._clock())
        delivery_id = delivery_id or self._make_id()
        row: Item = {
            "pk": event_key(event_id),
            "sk": "DELIVERY#" + delivery_id,
            "deliveryId": delivery_id,
            "eventId": event_id,
            "kind": kind,
            "class": klass,
            "product": product,
            "recipientKey": recipient_key,
            "channel": channel,
            "destinationMask": destination_mask,
            "status": status,
            "attempts": 0,
            "queuedAt": now,
            "ackRequested": ack_requested,
            "expiresAt": self.expires_at(),
        }
        optional = (
            ("site", site),
            ("reason", reason),
            ("groupId", group_id),
            ("incidentKey", incident_key),
        )
        row.update({name: value for name, value in optional if value is not None})
        if status in FINAL:
            row["finalAt"] = now
        if not self._store.put_if_absent(row):
            return self._store.get(row["pk"], row["sk"]) or row
        return row

    def get(self, event_id: str, delivery_id: str) -> Optional[Item]:
        return self._store.get(event_key(event_id), "DELIVERY#" + delivery_id)

    def for_event(self, event_id: str) -> List[Item]:
        return self._store.query(event_key(event_id), "DELIVERY#")

    def for_recipient(self, recipient_key: str) -> List[Item]:
        return self._store.by_recipient(recipient_key)

    def advance(
        self,
        event_id: str,
        delivery_id: str,
        status: str,
        *,
        reason: Optional[str] = None,
        provider_message_id: Optional[str] = None,
        attempts: Optional[int] = None,
    ) -> Item:
        """Report a new status. The row keeps the worse of what it has and what arrived."""
        if status not in RANK and status not in TERMINAL:
            raise ValueError("unknown status %s" % status)
        now = iso(self._clock())

        def change(row: Optional[Item]) -> Item:
            if row is None:
                raise KeyError("no delivery %s for event %s" % (delivery_id, event_id))
            updated = dict(row)
            before = row["status"]
            after = next_status(before, status)
            updated["status"] = after
            if after != before:
                if reason is not None:
                    updated["reason"] = reason
                if after == "sent":
                    updated["sentAt"] = now
                if after in FINAL:
                    updated["finalAt"] = now
            if provider_message_id and "providerMessageId" not in updated:
                updated["providerMessageId"] = provider_message_id
            if attempts is not None:
                updated["attempts"] = max(attempts, updated.get("attempts", 0))
            return updated

        return self._store.update(event_key(event_id), "DELIVERY#" + delivery_id, change)
