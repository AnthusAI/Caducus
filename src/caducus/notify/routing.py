"""Turn one accepted event into one queued delivery per person and channel.

First version: operator kinds go to the operators in the directory. Preferences
arrive later. Anything decided against is recorded in the delivery log with a
reason, never dropped silently.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional

from .catalog import Catalog
from .cloudwatch import KIND as ALARM_KIND, STATES
from .delivery_log import DeliveryLog
from .directory import Directory, Recipient
from .events import Event, EventInvalid, parse_event
from .grouping import Dispatcher, delivery_id_for
from .incidents import IncidentTracker
from .kinds import Kind, KindRegistry
from .logs import emit
from .store import Store
from .triage import TriageSettings, build_alarm_item, build_cleared_item

LOGGER = logging.getLogger("caducus.notify.router")
NO_RECIPIENT = "NONE"


class Preferences:
    """What a person turned off. The first version has none; storage arrives with
    the preferences stories."""

    def opted_out(self, recipient_key: str, kind: str) -> bool:
        return False


class MemoryPreferences(Preferences):
    def __init__(self) -> None:
        self._off: set = set()

    def turn_off(self, recipient_key: str, kind: str) -> None:
        self._off.add((recipient_key, kind))

    def opted_out(self, recipient_key: str, kind: str) -> bool:
        return (recipient_key, kind) in self._off


@dataclass
class Result:
    outcome: str  # accepted | duplicate | refused
    reason: str = ""
    deliveries: List[Dict[str, Any]] = field(default_factory=list)


class Router:
    def __init__(
        self,
        *,
        store: Store,
        kinds: KindRegistry,
        log: DeliveryLog,
        directory: Directory,
        dispatcher: Dispatcher,
        incidents: IncidentTracker,
        catalog: Catalog,
        triage: TriageSettings,
        clock: Any,
        preferences: Optional[Preferences] = None,
    ):
        self._preferences = preferences or Preferences()
        self._store = store
        self._kinds = kinds
        self._log = log
        self._directory = directory
        self._dispatcher = dispatcher
        self._incidents = incidents
        self._catalog = catalog
        self._triage = triage
        self._clock = clock

    # -- intake --------------------------------------------------------------

    def handle_detail(self, detail: Any) -> Result:
        """Accept `detail` from a Notification Requested event, or record why not."""
        try:
            event = parse_event(detail)
        except EventInvalid as error:
            event_id = detail.get("eventId") if isinstance(detail, Mapping) else None
            reason = "invalid event: %s" % error
            emit(LOGGER, "event_refused", reason=reason, eventId=event_id)
            if isinstance(event_id, str) and event_id:
                self._refuse(
                    event_id=event_id,
                    kind=str(detail.get("kind") or "unknown"),
                    klass="operator",
                    product=str(detail.get("product") or "unknown"),
                    site=None,
                    reason=reason,
                )
            return Result("refused", reason)
        return self.handle_event(event)

    def handle_event(self, event: Event) -> Result:
        first_time = self._store.put_if_absent(
            {
                "pk": "EVENT#" + event.event_id,
                "sk": "META",
                "event": event.to_detail(),
                "acceptedAt": self._clock().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "expiresAt": self._log.expires_at(),
            }
        )
        if not first_time:
            emit(LOGGER, "event_duplicate", eventId=event.event_id)
            return Result("duplicate")
        kind = self._kinds.get(event.kind)
        if kind is None:
            self._refuse(
                event_id=event.event_id,
                kind=event.kind,
                klass="operator",
                product=event.product,
                site=event.site,
                reason="unknown kind",
            )
            emit(LOGGER, "event_refused", eventId=event.event_id, reason="unknown kind")
            return Result("refused", "unknown kind")
        if kind.lifecycle == "incident":
            return self._handle_alarm(event, kind)
        item = {
            "state": "NOTICE",
            "alarmName": event.facts["title"],
            "product": event.product,
            "site": event.site or "",
            "what": event.facts["title"],
            "reason": event.facts.get("summary", ""),
            "link": event.facts.get("url", ""),
            "hasNotes": False,
            "notesLabel": "",
        }
        return self._send(event.event_id, kind, event.product, event.site, item)

    # -- alarms --------------------------------------------------------------

    def _handle_alarm(self, event: Event, kind: Kind) -> Result:
        facts = event.facts
        if not facts.get("alarmName") or facts.get("state") not in STATES:
            reason = "invalid event: alarmName and state are required"
            self._refuse(
                event_id=event.event_id, kind=kind.name, klass=kind.klass,
                product=event.product, site=event.site, reason=reason,
            )
            return Result("refused", reason)
        name, account, region = facts["alarmName"], facts.get("awsAccountId", ""), facts.get("region", "")
        item = build_alarm_item(event, self._catalog, self._triage)
        decision = self._incidents.apply(
            account=account,
            region=region,
            alarm=name,
            state=facts["state"],
            hold_down_seconds=self._catalog.hold_down_seconds(name),
            muted=bool(facts.get("muted")),
            item=item,
            product=event.product,
            site=event.site,
        )
        emit(
            LOGGER,
            "alarm_transition",
            eventId=event.event_id,
            alarm=name,
            state=facts["state"],
            action=decision.action,
            reason=decision.reason,
        )
        if decision.action == "open":
            return self._send(
                event.event_id,
                kind,
                event.product,
                event.site,
                item,
                incident_key=decision.incident_key,
            )
        recipients = self._directory.operators()
        rows = [
            self._record(
                event.event_id, kind, event.product, event.site, who, channel,
                "suppressed", decision.reason, decision.incident_key,
            )
            for who in recipients
            for channel in kind.channels
        ] or [
            self._refuse(
                event_id=event.event_id,
                kind=kind.name,
                klass=kind.klass,
                product=event.product,
                site=event.site,
                reason="no audience",
            )
        ]
        return Result("accepted", decision.reason, rows)

    def tick(self) -> int:
        """Run each minute: send due clearing messages, then flush ended groups."""
        sent = 0
        kind = self._kinds.get(ALARM_KIND)
        if kind is not None:
            for cleared in self._incidents.due_clears():
                item = build_cleared_item(
                    cleared.item, cleared.duration_seconds, cleared.flaps
                )
                self._send(
                    "clear:%s:%s" % (cleared.incident_key, cleared.opened_at),
                    kind,
                    cleared.product,
                    cleared.site,
                    item,
                    incident_key=cleared.incident_key,
                )
                sent += 1
        return sent + self._dispatcher.flush_groups(self._directory.get)

    # -- delivery ------------------------------------------------------------

    def _audience(self, kind: Kind) -> List[Recipient]:
        if kind.audience == "probe":
            probe = self._directory.probe()
            return [probe] if probe else []
        if kind.klass == "operator":
            return self._directory.operators()
        return []  # admin, account and public audiences arrive with preferences

    def _send(
        self,
        event_id: str,
        kind: Kind,
        product: str,
        site: Optional[str],
        item: Mapping[str, Any],
        incident_key: Optional[str] = None,
    ) -> Result:
        audience = self._audience(kind)
        if not audience:
            row = self._refuse(
                event_id=event_id,
                kind=kind.name,
                klass=kind.klass,
                product=product,
                site=site,
                reason="no audience",
            )
            return Result("refused", "no audience", [row])
        rows = []
        for who in audience:
            for channel in kind.channels:
                if self._preferences.opted_out(who.key, kind.name):
                    rows.append(
                        self._record(
                            event_id, kind, product, site, who, channel,
                            "skipped_preference", "turned off by the person",
                            incident_key,
                        )
                    )
                    continue
                rows.append(
                    self._dispatcher.dispatch(
                        event_id=event_id,
                        kind=kind,
                        product=product,
                        site=site,
                        recipient=who,
                        channel=channel,
                        item=item,
                        incident_key=incident_key,
                    )
                )
        return Result("accepted", deliveries=rows)

    def _record(
        self,
        event_id: str,
        kind: Kind,
        product: str,
        site: Optional[str],
        who: Recipient,
        channel: str,
        status: str,
        reason: str,
        incident_key: Optional[str],
    ) -> Dict[str, Any]:
        return self._log.create(
            event_id=event_id,
            kind=kind.name,
            klass=kind.klass,
            product=product,
            site=site,
            recipient_key=who.key,
            channel=channel,
            destination_mask=who.mask,
            status=status,
            reason=reason,
            incident_key=incident_key,
            delivery_id=delivery_id_for(event_id, who.key, channel),
        )

    def _refuse(
        self,
        *,
        event_id: str,
        kind: str,
        klass: str,
        product: str,
        site: Optional[str],
        reason: str,
    ) -> Dict[str, Any]:
        return self._log.create(
            event_id=event_id,
            kind=kind,
            klass=klass,
            product=product,
            site=site,
            recipient_key=NO_RECIPIENT,
            channel="none",
            status="refused",
            reason=reason,
            delivery_id=delivery_id_for(event_id, NO_RECIPIENT, "none"),
        )
