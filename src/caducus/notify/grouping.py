"""Deliver an item now, or hold it for the group it belongs to. Nothing is dropped.

The first item in a group goes at once and opens a window. Items arriving inside
the window are recorded as `grouped` and collected. When the window ends, the
minute-by-minute flush sends one message listing every collected item, each with
its own link and next step. Groups are per recipient, channel and message state,
so clearing messages group the same way as alarms.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Mapping, Optional

from .delivery_log import DeliveryLog, new_id
from .directory import Recipient
from .kinds import Kind, KindRegistry
from .queues import Queues
from .store import Item, Store


OPEN_GROUPS = "GROUPS#open"


def delivery_id_for(event_id: str, recipient_key: str, channel: str) -> str:
    """Deterministic, so routing the same event again cannot create a second delivery."""
    return hashlib.sha256(
        ("%s|%s|%s" % (event_id, recipient_key, channel)).encode("utf-8")
    ).hexdigest()[:24]


class Dispatcher:
    def __init__(
        self,
        store: Store,
        log: DeliveryLog,
        queues: Queues,
        kinds: KindRegistry,
        clock: Callable[[], datetime],
        make_id: Callable[[], str] = new_id,
    ):
        self._store = store
        self._log = log
        self._queues = queues
        self._kinds = kinds
        self._clock = clock
        self._make_id = make_id

    # -- sending -------------------------------------------------------------

    def _queue(
        self,
        *,
        event_id: str,
        kind: Kind,
        product: str,
        site: Optional[str],
        recipient: Recipient,
        channel: str,
        items: List[Mapping[str, Any]],
        incident_key: Optional[str],
        delivery_id: Optional[str] = None,
    ) -> Item:
        delivery = self._log.create(
            event_id=event_id,
            kind=kind.name,
            klass=kind.klass,
            product=product,
            site=site,
            recipient_key=recipient.key,
            channel=channel,
            destination_mask=recipient.mask,
            incident_key=incident_key,
            ack_requested=kind.ack == "requested",
            delivery_id=delivery_id
            or delivery_id_for(event_id, recipient.key, channel),
        )
        self._store.put(
            {
                "pk": delivery["pk"],
                "sk": "CONTENT#" + delivery["deliveryId"],
                "items": [dict(i) for i in items],
                "expiresAt": delivery["expiresAt"],
            }
        )
        self._queues.send(
            channel, {"eventId": event_id, "deliveryId": delivery["deliveryId"]}
        )
        return delivery

    def dispatch(
        self,
        *,
        event_id: str,
        kind: Kind,
        product: str,
        site: Optional[str],
        recipient: Recipient,
        channel: str,
        item: Mapping[str, Any],
        incident_key: Optional[str] = None,
    ) -> Item:
        """Queue the item, or collect it into the open group. Returns the delivery row."""
        if kind.grouping is None:
            return self._queue(
                event_id=event_id,
                kind=kind,
                product=product,
                site=site,
                recipient=recipient,
                channel=channel,
                items=[item],
                incident_key=incident_key,
            )
        values = {"product": product, "site": site or ""}
        group_key = "GROUP#%s|%s|%s|%s|%s" % (
            kind.name,
            item.get("state", ""),
            "/".join(values[f] for f in kind.grouping.by),
            recipient.key,
            channel,
        )
        now = self._clock()
        while True:
            group_id = "grp-" + self._make_id()
            opened = self._store.put_if_absent(
                {
                    "pk": group_key,
                    "sk": "OPEN",
                    "groupId": group_id,
                    "windowEndsAt": int(
                        (now + timedelta(seconds=kind.grouping.window_seconds)).timestamp()
                    ),
                    "kind": kind.name,
                    "product": product,
                    "site": site,
                    "recipientKey": recipient.key,
                    "channel": channel,
                    "items": [],
                }
            )
            if opened:
                self._store.put({"pk": OPEN_GROUPS, "sk": group_key})
                return self._queue(
                    event_id=event_id,
                    kind=kind,
                    product=product,
                    site=site,
                    recipient=recipient,
                    channel=channel,
                    items=[item],
                    incident_key=incident_key,
                )
            joined: Dict[str, str] = {}

            def collect(row: Optional[Item]) -> Item:
                if row is None:
                    raise KeyError("group was flushed")
                updated = dict(row)
                updated["items"] = list(row["items"]) + [dict(item)]
                joined["groupId"] = row["groupId"]  # noqa: B023
                return updated

            try:
                self._store.update(group_key, "OPEN", collect)
            except KeyError:
                continue  # flushed just now: open a new window
            return self._log.create(
                event_id=event_id,
                kind=kind.name,
                klass=kind.klass,
                product=product,
                site=site,
                recipient_key=recipient.key,
                channel=channel,
                destination_mask=recipient.mask,
                status="grouped",
                reason="held for the group message",
                group_id=joined["groupId"],
                incident_key=incident_key,
                delivery_id=delivery_id_for(event_id, recipient.key, channel),
            )

    # -- the minute-by-minute flush -----------------------------------------

    def flush_groups(self, directory_get: Callable[[str], Optional[Recipient]]) -> int:
        """Send one message for every group whose window has ended. Returns how many."""
        now = int(self._clock().timestamp())
        sent = 0
        for pointer in self._store.query(OPEN_GROUPS):
            row = self._store.get(pointer["sk"], "OPEN")
            if row is None:
                self._store.delete(OPEN_GROUPS, pointer["sk"])
                continue
            if row["windowEndsAt"] > now:
                continue
            recipient = directory_get(row["recipientKey"])
            if row["items"] and recipient is not None:
                self._send_group(row, recipient)
                sent += 1
            self._store.delete(row["pk"], row["sk"])
            self._store.delete(OPEN_GROUPS, row["pk"])
        return sent

    def _send_group(self, row: Item, recipient: Recipient) -> None:
        kind = self._kinds.get(row["kind"])
        meta = {
            "pk": "EVENT#" + row["groupId"],
            "sk": "META",
            "kind": row["kind"],
            "groupMessage": True,
            "expiresAt": self._log.expires_at(),
        }
        if kind is None or not self._store.put_if_absent(meta):
            return  # already sent by an earlier, interrupted flush
        self._queue(
            event_id=row["groupId"],
            kind=kind,
            product=row["product"],
            site=row.get("site"),
            recipient=recipient,
            channel=row["channel"],
            items=row["items"],
            incident_key=None,
        )
