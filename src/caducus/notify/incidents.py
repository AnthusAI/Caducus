"""One incident per alarm: one message when it starts, one when it clears.

An ALARM opens an incident and notifies once. Further ALARMs count and stay
quiet. OK starts a hold-down; if the alarm stays OK through it, the incident is
cleared with one message giving the duration and how many times it flapped. An
ALARM inside the hold-down is a flap and cancels the clearing.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Mapping, Optional

from .delivery_log import iso
from .events import parse_timestamp
from .store import Item, Store

PENDING_PK = "PENDING#clear"


def incident_key(account: str, region: str, alarm: str) -> str:
    return "INCIDENT#%s#%s#%s" % (account, region, alarm)


@dataclass(frozen=True)
class Decision:
    """What to do about one state change. `open` is the only one that notifies."""

    action: str  # open | counted | clearing | ignored | muted
    reason: str
    incident_key: str


@dataclass(frozen=True)
class Cleared:
    incident_key: str
    item: Dict[str, Any]
    duration_seconds: int
    flaps: int
    product: str
    site: Optional[str]
    opened_at: str


class IncidentTracker:
    def __init__(
        self,
        store: Store,
        clock: Callable[[], datetime],
        retention_days: int = 90,
    ):
        self._store = store
        self._clock = clock
        self._retention = timedelta(days=retention_days)

    def apply(
        self,
        *,
        account: str,
        region: str,
        alarm: str,
        state: str,
        hold_down_seconds: int,
        muted: bool,
        item: Mapping[str, Any],
        product: str,
        site: Optional[str],
    ) -> Decision:
        """Record one state change and say whether it deserves a message."""
        key = incident_key(account, region, alarm)
        if muted:
            return Decision("muted", "muted", key)
        if state not in ("ALARM", "OK"):
            return Decision("ignored", "insufficient data", key)
        now = self._clock()
        current = self._store.get(key, "STATE")
        if state == "OK" and (current is None or current["status"] == "closed"):
            return Decision("ignored", "no open incident", key)
        outcome: Dict[str, Decision] = {}

        def change(row: Optional[Item]) -> Item:
            open_row = row if row and row["status"] != "closed" else None
            if state == "ALARM":
                if open_row is None:
                    outcome["d"] = Decision("open", "incident opened", key)
                    return {
                        "status": "open",
                        "openedAt": iso(now),
                        "alarmCount": 1,
                        "flaps": 0,
                        "alarmName": alarm,
                        "product": product,
                        "site": site,
                        "item": dict(item),
                    }
                updated = dict(open_row)
                updated["alarmCount"] = open_row["alarmCount"] + 1
                if open_row["status"] == "clearing":
                    updated["status"] = "open"
                    updated["flaps"] = open_row["flaps"] + 1
                    updated.pop("clearStartedAt", None)
                outcome["d"] = Decision("counted", "incident already open", key)
                return updated
            if open_row["status"] == "clearing":
                outcome["d"] = Decision("counted", "waiting out the hold-down", key)
                return dict(open_row)
            updated = dict(open_row)
            updated["status"] = "clearing"
            updated["clearStartedAt"] = iso(now)
            outcome["d"] = Decision("clearing", "waiting out the hold-down", key)
            return updated

        self._store.update(key, "STATE", change)
        decision = outcome["d"]
        if decision.action == "clearing":
            self._store.put(
                {
                    "pk": PENDING_PK,
                    "sk": key,
                    "dueAt": int(now.timestamp()) + hold_down_seconds,
                }
            )
        elif state == "ALARM" and decision.action == "counted":
            self._store.delete(PENDING_PK, key)
        return decision

    def due_clears(self) -> List[Cleared]:
        """Close every incident that has stayed OK through its hold-down."""
        now = self._clock()
        cleared: List[Cleared] = []
        for pending in self._store.query(PENDING_PK):
            if pending["dueAt"] > int(now.timestamp()):
                continue
            key = pending["sk"]
            closed: Dict[str, Item] = {}

            def change(row: Optional[Item]) -> Item:
                updated = dict(row or {})
                if row and row["status"] == "clearing":
                    updated["status"] = "closed"
                    updated["expiresAt"] = int((now + self._retention).timestamp())
                    closed["row"] = row  # noqa: B023
                return updated

            if self._store.get(key, "STATE") is None:
                self._store.delete(PENDING_PK, key)
                continue
            self._store.update(key, "STATE", change)
            self._store.delete(PENDING_PK, key)
            row = closed.get("row")
            if row is None:
                continue
            started = parse_timestamp(row["openedAt"])
            ended = parse_timestamp(row["clearStartedAt"])
            cleared.append(
                Cleared(
                    incident_key=key,
                    item=row["item"],
                    duration_seconds=max(0, int((ended - started).total_seconds())),
                    flaps=row["flaps"],
                    product=row["product"],
                    site=row.get("site"),
                    opened_at=row["openedAt"],
                )
            )
        return cleared
