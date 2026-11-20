"""Turn a CloudWatch alarm state change (as EventBridge delivers it) into an Event."""

from __future__ import annotations

from typing import Any, Mapping, Optional

from .catalog import Catalog
from .events import Event

KIND = "cloudwatch.alarm"
SOURCE = "aws.cloudwatch"
DETAIL_TYPE = "CloudWatch Alarm State Change"
STATES = ("ALARM", "OK", "INSUFFICIENT_DATA")


def is_alarm_change(envelope: Mapping[str, Any]) -> bool:
    return (
        envelope.get("source") == SOURCE
        and envelope.get("detail-type") == DETAIL_TYPE
    )


def event_from_alarm_change(
    envelope: Mapping[str, Any], catalog: Optional[Catalog] = None
) -> Event:
    """Build the `cloudwatch.alarm` event for one state change.

    The event id is account, region, alarm, state and the time of the change, so
    EventBridge delivering the same change twice is one event.
    """
    detail = envelope["detail"]
    name = detail["alarmName"]
    state = detail["state"]
    account = envelope.get("account", "")
    region = envelope.get("region", "")
    found = catalog.match(name) if catalog else None
    reason = state.get("reason", "")
    return Event(
        event_id="cw:%s:%s:%s:%s:%s"
        % (account, region, name, state.get("value"), state.get("timestamp")),
        kind=KIND,
        product=found.entry.product if found else "unknown",
        site=found.site if found else None,
        occurred_at=envelope.get("time") or state.get("timestamp", ""),
        severity="critical" if state.get("value") == "ALARM" else "info",
        facts={
            "title": name,
            "summary": reason or name,
            "alarmName": name,
            "state": state.get("value"),
            "previousState": (detail.get("previousState") or {}).get("value"),
            "reason": reason,
            "reasonData": state.get("reasonData"),
            "awsAccountId": account,
            "region": region,
            "muted": bool(detail.get("muteDetail")),
        },
    )
