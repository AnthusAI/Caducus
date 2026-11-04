"""EventBridge target for SES events of the host's configuration set.

First version: only the heartbeat. A delivered heartbeat counts `HeartbeatDelivered`;
a heartbeat that bounced, was rejected or drew a complaint counts `HeartbeatSendFailed`.
Other mail is ignored here; updating the delivery log from SES events is a later story.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional

from ..metrics import (
    HEARTBEAT_DELIVERED,
    HEARTBEAT_KIND,
    HEARTBEAT_SEND_FAILED,
    Metrics,
)

FAILED = ("Bounce", "Reject", "Complaint", "RenderingFailure")


def _tag(detail: Mapping[str, Any], name: str) -> Optional[str]:
    values = (detail.get("mail") or {}).get("tags", {}).get(name)
    if isinstance(values, list) and values:
        return str(values[0])
    return values if isinstance(values, str) else None


def handle_event(metrics: Metrics, envelope: Mapping[str, Any]) -> Optional[str]:
    """Count the heartbeat outcome in `envelope`. Returns the metric name, or None."""
    detail = envelope.get("detail") or {}
    if _tag(detail, "kind") != HEARTBEAT_KIND:
        return None
    event_type = detail.get("eventType")
    if event_type == "Delivery":
        name = HEARTBEAT_DELIVERED
    elif event_type in FAILED:
        name = HEARTBEAT_SEND_FAILED
    else:
        return None
    metrics.count(name)
    return name


def handler(event: Mapping[str, Any], context: Any = None) -> Mapping[str, Any]:
    from ..wiring import runtime

    return {"metric": handle_event(runtime().metrics, event)}
