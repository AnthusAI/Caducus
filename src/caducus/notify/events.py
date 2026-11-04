"""The one event format every Anthus app emits."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Mapping, Optional

SCHEMA_VERSION = 1
SEVERITIES = ("info", "warning", "critical")
DETAIL_TYPE = "Notification Requested"
SOURCE_PREFIX = "anthus."


class EventInvalid(ValueError):
    """The event does not match schemas/notification-event.v1.json (shipped in the package)."""


@dataclass(frozen=True)
class Event:
    """One accepted notification request. Apps name a kind, never recipients."""

    event_id: str
    kind: str
    product: str
    occurred_at: str
    severity: str
    facts: Dict[str, Any]
    site: Optional[str] = None
    group_key: Optional[str] = None
    audience: Optional[str] = None
    schema: int = field(default=SCHEMA_VERSION)

    def to_detail(self) -> Dict[str, Any]:
        """Return the event as EventBridge `detail`."""
        detail: Dict[str, Any] = {
            "schema": self.schema,
            "eventId": self.event_id,
            "kind": self.kind,
            "product": self.product,
            "occurredAt": self.occurred_at,
            "severity": self.severity,
            "facts": dict(self.facts),
        }
        for name, value in (
            ("site", self.site),
            ("groupKey", self.group_key),
            ("audience", self.audience),
        ):
            if value is not None:
                detail[name] = value
        return detail


def _text(detail: Mapping[str, Any], name: str, required: bool = True) -> Optional[str]:
    value = detail.get(name)
    if value is None:
        if required:
            raise EventInvalid("%s is required" % name)
        return None
    if not isinstance(value, str) or not value.strip():
        raise EventInvalid("%s must be a non-empty string" % name)
    return value


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO 8601 timestamp, accepting a trailing Z."""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise EventInvalid("occurredAt must be an ISO 8601 timestamp") from None


def parse_event(detail: Any) -> Event:
    """Validate EventBridge `detail` and return the Event, or raise EventInvalid."""
    if not isinstance(detail, Mapping):
        raise EventInvalid("detail must be an object")
    if detail.get("schema") != SCHEMA_VERSION or isinstance(detail.get("schema"), bool):
        raise EventInvalid("schema must be %d" % SCHEMA_VERSION)
    occurred_at = _text(detail, "occurredAt")
    parse_timestamp(occurred_at)
    severity = _text(detail, "severity")
    if severity not in SEVERITIES:
        raise EventInvalid("severity must be one of %s" % ", ".join(SEVERITIES))
    facts = detail.get("facts")
    if not isinstance(facts, Mapping):
        raise EventInvalid("facts must be an object")
    _text(facts, "title")
    for name in ("summary", "url"):
        _text(facts, name, required=False)
    return Event(
        event_id=_text(detail, "eventId"),
        kind=_text(detail, "kind"),
        product=_text(detail, "product"),
        occurred_at=occurred_at,
        severity=severity,
        facts=dict(facts),
        site=_text(detail, "site", required=False),
        group_key=_text(detail, "groupKey", required=False),
        audience=_text(detail, "audience", required=False),
    )
