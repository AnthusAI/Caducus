"""Build the message content an operator can act on without logging in to AWS."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional
from urllib.parse import quote

from .catalog import Catalog
from .events import Event

NO_NOTES = "No triage notes yet"


@dataclass(frozen=True)
class TriageSettings:
    """From SSM at runtime, never from Git: the portal, the role, account names."""

    portal_url: str = ""
    role_name: str = ""
    accounts: Mapping[str, str] = field(default_factory=dict)


def _enc(value: str) -> str:
    return quote(value, safe="")


def alarm_page_url(region: str, alarm_name: str) -> str:
    return (
        "https://%s.console.aws.amazon.com/cloudwatch/home?region=%s"
        "#alarmsV2:alarm/%s" % (_enc(region), _enc(region), _enc(alarm_name))
    )


def console_link(
    settings: TriageSettings, account_id: str, region: str, alarm_name: str
) -> str:
    """An IAM Identity Center shortcut straight to the alarm. It grants nothing new."""
    destination = alarm_page_url(region, alarm_name)
    if not (settings.portal_url and settings.role_name):
        return destination
    return "%s/#/console?account_id=%s&role_name=%s&destination=%s" % (
        settings.portal_url.rstrip("/"),
        _enc(account_id),
        _enc(settings.role_name),
        _enc(destination),
    )


def datapoints_in_words(reason_data: Optional[str]) -> str:
    """Say what the alarm saw, from CloudWatch's own reasonData, or nothing."""
    if not reason_data:
        return ""
    try:
        data = json.loads(reason_data)
    except ValueError:
        return ""
    if not isinstance(data, dict):
        return ""
    parts = []
    points = data.get("recentDatapoints")
    if isinstance(points, list) and points:
        parts.append(
            "Recent datapoints: %s." % ", ".join(_number(p) for p in points)
        )
    if data.get("threshold") is not None:
        parts.append("The threshold is %s." % _number(data["threshold"]))
    return " ".join(parts)


def _number(value: Any) -> str:
    if isinstance(value, float) and value == int(value):
        return str(int(value))
    return str(value)


def build_alarm_item(
    event: Event, catalog: Catalog, settings: TriageSettings
) -> Dict[str, Any]:
    """The content of an ALARM message for one `cloudwatch.alarm` event."""
    facts = event.facts
    name = facts["alarmName"]
    account = facts.get("awsAccountId", "")
    region = facts.get("region", "")
    found = catalog.match(name)
    item: Dict[str, Any] = {
        "state": "ALARM",
        "alarmName": name,
        "product": event.product,
        "site": event.site or "",
        "awsAccount": settings.accounts.get(account, account),
        "region": region,
        "reason": facts.get("reason", ""),
        "datapoints": datapoints_in_words(facts.get("reasonData")),
        "link": console_link(settings, account, region, name),
        "occurredAt": event.occurred_at,
        "hasNotes": found is not None,
    }
    if found:
        item.update(
            what=found.entry.what,
            impact=found.entry.impact,
            nextStep=found.entry.next_step,
        )
    else:
        item.update(what=name, impact="", nextStep="", notesLabel=NO_NOTES)
    return item


def build_cleared_item(
    opened: Mapping[str, Any], duration_seconds: int, flaps: int
) -> Dict[str, Any]:
    """The content of the message that says an incident ended."""
    cleared = dict(opened)
    cleared.update(
        state="CLEARED",
        durationSeconds=duration_seconds,
        flaps=flaps,
        reason="",
        datapoints="",
    )
    return cleared
