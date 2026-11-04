"""Open incidents for alarms that are already firing, and catch lost events.

Runs on a schedule. It asks CloudWatch which alarms are in ALARM and feeds each
through the same path as a live state change. The event id is built from the
alarm's last state change, so an alarm the live event already announced is a
duplicate and sends nothing. It does not clear incidents.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Iterable, Iterator, Mapping, Sequence

from ..catalog import Catalog
from ..cloudwatch import DETAIL_TYPE, SOURCE
from ..routing import Router
from .router import process_envelope


def envelope_for(alarm: Mapping[str, Any]) -> Dict[str, Any]:
    """The state-change envelope CloudWatch would have sent for `alarm` (a describe_alarms row)."""
    arn_parts = alarm["AlarmArn"].split(":")
    changed = alarm["StateUpdatedTimestamp"]
    stamp = changed.strftime("%Y-%m-%dT%H:%M:%S.") + "%03d+0000" % (changed.microsecond // 1000)
    return {
        "source": SOURCE,
        "detail-type": DETAIL_TYPE,
        "account": arn_parts[4],
        "region": arn_parts[3],
        "time": changed.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "detail": {
            "alarmName": alarm["AlarmName"],
            "state": {
                "value": "ALARM",
                "reason": alarm.get("StateReason", ""),
                "reasonData": alarm.get("StateReasonData"),
                "timestamp": stamp,
            },
            "previousState": {},
        },
    }


def firing_alarms(client: Any) -> Iterator[Mapping[str, Any]]:
    paginator = client.get_paginator("describe_alarms")
    for page in paginator.paginate(
        StateValue="ALARM", AlarmTypes=["MetricAlarm", "CompositeAlarm"]
    ):
        for key in ("MetricAlarms", "CompositeAlarms"):
            for alarm in page.get(key, []):
                yield alarm


def reconcile(
    router: Router,
    catalog: Catalog,
    alarms: Iterable[Mapping[str, Any]],
    ignore_prefixes: Sequence[str] = (),
) -> Dict[str, int]:
    counts = {"seen": 0, "ignored": 0, "opened": 0}
    for alarm in alarms:
        counts["seen"] += 1
        if any(alarm["AlarmName"].startswith(p) for p in ignore_prefixes if p):
            counts["ignored"] += 1
            continue
        result = process_envelope(router, catalog, envelope_for(alarm))
        if result.outcome == "accepted" and any(
            d.get("status") in ("queued", "grouped") for d in result.deliveries
        ):
            counts["opened"] += 1
    return counts


def handler(event: Any = None, context: Any = None) -> Dict[str, int]:
    import boto3

    from ..wiring import runtime

    rt = runtime()
    prefixes = [p.strip() for p in os.environ.get("IGNORE_ALARM_PREFIXES", "").split(",")]
    return reconcile(rt.router, rt.catalog, firing_alarms(boto3.client("cloudwatch")), prefixes)
