"""SQS consumer for `caducus-email`: send one queued email delivery."""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, Mapping, Optional

from ..channels.base import Channel
from ..delivery_log import FINAL
from ..metrics import HEARTBEAT_KIND, HEARTBEAT_SEND_FAILED, Metrics
from ..store import Store


def handle_batch(
    store: Store,
    channel: Channel,
    sqs_event: Mapping[str, Any],
    metrics: Optional[Metrics] = None,
    fan_out: Optional[Callable[[Mapping[str, Any]], Any]] = None,
) -> Dict[str, Any]:
    """`fan_out` (the router's `fan_out_page`) handles the page messages of public kinds."""
    failures = []
    for record in sqs_event["Records"]:
        row = None
        try:
            body = json.loads(record["body"])
            if "fanout" in body:
                if fan_out is None:
                    raise RuntimeError("a fan-out page arrived but no router was given")
                fan_out(body)
                continue
            row = store.get("EVENT#" + body["eventId"], "DELIVERY#" + body["deliveryId"])
            if row is not None and row["status"] not in FINAL:
                channel.send(row)
        except Exception:
            if metrics is not None and row is not None and row.get("kind") == HEARTBEAT_KIND:
                metrics.count(HEARTBEAT_SEND_FAILED)  # a refused heartbeat send
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}


def handler(event: Mapping[str, Any], context: Any = None) -> Dict[str, Any]:
    from ..wiring import runtime

    rt = runtime()
    return handle_batch(rt.store, rt.email, event, rt.metrics, rt.router.fan_out_page)
