"""SQS consumer for `caducus-email`: send one queued email delivery."""

from __future__ import annotations

import json
from typing import Any, Dict, Mapping

from ..channels.base import Channel
from ..delivery_log import FINAL
from ..store import Store


def handle_batch(
    store: Store, channel: Channel, sqs_event: Mapping[str, Any]
) -> Dict[str, Any]:
    failures = []
    for record in sqs_event["Records"]:
        try:
            body = json.loads(record["body"])
            row = store.get("EVENT#" + body["eventId"], "DELIVERY#" + body["deliveryId"])
            if row is not None and row["status"] not in FINAL:
                channel.send(row)
        except Exception:
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}


def handler(event: Mapping[str, Any], context: Any = None) -> Dict[str, Any]:
    from ..wiring import runtime

    rt = runtime()
    return handle_batch(rt.store, rt.email, event)
