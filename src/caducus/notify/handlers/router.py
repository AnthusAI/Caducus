"""SQS consumer for `caducus-ingest`: EventBridge envelopes in, deliveries out."""

from __future__ import annotations

import json
from typing import Any, Dict, Mapping

from .. import cloudwatch
from ..events import DETAIL_TYPE
from ..routing import Router
from ..catalog import Catalog


def process_envelope(router: Router, catalog: Catalog, envelope: Mapping[str, Any]):  # type: ignore[no-untyped-def]
    """Route one EventBridge envelope from either source."""
    if cloudwatch.is_alarm_change(envelope):
        return router.handle_event(cloudwatch.event_from_alarm_change(envelope, catalog))
    if envelope.get("detail-type") == DETAIL_TYPE:
        return router.handle_detail(envelope.get("detail"))
    return router.handle_detail(None)


def handle_batch(router: Router, catalog: Catalog, sqs_event: Mapping[str, Any]) -> Dict[str, Any]:
    """Process an SQS batch; a failed record is reported so only it is retried."""
    failures = []
    for record in sqs_event["Records"]:
        try:
            process_envelope(router, catalog, json.loads(record["body"]))
        except Exception:
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}


def handler(event: Mapping[str, Any], context: Any = None) -> Dict[str, Any]:
    from ..wiring import runtime

    rt = runtime()
    return handle_batch(rt.router, rt.catalog, event)
