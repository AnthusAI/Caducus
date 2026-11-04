from __future__ import annotations

import itertools
import json
import os

import pytest

from caducus.notify.delivery_log import (
    FINAL,
    RANK,
    STATUSES,
    TERMINAL,
    DeliveryLog,
    mask_address,
    next_status,
    recipient_key_for_address,
)
from caducus.notify.fakes import FakeClock
from caducus.notify.logs import contains_address, emit
from caducus.notify.store import MemoryStore

SCHEMAS = os.path.join(os.path.dirname(__file__), "..", "..", "src", "caducus", "notify", "schemas")


def expected(current, new):
    """The rule, written out independently of next_status."""
    if current in TERMINAL:
        return current
    if new in TERMINAL:
        return new if current in ("queued", "sent", "delivered", "delayed") else current
    order = ["queued", "sent", "delivered", "delayed"]
    worst = {"bounced", "complained", "rejected", "failed"}
    if current in worst:
        return current
    if new in worst:
        return new
    return new if order.index(new) > order.index(current) else current


@pytest.mark.parametrize("current,new", list(itertools.product(STATUSES, STATUSES)))
def test_every_status_transition(current, new):
    assert next_status(current, new) == expected(current, new)


def test_documented_examples():
    assert next_status("bounced", "delivered") == "bounced"
    assert next_status("delivered", "bounced") == "bounced"
    assert next_status("queued", "sent") == "sent"
    assert next_status("sent", "queued") == "sent"
    assert next_status("queued", "dead_lettered") == "dead_lettered"
    assert next_status("grouped", "sent") == "grouped"


def make_log(**kwargs):
    store = MemoryStore()
    return store, DeliveryLog(store, FakeClock(), **kwargs)


def create(log, **overrides):
    values = dict(
        event_id="e1", kind="cloudwatch.alarm", klass="operator", product="papyrus",
        site="shipped", recipient_key="ADDR#abc", channel="email",
        destination_mask="o***@example.com",
    )
    values.update(overrides)
    return log.create(**values)


def test_row_matches_the_published_schema():
    jsonschema = pytest.importorskip("jsonschema")
    with open(os.path.join(SCHEMAS, "delivery-log.v1.json")) as handle:
        schema = json.load(handle)
    store, log = make_log()
    row = create(log, reason="x", group_id="g", incident_key="i")
    log.advance("e1", row["deliveryId"], "sent", provider_message_id="m1", attempts=1)
    jsonschema.validate(store.get(row["pk"], row["sk"]), schema)
    assert set(schema["properties"]["status"]["enum"]) == set(STATUSES)


def test_expiry_is_set_from_the_retention_setting():
    _, log = make_log(retention_days=90)
    row = create(log)
    start = FakeClock().now.timestamp()
    assert row["expiresAt"] == int(start + 90 * 86400)
    _, short = make_log(retention_days=1)
    assert create(short)["expiresAt"] == int(start + 86400)


def test_advance_sets_times_and_provider_id_once():
    store, log = make_log()
    row = create(log)
    clock = log._clock
    clock.advance(5)
    sent = log.advance("e1", row["deliveryId"], "sent", provider_message_id="m1", attempts=2)
    assert sent["sentAt"] == "2026-11-04T12:00:05Z" and sent["attempts"] == 2
    assert "finalAt" not in sent
    clock.advance(5)
    done = log.advance("e1", row["deliveryId"], "delivered", provider_message_id="other")
    assert done["providerMessageId"] == "m1" and done["finalAt"] == "2026-11-04T12:00:10Z"


def test_decided_against_rows_are_final_at_birth():
    _, log = make_log()
    row = create(log, status="refused", reason="no audience")
    assert row["finalAt"] == row["queuedAt"] and row["status"] in FINAL


def test_create_is_idempotent_for_a_given_delivery_id():
    _, log = make_log()
    first = create(log, delivery_id="d1")
    log.advance("e1", "d1", "sent")
    again = create(log, delivery_id="d1")
    assert again["status"] == "sent" and again["deliveryId"] == first["deliveryId"]
    assert len(log.for_event("e1")) == 1


def test_unknown_status_and_missing_row_are_errors():
    _, log = make_log()
    with pytest.raises(ValueError):
        create(log, status="maybe")
    with pytest.raises(ValueError):
        log.advance("e1", "x", "maybe")
    with pytest.raises(KeyError):
        log.advance("e1", "missing", "sent")


def test_lookup_by_recipient_is_ordered_by_queue_time():
    _, log = make_log()
    a = create(log)
    log._clock.advance(10)
    b = create(log, event_id="e2")
    create(log, recipient_key="ADDR#other")
    assert [r["deliveryId"] for r in log.for_recipient("ADDR#abc")] == [
        a["deliveryId"], b["deliveryId"],
    ]


def test_masking_and_pseudonyms():
    assert mask_address("reader@example.com") == "r***@example.com"
    one = recipient_key_for_address("Reader@Example.com ", "secret")
    assert one == recipient_key_for_address("reader@example.com", "secret")
    assert one != recipient_key_for_address("reader@example.com", "other")
    assert "reader" not in one and "@" not in one


def test_log_lines_refuse_addresses(caplog):
    import logging

    logger = logging.getLogger("caducus.test")
    emit(logger, "ok", eventId="e1", status="sent")
    with pytest.raises(ValueError):
        emit(logger, "bad", destination="someone@example.com")
    assert contains_address("a@b.io") and not contains_address("e1 sent")


def test_rank_table_covers_exactly_the_non_terminal_statuses():
    assert set(RANK) | TERMINAL == set(STATUSES)
    assert not set(RANK) & TERMINAL
