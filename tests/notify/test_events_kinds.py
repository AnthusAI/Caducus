from __future__ import annotations

import copy
import json
import os

import pytest

from caducus.notify.events import EventInvalid, parse_event
from caducus.notify.kinds import load_registry, registry_from_mapping

SCHEMAS = os.path.join(os.path.dirname(__file__), "..", "..", "src", "caducus", "notify", "schemas")

VALID = {
    "schema": 1,
    "eventId": "e1",
    "kind": "story.published",
    "product": "papyrus",
    "site": "shipped",
    "occurredAt": "2026-11-04T12:00:00Z",
    "severity": "info",
    "groupKey": "g",
    "facts": {"title": "T", "summary": "S", "url": "https://example.com/x", "more": 1},
}


def _without(path):
    detail = copy.deepcopy(VALID)
    target = detail
    for part in path[:-1]:
        target = target[part]
    del target[path[-1]]
    return detail


def _with(path, value):
    detail = copy.deepcopy(VALID)
    target = detail
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value
    return detail


CASES = [
    VALID,
    _without(("site",)),
    _without(("groupKey",)),
    _without(("facts", "summary")),
    _without(("eventId",)),
    _without(("kind",)),
    _without(("product",)),
    _without(("occurredAt",)),
    _without(("severity",)),
    _without(("facts",)),
    _without(("facts", "title")),
    _with(("schema",), 2),
    _with(("schema",), True),
    _with(("severity",), "loud"),
    _with(("eventId",), ""),
    _with(("eventId",), 5),
    _with(("facts",), "text"),
    _with(("extra",), 1),
    {**VALID, "audience": "person-1"},
]


def test_parse_event_round_trips():
    event = parse_event(VALID)
    assert event.to_detail() == VALID
    assert event.kind == "story.published"


def test_parse_event_rejects_a_timestamp_that_is_not_iso():
    with pytest.raises(EventInvalid, match="occurredAt"):
        parse_event(_with(("occurredAt",), "yesterday"))


def test_parse_event_rejects_non_objects():
    with pytest.raises(EventInvalid):
        parse_event(None)


@pytest.mark.parametrize("detail", CASES)
def test_parser_and_json_schema_agree(detail):
    jsonschema = pytest.importorskip("jsonschema")
    with open(os.path.join(SCHEMAS, "notification-event.v1.json")) as handle:
        schema = json.load(handle)
    validator = jsonschema.Draft202012Validator(
        schema, format_checker=jsonschema.FormatChecker()
    )
    schema_ok = not list(validator.iter_errors(detail))
    try:
        parse_event(detail)
        parser_ok = True
    except EventInvalid:
        parser_ok = False
    # The parser is allowed to be a little stricter than the schema only for
    # unknown top-level fields, which the schema forbids and the parser ignores.
    if "extra" in detail:
        assert not schema_ok
        return
    assert schema_ok == parser_ok


def test_registry_loads_the_shipped_kinds():
    registry = load_registry()
    kind = registry.get("cloudwatch.alarm")
    assert kind.klass == "operator" and kind.channels == ("email",)
    assert kind.grouping.by == ("product", "site")
    assert kind.lifecycle == "incident"
    assert registry.get("nope") is None
    assert registry.names() == ("cloudwatch.alarm",)


@pytest.mark.parametrize(
    "raw,message",
    [
        ({"class": "everyone", "channels": ["email"]}, "class"),
        ({"class": "operator", "channels": []}, "channel"),
        ({"class": "operator", "channels": ["email"], "ack": "maybe"}, "ack"),
        (
            {
                "class": "operator",
                "channels": ["email"],
                "grouping": {"by": ["colour"], "windowSeconds": 5},
            },
            "group",
        ),
    ],
)
def test_registry_rejects_bad_kinds(raw, message):
    with pytest.raises(ValueError, match=message):
        registry_from_mapping({"kinds": {"x.y": raw}})
