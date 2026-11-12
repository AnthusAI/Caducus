from __future__ import annotations

import json
import os

import pytest

from caducus.notify.channels.x import UnknownOutcome, UrllibHttp, XChannel
from caducus.notify.fakes import FakeGuard, FakeTokens, X_PRICES, XWorld
from caducus.notify.prices import Price

LINK = "https://example.com/s/one"


@pytest.fixture
def world():
    w = XWorld()
    yield w
    w.close()


def test_price_validation():
    assert Price("0.200", "USD", "list").times(3).amount == "0.600"
    for bad in (("-1", "USD", "n"), ("abc", "USD", "n"), ("1", "", "n"), ("1", "USD", " ")):
        with pytest.raises(ValueError):
            Price(*bad)


def test_the_built_in_reader_needs_a_look_back_count(world):
    with pytest.raises(ValueError):
        XChannel(
            world.store, world.log, world.directory, tokens=FakeTokens(), prices=X_PRICES,
            guard=FakeGuard(), clock=world.clock, http=UrllibHttp(timeout=1),
        )


def test_no_answer_at_all_is_an_unknown_outcome(world):
    world.announce("e1", "Words " + LINK, url=LINK)
    world.x_server.close()
    row = world.queued_row("e1")
    with pytest.raises(UnknownOutcome):
        world.x.send(row)
    assert world.queued_row("e1")["attempts"] == 1
    assert world.queued_row("e1")["status"] == "queued"


def test_a_429_without_a_reset_time_is_not_guessed(world):
    world.x_server.script.append(("status", 429, {"title": "Too Many Requests"}, {}))
    world.announce("e1", "Words " + LINK, url=LINK)
    out = world.x.send(world.queued_row("e1"))
    assert out.status == "failed" and "x-rate-limit-reset" in out.reason
    assert world.waits == [] and world.x_server.posts == []


def test_a_redelivered_message_after_sent_is_a_duplicate(world):
    world.announce("e1", "Words " + LINK, url=LINK)
    first = world.x.send(world.queued_row("e1"))
    again = world.x.send(world.queued_row("e1"))
    assert (first.status, again.status) == ("sent", "duplicate")
    assert len(world.x_server.posts) == 1


def test_delete_is_idempotent_and_needs_a_post(world):
    world.announce("e1", "Words " + LINK, url=LINK)
    row = world.queued_row("e1")
    assert world.x.delete(row, by="editor").status == "failed"  # nothing posted yet
    world.x.send(row)
    assert world.x.delete(world.queued_row("e1"), by="editor").status == "withdrawn"
    assert world.x.delete(world.queued_row("e1"), by="editor").status == "duplicate"
    assert world.queued_row("e1")["cost"] == "0.210"
    assert world.guard.spent == __import__("decimal").Decimal("0.210")


def test_the_log_row_with_cost_matches_the_schema(world):
    jsonschema = pytest.importorskip("jsonschema")
    path = os.path.join(
        os.path.dirname(__file__), "..", "..", "src", "caducus", "notify", "schemas",
        "delivery-log.v1.json",
    )
    with open(path) as handle:
        schema = json.load(handle)
    world.announce("e1", "Words " + LINK, url=LINK)
    world.x.send(world.queued_row("e1"))
    world.x.delete(world.queued_row("e1"), by="editor")
    jsonschema.validate(world.queued_row("e1"), schema)
