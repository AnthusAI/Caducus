"""Step definitions for the X channel and broadcast features. A fake X answers over real HTTP."""

from __future__ import annotations

import json
import logging
from decimal import Decimal

from behave import given, then, when

from caducus.notify.channels.x import UnknownOutcome
from caducus.notify.delivery_log import DeliveryLog
from caducus.notify.directory import ChannelAccount, account_key
from caducus.notify.fakes import FakeClock, World, XWorld, X_TOKEN
from caducus.notify.kinds import registry_from_mapping
from caducus.notify.prices import Price
from caducus.notify.store import MemoryStore

LINK = "https://example.com/s/story?utm_source=x"
TEXT_WITH_LINK = "A new Story " + LINK


class _Lines(logging.Handler):
    def __init__(self):
        super().__init__(logging.INFO)
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


def _row(context):
    (row,) = context.xw.log.for_event(context.event_ids[0])
    return row


# -- the host ---------------------------------------------------------------


@given('a host that connected the X account "{handle}" as a broadcast recipient')
def step_connected(context, handle):
    assert handle == "@example_pub"
    context.xw = XWorld()
    context.add_cleanup(context.xw.close)
    context.lines = _Lines()
    logger = logging.getLogger("caducus.notify")
    logger.setLevel(logging.INFO)
    logger.addHandler(context.lines)
    context.add_cleanup(logger.removeHandler, context.lines)
    context.results = []
    context.event_ids = []
    context.unknown = 0


@given('the host also connected the X account "{handle}"')
def step_also_connected(context, handle):
    context.xw.directory.connect(
        ChannelAccount(
            key=account_key("x", "acct2"), address=handle, channel="x", external_id="9002"
        )
    )


@given("X's price list as of 2026-11-12: post with a link 0.200 USD, delete 0.010 USD, read 0.005 USD per post")
def step_prices(context):
    assert context.xw.x._prices.post_with_url.amount == "0.200"


@given("the host has paused announcements because \"{reason}\"")
def step_paused(context, reason):
    context.xw.guard.paused = reason


@given("the host's budget is {amount} USD")
def step_budget(context, amount):
    context.xw.guard.budget = Decimal(amount)


# -- what X does ------------------------------------------------------------


@given("X does not answer the post request in time")
def step_no_answer(context):
    context.xw.x_server.script.append(("hang",))


@given("X creates the post but does not answer in time")
def step_post_then_hang(context):
    context.xw.x_server.script.append(("post-then-hang",))


@given("X answers the post request with a 503")
def step_503(context):
    context.xw.x_server.script.append(("status", 503, {"title": "Service Unavailable"}, {}))


@given("X answers the post request with 429 and a reset time {seconds:d} seconds away")
def step_429(context, seconds):
    reset = int(context.xw.clock().timestamp()) + seconds
    context.xw.x_server.script.append(
        ("status", 429, {"title": "Too Many Requests"}, {"x-rate-limit-reset": str(reset)})
    )


@given('X answers the post request with 403 "{detail}"')
def step_403(context, detail):
    context.xw.x_server.script.append(
        ("status", 403, {"title": "Forbidden", "detail": detail}, {})
    )


@given("the account already shows a post for the link")
def step_already_posted(context):
    context.xw.x_server.posts.append({"id": "777", "text": TEXT_WITH_LINK})


# -- the host sends ---------------------------------------------------------


def _announce(context, event_id, text, times):
    context.event_ids.append(event_id)
    for _ in range(times):
        context.results.append(
            context.xw.announce(event_id, text, url=LINK if LINK in (text or "") else "")
        )


@when('the host sends the announcement "{event_id}" with a link twice')
def step_send_twice(context, event_id):
    _announce(context, event_id, TEXT_WITH_LINK, 2)


@when('the host sends the announcement "{event_id}" with a link')
def step_send_once(context, event_id):
    _announce(context, event_id, TEXT_WITH_LINK, 1)


@when('the host sends the announcement "{event_id}" saying "{text}"')
def step_send_saying(context, event_id, text):
    _announce(context, event_id, text, 1)


@when('the host sends an announcement to the account "{key}"')
def step_send_to_account(context, key):
    context.results.append(context.xw.announce("story.announce:nobody", TEXT_WITH_LINK, account=key))


@when("the host sends an announcement with no text")
def step_send_no_text(context):
    context.results.append(context.xw.announce("story.announce:notext", None))


@when("the X channel handles the queue")
@when("the X channel handles the queue again")
def step_handle(context):
    for body in context.xw.queued_for("x"):
        row = context.xw.log.get(body["eventId"], body["deliveryId"])
        context.xw.x.send(row)


@when("the X channel handles the queue and the outcome is unknown")
def step_handle_unknown(context):
    for body in context.xw.queued_for("x"):
        row = context.xw.log.get(body["eventId"], body["deliveryId"])
        try:
            context.xw.x.send(row)
        except UnknownOutcome:
            context.unknown += 1
    assert context.unknown, "the channel should have reported an unknown outcome"


@given("a delivered announcement")
def step_delivered(context):
    step_connected(context, "@example_pub")
    _announce(context, "story.announce:acct1:delivered", TEXT_WITH_LINK, 1)
    step_handle(context)
    assert _row(context)["status"] == "sent"


@when('the host withdraws the post as "{who}"')
def step_withdraw(context, who):
    context.xw.x.delete(_row(context), by=who)


# -- what happened ----------------------------------------------------------


@then("exactly one post is made at X")
def step_one_post(context):
    assert len(context.xw.x_server.posts) == 1, context.xw.x_server.posts


@then("nothing was posted at X")
def step_no_post(context):
    assert context.xw.x_server.posts == []


@then("the post at X says exactly \"{text}\"")
def step_post_text(context, text):
    assert [p["text"] for p in context.xw.x_server.posts] == [text]
    (request,) = [r for r in context.xw.x_server.requests if r["method"] == "POST"]
    assert request["body"] == {"text": text}


@then("the delivery records the post id and a cost of {amount} USD")
def step_post_id_and_cost(context, amount):
    row = _row(context)
    assert row["providerMessageId"] == context.xw.x_server.posts[0]["id"]
    assert row["cost"] == amount and row["costCurrency"] == "USD", row


@then("the delivery records a cost of {amount} USD")
def step_cost(context, amount):
    row = _row(context)
    assert row["cost"] == amount and row["costCurrency"] == "USD", row


@then("the delivery notes the price list its cost came from")
def step_cost_note(context):
    assert "X price list, 2026-11-12" in _row(context)["costNote"]


@then("the account's recent posts were read before anything was posted again")
def step_read_first(context):
    methods = [r["method"] for r in context.xw.x_server.requests]
    assert methods == ["POST", "GET"], methods


@then('the delivery status is "{status}" with that post\'s id')
def step_status_with_id(context, status):
    row = _row(context)
    assert row["status"] == status
    assert row["providerMessageId"] == context.xw.x_server.posts[0]["id"]


@then('the delivery status is "{status}"')
def step_status(context, status):
    assert _row(context)["status"] == status


@then('the delivery status is still "{status}"')
def step_status_still(context, status):
    assert _row(context)["status"] == status


@then('the delivery failed and says why')
def step_failed_why(context):
    row = _row(context)
    assert row["status"] == "failed" and row["reason"], row


@then('the delivery was refused because "{reason}"')
def step_refused_because(context, reason):
    row = _row(context)
    assert row["status"] == "refused" and row["reason"] == reason, row


@then("the channel waited {seconds:d} seconds and then posted once")
def step_waited(context, seconds):
    assert context.xw.waits == [float(seconds)], context.xw.waits
    assert len(context.xw.x_server.posts) == 1


@then("the post is deleted at X")
def step_deleted(context):
    assert context.xw.x_server.posts == []
    assert [r["method"] for r in context.xw.x_server.requests][-1] == "DELETE"


@then('the delivery was withdrawn by "{who}" with the time')
def step_withdrawn(context, who):
    row = _row(context)
    assert row["status"] == "withdrawn" and row["withdrawnBy"] == who and row["withdrawnAt"], row


@then("the host's budget guard was told about {amount} USD")
def step_guard_told(context, amount):
    assert context.xw.guard.spent == Decimal(amount)


@then("the access token was sent to X as a bearer token")
def step_bearer(context):
    assert context.xw.x_server.requests
    assert all(r["authorization"] == "Bearer " + X_TOKEN for r in context.xw.x_server.requests)


@then("no token appears in the delivery log or in the logs")
def step_no_token(context):
    stored = json.dumps(sorted(map(str, context.xw.store.items.values())))
    assert X_TOKEN not in stored
    assert X_TOKEN not in "\n".join(context.lines.lines)
    assert context.lines.lines, "the channel should have logged something"


# -- broadcast routing ------------------------------------------------------


@then('there is one delivery, to the account "{handle}" on the channel "{channel}"')
def step_one_delivery(context, handle, channel):
    (row,) = context.xw.rows()
    assert row["recipientKey"] == context.xw.account.key
    assert row["destinationMask"] == handle and row["channel"] == channel
    assert row["class"] == "broadcast"


@then("the second send was reported as a duplicate")
def step_second_duplicate(context):
    assert [r.outcome for r in context.results] == ["accepted", "duplicate"]


@given("every person has turned off every kind")
def step_everyone_off(context):
    context.looked_up = []
    context.xw.preferences.opted_out = lambda *args: context.looked_up.append(args) or True


@then("no preference, consent or suppression was looked up")
def step_no_lookup(context):
    assert context.looked_up == []


@then("one delivery is queued")
def step_one_queued(context):
    assert [r["status"] for r in context.xw.rows()] == ["queued"]


@then('one delivery is queued, to "{handle}" only')
def step_one_queued_to(context, handle):
    (row,) = context.xw.rows()
    assert row["status"] == "queued" and row["destinationMask"] == handle


@then('the event is refused with "{reason}"')
def step_refused(context, reason):
    result = context.results[-1]
    assert result.outcome == "refused" and reason in result.reason, result
    assert context.xw.queued_for("x") == []


@when("a host registers a broadcast kind that groups its events")
def step_bad_kind(context):
    try:
        registry_from_mapping(
            {"kinds": {"k": {"class": "broadcast", "channels": ["x"],
                             "grouping": {"by": ["product"], "windowSeconds": 60}}}}
        )
        context.refused = None
    except ValueError as error:
        context.refused = str(error)


@then("the registry refuses it")
def step_registry_refuses(context):
    assert context.refused and "broadcast" in context.refused


# -- delivery cost on the log ----------------------------------------------


@given("a delivery on a channel that charges per request")
def step_billable(context):
    context.log = DeliveryLog(MemoryStore(), FakeClock())
    context.delivery = context.log.create(
        event_id="e1", kind="story.announce", klass="broadcast", product="papyrus",
        site="mom", recipient_key="CHANNELACCT#x#acct1", channel="x",
    )


@when("a charge of {first} {first_currency} and then a charge of {second} {second_currency} are recorded")
def step_two_charges(context, first, first_currency, second, second_currency):
    context.refused = None
    for amount, currency in ((first, first_currency), (second, second_currency)):
        try:
            context.log.advance("e1", context.delivery["deliveryId"], "queued",
                                charge=Price(amount, currency, "test price list"))
        except ValueError as error:
            context.refused = str(error)


def _logged(context):
    return context.log.get("e1", context.delivery["deliveryId"])


@then("the delivery shows a cost of {amount} {currency}")
@then("the delivery still shows a cost of {amount} {currency}")
def step_shows_cost(context, amount, currency):
    row = _logged(context)
    assert row["cost"] == amount and row["costCurrency"] == currency, row


@then("no later report lowers it")
def step_cost_never_lower(context):
    for status in ("sent", "delivered", "bounced"):
        context.log.advance("e1", context.delivery["deliveryId"], status)
    assert _logged(context)["cost"] == "0.210"


@then("the second charge is refused")
def step_second_refused(context):
    assert context.refused and "EUR" in context.refused


@given("a delivery that was sent")
def step_was_sent(context):
    step_billable(context)
    context.log.advance("e1", context.delivery["deliveryId"], "sent", provider_message_id="p1")


@when('it is withdrawn by "{who}"')
def step_withdrawn_by(context, who):
    context.log.advance("e1", context.delivery["deliveryId"], "withdrawn", actor=who)


@then('the delivery shows "withdrawn", who withdrew it and when')
def step_shows_withdrawn(context):
    row = _logged(context)
    assert row["status"] == "withdrawn" and row["withdrawnBy"] == "editor@example.org"
    assert row["withdrawnAt"] and row["finalAt"]


@then('a later report of "failed" does not change it')
def step_failed_later(context):
    context.log.advance("e1", context.delivery["deliveryId"], "failed")
    assert _logged(context)["status"] == "withdrawn"
