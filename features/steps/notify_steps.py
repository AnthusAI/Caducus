"""Step definitions for the notifications features. Everything runs on fakes."""

from __future__ import annotations

import json
import logging
import re
from urllib.parse import parse_qs, quote, urlparse

from behave import given, then, when

from caducus.notify.channels.email import Throttled
from caducus.notify.fakes import ACCOUNT_ID, World
from caducus.notify.handlers import router as router_handler
from caducus.notify.logs import contains_address
from caducus.notify.render.operator import render_operator_email

ALARM = "papyrus-monitoring-router-errors"


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__(logging.INFO)
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


def _world(context, operators=1):
    context.world = World(operators=operators)
    context.capture = _Capture()
    logger = logging.getLogger("caducus.notify")
    logger.setLevel(logging.INFO)
    for old in [h for h in logger.handlers if isinstance(h, _Capture)]:
        logger.removeHandler(old)
    logger.addHandler(context.capture)
    return context.world


def after_scenario_cleanup(context):
    logger = logging.getLogger("caducus.notify")
    for old in [h for h in logger.handlers if isinstance(h, _Capture)]:
        logger.removeHandler(old)


# -- the service ------------------------------------------------------------


@given("the notification service has one operator")
def step_one_operator(context):
    _world(context, 1)


@given("the notification service has {count:d} operators")
def step_n_operators(context, count):
    _world(context, count)


@given("the notification service has no operators")
def step_no_operators(context):
    _world(context, 0)


@given('the kind "{kind}" is registered')
def step_kind_registered(context, kind):
    assert context.world.kinds.get(kind) is not None


@given("the alarm hold-down is {seconds:d} seconds")
def step_hold_down(context, seconds):
    assert context.world.catalog.hold_down_seconds(ALARM) == seconds


@given("the alarm grouping window is {seconds:d} seconds")
def step_window(context, seconds):
    assert context.world.kinds.get("cloudwatch.alarm").grouping.window_seconds == seconds


# -- events ------------------------------------------------------------------


@when('an event of kind "{kind}" arrives with a title and the event id "{event_id}"')
def step_event_arrives(context, kind, event_id):
    context.result = context.world.router.handle_detail(
        context.world.notification(event_id, kind)
    )


@when('an event arrives with the event id "{event_id}" and no title')
def step_event_no_title(context, event_id):
    detail = context.world.notification(event_id)
    detail["facts"] = {}
    context.result = context.world.router.handle_detail(detail)


@given('an event with id "{event_id}" was accepted')
def step_event_accepted(context, event_id):
    result = context.world.router.handle_detail(context.world.notification(event_id))
    assert result.outcome == "accepted"
    context.queued_before = len(context.world.queues.sent)


@when('an event with id "{event_id}" arrives again')
def step_event_again(context, event_id):
    context.result = context.world.router.handle_detail(
        context.world.notification(event_id)
    )


@then("it is accepted for routing")
def step_accepted(context):
    assert context.result.outcome == "accepted", context.result
    assert context.world.queues.sent, "nothing was queued"


@then("no one is notified")
def step_no_one(context):
    assert context.world.queues.sent == []


@then('the delivery log records the event as refused with the reason "{reason}"')
def step_refused(context, reason):
    refused = context.world.rows("refused")
    assert [r["reason"] for r in refused] == [reason], refused


@then("nothing further is routed")
def step_nothing_further(context):
    assert context.result.outcome == "duplicate"
    assert len(context.world.queues.sent) == context.queued_before


# -- delivery log ------------------------------------------------------------


@given("a notification is queued for a person by email")
def step_queued_for_person(context):
    _world(context, 1)
    context.world.alarm(ALARM)
    (context.delivery,) = context.world.rows("queued")


@when("it is sent and the provider reports delivery")
def step_sent_and_delivered(context):
    world = context.world
    world.send_all_queued()
    row = context.delivery
    world.log.advance(row["eventId"], row["deliveryId"], "delivered")


@then(
    'the log shows the kind, the channel, a masked address, the provider id and "{status}"'
)
def step_log_shows(context, status):
    row = context.world.log.get(
        context.delivery["eventId"], context.delivery["deliveryId"]
    )
    assert row["kind"] == "cloudwatch.alarm"
    assert row["channel"] == "email"
    assert row["destinationMask"] == "o***@example.com"
    assert row["providerMessageId"] == "fake-message-1"
    assert row["status"] == status


@given('a delivery is recorded as "{status}"')
def step_recorded_as(context, status):
    _world(context, 1)
    context.world.alarm(ALARM)
    (row,) = context.world.rows("queued")
    context.world.log.advance(row["eventId"], row["deliveryId"], status)
    context.delivery = row


@when('a "{status}" report for it arrives afterwards')
def step_late_report(context, status):
    row = context.delivery
    context.world.log.advance(row["eventId"], row["deliveryId"], status)


@then('the delivery stays "{status}"')
def step_stays(context, status):
    row = context.world.log.get(
        context.delivery["eventId"], context.delivery["deliveryId"]
    )
    assert row["status"] == status, row["status"]


@given("a person turned this kind off")
def step_turned_off(context):
    world = _world(context, 1)
    world.preferences.turn_off(world.directory.operators()[0].key, "cloudwatch.alarm")


@when("an event of this kind is routed")
def step_routed(context):
    context.world.alarm(ALARM)


@then('the log shows "{status}" for that person')
def step_shows_for_person(context, status):
    person = context.world.directory.operators()[0].key
    rows = context.world.log.for_recipient(person)
    assert [r["status"] for r in rows] == [status], rows
    assert context.world.queues.sent == []


@then("no stored row and no log line contains the address")
def step_no_address(context):
    world = context.world
    stored = json.dumps(list(world.store.items.values()), default=str, sort_keys=True)
    for address in world.addresses:
        assert address not in stored, "address stored"
        assert all(address not in line for line in context.capture.lines)
    assert context.capture.lines, "expected log lines to scan"
    assert not any(contains_address(line) for line in context.capture.lines)


# -- routing -----------------------------------------------------------------


@when("an operator event arrives twice")
def step_operator_event_twice(context):
    envelope = context.world.alarm_envelope(ALARM)
    for _ in range(2):
        router_handler.process_envelope(
            context.world.router, context.world.catalog, envelope
        )


@when("an operator event arrives")
def step_operator_event(context):
    context.world.alarm(ALARM)


@then("each operator has exactly one queued email delivery")
def step_each_once(context):
    world = context.world
    queued = world.rows("queued")
    assert len(queued) == len(world.addresses)
    assert sorted(r["recipientKey"] for r in queued) == sorted(
        r.key for r in world.directory.operators()
    )
    assert len(world.queued_for("email")) == len(world.addresses)


@then("the operator has exactly one queued email delivery")
def step_operator_once(context):
    assert len(context.world.rows("queued")) == 1
    assert len(context.world.queued_for("email")) == 1


@then('the delivery log records "{status}" with the reason "{reason}"')
def step_log_records(context, status, reason):
    rows = context.world.rows(status)
    assert [r["reason"] for r in rows] == [reason], rows


@when("the same queue message is handled twice")
def step_same_message_twice(context):
    world = context.world
    event = {
        "Records": [
            {"messageId": "m1", "body": json.dumps(world.alarm_envelope(ALARM))}
        ]
    }
    for _ in range(2):
        out = router_handler.handle_batch(world.router, world.catalog, event)
        assert out == {"batchItemFailures": []}


# -- incidents ---------------------------------------------------------------


@given("an alarm entered ALARM and a message went out")
def step_alarm_entered(context):
    context.world.alarm(ALARM)
    assert len(context.world.queued_for("email")) == 1


@given("the alarm went OK")
def step_went_ok(context):
    context.world.alarm(ALARM, "OK")


@given("the alarm went OK {minutes:d} minutes later")
def step_went_ok_later(context, minutes):
    context.world.clock.advance(minutes * 60)
    context.world.alarm(ALARM, "OK")


@given("the alarm went back to ALARM")
def step_went_back(context):
    context.world.clock.advance(60)
    context.world.alarm(ALARM, "ALARM")


@when("it goes OK and back to ALARM within the hold-down, three times")
def step_flaps(context):
    for _ in range(3):
        context.world.clock.advance(30)
        context.world.alarm(ALARM, "OK")
        context.world.clock.advance(30)
        context.world.alarm(ALARM, "ALARM")


@then("no further message goes out")
def step_no_further(context):
    context.world.tick(0)
    assert len(context.world.queued_for("email")) == 1


@when("the hold-down passes")
def step_hold_down_passes(context):
    context.world.tick(301)


@then("one message says it cleared, how long it lasted and how many times it flapped")
def step_cleared_message(context):
    world = context.world
    assert len(world.queued_for("email")) == 2
    world.send_all_queued()
    mail = world.mailer.calls[-1]
    assert mail["subject"].startswith("[CLEARED]")
    assert "It lasted 10 minutes and flapped 0 times." in mail["text"]


@then("no clearing message goes out")
def step_no_clearing(context):
    assert len(context.world.queued_for("email")) == 1


@when("an alarm enters INSUFFICIENT_DATA")
def step_insufficient(context):
    context.world.alarm(ALARM, "INSUFFICIENT_DATA")


@then("no message goes out and the transition is logged")
def step_logged(context):
    assert context.world.queues.sent == []
    lines = [json.loads(line) for line in context.capture.lines]
    assert any(
        line["message"] == "alarm_transition" and line["action"] == "ignored"
        for line in lines
    )


@when("a muted alarm enters ALARM")
def step_muted(context):
    context.world.alarm(ALARM, "ALARM", muted=True)


@then("no message goes out and the delivery log says it was muted")
def step_muted_logged(context):
    assert context.world.queues.sent == []
    assert [r["reason"] for r in context.world.rows("suppressed")] == ["muted"]


# -- triage ------------------------------------------------------------------


@given('the alarm "{name}" is in the catalog')
def step_in_catalog(context, name):
    assert context.world.catalog.match(name) is not None
    context.alarm_name = name


@given("an alarm that is not in the catalog")
def step_not_in_catalog(context):
    context.alarm_name = "some-unlisted-alarm"
    assert context.world.catalog.match(context.alarm_name) is None


@when("it fires")
def step_fires(context):
    context.world.alarm(context.alarm_name)
    context.world.send_all_queued()
    context.mail = context.world.mailer.calls[0]


@then("the message names what broke, where, the impact and the next step")
def step_names(context):
    for label in ("What broke", "Where", "Impact", "Next step"):
        match = re.search(r"^%s: (.+)$" % label, context.mail["text"], re.M)
        assert match, label


@then("it links straight to that alarm in the right AWS account")
def step_links(context):
    links = re.findall(r"^Open the alarm: (\S+)$", context.mail["text"], re.M)
    assert len(links) == 1
    query = parse_qs(urlparse(links[0]).fragment.split("?", 1)[1])
    assert query["account_id"] == [ACCOUNT_ID]
    assert query["destination"][0].endswith("alarm/" + quote(context.alarm_name, safe=""))


@then('the message says the site is "{site}"')
def step_site(context, site):
    assert "papyrus/%s" % site in context.mail["text"]


@then("the message gives its name, its account and its reason")
def step_unknown_fields(context):
    text = context.mail["text"]
    assert context.alarm_name in text
    assert "papyrus-production" in text
    assert "Threshold Crossed" in text


@then('it is labelled "{label}"')
def step_label(context, label):
    assert label in context.mail["text"]
    assert label in context.mail["html"]


@then('the subject starts with "{prefix}"')
def step_subject(context, prefix):
    assert context.mail["subject"].startswith(prefix)


@then('the body says "{word}" with a mark beside it')
def step_mark(context, word):
    assert "▲ " + word in context.mail["text"]
    assert "▲ " + word in context.mail["html"]


# -- grouping ----------------------------------------------------------------


@given("alarms for one site")
def step_alarms_one_site(context):
    context.site = "alpha"


def _fire(context, site, count, state="ALARM"):
    for n in range(1, count + 1):
        context.world.clock.advance(1)
        context.world.alarm("storm-%s-%d" % (site, n), state)


@when("ten fire within the grouping window")
def step_fire_ten(context):
    _fire(context, context.site, 10)


@when("{count:d} fire within the grouping window")
def step_fire_n(context, count):
    _fire(context, context.site, count)


@then("the first sends at once")
def step_first_at_once(context):
    assert len(context.world.queued_for("email")) == 1


@then("one further message lists the other nine, each with its link")
def step_further(context):
    world = context.world
    world.tick(121)
    assert len(world.queued_for("email")) == 2
    world.send_all_queued()
    group_mail = world.mailer.calls[-1]
    assert group_mail["subject"].startswith("[ALARM] papyrus/alpha: 9 alarms")
    assert group_mail["text"].count("Open the alarm: https://") == 9
    assert group_mail["text"].count("Next step:") == 9


@then("all {count:d} are in the delivery log")
def step_all_logged(context, count):
    rows = [r for r in context.world.rows() if r["eventId"].startswith("cw:")]
    assert len(rows) == count
    assert {r["status"] for r in rows} <= {"queued", "sent", "grouped"}


@then("all ten are in the delivery log")
def step_all_ten(context):
    step_all_logged(context, 10)


@when("alarms fire for two different sites")
def step_two_sites(context):
    context.world.alarm("storm-alpha-1")
    context.world.alarm("storm-beta-1")


@then("each site has its own message")
def step_each_site(context):
    world = context.world
    world.send_all_queued()
    subjects = sorted(m["subject"] for m in world.mailer.calls)
    assert len(subjects) == 2
    assert "papyrus/alpha" in subjects[0] and "papyrus/beta" in subjects[1]


@then("{count:d} messages were queued in all")
def step_messages_total(context, count):
    context.world.tick(121)
    assert len(context.world.queued_for("email")) == count


@when("ten fire and all clear within the grouping window")
def step_ten_fire_clear(context):
    world = context.world
    _fire(context, context.site, 10)
    world.tick(121)
    world.send_all_queued()
    world.mailer.calls.clear()
    before = len(world.queued_for("email"))
    context.before_clear = before
    _fire(context, context.site, 10, state="OK")
    world.tick(301)


@then("the first clearing sends at once")
def step_first_clearing(context):
    world = context.world
    assert len(world.queued_for("email")) == context.before_clear + 1


@then("one further message lists the other nine clearings")
def step_nine_clearings(context):
    world = context.world
    world.tick(121)
    assert len(world.queued_for("email")) == context.before_clear + 2
    world.send_all_queued()
    subjects = [m["subject"] for m in world.mailer.calls]
    assert subjects[0].startswith("[CLEARED]")
    assert subjects[1].startswith("[CLEARED] papyrus/alpha: 9 alarms"), subjects


# -- email -------------------------------------------------------------------


@given("a queued email delivery")
def step_queued_delivery(context):
    context.world.alarm(ALARM)
    (context.delivery,) = context.world.rows("queued")


@when("the sender handles it twice")
def step_handles_twice(context):
    for _ in range(2):
        context.world.email.send(context.delivery)


@then("SES was called once")
def step_ses_once(context):
    assert len(context.world.mailer.calls) == 1


@then('the log shows "sent" with the provider id')
def step_sent_provider(context):
    row = context.world.log.get(
        context.delivery["eventId"], context.delivery["deliveryId"]
    )
    assert row["status"] == "sent" and row["providerMessageId"] == "fake-message-1"


@given("SES answers with a throttling error twice")
def step_throttle_twice(context):
    context.world.mailer.script = [Throttled(), Throttled()]


@when("the sender handles it")
def step_handles(context):
    context.world.email.send(context.delivery)


@then("it retries with backoff of 1 and 2 seconds")
def step_backoff(context):
    assert context.world.sleeps == [1, 2]


@then('SES accepted it once and the delivery is "{status}"')
def step_accepted_once(context, status):
    assert len(context.world.mailer.calls) == 1
    row = context.world.log.get(
        context.delivery["eventId"], context.delivery["deliveryId"]
    )
    assert row["status"] == status


@given("SES refuses the first call")
def step_refuses(context):
    context.world.mailer.script = [RuntimeError("refused")]


@when("the sender handles it and fails")
def step_handles_fails(context):
    try:
        context.world.email.send(context.delivery)
    except RuntimeError:
        context.send_failed = True
    assert context.send_failed


@when("the sender handles it again")
def step_handles_again(context):
    context.world.email.send(context.delivery)


@when("any notification email is rendered")
def step_render_any(context):
    world = _world(context, 1)
    world.alarm(ALARM)
    world.alarm("some-unlisted-alarm")
    context.rendered = [
        render_operator_email([i for i in [_item(world, body)]])
        for body in world.queued_for("email")
    ]


def _item(world, body):
    return world.store.get("EVENT#" + body["eventId"], "CONTENT#" + body["deliveryId"])[
        "items"
    ][0]


@then("it has no tracking image and every link points at its real destination")
def step_no_tracking(context):
    for mail in context.rendered:
        assert "<img" not in mail.html.lower()
        assert "url(" not in mail.html.lower()
        hrefs = re.findall(r'href="([^"]+)"', mail.html)
        assert hrefs
        for href in hrefs:
            assert href.startswith("https://portal.example.awsapps.com/start/#/console?")
            assert "account_id=" in href and "destination=" in href


@then("it has a plain-text part")
def step_text_part(context):
    for mail in context.rendered:
        assert mail.text.strip()
