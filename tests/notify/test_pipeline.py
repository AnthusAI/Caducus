"""Routing, incidents, triage, grouping and the email channel, on fakes."""

from __future__ import annotations

import json
import re
from urllib.parse import parse_qs, quote, urlparse

import pytest

from caducus.notify.channels.email import Pacer, Throttled
from caducus.notify.cloudwatch import event_from_alarm_change, is_alarm_change
from caducus.notify.fakes import ACCOUNT_ID, World
from caducus.notify.handlers import email_sender, router as router_handler
from caducus.notify.incidents import PENDING_PK, incident_key
from caducus.notify.render.operator import describe_duration, render_operator_email
from caducus.notify.triage import (
    TriageSettings, alarm_page_url, console_link, datapoints_in_words,
)

ALARM = "papyrus-monitoring-router-errors"


# -- routing -----------------------------------------------------------------


def test_unknown_kind_is_refused_once_with_a_reason():
    world = World()
    result = world.router.handle_detail(world.notification("e1", kind="no.such"))
    assert (result.outcome, result.reason) == ("refused", "unknown kind")
    assert [r["reason"] for r in world.rows("refused")] == ["unknown kind"]


def test_invalid_event_with_an_id_is_recorded_and_without_one_only_logged():
    world = World()
    bad = world.notification("e1")
    bad["severity"] = "loud"
    assert world.router.handle_detail(bad).outcome == "refused"
    assert world.rows("refused")[0]["reason"].startswith("invalid event:")
    assert world.router.handle_detail(None).outcome == "refused"
    assert len(world.rows("refused")) == 1


def test_alarm_event_without_alarm_facts_is_refused():
    world = World()
    detail = world.notification("e1")
    detail["facts"] = {"title": "T"}
    result = world.router.handle_detail(detail)
    assert result.reason == "invalid event: alarmName and state are required"


def test_notice_kind_without_lifecycle_is_routed_generically():
    from caducus.notify.kinds import registry_from_mapping

    world = World()
    world.router._kinds = registry_from_mapping(
        {"kinds": {"site.note": {"class": "operator", "channels": ["email"]}}}
    )
    world.dispatcher._kinds = world.router._kinds
    result = world.router.handle_detail(world.notification("n1", kind="site.note", summary="S"))
    assert result.outcome == "accepted"
    world.send_all_queued()
    assert "NOTICE" in world.mailer.calls[0]["text"]


def test_non_operator_audiences_are_visibly_refused_until_preferences_exist():
    from caducus.notify.kinds import registry_from_mapping

    world = World()
    world.router._kinds = registry_from_mapping(
        {"kinds": {"story.published": {"class": "public", "channels": ["email"]}}}
    )
    result = world.router.handle_detail(world.notification("p1", kind="story.published"))
    assert result.reason == "no audience"


def test_redelivery_cannot_create_a_second_delivery_even_by_id():
    world = World(operators=2)
    envelope = world.alarm_envelope(ALARM)
    for _ in range(3):
        router_handler.process_envelope(world.router, world.catalog, envelope)
    assert len(world.rows()) == 2 and len(world.queued_for("email")) == 2


def test_batch_reports_only_the_failing_record():
    world = World()
    good = {"messageId": "1", "body": json.dumps(world.alarm_envelope(ALARM))}
    bad = {"messageId": "2", "body": "{not json"}
    out = router_handler.handle_batch(world.router, world.catalog, {"Records": [good, bad]})
    assert out == {"batchItemFailures": [{"itemIdentifier": "2"}]}


def test_notification_requested_envelopes_are_routed_and_others_refused():
    world = World()
    envelope = {
        "source": "anthus.papyrus",
        "detail-type": "Notification Requested",
        "detail": world.notification("e9"),
    }
    assert router_handler.process_envelope(world.router, world.catalog, envelope).outcome == "accepted"
    other = {"source": "x", "detail-type": "Something else"}
    assert router_handler.process_envelope(world.router, world.catalog, other).outcome == "refused"


# -- cloudwatch ---------------------------------------------------------------


def test_cloudwatch_change_becomes_an_event_with_a_stable_id():
    world = World()
    envelope = world.alarm_envelope("papyrus-newsletter-shipped-bounce-rate")
    assert is_alarm_change(envelope) and not is_alarm_change({})
    one = event_from_alarm_change(envelope, world.catalog)
    two = event_from_alarm_change(envelope, world.catalog)
    assert one.event_id == two.event_id
    assert (one.product, one.site, one.severity) == ("papyrus", "shipped", "critical")
    assert one.facts["state"] == "ALARM" and one.facts["muted"] is False
    unknown = event_from_alarm_change(world.alarm_envelope("nope", "OK"), world.catalog)
    assert (unknown.product, unknown.site, unknown.severity) == ("unknown", None, "info")


# -- incidents ---------------------------------------------------------------

STATE_TABLE = [
    # (history, next state, reason recorded when no message goes out; None = notified)
    ([], "ALARM", None),
    ([], "OK", "no open incident"),
    ([], "INSUFFICIENT_DATA", "insufficient data"),
    (["ALARM"], "ALARM", "incident already open"),
    (["ALARM"], "OK", "waiting out the hold-down"),
    (["ALARM"], "INSUFFICIENT_DATA", "insufficient data"),
    (["ALARM", "OK"], "OK", "waiting out the hold-down"),
    (["ALARM", "OK"], "ALARM", "incident already open"),
    (["ALARM", "OK", "ALARM"], "OK", "waiting out the hold-down"),
]


@pytest.mark.parametrize("history,state,reason", STATE_TABLE)
def test_incident_state_machine(history, state, reason):
    world = World()
    for step in history:
        world.clock.advance(1)
        world.alarm(ALARM, step)
    queued_before = len(world.queued_for("email"))
    world.clock.advance(1)
    world.alarm(ALARM, state)
    notified = len(world.queued_for("email")) > queued_before
    if reason is None:
        assert notified
        return
    assert not notified
    latest = sorted(world.rows("suppressed"), key=lambda r: r["queuedAt"])[-1]
    assert latest["reason"] == reason


def test_clearing_message_has_duration_and_flap_count():
    world = World()
    world.alarm(ALARM)
    for _ in range(2):
        world.clock.advance(60)
        world.alarm(ALARM, "OK")
        world.clock.advance(60)
        world.alarm(ALARM, "ALARM")
    world.clock.advance(3600)
    world.alarm(ALARM, "OK")
    assert world.tick(299) == 0
    assert world.tick(2) == 1
    world.send_all_queued()
    mail = world.mailer.calls[-1]
    assert "It lasted 1 hour 4 minutes and flapped 2 times." in mail["text"]
    assert mail["subject"] == "[CLEARED] papyrus: %s is back to normal" % ALARM
    assert "✓ CLEARED" in mail["html"]


def test_a_new_alarm_after_clearing_is_a_new_incident():
    world = World()
    world.alarm(ALARM)
    world.alarm(ALARM, "OK")
    world.tick(301)
    world.alarm(ALARM)
    assert len(world.queued_for("email")) == 3  # opened, cleared, opened again


def test_pending_clear_is_removed_when_it_is_sent():
    world = World()
    world.alarm(ALARM)
    world.alarm(ALARM, "OK")
    assert world.store.query(PENDING_PK)
    world.tick(301)
    assert world.store.query(PENDING_PK) == []
    assert world.tick(1000) == 0


def test_incident_rows_are_per_account_region_and_alarm():
    world = World()
    world.alarm(ALARM)
    assert world.store.get(incident_key(ACCOUNT_ID, "us-east-1", ALARM), "STATE")["status"] == "open"


def test_hold_down_is_read_per_alarm_from_the_catalog():
    world = World()
    assert world.catalog.hold_down_seconds(ALARM) == 300
    from caducus.notify.catalog import catalog_from_mapping

    catalog = catalog_from_mapping(
        {
            "defaults": {"holdDownSeconds": 10},
            "alarms": [
                {"pattern": "^slow$", "product": "p", "what": "w", "impact": "i",
                 "next": "n", "holdDownSeconds": 900}
            ],
        }
    )
    assert (catalog.hold_down_seconds("slow"), catalog.hold_down_seconds("other")) == (900, 10)


def test_suppressed_rows_say_why():
    world = World()
    world.alarm(ALARM)
    world.alarm(ALARM)
    world.alarm(ALARM, "OK")
    world.alarm(ALARM, "INSUFFICIENT_DATA")
    reasons = sorted(r["reason"] for r in world.rows("suppressed"))
    assert reasons == ["incident already open", "insufficient data", "waiting out the hold-down"]


# -- triage ------------------------------------------------------------------


def test_every_catalog_entry_has_all_four_fields():
    world = World()
    entries = world.catalog._entries
    assert len(entries) >= 9
    for entry in entries:
        assert entry.what.strip() and entry.impact.strip() and entry.next_step.strip()
        assert entry.product == "papyrus"


SEEDED = [
    "papyrus-monitoring-collector-errors",
    "papyrus-monitoring-search-provider-failing",
    "papyrus-monitoring-router-errors",
    "papyrus-monitoring-router-dead-letters",
    "papyrus-monitoring-decisions-stopped",
    "ScheduledJobStack-FailureAlarm1A2B3C4D",
    "ScheduledJobStack-MissedWeekAlarmFFEE0011",
    "papyrus-newsletter-shipped-bounce-rate",
    "papyrus-newsletter-pilobolus-complaint-rate",
]


@pytest.mark.parametrize("name", SEEDED)
def test_each_seeded_alarm_has_all_four_fields_in_its_message(name):
    world = World()
    world.alarm(name)
    world.send_all_queued()
    text = world.mailer.calls[0]["text"]
    for label in ("What broke", "Where", "Impact", "Next step", "Open the alarm"):
        assert re.search(r"^%s: \S" % label, text, re.M), (name, label)
    assert "No triage notes yet" not in text


def test_unknown_alarm_is_labelled_and_still_complete_enough():
    world = World()
    world.alarm("who-knows")
    world.send_all_queued()
    text = world.mailer.calls[0]["text"]
    assert "No triage notes yet" in text and "who-knows" in text
    assert "Impact:" not in text and "papyrus-production" in text


def test_console_link_encodes_everything_and_survives_odd_names():
    settings = TriageSettings("https://portal.example.awsapps.com/start/", "Triage Role", {})
    link = console_link(settings, "123456789012", "us-east-1", "a b&c#d/e")
    assert " " not in link and link.count("&") == 2
    fragment = urlparse(link).fragment
    query = parse_qs(fragment.split("?", 1)[1])
    assert query["role_name"] == ["Triage Role"]
    assert query["destination"][0] == alarm_page_url("us-east-1", "a b&c#d/e")
    assert alarm_page_url("us-east-1", "a b").endswith("alarm/" + quote("a b", safe=""))


def test_without_a_portal_the_link_goes_straight_to_the_alarm_page():
    link = console_link(TriageSettings(), "1", "eu-west-1", "x")
    assert link == alarm_page_url("eu-west-1", "x")


def test_datapoints_in_words():
    data = json.dumps({"recentDatapoints": [2.0, 3.5], "threshold": 1.0})
    assert datapoints_in_words(data) == "Recent datapoints: 2, 3.5. The threshold is 1."
    assert datapoints_in_words(None) == "" and datapoints_in_words("{x") == ""
    assert datapoints_in_words("[1]") == ""


def test_describe_duration():
    assert describe_duration(5) == "less than a minute"
    assert describe_duration(60) == "1 minute"
    assert describe_duration(3900) == "1 hour 5 minutes"
    assert describe_duration(2 * 86400 + 3 * 3600 + 120) == "2 days 3 hours"


# -- grouping ----------------------------------------------------------------


def test_burst_of_fifty_is_two_messages_and_nothing_is_dropped():
    world = World()
    for n in range(50):
        world.clock.advance(1)
        world.alarm("storm-alpha-%d" % n)
    assert len(world.queued_for("email")) == 1
    world.tick(121)
    assert len(world.queued_for("email")) == 2
    world.send_all_queued()
    assert world.mailer.calls[1]["text"].count("Open the alarm:") == 49
    cw = [r for r in world.rows() if r["eventId"].startswith("cw:")]
    assert len(cw) == 50 and sum(r["status"] == "grouped" for r in cw) == 49
    assert all(r["groupId"] for r in cw if r["status"] == "grouped")


def test_a_group_window_that_collected_nothing_just_closes():
    world = World()
    world.alarm("storm-alpha-1")
    assert world.tick(121) == 0
    world.alarm("storm-alpha-2")
    assert len(world.queued_for("email")) == 2  # a fresh window: sent at once


def test_flushing_twice_sends_the_group_once():
    world = World()
    world.alarm("storm-alpha-1")
    world.alarm("storm-alpha-2")
    world.clock.advance(121)
    assert world.router.tick() == 1
    assert world.router.tick() == 0
    assert len(world.queued_for("email")) == 2


def test_an_interrupted_flush_does_not_send_twice():
    world = World()
    world.alarm("storm-alpha-1")
    world.alarm("storm-alpha-2")
    world.clock.advance(121)
    row = world.store.query("GROUPS#open")[0]["sk"]
    group = world.store.get(row, "OPEN")
    recipient = world.directory.operators()[0]
    world.dispatcher._send_group(group, recipient)  # crashed before deleting the row
    assert world.router.tick() == 1
    assert len(world.queued_for("email")) == 2


def test_each_operator_has_their_own_group():
    world = World(operators=2)
    world.alarm("storm-alpha-1")
    world.alarm("storm-alpha-2")
    world.tick(121)
    assert len(world.queued_for("email")) == 4


def test_a_group_for_a_removed_operator_is_dropped_quietly():
    world = World()
    world.alarm("storm-alpha-1")
    world.alarm("storm-alpha-2")
    world.directory._recipients = []
    assert world.tick(121) == 0


# -- email -------------------------------------------------------------------


def test_backoff_is_one_two_four_then_the_error_surfaces_and_the_claim_is_released():
    world = World()
    world.alarm(ALARM)
    row = world.rows("queued")[0]
    world.mailer.script = [Throttled()] * 4
    with pytest.raises(Throttled):
        world.email.send(row)
    assert world.sleeps == [1, 2, 4]
    assert world.store.get(row["pk"], "CLAIM#" + row["deliveryId"]) is None
    assert world.log.get(row["eventId"], row["deliveryId"])["attempts"] == 1
    world.email.send(world.log.get(row["eventId"], row["deliveryId"]))
    assert world.log.get(row["eventId"], row["deliveryId"])["status"] == "sent"


def test_unknown_recipient_fails_the_delivery_without_calling_ses():
    world = World()
    world.alarm(ALARM)
    row = world.rows("queued")[0]
    world.directory._recipients = []
    assert world.email.send(row).status == "failed"
    assert world.mailer.calls == [] and world.log.get(row["eventId"], row["deliveryId"])["status"] == "failed"


def test_message_carries_the_delivery_and_kind_tags():
    world = World()
    world.alarm(ALARM)
    world.send_all_queued()
    row = world.rows()[0]
    assert world.mailer.calls[0]["tags"] == {"delivery": row["deliveryId"], "kind": "cloudwatch.alarm"}
    assert world.mailer.calls[0]["to"] == world.addresses[0]


def test_sender_handler_skips_final_rows_and_reports_failures():
    world = World()
    world.alarm(ALARM)
    body = world.queued_for("email")[0]
    record = {"messageId": "m", "body": json.dumps(body)}
    assert email_sender.handle_batch(world.store, world.email, {"Records": [record]}) == {"batchItemFailures": []}
    assert len(world.mailer.calls) == 1
    email_sender.handle_batch(world.store, world.email, {"Records": [record]})
    assert len(world.mailer.calls) == 1
    world.mailer.script = [RuntimeError("x")]
    world.alarm("storm-alpha-1")
    second = {"messageId": "m2", "body": json.dumps(world.queued_for("email")[1])}
    out = email_sender.handle_batch(world.store, world.email, {"Records": [second]})
    assert out == {"batchItemFailures": [{"itemIdentifier": "m2"}]}


def test_pacer_spaces_calls_to_the_send_rate():
    now = [0.0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    pacer = Pacer(lambda: 2.0, now=lambda: now[0], sleep=sleep)
    pacer.wait()
    pacer.wait()
    now[0] += 0.2
    pacer.wait()
    assert sleeps == pytest.approx([0.5, 0.3])


def test_rendered_mail_escapes_everything_and_never_trusts_a_link():
    item = {
        "state": "ALARM", "alarmName": "<script>x</script>", "product": "p<b>", "site": "",
        "what": "Bad & <i>worse</i>", "impact": "I", "nextStep": "N", "reason": "r\"q'",
        "datapoints": "", "link": "javascript:alert(1)", "hashasNotes": True, "hasNotes": True,
        "awsAccount": "acct", "region": "us-east-1",
    }
    mail = render_operator_email([item])
    assert "<script>" not in mail.html and "<i>" not in mail.html
    assert "&lt;script&gt;" in mail.html and "href=" not in mail.html
    assert 'name="color-scheme"' in mail.html and "prefers-color-scheme: dark" in mail.html
    assert "\n" not in mail.subject


def test_rendering_nothing_is_an_error():
    with pytest.raises(ValueError):
        render_operator_email([])


def test_no_log_line_in_a_full_run_holds_an_address(caplog):
    import logging

    caplog.set_level(logging.INFO, logger="caducus.notify")
    world = World(operators=2)
    world.alarm(ALARM)
    world.send_all_queued()
    world.alarm("storm-alpha-1")
    assert caplog.records
    from caducus.notify.logs import contains_address

    assert not any(contains_address(r.getMessage()) for r in caplog.records)


def test_html_shows_a_named_link_not_the_raw_url_and_text_keeps_the_full_url():
    world = World()
    world.alarm(ALARM)
    world.send_all_queued()
    mail = world.mailer.calls[0]
    link = re.search(r"^Open the alarm: (\S+)$", mail["text"], re.M).group(1)
    assert re.search(r'<a [^>]*href="[^"]+"[^>]*>Open the alarm in CloudWatch</a>', mail["html"])
    assert 'href="%s"' % link.replace("&", "&amp;") in mail["html"]
    assert ">https://" not in mail["html"]
