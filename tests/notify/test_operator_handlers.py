"""The heartbeat, the SES-event metric, reconcile and the triage settings a host configures."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from caducus.notify.fakes import ACCOUNT_ID, PROBE_ADDRESS, World
from caducus.notify.handlers import email_sender, reconcile, ses_events
from caducus.notify.handlers.router import process_envelope
from caducus.notify.kinds import load_registry
from caducus.notify.metrics import CloudWatchMetrics, MemoryMetrics
from caducus.notify.wiring import triage_settings


def test_heartbeat_kind_is_a_probe_kind():
    assert load_registry().get("caducus.heartbeat").audience == "probe"


def test_unknown_audience_is_refused():
    from caducus.notify.kinds import registry_from_mapping

    with pytest.raises(ValueError):
        registry_from_mapping(
            {"kinds": {"x": {"class": "operator", "channels": ["email"], "audience": "all"}}}
        )


def test_heartbeat_reaches_only_the_probe():
    world = World(operators=2)
    process_envelope(world.router, world.catalog, world.heartbeat_envelope("hb-1"))
    world.send_all_queued()
    assert [c["to"] for c in world.mailer.calls] == [PROBE_ADDRESS]


def test_heartbeat_with_no_probe_is_refused_not_sent():
    world = World()
    world.directory._probe = None
    result = process_envelope(world.router, world.catalog, world.heartbeat_envelope("hb-1"))
    assert result.outcome == "refused" and world.queued_for("email") == []


def test_same_heartbeat_event_twice_sends_once():
    world = World()
    for _ in range(2):
        process_envelope(world.router, world.catalog, world.heartbeat_envelope("hb-1"))
    assert len(world.queued_for("email")) == 1


@pytest.mark.parametrize(
    "event_type,expected",
    [("Delivery", ["HeartbeatDelivered"]), ("Bounce", ["HeartbeatSendFailed"]),
     ("Reject", ["HeartbeatSendFailed"]), ("Send", [])],
)
def test_ses_events_count_the_heartbeat(event_type, expected):
    metrics = MemoryMetrics()
    ses_events.handle_event(
        metrics, {"detail": {"eventType": event_type, "mail": {"tags": {"kind": ["caducus.heartbeat"]}}}}
    )
    assert metrics.counted == expected


def test_ses_events_ignore_other_mail_and_odd_shapes():
    metrics = MemoryMetrics()
    for envelope in (
        {"detail": {"eventType": "Delivery", "mail": {"tags": {"kind": ["cloudwatch.alarm"]}}}},
        {"detail": {"eventType": "Delivery", "mail": {}}},
        {},
    ):
        assert ses_events.handle_event(metrics, envelope) is None
    assert metrics.counted == []


def test_refused_heartbeat_send_is_counted_and_retried():
    world = World()
    metrics = MemoryMetrics()
    process_envelope(world.router, world.catalog, world.heartbeat_envelope("hb-1"))
    world.mailer.script = [RuntimeError("no")]
    record = {"messageId": "m", "body": json.dumps(world.queued_for("email")[0])}
    out = email_sender.handle_batch(world.store, world.email, {"Records": [record]}, metrics)
    assert metrics.counted == ["HeartbeatSendFailed"]
    assert out["batchItemFailures"] == [{"itemIdentifier": "m"}]


def test_failed_alarm_send_is_not_a_heartbeat_failure():
    world = World()
    metrics = MemoryMetrics()
    world.alarm("papyrus-monitoring-router-errors")
    world.mailer.script = [RuntimeError("no")]
    record = {"messageId": "m", "body": json.dumps(world.queued_for("email")[0])}
    email_sender.handle_batch(world.store, world.email, {"Records": [record]}, metrics)
    assert metrics.counted == []


def _row(name, hour=11):
    return {
        "AlarmName": name,
        "AlarmArn": "arn:aws:cloudwatch:us-east-1:%s:alarm:%s" % (ACCOUNT_ID, name),
        "StateUpdatedTimestamp": datetime(2026, 11, 4, hour, 0, 0, 123000, tzinfo=timezone.utc),
        "StateReason": "Threshold Crossed",
        "StateReasonData": "{}",
    }


def test_envelope_for_matches_a_live_state_change_id():
    from caducus.notify import cloudwatch

    envelope = reconcile.envelope_for(_row("a"))
    event = cloudwatch.event_from_alarm_change(envelope)
    assert event.event_id == "cw:%s:us-east-1:a:ALARM:2026-11-04T11:00:00.123+0000" % ACCOUNT_ID


def test_reconcile_counts_and_ignores_prefixes():
    world = World()
    rows = [_row("papyrus-monitoring-router-errors"), _row("papyrus-operator-deadman")]
    counts = reconcile.reconcile(world.router, world.catalog, rows, ["papyrus-operator-"])
    assert counts == {"seen": 2, "ignored": 1, "opened": 1}
    again = reconcile.reconcile(world.router, world.catalog, rows, ["papyrus-operator-"])
    assert again["opened"] == 0 and len(world.queued_for("email")) == 1


def test_firing_alarms_pages_both_kinds():
    class Paginator:
        def paginate(self, **kw):
            assert kw["StateValue"] == "ALARM"
            yield {"MetricAlarms": [{"AlarmName": "m"}], "CompositeAlarms": [{"AlarmName": "c"}]}
            yield {}

    class Client:
        def get_paginator(self, name):
            assert name == "describe_alarms"
            return Paginator()

    assert [a["AlarmName"] for a in reconcile.firing_alarms(Client())] == ["m", "c"]


def test_cloudwatch_metrics_put_one_count():
    sent = {}

    class Client:
        def put_metric_data(self, **kw):
            sent.update(kw)

    CloudWatchMetrics(Client(), "Ns").count("HeartbeatDelivered")
    assert sent["Namespace"] == "Ns" and sent["MetricData"][0]["MetricName"] == "HeartbeatDelivered"


class _Ssm:
    class exceptions:
        class ParameterNotFound(Exception):
            pass

    def __init__(self, value=None):
        self.value = value

    def get_parameter(self, **_):
        if self.value is None:
            raise self.exceptions.ParameterNotFound()
        return {"Parameter": {"Value": self.value}}


def test_missing_triage_parameter_falls_back_to_the_default_role():
    settings = triage_settings(_Ssm(), "/x/triage", "AdministratorAccess")
    assert settings.portal_url == "" and settings.role_name == "AdministratorAccess"


def test_triage_parameter_wins_over_the_default_role_and_maps_sites():
    value = json.dumps({"portalUrl": "https://p.example", "roleName": "ReadOnly", "sites": {"d1": "shipped"}})
    settings = triage_settings(_Ssm(value), "/x/triage", "AdministratorAccess")
    assert settings.role_name == "ReadOnly" and settings.sites == {"d1": "shipped"}


def test_site_names_replace_ids_in_the_email_item():
    from caducus.notify.triage import TriageSettings, build_alarm_item
    from caducus.notify import cloudwatch

    world = World()
    envelope = world.alarm_envelope("storm-d1-1")
    event = cloudwatch.event_from_alarm_change(envelope, world.catalog)
    item = build_alarm_item(event, world.catalog, TriageSettings(sites={"d1": "shipped"}))
    assert item["site"] == "shipped"
