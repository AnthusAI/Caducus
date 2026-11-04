"""The AWS-facing adapters, exercised against moto's in-memory AWS. No real AWS is touched."""

from __future__ import annotations

import json
import os

import pytest

moto = pytest.importorskip("moto")
import boto3  # noqa: E402

from caducus.notify.channels.email import SesMailer, Throttled  # noqa: E402
from caducus.notify.directory import SsmDirectory  # noqa: E402
from caducus.notify.files import find_config, load_mapping  # noqa: E402
from caducus.notify.queues import SqsQueues  # noqa: E402
from caducus.notify.wiring import triage_settings  # noqa: E402


@pytest.fixture(autouse=True)
def fake_credentials(monkeypatch):
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.setenv(name, "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_PROFILE", raising=False)


def test_ses_mailer_sends_html_text_and_tags():
    with moto.mock_aws():
        ses = boto3.client("sesv2", region_name="us-east-1")
        ses.create_email_identity(EmailIdentity="alerts@example.com")
        mailer = SesMailer(ses, "alerts@example.com")
        message_id = mailer.send(
            to_address="ops@example.com", subject="S", text="T", html="<p>H</p>",
            tags={"delivery": "d1", "kind": "cloudwatch.alarm"},
        )
        assert message_id


def test_max_send_rate_reads_the_account_quota():
    class Client:
        def get_account(self):
            return {"SendQuota": {"MaxSendRate": 14.0}}

    assert SesMailer(Client(), "a@example.com").max_send_rate() == 14.0


def test_ses_throttling_becomes_throttled():
    from botocore.exceptions import ClientError

    class Client:
        def send_email(self, **_):
            raise ClientError({"Error": {"Code": "Throttling", "Message": "slow"}}, "SendEmail")

    with pytest.raises(Throttled):
        SesMailer(Client(), "a@example.com").send(
            to_address="b@example.com", subject="s", text="t", html="h", tags={}
        )


def test_other_ses_errors_pass_through():
    from botocore.exceptions import ClientError

    class Client:
        def send_email(self, **_):
            raise ClientError({"Error": {"Code": "MessageRejected", "Message": "no"}}, "SendEmail")

    with pytest.raises(ClientError):
        SesMailer(Client(), "a@example.com").send(
            to_address="b@example.com", subject="s", text="t", html="h", tags={}
        )


def test_ses_mailer_passes_the_configuration_set():
    sent = {}

    class Client:
        def send_email(self, **request):
            sent.update(request)
            return {"MessageId": "m"}

    SesMailer(Client(), "a@example.com", "no-tracking").send(
        to_address="b@example.com", subject="s", text="t", html="h", tags={"k": "v"}
    )
    assert sent["ConfigurationSetName"] == "no-tracking"
    assert sent["EmailTags"] == [{"Name": "k", "Value": "v"}]


def test_ssm_directory_reads_operators_and_key():
    with moto.mock_aws():
        ssm = boto3.client("ssm", region_name="us-east-1")
        ssm.put_parameter(Name="/x/operators", Type="StringList", Value="a@example.com, b@example.com")
        ssm.put_parameter(Name="/x/key", Type="SecureString", Value="secret")
        directory = SsmDirectory(ssm, "/x/operators", "/x/key")
        people = directory.operators()
        assert [p.address for p in people] == ["a@example.com", "b@example.com"]
        assert directory.get(people[1].key).address == "b@example.com"
        assert directory.get("nope") is None
        assert people[0].mask == "a***@example.com"


def test_triage_settings_come_from_one_json_parameter():
    with moto.mock_aws():
        ssm = boto3.client("ssm", region_name="us-east-1")
        ssm.put_parameter(
            Name="/x/triage", Type="SecureString",
            Value=json.dumps({"portalUrl": "https://p.example", "roleName": "R", "accounts": {"1": "one"}}),
        )
        settings = triage_settings(ssm, "/x/triage")
        assert (settings.portal_url, settings.role_name, settings.accounts) == ("https://p.example", "R", {"1": "one"})
        assert triage_settings(ssm, None).portal_url == ""


def test_sqs_queues_send_json():
    with moto.mock_aws():
        sqs = boto3.client("sqs", region_name="us-east-1")
        url = sqs.create_queue(QueueName="caducus-email")["QueueUrl"]
        SqsQueues(sqs, {"email": url}).send("email", {"eventId": "e", "deliveryId": "d"})
        body = sqs.receive_message(QueueUrl=url)["Messages"][0]["Body"]
        assert json.loads(body) == {"eventId": "e", "deliveryId": "d"}


def test_config_files_load_from_json_or_yaml(tmp_path):
    (tmp_path / "a.yaml").write_text("x: 1\n")
    assert load_mapping(find_config("a.yaml", str(tmp_path))) == {"x": 1}
    (tmp_path / "a.json").write_text('{"x": 2}')
    assert load_mapping(find_config("a.yaml", str(tmp_path))) == {"x": 2}
    (tmp_path / "list.yaml").write_text("- 1\n")
    with pytest.raises(ValueError):
        load_mapping(str(tmp_path / "list.yaml"))
    assert find_config("missing.yaml", str(tmp_path)) is None
    assert find_config("a.yaml") is None
    os.environ["CADUCUS_CONFIG_DIR"] = str(tmp_path)
    try:
        assert find_config("a.yaml") == str(tmp_path / "a.json")
    finally:
        del os.environ["CADUCUS_CONFIG_DIR"]


def test_hosts_add_kinds_and_a_catalog_through_the_config_dir(tmp_path):
    from caducus.notify.catalog import load_host_catalog
    from caducus.notify.kinds import load_registry

    (tmp_path / "notification-kinds.json").write_text(json.dumps({"kinds": {
        "site.note": {"class": "operator", "channels": ["email"]},
        "cloudwatch.alarm": {"class": "operator", "channels": ["email"], "lifecycle": "incident"},
    }}))
    (tmp_path / "alarm-catalog.json").write_text(json.dumps({"defaults": {"holdDownSeconds": 7}, "alarms": []}))
    registry = load_registry(str(tmp_path))
    assert registry.get("site.note") is not None
    assert registry.get("cloudwatch.alarm").grouping is None  # the host's definition won
    assert load_host_catalog(str(tmp_path)).hold_down_seconds("x") == 7
    assert load_host_catalog(str(tmp_path / "nowhere")).match("x") is None  # no catalog: still works
    assert load_registry(str(tmp_path / "nowhere")).get("cloudwatch.alarm").grouping is not None


def test_a_host_can_pass_its_own_renderer():
    from caducus.notify.channels.email import Renderer
    from caducus.notify.fakes import World
    from caducus.notify.render.operator import RenderedEmail

    class Branded(Renderer):
        def render(self, items):
            return RenderedEmail("Branded", "plain", "<p>html</p>")

    world = World()
    world.email._renderer = Branded()
    world.alarm("papyrus-monitoring-router-errors")
    world.send_all_queued()
    assert world.mailer.calls[0]["subject"] == "Branded"


def test_the_runtime_imports_and_loads_its_config_without_pyyaml():
    """The Lambda case: only the package, its JSON data and boto3, with PyYAML unavailable."""
    import subprocess
    import sys

    code = (
        "import sys; sys.modules['yaml'] = None;"
        "import caducus.notify.handlers.router, caducus.notify.handlers.email_sender, "
        "caducus.notify.handlers.flush, caducus.notify.handlers.ses_events, "
        "caducus.notify.handlers.reconcile, caducus.notify.wiring;"
        "from caducus.notify.kinds import load_registry;"
        "from caducus.notify.catalog import load_host_catalog;"
        "assert load_registry().get('cloudwatch.alarm');"
        "assert load_host_catalog().match('anything') is None"
    )
    done = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True)
    assert done.returncode == 0, done.stderr


def _provision():
    """Everything the stack will create, in moto, so the real wiring can run."""
    ddb = boto3.client("dynamodb", region_name="us-east-1")
    ddb.create_table(
        TableName="caducus-notifications", BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "recipientKey", "AttributeType": "S"},
            {"AttributeName": "queuedAt", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[{
            "IndexName": "byRecipient",
            "KeySchema": [
                {"AttributeName": "recipientKey", "KeyType": "HASH"},
                {"AttributeName": "queuedAt", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
    )
    ssm = boto3.client("ssm", region_name="us-east-1")
    ssm.put_parameter(Name="/t/operators", Type="StringList", Value="ops@example.com")
    ssm.put_parameter(Name="/t/key", Type="SecureString", Value="secret")
    ssm.put_parameter(
        Name="/t/triage", Type="SecureString",
        Value=json.dumps({"portalUrl": "https://portal.example.awsapps.com/start",
                          "roleName": "Triage", "accounts": {"123456789012": "papyrus-production"}}),
    )
    boto3.client("sesv2", region_name="us-east-1").create_email_identity(EmailIdentity="alerts@example.com")
    queue = boto3.client("sqs", region_name="us-east-1").create_queue(QueueName="caducus-email")["QueueUrl"]
    return queue


def test_the_real_wiring_runs_an_alarm_end_to_end_in_moto(monkeypatch):
    from caducus.notify import wiring
    from caducus.notify.handlers import email_sender, flush, router as router_handler

    fixtures = os.path.join(os.path.dirname(__file__), "fixtures")
    with moto.mock_aws():
        queue = _provision()
        runtime = wiring.build_runtime({
            "TABLE_NAME": "caducus-notifications", "OPERATORS_PARAMETER": "/t/operators",
            "RECIPIENT_KEY_PARAMETER": "/t/key", "TRIAGE_PARAMETER": "/t/triage",
            "EMAIL_QUEUE_URL": queue, "FROM_ADDRESS": "alerts@example.com",
            "CADUCUS_CONFIG_DIR": fixtures,
        })
        runtime.email._pacer = None  # moto has no get_account
        monkeypatch.setattr(wiring, "_RUNTIME", runtime)
        envelope = {
            "source": "aws.cloudwatch", "detail-type": "CloudWatch Alarm State Change",
            "account": "123456789012", "region": "us-east-1", "time": "2026-11-04T12:00:00Z",
            "detail": {"alarmName": "papyrus-monitoring-router-errors",
                       "state": {"value": "ALARM", "reason": "r", "timestamp": "t1"},
                       "previousState": {"value": "OK"}},
        }
        record = {"messageId": "m1", "body": json.dumps(envelope)}
        assert router_handler.handler({"Records": [record]}) == {"batchItemFailures": []}
        router_handler.handler({"Records": [record]})  # redelivered
        sqs = boto3.client("sqs", region_name="us-east-1")
        messages = sqs.receive_message(QueueUrl=queue, MaxNumberOfMessages=10)["Messages"]
        assert len(messages) == 1
        records = [{"messageId": "s1", "body": messages[0]["Body"]}]
        assert email_sender.handler({"Records": records}) == {"batchItemFailures": []}
        rows = runtime.store.query("EVENT#" + json.loads(messages[0]["Body"])["eventId"], "DELIVERY#")
        assert [r["status"] for r in rows] == ["sent"] and rows[0]["providerMessageId"]
        assert flush.handler({}) == {"sent": 0}
