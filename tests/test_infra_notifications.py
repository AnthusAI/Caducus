"""The CDK app, checked by synthesising it and asserting on the template. Nothing is deployed."""

from __future__ import annotations

import json
import os
import sys

import pytest

pytest.importorskip("aws_cdk")
import yaml  # noqa: E402
from aws_cdk import assertions  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
INFRA = os.path.join(ROOT, "infra", "notifications")
sys.path.insert(0, INFRA)

import aws_cdk as cdk  # noqa: E402
from assets import build_bundle, files_in  # noqa: E402
from notifications_stack import NotificationsStack  # noqa: E402


@pytest.fixture(scope="module")
def bundle(tmp_path_factory):
    return build_bundle(ROOT, str(tmp_path_factory.mktemp("bundle") / "lambda"))


@pytest.fixture(scope="module")
def template(bundle):
    app = cdk.App()
    stack = NotificationsStack(app, "T", bundle_dir=bundle)
    return assertions.Template.from_stack(stack)


def test_table_keys_ttl_encryption_and_no_point_in_time_recovery(template):
    template.has_resource_properties(
        "AWS::DynamoDB::Table",
        {
            "TableName": "caducus-notifications",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "TimeToLiveSpecification": {"AttributeName": "expiresAt", "Enabled": True},
            "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": False},
            "SSESpecification": assertions.Match.object_like(
                {"SSEEnabled": True, "SSEType": "KMS"}
            ),
            "GlobalSecondaryIndexes": [
                assertions.Match.object_like(
                    {
                        "IndexName": "byRecipient",
                        "KeySchema": [
                            {"AttributeName": "recipientKey", "KeyType": "HASH"},
                            {"AttributeName": "queuedAt", "KeyType": "RANGE"},
                        ],
                    }
                )
            ],
        },
    )
    template.has_resource("AWS::DynamoDB::Table", {"DeletionPolicy": "Retain"})
    template.has_resource_properties("AWS::KMS::Key", {"EnableKeyRotation": True})


def test_event_bus_and_the_two_rules_feeding_the_ingest_queue(template):
    template.has_resource_properties("AWS::Events::EventBus", {"Name": "caducus-notifications"})
    template.has_resource_properties(
        "AWS::Events::Rule",
        {
            "EventPattern": {
                "source": [{"prefix": "anthus."}],
                "detail-type": ["Notification Requested"],
            }
        },
    )
    template.has_resource_properties(
        "AWS::Events::Rule",
        {
            "EventPattern": {
                "source": ["aws.cloudwatch"],
                "detail-type": ["CloudWatch Alarm State Change"],
            }
        },
    )
    template.has_resource_properties(
        "AWS::Events::Rule", {"ScheduleExpression": "rate(1 minute)"}
    )


def test_each_queue_has_a_dead_letter_queue(template):
    for name in ("ingest", "email"):
        template.has_resource_properties("AWS::SQS::Queue", {"QueueName": "caducus-" + name})
        template.has_resource_properties("AWS::SQS::Queue", {"QueueName": "caducus-%s-dlq" % name})
    queues = template.find_resources(
        "AWS::SQS::Queue", {"Properties": {"RedrivePolicy": assertions.Match.any_value()}}
    )
    assert len(queues) == 2


def test_lambdas_are_python_312_arm64_and_use_max_concurrency_not_reserved(template):
    functions = template.find_resources(
        "AWS::Lambda::Function",
        {"Properties": {"FunctionName": assertions.Match.string_like_regexp("^caducus-notifications-")}},
    )
    assert len(functions) == 3
    for fn in functions.values():
        props = fn["Properties"]
        assert props["Runtime"] == "python3.12" and props["Architectures"] == ["arm64"]
        assert "ReservedConcurrentExecutions" not in props
    mappings = template.find_resources("AWS::Lambda::EventSourceMapping")
    assert len(mappings) == 2
    for mapping in mappings.values():
        props = mapping["Properties"]
        assert props["ScalingConfig"]["MaximumConcurrency"] == 2
        assert props["FunctionResponseTypes"] == ["ReportBatchItemFailures"]


def test_handlers_point_at_the_notify_package(template):
    handlers = {
        fn["Properties"]["Handler"]
        for fn in template.find_resources("AWS::Lambda::Function").values()
        if fn["Properties"].get("FunctionName", "").startswith("caducus-notifications-")
    }
    assert handlers == {
        "caducus.notify.handlers.router.handler",
        "caducus.notify.handlers.email_sender.handler",
        "caducus.notify.handlers.flush.handler",
    }


def test_generated_secrets_are_create_only_and_retained(template):
    for name in ("/caducus/link-key", "/caducus/recipient-key"):
        template.has_resource_properties(
            "Custom::SecureStringParameter", {"ParameterName": name, "Length": "64"}
        )
    template.has_resource("Custom::SecureStringParameter", {"DeletionPolicy": "Retain"})
    with open(os.path.join(INFRA, "secure_string.py")) as handle:
        assert "Overwrite=False" in handle.read()


def test_ses_permission_is_send_only_and_never_a_wildcard_action(template):
    policies = json.dumps(template.find_resources("AWS::IAM::Policy"))
    assert "ses:SendEmail" in policies and "ses:GetAccount" in policies
    assert '"ses:*"' not in policies and '"*"' not in json.dumps(
        [
            s
            for p in template.find_resources("AWS::IAM::Policy").values()
            for s in p["Properties"]["PolicyDocument"]["Statement"]
            if "ses:SendEmail" in json.dumps(s.get("Action"))
        ][0]["Action"]
    )


def test_no_account_id_or_address_is_baked_into_the_template(template):
    text = json.dumps(template.to_json())
    import re

    assert not re.search(r"\b\d{12}\b", text.replace("${AWS::AccountId}", ""))
    assert not re.search(r"[\w.+-]+@(?!notify\.example\.com)[\w-]+\.\w+", text)


def test_sender_address_comes_from_context(bundle):
    app = cdk.App(context={"fromAddress": "alerts@notify.example.org"})
    stack = NotificationsStack(app, "C", bundle_dir=bundle)
    template = assertions.Template.from_stack(stack)
    template.has_resource_properties(
        "AWS::Lambda::Function",
        {"Environment": {"Variables": assertions.Match.object_like({"FROM_ADDRESS": "alerts@notify.example.org"})}},
    )


def test_bundle_has_the_package_and_json_config_but_no_fakes_or_yaml_dependency(bundle):
    files = files_in(bundle)
    assert "caducus/notify/handlers/router.py" in files
    assert "caducus/notify/fakes.py" not in files
    assert "config/notification-kinds.json" in files and "config/alarm-catalog.json" in files
    with open(os.path.join(bundle, "config", "alarm-catalog.json")) as handle:
        built = json.load(handle)
    with open(os.path.join(ROOT, "config", "alarm-catalog.yaml")) as handle:
        assert built == yaml.safe_load(handle)


def test_the_bundle_runs_without_pyyaml(bundle):
    """Import and load the config the way a Lambda would, with PyYAML made unavailable."""
    import subprocess

    code = (
        "import sys; sys.modules['yaml'] = None; sys.path.insert(0, %r);"
        "from caducus.notify.kinds import load_registry;"
        "from caducus.notify.catalog import load_catalog;"
        "from caducus.notify.files import config_path;"
        "import caducus.notify.handlers.router, caducus.notify.handlers.email_sender, caducus.notify.handlers.flush;"
        "d=%r;"
        "assert load_registry(config_path('notification-kinds.yaml', d)).get('cloudwatch.alarm');"
        "assert load_catalog(config_path('alarm-catalog.yaml', d)).match('papyrus-monitoring-router-errors')"
        % (bundle, os.path.join(bundle, "config"))
    )
    done = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True)
    assert done.returncode == 0, done.stderr


def test_the_oidc_role_trusts_only_main_of_this_repository():
    with open(os.path.join(INFRA, "bootstrap-oidc.yaml")) as handle:
        doc = yaml.safe_load(handle)
    role = doc["Resources"]["DeployRole"]["Properties"]
    trust = role["AssumeRolePolicyDocument"]["Statement"][0]
    assert trust["Action"] == "sts:AssumeRoleWithWebIdentity"
    assert trust["Condition"]["StringEquals"]["token.actions.githubusercontent.com:sub"] == {
        "Fn::Sub": "repo:${GitHubRepository}:ref:refs/heads/main"
    }
    assert doc["Parameters"]["GitHubRepository"]["Default"] == "AnthusAI/Caducus"
    grants = role["Policies"][0]["PolicyDocument"]["Statement"]
    assert [g["Action"] for g in grants] == ["sts:AssumeRole"]
    assert "cdk-" in grants[0]["Resource"]["Fn::Sub"]


def test_deploy_workflow_guards_the_account_before_any_deploy():
    with open(os.path.join(ROOT, ".github", "workflows", "deploy-notifications.yml")) as handle:
        workflow = yaml.safe_load(handle)
    triggers = workflow.get("on", workflow.get(True))
    assert triggers["push"]["branches"] == ["main"]
    deploy = workflow["jobs"]["deploy"]
    assert deploy["needs"] == "test" and "CADUCUS_AWS_ACCOUNT" in deploy["if"]
    assert deploy["permissions"]["id-token"] == "write"
    names = [step.get("name", step.get("uses", "")) for step in deploy["steps"]]
    assert names.index("Stop unless this is the Caducus account") < names.index("Deploy")
    guard = next(s for s in deploy["steps"] if s.get("name", "").startswith("Stop unless"))
    assert "exit 1" in guard["run"] and "get-caller-identity" in guard["run"]
    assert not any("secrets." in json.dumps(step) for step in deploy["steps"])


def test_ci_also_runs_on_pushes_to_develop():
    with open(os.path.join(ROOT, ".github", "workflows", "ci.yml")) as handle:
        workflow = yaml.safe_load(handle)
    triggers = workflow.get("on", workflow.get(True))
    assert triggers["push"]["branches"] == ["develop", "main"]
