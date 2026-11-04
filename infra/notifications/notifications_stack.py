"""The Caducus notification service: one table, one bus, two queues, three Lambdas.

Nothing here names an AWS account, an address or a domain. The sender address and
the SES configuration set arrive as CDK context at deploy time.
"""

from __future__ import annotations

from aws_cdk import ArnFormat, Duration, RemovalPolicy, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as sources
from aws_cdk import aws_sqs as sqs
from constructs import Construct

from secure_string import SecureStringParameter

TABLE_NAME = "caducus-notifications"
BUS_NAME = "caducus-notifications"
LINK_KEY = "/caducus/link-key"
RECIPIENT_KEY = "/caducus/recipient-key"
# Supplied by Ryan's script (kanbus-91d450), not generated here.
OPERATORS = "/caducus/operators"
TRIAGE = "/caducus/triage"
RETENTION_DAYS = 90
LAMBDA_TIMEOUT = Duration.seconds(60)
MAX_RECEIVES = 5
PLACEHOLDER_FROM = "alerts@notify.example.com"


class NotificationsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, bundle_dir: str, **kwargs):  # type: ignore[no-untyped-def]
        super().__init__(scope, construct_id, **kwargs)
        from_address = self.node.try_get_context("fromAddress") or PLACEHOLDER_FROM
        config_set = self.node.try_get_context("configurationSet") or ""

        key = kms.Key(
            self,
            "Key",
            alias="alias/caducus-notifications",
            enable_key_rotation=True,
            description="Encrypts the Caducus notifications table",
            removal_policy=RemovalPolicy.RETAIN,
        )
        self.table = dynamodb.Table(
            self,
            "Table",
            table_name=TABLE_NAME,
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=key,
            time_to_live_attribute="expiresAt",
            # Off on purpose: erasure has to be real, so there is no backup to restore from.
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=False
            ),
            removal_policy=RemovalPolicy.RETAIN,
        )
        self.table.add_global_secondary_index(
            index_name="byRecipient",
            partition_key=dynamodb.Attribute(name="recipientKey", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="queuedAt", type=dynamodb.AttributeType.STRING),
        )

        self.bus = events.EventBus(self, "Bus", event_bus_name=BUS_NAME)

        def queue_with_dlq(name: str):  # type: ignore[no-untyped-def]
            dlq = sqs.Queue(
                self,
                name + "Dlq",
                queue_name="caducus-%s-dlq" % name,
                retention_period=Duration.days(14),
                encryption=sqs.QueueEncryption.SQS_MANAGED,
            )
            queue = sqs.Queue(
                self,
                name + "Queue",
                queue_name="caducus-" + name,
                visibility_timeout=Duration.seconds(6 * LAMBDA_TIMEOUT.to_seconds()),
                encryption=sqs.QueueEncryption.SQS_MANAGED,
                dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=MAX_RECEIVES, queue=dlq),
            )
            return queue, dlq

        self.ingest, self.ingest_dlq = queue_with_dlq("ingest")
        self.email_queue, self.email_dlq = queue_with_dlq("email")

        for rule_id, pattern in (
            (
                "AppEvents",
                events.EventPattern(
                    source=events.Match.prefix("anthus."),
                    detail_type=["Notification Requested"],
                ),
            ),
            (
                "AlarmEvents",
                events.EventPattern(
                    source=["aws.cloudwatch"],
                    detail_type=["CloudWatch Alarm State Change"],
                ),
            ),
        ):
            events.Rule(
                self,
                rule_id,
                event_bus=self.bus,
                event_pattern=pattern,
                targets=[targets.SqsQueue(self.ingest)],
            )

        link_key = SecureStringParameter(
            self, "LinkKey", parameter_name=LINK_KEY,
            description="Signs acknowledgement and manage links (Caducus notifications)",
        )
        recipient_key = SecureStringParameter(
            self, "RecipientKey", parameter_name=RECIPIENT_KEY,
            description="Pseudonymises recipient addresses in the delivery log (Caducus notifications)",
        )
        read_parameters = [
            self.format_arn(
                service="ssm", resource="parameter", resource_name=name[1:],
                arn_format=ArnFormat.SLASH_RESOURCE_NAME,
            )
            for name in (OPERATORS, TRIAGE, RECIPIENT_KEY)
        ]

        code = lambda_.Code.from_asset(bundle_dir)
        environment = {
            "TABLE_NAME": TABLE_NAME,
            "RETENTION_DAYS": str(RETENTION_DAYS),
            "CADUCUS_CONFIG_DIR": "/var/task/config",
            "OPERATORS_PARAMETER": OPERATORS,
            "RECIPIENT_KEY_PARAMETER": RECIPIENT_KEY,
            "TRIAGE_PARAMETER": TRIAGE,
            "EMAIL_QUEUE_URL": self.email_queue.queue_url,
            "FROM_ADDRESS": from_address,
            "CONFIGURATION_SET": config_set,
        }

        def function(name: str, handler: str, description: str):  # type: ignore[no-untyped-def]
            fn = lambda_.Function(
                self,
                name,
                function_name="caducus-notifications-" + name.lower(),
                runtime=lambda_.Runtime.PYTHON_3_12,
                architecture=lambda_.Architecture.ARM_64,
                handler=handler,
                code=code,
                timeout=LAMBDA_TIMEOUT,
                memory_size=256,
                environment=environment,
                description=description,
            )
            self.table.grant_read_write_data(fn)
            fn.add_to_role_policy(
                iam.PolicyStatement(actions=["ssm:GetParameter"], resources=read_parameters)
            )
            return fn

        self.router_fn = function("Router", "caducus.notify.handlers.router.handler", "Routes one event")
        self.email_fn = function("EmailSender", "caducus.notify.handlers.email_sender.handler", "Sends one email")
        self.flush_fn = function("Flush", "caducus.notify.handlers.flush.handler", "Each minute: clearings and groups")
        self.email_queue.grant_send_messages(self.router_fn)
        self.email_queue.grant_send_messages(self.flush_fn)
        self.email_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ses:SendEmail"],
                resources=[
                    self.format_arn(service="ses", resource="identity", resource_name="*"),
                    self.format_arn(service="ses", resource="configuration-set", resource_name="*"),
                ],
            )
        )
        self.email_fn.add_to_role_policy(
            iam.PolicyStatement(actions=["ses:GetAccount"], resources=["*"])
        )

        # SQS maximum concurrency, not reserved concurrency: accounts here can have a
        # Lambda concurrency limit as low as 10.
        self.router_fn.add_event_source(
            sources.SqsEventSource(
                self.ingest, batch_size=10, max_concurrency=2, report_batch_item_failures=True
            )
        )
        self.email_fn.add_event_source(
            sources.SqsEventSource(
                self.email_queue, batch_size=5, max_concurrency=2, report_batch_item_failures=True
            )
        )
        events.Rule(
            self,
            "EveryMinute",
            schedule=events.Schedule.rate(Duration.minutes(1)),
            targets=[targets.LambdaFunction(self.flush_fn)],
        )
        self.link_key = link_key
        self.recipient_key = recipient_key
