"""A generated secret in SSM Parameter Store as a standard SecureString.

Ported from Papyrus `amplify/plugins/newsletter/secure-string-parameter.ts`.
CloudFormation cannot create a SecureString, so a custom resource Lambda puts it:
it generates the value, never returns or logs it, never overwrites an existing
parameter, and delete retains it. Readers get `ssm:GetParameter` on the parameter
ARN only; the AWS-managed `aws/ssm` key needs no KMS grant.
"""

from __future__ import annotations

from aws_cdk import ArnFormat, CustomResource, Duration, RemovalPolicy, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import custom_resources as cr
from constructs import Construct

HANDLER_SOURCE = '''
import secrets
import string

import boto3

ALPHABET = string.ascii_letters + string.digits


def handler(event, context):
    props = event["ResourceProperties"]
    name = props["ParameterName"]
    if event["RequestType"] == "Delete":
        return {"PhysicalResourceId": event["PhysicalResourceId"]}
    ssm = boto3.client("ssm")
    value = "".join(secrets.choice(ALPHABET) for _ in range(int(props["Length"])))
    try:
        ssm.put_parameter(
            Name=name, Description=props["Description"], Type="SecureString",
            Tier="Standard", Overwrite=False, Value=value,
        )
    except ssm.exceptions.ParameterAlreadyExists:
        pass
    except Exception as error:
        raise RuntimeError("PutParameter failed for %s (%s)" % (name, type(error).__name__))
    return {"PhysicalResourceId": name}
'''


class _Provider(Construct):
    """One Lambda and role per stack, shared by every SecureStringParameter in it."""

    ID = "SecureStringParameterProvider"

    def __init__(self, stack: Stack):
        super().__init__(stack, self.ID)
        self.role = iam.Role(
            self,
            "Role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Creates generated SecureString parameters, never overwriting",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        on_event = lambda_.Function(
            self,
            "OnEvent",
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            handler="index.handler",
            code=lambda_.Code.from_inline(HANDLER_SOURCE),
            role=self.role,
            timeout=Duration.seconds(30),
            description="Creates a generated SecureString once; never returns or logs the value",
        )
        self.provider = cr.Provider(self, "Provider", on_event_handler=on_event)

    @staticmethod
    def of(stack: Stack) -> "_Provider":
        found = stack.node.try_find_child(_Provider.ID)
        return found if found is not None else _Provider(stack)  # type: ignore[return-value]


class SecureStringParameter(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        parameter_name: str,
        description: str,
        length: int = 64,
    ):
        super().__init__(scope, construct_id)
        if not parameter_name.startswith("/"):
            raise ValueError("SSM parameter name must be absolute: %s" % parameter_name)
        if length < 32:
            raise ValueError("A generated secret must be at least 32 characters.")
        stack = Stack.of(self)
        self.parameter_name = parameter_name
        self.parameter_arn = stack.format_arn(
            service="ssm",
            resource="parameter",
            resource_name=parameter_name[1:],
            arn_format=ArnFormat.SLASH_RESOURCE_NAME,
        )
        shared = _Provider.of(stack)
        shared.role.add_to_policy(
            iam.PolicyStatement(
                sid="Put" + "".join(c for c in construct_id if c.isalnum()),
                actions=["ssm:PutParameter"],
                resources=[self.parameter_arn],
            )
        )
        resource = CustomResource(
            self,
            "Resource",
            service_token=shared.provider.service_token,
            resource_type="Custom::SecureStringParameter",
            properties={
                "ParameterName": parameter_name,
                "Description": description,
                "Length": str(length),
            },
            removal_policy=RemovalPolicy.RETAIN,
        )
        resource.node.add_dependency(shared.role)
