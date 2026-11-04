#!/usr/bin/env python3
"""CDK app for the Caducus notification service. Deploys only through CI."""

import os

import aws_cdk as cdk

from assets import build_bundle
from notifications_stack import NotificationsStack

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))


def make_app(outdir: str = "") -> cdk.App:
    app = cdk.App(outdir=outdir or None)
    bundle = build_bundle(REPO, os.path.join(HERE, ".build", "lambda"))
    NotificationsStack(
        app,
        "CaducusNotifications",
        bundle_dir=bundle,
        env=cdk.Environment(
            account=os.environ.get("CDK_DEFAULT_ACCOUNT"), region="us-east-1"
        ),
    )
    return app


if __name__ == "__main__":
    make_app().synth()
