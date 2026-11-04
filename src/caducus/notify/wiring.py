"""Build the real dependencies from the Lambda environment. Needs boto3 and AWS."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from .catalog import Catalog, load_host_catalog
from .channels.email import EmailChannel, Pacer, SesMailer
from .delivery_log import DeliveryLog
from .directory import SsmDirectory
from .grouping import Dispatcher
from .incidents import IncidentTracker
from .kinds import load_registry
from .queues import SqsQueues
from .routing import Router
from .store import DynamoStore, Store
from .triage import TriageSettings


@dataclass
class Runtime:
    store: Store
    router: Router
    catalog: Catalog
    email: EmailChannel


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def triage_settings(ssm: Any, parameter: Optional[str]) -> TriageSettings:
    """The portal URL, role and account names are one JSON SSM parameter, never in Git."""
    if not parameter:
        return TriageSettings()
    raw = ssm.get_parameter(Name=parameter, WithDecryption=True)["Parameter"]["Value"]
    data = json.loads(raw)
    return TriageSettings(
        portal_url=data.get("portalUrl", ""),
        role_name=data.get("roleName", ""),
        accounts=data.get("accounts", {}),
    )


def build_runtime(env: Optional[Mapping[str, str]] = None) -> Runtime:
    import boto3

    env = env if env is not None else os.environ
    store = DynamoStore(boto3.resource("dynamodb").Table(env["TABLE_NAME"]))
    ssm = boto3.client("ssm")
    config_dir = env.get("CADUCUS_CONFIG_DIR", "")
    kinds = load_registry(config_dir)
    catalog = load_host_catalog(config_dir)
    directory = SsmDirectory(ssm, env["OPERATORS_PARAMETER"], env["RECIPIENT_KEY_PARAMETER"])
    log = DeliveryLog(store, utcnow, int(env.get("RETENTION_DAYS", "90")))
    queues = SqsQueues(boto3.client("sqs"), {"email": env["EMAIL_QUEUE_URL"]})
    dispatcher = Dispatcher(store, log, queues, kinds, utcnow)
    router = Router(
        store=store,
        kinds=kinds,
        log=log,
        directory=directory,
        dispatcher=dispatcher,
        incidents=IncidentTracker(store, utcnow),
        catalog=catalog,
        triage=triage_settings(ssm, env.get("TRIAGE_PARAMETER")),
        clock=utcnow,
    )
    mailer = SesMailer(
        boto3.client("sesv2"), env["FROM_ADDRESS"], env.get("CONFIGURATION_SET", "")
    )
    email = EmailChannel(store, log, directory, mailer, pacer=Pacer(mailer.max_send_rate))
    return Runtime(store=store, router=router, catalog=catalog, email=email)


_RUNTIME: Optional[Runtime] = None


def runtime() -> Runtime:
    global _RUNTIME
    if _RUNTIME is None:
        _RUNTIME = build_runtime()
    return _RUNTIME
