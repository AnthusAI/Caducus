"""Fakes for tests and specs: nothing here touches AWS."""

from __future__ import annotations

import itertools
import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional

from .catalog import catalog_from_mapping
from .files import load_mapping
from .channels.email import EmailChannel, Mailer, Throttled
from .delivery_log import DeliveryLog
from .directory import StaticDirectory
from .grouping import Dispatcher
from .handlers.router import process_envelope
from .incidents import IncidentTracker
from .kinds import load_registry
from .queues import MemoryQueues
from .routing import MemoryPreferences, Router
from .store import MemoryStore
from .triage import TriageSettings


class FakeClock:
    def __init__(self, start: str = "2026-11-04T12:00:00+00:00"):
        self.now = datetime.fromisoformat(start)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now = self.now + timedelta(seconds=seconds)


class FakeMailer(Mailer):
    """Records calls. `script` is a list of exceptions or None, consumed per call."""

    def __init__(self, rate: float = 14.0):
        self.calls: List[Mapping[str, Any]] = []
        self.script: List[Any] = []
        self.rate = rate
        self.attempts = 0

    def send(self, *, to_address, subject, text, html, tags):  # type: ignore[no-untyped-def]
        self.attempts += 1
        if self.script:
            problem = self.script.pop(0)
            if problem is not None:
                raise problem
        self.calls.append(
            {"to": to_address, "subject": subject, "text": text, "html": html, "tags": dict(tags)}
        )
        return "fake-message-%d" % len(self.calls)

    def max_send_rate(self) -> float:
        return self.rate


FIXTURES = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "tests", "notify", "fixtures"
)
OPERATOR_ADDRESS = "ops-{}@example.com"
PORTAL = "https://portal.example.awsapps.com/start"
ACCOUNT_ID = "123456789012"
PROBE_ADDRESS = "success@simulator.amazonses.com"


class World:
    """The whole service wired to in-memory fakes, for specs and unit tests."""

    def __init__(self, operators: int = 1, sleep: Any = None):
        self.clock = FakeClock()
        self.store = MemoryStore()
        self.queues = MemoryQueues()
        self.mailer = FakeMailer()
        self.sleeps: List[float] = []
        self.addresses = [OPERATOR_ADDRESS.format(n + 1) for n in range(operators)]
        self.directory = StaticDirectory(
            self.addresses, "test-key-secret", probe_address=PROBE_ADDRESS
        )
        self.kinds = load_registry()
        catalog_data = load_mapping(os.path.join(FIXTURES, "alarm-catalog.yaml"))
        # A made-up family of alarms so a storm can hit one site with many alarms.
        catalog_data["alarms"].append(
            {
                "pattern": r"^storm-(?P<site>[a-z0-9]+)-\d+$",
                "product": "papyrus",
                "what": "A storm test alarm fired.",
                "impact": "None; this is a test.",
                "next": "Nothing to do.",
            }
        )
        self.catalog = catalog_from_mapping(catalog_data)
        self.preferences = MemoryPreferences()
        counter = itertools.count(1)
        self.log = DeliveryLog(self.store, self.clock, make_id=lambda: "id%04d" % next(counter))
        self.dispatcher = Dispatcher(
            self.store, self.log, self.queues, self.kinds, self.clock,
            make_id=lambda: "g%04d" % next(counter),
        )
        self.router = Router(
            store=self.store,
            kinds=self.kinds,
            log=self.log,
            directory=self.directory,
            dispatcher=self.dispatcher,
            incidents=IncidentTracker(self.store, self.clock),
            catalog=self.catalog,
            triage=TriageSettings(
                portal_url=PORTAL,
                role_name="TriageReadOnly",
                accounts={ACCOUNT_ID: "papyrus-production"},
            ),
            clock=self.clock,
            preferences=self.preferences,
        )
        self.email = EmailChannel(
            self.store, self.log, self.directory, self.mailer,
            sleep=self.sleeps.append,
        )
        self._seq = itertools.count(1)

    # -- inputs --------------------------------------------------------------

    def alarm_envelope(
        self, name: str, state: str = "ALARM", muted: bool = False,
        reason: str = "Threshold Crossed: 1 datapoint [2.0] was greater than the threshold (1.0).",
    ) -> Dict[str, Any]:
        stamp = "2026-11-04T12:00:%02d.000+0000" % (next(self._seq) % 60)
        detail: Dict[str, Any] = {
            "alarmName": name,
            "state": {
                "value": state,
                "reason": reason,
                "reasonData": json.dumps(
                    {"recentDatapoints": [2.0], "threshold": 1.0}
                ),
                "timestamp": "%s#%d" % (stamp, next(self._seq)),
            },
            "previousState": {"value": "OK"},
        }
        if muted:
            detail["muteDetail"] = {"muted": True}
        return {
            "source": "aws.cloudwatch",
            "detail-type": "CloudWatch Alarm State Change",
            "account": ACCOUNT_ID,
            "region": "us-east-1",
            "time": self.clock().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "detail": detail,
        }

    def alarm(self, name: str, state: str = "ALARM", **kwargs: Any):  # type: ignore[no-untyped-def]
        return process_envelope(
            self.router, self.catalog, self.alarm_envelope(name, state, **kwargs)
        )

    def heartbeat_envelope(self, event_id: str) -> Dict[str, Any]:
        """What the heartbeat schedule puts on the ingest queue."""
        return {
            "detail-type": "Notification Requested",
            "detail": {
                "schema": 1,
                "eventId": event_id,
                "kind": "caducus.heartbeat",
                "product": "caducus",
                "occurredAt": "2026-11-04T12:00:00Z",
                "severity": "info",
                "facts": {"title": "Operator mail heartbeat"},
            },
        }

    def notification(self, event_id: str, kind: str = "cloudwatch.alarm", **facts: Any):  # type: ignore[no-untyped-def]
        facts.setdefault("title", "A title")
        if kind == "cloudwatch.alarm":
            facts.setdefault("alarmName", "papyrus-monitoring-router-errors")
            facts.setdefault("state", "ALARM")
            facts.setdefault("awsAccountId", ACCOUNT_ID)
            facts.setdefault("region", "us-east-1")
        detail = {
            "schema": 1,
            "eventId": event_id,
            "kind": kind,
            "product": "papyrus",
            "occurredAt": "2026-11-04T12:00:00Z",
            "severity": "info",
            "facts": facts,
        }
        return detail

    # -- outputs -------------------------------------------------------------

    def rows(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        found = [
            i for (pk, sk), i in sorted(self.store.items.items())
            if sk.startswith("DELIVERY#")
        ]
        return [r for r in found if status is None or r["status"] == status]

    def queued_for(self, channel: str = "email") -> List[Dict[str, Any]]:
        return [b for c, b in self.queues.sent if c == channel]

    def send_all_queued(self) -> int:
        """Play the email queue through the sender, as SQS would."""
        sent = 0
        for body in self.queued_for("email"):
            row = self.log.get(body["eventId"], body["deliveryId"])
            if row and row["status"] == "queued":
                self.email.send(row)
                sent += 1
        return sent

    def tick(self, advance: float = 0) -> int:
        self.clock.advance(advance)
        return self.router.tick()


__all__ = ["FakeClock", "FakeMailer", "Throttled", "World"]
