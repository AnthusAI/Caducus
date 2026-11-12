"""Fakes for tests and specs: nothing here touches AWS."""

from __future__ import annotations

import itertools
import json
import os
import threading
import time
from datetime import datetime, timedelta
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .catalog import catalog_from_mapping
from .files import load_mapping
from .channels.email import EmailChannel, Mailer, Throttled
from .channels.x import Guard, PriceTable, TokenProvider, UrllibHttp, XChannel
from .delivery_log import DeliveryLog
from .directory import ChannelAccount, StaticDirectory, account_key
from .prices import Price
from .grouping import Dispatcher
from .handlers.router import process_envelope
from .incidents import IncidentTracker
from .kinds import Kind, load_registry
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

    def __init__(
        self,
        operators: int = 1,
        sleep: Any = None,
        accounts: Sequence[ChannelAccount] = (),
        extra_kinds: Sequence[Kind] = (),
    ):
        self.clock = FakeClock()
        self.store = MemoryStore()
        self.queues = MemoryQueues()
        self.mailer = FakeMailer()
        self.sleeps: List[float] = []
        self.addresses = [OPERATOR_ADDRESS.format(n + 1) for n in range(operators)]
        self.directory = StaticDirectory(
            self.addresses, "test-key-secret", probe_address=PROBE_ADDRESS,
            accounts=accounts,
        )
        self.kinds = load_registry()
        for kind in extra_kinds:
            self.kinds._kinds[kind.name] = kind
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


X_ACCOUNT_ID = "acct1"
X_USER_ID = "9001"
X_TOKEN = "fake-access-token-do-not-log"
X_PRICES = PriceTable(
    post=Price("0.015", "USD", "X price list, 2026-11-12: create post"),
    post_with_url=Price("0.200", "USD", "X price list, 2026-11-12: create post with URL"),
    delete=Price("0.010", "USD", "X price list, 2026-11-12: delete"),
    read_per_post=Price("0.005", "USD", "X price list, 2026-11-12: read, per post returned"),
)


class FakeX:
    """A small HTTP server that answers like X's API v2, so the adapter's real
    HTTP client is exercised and nothing leaves the machine.

    `script` is a list of steps consumed one per request, before normal
    behaviour: ("status", code, body, headers); ("hang",) to never answer; or
    ("post-then-hang",) to create the post and then not answer.
    """

    def __init__(self) -> None:
        self.requests: List[Dict[str, Any]] = []
        self.posts: List[Dict[str, Any]] = []
        self.script: List[Any] = []
        self._ids = itertools.count(1000)
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # type: ignore[no-untyped-def]
                pass

            def _answer(self, code: int, body: Any, headers: Optional[Mapping[str, str]] = None) -> None:
                raw = json.dumps(body).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                for name, value in (headers or {}).items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(raw)

            def _handle(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length)) if length else None
                fake.requests.append(
                    {"method": self.command, "path": self.path, "body": body,
                     "authorization": self.headers.get("Authorization")}
                )
                step = fake.script.pop(0) if fake.script else None
                if step and step[0] == "status":
                    return self._answer(step[1], step[2], step[3])
                if step and step[0] == "hang":
                    return time.sleep(1)  # longer than the client waits: it times out
                if self.command == "POST" and self.path == "/2/tweets":
                    post = {"id": str(next(fake._ids)), "text": body["text"]}
                    fake.posts.append(post)
                    if step and step[0] == "post-then-hang":
                        return time.sleep(1)
                    return self._answer(201, {"data": post})
                if self.command == "DELETE" and self.path.startswith("/2/tweets/"):
                    post_id = self.path.rsplit("/", 1)[1]
                    fake.posts = [p for p in fake.posts if p["id"] != post_id]
                    return self._answer(200, {"data": {"deleted": True}})
                if self.command == "GET" and self.path.startswith("/2/users/%s/tweets" % X_USER_ID):
                    shown = [
                        {"id": p["id"], "text": p["text"], "entities": {"urls": [
                            {"expanded_url": w} for w in p["text"].split() if w.startswith("http")
                        ]}}
                        for p in reversed(fake.posts)
                    ]
                    return self._answer(200, {"data": shown} if shown else {"meta": {"result_count": 0}})
                self._answer(404, {"title": "Not Found"})

            do_GET = do_POST = do_DELETE = _handle

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = "http://127.0.0.1:%d" % self._server.server_address[1]
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


class FakeTokens(TokenProvider):
    def access_token(self, account: ChannelAccount) -> str:
        return X_TOKEN


class FakeGuard(Guard):
    """Pause and budget kept in memory: `paused` is a reason or None."""

    def __init__(self) -> None:
        self.paused: Optional[str] = None
        self.budget: Optional[Decimal] = None
        self.spent = Decimal("0")

    def refusal(self, account, price):  # type: ignore[no-untyped-def]
        if self.paused:
            return self.paused
        if self.budget is not None and self.spent + Decimal(price.amount) > self.budget:
            return "budget reached"
        return None

    def charged(self, account, price):  # type: ignore[no-untyped-def]
        self.spent += Decimal(price.amount)


class XWorld(World):
    """The World plus a connected X account, a fake X server and the x channel."""

    def __init__(self) -> None:
        self.account = ChannelAccount(
            key=account_key("x", X_ACCOUNT_ID), address="@example_pub",
            channel="x", external_id=X_USER_ID,
        )
        super().__init__(
            accounts=[self.account],
            extra_kinds=[
                Kind(name="story.announce", klass="broadcast", default_on=True, channels=("x",))
            ],
        )
        self.x_server = FakeX()
        self.guard = FakeGuard()
        self.waits: List[float] = []
        self.x = XChannel(
            self.store, self.log, self.directory,
            tokens=FakeTokens(), prices=X_PRICES, guard=self.guard, clock=self.clock,
            http=UrllibHttp(timeout=0.3), recent_count=5, base_url=self.x_server.url,
            sleep=self.waits.append,
        )

    def announce(self, event_id: str, text: Optional[str], url: str = "", account: Optional[str] = None):  # type: ignore[no-untyped-def]
        facts: Dict[str, Any] = {"title": "A Story"}
        if text is not None:
            facts["text"] = text
        if url:
            facts["url"] = url
        return self.router.handle_detail(
            {
                "schema": 1, "eventId": event_id, "kind": "story.announce",
                "product": "papyrus", "site": "mom", "occurredAt": "2026-11-04T12:00:00Z",
                "severity": "info", "facts": facts, "audience": account or self.account.key,
            }
        )

    def queued_row(self, event_id: str) -> Dict[str, Any]:
        (row,) = self.log.for_event(event_id)
        return row

    def close(self) -> None:
        self.x_server.close()


__all__ = ["FakeClock", "FakeMailer", "Throttled", "World", "XWorld"]
