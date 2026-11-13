"""Unit tests for the public audience's parts. Behaviour is specified in features/notifications/public_audience.feature."""

from __future__ import annotations

import sys
import types

import pytest

from caducus.notify.audience import addresses, tokens
from caducus.notify.audience.domains import DnsDomainChecker
from caducus.notify.audience.limits import ALPHA, ConfirmLag, poisson_tail, upper_bound_rank
from caducus.notify.channels.email import SesMailer, unsubscribe_headers
from caducus.notify.fakes import PublicWorld
from caducus.notify.store import MemoryStore


def test_tokens_round_trip_and_refuse_tampering_purpose_and_expiry():
    key = b"k" * 32
    token = tokens.issue(key, tokens.Claims("ADDR#x", "confirm", "g1", "n1", 100))
    assert tokens.verify(key, token, "confirm", 50).nonce == "n1"
    for bad, purpose, now in ((token, "manage", 50), (token, "confirm", 101), (token[:-2] + "AA", "confirm", 50), ("x", "confirm", 0)):
        with pytest.raises(tokens.InvalidToken):
            tokens.verify(key, bad, purpose, now)
    with pytest.raises(tokens.InvalidToken):
        tokens.verify(b"other" * 8, token, "confirm", 50)


def test_addresses_networks_and_masks():
    assert addresses.is_valid("a.b+c@example.co.uk")
    assert not addresses.is_valid("A@example.com") and not addresses.is_valid("a@localhost")
    assert addresses.network_of("203.0.113.77") == "203.0.113.0/24"
    assert addresses.network_of("2001:db8:1:2::5") == "2001:db8:1::/48"
    assert addresses.network_of("junk") == "unknown"
    assert addresses.mask("reader@example.com") == "r***@example.com"


def test_poisson_tail_and_the_yearly_bound():
    assert poisson_tail(0, 3.0) == 1.0
    assert abs(poisson_tail(1, 1.0) - (1 - 2.718281828 ** -1)) < 1e-6
    # With a mean of one signup per network-hour, the seventh in an hour is the first improbable one.
    assert min(c for c in range(1, 20) if poisson_tail(c, 1.0) < ALPHA) == 7


def test_the_resend_gap_needs_59_confirmations_and_is_their_upper_bound():
    assert upper_bound_rank(58) is None and upper_bound_rank(59) == 59
    store = MemoryStore()
    lag = ConfirmLag(store)
    for _ in range(58):
        lag.record(300)
    assert lag.resend_gap(259200) == 259200
    lag.record(300)
    assert lag.resend_gap(259200) == 512  # 300 s falls in the 2**9 bucket
    assert lag.resend_gap(100) == 100  # never longer than the window


class _Answer(list):
    pass


def _fake_dns(monkeypatch, answers):
    """A stand-in for dnspython: `answers` maps (domain, type) to a list of exchanges, or an exception name."""
    resolver_module = types.ModuleType("dns.resolver")
    exception_module = types.ModuleType("dns.exception")

    class DNSException(Exception):
        pass

    class NXDOMAIN(DNSException):
        pass

    class NoAnswer(DNSException):
        pass

    class Timeout(DNSException):
        pass

    class Resolver:
        lifetime = 0

        def resolve(self, domain, record_type):
            found = answers.get((domain, record_type), "NoAnswer")
            if isinstance(found, str):
                raise {"NXDOMAIN": NXDOMAIN, "NoAnswer": NoAnswer, "Timeout": Timeout}[found]()
            return _Answer(types.SimpleNamespace(exchange=e) for e in found)

    resolver_module.Resolver, resolver_module.NXDOMAIN, resolver_module.NoAnswer = Resolver, NXDOMAIN, NoAnswer
    exception_module.DNSException = DNSException
    package = types.ModuleType("dns")
    package.resolver, package.exception = resolver_module, exception_module
    monkeypatch.setitem(sys.modules, "dns", package)
    monkeypatch.setitem(sys.modules, "dns.resolver", resolver_module)
    monkeypatch.setitem(sys.modules, "dns.exception", exception_module)


def test_dns_check_follows_rfc_5321_and_7505(monkeypatch):
    _fake_dns(monkeypatch, {
        ("mx.example", "MX"): ["mail.mx.example."],
        ("null.example", "MX"): ["."],
        ("gone.example", "MX"): "NXDOMAIN",
        ("implicit.example", "A"): ["192.0.2.1"],
        ("slow.example", "MX"): "Timeout",
    })
    checker = DnsDomainChecker()
    assert checker.accepts_mail("mx.example")
    assert not checker.accepts_mail("null.example")
    assert not checker.accepts_mail("gone.example")
    assert checker.accepts_mail("implicit.example")
    assert not checker.accepts_mail("nothing.example")
    assert checker.accepts_mail("slow.example")  # a DNS failure never turns a reader away


def test_ses_mailer_sends_the_unsubscribe_headers():
    calls = []

    class Client:
        def send_email(self, **request):
            calls.append(request)
            return {"MessageId": "m1"}

    mailer = SesMailer(Client(), "News <news@example.com>", "papyrus-example")
    mailer.send(to_address="r@example.com", subject="s", text="t", html="h", tags={"kind": "k"},
                headers=unsubscribe_headers("https://news.example.com/api/notifications/unsubscribe?t=v1.x"))
    headers = calls[0]["Content"]["Simple"]["Headers"]
    assert {"Name": "List-Unsubscribe-Post", "Value": "List-Unsubscribe=One-Click"} in headers
    mailer.send(to_address="r@example.com", subject="s", text="t", html="h", tags={})
    assert "Headers" not in calls[1]["Content"]["Simple"]


def test_public_mail_without_an_audience_is_refused_not_sent():
    world = PublicWorld()
    world.subscribe("reader@example.com")
    world.deliver()
    world.act("confirm", token=world.confirm_token("reader@example.com"))
    world.email._audience = None
    world.publish("story.published:x")
    world.deliver()
    (row,) = world.log.for_event("story.published:x")
    assert row["status"] == "refused" and len(world.mail_to("reader@example.com")) == 1


def test_a_public_event_without_lists_is_refused():
    world = PublicWorld()
    result = world.publish("story.published:none", lists=())
    assert result.outcome == "refused" and result.reason == "no audience"


def test_a_failed_confirmation_lets_the_reader_try_again_at_once():
    world = PublicWorld()
    real = world.audience._emit
    world.audience._emit = lambda detail: (_ for _ in ()).throw(RuntimeError("queue down"))
    with pytest.raises(RuntimeError):
        world.subscribe("reader@example.com")
    world.audience._emit = real
    assert world.subscribe("reader@example.com")["body"]["status"] == "check_email"
    world.deliver()
    assert len(world.mail_to("reader@example.com")) == 1


def test_no_log_line_holds_an_address(caplog):
    import logging

    caplog.set_level(logging.INFO, logger="caducus.notify")
    world = PublicWorld()
    world.subscribe("secret.reader@example.com", ["stories", "weekly"])
    world.deliver()
    reply = world.act("confirm", token=world.confirm_token("secret.reader@example.com"))
    world.publish("story.published:y")
    world.deliver()
    world.act("unsubscribe", token=reply["body"]["manageToken"])
    world.audience.erase("secret.reader@example.com")
    assert "secret.reader" not in caplog.text


def test_the_whole_reader_journey_runs_on_dynamodb_in_moto(monkeypatch):
    """The same journey against the real DynamoDB adapter (moto), so types and conditions are exercised."""
    import re

    moto = pytest.importorskip("moto")
    import boto3

    from caducus.notify import fakes
    from caducus.notify.store import DynamoStore

    with moto.mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.create_table(
            TableName="site",
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": n, "AttributeType": "S"} for n in ("pk", "sk", "recipientKey", "queuedAt", "gsiList")],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            GlobalSecondaryIndexes=[
                {"IndexName": "byRecipient", "KeySchema": [{"AttributeName": "recipientKey", "KeyType": "HASH"},
                                                          {"AttributeName": "queuedAt", "KeyType": "RANGE"}],
                 "Projection": {"ProjectionType": "ALL"}},
                {"IndexName": "byList", "KeySchema": [{"AttributeName": "gsiList", "KeyType": "HASH"}],
                 "Projection": {"ProjectionType": "KEYS_ONLY"}},
            ],
        )
        table = boto3.resource("dynamodb", region_name="us-east-1").Table("site")
        monkeypatch.setattr(fakes, "MemoryStore", lambda: DynamoStore(table))
        world = fakes.PublicWorld()
        address = "reader@example.com"
        assert world.subscribe(address, ["stories", "weekly"])["statusCode"] == 202
        world.deliver()
        token = re.search(r"token=(v1\.[A-Za-z0-9_\-.]+)", world.mail_to(address)[0]["text"]).group(1)
        reply = world.act("confirm", token=token)
        assert reply["body"]["status"] == "confirmed"
        manage = reply["body"]["manageToken"]
        world.publish("story.published:moto", ["stories", "weekly"])
        world.deliver()
        assert len(world.mail_to(address)) == 2
        assert world.act("update_preferences", token=manage, lists=["weekly"])["body"]["status"] == "saved"
        assert world.act("unsubscribe", token=manage, list="weekly")["body"]["status"] == "unsubscribed"
        assert world.audience.export(address)["body"]["consent"][0]["source"] == "form"
        assert world.audience.erase(address)["body"]["status"] == "deleted"
        assert world.act("preferences_state", token=manage)["body"]["status"] == "invalid"
