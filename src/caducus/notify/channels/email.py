"""The email channel: claim, pace, send with backoff, record. Never tracked.

A retry must not double-send, so the delivery is claimed with a conditional
write before SES is called. A definite failure releases the claim so the queue's
retry can try again. If the process dies between the claim and the answer, that
delivery is at-most-once: it stays claimed and shows as queued in the log, and
the dead-letter path (M2) will surface it.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

from ..delivery_log import DeliveryLog
from ..directory import Directory
from ..logs import emit
from ..render.operator import render_operator_email
from ..store import Store
from .base import Channel, Outcome

LOGGER = logging.getLogger("caducus.notify.email")
BACKOFF_SECONDS = (1, 2, 4)
THROTTLE_CODES = (
    "Throttling",
    "ThrottlingException",
    "TooManyRequestsException",
    "Throttled",
    "RequestLimitExceeded",
)


class Throttled(Exception):
    """The provider asked us to slow down."""


class Mailer:
    """The interface behind which SES sits. Tests use a fake."""

    def send(
        self,
        *,
        to_address: str,
        subject: str,
        text: str,
        html: str,
        tags: Mapping[str, str],
    ) -> str:
        """Send one message and return the provider's message id."""
        raise NotImplementedError

    def max_send_rate(self) -> float:
        """Messages per second the account may send."""
        raise NotImplementedError


class SesMailer(Mailer):
    """Amazon SES v2. Uses a configuration set with no open or click tracking."""

    def __init__(self, client: Any, from_address: str, configuration_set: str = ""):
        self._client = client
        self._from = from_address
        self._set = configuration_set

    def send(self, *, to_address, subject, text, html, tags):  # type: ignore[no-untyped-def]
        from botocore.exceptions import ClientError

        request: Dict[str, Any] = {
            "FromEmailAddress": self._from,
            "Destination": {"ToAddresses": [to_address]},
            "Content": {
                "Simple": {
                    "Subject": {"Data": subject, "Charset": "UTF-8"},
                    "Body": {
                        "Text": {"Data": text, "Charset": "UTF-8"},
                        "Html": {"Data": html, "Charset": "UTF-8"},
                    },
                }
            },
            "EmailTags": [{"Name": k, "Value": v} for k, v in tags.items()],
        }
        if self._set:
            request["ConfigurationSetName"] = self._set
        try:
            return self._client.send_email(**request)["MessageId"]
        except ClientError as error:
            if error.response["Error"]["Code"] in THROTTLE_CODES:
                raise Throttled(str(error.response["Error"]["Code"])) from error
            raise

    def max_send_rate(self) -> float:
        return float(self._client.get_account()["SendQuota"]["MaxSendRate"])


class Pacer:
    """Spaces calls to the account's MaxSendRate, which it reads once."""

    def __init__(
        self,
        rate: Callable[[], float],
        now: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._rate = rate
        self._now = now
        self._sleep = sleep
        self._interval: Optional[float] = None
        self._last: Optional[float] = None

    def wait(self) -> None:
        if self._interval is None:
            self._interval = 1.0 / max(self._rate(), 0.001)
        if self._last is not None:
            gap = self._last + self._interval - self._now()
            if gap > 0:
                self._sleep(gap)
        self._last = self._now()


class EmailChannel(Channel):
    name = "email"

    def __init__(
        self,
        store: Store,
        log: DeliveryLog,
        directory: Directory,
        mailer: Mailer,
        pacer: Optional[Pacer] = None,
        sleep: Callable[[float], None] = time.sleep,
        backoff: Sequence[float] = BACKOFF_SECONDS,
    ):
        self._store = store
        self._log = log
        self._directory = directory
        self._mailer = mailer
        self._pacer = pacer
        self._sleep = sleep
        self._backoff = tuple(backoff)

    def _send_with_backoff(self, **message: Any) -> str:
        delays = list(self._backoff)
        while True:
            if self._pacer:
                self._pacer.wait()
            try:
                return self._mailer.send(**message)
            except Throttled:
                if not delays:
                    raise
                self._sleep(delays.pop(0))

    def send(self, delivery: Mapping[str, Any]) -> Outcome:
        event_id, delivery_id = delivery["eventId"], delivery["deliveryId"]
        pk = delivery["pk"]
        recipient = self._directory.get(delivery["recipientKey"])
        content = self._store.get(pk, "CONTENT#" + delivery_id)
        if recipient is None or content is None:
            reason = "recipient unknown" if recipient is None else "content missing"
            self._log.advance(event_id, delivery_id, "failed", reason=reason)
            emit(LOGGER, "email_failed", eventId=event_id, deliveryId=delivery_id, reason=reason)
            return Outcome("failed", reason=reason)
        claimed = self._store.put_if_absent(
            {"pk": pk, "sk": "CLAIM#" + delivery_id, "expiresAt": delivery["expiresAt"]}
        )
        if not claimed:
            return Outcome("duplicate")
        rendered = render_operator_email(content["items"])
        try:
            message_id = self._send_with_backoff(
                to_address=recipient.address,
                subject=rendered.subject,
                text=rendered.text,
                html=rendered.html,
                tags={"delivery": delivery_id, "kind": delivery["kind"]},
            )
        except Exception:
            self._store.delete(pk, "CLAIM#" + delivery_id)
            self._log.advance(
                event_id,
                delivery_id,
                "queued",
                attempts=delivery.get("attempts", 0) + 1,
            )
            emit(LOGGER, "email_attempt_failed", eventId=event_id, deliveryId=delivery_id)
            raise
        self._log.advance(
            event_id,
            delivery_id,
            "sent",
            provider_message_id=message_id,
            attempts=delivery.get("attempts", 0) + 1,
        )
        emit(LOGGER, "email_sent", eventId=event_id, deliveryId=delivery_id, status="sent")
        return Outcome("sent", provider_message_id=message_id)
