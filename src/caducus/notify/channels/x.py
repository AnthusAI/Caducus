"""The X channel: post a drafted text to a connected X account, and take it down.

Contract with the host
----------------------
The host passes, and Caducus never stores or logs:

* `tokens` (`TokenProvider`): `access_token(account)` returns a current OAuth 2.0
  user-context access token for that account. The host keeps the rotating
  refresh token wherever it likes and refreshes under its own lock. If it
  cannot, it raises; the delivery then stays queued and is retried.
* `prices` (`PriceTable`): what X charges, copied from X's price list with its
  date. X returns no price with an answer, so the channel records the price it
  applied from this table and never guesses one.
* `guard` (`Guard`): the host's pause and budget. Asked before every post.
* `recent` (`RecentPosts`, optional): the check before retry. The default reads
  the account's latest posts from X.

Exactly once. X documents no idempotency key for creating a post, so a request
that timed out or answered 5xx has an unknown outcome and is never repeated
blindly. Before the request the delivery is claimed. If the claim is already
there when `send` runs, an earlier attempt may have posted (it timed out, or the
process died); the channel then asks `recent` for the account's posts and, if the
post is there, records it as sent instead of posting again. Only a definite "not
posted" lets the post go out. A definite refusal (4xx) is recorded as failed and
not retried. A 429 is a definite "not processed": the channel waits until
`x-rate-limit-reset` and repeats the request.

The text is posted exactly as the host drafted it; the channel adds nothing that
X would turn into a mention or a link.
"""

from __future__ import annotations

import json
import logging
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence
from urllib.parse import quote, urlencode

from ..delivery_log import DeliveryLog
from ..directory import ChannelAccount, Directory
from ..logs import emit
from ..prices import Price
from ..store import Store
from .base import Channel, Outcome

LOGGER = logging.getLogger("caducus.notify.x")
API = "https://api.x.com"
URL_IN_TEXT = re.compile(r"https?://\S+")


class UnknownOutcome(Exception):
    """The request may or may not have reached X. Do not repeat it blindly: the
    delivery stays claimed, and the next attempt checks X first."""


class XRefused(Exception):
    """X definitely refused the request."""


@dataclass(frozen=True)
class PriceTable:
    """The host's copy of X's price list. Each price names its source and date."""

    post: Price
    post_with_url: Price
    delete: Price
    read_per_post: Price


class TokenProvider:
    def access_token(self, account: ChannelAccount) -> str:
        raise NotImplementedError


class Guard:
    """The host's pause and budget."""

    def refusal(self, account: ChannelAccount, price: Price) -> Optional[str]:
        """Why this account may not spend `price` now (paused, budget reached), or None."""
        raise NotImplementedError

    def charged(self, account: ChannelAccount, price: Price) -> None:
        """Called once for every price the channel records on a delivery."""
        raise NotImplementedError


@dataclass(frozen=True)
class RecentResult:
    post_id: Optional[str]  # the post found, or None
    posts_read: int  # what X returned, because reads are priced per post returned


class RecentPosts:
    """The check before retry: is a post matching `match` already on the account?"""

    def find(self, account: ChannelAccount, match: str) -> RecentResult:
        raise NotImplementedError


@dataclass(frozen=True)
class HttpResponse:
    status: int
    headers: Mapping[str, str]  # lower-case names
    body: bytes


class Http:
    def request(
        self, method: str, url: str, headers: Mapping[str, str], body: Optional[bytes]
    ) -> HttpResponse:
        """Return any answer X gives; raise OSError (including timeouts) when none arrived."""
        raise NotImplementedError


class UrllibHttp(Http):
    def __init__(self, timeout: float):
        self._timeout = timeout

    def request(self, method, url, headers, body):  # type: ignore[no-untyped-def]
        request = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as answer:
                return HttpResponse(
                    answer.status,
                    {k.lower(): v for k, v in answer.headers.items()},
                    answer.read(),
                )
        except urllib.error.HTTPError as error:
            return HttpResponse(
                error.code, {k.lower(): v for k, v in error.headers.items()}, error.read()
            )
        except urllib.error.URLError as error:
            raise OSError(str(error.reason)) from None


class XApi:
    """Authenticated calls to X that wait out a 429 and never swallow an unknown outcome."""

    def __init__(
        self,
        http: Http,
        tokens: TokenProvider,
        base_url: str,
        clock: Callable[[], datetime],
        sleep: Callable[[float], None],
    ):
        self._http = http
        self._tokens = tokens
        self._base = base_url.rstrip("/")
        self._clock = clock
        self._sleep = sleep

    def call(
        self, account: ChannelAccount, method: str, path: str, payload: Optional[Any] = None
    ) -> Any:
        while True:
            headers = {
                "Authorization": "Bearer " + self._tokens.access_token(account),
                "Content-Type": "application/json",
            }
            body = json.dumps(payload).encode("utf-8") if payload is not None else None
            try:
                answer = self._http.request(method, self._base + path, headers, body)
            except OSError as error:
                raise UnknownOutcome("no answer from X: %s" % error) from None
            if answer.status == 429:
                reset = answer.headers.get("x-rate-limit-reset")
                if reset is None:
                    raise XRefused("429 with no x-rate-limit-reset; not processed")
                wait = float(reset) - self._clock().timestamp()
                emit(LOGGER, "x_rate_limited", wait=max(wait, 0))
                self._sleep(max(wait, 0))
                continue
            if answer.status >= 500:
                raise UnknownOutcome("X answered %d" % answer.status)
            data = _json(answer.body)
            if answer.status >= 400:
                raise XRefused(
                    "%d %s" % (answer.status, data.get("detail") or data.get("title") or "")
                )
            return data


def _json(body: bytes) -> Dict[str, Any]:
    try:
        value = json.loads(body.decode("utf-8")) if body else {}
    except ValueError:
        return {}
    return value if isinstance(value, dict) else {}


class XRecentPosts(RecentPosts):
    """Reads the account's latest posts (GET /2/users/:id/tweets) and matches a link or text.

    `count` is how many of the latest posts to read (X allows 5 to 100). The
    account is the publication's own, so a post made a moment ago is among the
    newest; the host chooses how far back to look, and pays $ per post read.
    """

    def __init__(self, api: XApi, count: int):
        self._api = api
        self._count = count

    def find(self, account: ChannelAccount, match: str) -> RecentResult:
        query = urlencode({"max_results": self._count, "tweet.fields": "entities"})
        data = self._api.call(account, "GET", "/2/users/%s/tweets?%s" % (quote(account.external_id), query))
        posts: List[Mapping[str, Any]] = data.get("data") or []
        for post in posts:
            urls = [u.get("expanded_url") for u in (post.get("entities") or {}).get("urls") or []]
            if match in urls or match == post.get("text"):
                return RecentResult(post["id"], len(posts))
        return RecentResult(None, len(posts))


class XChannel(Channel):
    name = "x"

    def __init__(
        self,
        store: Store,
        log: DeliveryLog,
        directory: Directory,
        *,
        tokens: TokenProvider,
        prices: PriceTable,
        guard: Guard,
        clock: Callable[[], datetime],
        http: Http,
        recent: Optional[RecentPosts] = None,
        recent_count: Optional[int] = None,
        base_url: str = API,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._store = store
        self._log = log
        self._directory = directory
        self._prices = prices
        self._guard = guard
        self._api = XApi(http, tokens, base_url, clock, sleep)
        if recent is None:
            if recent_count is None:
                raise ValueError("pass `recent`, or `recent_count` for the built-in reader")
            recent = XRecentPosts(self._api, recent_count)
        self._recent = recent

    # -- bookkeeping ---------------------------------------------------------

    def _fail(self, delivery: Mapping[str, Any], reason: str) -> Outcome:
        self._log.advance(delivery["eventId"], delivery["deliveryId"], "failed", reason=reason)
        emit(LOGGER, "x_failed", eventId=delivery["eventId"], deliveryId=delivery["deliveryId"], reason=reason)
        return Outcome("failed", reason=reason)

    def _sent(
        self, delivery: Mapping[str, Any], account: ChannelAccount, post_id: str,
        attempts: int, charge: Optional[Price],
    ) -> Outcome:
        """Record the post id and what it cost in one write, so neither is lost without the other."""
        self._log.advance(
            delivery["eventId"], delivery["deliveryId"], "sent",
            provider_message_id=post_id, attempts=attempts, charge=charge,
        )
        if charge is not None:
            self._guard.charged(account, charge)
        emit(LOGGER, "x_sent", eventId=delivery["eventId"], deliveryId=delivery["deliveryId"], postId=post_id)
        return Outcome("sent", provider_message_id=post_id)

    def _read_price(self, result: RecentResult) -> Optional[Price]:
        return self._prices.read_per_post.times(result.posts_read) if result.posts_read else None

    def _charge_read(self, delivery: Mapping[str, Any], account: ChannelAccount, charge: Optional[Price]) -> None:
        if charge is not None:
            self._log.advance(delivery["eventId"], delivery["deliveryId"], "queued", charge=charge)
            self._guard.charged(account, charge)

    # -- send ----------------------------------------------------------------

    def send(self, delivery: Mapping[str, Any]) -> Outcome:
        event_id, delivery_id, pk = delivery["eventId"], delivery["deliveryId"], delivery["pk"]
        current = self._log.get(event_id, delivery_id)
        if current is None or current["status"] != "queued":
            return Outcome("duplicate")
        account = self._directory.account(delivery["recipientKey"])
        content = self._store.get(pk, "CONTENT#" + delivery_id)
        if account is None or content is None:
            return self._fail(delivery, "account unknown" if account is None else "content missing")
        item = content["items"][0]
        text, match = item["text"], item.get("url") or item["text"]
        post_price = self._prices.post_with_url if URL_IN_TEXT.search(text) else self._prices.post
        attempts = current.get("attempts", 0) + 1
        claimed = self._store.put_if_absent(
            {"pk": pk, "sk": "CLAIM#" + delivery_id, "expiresAt": delivery["expiresAt"]}
        )
        try:
            if not claimed:
                # An earlier attempt may have posted. Look before posting again.
                try:
                    result = self._recent.find(account, match)
                except XRefused as error:
                    return self._fail(delivery, "could not check for an earlier post: %s" % error)
                if result.post_id:
                    return self._sent(delivery, account, result.post_id, attempts, self._read_price(result))
                self._charge_read(delivery, account, self._read_price(result))
            reason = self._guard.refusal(account, post_price)
            if reason:
                self._store.delete(pk, "CLAIM#" + delivery_id)
                self._log.advance(event_id, delivery_id, "refused", reason=reason)
                emit(LOGGER, "x_refused", eventId=event_id, deliveryId=delivery_id, reason=reason)
                return Outcome("failed", reason=reason)
            return self._post(delivery, account, text, match, post_price, attempts)
        except UnknownOutcome:
            self._log.advance(event_id, delivery_id, "queued", attempts=attempts)
            emit(LOGGER, "x_unknown_outcome", eventId=event_id, deliveryId=delivery_id)
            raise
        except XRefused as error:
            return self._fail(delivery, str(error))

    def _post(
        self, delivery: Mapping[str, Any], account: ChannelAccount, text: str,
        match: str, price: Price, attempts: int,
    ) -> Outcome:
        try:
            answer = self._api.call(account, "POST", "/2/tweets", {"text": text})
        except XRefused as error:
            if "duplicate content" in str(error).lower():
                # X says this text is already posted. Believe it only once the account shows it.
                result = self._recent.find(account, match)
                if result.post_id:
                    return self._sent(delivery, account, result.post_id, attempts, self._read_price(result))
                self._charge_read(delivery, account, self._read_price(result))
            raise
        post_id = (answer.get("data") or {}).get("id")
        if not post_id:
            raise UnknownOutcome("X accepted the post but returned no id")
        return self._sent(delivery, account, post_id, attempts, price)

    # -- delete --------------------------------------------------------------

    def delete(self, delivery: Mapping[str, Any], *, by: str) -> Outcome:
        """Take the post down (DELETE /2/tweets/:id) and record who did, when and what it cost."""
        event_id, delivery_id = delivery["eventId"], delivery["deliveryId"]
        current = self._log.get(event_id, delivery_id)
        if current is None or "providerMessageId" not in current:
            return Outcome("failed", reason="no post to delete")
        if current["status"] == "withdrawn":
            return Outcome("duplicate", provider_message_id=current["providerMessageId"])
        account = self._directory.account(current["recipientKey"])
        if account is None:
            return Outcome("failed", reason="account unknown")
        post_id = current["providerMessageId"]
        answer = self._api.call(account, "DELETE", "/2/tweets/" + quote(post_id))
        if not (answer.get("data") or {}).get("deleted"):
            raise XRefused("X did not confirm the delete")
        self._log.advance(
            event_id, delivery_id, "withdrawn", actor=by, charge=self._prices.delete,
            reason="withdrawn by " + by,
        )
        self._guard.charged(account, self._prices.delete)
        emit(LOGGER, "x_withdrawn", eventId=event_id, deliveryId=delivery_id, postId=post_id)
        return Outcome("withdrawn", provider_message_id=post_id)
