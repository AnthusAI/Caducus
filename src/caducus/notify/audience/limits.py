"""Abuse limits learned from the host's own traffic. No fixed counts.

* Per network: a Poisson tail test against the host's mean signups per network
  per hour. The bound is one wrongful refusal a year for a network that behaves
  like the host's average network (one test per network-hour; 8766 hours a year).
* Per host: the same test on the whole audience's hourly count raises a surge
  notice; it never refuses, since a story can go viral.
* Per address: one outstanding confirmation, with a resend gap learned from how
  long this host's readers take to confirm (`ConfirmLag`).

Counts use hour buckets with a two-day TTL; the running totals are one row per
scope, updated atomically, so each request costs a constant number of writes.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Callable, List, Optional

from ..store import Store

HOURS_PER_YEAR = 8766  # 365.25 days
#: A network or host that behaves like this host's average is flagged at most once a year.
ALPHA = 1.0 / HOURS_PER_YEAR
BUCKET_TTL_SECONDS = 2 * 24 * 3600
# Confidence and coverage of the learned resend gap: the 95% upper bound of the 95th percentile.
COVERAGE = 0.95
CONFIDENCE = 0.95
LAG_BUCKETS = 48  # power-of-two seconds: 2**47 s is millions of years, so every real lag fits


def poisson_tail(count: int, mean: float) -> float:
    """P(X >= count) for X ~ Poisson(mean)."""
    if count <= 0:
        return 1.0
    term = math.exp(-mean)
    below = term
    for k in range(1, count):
        term *= mean / k
        below += term
    return max(0.0, 1.0 - below)


class RateTest:
    """One scope (`network` or `audience`): counts per key per hour and the running mean."""

    def __init__(self, store: Store, clock: Callable[[], datetime], scope: str):
        self._store = store
        self._clock = clock
        self._scope = scope

    def _hour(self) -> int:
        return int(self._clock().timestamp() // 3600)

    def _bucket(self, key: str):  # type: ignore[no-untyped-def]
        return "RATE#%s#%s#%d" % (self._scope, key, self._hour()), "RATE"

    def _stats(self):  # type: ignore[no-untyped-def]
        return self._store.get("STATS#" + self._scope, "STATS") or {}

    def mean(self, leave_out: int = 0) -> float:
        """Mean accepted requests per key-hour, leaving out one cell holding `leave_out` requests.

        The cell under test is left out, so a key cannot raise the baseline it is tested against:
        at a host's start, one busy network would otherwise be the whole of the data.
        """
        stats = self._stats()
        requests = stats.get("requests", 0) - leave_out
        cells = stats.get("keyHours", 0) - (1 if leave_out else 0)
        return (requests + 1) / (cells + 1)

    def improbable(self, key: str, extra: int = 1) -> bool:
        """Would one more accepted request from `key` this hour be improbable for this host?"""
        pk, sk = self._bucket(key)
        current = (self._store.get(pk, sk) or {}).get("count", 0)
        return poisson_tail(current + extra, self.mean(leave_out=current)) < ALPHA

    def record(self, key: str) -> int:
        """Count one accepted request. Returns the key's count this hour."""
        pk, sk = self._bucket(key)
        expires = int(self._clock().timestamp()) + BUCKET_TTL_SECONDS
        row = self._store.increment(pk, sk, {"count": 1}, expires)
        deltas = {"requests": 1}
        if row["count"] == 1:
            deltas["keyHours"] = 1
        self._store.increment("STATS#" + self._scope, "STATS", deltas)
        return row["count"]


def upper_bound_rank(n: int) -> Optional[int]:
    """The order statistic (1-based) that bounds the COVERAGE quantile with CONFIDENCE, or None if n is too small.

    The k-th smallest of n lags bounds the quantile when P(Binomial(n, COVERAGE) <= k - 1) >= CONFIDENCE.
    One pass in log space, so it stays linear in n.
    """
    log_p, log_q = math.log(COVERAGE), math.log1p(-COVERAGE)
    log_n = math.lgamma(n + 1)
    below = 0.0
    for i in range(n):
        below += math.exp(log_n - math.lgamma(i + 1) - math.lgamma(n - i + 1) + i * log_p + (n - i) * log_q)
        if below >= CONFIDENCE:
            return i + 1
    return None


class ConfirmLag:
    """How long this host's readers take to confirm, as a histogram on one row."""

    PK, SK = "STATS#confirm-lag", "STATS"

    def __init__(self, store: Store):
        self._store = store

    @staticmethod
    def bucket(seconds: float) -> int:
        return min(LAG_BUCKETS - 1, max(0, math.ceil(math.log2(max(seconds, 1.0)))))

    def record(self, seconds: float) -> None:
        self._store.increment(self.PK, self.SK, {"b%d" % self.bucket(seconds): 1, "n": 1})

    def resend_gap(self, window_seconds: int) -> int:
        """The learned gap, never longer than the confirm window; the window itself until enough data exists."""
        row = self._store.get(self.PK, self.SK) or {}
        counts: List[int] = [row.get("b%d" % i, 0) for i in range(LAG_BUCKETS)]
        n = sum(counts)
        rank = upper_bound_rank(n) if n else None
        if rank is None:
            return window_seconds
        seen = 0
        for i, count in enumerate(counts):
            seen += count
            if seen >= rank:
                return min(window_seconds, 2 ** i)
        return window_seconds
