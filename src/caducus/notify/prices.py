"""What a request costs, as the host states it.

Providers that charge per request do not return a price with the answer, so the
host passes the price list (copied from the provider's page, with its date in
`note`) and the channel records what it applied. Nothing here is a constant.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


@dataclass(frozen=True)
class Price:
    """An amount of money in a currency, as a decimal string, and where it came from."""

    amount: str
    currency: str
    note: str

    def __post_init__(self) -> None:
        try:
            value = Decimal(self.amount)
        except InvalidOperation:
            raise ValueError("price amount must be a decimal string: %r" % self.amount) from None
        if not value.is_finite() or value < 0:
            raise ValueError("price amount must not be negative: %r" % self.amount)
        if not self.currency.strip():
            raise ValueError("price needs a currency")
        if not self.note.strip():
            raise ValueError("price needs a note saying where it comes from")

    def times(self, count: int) -> "Price":
        return Price(str(Decimal(self.amount) * count), self.currency, self.note)
