"""Who can be notified. First version: the operators, from SSM."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Sequence

from .delivery_log import mask_address, recipient_key_for_address


@dataclass(frozen=True)
class Recipient:
    key: str
    address: str

    @property
    def mask(self) -> str:
        return mask_address(self.address)


@dataclass(frozen=True)
class ChannelAccount(Recipient):
    """A site's connected public account on a channel: the recipient of a broadcast.

    `key` is `CHANNELACCT#<channel>#<accountId>`, `address` is the public handle
    (shown as is: it is public), and `external_id` is the provider's own id for it.
    """

    channel: str = ""
    external_id: str = ""

    @property
    def mask(self) -> str:
        return self.address


def account_key(channel: str, account_id: str) -> str:
    return "CHANNELACCT#%s#%s" % (channel, account_id)


class Directory:
    def operators(self) -> List[Recipient]:
        raise NotImplementedError

    def get(self, key: str) -> Optional[Recipient]:
        raise NotImplementedError

    def probe(self) -> Optional[Recipient]:
        """The mailbox heartbeats go to (the SES mailbox simulator), or None."""
        return None

    def account(self, key: str) -> Optional[ChannelAccount]:
        """A connected channel account by its `CHANNELACCT#...` key, or None."""
        return None


class StaticDirectory(Directory):
    def __init__(
        self,
        addresses: Sequence[str],
        key_secret: str,
        probe_address: Optional[str] = None,
        accounts: Sequence[ChannelAccount] = (),
    ):
        self._accounts = {a.key: a for a in accounts}
        self._recipients = [
            Recipient(recipient_key_for_address(a, key_secret), a) for a in addresses
        ]
        self._probe = (
            Recipient(recipient_key_for_address(probe_address, key_secret), probe_address)
            if probe_address
            else None
        )

    def probe(self) -> Optional[Recipient]:
        return self._probe

    def account(self, key: str) -> Optional[ChannelAccount]:
        return self._accounts.get(key)

    def connect(self, account: ChannelAccount) -> None:
        self._accounts[account.key] = account

    def operators(self) -> List[Recipient]:
        return list(self._recipients)

    def get(self, key: str) -> Optional[Recipient]:
        everyone = self._recipients + ([self._probe] if self._probe else [])
        return next((r for r in everyone if r.key == key), None)


class SsmDirectory(StaticDirectory):
    """Reads a StringList parameter of operator addresses and a key secret from SSM.

    The addresses live in AWS configuration, never in the repository. Read once
    per Lambda container.
    """

    def __init__(
        self,
        client: Any,
        operators_parameter: str,
        key_parameter: str,
        probe_address: Optional[str] = None,
    ):
        def value(name: str) -> str:
            return client.get_parameter(Name=name, WithDecryption=True)["Parameter"][
                "Value"
            ]

        raw = value(operators_parameter)
        addresses = [a.strip() for a in raw.split(",") if a.strip()]
        super().__init__(addresses, value(key_parameter), probe_address)
