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


class Directory:
    def operators(self) -> List[Recipient]:
        raise NotImplementedError

    def get(self, key: str) -> Optional[Recipient]:
        raise NotImplementedError


class StaticDirectory(Directory):
    def __init__(self, addresses: Sequence[str], key_secret: str):
        self._recipients = [
            Recipient(recipient_key_for_address(a, key_secret), a) for a in addresses
        ]

    def operators(self) -> List[Recipient]:
        return list(self._recipients)

    def get(self, key: str) -> Optional[Recipient]:
        return next((r for r in self._recipients if r.key == key), None)


class SsmDirectory(StaticDirectory):
    """Reads a StringList parameter of operator addresses and a key secret from SSM.

    The addresses live in AWS configuration, never in the repository. Read once
    per Lambda container.
    """

    def __init__(self, client: Any, operators_parameter: str, key_parameter: str):
        def value(name: str) -> str:
            return client.get_parameter(Name=name, WithDecryption=True)["Parameter"][
                "Value"
            ]

        raw = value(operators_parameter)
        addresses = [a.strip() for a in raw.split(",") if a.strip()]
        super().__init__(addresses, value(key_parameter))
