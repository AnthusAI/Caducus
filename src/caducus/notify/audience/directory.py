"""A directory that also knows the public audience's readers by their recipient key."""

from __future__ import annotations

from typing import List, Optional

from ..directory import ChannelAccount, Directory, Recipient
from .service import PublicAudience


class AudienceDirectory(Directory):
    """The host's directory (operators, probe, accounts) plus the readers in the audience."""

    def __init__(self, base: Directory, audience: Optional[PublicAudience] = None):
        self._base = base
        self.audience = audience

    def operators(self) -> List[Recipient]:
        return self._base.operators()

    def probe(self) -> Optional[Recipient]:
        return self._base.probe()

    def account(self, key: str) -> Optional[ChannelAccount]:
        return self._base.account(key)

    def get(self, key: str) -> Optional[Recipient]:
        found = self._base.get(key)
        if found is not None or self.audience is None:
            return found
        address = self.audience.address_of(key)
        return Recipient(key, address) if address else None
