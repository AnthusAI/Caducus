"""What every channel provides, so Slack, Discord and a webhook can be added
without touching routing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class Outcome:
    status: str  # sent | duplicate | failed
    provider_message_id: Optional[str] = None
    reason: Optional[str] = None


class Channel:
    name = ""

    def send(self, delivery: Mapping[str, Any]) -> Outcome:
        raise NotImplementedError
