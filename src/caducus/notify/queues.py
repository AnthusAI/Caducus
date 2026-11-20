"""Per-channel queues. Routing writes to them; each channel's sender reads one."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Tuple


class Queues:
    def send(self, channel: str, body: Mapping[str, Any]) -> None:
        raise NotImplementedError


class MemoryQueues(Queues):
    def __init__(self) -> None:
        self.sent: List[Tuple[str, Dict[str, Any]]] = []

    def send(self, channel: str, body: Mapping[str, Any]) -> None:
        self.sent.append((channel, dict(body)))


class SqsQueues(Queues):
    """`urls` maps a channel name to its queue URL."""

    def __init__(self, client: Any, urls: Mapping[str, str]):
        self._client = client
        self._urls = dict(urls)

    def send(self, channel: str, body: Mapping[str, Any]) -> None:
        self._client.send_message(
            QueueUrl=self._urls[channel], MessageBody=json.dumps(dict(body))
        )
