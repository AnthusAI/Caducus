"""Heartbeat metrics: the operator's dead-man's switch reads these."""

from __future__ import annotations

from typing import Any, List

HEARTBEAT_KIND = "caducus.heartbeat"
HEARTBEAT_DELIVERED = "HeartbeatDelivered"
HEARTBEAT_SEND_FAILED = "HeartbeatSendFailed"


class Metrics:
    def count(self, name: str) -> None:
        raise NotImplementedError


class MemoryMetrics(Metrics):
    def __init__(self) -> None:
        self.counted: List[str] = []

    def count(self, name: str) -> None:
        self.counted.append(name)


class CloudWatchMetrics(Metrics):
    """Adds one to a metric in `namespace`. The host chooses the namespace."""

    def __init__(self, client: Any, namespace: str):
        self._client = client
        self._namespace = namespace

    def count(self, name: str) -> None:
        self._client.put_metric_data(
            Namespace=self._namespace,
            MetricData=[{"MetricName": name, "Value": 1.0, "Unit": "Count"}],
        )
