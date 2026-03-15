"""Canonical event model for Caducus."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class CanonicalEvent:
    """
    A single canonical ops event.

    Schema is defined in docs/caducus-biblicus-contract.md.
    """

    id: str
    timestamp: str  # ISO 8601
    source: str
    group_id: str
    text: str
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Serialize for storage."""
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "source": self.source,
            "group_id": self.group_id,
            "text": self.text,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CanonicalEvent":
        """Deserialize from storage."""
        return cls(
            id=data["id"],
            timestamp=data["timestamp"],
            source=data["source"],
            group_id=data["group_id"],
            text=data["text"],
            metadata=data.get("metadata", {}),
        )
