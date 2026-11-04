"""The alarm catalog: what each known alarm means and what to do about it."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, List, Mapping, Optional, Pattern

from .files import load_mapping

DEFAULT_HOLD_DOWN_SECONDS = 300


@dataclass(frozen=True)
class CatalogEntry:
    pattern: Pattern[str]
    product: str
    what: str
    impact: str
    next_step: str
    hold_down_seconds: Optional[int] = None


@dataclass(frozen=True)
class CatalogMatch:
    entry: CatalogEntry
    site: Optional[str]


class Catalog:
    def __init__(self, entries: List[CatalogEntry], hold_down_seconds: int):
        self._entries = entries
        self._default_hold_down = hold_down_seconds

    def match(self, alarm_name: str) -> Optional[CatalogMatch]:
        for entry in self._entries:
            found = entry.pattern.match(alarm_name)
            if found:
                site = found.groupdict().get("site")
                return CatalogMatch(entry=entry, site=site or None)
        return None

    def hold_down_seconds(self, alarm_name: str) -> int:
        found = self.match(alarm_name)
        if found and found.entry.hold_down_seconds is not None:
            return found.entry.hold_down_seconds
        return self._default_hold_down


def catalog_from_mapping(data: Mapping[str, Any]) -> Catalog:
    entries = []
    for raw in data.get("alarms") or []:
        entries.append(
            CatalogEntry(
                pattern=re.compile(raw["pattern"]),
                product=raw["product"],
                what=raw["what"],
                impact=raw["impact"],
                next_step=raw["next"],
                hold_down_seconds=raw.get("holdDownSeconds"),
            )
        )
    default = (data.get("defaults") or {}).get(
        "holdDownSeconds", DEFAULT_HOLD_DOWN_SECONDS
    )
    return Catalog(entries, int(default))


def load_catalog(path: str) -> Catalog:
    return catalog_from_mapping(load_mapping(path))
