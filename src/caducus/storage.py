"""Virtuus-backed storage for Caducus canonical events."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

from virtuus import Database

from caducus.events import CanonicalEvent

if TYPE_CHECKING:
    from virtuus._python.table import Table

EVENTS_SCHEMA = {
    "tables": {
        "events": {
            "primary_key": "id",
            "directory": "events",
            "storage": "index_only",
            "gsis": {
                "by_group": {
                    "partition_key": "group_id",
                    "sort_key": "timestamp",
                },
            },
        },
    },
}


def get_events_table(data_dir: str) -> "Table":
    """Return the Virtuus events table for the given data root."""
    db = Database.from_schema_dict(EVENTS_SCHEMA, data_root=data_dir)
    return db.tables["events"]


def put_events(table: "Table", events: list[CanonicalEvent]) -> int:
    """Write canonical events to the table. Returns count written."""
    for ev in events:
        table.put(ev.to_dict())
    return len(events)


def get_events_for_group(table: "Table", group_id: str) -> list[dict[str, Any]]:
    """Return all events for a group_id, ordered by timestamp ascending."""
    return table.query_gsi("by_group", group_id, None, descending=False)


def list_group_ids(table: "Table") -> list[str]:
    """Return sorted distinct group_id values present in the events table."""
    seen: set[str] = set()
    for row in table.scan():
        gid = row.get("group_id")
        if gid:
            seen.add(gid)
    return sorted(seen)
