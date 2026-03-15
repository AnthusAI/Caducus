"""Demo dataset collector: HDFS-style log rows -> canonical events."""

from __future__ import annotations

import csv
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

from caducus.events import CanonicalEvent

if TYPE_CHECKING:
    from virtuus._python.table import Table

SOURCE_ID = "hdfs-demo"


def _parse_timestamp(date_str: str, time_str: str) -> str:
    """Turn YYMMDD and HHMMSS into ISO 8601. Assumes 20xx for year."""
    if not date_str or not time_str:
        return ""
    try:
        y, m, d = int(date_str[:2]), int(date_str[2:4]), int(date_str[4:6])
        year = 2000 + y if y < 50 else 1900 + y
        h = int(time_str[:2])
        mi = int(time_str[2:4])
        s = int(time_str[4:6]) if len(time_str) >= 6 else 0
        return f"{year:04d}-{m:02d}-{d:02d}T{h:02d}:{mi:02d}:{s:02d}Z"
    except (ValueError, IndexError):
        return ""


def _row_to_event(row: dict[str, Any], index: int) -> CanonicalEvent:
    """Map one HDFS-style row to a canonical event."""
    date_str = str(row.get("date", ""))
    time_str = str(row.get("time", ""))
    content = str(row.get("content", ""))
    component = str(row.get("component", "unknown")).strip() or "unknown"
    event_id = row.get("id") or str(uuid.uuid4())
    timestamp = _parse_timestamp(date_str, time_str)
    group_id = f"{SOURCE_ID}:{component}"
    metadata: dict[str, Any] = {
        "level": row.get("level"),
        "pid": row.get("pid"),
        "block_id": row.get("block_id"),
        "anomaly": row.get("anomaly"),
    }
    return CanonicalEvent(
        id=event_id,
        timestamp=timestamp,
        source=SOURCE_ID,
        group_id=group_id,
        text=content,
        metadata={k: v for k, v in metadata.items() if v is not None},
    )


def _read_csv(path: Path) -> Iterator[dict[str, Any]]:
    """Read CSV with headers. Normalize keys to lowercase."""
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield {k.strip().lower(): v for k, v in row.items() if k}


def ingest_demo_file(input_path: str, table: "Table") -> int:
    """
    Read a demo dataset file (CSV with HDFS-style columns) and write canonical events.

    Expected columns: date, time, level, component, pid, content, block_id, anomaly.
    """
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Demo file not found: {input_path}")

    events: list[CanonicalEvent] = []
    for i, row in enumerate(_read_csv(path)):
        ev = _row_to_event(row, i)
        events.append(ev)

    from caducus.storage import put_events

    return put_events(table, events)
