#!/usr/bin/env python3
"""
Download a subset of the HDFS_v1 log dataset from Hugging Face and save as CSV.

Real HDFS rows use component names like dfs.DataNode$DataXceiver; group IDs are
hdfs-demo:<component>. After ingest, run `caducus groups --data-dir ./caducus-data`
to list valid group IDs, then run analyze with one of them.

Example:

    pip install datasets
    python scripts/download_hdfs_demo.py --output demo_data/hdfs_sample.csv --max-rows 10000 --anchor-now "2026-03-16T12:00:00Z"
    caducus demo ingest --input demo_data/hdfs_sample.csv --data-dir ./caducus-data
    caducus groups --data-dir ./caducus-data
    caducus analyze --group-id "hdfs-demo:dfs.DataNode$DataXceiver" --data-dir ./caducus-data

Dataset: https://huggingface.co/datasets/logfit-project/HDFS_v1
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _parse_hdfs_datetime(date_str: str, time_str: str) -> datetime | None:
    """Parse HDFS YYMMDD + HHMMSS strings as UTC."""
    if not date_str or not time_str:
        return None
    try:
        y = int(date_str[:2])
        year = 2000 + y if y < 50 else 1900 + y
        month = int(date_str[2:4])
        day = int(date_str[4:6])
        hour = int(time_str[:2])
        minute = int(time_str[2:4])
        second = int(time_str[4:6]) if len(time_str) >= 6 else 0
        return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    except (ValueError, IndexError):
        return None


def _parse_anchor_now(value: str) -> datetime:
    """Parse ISO-8601 anchor timestamp and normalize to UTC."""
    iso = value.strip()
    if iso.endswith("Z"):
        iso = iso[:-1] + "+00:00"
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def main() -> int:
    parser = argparse.ArgumentParser(description="Download HDFS_v1 subset for Caducus demo")
    parser.add_argument(
        "--output",
        default="demo_data/hdfs_sample.csv",
        help="Output CSV path",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=10_000,
        help="Max rows to export (default 10000)",
    )
    parser.add_argument(
        "--anchor-now",
        default="",
        help=(
            "Optional ISO-8601 timestamp (e.g. 2026-03-16T12:00:00Z). "
            "If provided, shifts exported dates/times so the newest row aligns "
            "to this timestamp while preserving relative spacing."
        ),
    )
    args = parser.parse_args()

    try:
        from datasets import load_dataset
    except ImportError:
        print("Install the datasets package: pip install datasets")
        return 1

    print(f"Loading logfit-project/HDFS_v1 (first {args.max_rows} rows)...")
    ds = load_dataset("logfit-project/HDFS_v1", split="train")
    subset = ds.select(range(min(args.max_rows, len(ds))))
    rows = list(subset)

    delta: timedelta | None = None
    if args.anchor_now:
        anchor_now = _parse_anchor_now(args.anchor_now)
        parsed = [
            _parse_hdfs_datetime(str(r.get("date") or ""), str(r.get("time") or ""))
            for r in rows
        ]
        parsed = [p for p in parsed if p is not None]
        if parsed:
            latest = max(parsed)
            delta = anchor_now - latest

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    columns = ["date", "time", "level", "component", "pid", "content", "block_id", "anomaly"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            out_row = {k: str(row.get(k) or "") for k in columns}
            if delta is not None:
                parsed = _parse_hdfs_datetime(out_row["date"], out_row["time"])
                if parsed is not None:
                    shifted = parsed + delta
                    out_row["date"] = shifted.strftime("%y%m%d")
                    out_row["time"] = shifted.strftime("%H%M%S")
            writer.writerow(out_row)

    if delta is not None:
        print(f"Applied anchor-now shift. Newest row aligned to {args.anchor_now}")
    print(f"Wrote {len(rows)} rows to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
