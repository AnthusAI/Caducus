#!/usr/bin/env python3
"""
Download a subset of the HDFS_v1 log dataset from Hugging Face and save as CSV.

Real HDFS rows use component names like dfs.DataNode$DataXceiver; group IDs are
hdfs-demo:<component>. After ingest, run `caducus groups --data-dir ./caducus-data`
to list valid group IDs, then run analyze with one of them.

Example:

    pip install datasets
    python scripts/download_hdfs_demo.py --output demo_data/hdfs_sample.csv --max-rows 10000
    caducus demo ingest --input demo_data/hdfs_sample.csv --data-dir ./caducus-data
    caducus groups --data-dir ./caducus-data
    caducus analyze --group-id "hdfs-demo:dfs.DataNode$DataXceiver" --data-dir ./caducus-data

Dataset: https://huggingface.co/datasets/logfit-project/HDFS_v1
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


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
    args = parser.parse_args()

    try:
        from datasets import load_dataset
    except ImportError:
        print("Install the datasets package: pip install datasets")
        return 1

    print(f"Loading logfit-project/HDFS_v1 (first {args.max_rows} rows)...")
    ds = load_dataset("logfit-project/HDFS_v1", split="train")
    subset = ds.select(range(min(args.max_rows, len(ds))))

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    columns = ["date", "time", "level", "component", "pid", "content", "block_id", "anomaly"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in subset:
            writer.writerow({k: str(row.get(k) or "") for k in columns})

    print(f"Wrote {len(subset)} rows to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
