"""Caducus CLI entrypoint."""

from __future__ import annotations

import argparse
import sys


def main() -> int:
    """Entry point for the caducus command."""
    parser = argparse.ArgumentParser(prog="caducus", description="Collect ops events and run analysis.")
    sub = parser.add_subparsers(dest="command", required=True)

    demo = sub.add_parser("demo", help="Demo dataset commands")
    demo_sub = demo.add_subparsers(dest="demo_command", required=True)

    demo_ingest = demo_sub.add_parser("ingest", help="Ingest a local demo dataset file into canonical events")
    demo_ingest.add_argument("--input", required=True, help="Path to demo dataset file (e.g. CSV or JSONL)")
    demo_ingest.add_argument("--data-dir", default="./caducus-data", help="Caducus data directory")
    demo_ingest.set_defaults(func=cmd_demo_ingest)

    demo_run = demo_sub.add_parser("run", help="Ingest demo file and run analysis for one group")
    demo_run.add_argument("--input", required=True, help="Path to demo dataset file")
    demo_run.add_argument("--group-id", required=True, help="Group ID to analyze")
    demo_run.add_argument("--data-dir", default=None, help="Caducus data directory")
    demo_run.add_argument("--config", action="append", default=[], metavar="KEY=VALUE", help="Config override (repeatable)")
    demo_run.add_argument("--configuration", action="append", default=[], metavar="FILE", dest="config_files", help="Config YAML file (repeatable)")
    demo_run.set_defaults(func=cmd_demo_run)

    analyze_p = sub.add_parser("analyze", help="Run reinforcement-memory analysis for a group")
    analyze_p.add_argument("--group-id", required=True, help="Group ID to analyze")
    analyze_p.add_argument("--data-dir", default=None, help="Caducus data directory")
    analyze_p.add_argument("--config", action="append", default=[], metavar="KEY=VALUE", help="Config override (repeatable)")
    analyze_p.add_argument("--configuration", action="append", default=[], metavar="FILE", dest="config_files", help="Config YAML file (repeatable)")
    analyze_p.set_defaults(func=cmd_analyze)

    groups_p = sub.add_parser("groups", help="List group IDs that have events (use after ingest)")
    groups_p.add_argument("--data-dir", default="./caducus-data", help="Caducus data directory")
    groups_p.set_defaults(func=cmd_groups)

    args = parser.parse_args()
    if hasattr(args, "func"):
        return args.func(args)
    return 0


def cmd_demo_ingest(args: argparse.Namespace) -> int:
    """Ingest demo dataset file into canonical events."""
    from caducus.collectors.demo_dataset import ingest_demo_file
    from caducus.storage import get_events_table

    table = get_events_table(args.data_dir)
    count = ingest_demo_file(args.input, table)
    print(f"Ingested {count} events into {args.data_dir}")
    return 0


def _load_config(args: argparse.Namespace) -> tuple[dict, str]:
    """Load merged config and resolve data_dir. Returns (config, data_dir)."""
    from caducus.config import get_data_dir, load_config

    config = load_config(
        config_file_paths=getattr(args, "config_files", None) or [],
        overrides=getattr(args, "config", None) or [],
    )
    data_dir = get_data_dir(config, getattr(args, "data_dir", None))
    return config, data_dir


def cmd_demo_run(args: argparse.Namespace) -> int:
    """Ingest demo file and run analysis for one group."""
    from caducus.biblicus_adapter import run_analysis_for_group
    from caducus.collectors.demo_dataset import ingest_demo_file
    from caducus.storage import get_events_table

    config, data_dir = _load_config(args)
    table = get_events_table(data_dir)
    ingest_demo_file(args.input, table)
    run_analysis_for_group(data_dir, args.group_id, table, config=config)
    return 0


def cmd_analyze(args: argparse.Namespace) -> int:
    """Run analysis for a group from stored canonical events."""
    from caducus.biblicus_adapter import run_analysis_for_group
    from caducus.storage import get_events_table

    config, data_dir = _load_config(args)
    table = get_events_table(data_dir)
    run_analysis_for_group(data_dir, args.group_id, table, config=config)
    return 0


def cmd_groups(args: argparse.Namespace) -> int:
    """List group IDs that have events in storage."""
    from caducus.storage import get_events_table, list_group_ids

    table = get_events_table(args.data_dir)
    group_ids = list_group_ids(table)
    if not group_ids:
        print("No groups found. Run 'caducus demo ingest' first.")
        return 0
    for gid in group_ids:
        print(gid)
    return 0


if __name__ == "__main__":
    sys.exit(main())
