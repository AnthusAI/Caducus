"""Thin adapter over Biblicus reinforcement memory for Caducus analysis."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from virtuus._python.table import Table

try:
    from biblicus.analysis.reinforcement_memory import (
        LocalVectorStore,
        ReinforcementMemory,
        TimestampedText,
        hash_embedder,
    )
except ImportError as e:
    raise ImportError(
        "Caducus analysis requires biblicus with reinforcement-memory. "
        "Install with: pip install 'biblicus[reinforcement-memory]' or pip install -e ../Biblicus"
    ) from e

from caducus.storage import get_events_for_group


def _normalize_temporal_signal(lifecycle_tier: str) -> str:
    """Map Biblicus lifecycle tiers to stable operator-facing recency labels."""
    value = (lifecycle_tier or "").strip().lower()
    if value in {"new", "emerging"}:
        return "new"
    if value in {"trending", "active"}:
        return "trending"
    return "known"


def _events_to_timestamped_text(rows: list[dict[str, Any]], group_id: str) -> list[TimestampedText]:
    """Map Caducus canonical event rows to Biblicus TimestampedText."""
    out = []
    for r in rows:
        meta = dict(r.get("metadata") or {})
        meta["source"] = r.get("source", "")
        meta["group_id"] = r.get("group_id", group_id)
        out.append(
            TimestampedText(
                id=r["id"],
                group_id=group_id,
                timestamp=r.get("timestamp", ""),
                text=r.get("text", ""),
                metadata=meta,
            )
        )
    return out


def _analysis_dirs_from_config(
    data_dir: str, config: dict[str, Any] | None
) -> tuple[str, str]:
    """
    Resolve analysis_dir and vector_dir from Caducus data_dir and optional biblicus config.
    Returns (analysis_dir, vector_dir).
    """
    analysis_dir = os.path.join(data_dir, "analysis")
    vector_dir = os.path.join(analysis_dir, "vectors")
    if config:
        rm = config.get("biblicus", {}).get("reinforcement_memory") or {}
        if rm.get("data_dir"):
            analysis_dir = rm["data_dir"]
            vector_dir = os.path.join(analysis_dir, "vectors")
        vs = rm.get("vector_store")
        if isinstance(vs, dict) and vs.get("kind") == "local" and vs.get("path"):
            vector_dir = vs["path"]
    return analysis_dir, vector_dir


def run_analysis_for_group(
    data_dir: str,
    group_id: str,
    table: "Table",
    config: dict[str, Any] | None = None,
) -> None:
    """
    Load canonical events for the group, run Biblicus reinforcement-memory analysis,
    and print structured topic output to stdout.

    Config may contain a biblicus.reinforcement_memory subtree with data_dir and
    vector_store (e.g. { kind: local, path: ... }). If absent, defaults are used.
    """
    analysis_dir, vector_dir = _analysis_dirs_from_config(data_dir, config)
    Path(analysis_dir).mkdir(parents=True, exist_ok=True)
    Path(vector_dir).mkdir(parents=True, exist_ok=True)

    memory = ReinforcementMemory(
        data_dir=analysis_dir,
        vector_store=LocalVectorStore(vector_dir),
        embed=hash_embedder(),
    )

    rows = get_events_for_group(table, group_id)
    if not rows:
        print(f"No events found for group_id={group_id}")
        return

    texts = _events_to_timestamped_text(rows, group_id)
    memory.ingest(texts)
    result = memory.analyze(group_id=group_id)

    print(f"Group: {result.group_id}  Texts: {result.texts_analyzed}  Run: {result.run_id}")
    for t in result.topics:
        temporal = _normalize_temporal_signal(t.lifecycle_tier)
        line = (
            f"  {t.label}  "
            f"[weight={t.memory_tier} temporal={temporal}]  "
            f"n={t.member_count}"
        )
        if t.root_cause:
            line += f"  cause: {t.root_cause[:60]}..."
        print(line)
