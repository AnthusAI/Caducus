"""Tests for the demo vertical slice: ingest and analysis."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from caducus.collectors.demo_dataset import ingest_demo_file
from caducus.events import CanonicalEvent
from caducus.storage import get_events_table, get_events_for_group, put_events


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def test_canonical_event_roundtrip() -> None:
    """CanonicalEvent serializes and deserializes."""
    ev = CanonicalEvent(
        id="e1",
        timestamp="2024-01-01T12:00:00Z",
        source="hdfs-demo",
        group_id="hdfs-demo:DataNode",
        text="Receiving block blk_123",
        metadata={"level": "INFO"},
    )
    d = ev.to_dict()
    ev2 = CanonicalEvent.from_dict(d)
    assert ev2.id == ev.id
    assert ev2.text == ev.text
    assert ev2.metadata == ev.metadata


def test_demo_ingest_writes_canonical_events() -> None:
    """Demo ingest reads CSV and writes one canonical event per row."""
    csv_path = FIXTURES_DIR / "demo_hdfs_sample.csv"
    if not csv_path.exists():
        pytest.skip("fixture demo_hdfs_sample.csv not found")
    with tempfile.TemporaryDirectory() as tmp:
        table = get_events_table(tmp)
        count = ingest_demo_file(str(csv_path), table)
        assert count == 5
        rows = get_events_for_group(table, "hdfs-demo:DataNode")
        assert len(rows) == 3
        rows_nn = get_events_for_group(table, "hdfs-demo:NameNode")
        assert len(rows_nn) == 2
        for r in rows + rows_nn:
            assert "id" in r and "text" in r and "timestamp" in r
            assert r["source"] == "hdfs-demo"
            assert "group_id" in r


def test_analyze_requires_biblicus() -> None:
    """Analyze path imports Biblicus; we only test that adapter imports."""
    try:
        from caducus.biblicus_adapter import _events_to_timestamped_text
    except ImportError:
        pytest.skip("biblicus reinforcement-memory not installed")
    from biblicus.analysis.reinforcement_memory import TimestampedText

    rows = [
        {
            "id": "1",
            "timestamp": "2024-01-01T12:00:00Z",
            "source": "hdfs-demo",
            "group_id": "hdfs-demo:DataNode",
            "text": "Receiving block",
            "metadata": {},
        }
    ]
    texts = _events_to_timestamped_text(rows, "hdfs-demo:DataNode")
    assert len(texts) == 1
    assert isinstance(texts[0], TimestampedText)
    assert texts[0].text == "Receiving block"


def test_groups_lists_group_ids_after_ingest() -> None:
    """After ingest, groups command returns sorted distinct group_ids."""
    csv_path = FIXTURES_DIR / "demo_hdfs_sample.csv"
    if not csv_path.exists():
        pytest.skip("fixture demo_hdfs_sample.csv not found")
    with tempfile.TemporaryDirectory() as tmp:
        table = get_events_table(tmp)
        ingest_demo_file(str(csv_path), table)
        from caducus.storage import list_group_ids

        group_ids = list_group_ids(table)
        assert "hdfs-demo:DataNode" in group_ids
        assert "hdfs-demo:NameNode" in group_ids
        assert group_ids == sorted(group_ids)


def test_demo_run_produces_stable_topic_output() -> None:
    """End-to-end: ingest fixture, run analyze for a real group, assert topic output shape."""
    try:
        from caducus.biblicus_adapter import run_analysis_for_group
    except ImportError:
        pytest.skip("biblicus reinforcement-memory not installed")
    csv_path = FIXTURES_DIR / "demo_hdfs_sample.csv"
    if not csv_path.exists():
        pytest.skip("fixture demo_hdfs_sample.csv not found")
    with tempfile.TemporaryDirectory() as tmp:
        table = get_events_table(tmp)
        ingest_demo_file(str(csv_path), table)
        import io
        import sys

        out = io.StringIO()
        old_stdout = sys.stdout
        sys.stdout = out
        try:
            run_analysis_for_group(tmp, "hdfs-demo:DataNode", table, config=None)
        finally:
            sys.stdout = old_stdout
        text = out.getvalue()
    assert "Group:" in text
    assert "Texts:" in text or "texts_analyzed" in text or "n=" in text
    assert "n=" in text
    assert "weight=" in text
    assert "temporal=" in text
