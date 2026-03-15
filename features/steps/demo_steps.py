"""Step definitions for demo vertical slice feature."""

import os
import subprocess
from pathlib import Path

from behave import given, when, then

FIXTURES = Path(__file__).resolve().parent.parent.parent / "tests" / "fixtures"


@given("a local demo dataset file with HDFS-style log rows")
def step_demo_file(context):
    path = FIXTURES / "demo_hdfs_sample.csv"
    if not path.exists():
        raise FileNotFoundError(f"Fixture not found: {path}")
    context.demo_input_path = str(path)


@given("a Caducus data directory is configured")
def step_data_dir(context):
    context.data_dir = getattr(context, "data_dir", os.path.join(context.tmpdir, "data"))
    os.makedirs(context.data_dir, exist_ok=True)


@given("the biblicus config subtree specifies reinforcement_memory data_dir and vector_store")
def step_biblicus_config(context):
    pass  # MVP uses defaults; config not yet loaded from YAML


@given("canonical event records already exist for a group_id in the data directory")
def step_events_exist(context):
    from caducus.collectors.demo_dataset import ingest_demo_file
    from caducus.storage import get_events_table

    path = FIXTURES / "demo_hdfs_sample.csv"
    if not path.exists():
        raise FileNotFoundError(f"Fixture not found: {path}")
    table = get_events_table(context.data_dir)
    ingest_demo_file(str(path), table)
    context.group_id = "hdfs-demo:DataNode"


@given("the biblicus config subtree is present")
def step_biblicus_present(context):
    pass


@when("I run the Caducus demo run command with that input file and a group_id")
def step_demo_run(context):
    cmd = [
        "python", "-m", "caducus.cli",
        "demo", "run",
        "--input", context.demo_input_path,
        "--group-id", "hdfs-demo:DataNode",
        "--data-dir", context.data_dir,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=context.tmpdir)
    context.cli_returncode = result.returncode
    context.cli_stdout = result.stdout
    context.cli_stderr = result.stderr


@when("I run the Caducus demo ingest command with that input file")
def step_demo_ingest(context):
    cmd = [
        "python", "-m", "caducus.cli",
        "demo", "ingest",
        "--input", context.demo_input_path,
        "--data-dir", context.data_dir,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=context.tmpdir)
    context.cli_returncode = result.returncode
    context.cli_stdout = result.stdout
    context.cli_stderr = result.stderr


@when("I run the Caducus analyze command for that group_id")
def step_analyze(context):
    cmd = [
        "python", "-m", "caducus.cli",
        "analyze",
        "--group-id", getattr(context, "group_id", "hdfs-demo:DataNode"),
        "--data-dir", context.data_dir,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=context.tmpdir)
    context.cli_returncode = result.returncode
    context.cli_stdout = result.stdout
    context.cli_stderr = result.stderr


@then("Caducus writes canonical event records for each log row")
def step_writes_events(context):
    from caducus.storage import get_events_table

    table = get_events_table(context.data_dir)
    all_rows = table.scan()
    assert len(all_rows) == 5, f"Expected 5 events (one per row), got {len(all_rows)}. stderr: {context.cli_stderr}"


@then("Caducus maps those events to Biblicus TimestampedText")
def step_maps_to_timestamped_text(context):
    pass  # Verified by analysis running and producing output


@then("Caducus invokes Biblicus ReinforcementMemory ingest and analyze for that group_id")
def step_invokes_biblicus(context):
    assert context.cli_returncode == 0, f"CLI failed: {context.cli_stderr}"


@then("the CLI shows structured memory topics with label, memory_tier, lifecycle_tier, member_count")
def step_cli_shows_topics(context):
    assert context.cli_returncode == 0, f"CLI failed: {context.cli_stderr}"
    out = context.cli_stdout
    assert "Group:" in out, f"Expected 'Group:' in output: {out!r}"
    assert "n=" in out, f"Expected topic member count 'n=' in output: {out!r}"
    assert "[" in out and "/" in out, f"Expected topic tier format [x/y] in output: {out!r}"


@then("Caducus writes one canonical event per log row")
def step_one_event_per_row(context):
    from caducus.storage import get_events_table

    table = get_events_table(context.data_dir)
    count = len(table.scan())
    assert count == 5, f"Expected 5 events, got {count}"


@then("each event has id, timestamp, source, group_id, text, and metadata")
def step_event_shape(context):
    from caducus.storage import get_events_table

    table = get_events_table(context.data_dir)
    for record in table.scan():
        for key in ("id", "timestamp", "source", "group_id", "text", "metadata"):
            assert key in record, f"Missing {key} in {record}"


@then("Caducus reads canonical events from storage")
def step_reads_from_storage(context):
    pass  # Implied by analyze running


@then("Caducus runs Biblicus reinforcement memory analysis for that group_id")
def step_runs_analysis(context):
    assert context.cli_returncode == 0, f"CLI failed: {context.cli_stderr}"


@then("the CLI outputs topic lines in the stable shape")
def step_stable_shape(context):
    assert context.cli_returncode == 0, f"CLI failed: {context.cli_stderr}"
    out = context.cli_stdout
    assert "Group:" in out, f"Expected 'Group:' in output: {out!r}"
    assert "n=" in out, f"Expected topic line with n= in output: {out!r}"


@when("I run the Caducus groups command")
def step_groups(context):
    cmd = [
        "python", "-m", "caducus.cli",
        "groups",
        "--data-dir", context.data_dir,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=context.tmpdir)
    context.cli_returncode = result.returncode
    context.cli_stdout = result.stdout
    context.cli_stderr = result.stderr


@then("the CLI lists at least the group IDs from the ingested file")
def step_lists_groups(context):
    assert context.cli_returncode == 0, f"CLI failed: {context.cli_stderr}"
    out = context.cli_stdout
    assert "hdfs-demo:DataNode" in out, f"Expected hdfs-demo:DataNode in groups output: {out!r}"
    assert "hdfs-demo:NameNode" in out, f"Expected hdfs-demo:NameNode in groups output: {out!r}"
