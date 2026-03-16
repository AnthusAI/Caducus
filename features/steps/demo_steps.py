"""Step definitions for demo vertical slice feature."""

import csv
import os
import subprocess
from pathlib import Path

from behave import given, then, when

FIXTURES = Path(__file__).resolve().parent.parent.parent / "tests" / "fixtures"


def _run_cli(context, args: list[str]) -> None:
    cmd = ["python", "-m", "caducus.cli", *args]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=context.tmpdir)
    context.cli_returncode = result.returncode
    context.cli_stdout = result.stdout
    context.cli_stderr = result.stderr


def _events_table(context):
    from caducus.storage import get_events_table

    return get_events_table(context.data_dir)


@given("a local demo dataset file with HDFS-style log rows")
def step_demo_file(context):
    path = FIXTURES / "demo_hdfs_sample.csv"
    if not path.exists():
        raise FileNotFoundError(f"Fixture not found: {path}")
    context.demo_input_path = str(path)
    with path.open(encoding="utf-8", newline="") as handle:
        context.expected_row_count = sum(1 for _ in csv.DictReader(handle))


@given("a local demo dataset file with a custom source column")
def step_custom_source_demo_file(context):
    path = FIXTURES / "demo_source_sample.csv"
    if not path.exists():
        raise FileNotFoundError(f"Fixture not found: {path}")
    context.demo_input_path = str(path)
    with path.open(encoding="utf-8", newline="") as handle:
        context.expected_row_count = sum(1 for _ in csv.DictReader(handle))


@given("a Caducus data directory is configured")
def step_data_dir(context):
    context.data_dir = getattr(context, "data_dir", os.path.join(context.tmpdir, "data"))
    os.makedirs(context.data_dir, exist_ok=True)


@given('canonical event records already exist for group_id "{group_id}" in the data directory')
def step_events_exist(context, group_id):
    from caducus.collectors.demo_dataset import ingest_demo_file

    ingest_demo_file(context.demo_input_path, _events_table(context))
    context.group_id = group_id


@when('I run the Caducus demo run command for group_id "{group_id}"')
def step_demo_run(context, group_id):
    _run_cli(
        context,
        [
            "demo",
            "run",
            "--input",
            context.demo_input_path,
            "--group-id",
            group_id,
            "--data-dir",
            context.data_dir,
        ],
    )


@when('I run the Caducus demo run command with config for group_id "{group_id}"')
def step_demo_run_with_config(context, group_id):
    _run_cli(
        context,
        [
            "demo",
            "run",
            "--input",
            context.demo_input_path,
            "--group-id",
            group_id,
            "--configuration",
            context.config_path,
        ],
    )


@when("I run the Caducus demo ingest command with that input file")
def step_demo_ingest(context):
    _run_cli(
        context,
        ["demo", "ingest", "--input", context.demo_input_path, "--data-dir", context.data_dir],
    )


@when('I run the Caducus analyze command for group_id "{group_id}"')
def step_analyze(context, group_id):
    _run_cli(context, ["analyze", "--group-id", group_id, "--data-dir", context.data_dir])


@when("I run the Caducus groups command")
def step_groups(context):
    _run_cli(context, ["groups", "--data-dir", context.data_dir])


@then("the command succeeds")
def step_command_succeeds(context):
    assert context.cli_returncode == 0, f"CLI failed: {context.cli_stderr}"


@then("Caducus writes one canonical event per log row")
def step_one_event_per_row(context):
    all_rows = _events_table(context).scan()
    expected = getattr(context, "expected_row_count", 0)
    assert len(all_rows) == expected, (
        f"Expected {expected} events, got {len(all_rows)}. "
        f"stdout={context.cli_stdout!r} stderr={context.cli_stderr!r}"
    )


@then("each stored event has id, timestamp, source, group_id, text, and metadata")
def step_event_shape(context):
    for record in _events_table(context).scan():
        for key in ("id", "timestamp", "source", "group_id", "text", "metadata"):
            assert key in record, f"Missing {key} in {record}"


@then('the radar output includes group "{group_id}"')
def step_radar_output_group(context, group_id):
    out = context.cli_stdout
    assert f"Group: {group_id}" in out, f"Expected group header in output: {out!r}"


@then("the radar output includes ranked blips with weight, temporal, and member count")
def step_radar_output_shape(context):
    out = context.cli_stdout
    assert "n=" in out, f"Expected 'n=' in output: {out!r}"
    assert "weight=" in out, f"Expected 'weight=' in output: {out!r}"
    assert "temporal=" in out, f"Expected 'temporal=' in output: {out!r}"


@then("the CLI lists at least the group IDs from the ingested file")
def step_lists_groups(context):
    out = context.cli_stdout
    assert "hdfs-demo:DataNode" in out, f"Expected hdfs-demo:DataNode in output: {out!r}"
    assert "hdfs-demo:NameNode" in out, f"Expected hdfs-demo:NameNode in output: {out!r}"


@then('the CLI includes group_id "{group_id}"')
def step_lists_specific_group(context, group_id):
    out = context.cli_stdout
    assert group_id in out, f"Expected {group_id} in output: {out!r}"


@then('the CLI says no events were found for group_id "{group_id}"')
def step_no_events_message(context, group_id):
    out = context.cli_stdout
    assert f"No events found for group_id={group_id}" in out, (
        f"Expected no-events message for {group_id}. Output: {out!r}"
    )


@given("a Caducus YAML config file sets data_dir and biblicus reinforcement-memory paths")
def step_yaml_config(context):
    configured_data_dir = os.path.join(context.tmpdir, "configured-data")
    configured_analysis_dir = os.path.join(context.tmpdir, "configured-analysis")
    configured_vector_dir = os.path.join(configured_analysis_dir, "vectors-local")
    config_path = os.path.join(context.tmpdir, "caducus.yaml")
    contents = (
        f"data_dir: {configured_data_dir}\n"
        "biblicus:\n"
        "  reinforcement_memory:\n"
        f"    data_dir: {configured_analysis_dir}\n"
        "    vector_store:\n"
        "      kind: local\n"
        f"      path: {configured_vector_dir}\n"
    )
    Path(config_path).write_text(contents, encoding="utf-8")
    context.config_path = config_path
    context.configured_data_dir = configured_data_dir
    context.configured_analysis_dir = configured_analysis_dir
    context.configured_vector_dir = configured_vector_dir


@then("the configured Caducus data_dir contains canonical events")
def step_configured_data_dir_has_events(context):
    from caducus.storage import get_events_table

    table = get_events_table(context.configured_data_dir)
    rows = table.scan()
    assert rows, f"Expected events in configured data_dir {context.configured_data_dir}"


@then("the configured Biblicus analysis and vector directories exist")
def step_configured_analysis_dirs_exist(context):
    assert Path(context.configured_analysis_dir).exists(), (
        f"Missing analysis_dir {context.configured_analysis_dir}"
    )
    assert Path(context.configured_vector_dir).exists(), (
        f"Missing vector_dir {context.configured_vector_dir}"
    )
