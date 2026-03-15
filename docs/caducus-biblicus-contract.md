# Caducus to Biblicus Integration Contract

This document defines the integration boundary between Caducus and Biblicus reinforcement memory so Caducus can implement a thin adapter without depending on Biblicus internals.

## 1. Input Shape

Caducus converts canonical events into Biblicus `TimestampedText` using the following mapping.

### Canonical event schema (Caducus-owned)

Each collected event is stored with at least:

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Stable unique identifier (e.g. UUID or source_event_id). |
| `timestamp` | string | ISO 8601 event time. |
| `source` | string | Collector/source identifier (e.g. `cloudwatch`, `sqs-dlq`, `alerts`). |
| `group_id` | string | Analysis partition; may be defaulted from source + logical id. |
| `text` | string | Normalized text used for clustering and display. |
| `metadata` | object | Generalized key-value metadata from the source. |

### Mapping to Biblicus TimestampedText

| Caducus field | Biblicus TimestampedText field | Notes |
|---------------|--------------------------------|-------|
| `event.id` | `id` | Pass through unchanged. |
| `event.group_id` | `group_id` | Use resolved group_id (config override or default). |
| `event.timestamp` | `timestamp` | ISO 8601 string. |
| `event.text` | `text` | Primary content for embedding and clustering. |
| `event.metadata` (plus source/group_id) | `metadata` | Preserve full metadata; Caducus may add `source`, `group_id` for traceability. |

### Text field

- The canonical field used for analysis is `text`. Collectors are responsible for producing a short, representative event text (e.g. log line, alert message, or DLQ body excerpt).
- Raw payloads or long content may be stored in `metadata` or a separate field but must not replace `text` for analysis.

### Pre-filtering

- Caducus does not pre-filter events by value before analysis in the MVP. All ingested events for a group are passed to Biblicus unless a future filter is explicitly configured.
- Deduplication is done at collection time via event `id`; Caducus does not send duplicate `id`s to Biblicus.

---

## 2. Config Boundary

A single Caducus config tree is used. The `biblicus` subtree is passed through to Biblicus without schema duplication.

### Caducus-owned config

| Key / area | Purpose |
|------------|---------|
| `data_dir` | Root for Caducus canonical event storage and, if desired, analysis subdir. |
| `collectors` | Per-collector config (enabled, source ids, credentials, group_id override). |
| `group_id` | Default grouping strategy or per-collector overrides. |
| Checkpoint / CDC | Collector state (e.g. last token, last poll time) — location and keys TBD in collector tasks. |
| CLI | Command-specific options (e.g. which group to analyze, output format). |

### Biblicus-owned config (pass-through)

The `biblicus` subtree is passed as-is when constructing Biblicus components. Caducus does not redefine or validate its shape. Expected concepts (from Biblicus public API and docs) include:

- `reinforcement_memory.data_dir` — Biblicus Virtuus root (texts, topics, runs).
- `reinforcement_memory.vector_store` — e.g. `kind: local` with `path`, or S3 config.
- `reinforcement_memory.embed` — embedding model/cache config.
- `reinforcement_memory.label` / `infer_cause` / `synthesize_cause` — optional LLM hooks.
- Any other keys Biblicus documents; Caducus forwards them.

### Precedence

1. Caducus YAML (merged: home, then local, then explicit `--configuration` paths).
2. Environment variables (e.g. `CADUCUS_*`, `BIBLICUS_*`, `OPENAI_API_KEY`). Env wins over YAML for values that are explicitly resolved from env.
3. CLI `--config key=value` (dotted). Overrides apply to the merged tree, including keys under `biblicus.*`.

Biblicus env interpolation inside YAML (e.g. `{{ VAR|default }}`) is applied when Caducus loads the config; the resulting values are then passed into the Biblicus adapter.

---

## 3. Output Shape

### What Caducus consumes from Biblicus

- **AnalysisResult** — Returned by `ReinforcementMemory.analyze(group_id=...)`. Contains `group_id`, `topics` (list of `TopicResult`), `texts_analyzed`, `cluster_version`, `run_id`.
- **TopicResult** — For each topic: `topic_id`, `label`, `keywords`, `exemplars`, `member_count`, `memory_weight`, `memory_tier`, `lifecycle_tier`, `is_new`, `is_trending`, `days_inactive`, `root_cause`.
- **Persisted artifacts** — Biblicus writes texts/topics/runs under its `data_dir`; vector store writes to its configured path. Caducus may read these for “inspect” commands but does not depend on their internal format for correctness.

### Stable user-facing shape (Caducus CLI)

Caducus exposes analysis in a stable, minimal form:

- **Topic line**: label, memory_tier, lifecycle_tier, member_count, optional root_cause.
- **List view**: one line per topic with key fields.
- **Detail view**: full topic (keywords, exemplars, root_cause) when requested.

Fields treated as stable for MVP: `label`, `keywords`, `exemplars`, `member_count`, `memory_weight`, `memory_tier`, `lifecycle_tier`, `root_cause`. Caducus does not expose raw `topic_id` in user-facing output unless needed; it may be used internally for ordering or reference.

---

## 4. Storage Ownership

### Caducus owns

- **Canonical event storage** — Virtuus (or equivalent) table of events keyed by `id`, with GSIs as needed (e.g. by `group_id`, timestamp, source). Path: under `data_dir` (e.g. `data_dir/events/`).
- **Collector checkpoints / CDC state** — Per-collector state (e.g. last CloudWatch token, last SQS receipt, last alert id). Path: under `data_dir` (e.g. `data_dir/collectors/<collector_id>/state` or similar). Exact schema is defined in collector tasks.
- **Collector run metadata** — Optional run logs or timestamps for “last collect” per collector.

### Biblicus owns

- **Reinforcement-memory state** — `data_dir` (or a dedicated subdir such as `data_dir/analysis/`) passed to `ReinforcementMemory`: Virtuus `texts/`, `topics/`, `runs/`.
- **Vector store artifacts** — Local directory or S3 Vectors; configured via the `biblicus` config subtree.

### Derived vs source of truth

- **Canonical event storage is the source of truth.** Caducus writes events once; the same events (or a filtered set) are passed to Biblicus for analysis. Biblicus analysis state is derived and can be recreated by re-running analysis on the same event set.
- Caducus does not mirror event content into a second store for Biblicus; the adapter reads from Caducus event storage and calls `ReinforcementMemory.ingest(TimestampedText(...))` (and then `analyze`) so that Biblicus persists its own texts/topics/runs. Event identity is preserved via `id` so re-ingestion is idempotent where Biblicus supports it.

---

## 5. Scope Isolation

### group_id

- **Default derivation**: `group_id` = `{source_type}:{logical_id}` (e.g. `cloudwatch:/aws/lambda/auth`, `sqs-dlq:my-dlq-name`). Config may override per collector.
- **Uniqueness**: Each distinct `group_id` is analyzed independently; Caducus passes one `group_id` per `analyze()` call.

### Vector store and analysis scope

- **MVP**: Use one Biblicus `ReinforcementMemory` instance per Caducus process (or per “analysis run”). The vector store is shared across groups in that process; Biblicus’s current implementation clears the vector store on each `analyze()` for the group being analyzed, so Caducus will invoke `analyze()` once per group and will not rely on multiple groups’ vectors coexisting in one store in the same run. For MVP, “one group per analyze run” is the rule; batching multiple groups in one run is out of scope until Biblicus supports non-destructive per-group vector scope.
- **Data dir**: Caducus may pass a single `data_dir` for Biblicus (e.g. `data_dir/analysis`) so that all groups’ texts/topics/runs live under one tree, with Biblicus partitioning by `group_id` internally.

### Summary

- **One analysis run** = one or more `analyze(group_id)` calls, each for a single `group_id`.
- **Vector store**: One store per process/run; one group analyzed at a time for MVP.
- **Config**: `group_id` default = `source_type:logical_id`; overridable per collector.

---

## Adapter Responsibilities (summary)

The Caducus adapter layer will:

1. **Build TimestampedText** from canonical events using the mapping in §1.
2. **Construct ReinforcementMemory** from Caducus config: resolve `data_dir`, vector store, embed, and optional LLM callables from the `biblicus` subtree (and env).
3. **Call** `memory.ingest(texts)` then `memory.analyze(group_id=...)` per group.
4. **Map** `AnalysisResult` to the stable CLI output shape in §3.

Caducus does not change Biblicus’s persistence layout or public API; it only feeds input and reads output according to this contract.
