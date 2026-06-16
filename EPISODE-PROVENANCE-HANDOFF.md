# Episode Provenance Handoff

Branch: `codex/episode-provenance`

Purpose: make Coral sessions inspectable as first-class local episodes. Each
query or catalog call writes a small local receipt. Coral then exposes those
receipts back through SQL as `coral_provenance.*` tables, so agents can inspect
recent calls, entities, and input/output continuity without a cloud dependency.

## What changed

- Added local provenance receipts in `crates/coral-app/src/provenance.rs`.
- Added a runtime-table extension point in `coral-engine` so app-owned tables
  can be registered into a single DataFusion runtime.
- Recorded SQL execute/explain calls from `QueryManager`.
- Recorded successful catalog list/search/describe/list-columns calls from
  `CatalogService`.
- Documented the local provenance tables in the OpenTelemetry guide.

## Data model

Raw storage is append-only JSONL:

`<coral config dir>/telemetry/provenance/events.jsonl`

The JSONL records are the local receipts. The SQL tables are read models built
from that file at runtime:

- `coral_provenance.episodes`
- `coral_provenance.calls`
- `coral_provenance.occurrences`
- `coral_provenance.bindings`

An episode is currently the active OpenTelemetry trace ID. If no valid trace is
available, the recorder falls back to a local process episode ID.

A call has:

- `call_id`
- `episode_id`
- `workspace`
- `operation`
- `status`
- timing
- optional row count
- input JSON
- output summary JSON

An occurrence is a scalar value or named entity observed in a call input or
output. Schema/table/column entities keep readable keys such as
`table:github.pull_requests`. Scalar values use stable SHA-256 keys, with a
short preview for inspection.

A binding links a later input occurrence to the nearest earlier output
occurrence in the same episode with the same `entity_key`.

Current evidence kind:

`nearest_earlier_output_value`

This is continuity evidence. It is intentionally recorded as a concrete binding
between observed values, not as a broad claim that the whole call caused the
later call.

## Important behavior

- Provenance tables are only injected into the query runtime when needed.
- Normal unfiltered catalog listings do not include `coral_provenance`.
- Explicit `coral_provenance` catalog lookups can discover the tables.
- SQL over `coral_provenance` is recorded as a call, but its result rows are not
  re-ingested as output occurrences. This avoids self-feeding provenance loops.
- Query result output occurrence extraction is capped at 512 scalar values per
  successful SQL call.
- Catalog calls currently record successful responses. Query calls record both
  success and error summaries.

## Useful SQL

Recent episodes:

```sql
select *
from coral_provenance.episodes
order by last_call_unix_nanos desc;
```

Recent calls in an episode:

```sql
select *
from coral_provenance.calls
where episode_id = '<episode id>'
order by started_at_unix_nanos;
```

Continuity edges:

```sql
select
  b.episode_id,
  b.source_call_id,
  b.target_call_id,
  b.entity_key,
  src.path as source_path,
  dst.path as target_path,
  dst.value_preview
from coral_provenance.bindings b
join coral_provenance.occurrences src
  on src.occurrence_id = b.source_occurrence_id
join coral_provenance.occurrences dst
  on dst.occurrence_id = b.target_occurrence_id
order by b.observed_at_unix_nanos;
```

## Key files

- `crates/coral-app/src/provenance.rs`
- `crates/coral-app/src/query/manager.rs`
- `crates/coral-app/src/catalog/service.rs`
- `crates/coral-app/src/state/layout.rs`
- `crates/coral-engine/src/composition.rs`
- `crates/coral-engine/src/runtime/query.rs`
- `docs/guides/observe-with-opentelemetry.mdx`

## Verification run

These checks passed on this branch:

- `cargo test -p coral-app`
- `cargo test -p coral-mcp`
- `cargo test -p coral-engine`
- `make rust-checks` progressed through fmt, clippy, and nextest successfully.
  The shell session disappeared during the final docs phase, so docs were
  re-run explicitly with the command below.
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked`
- `make docs-check`
- `git diff --check`

## Review notes

This branch is larger than the preferred ~400 LOC PR size. If we want to split
it before review, the clean split is:

1. Engine runtime-table extension point.
2. App provenance recorder and SQL read models.
3. Query/catalog instrumentation plus docs.

The current single-branch shape is useful for testing the full loop end to end.

## Known limits and follow-ups

- SQL input extraction is deliberately lightweight: string literals plus
  `FROM`/`JOIN` table references. It does not yet use DataFusion planner lineage.
- Output extraction records scalar result values, not row fingerprints or
  column-level row continuity.
- Catalog errors are not recorded yet because the current instrumentation records
  after successful catalog response construction.
- JSONL writes use a per-process mutex, not a cross-process file lock.
- There is no retention, compaction, or corruption repair beyond skipping
  malformed JSONL lines.
- Value hashes are good local correlation keys. They are not a privacy boundary
  for low-entropy values.
- Episode quality depends on trace propagation. The local fallback is good for
  tests and simple processes, but not a durable cross-process episode model.

## Suggested next work

- Exercise this through MCP with real GitHub/Linear/Slack/Datadog workflows and
  inspect whether the bindings are useful without additional planner lineage.
- Add a tiny runbook of canonical provenance queries for agents.
- Decide whether row fingerprints are needed for column-to-row continuity.
- If planner lineage is still interesting, add it as a second evidence source
  rather than replacing the current observed-value bindings.
- Add retention/file-locking if this becomes a default always-on local feature.
