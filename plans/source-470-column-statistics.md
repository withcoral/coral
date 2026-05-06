# SOURCE-470: Column-Level Statistics Implementation Plan

## Goal

Add column-level statistics to `coral.columns` while keeping `coral.columns` a
projection over a real statistics model, not the storage model.

The implementation must answer the immediate SOURCE-470 need:

- expose nullable statistics fields in `coral.columns`
- collect conservative runtime observations for current backend families
- report observations once through the existing telemetry/trace-history pipeline
- materialize `profile.json` as a rebuildable workspace cache derived from
  local trace history
- reload the materialized profile into later runtime builds

It must not pull in the full old Portable Statistics / Visible Learning system.
This ticket should leave clean seams for later `coral.statistics`, `ANALYZE`,
import/export/reset, planner estimates, learning logs, and cost guards.

Confidence: high for the engine/app architecture and Graphite PR split.
Confidence: moderate for exact Parquet footer-stat extraction until the
DataFusion/parquet APIs are exercised in code. If exact Parquet footer metadata
is awkward or unstable, land observed Parquet scan statistics first and keep
metadata-derived exactness as a follow-up, not a blocker for honest nullable
stats.

## Non-Goals

Do not implement these in SOURCE-470:

- `ANALYZE`
- connect-time probing of every table
- `coral.statistics`
- `coral.learning_log`
- CLI/API export, import, reset, or inspect commands for the stats cache
- planner/cost/budget integration
- raw sample value persistence
- filter-scoped statistics in the public catalog
- hidden API fan-out during runtime registration

## Current Repo Reality

The current migrated repo shape matters more than the old ADP shape.

- `crates/coral-engine/src/runtime/catalog.rs` builds fixed in-memory
  `coral.tables`, `coral.columns`, and `coral.inputs` tables.
- `coral.columns` currently has:
  `schema_name`, `table_name`, `ordinal_position`, `column_name`, `data_type`,
  `is_nullable`, `is_virtual`, `is_required_filter`, and `description`.
- `crates/coral-engine/src/contracts/query.rs` owns
  `QueryRuntimeConfig`, `QueryRuntimeContext`, `QuerySource`, and
  `QueryExecution`.
- `crates/coral-engine/src/runtime/query.rs` builds the DataFusion
  `SessionContext`, registers sources, registers the catalog, executes SQL, and
  collects final `RecordBatch` results.
- `crates/coral-engine/src/backends/common.rs` owns registry-visible
  `RegisteredSource`, `RegisteredTable`, and `RegisteredColumn` metadata.
- `crates/coral-engine/src/backends/shared/json_exec.rs` is the shared
  execution node used by JSON-producing backends after rows are fetched and
  converted into Arrow batches.
- `crates/coral-engine/src/backends/jsonl/mod.rs` uses `JsonExec` over local
  JSONL files. It reads matching files on scan, not at registration.
- `crates/coral-engine/src/backends/http/provider.rs` uses `JsonExec` over
  HTTP fetches. It knows pushed filters and pushed limits at scan time.
- `crates/coral-engine/src/backends/parquet/mod.rs` wraps DataFusion
  `ListingTable` and can wrap its returned scan plan.
- `crates/coral-app/src/query/manager.rs` is the app orchestration point that
  loads installed sources, builds `QueryRuntimeConfig`, calls `CoralQuery`, and
  should refresh the materialized stats cache only after successful execution.
- `crates/coral-app/src/telemetry/local_store.rs` owns local trace history as
  rolling JSONL span records with retention and pruning. SOURCE-470 should use
  this as the durable observation log instead of adding a parallel observation
  log.
- `crates/coral-app/src/state/layout.rs` owns workspace paths under
  `CORAL_CONFIG_DIR`.
- `crates/coral-app/src/storage/fs.rs` already has private directory creation,
  file locking, and atomic write helpers.
- The repo instruction remains: run `make rust-checks` before finishing Rust
  code changes.

## User-Visible Contract

Append these nullable fields to `coral.columns`:

```sql
null_fraction DOUBLE NULL,
approx_distinct_count BIGINT NULL,
stats_sample_count BIGINT NULL,
stats_observed_at TEXT NULL,
stats_precision TEXT NULL
```

Rules:

- Existing `coral.columns` columns keep their current names, types, order, and
  meaning.
- New fields are additive and appear after `description`.
- All new fields are nullable.
- Sources and columns without stats show `NULL`, never fabricated values.
- `approx_distinct_count` is the name. Do not expose `distinct_count`.
- `stats_precision` uses exactly these stable strings:
  - `exact`
  - `approximate`
  - `observed_sample`
  - `unknown`
- `stats_observed_at` is an RFC3339 UTC timestamp string.
- `stats_sample_count` is the row count used for the displayed per-column
  statistic. Use SQL `BIGINT` / Arrow `Int64`, saturating only if a `u64`
  internal value exceeds `i64::MAX`.
- `null_fraction` is derived as `null_count / sample_count`. It is `NULL` when
  the internal profile does not know both values or when `sample_count == 0`.
- `approx_distinct_count` is `NULL` for unsupported types or unsafe scopes.
- Do not expose or persist `sample_values`.

## Architecture Decisions

1. Keep the real model in `coral-engine` contracts.

   Add transport-neutral statistics types under
   `crates/coral-engine/src/contracts/statistics.rs` and re-export them through
   `contracts/mod.rs` and `lib.rs` as needed by `coral-app`.

2. Make `coral.columns` a pure projection.

   `runtime/catalog.rs` receives a `StatisticsProfile` and projects matching
   profile entries into the new nullable fields while building the existing
   `MemTable`.

3. Load the materialized stats cache before runtime registration.

   `coral-app` loads a workspace profile, filters it to the selected sources,
   and passes it through `QueryRuntimeConfig`. `coral-engine` must not read app
   state or environment variables directly.

4. Report observations from scan nodes through telemetry, not final results.

   Final query results may be aggregated, filtered, joined, or projected in a
   way that no longer maps cleanly to source-table columns. Observations should
   be produced at backend scan boundaries where source, table, pushed filters,
   projection, and limit are known. They should be serialized as structured
   telemetry events on the active query/scan trace, making local trace history
   the durable record.

5. Rebuild the cache only after successful query execution.

   `QueryManager::execute_sql` should refresh `profile.json` from local trace
   history only if `CoralQuery::execute_sql` returns `Ok(QueryExecution)`. If
   DataFusion starts scanning and later fails, the failed query trace can still
   exist for diagnostics, but its observations must not update the current
   catalog cache.

6. Scope controls cache eligibility.

   Only table-global observations may update table-global cached stats.
   Filtered, required-filter, limited, or unknown-scope observations can be
   carried in trace history for future use but must not update `coral.columns`
   in this ticket.

7. Prefer absent stats over misleading stats.

   `NULL` is a correct answer when the system cannot defend a statistic.
   Misleading precision is worse than no precision.

## Statistics Model

Use this shape as the implementation target. Adjust field names for Rust
ergonomics, but keep the semantics.

```rust
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatisticsProfile {
    pub version: u32,
    pub sources: BTreeMap<String, SourceStatistics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceStatistics {
    pub schema_name: String,
    pub source_version: Option<String>,
    pub tables: BTreeMap<String, TableStatistics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    pub schema_name: String,
    pub table_name: String,
    pub source_version: Option<String>,
    pub schema_signature: TableSchemaSignature,
    pub columns: BTreeMap<String, ColumnStatistics>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchemaSignature {
    pub columns: Vec<ColumnSchemaSignature>,
    pub required_filters: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchemaSignature {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub is_virtual: bool,
    pub is_required_filter: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    pub column_name: String,
    pub sample_count: u64,
    pub null_count: Option<StatisticValue<u64>>,
    pub approx_distinct_count: Option<StatisticValue<u64>>,
    pub observed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticValue<T> {
    pub value: T,
    pub precision: StatisticPrecision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatisticPrecision {
    Exact,
    Approximate,
    ObservedSample,
    Unknown,
}
```

Important details:

- Store counts, not only fractions. `null_fraction` is a projection.
- Store a table schema signature so stale stats are ignored when source column
  metadata changes.
- `source_version` is useful but not sufficient by itself because custom
  manifests may change without version discipline.
- Do not add a hash dependency only to identify schemas unless there is a
  strong reason. A structured signature is simple, reviewable, and avoids
  pretending to solve data freshness.
- File contents and API data can change without schema changes. That is not a
  correctness bug as long as `stats_observed_at` and `stats_precision` are
  honest.

### Observations

Add a separate runtime observation type. Persisted profiles and telemetry
observations should not be the same object.

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticsObservation {
    pub schema_name: String,
    pub table_name: String,
    pub source_version: Option<String>,
    pub schema_signature: TableSchemaSignature,
    pub scope: StatisticsObservationScope,
    pub observed_at: String,
    pub columns: Vec<ColumnStatisticsObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatisticsObservationScope {
    TableGlobal,
    Filtered { filter_columns: Vec<String> },
    Limited,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatisticsObservation {
    pub column_name: String,
    pub sample_count: u64,
    pub null_count: Option<StatisticValue<u64>>,
    pub approx_distinct_count: Option<StatisticValue<u64>>,
}
```

Materialization policy:

- `TableGlobal` observations from successful query traces may update cached
  table stats.
- `Filtered`, `Limited`, and `Unknown` observations must remain in trace
  history only for now.
- The cache is a current table snapshot. For the same source/table, the latest
  eligible observation replaces the previous cached table entry rather than
  accumulating raw observations.
- `null_fraction` projects from the stored `null_count` and `sample_count`.
- `observed_at` is the observation timestamp from the telemetry event.
- If schema signatures differ, the catalog projection ignores the stale cached
  table stats.

## Engine Implementation

### 1. Contracts

Files:

- `crates/coral-engine/Cargo.toml`
- `crates/coral-engine/src/contracts/statistics.rs`
- `crates/coral-engine/src/contracts/mod.rs`
- `crates/coral-engine/src/contracts/query.rs`
- `crates/coral-engine/src/lib.rs`

Tasks:

- Add `serde.workspace = true` to `crates/coral-engine/Cargo.toml` for the
  persisted profile contract types.
- Add statistics profile, observation, signature, precision, and merge helper
  types.
- Add `StatisticsProfile::empty()` or `Default`.
- Add `StatisticPrecision::as_str()` for catalog projection.
- Add `StatisticPrecision::weaker(self, other)`.
- Add helpers to compute projected catalog values:
  - `null_fraction() -> Option<f64>`
  - `sample_count_i64() -> Option<i64>`
  - `precision_for_catalog() -> Option<&'static str>`
- Extend `QueryRuntimeConfig` with:

  ```rust
  pub statistics: StatisticsProfile,
  ```

  Keep default config behavior as "empty stats".

- Do not extend `QueryExecution` with observations. Observations are reported
  through telemetry, and app materialization consumes local trace history.

### 2. Catalog Projection

File:

- `crates/coral-engine/src/runtime/catalog.rs`

Tasks:

- Change `register` signature to accept a stats profile:

  ```rust
  pub(crate) fn register(
      ctx: &SessionContext,
      active_sources: &[RegisteredSource],
      statistics: &StatisticsProfile,
  ) -> Result<()>
  ```

- Change `build_columns_table` to accept the profile.
- Add new Arrow fields after `description`:
  - `Field::new("null_fraction", DataType::Float64, true)`
  - `Field::new("approx_distinct_count", DataType::Int64, true)`
  - `Field::new("stats_sample_count", DataType::Int64, true)`
  - `Field::new("stats_observed_at", DataType::Utf8, true)`
  - `Field::new("stats_precision", DataType::Utf8, true)`
- Extend `CatalogColumn` with optional projected stats fields.
- Use `Float64Array`, `Int64Array`, and nullable `StringArray` builders.
- Match stats by `(schema_name, table_name, column_name)` and require the
  stored table schema signature to match the current `RegisteredTable`.
- Add unit tests:
  - `coral.columns` has the new columns.
  - empty profile yields all new fields as `NULL`.
  - matching profile projects expected values.
  - mismatched schema signature yields `NULL`, not stale stats.
  - existing columns and ordering before `description` are unchanged.

### 3. Runtime Telemetry Reporting

Files:

- `crates/coral-engine/src/runtime/query.rs`
- `crates/coral-engine/src/runtime/statistics.rs` or
  `crates/coral-engine/src/contracts/statistics.rs` if small enough
- `crates/coral-engine/src/backends/shared/json_exec.rs`
- `crates/coral-engine/src/backends/file/mod.rs`

Tasks:

- Do not add `StatisticsObservationSink` or `RuntimeStatisticsContext`.
- Keep backend registration free of app-owned telemetry/storage context. Scan
  providers build `BatchStatisticsPlan` from their existing source/table
  metadata and report observations directly.
- Add one reporting helper that serializes `StatisticsObservation` as JSON and
  emits a structured telemetry event on the current span:

  ```text
  event name: coral.statistics.observation
  attribute: coral.statistics.observation = <serialized observation JSON>
  ```

- If serialization/reporting fails, warn and keep the query path working.
- Add a shared Arrow batch collector:
  - input: schema/table identity, schema signature, scope, projected fields,
    and one or more `RecordBatch` values
  - output: `StatisticsObservation`
  - null counts: use `Array::null_count()` and `RecordBatch::num_rows()`
  - distinct counts: support manifest scalar types first (`Utf8`, `Int64`,
    `Boolean`, `Timestamp`). For manifest `Utf8`, handle Arrow `Utf8`,
    `LargeUtf8`, `Utf8View`, and dictionary-backed string arrays because local
    Parquet scans can surface string columns as `Utf8View`.
  - return `NULL` for unsupported nested, JSON, `Float64`, or awkward types.
  - distinct implementation uses in-memory sets only for the current batch or
    current scan observation; never persist sets or raw values.
  - `Float64` distinct remains `NULL` until NaN and signed-zero behavior is
    specified deliberately.

### 4. JSONL Collection

Files:

- `crates/coral-engine/src/backends/jsonl/mod.rs`
- `crates/coral-engine/src/backends/shared/json_exec.rs`

Behavior:

- JSONL does not scan during registration.
- JSONL observes rows only when a query scans a table.
- Since current JSONL filters are not pushed down to file reading, the scan
  sees the file-backed table rows. Treat observations as `TableGlobal` only
  when:
  - `limit` is absent, and
  - no required filter columns are projected as real cached stats, and
  - the observed column is not virtual / filter-only.
- Virtual or required-filter columns should get no cached stats in this
  ticket unless their value truly comes from stored data.
- Precision is `observed_sample` even if the scan happens to read all current
  files. The data can change and there is no snapshot identity.
- Add engine tests that run `SELECT` over local JSONL fixtures under a test
  trace subscriber and assert the telemetry event contains expected null and
  distinct counts.

### 5. HTTP/API Collection

Files:

- `crates/coral-engine/src/backends/http/provider.rs`
- `crates/coral-engine/src/backends/shared/json_exec.rs`

Behavior:

- Do not probe HTTP tables during runtime registration.
- Observe only rows returned by successful query execution.
- Scope is:
  - `Filtered` when any pushed filter is present
  - `Limited` when the scan limit is present
  - `TableGlobal` only when there are no pushed filters and no pushed limit
  - `Unknown` if the provider cannot determine scope
- App materialization only includes `TableGlobal`.
- Precision is `observed_sample`, not `exact`.
- Required-filter tables will often show `NULL` stats in `coral.columns`. That
  is correct.
- Add engine tests with `wiremock`:
  - unfiltered query reports a `TableGlobal` observation event
  - filtered query reports a `Filtered` observation event
  - limited query reports a `Limited` observation event
  - only table-global observations are cache-eligible in app tests

### 6. Parquet Collection

Files:

- `crates/coral-engine/src/backends/file/mod.rs`
- `crates/coral-engine/src/runtime/statistics.rs`

Implementation path:

1. Wrap the `ExecutionPlan` returned by `ListingTable::scan` in an
   `ObservingExec`.
2. `ObservingExec` delegates children/properties/schema to the inner plan and
   wraps the returned stream so each successful batch is forwarded unchanged
   while being accumulated for stats.
3. Report one telemetry observation when all output partitions complete
   successfully.
4. Scope is:
   - `Filtered` when pushed filters are present
   - `Limited` when `limit` is present
   - `TableGlobal` only when no filters and no limit are present
5. Precision for scan-observed Parquet stats is `observed_sample`.

Exact metadata path:

- Attempt to collect exact `row_count` and `null_count` from Parquet footer row
  group metadata when all required metadata is available without forcing a full
  scan.
- Mark metadata-derived `null_fraction` as `exact` only when every file and row
  group needed for the table has row count and null count for the column.
- Leave `approx_distinct_count` `NULL` unless the metadata genuinely provides a
  defensible distinct estimate.
- Do not block SOURCE-470 on exact footer metadata if DataFusion's listing
  abstractions make this unstable. The honest fallback is observed scan stats
  with `observed_sample`.

Tests:

- Local Parquet fixture with known nulls yields observed stats after an
  unfiltered scan.
- Filtered or limited Parquet scans do not materialize as table-global cached
  stats.
- If exact footer stats land, add a test proving all-row-group metadata is
  required before precision is `exact`.

## App Materialization

Files:

- `crates/coral-app/src/state/layout.rs`
- `crates/coral-app/src/state/statistics.rs`
- `crates/coral-app/src/state/mod.rs`
- `crates/coral-app/src/query/manager.rs`
- `crates/coral-app/src/telemetry/local_store.rs`
- `crates/coral-app/src/telemetry/mod.rs`
- `crates/coral-app/src/bootstrap/server.rs`

### Layout

Add workspace-scoped cache paths:

```rust
pub(crate) fn statistics_dir(&self, workspace_name: &WorkspaceName) -> PathBuf {
    self.workspace_dir(workspace_name).join("statistics")
}

pub(crate) fn statistics_profile_file(&self, workspace_name: &WorkspaceName) -> PathBuf {
    self.statistics_dir(workspace_name).join("profile.json")
}
```

### Store

Add `StatisticsStore`:

```rust
#[derive(Debug, Clone)]
pub(crate) struct StatisticsStore {
    layout: AppStateLayout,
}
```

Responsibilities:

- load missing profile as empty
- parse JSON profile with versioning
- save with `storage::fs::write_atomic`
- use the existing state lock for load/save/update
- rebuild `profile.json` from telemetry observations read from local trace
  history
- include only table-global observations in this ticket
- filter the rebuilt cache to sources currently selected in the workspace
- never persist raw values

Storage format:

```text
$CORAL_CONFIG_DIR/workspaces/<workspace>/statistics/profile.json
```

This file is a derived cache, not the durable observation log and not a public
API. Still include a `version` field so cache migrations are possible later.

### Telemetry Reader

Extend the local trace store with a stats-observation reader:

- scan local trace JSONL files under the configured trace-history directory
- parse each span's `events_json`
- find `coral.statistics.observation` events
- deserialize the JSON payload into `StatisticsObservation`
- preserve chronological file/line order so later eligible observations replace
  earlier cache entries deterministically
- tolerate malformed observation payloads by warning and skipping that event;
  malformed trace JSONL rows should continue to use the existing trace-store
  error behavior

Expose a narrow `telemetry` module helper for the query manager:

```rust
pub(crate) async fn load_statistics_observations(
    store: &InstalledLocalTraceStore,
) -> Result<Vec<StatisticsObservation>, AppError>
```

Add a `force_flush_tracing()` helper so successful queries can flush the local
span exporter before rebuilding the cache.

### Query Manager

Update `QueryManager`:

- add a `statistics_store: StatisticsStore` field
- add an optional installed local trace store field
- construct it in `new(...)`
- in `runtime_config`, load the workspace profile and include it in
  `QueryRuntimeConfig`
- after successful `execute_sql`, force-flush tracing, read statistics
  observations from local trace history, and rebuild the workspace profile cache
- do not merge on query failure
- do not refresh stats from `validate_source` / `coral source test` in this
  ticket; keep cache refresh tied to user query execution
- if trace history is disabled or unavailable, stats stay `NULL`; do not fall
  back to a parallel storage path
- make warnings non-fatal: stats cache load/rebuild/save problems should warn
  and keep the query result path working unless the failure indicates broader
  state corruption already treated as fatal elsewhere

Change the manager helper so callers load stats explicitly:

```rust
fn runtime_config(
    &self,
    selected_sources: &[QuerySource],
    statistics: StatisticsProfile,
) -> QueryRuntimeConfig
```

`list_tables` and `execute_sql` should load the workspace profile before
calling `runtime_config`. `validate_source` can load the profile or pass an
empty profile, but it must not refresh the stats cache. That keeps environment
and filesystem access in app orchestration, not in the config helper.

### App Tests

Add tests proving:

- missing profile loads as empty
- profile save/load round-trips
- rebuild is atomic and uses the state lock
- matching `TableGlobal` observations update profile cache
- filtered/limited/unknown observations are ignored
- schema signature mismatch drops old stats
- local trace-store reader extracts stats observation events
- `QueryManager::execute_sql` refreshes the cache after success when local
  trace history is installed
- failed queries do not refresh the stats cache
- later runtime builds project cached stats through `coral.columns`

## Docs And MCP

Files:

- `crates/coral-mcp/src/guide_template.md`
- `crates/coral-mcp/src/surface/resources.rs`
- `docs/guides/use-coral-over-mcp.mdx`
- `docs/reference/source-spec-reference.mdx` if source-author guidance needs a
  short note

Guidance:

- Keep MCP prose small. This is not a broad discovery workflow rewrite.
- Update column-inspection examples to mention the optional stats fields only
  where useful.
- Make clear that stats are nullable and may be sample-derived.
- Do not imply every API table has stats.
- Do not direct agents to treat `approx_distinct_count` as exact.

Example SQL to use in docs/resources:

```sql
SELECT
  column_name,
  data_type,
  is_nullable,
  null_fraction,
  approx_distinct_count,
  stats_sample_count,
  stats_precision,
  description
FROM coral.columns
WHERE schema_name = '<schema>' AND table_name = '<table>'
ORDER BY ordinal_position;
```

## Logical PR Split

Use a Graphite stack. Do not put this entire feature in one PR.

### PR 1: `feat(engine): add statistics profile to catalog`

Branch:

```text
source-470-stats-catalog
```

Scope:

- statistics contracts
- `QueryRuntimeConfig.statistics`
- `coral.columns` projection fields
- serializable observation contract for trace-event payloads
- catalog tests with empty and synthetic profiles
- no backend collection
- no app materialization

Primary files:

- `crates/coral-engine/Cargo.toml`
- `crates/coral-engine/src/contracts/statistics.rs`
- `crates/coral-engine/src/contracts/mod.rs`
- `crates/coral-engine/src/contracts/query.rs`
- `crates/coral-engine/src/runtime/catalog.rs`
- `crates/coral-engine/src/runtime/query.rs`
- `crates/coral-engine/src/lib.rs`

Validation:

```shell
cargo fmt --all -- --check
cargo test -p coral-engine runtime::catalog --locked
cargo test -p coral-engine contracts::statistics --locked
```

Acceptance:

- `SELECT * FROM coral.columns` still works with no stats profile.
- New fields exist and are nullable.
- Synthetic matching stats project correctly.
- Synthetic stale stats do not project.

### PR 2: `feat(engine): report column statistics observations`

Branch:

```text
source-470-stats-observations
```

Scope:

- shared Arrow batch stats collector
- telemetry observation reporter
- `JsonExec` instrumentation
- JSONL observed stats
- HTTP observed stats
- merge-eligibility scope classification in engine-level types
- no app materialization yet

Primary files:

- `crates/coral-engine/src/contracts/statistics.rs`
- `crates/coral-engine/src/runtime/statistics.rs`
- `crates/coral-engine/src/backends/shared/json_exec.rs`
- `crates/coral-engine/src/backends/file/mod.rs`
- `crates/coral-engine/src/backends/http/provider.rs`

Validation:

```shell
cargo fmt --all -- --check
cargo test -p coral-engine backends::jsonl --locked
cargo test -p coral-engine backends::http --locked
cargo test -p coral-engine runtime::statistics --locked
```

Acceptance:

- JSONL unfiltered scans report table-global telemetry observations.
- HTTP filtered scans report filtered telemetry observations.
- HTTP limited scans report limited telemetry observations.
- Query failures do not update the app cache because app refresh only runs
  after success.
- No raw values are persisted or exposed.

### PR 3: `feat(app): materialize workspace column statistics`

Branch:

```text
source-470-stats-persistence
```

Scope:

- workspace stats cache path
- `StatisticsStore`
- local trace-history statistics observation reader
- load materialized profile into runtime config
- rebuild cache from telemetry after successful query execution
- cached stats visible in later `coral.columns` queries
- failed queries do not refresh the cache

Primary files:

- `crates/coral-app/src/state/layout.rs`
- `crates/coral-app/src/state/statistics.rs`
- `crates/coral-app/src/state/mod.rs`
- `crates/coral-app/src/telemetry/local_store.rs`
- `crates/coral-app/src/telemetry/mod.rs`
- `crates/coral-app/src/bootstrap/server.rs`
- `crates/coral-app/src/query/manager.rs`
- app tests under the existing test modules

Validation:

```shell
cargo fmt --all -- --check
cargo test -p coral-app state::statistics --locked
cargo test -p coral-app query::manager --locked
```

Acceptance:

- profile is written under
  `$CORAL_CONFIG_DIR/workspaces/default/statistics/profile.json`
- trace-derived stats survive across separate CLI invocations using the same
  `CORAL_CONFIG_DIR`
- filtered/limited observations do not update table-global catalog stats
- corrupt or mismatched old stats do not poison new catalog rows

### PR 4: `feat(engine): add parquet column statistics observations`

Branch:

```text
source-470-parquet-stats
```

Scope:

- Parquet scan telemetry observation via `ObservingExec`
- observed-sample Parquet scan stats
- local Parquet tests
- no exact Parquet footer-stat extraction in SOURCE-470; keep that as a
  follow-up unless the DataFusion API path becomes trivial and reviewer-safe
- no docs-only changes unless a small note is needed for reviewers

Primary files:

- `crates/coral-engine/src/backends/file/mod.rs`
- `crates/coral-engine/src/runtime/statistics.rs`
- `crates/coral-engine/src/contracts/statistics.rs`

Validation:

```shell
cargo fmt --all -- --check
cargo test -p coral-engine backends::parquet --locked
cargo test -p coral-engine runtime::statistics --locked
```

Acceptance:

- local Parquet unfiltered scans report observed stats through telemetry.
- filtered/limited Parquet scans are not merged as table-global stats.
- Parquet string columns that surface as Arrow `Utf8View` still get bounded
  distinct-count collection when the manifest type is `Utf8`.
- the PR description explicitly says Parquet currently lands as
  `observed_sample`.

### PR 5: `docs(mcp): document nullable column statistics`

Branch:

```text
source-470-stats-docs-smoke
```

Scope:

- concise MCP guide/resource updates
- public docs note
- compiled-app local-source smoke validation
- optional smoke script or checked fixture files if that prevents drift

Primary files:

- `crates/coral-mcp/src/guide_template.md`
- `crates/coral-mcp/src/surface/resources.rs`
- `docs/guides/use-coral-over-mcp.mdx`
- `docs/reference/source-spec-reference.mdx`
- optional `scripts/` or test fixture files for repeatable local-source smoke

Validation:

```shell
cargo fmt --all -- --check
cargo test -p coral-mcp --locked
cargo build -p coral-cli --locked
make rust-checks
```

Acceptance:

- docs say stats are nullable and may be observed samples.
- MCP examples remain concise.
- compiled app has been run against local JSONL, local HTTP, and local Parquet
  sources as described below.

## Graphite Workflow

The stack should be created and managed with `gt`, not raw `git push`.

Start from trunk:

```shell
gt sync
gt checkout main
gt log --stack
```

For each PR, edit the files for that slice, run the slice validation, then
create the next stacked branch:

```shell
gt create source-470-stats-catalog \
  --all \
  --message "feat(engine): add statistics profile to catalog"

gt create source-470-stats-observations \
  --all \
  --message "feat(engine): report column statistics observations"

gt create source-470-stats-persistence \
  --all \
  --message "feat(app): materialize workspace column statistics"

gt create source-470-parquet-stats \
  --all \
  --message "feat(engine): add parquet column statistics observations"

gt create source-470-stats-docs-smoke \
  --all \
  --message "docs(mcp): document nullable column statistics"
```

Before opening PRs:

```shell
gt restack
gt log
gt submit --stack --dry-run
```

Open the stack as drafts first:

```shell
gt submit --stack --draft
```

When updating a lower PR after review:

```shell
gt checkout source-470-stats-catalog
# edit, test
gt modify --all --message "feat(engine): add statistics profile to catalog"
gt restack
gt submit --stack --draft --no-edit
```

Operational rules:

- Keep every branch reviewable on its own.
- Keep PR titles in Conventional Commit form.
- Put the validation commands actually run in each PR description.
- If a lower branch changes a public Rust contract, immediately restack and
  re-run affected upstack tests.
- Do not submit only the top branch. Use `gt submit --stack` from the stack tip.

## Required Validation

Unit and crate tests are required, but they are not enough. The feature must be
validated by running the compiled app against locally connected sources.

### Rust Checks

Run before final stack submission:

```shell
cargo fmt --check
CARGO_TARGET_DIR=target/source-470-check cargo test --locked -p coral-engine -p coral-app -p coral-mcp -p xtask
CARGO_TARGET_DIR=target/source-470-check cargo clippy --locked -p coral-engine -p coral-app -p coral-mcp -p xtask --all-targets --all-features -- -D warnings
CARGO_TARGET_DIR=target/source-470-check cargo build --locked -p coral-cli
```

Also run `make rust-checks` before final merge readiness if full-workspace
nextest/docs coverage is required by CI or reviewer request. For fast
iteration, run focused checks per PR as listed above.

### Compiled App Smoke: JSONL

This smoke proves stats are absent before observation, then visible and
materialized after a real compiled CLI query against a local JSONL source.
It relies on default-enabled local trace history; disabling trace history is
expected to leave stats as `NULL`.

```shell
cargo build -p coral-cli --locked

SMOKE_ROOT="$(mktemp -d)"
export CORAL_CONFIG_DIR="$SMOKE_ROOT/config"
mkdir -p "$SMOKE_ROOT/jsonl-data"

cat > "$SMOKE_ROOT/jsonl-data/events.jsonl" <<'EOF'
{"id":1,"category":"alpha","nullable_text":"one","active":true}
{"id":2,"category":"beta","nullable_text":null,"active":false}
{"id":3,"category":"alpha","nullable_text":"three","active":true}
{"id":4,"category":"gamma","active":false}
{"id":5,"category":"beta","nullable_text":"five","active":true}
EOF

cat > "$SMOKE_ROOT/local-stats-jsonl.yaml" <<EOF
name: local_stats_jsonl
version: 0.1.0
dsl_version: 3
backend: file
tables:
  - name: events
    description: Local stats smoke events
    format: jsonl
    source:
      location: file://$SMOKE_ROOT/jsonl-data/
      glob: "**/*.jsonl"
    columns:
      - name: id
        type: Int64
      - name: category
        type: Utf8
      - name: nullable_text
        type: Utf8
        nullable: true
      - name: active
        type: Boolean
EOF

./target/debug/coral source add --file "$SMOKE_ROOT/local-stats-jsonl.yaml"

./target/debug/coral sql --format json "
  SELECT column_name, null_fraction, approx_distinct_count, stats_sample_count, stats_precision
  FROM coral.columns
  WHERE schema_name = 'local_stats_jsonl' AND table_name = 'events'
  ORDER BY ordinal_position
" > "$SMOKE_ROOT/jsonl-before.json"

./target/debug/coral sql --format json "
  SELECT id, category, nullable_text, active
  FROM local_stats_jsonl.events
  ORDER BY id
" > "$SMOKE_ROOT/jsonl-query.json"

./target/debug/coral sql --format json "
  SELECT column_name, null_fraction, approx_distinct_count, stats_sample_count, stats_precision
  FROM coral.columns
  WHERE schema_name = 'local_stats_jsonl' AND table_name = 'events'
  ORDER BY ordinal_position
" > "$SMOKE_ROOT/jsonl-after.json"

python3 - "$SMOKE_ROOT/jsonl-before.json" "$SMOKE_ROOT/jsonl-after.json" <<'PY'
import json
import math
import sys

before = json.load(open(sys.argv[1]))
after = json.load(open(sys.argv[2]))

assert before, "expected coral.columns rows before observation"
assert all(row["stats_sample_count"] is None for row in before), before

by_name = {row["column_name"]: row for row in after}
nullable_text = by_name["nullable_text"]
category = by_name["category"]

assert nullable_text["stats_sample_count"] == 5, nullable_text
assert math.isclose(nullable_text["null_fraction"], 0.4), nullable_text
assert nullable_text["stats_precision"] == "observed_sample", nullable_text

assert category["approx_distinct_count"] == 3, category
assert category["stats_sample_count"] == 5, category
PY
```

Then invoke the compiled CLI again using the same `CORAL_CONFIG_DIR` and assert
the stats are still present. This proves the materialized cache is reusable
across runtime rebuilds, not just in-memory observation.

```shell
./target/debug/coral sql --format json "
  SELECT column_name, stats_sample_count, stats_precision
  FROM coral.columns
  WHERE schema_name = 'local_stats_jsonl' AND table_name = 'events'
  ORDER BY ordinal_position
" > "$SMOKE_ROOT/jsonl-reload.json"

python3 - "$SMOKE_ROOT/jsonl-reload.json" <<'PY'
import json
import sys

rows = json.load(open(sys.argv[1]))
assert rows
assert any(row["stats_sample_count"] == 5 for row in rows), rows
assert all(
    row["stats_precision"] in (None, "observed_sample", "approximate", "exact", "unknown")
    for row in rows
), rows
PY
```

### Compiled App Smoke: HTTP

This smoke proves a locally connected HTTP source does not merge filtered
observations into table-global stats.

```shell
mkdir -p "$SMOKE_ROOT/http-data"
cat > "$SMOKE_ROOT/http-data/messages.json" <<'EOF'
[
  {"id": "m1", "status": "open", "body": "first"},
  {"id": "m2", "status": "closed", "body": null},
  {"id": "m3", "status": "open", "body": "third"}
]
EOF

python3 -m http.server 8765 --directory "$SMOKE_ROOT/http-data" >/tmp/coral-stats-http.log 2>&1 &
HTTP_PID=$!
trap 'kill "$HTTP_PID" 2>/dev/null || true' EXIT

cat > "$SMOKE_ROOT/local-stats-http.yaml" <<'EOF'
name: local_stats_http
version: 0.1.0
dsl_version: 3
backend: http
inputs:
  API_BASE:
    kind: variable
    default: http://127.0.0.1:8765
base_url: "{{input.API_BASE}}"
tables:
  - name: messages
    description: Local HTTP stats smoke messages
    filters:
      - name: status
        required: false
    request:
      method: GET
      path: /messages.json
      query:
        - name: status
          from: filter
          key: status
          default: all
    response:
      row_strategy: direct
    columns:
      - name: id
        type: Utf8
      - name: status
        type: Utf8
      - name: body
        type: Utf8
        nullable: true
EOF

./target/debug/coral source add --file "$SMOKE_ROOT/local-stats-http.yaml"

./target/debug/coral sql --format json "
  SELECT id, body
  FROM local_stats_http.messages
  WHERE status = 'open'
  ORDER BY id
" > "$SMOKE_ROOT/http-filtered-query.json"

./target/debug/coral sql --format json "
  SELECT column_name, stats_sample_count
  FROM coral.columns
  WHERE schema_name = 'local_stats_http' AND table_name = 'messages'
  ORDER BY ordinal_position
" > "$SMOKE_ROOT/http-after-filtered.json"

python3 - "$SMOKE_ROOT/http-after-filtered.json" <<'PY'
import json
import sys

rows = json.load(open(sys.argv[1]))
assert rows
assert all(row["stats_sample_count"] is None for row in rows), rows
PY

./target/debug/coral sql --format json "
  SELECT id, status, body
  FROM local_stats_http.messages
  ORDER BY id
" > "$SMOKE_ROOT/http-unfiltered-query.json"

./target/debug/coral sql --format json "
  SELECT column_name, stats_sample_count, stats_precision
  FROM coral.columns
  WHERE schema_name = 'local_stats_http' AND table_name = 'messages'
  ORDER BY ordinal_position
" > "$SMOKE_ROOT/http-after-unfiltered.json"

python3 - "$SMOKE_ROOT/http-after-unfiltered.json" <<'PY'
import json
import sys

rows = json.load(open(sys.argv[1]))
assert rows
assert any(row["stats_sample_count"] == 3 for row in rows), rows
assert all(
    row["stats_precision"] in (None, "observed_sample")
    for row in rows
), rows
PY
```

### Compiled App Smoke: Parquet

Do not skip Parquet. Use a deterministic generated fixture. The local
development smoke used `pyarrow` to write a small Parquet file with `Int64`,
`Utf8`, nullable `Utf8`, and `Boolean` columns; this caught the important
Arrow `Utf8View` string representation path.

Then run the compiled CLI against that generated local file:

```shell
python3 - "$SMOKE_ROOT/parquet-data/metrics.parquet" <<'PY'
import pathlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq

path = pathlib.Path(sys.argv[1])
path.parent.mkdir(parents=True, exist_ok=True)
table = pa.table({
    "id": pa.array([1, 2, 3, 4], type=pa.int64()),
    "category": pa.array(["alpha", "beta", "alpha", "gamma"], type=pa.string()),
    "nullable_text": pa.array(["one", None, "three", None], type=pa.string()),
    "active": pa.array([True, False, True, False], type=pa.bool_()),
})
pq.write_table(table, path)
PY

PARQUET_DIR="$SMOKE_ROOT/parquet-data"

cat > "$SMOKE_ROOT/local-stats-parquet.yaml" <<EOF
name: local_stats_parquet
version: 0.1.0
dsl_version: 3
backend: file
tables:
  - name: metrics
    description: Local stats smoke metrics
    format: parquet
    source:
      location: file://$PARQUET_DIR/
      glob: "**/*.parquet"
    columns: []
EOF

./target/debug/coral source add --file "$SMOKE_ROOT/local-stats-parquet.yaml"

./target/debug/coral sql --format json "
  SELECT *
  FROM local_stats_parquet.metrics
  ORDER BY 1
" > "$SMOKE_ROOT/parquet-query.json"

./target/debug/coral sql --format json "
  SELECT column_name, null_fraction, stats_sample_count, stats_precision
  FROM coral.columns
  WHERE schema_name = 'local_stats_parquet' AND table_name = 'metrics'
  ORDER BY ordinal_position
" > "$SMOKE_ROOT/parquet-after.json"

python3 - "$SMOKE_ROOT/parquet-after.json" <<'PY'
import json
import sys

rows = json.load(open(sys.argv[1]))
assert rows
assert any(row["stats_sample_count"] is not None for row in rows), rows
assert all(
    row["stats_precision"] in (None, "observed_sample", "exact")
    for row in rows
), rows
PY
```

The local HTTP smoke must run a filtered query first and assert
`stats_sample_count` remains `NULL`, then run an unfiltered query and assert
table-global stats appear. The local JSONL smoke must corrupt
`profile.json`, run another successful source query, and assert the cache is
rebuilt from trace history.

## Acceptance Criteria

- `SELECT * FROM coral.columns` continues to work for all current sources.
- Existing `coral.columns` fields keep their behavior.
- New stats fields exist and are nullable.
- Empty or missing stats profile yields `NULL` fields.
- Stale schema stats do not project.
- JSONL, HTTP, and Parquet have backend-specific tests.
- Workspace stats are materialized from local trace history and survive runtime
  rebuilds through `profile.json`.
- Filtered HTTP/API observations do not update table-global stats.
- Limited observations do not update table-global stats.
- Failed queries do not refresh the materialized cache.
- No raw user values are persisted.
- MCP/docs describe stats as nullable and sample-derived.
- `make rust-checks` passes.
- Compiled `./target/debug/coral` has been run against local JSONL, HTTP, and
  Parquet sources with the smoke checks above or an equivalent checked script.

## Risks And Guardrails

- Bad stats are worse than absent stats. Prefer `NULL`.
- Do not add hidden registration-time scans for APIs or large local files.
- Do not bind cache materialization to the `coral.columns` schema.
- Do not persist raw sample values or in-memory distinct sets.
- Do not treat filtered API results as table-global.
- Do not make the MCP guide noisy; keep docs focused on how to interpret the
  nullable fields.
- Do not widen CLI/API transport contracts unless required. This feature is
  visible through SQL metadata, not a new public RPC.
- Keep app cache materialization in `coral-app`; keep runtime/stat semantics in
  `coral-engine`.
- If exact Parquet footer stats get complicated, land observed Parquet stats
  with honest `observed_sample` precision and document the exact-stat follow-up
  in the PR.

## Definition Of Done

The stack is done when:

1. The five PRs are open as a Graphite stack.
2. Every PR has the focused validation commands in its description.
3. The stack tip passes `make rust-checks`.
4. The compiled-app local-source smoke passes and is recorded in the docs/smoke
   PR description.
5. `coral.columns` shows the new nullable fields for locally connected sources.
6. Trace-derived cached stats survive a separate CLI invocation with the same
   `CORAL_CONFIG_DIR`.
7. Filtered/limited observations remain out of table-global catalog stats.
