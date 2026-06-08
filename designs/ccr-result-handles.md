# CCR Result Handles Design

## Problem

Coral's MCP `sql` tool currently returns every row from a query as JSON rows in
the tool result. Large query results dominate model and protocol tokens even
when the agent only needs a preview, schema, or a small slice of the rows.

CCR should reduce token usage by storing large SQL results server-side for the
MCP session and returning a compact preview plus an opaque handle. The agent can
then fetch only the rows and columns it needs.

This design is for v1. It is intentionally MCP-only, in-memory, process-local,
and additive.

## Current Context

- `crates/coral-api/proto/coral/v1/query.proto` defines `ExecuteSqlResponse` as
  Arrow IPC bytes plus a row count. The app/query service already returns fully
  materialized query results.
- `crates/coral-client/src/lib.rs` owns shared query-result helpers. It decodes
  Arrow IPC into `CollectedQueryResult`, which holds `SchemaRef`,
  `Vec<RecordBatch>`, and `row_count`. It also owns table/JSON row rendering.
- `crates/coral-mcp/src/server.rs` is the MCP adapter. Its current `sql` path
  calls `execute_sql`, decodes the response with `coral-client`, converts all
  batches into JSON-safe rows, and returns `{ "rows": [...] }`.
- `crates/coral-mcp/src/surface/tools.rs` defines MCP tool schemas and
  currently pretty-prints every structured result into text content.
- `crates/coral-cli/src/lib.rs` separately decodes the same Arrow result and
  renders full table/JSON output for humans. CLI should stay unchanged in v1.
- `crates/coral-mcp/AGENTS.md` requires MCP to stay thin, to decode query
  payloads through `coral-client`, and to update docs for MCP surface changes.

## Technical Plan

### Scope

Do not change `coral-api`, `coral-app`, or `coral-engine` for v1. The server
already delivers the full Arrow result. CCR changes how the MCP adapter stores
and exposes that result.

Keep storage in memory. Do not write result handles to files until there is a
proven need for restart persistence or results larger than RAM.

### Data Flow

```text
MCP sql tool
  -> query gRPC execute_sql
  -> coral-client decodes Arrow IPC into CollectedQueryResult
  -> small result: return legacy { rows: [...] } compactly
  -> large result:
       store CollectedQueryResult in ResultStore
       return result_id + schema summary + preview rows + suggested result_get call

MCP result_get tool
  -> look up result_id in ResultStore
  -> slice rows and optional columns from stored Arrow batches
  -> render only that slice as JSON-safe rows
```

### Tool Surface

Add one new MCP tool:

```text
result_get(result_id, offset?, limit?, columns?)
```

Arguments:

- `result_id`: required opaque string returned by `sql`.
- `offset`: default `0`, minimum `0`.
- `limit`: default `50`, minimum `0`, maximum `500`.
- `columns`: optional array of column names. If omitted, return all columns.

`limit = 0` is valid and returns metadata/schema with no rows. `offset` beyond
the end of the result returns an empty page with `has_more = false`.

Column projection rules:

- omitted `columns`: return all columns in schema order
- empty `columns`: invalid params
- duplicate requested names: invalid params
- missing requested name: invalid params
- requested name that appears more than once in the result schema: invalid
  params because name-based projection is ambiguous

Output:

```json
{
  "result_id": "res_...",
  "row_count": 1842,
  "columns": [
    { "name": "number", "data_type": "Int64", "is_nullable": false, "ordinal_position": 0 }
  ],
  "offset": 0,
  "limit": 50,
  "has_more": true,
  "next_offset": 50,
  "rows": []
}
```

Do not add `result_schema` in v1. `result_get` can return schema with
`limit = 0`, and `sql` already returns schema summary for handled results.

Do not add `result_find` in v1. It is useful, but it needs separate design for
string matching semantics across nested values, case sensitivity, and large
result scans.

Error contract:

- malformed arguments, unknown handles, expired handles, evicted handles, and
  invalid column projection are protocol-level `ErrorData::invalid_params`
  errors
- SQL execution failures keep the existing structured tool-error behavior
- oversized preview-only SQL output is not an error; it returns a warning field

### SQL Tool Output

For small results, preserve the current structured shape:

```json
{ "rows": [...] }
```

This protects tiny/empty SQL cases where handle metadata would cost more tokens
than it saves.

For handled results, return:

```json
{
  "result_id": "res_...",
  "row_count": 1842,
  "column_count": 4,
  "columns": [
    { "name": "number", "data_type": "Int64", "is_nullable": false, "ordinal_position": 0 }
  ],
  "preview": {
    "offset": 0,
    "limit": 20,
    "has_more": true,
    "next_offset": 20,
    "rows": []
  },
  "next_call": {
    "tool": "result_get",
    "arguments": {
      "result_id": "res_...",
      "offset": 20,
      "limit": 50
    }
  }
}
```

For results too large to store under the configured memory cap, return a
preview-only shape:

```json
{
  "preview_only": true,
  "row_count": 1842,
  "column_count": 4,
  "columns": [],
  "preview": {
    "offset": 0,
    "limit": 20,
    "has_more": true,
    "next_offset": 20,
    "rows": []
  },
  "warning": "Result exceeded the in-memory handle limit; rerun the SQL with LIMIT, filters, or a smaller column set."
}
```

Use compact JSON text content for `sql` and `result_get`, not pretty JSON. The
structured content can remain the same object.

For v1, keep `sql` without an advertised output schema because it already lacks
one and will now have multiple compatible output variants. `result_get` should
advertise an output schema because it is a new tool with one stable page shape.

### Adaptive Handling

Use a bounded threshold so CCR only handles results when it wins without
expanding huge results into JSON just to make the decision:

1. If `row_count <= SQL_INLINE_BYTE_CHECK_MAX_ROWS`, render the full result once
   as compact JSON-safe rows and inline only when the compact `{ "rows": ... }`
   body is at or below `SQL_INLINE_MAX_BYTES`.
2. Otherwise, skip full JSON expansion and store/preview the result.
3. If storing is impossible because the single result exceeds the store cap,
   return the preview-only shape.

Initial constants:

- `SQL_PREVIEW_ROWS = 20`
- `SQL_INLINE_BYTE_CHECK_MAX_ROWS = 100`
- `RESULT_GET_DEFAULT_LIMIT = 50`
- `RESULT_GET_MAX_LIMIT = 500`
- `SQL_INLINE_MAX_BYTES = 8192`
- `RESULT_TTL = 30 minutes`
- `RESULT_STORE_MAX_RESULTS = 64`
- `RESULT_STORE_MAX_BYTES = 128 MiB`

These can be hard-coded in `coral-mcp` for v1. Expose config later only if the
defaults prove wrong.

### Result Store

Add an MCP-owned `ResultStore`:

```text
crates/coral-mcp/src/result_store.rs
```

Store:

- `result_id`
- `Arc<CollectedQueryResult>`
- estimated bytes from `RecordBatch::get_array_memory_size()`
- created time
- last accessed time
- expiry time

Do not store SQL text in v1. The handle store only needs the decoded result,
size accounting, and lifecycle metadata.

`ResultStore` must be cloneable by sharing state:

```text
ResultStore
  state: Arc<Mutex<ResultStoreState>>
  clock: Arc<dyn ResultClock>
  limits: ResultStoreLimits
```

`CoralMcpServer` is `Clone`, so every clone must see the same handle map for
one MCP server instance. Do not clone the map itself.

Use short process-local opaque IDs with a `res_` prefix in the external string
such as `res_1`. Handles are scoped to one MCP server process, so global
uniqueness is unnecessary and longer random IDs waste model tokens.

Use a synchronous `Mutex` internally. Store operations should not await while
holding the lock.

Keep `ResultStoreState` and `ResultStoreLimits` private to the store module.
The server should only depend on `ResultStore` and `ResultStoreError`:

```text
insert(result, estimated_bytes) -> Result<result_id, ResultStoreError>
get(result_id) -> Result<Arc<CollectedQueryResult>, ResultStoreError>
```

Add defaults for TTL, max results, and max bytes. Tests can construct a store
with small limits. Add a small clock abstraction:

```text
trait ResultClock: Send + Sync {
    fn now(&self) -> Instant;
}
```

Use a system clock in production and a manual clock in tests. Avoid sleeps in
expiry tests.

On insert and get:

- purge expired entries
- reject unknown or expired handles with an MCP invalid-params error
- evict least-recently-accessed entries until the byte/result caps are satisfied
- if a single result is larger than the store cap, return a preview-only result
  with a warning and no handle

Use fixed expiry from creation time. Update `last_accessed` only for LRU
eviction, not for extending TTL. This makes handle lifetime predictable.

### Arrow Slicing and Rendering

Add small helpers in `coral-client` instead of reimplementing Arrow/JSON logic
inside MCP:

```text
crates/coral-client/src/result_slice.rs
```

Responsibilities:

- Produce a schema summary from a `CollectedQueryResult`. Column summaries
  include `name`, `data_type`, `is_nullable`, and `ordinal_position`. Render
  `data_type` with Arrow's `DataType` display string.
- Project selected column names, preserving requested order.
- Slice offset/limit across multiple record batches.
- Convert only the sliced/projected batches to JSON-safe rows using the existing
  JSON-safe number encoder.
- Return pagination metadata.

Implementation detail: use `RecordBatch::slice` for row windows and
`RecordBatch::project` for column projection. Apply slicing before JSON
conversion so large stored results are not expanded into JSON just to serve a
small page.

`coral-client` will need `serde.workspace = true` if these result/page structs
derive `Serialize`.

Keep `coral-mcp` responsible for MCP argument parsing, handle lookup, and tool
result shaping.

### Text Rendering

Add a compact tool-result path in `crates/coral-mcp/src/surface/tools.rs`.

Current `build_tool_result` pretty-prints every structured JSON value. For CCR:

- keep existing pretty behavior for catalog/discovery tools unless changed by a
  separate PR
- use compact `serde_json::to_string` for `sql` and `result_get`

This avoids coupling CCR to the broader pretty-printing PR stack.

Implementation detail: change `ToolCallOutcome::Success(Value)` into a shape
that carries rendering policy, for example:

```text
Success { value: Value, text: ToolTextFormat }
```

where `ToolTextFormat` is `PrettyJson` or `CompactJson`. Keep discovery tools
on `PrettyJson`; return `CompactJson` for `sql` and `result_get`.

## Alternatives

### Store Results in `coral-app`

Rejected for v1. App-level storage would require new protobuf contracts and
server lifecycle semantics. CCR is an MCP presentation problem first.

### Store Results on Disk

Rejected for v1. Disk storage creates cleanup, path, encryption, crash, and
cross-process questions before the handle UX is proven. In-memory storage is
faster and easier to reason about.

### Always Return Handles

Rejected. Empty and tiny results are common in agent workflows. A handle envelope
can cost more tokens than the result itself.

### Add `result_find` Immediately

Rejected for v1. It is probably useful, but not required to prove token
reduction. Add it after the basic handle flow works.

### Change CLI Output at the Same Time

Rejected. CLI output is a human-facing contract and not the source of MCP token
burn. Keep CLI unchanged until there is an explicit CLI handle story.

## Detailed Implementation

### `crates/coral-client/src/lib.rs`

- Export a new result-slicing module.
- Keep existing public decode/render helpers intact.
- Add unit tests for schema summary, multi-batch slicing, column projection,
  missing columns, `limit = 0`, empty results, and JSON-safe int64/decimal
  preservation in slices.

### `crates/coral-client/Cargo.toml`

- Add `serde.workspace = true` if result page/schema structs derive
  `Serialize`.

### `crates/coral-client/src/result_slice.rs`

- Define `ColumnSummary`, `ResultPage`, and `ResultSliceRequest`.
- Implement `schema_summary(result)`.
- Implement `slice_result(result, offset, limit, columns)`.
- Reuse `batches_to_json_rows_json_safe_numbers` for the final sliced batches.
- Enforce projection rules from the Tool Surface section.

### `crates/coral-mcp/src/result_store.rs`

- Implement `ResultStore`.
- Implement `insert`, `get`, expiry purge, LRU eviction, byte accounting, and
  tests.
- Keep IDs opaque and scoped to one MCP server instance.

### `crates/coral-mcp/src/server.rs`

- Add `result_store: ResultStore` to `CoralMcpServer`.
- Change `query_rows` into a lower-level `execute_sql_result` returning
  `CollectedQueryResult`.
- Change `execute_sql_value` to choose inline vs handled output.
- Add `result_get_tool_result`.
- Add `"result_get"` to dispatch.

### `crates/coral-mcp/src/surface/tools.rs`

- Add `result_get_tool()` and argument parsing.
- Add compact result builder for `sql` and `result_get`.
- Add output schema handling for the new `result_get` tool.
- Do not reuse existing pagination helpers for `result_get`, because `limit = 0`
  is intentionally valid here while catalog pagination requires `limit >= 1`.

### `crates/coral-mcp/src/lib.rs`

- Update exposed MCP surface docs in crate-level comments to include
  `result_get`.

### `crates/coral-mcp/src/guide_template.md`

- Explain that large `sql` results can return a `result_id`.
- Tell agents to call `result_get` for more rows or narrower columns instead of
  re-running broad SQL.

### `crates/coral-mcp/src/tests.rs`

- Update tool list expectations.
- Add an MCP session test where a large SQL result returns `result_id` plus a
  preview, and `result_get` returns the requested next slice.
- Add tests for column projection, expired/unknown handle errors, and small
  result legacy inline behavior.
- Add a JSON-safe number test through `result_get`.
- Add tests for preview-only oversized result behavior using tiny store limits.

### `docs/guides/use-coral-over-mcp.mdx`

- Document that large SQL results may return a handle.
- Show how an agent should call `result_get` for more rows or fewer columns.
- State that handles are process-local, in-memory, and expire.

### Token Benchmark

- Add or update an MCP token benchmark that compares:
  - small inline SQL result
  - empty result
  - wide catalog-like result
  - many-row result
  - result follow-up slice

If the existing `mcp-token-bench` command is on another branch, port it or add
the CCR cases there when that benchmark exists on the target base.

## Acceptance Criteria

- `sql` still returns `{ "rows": [...] }` for small results.
- Large `sql` responses return a compact preview, `result_id`, row/column
  counts, schema summary, and a suggested `result_get` call when they fit the
  store cap.
- Results larger than the store cap return preview-only output with a warning
  and no `result_id`.
- `result_get` returns correct rows for offset/limit across multiple batches.
- `result_get` supports column projection by name and preserves requested
  column order.
- `result_get` rejects empty, duplicate, missing, and ambiguous column requests
  with invalid params.
- `result_get(limit = 0)` returns metadata/schema with no rows.
- `result_get` with `offset >= row_count` returns an empty non-error page.
- Unknown, expired, and evicted handles return clear MCP invalid-params errors.
- JSON-safe number behavior is preserved for stored-result slices.
- Handles are isolated to one MCP server process and are not persisted.
- Store limits prevent unbounded memory growth.
- Docs explain handle lifetime and follow-up usage.
- Targeted tests pass:
  - `cargo test --locked -p coral-client result_slice`
  - `cargo test --locked -p coral-mcp result`
- Final Rust gate passes before PR:
  - `make rust-checks`

## Open Questions

- Should handled SQL results always include all column summaries, or should very
  wide results cap schema summaries and ask for `result_get(limit = 0)`?
- Should future renderer-specific SQL text optimizations compose before or
  after the handle threshold decision?
- What exact token benchmark threshold should block regressions?
- Should result handles be feature-gated initially, or ship as the default MCP
  behavior once tests and docs are in place?
