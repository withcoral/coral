# Dependent Predicate Pushdown — V1 Design

**Status:** Draft
**Date:** 2026-05-06
**Owner:** Ilia Aphtsiauri
**Scope:** V1 — inner equi-join pushdown for HTTP/API-backed sources, on a substrate that future operators (semi-join, scalar apply, lateral join, dynamic-filter pruning) can extend without re-architecting.

## 1. Problem

Coral can push literal `WHERE` predicates into a single source's API today, but it cannot carry values discovered while scanning one source and use them to constrain another source. The result is broad fetches followed by local filtering: high latency, memory pressure, and rate-limit pressure. The canonical example is "show GitHub PR status for all in-progress Linear issues," where Coral currently fetches every visible PR and filters locally instead of issuing one PR lookup per matched Linear issue.

The same gap blocks sources whose required filters must be resolved at runtime — for example, `slack.messages` requires a `channel` filter, but Coral cannot fulfil it from a join against `slack.channels`.

This document specifies dependent predicate pushdown. The broader capability lets Coral carry runtime-derived values across query subtrees and bind them to source-declared API filters. Sources declare which filters can be supplied dynamically (a *table-local capability*, not a hard-coded source-pair rule); the planner uses those declarations to plan bounded, rate-limit-aware dependent fetches while preserving SQL correctness.

V1 ships the safest subset: dependent inner equi-joins for HTTP/API-backed tables. The architecture is deliberately shaped so that later phases can add scalar subqueries, semi-joins, lateral joins, dynamic-filter scan pruning, multi-source resolver subtrees, and composite key bindings without re-architecting.

DataFusion dynamic filters are explicitly *not* the V1 mechanism. Their documented join behaviour is generic runtime pruning, often min/max-based, and does not guarantee exact correlated key tuples or required API parameter handling. They remain a complementary future option for opportunistic scan narrowing.

## 2. Architecture

### 2.1 Layered design

The design separates three layers so future dependent-pushdown operators reuse the lower two:

```
┌────────────────────────────────────────────┐
│  Operator-shaped consumers                 │
│  V1:    DependentJoinExec (inner equi)     │
│  later: DependentSemiExec, scalar apply,   │
│         dynamic-filter-source bridge       │
└─────────────┬──────────────────────────────┘
              │
┌─────────────▼──────────────────────────────┐
│  Reusable core: BindingFetcher             │
│  - resolves manifest bindable flag         │
│  - dedups + caps binding tuples            │
│  - bounded-concurrency dispatch            │
│  - merges literal + correlated filters     │
│  - returns rate-limit-aware row stream     │
└─────────────┬──────────────────────────────┘
              │
┌─────────────▼──────────────────────────────┐
│  Source capability layer (coral-spec)      │
│  FilterSpec.bindable: bool                 │
│  FilterSpec.max_bindings: Option<usize>    │
│  source.rate_limit.max_concurrency         │
└────────────────────────────────────────────┘
```

The bottom layer is a *table-local capability*, not a pair-specific rule. Any planner-level pattern matcher (V1 join, future semi-join, future scalar apply) consults `FilterSpec.bindable` and uses the same `BindingFetcher` core to convert "I have these N tuples" into "fetch these N times, dedup, cap, bound concurrency, propagate the first error."

### 2.2 V1 pieces

1. **Manifest surface** in `coral-spec`: `bindable: bool` on `FilterSpec`, optional per-filter `max_bindings: usize`, optional per-source `rate_limit.max_concurrency: usize`. Existing manifests parse and run identically.
2. **Logical optimizer rule** in `coral-engine`: pattern-matches `LogicalPlan::Join` (inner equi, conjunction-of-equalities), confirms exactly one join input reduces to an HTTP `TableScan` (after peeling safe `Filter`/`Projection` wrappers) whose joined keys correspond to `bindable: true` filters, and rewrites to `LogicalPlan::Extension(DependentJoinNode)`. The resolver subtree is opaque, so multi-source resolvers work without extra effort. Pattern misses fall back to standard execution.
3. **Physical lowering** in `coral-engine`: a custom `ExtensionPlanner` (registered via a `QueryPlanner` extension on the `SessionContext`) lowers `DependentJoinNode` into `DependentJoinExec`. The planner resolves `dependent_table` against the catalog, downcasts the provider to `HttpSourceTableProvider`, and constructs the exec with the live `HttpSourceClient` and `HttpTableSpec`. `ExtensionPlanner` is the DataFusion-blessed seam for logical-extension lowering; physical optimizer rules are reserved for plan-shape optimisation, not primary lowering.
4. **Physical execution** in `coral-engine`: `DependentJoinExec` is an Apply-style operator owning the resolver child plus the resolved dependent handles. At `execute()` it drains the resolver, derives distinct binding tuples, dispatches `HttpSourceClient::fetch` calls under the source's existing rate limiter (capped by `max_concurrency`), and emits joined rows.

### 2.3 What does not change

The single-source literal pushdown path (`HttpSourceTableProvider::supports_filters_pushdown` plus `scan`) is untouched. Tables with no `bindable: true` filters behave identically. Tables with bindable filters that are not joined still go through the existing path. The new operator is purely additive.

### 2.4 Future extension points (designed-in, not built in V1)

- **Correlated scalar subqueries.** Add a sibling pattern matcher that produces a `DependentScalarNode`; new exec node, same `BindingFetcher` core.
- **Semi/anti-joins** (`WHERE col IN (subq)` after de-correlation). New extension node, reusing `BindingFetcher`; emits resolver rows (semi) or non-matching rows (anti).
- **Lateral / `LEFT JOIN LATERAL`.** Extension node that emits null-padded rows for unmatched bindings; cap and concurrency same.
- **DataFusion dynamic filters as opportunistic pruning.** A possible follow-up direction, not a designed-in V1 API. Once the binding set is known at runtime, an operator could in principle publish a `DynamicPhysicalExpr` for sibling scans on the same source. Implementation details (lifecycle, plan integration, cost) deserve a separate design when a concrete use case appears. Complementary to V1's exact correlated pushdown, never a substitute.
- **Per-source resolver dialects.** `BindingFetcher` is HTTP-specific in V1 because all bindable backends today are HTTP. Its trait boundary (`HttpSourceClient::fetch`) can be widened to a `BindableBackend` trait when a non-HTTP backend (for example, GraphQL with batched lookups) needs to opt in.

### 2.5 Crate layout

```
coral-engine/
  src/runtime/dependent_join/
    mod.rs                 # rule registration; public surface
    logical.rs             # DependentJoinNode (LogicalPlan::Extension)
    optimizer.rs           # logical + physical optimizer rules
    exec.rs                # DependentJoinExec
    fetcher.rs             # reusable BindingFetcher (V2/V3 reuse seam)
    bindings.rs            # tuple projection, dedup, cap enforcement
```

## 3. Manifest grammar

### 3.1 `FilterSpec` additions (in `coral-spec/src/common.rs`)

```rust
pub struct FilterSpec {
    pub name: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub mode: FilterMode,
    #[serde(default)]
    pub bindable: bool,                // NEW
    #[serde(default)]
    pub max_bindings: Option<usize>,   // NEW; None → engine default
}
```

YAML:

```yaml
filters:
  - name: channel
    required: true
    bindable: true
    max_bindings: 200
  - name: oldest
  - name: latest
```

### 3.2 Source-level concurrency knob

The concurrency cap reuses the existing `rate_limit` block as a new optional field. No new top-level grammar:

```yaml
rate_limit:
  ...
  max_concurrency: 8
```

### 3.3 Interactions

- `bindable` is orthogonal to `required`. All four combinations are valid: required-and-bindable (Slack `channel`), bindable-only, required-only (today's behaviour), neither.
- `bindable: true` requires `mode: equality`. Combining with `mode: search` or `mode: contains` is rejected at validation time. Non-equality binding has no clean V1 fetch contract.
- A bindable filter still accepts literal pushdown unchanged. The new operator triggers only when a join supplies a binding tuple.

### 3.4 Validation rules

`coral-spec` accepts `bindable: true` on any backend. The spec layer is intentionally engine-agnostic: it captures the *capability declaration*, not which engine version supports it.

Spec-layer rules (additions to `coral-spec/src/validate.rs`):

1. `bindable: true` with `mode != equality` → error: `filter '<name>': bindable=true requires mode=equality (V1)`.
2. `max_bindings` must be ≥ 1 if present.
3. `rate_limit.max_concurrency` must be ≥ 1 if present.

Engine-layer rule (in `coral-engine` source registration, alongside the existing source compile path):

4. If a source declares `bindable: true` filters but its backend is not HTTP, registration emits a `SourceRegistrationFailure` with detail `source '<name>': bindable filters are not supported by the current engine for backend '<kind>' (V1)`. Lifted when a `BindableBackend` trait widens the substrate. Other features of the source remain valid — only the bindable capability is engine-rejected.

### 3.5 Engine-wide defaults

```rust
pub const DEFAULT_MAX_BINDINGS: usize = 100;
pub const DEFAULT_BINDING_CONCURRENCY: usize = 8;
pub const DEFAULT_MAX_RESOLVER_ROWS: usize = 1000;
```

Resolution order at runtime, highest priority first:

| Knob | Order |
| --- | --- |
| `max_bindings` | filter `max_bindings` → `DEFAULT_MAX_BINDINGS` |
| `max_concurrency` | source `rate_limit.max_concurrency` → `DEFAULT_BINDING_CONCURRENCY` |
| `max_resolver_rows` | engine constant only in V1 (`DEFAULT_MAX_RESOLVER_ROWS`) — no manifest knob; future revision may add a session config or manifest field |

`max_bindings` caps **distinct binding tuples**. `max_resolver_rows` caps **total resolver rows ingested**. Both must hold; either being exceeded is an error. They cap different shapes of overrun: `max_bindings` prevents fanning out into too many HTTP calls; `max_resolver_rows` prevents the resolver buffer from growing unboundedly even when distinct-tuple count stays small (e.g. one million resolver rows that all share the same tuple still need to be buffered to emit joined rows).

For composite bindings (multiple bindable filters on one table), `max_bindings` is the **minimum** of all participating filters' caps. The most restrictive value wins; this is documented so source authors know that a permissive cap on one filter cannot accidentally enable unbounded fetches.

### 3.6 Documentation

Add a "Bindable filters" subsection to `docs/reference/source-spec-reference.mdx` with one example per shape: Slack messages (single required bindable), GitHub pull requests (composite tuple), Linear issues (optional bindable).

### 3.7 Pilot manifests

V1 lands the following filters as `bindable: true`:

- `slack.messages.channel` — required, single-column.
- `github.pull_requests.{owner, repo, number}` — composite tuple.
- `linear.issues.team_id` — optional, single-column.

Each pilot ships with at least one integration test (Section 7).

## 4. Detection (logical-plan rewrite)

### 4.1 Pattern

A new `OptimizerRule` registered with the `SessionContext` walks the logical plan. It accepts a `LogicalPlan::Join` if and only if all of:

1. `join_type == JoinType::Inner`.
2. `on` is a non-empty conjunction of equalities `(left_expr_i = right_expr_i)`. No `OR`, no inequalities, no expressions on either side beyond `Column` (with optional unwrappable casts).
3. Exactly one of the join's two inputs reduces to a `TableScan` whose provider is `HttpSourceTableProvider` (downcast via `provider.as_any()`), after peeling a stack of *safe wrappers* — `LogicalPlan::Filter` and `LogicalPlan::Projection` only — that surround the scan. Other plan shapes between the join and the scan disqualify the side. The matching side is the *dependent side*; the other is the *resolver side*. SQL position (LHS or RHS) is irrelevant; both orientations are normalised.
4. The post-`ON` `filter` is a conjunction of pushable equalities against the dependent side's columns, or empty. Predicates carried by a peeled `Filter` wrapper on the dependent side are merged into this set.
5. Every dependent-side column appearing in `on` corresponds to a `FilterSpec` with `bindable: true` on the dependent table. Mixed (some bindable, some not) → fall back.
6. Required filters not satisfied by join keys are satisfied by literal pushable predicates from the join `filter` or any peeled `Filter` wrapper. Otherwise, fall back; the existing `MissingRequiredFilter` error fires at scan time, unchanged.
7. No bindable filter is over-constrained — that is, bound by both a join key and a literal predicate. Fall back, do not error.

Wrapper peeling is purely structural: the rule recognises the wrappers, records their predicates / projection columns, and rebuilds them above the new extension node so the surrounding plan stays semantically identical.

### 4.2 `DependentJoinNode`

The logical extension node carries only stable logical metadata. Runtime handles such as `TableProvider` are resolved during physical planning, not stored here.

```rust
struct DependentJoinNode {
    resolver: Arc<LogicalPlan>,
    dependent_table: TableReference,      // catalog.schema.table — stable; supports Eq/Hash
    dependent_table_schema: DFSchemaRef,  // dependent table's column schema (logical)
    binding_keys: Vec<BindingKey>,
    literal_filters: HashMap<String, String>,
    projection: Option<Vec<usize>>,
    schema: DFSchemaRef,                  // matches the original Join's schema
}

struct BindingKey {
    resolver_column: Column,
    dependent_filter: String,             // FilterSpec name (string identifier)
    coercion: Option<CastInto>,           // resolver type → filter wire type
}
```

`dependent_table_schema` is captured at logical-rewrite time so column resolution and type coercion logic (Section 4.4) does not need a live provider. Required-filter satisfaction (Section 4.1 condition 6) and bindable-filter membership (condition 5) are checked at rewrite time against the manifest-derived `HttpTableSpec` reachable through the source registry — only the *check* runs at rewrite; the resolved spec is not embedded in the node.

The `schema` field reuses the original `Join`'s output schema so that parent plan nodes do not need updating.

During physical planning (Section 5), the custom `ExtensionPlanner` resolves `dependent_table` against the `SessionContext` catalog, downcasts the resulting provider to `HttpSourceTableProvider`, and constructs the `DependentJoinExec` with the live `HttpSourceClient` and `HttpTableSpec`. Resolution failures (provider missing or non-HTTP) are surfaced as a planner-level error rather than silent fallback at this stage — by the time we reach physical planning, the optimizer rule has already accepted the rewrite, so a missing provider is a registration bug, not a normal pattern miss.

### 4.3 Cast and type coercion

A `BindingKey` records the cast required (if any) from the resolver column's Arrow type to the filter's wire type (today: string after `literal_to_string`). Two-phase handling:

- **Planning time (logical rewrite).** If the resolver column type and the filter wire type have no defined coercion (per Arrow's coercion rules + `literal_to_string`'s accepted scalar set), the rule falls back to standard execution with `reason = non_coercible`. Type incompatibility is decided up-front, not at row time.
- **Runtime (`bindings.rs`).** A NULL value drops the row (inner-join semantics). A non-NULL value that the planning-time check declared coercible but that fails to convert at runtime (e.g. an `Int64` value outside a target range) raises `DependentJoinCoercionFailed { schema, table, filter, value_repr }` rather than silently dropping the row. Silent drop is reserved for NULLs only.

### 4.4 NULL handling

Tuples with any NULL in a binding column are dropped before fetching. Inner-join semantics already drop these rows; pre-filtering saves a round-trip.

### 4.5 Distinct-tuple dedup

Performed inside `bindings.rs` over a hash of the projected tuple. A side table `HashMap<Tuple, SmallVec<resolver_row_id>>` is kept so the operator can join dependent rows back to the originating resolver rows for output emission.

### 4.6 Fallback path

When any check fails, the optimizer leaves the `Join` untouched and emits `tracing::debug!` with a `reason` field: `"non_inner"`, `"non_equi"`, `"mixed_bindable"`, `"missing_required"`, `"over_constrained"`, `"non_http_provider"`, `"non_coercible"`, `"non_peelable_wrapper"`. Default DataFusion log level is silent; `RUST_LOG=coral_engine::dependent_join=debug` surfaces it.

### 4.7 Statistics

The extension node reports `Statistics::new_unknown` for V1. Future improvement once binding cardinality is observed and recorded.

### 4.8 Why rewrite at the logical layer

Rewriting at the logical level lets the `ExtensionPlanner` produce children for `DependentJoinExec` through standard physical planning (resolver subtree on the resolver side, resolved dependent handles via catalog lookup), and lets DataFusion's cost model see `DependentJoinNode` as a single boundary instead of mid-physical-plan surgery on `HashJoinExec`. The approach is also robust against future DataFusion physical-planning changes.

## 5. Runtime execution

### 5.1 Struct shape

```rust
pub(crate) struct DependentJoinExec {
    resolver: Arc<dyn ExecutionPlan>,
    dependent: HttpSourceClient,
    table: Arc<HttpTableSpec>,
    binding_keys: Arc<[BindingKey]>,
    literal_filters: Arc<HashMap<String, String>>,
    max_bindings: usize,
    max_resolver_rows: usize,
    max_concurrency: usize,
    page_hint: Option<usize>,            // pagination-step hint only; not semantic
    projection: Option<Vec<usize>>,
    output_schema: SchemaRef,            // matches the original Join's schema
    props: Arc<PlanProperties>,
}
```

`children()` returns `[&resolver]`. `with_new_children` rebuilds the resolver child only.

### 5.2 Execution flow

1. **Drive the resolver child to completion.** Collect each `RecordBatch` into a small `Vec<RecordBatch>`. Extract binding tuples per row using `binding_keys` (with cast and NULL-skip). Maintain a side table `bindings_by_tuple: HashMap<Tuple, SmallVec<(batch_idx, row_idx)>>`. Track two counters:
   - **resolver row count** (running total of rows ingested). On exceeding `max_resolver_rows`, abort drain and return `DependentJoinResolverRowsExceeded` before any HTTP traffic.
   - **distinct tuple count.** On exceeding `max_bindings` after drain (or eagerly during drain), return `DependentJoinCardinalityExceeded` before any HTTP traffic.

   If drain completes with zero distinct tuples, return an empty stream.
2. **Spawn a capped-concurrency dispatcher.** A `tokio::Semaphore(max_concurrency)` bounds in-flight calls. For each distinct tuple: acquire a permit; build `filters = literal_filters ∪ tuple_to_filter_map(tuple)`; call `HttpSourceClient::fetch(&table, &filters, page_hint)` (the third argument is the pagination-step hint, not a semantic limit — see Section 5.6); on `Ok(rows)`, send `(tuple, rows)` onto an mpsc channel; on `Err(e)`, propagate, cancel siblings, first error wins.
3. **Stream record batches out.** Convert each fetched JSON row via the table's mapping converter. For each dependent row, look up `bindings_by_tuple[tuple]` to find resolver rows, splice each (resolver, dependent) pair into a joined `RecordBatch`, apply the output projection, and yield via `RecordBatchStreamAdapter`.

### 5.3 Why the resolver is fully drained before any fetch starts

1. Cap enforcement is exact only after the resolver is drained. Streaming dispatch could issue calls that should never have happened, then have to error out partway through.
2. Dedup of binding tuples requires the full set; partial dedup wastes calls.
3. Resolver subtrees are typically small (tens to hundreds of rows). If a future use case has very large resolvers, we revisit with a chunked-streaming variant.

### 5.4 `BindingFetcher` core

```rust
pub(crate) struct BindingFetcher {
    client: HttpSourceClient,
    table: Arc<HttpTableSpec>,
    literal_filters: Arc<HashMap<String, String>>,
    semaphore: Arc<Semaphore>,
    page_hint: Option<usize>,                 // pagination-step hint; not semantic
}

impl BindingFetcher {
    pub(crate) fn dispatch(
        &self,
        tuples: Vec<Tuple>,
    ) -> mpsc::Receiver<Result<(Tuple, Vec<Value>)>> { /* ... */ }
}
```

This is the V2/V3 reuse seam. Future operators construct a `BindingFetcher` and consume its receiver with their own emission policy.

### 5.5 Literal-filter merging

`literal_filters` is the set captured at logical-rewrite time from the join `filter` plus any peeled dependent-side `Filter` wrapper. Merge is plain map union; literal-bindable conflicts were already rejected as over-constrained in the optimizer rule. Required filters not bound by either source never reach this exec — they triggered fallback at logical rewrite.

### 5.6 `sql_limit` semantics

V1 treats any `Limit` above the join as a parent-enforced bound only. The dependent fetch does **not** receive a per-binding semantic limit, because `LIMIT N` over a multi-binding join is not equivalent to `LIMIT N` per binding (early-stopping per binding can drop rows the parent would have selected when ordering or further filtering happens above the join).

Concretely:

- `DependentJoinExec` does **not** propagate a parent-`LIMIT` value into `HttpSourceClient::fetch` as `sql_limit`.
- A separate hint, `page_hint: Option<usize>`, may be derived (default: the source's existing `fetch_limit_default`). It is forwarded as a *page-size / pagination-step* hint only — never as a completeness boundary. The fetcher must complete its pagination loop normally for each binding.
- Correctness for the parent `LIMIT` is enforced by DataFusion's standard `LimitExec` above the dependent join.

This is intentionally conservative for V1. A future revision can prove safe early-stop conditions (single-binding scans without ordering above, parent `LIMIT` plus no parent `Filter`/`Sort`, etc.) and pass through a true semantic limit.

### 5.7 Resolver row materialisation

Resolver rows are kept as `Vec<RecordBatch>` plus a `(batch_idx, row_idx)` index in the side table. Row construction at emission time groups indices by `batch_idx` and uses Arrow's `take` kernel column-by-column against each batch — no row-by-row copy. Same pattern as DataFusion's `HashJoinExec` build side.

### 5.8 Display and EXPLAIN

```
DependentJoinExec: source=github table=pull_requests
  binding_keys=[number ← outer.col2, owner ← outer.col0, repo ← outer.col1]
  literal_filters={state="open"}
  max_bindings=200 max_concurrency=8
  ResolverChild: ...
```

Visible in `EXPLAIN` and `EXPLAIN ANALYZE`.

### 5.9 Metrics

Standard `BaselineMetrics` plus three custom `Count` metrics:

- `binding_count` — distinct binding tuples after dedup.
- `fetch_count` — actual HTTP calls (equal to `binding_count` in V1).
- `resolver_rows` — rows consumed from the resolver child.

Surfaced via `EXPLAIN ANALYZE`.

### 5.10 Thread-safety

`HttpSourceClient` is already `Clone + Send + Sync`. `HttpTableSpec` is `Clone + Send + Sync`. `Tuple` is a small `Vec<ScalarValue>` wrapper. No new shared mutable state.

## 6. Errors and limits

### 6.1 Cap resolution at exec build time

Deterministic; no runtime drift:

```text
max_bindings    := filter.max_bindings
                ?? DEFAULT_MAX_BINDINGS                  (= 100)
max_concurrency := source.rate_limit.max_concurrency
                ?? DEFAULT_BINDING_CONCURRENCY           (= 8)
```

For composite bindings, `max_bindings` is the **minimum** across participating filters' caps.

### 6.2 Error contract

Three new variants in `coral-engine/src/backends/http/error.rs`:

```rust
DependentJoinCardinalityExceeded {
    schema: String,
    table: String,
    observed: usize,
    cap: usize,
    cap_source: CapSource,           // FilterDecl | EngineDefault
    binding_filters: Vec<String>,
}

DependentJoinResolverRowsExceeded {
    schema: String,
    table: String,
    observed: usize,
    cap: usize,
}

DependentJoinCoercionFailed {
    schema: String,
    table: String,
    filter: String,
    value_repr: String,              // truncated, redacted scalar repr
}
```

User-facing message:

```
dependent join into 'github.pull_requests' produced 487 binding tuples,
exceeds cap of 200 (from filter 'number'). Narrow the resolver subtree
or raise 'max_bindings' on filter 'number' in the github source manifest.
binding_filters=[owner, repo, number]
```

`cap_source` surfaces *which* manifest knob to change. The message names the smallest-cap filter when caps differ.

### 6.3 Failure paths and dispositions

| Path | Where | Disposition |
| --- | --- | --- |
| Cardinality overflow (distinct binding tuples > `max_bindings`) | `bindings.rs` after dedup | hard error `DependentJoinCardinalityExceeded` |
| Resolver rows overflow (total resolver rows > `max_resolver_rows`) | `bindings.rs` during drain | hard error `DependentJoinResolverRowsExceeded`; drain aborts immediately |
| Required + bindable filter not satisfied (no literal, no join key) | logical optimizer | rule falls back; existing `MissingRequiredFilter` fires at scan time, unchanged |
| Resolver produced zero distinct tuples | `DependentJoinExec` | empty output stream, zero HTTP calls, success |
| Pattern mismatch (non-inner, non-equi, etc.) | logical optimizer | fall back to standard execution + `tracing::debug!` with `reason` |
| Per-fetch HTTP error during dispatch | `BindingFetcher` | first error cancels dispatcher (drops semaphore, closes mpsc), propagates upward; partial results discarded |
| Runtime coercion failure on a non-NULL binding value | `bindings.rs` per row | hard error `DependentJoinCoercionFailed` (planning-time check declared compatibility; runtime conversion failed) |
| NULL value in a binding column | `bindings.rs` per row | row dropped (inner-join semantics); not an error |
| Planning-time type incompatibility | logical optimizer | rule falls back; reason `non_coercible` |
| Resolver child error before drain completes | `DependentJoinExec` | propagate; dispatcher never starts |
| Dependent source has `bindable` filters but a non-HTTP backend | `coral-engine` source registration | source registration failure; surfaced as `SourceRegistrationFailure` (Section 3.4 rule 4) |

### 6.4 Diagnostic logging

`target = coral_engine::dependent_join`:

- `info!` once per match: `source`, `table`, `binding_keys`, `literal_filter_count`, `cap`.
- `debug!` once per fallback: `source`, `table`, `reason`.
- `info!` per executed dependent-join: `binding_count`, `fetch_count`, `resolver_rows`, `elapsed_ms`.
- `warn!` on first per-fetch error: `source`, `table`, `tuple_repr`, `error_kind`.

No new tracing spans; joins inherit the existing query span.

### 6.5 Rate-limit interaction

`HttpSourceClient`'s existing per-source rate limiter sits *inside* `fetch()`. The semaphore in `BindingFetcher` caps in-flight calls; the rate limiter caps calls per window. Both apply. `max_concurrency` is an upper bound, not a guarantee of parallelism — calls queue inside `fetch()` if the rate limiter blocks.

### 6.6 Timeout

Per-fetch timeouts are governed by the existing `request_timeout` on `HttpSourceClient`. The dependent-join exec adds no separate timeout. A pathologically slow source eventually errors via the per-fetch timeout, which propagates as an all-or-nothing failure.

### 6.7 Cancellation

Dropping the output stream:

1. closes the mpsc receiver,
2. dispatcher tasks observe channel-closed on next `send` and return early,
3. semaphore permits release as tasks unwind,
4. in-flight reqwest calls abort via reqwest's tokio cancellation.

Standard tokio drop semantics; no leaks.

### 6.8 Memory bounds

Two independent caps protect against two failure modes:

- **Resolver buffer:** ≤ `max_resolver_rows` × resolver row width. Bounds the case where many resolver rows share the same tuple (`max_bindings` alone would not cap this).
- **Binding-tuple side table:** ≤ `max_bindings` distinct tuples × (tuple width + resolver row index list size). The index list per tuple is bounded by `max_resolver_rows` overall, not per tuple.
- **Per-fetch payload:** bounded by `fetch_limit_default × max_concurrency` bytes resident at any moment.

No new unbounded structures. A query that would exceed either cap fails before any HTTP traffic is issued.

### 6.9 Backpressure

The mpsc channel is sized to `max_concurrency` slots. Producers block on a full channel — the standard DataFusion streaming pattern.

## 7. Testing

Three layers plus one property test.

### 7.1 Layer 1 — `coral-spec` unit tests

In `crates/coral-spec/src/validate.rs`:

| Test | Expectation |
| --- | --- |
| `bindable: true` with `mode: equality` | accepts |
| `bindable: true` with `mode: search` | rejects |
| `bindable: true` with `mode: contains` | rejects |
| `max_bindings: 0` | rejects |
| `max_bindings: 1` | accepts |
| `rate_limit.max_concurrency: 0` | rejects |
| Manifest with no `bindable` filters | round-trips identically (regression guard for grammar additivity) |
| `bindable: true` on jsonl/parquet/file backend | accepts (spec-layer agnostic; engine layer is the gate) |

Engine-layer registration tests (added to existing source-registration suite in `coral-engine`):

| Test | Expectation |
| --- | --- |
| `bindable_filter_on_jsonl_source_fails_registration` | `SourceRegistrationFailure` with detail naming the backend kind and "(V1)" |
| `bindable_filter_on_http_source_registers` | source registers normally |

### 7.2 Layer 2 — optimizer rule unit tests

In `crates/coral-engine/src/runtime/dependent_join/optimizer.rs`. Build `LogicalPlan` directly via DataFusion's builder API; assert the rewrite output. Each test names the variant explicitly:

- `inner_equi_single_binding_rewrites`
- `inner_equi_composite_rewrites`
- `inner_equi_mixed_bindable_falls_back`
- `inner_equi_over_constrained_falls_back`
- `inner_equi_missing_required_falls_back`
- `non_inner_join_falls_back`
- `non_equi_join_falls_back`
- `non_http_provider_falls_back`
- `swapped_sides_rewrites`
- `dependent_side_with_filter_wrapper_peels_and_merges_predicates`
- `dependent_side_with_projection_wrapper_peels_cleanly`
- `dependent_side_with_unsupported_wrapper_falls_back` (e.g. `Aggregate` between scan and join)
- `cast_compatible_binding_keys_normalize`
- `cast_incompatible_binding_falls_back`
- `dedup_in_logical_phase_is_not_done`

### 7.3 Layer 3 — `DependentJoinExec` unit tests

In `crates/coral-engine/src/runtime/dependent_join/exec.rs`. A `MockBindableBackend` satisfies the same interface `BindingFetcher` consumes, verifying operator semantics independent of HTTP:

- `empty_resolver_yields_empty_stream_and_zero_calls`
- `single_tuple_one_call`
- `composite_tuple_one_call_per_distinct`
- `dedup_collapses_duplicate_tuples`
- `null_binding_drops_row`
- `cast_applied_to_binding_value`
- `cap_overflow_returns_dependent_join_cardinality_exceeded`
- `cap_overflow_when_filter_unset_uses_engine_default`
- `cap_minimum_wins_for_composite`
- `resolver_rows_cap_exceeded_returns_resolver_rows_exceeded` (many rows, all same tuple → trips `max_resolver_rows` even with low distinct-tuple count)
- `resolver_rows_cap_aborts_drain_before_any_http_call`
- `concurrency_cap_honored`
- `first_error_cancels_dispatcher`
- `cancellation_drops_in_flight`
- `parent_limit_does_not_propagate_as_semantic_sql_limit` (asserts `fetch` argument is the page hint, not the parent `LIMIT`)
- `runtime_coercion_failure_raises_typed_error_not_silent_drop`
- `metrics_count_resolver_rows_bindings_and_fetches`

### 7.4 Layer 4 — integration tests

In `crates/coral-engine/tests/`, alongside existing wiremock suites. Real `HttpSourceClient` against `wiremock`. One fixture per pilot manifest:

- `slack_messages_via_channel_join_dispatches_per_channel`
- `slack_required_filter_satisfied_by_join`
- `slack_required_filter_unsatisfied_without_join`
- `github_pr_composite_binding_dispatches_per_tuple`
- `github_pr_with_literal_owner_repo`
- `linear_optional_bindable_team_id_falls_back_when_absent`
- `cross_source_linear_to_github_pr_status_e2e`
- `cap_overflow_surfaces_user_error`
- `resolver_rows_overflow_surfaces_user_error`
- `dispatcher_first_error_cancels_others`
- `concurrency_cap_observed_under_load`
- `non_pushable_join_falls_back_silently`

Wiremock fixtures live under `crates/coral-engine/tests/fixtures/dependent_join/`.

### 7.5 Property test

A small `proptest` harness generates randomised inner-equi joins (1–4 binding keys, 1–50 resolver rows, types ∈ {Utf8, Int64, Bool}, optional NULLs, optional duplicates, optional literal predicates) and asserts: fetch count equals distinct non-NULL tuples; output rowset matches standard inner-join semantics; no spurious calls. Catches dedup, cast, and NULL-handling regressions across shapes.

### 7.6 Bench (deferred)

Latency comparison "resolver → broad fetch + local filter" versus "dependent join" lives in `xtask` benches and is deferred to a follow-up. Pilot manifest integration tests are sufficient signal for V1 sign-off.

### 7.7 CI surface

Runs under the existing `cargo test --workspace` and `make test` targets. No new CI job; wiremock is already a dev-dependency of `coral-engine`.

## 8. Out of scope for V1

- Outer joins (left, right, full).
- Correlated scalar subqueries.
- IN-subquery decorrelation pre-pass (`WHERE col IN (SELECT ...)`) — would need to land in a separate ticket with concrete query motivations.
- Lateral joins.
- Aggregation pushdown.
- Semantic `LIMIT` propagation into per-binding fetches — requires proof of safe-early-stop conditions; deferred.
- DataFusion dynamic-filter scan pruning — captured as a follow-up direction, not designed-in API. Implementation evaluated separately when a concrete sibling-scan use case lands.
- SQL-warehouse / GraphQL federation via `datafusion-federation` (orthogonal; future).
- Per-query session overrides for caps (`SET coral.dependent_join.max_bindings = ...`) — possible follow-up.
- Non-HTTP backends declaring `bindable` filters — gated behind a `BindableBackend` trait widening.

## 9. Migration and rollout

V1 is purely additive. Existing manifests parse and run unchanged. Pilot manifests (`slack`, `github`, `linear`) gain `bindable: true` markers in the same PR; the absence of a join in user SQL leaves behaviour identical to today. Feature lands behind no flag — the optimizer rule is always registered, but it is a no-op when no manifest declares `bindable`.

## 10. Open questions

None for V1. Follow-up tickets capture: (a) per-query cap overrides via session config, (b) `BindableBackend` trait widening for non-HTTP backends, (c) `DynamicPhysicalExpr` bridge for opportunistic sibling scan pruning.
