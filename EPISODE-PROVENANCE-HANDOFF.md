# Episode Provenance / Trajectory Memory Handoff

Branch: `codex/episode-provenance`

Audience: an engineer who has not been in the prior discussion and needs enough
context to take the idea forward, not just review the patch.

## Short version

Agents do not just query tools. They discover values, pick some of those values
up, and use them in later calls. That reuse is valuable evidence.

The idea is to make Coral record those input/output continuities as first-class
episode data. Then we can ask questions like:

- What happened in this session?
- Which earlier call produced the value used later?
- What source columns are repeatedly connected by real successful queries?
- Which calls were actually on the useful path to the final answer?
- Can we mine repeated agent workflows into reusable recipes?

The current branch is a local first slice: append-only receipts plus SQL tables
under `coral_provenance`. It is not the whole vision. It is meant to prove the
basic loop is useful before we build heavier planner lineage, row continuity, UI
surfaces, or workflow compilation.

## Why this matters

Coral sits at a good layer for this problem. It already sees live data access
through SQL, MCP, catalog discovery, and source adapters. That means Coral can
observe the actual tool calls and actual returned data, instead of trying to
reconstruct agent behavior from chat text.

The useful thing to mine is not "the LLM thought X". We cannot reliably inspect
that. The useful thing is simpler:

1. Call A returned value/entity X.
2. Later Call B used X as an input.
3. Therefore the episode has a concrete continuity edge from A to B.

This does not have to prove philosophical causality to be useful. It is enough
that the value was not available to the agent before Call A, then appeared in a
later input. For product purposes that is causal enough: A enabled B.

This gives us a substrate for debugging, summarizing, pruning, and eventually
compiling workflows.

## Key concepts

Episode:

The session-like container. Episodes should be first-class objects, not a graph
we derive after the fact. A trajectory is one way to look at an episode.

Call:

One Coral-visible operation: SQL execution, SQL explain, MCP/catalog discovery,
or eventually connector/tool call. Calls have inputs, outputs, status, timing,
workspace, and operation name.

Occurrence:

One observed entity or scalar value inside a call input or output. Examples:

- `table:github.pull_requests`
- `column:github.pull_requests.html_url`
- `url:<sha256>`
- `number:<sha256>`
- `value:<sha256>`

Scalar values should use stable hashes as identity keys, with short previews for
debuggability. The hash is a correlation key, not a privacy boundary.

Binding:

Evidence that a later input occurrence reused an earlier output occurrence.
The current rule is:

> Edge = nearest earlier call in the same episode whose output occurrences
> contain this later input entity.

That rule gives time-respecting edges. An edge cannot go backwards in time.

Continuity context:

A flowing context that can pass through calls. A simple value binding is one
continuity context. A row-derived context is richer: Call A returned row R,
column `html_url` had hash H, Call B used H as `attachment.url`, and Call C then
used another value derived from the row returned by B. To preserve that chain we
need row fingerprints and derivation edges, not just value equality.

## Trajectory graph vs causal tuples

There are two related products here.

Full trajectory:

A call-level view of the episode. This is what people intuitively ask for when
they say "show me the graph" or "show me the tree". It is good for replay,
debugging, pruning, and workflow compilation. It makes stronger assumptions
about how agent calls relate.

Causal tuples:

Fine-grained evidence rows like:

```text
output github.search_issues.html_url
  -> input linear.attachment.url
  via value hash H
  in episode E
```

This has fewer assumptions. It does not need to know the whole call graph. It
just says a value/entity moved from one observed output field to one observed
input field.

The current direction should favor causal tuples as the primitive and derive
trajectory views from them. That keeps the storage model honest while still
supporting graph/tree renderings.

## Why graphs are not always trees

Users often expect a trajectory to be a tree. That is sometimes true, but only
for a focused continuity view.

The raw episode graph is usually a DAG:

- one output can feed multiple later calls
- one later call can use values from multiple earlier calls
- common values can create accidental joins
- repeated queries can create multiple candidate parents

If we define the edge as "nearest earlier output for this later input", then
each input occurrence gets at most one parent. That can produce tree-like chains
for a single continuity context. The episode as a whole can still be a DAG.

That is fine. Store evidence rows. Render the view needed for the question.

## What DataFusion can add

The first implementation uses lightweight extraction. That is enough to prove
the loop, but Coral can do better because it uses DataFusion.

Planner/analyzer access can let us record relationships from resolved query
structure instead of regex/string parsing:

- resolved table references, including aliases
- resolved column references
- projection lineage
- filter predicates and literals
- join predicates
- function calls and derived expressions
- output schema
- row count and empty-result status

The important rule: only promote query-shape evidence when the query actually
returned rows. A join predicate is more meaningful if it produced results. An
empty join is still a useful failed experiment, but it should not become the
same kind of relationship evidence in a knowledge graph.

Useful evidence types DataFusion could emit:

- `query_referenced_table`
- `query_referenced_column`
- `query_filtered_column_by_value`
- `query_joined_columns`
- `query_projected_column`
- `query_derived_column`
- `query_returned_rows`
- `query_returned_empty`

This lets Coral mine relationships between columns in a DataFusion-native way.
For example, a successful query joining `github.pull_requests.html_url` to
`linear.attachments.url` gives relationship evidence between those columns.

## Why AST/planner beats string parsing

String parsing is fine for a prototype. It is not a reliable foundation.

The planner knows what the query means after aliases, quoting, catalog
resolution, function calls, casts, joins, subqueries, and projections. It can
distinguish a string literal from a column, a table name from a comment, and a
join predicate from arbitrary text.

Planner-level evidence gives us cleaner relationship mining, better false
positive control, and a route to row/column lineage.

## Storage shape we probably want

Do not start with RDF as the primary store. It is attractive for knowledge graph
projection, but it makes the operational path heavier too early.

Start with a relational/event model:

- append-only receipts for durability and auditability
- normalized SQL read models for agent/user queries
- later KG/RDF projection if needed

Suggested durable objects:

- `episodes`
- `calls`
- `call_inputs`
- `call_outputs`
- `occurrences`
- `bindings`
- `row_fingerprints`
- `column_relationship_evidence`
- `episode_summaries` or `episode_annotations`

Suggested binding/evidence fields:

- `evidence_id`
- `episode_id`
- `source_call_id`
- `target_call_id`
- `source_occurrence_id`
- `target_occurrence_id`
- `relationship_type`
- `evidence_kind`
- `direction`
- `observed_at`
- `confidence`
- `query_hash`
- `row_count`
- `source_path`
- `target_path`

RDF/KG can be a projection:

```text
column github.pull_requests.html_url
  related_to linear.attachments.url
  supported_by evidence rows [...]
```

The evidence rows should stay queryable in SQL.

## What the current branch implements

This branch adds a local prototype.

Raw storage:

```text
<coral config dir>/telemetry/provenance/events.jsonl
```

Queryable read model:

- `coral_provenance.episodes`
- `coral_provenance.calls`
- `coral_provenance.occurrences`
- `coral_provenance.bindings`

Current behavior:

- Episodes use the active OpenTelemetry trace ID when available.
- SQL execute/explain calls are recorded.
- Successful catalog list/search/describe/list-columns calls are recorded.
- SQL result scalar values are recorded as output occurrences.
- SQL string literals and simple `FROM`/`JOIN` table references are recorded as
  input occurrences.
- Bindings are created from later inputs to the nearest earlier matching output
  in the same episode.
- SQL over `coral_provenance` is recorded, but result rows are not re-ingested
  as output occurrences. This avoids self-feeding provenance loops.
- Query output occurrence extraction is capped at 512 scalar values per
  successful SQL call.

Key files:

- `crates/coral-app/src/provenance.rs`
- `crates/coral-app/src/query/manager.rs`
- `crates/coral-app/src/catalog/service.rs`
- `crates/coral-app/src/state/layout.rs`
- `crates/coral-engine/src/composition.rs`
- `crates/coral-engine/src/runtime/query.rs`
- `docs/guides/observe-with-opentelemetry.mdx`

## Useful SQL on the prototype

Recent episodes:

```sql
select *
from coral_provenance.episodes
order by last_call_unix_nanos desc;
```

Calls in an episode:

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

Calls that are on a connected input/output chain:

```sql
select distinct call_id
from (
  select source_call_id as call_id from coral_provenance.bindings
  union all
  select target_call_id as call_id from coral_provenance.bindings
)
order by call_id;
```

## Use cases to keep in mind

Recent trajectory inspection:

Show the last connected chains from the current agent session.

Audit and explainability:

Explain why the agent queried Linear after GitHub, or why a Datadog log value
showed up in a Sentry query.

Context pruning:

Identify calls that did not feed any later useful call or final answer. This
could make episode summaries shorter and sharper.

Workflow compilation:

Mine repeated call patterns into reusable workflows. Example: catalog search,
then SQL over GitHub, then lookup Linear attachments, then summarize linked
issues. This needs more than exact value bindings. It needs stable operation,
schema, column, predicate, and result-shape evidence.

Relationship mining / KG:

Discover that two columns from different sources are practically related because
agents or queries repeatedly connect them in successful episodes.

Source design feedback:

Find tables and columns agents repeatedly search for, join on, or filter by.
This can inform better source specs, guides, indexes, and examples.

Evaluation:

Measure useful-call ratio, dead-end exploration, repeated workflow quality, and
whether an agent converges through sensible evidence paths.

## Failure modes

Exact value matching is situational.

It works well for URLs, IDs, PR numbers, issue keys, hashes, trace IDs, emails,
and other stable values. It works poorly when the agent paraphrases, summarizes,
normalizes, truncates, or derives a value.

Common values create false positives.

Values like `true`, `open`, `main`, `default`, `1`, or common dates need entropy
or column-aware handling. Otherwise they will connect unrelated calls.

Row context can be lost.

If Call A returns a row and Call B uses one column from that row, later uses of
other columns from B need row-level continuity to preserve the chain. This is
where row fingerprints matter.

Empty results should not become relationship proof.

A query can express a relationship and return nothing. That is a failed
experiment, not evidence that the relationship exists in the data.

Time is necessary but not sufficient.

Edges must respect time, but time order alone is weak. Use time plus exact
entity/value overlap plus call/column context.

Trace propagation may be incomplete.

Episodes need a durable identity. Today the branch uses trace ID where possible
and a local fallback otherwise. That is enough for a prototype, not enough for a
polished cross-process episode model.

Privacy and secrets need care.

Previews are useful for debugging but risky. Secret-shaped values should be
suppressed or redacted before this becomes default and broad.

## How to test the idea

Use real agent workflows, not synthetic unit tests only.

Good manual scenarios:

- Search GitHub issues or PRs, then query Linear attachments using a returned
  GitHub URL.
- Query Datadog logs for a request ID, then query another source using that ID.
- Search catalog tables/columns, then run SQL using the discovered table.
- Run a successful join across two sources and verify column relationship
  evidence.
- Run the same join with no returned rows and verify it is recorded as a failed
  experiment, not a positive relationship.
- Use common values like `open` or `main` and check whether false edges appear.

Useful evaluation questions:

- Can another agent reconstruct what happened from `coral_provenance` alone?
- Are the connected chains mostly real, or noisy?
- Does the nearest-earlier rule make the graph easier to read?
- Which useful chains are missed because the value was transformed?
- Which false positives come from low-entropy values?
- What row-level evidence would have preserved chains that currently break?

## What to build next

The next serious increment is not UI. It is better evidence.

1. Keep the current observed-value binding model.
2. Add planner-derived query evidence from DataFusion.
3. Only promote successful-result relationship evidence when rows are returned.
4. Add row fingerprints for returned rows.
5. Preserve row-derived continuity across later calls.
6. Add episode metadata beyond trace ID.
7. Add retention, file locking, and redaction.
8. Then build agent-facing runbooks and graph views.

The planner-derived evidence should be additive. Do not replace observed-value
bindings. They answer different questions.

## Suggested PR split

This branch is larger than the preferred Coral review size. If splitting before
review:

1. Engine runtime-table extension point.
2. App provenance receipt store and SQL read models.
3. Query/catalog instrumentation.
4. Docs and agent runbook.

If continuing as an experiment branch, keep it together until the live workflow
tests answer whether the evidence is useful.

## Verification already run

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

## Advice for the next engineer

Treat the current implementation as a working probe, not the final architecture.

The big bet is this:

> Coral can turn agent data access into episode memory by recording observed
> input/output continuity and planner-backed relationship evidence.

Do not overfit to the first JSONL shape. Do not jump straight to RDF. Keep the
evidence relational, inspectable, and grounded in actual returned results. Build
the KG and workflow views as projections over evidence, not as the source of
truth.
