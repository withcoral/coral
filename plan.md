# Live Eval Injection Plan

## Problem

We want live evals against real sources, but with a controlled way to inject
 synthetic rows into selected tables so an eval harness can ask, "did the agent
 find the planted information?"

The current PR proves the mechanics, but it puts benchmark-specific concepts
 directly into OSS engine seams:

- `QueryRuntimeContext` learns about `needles_file`
- source registration learns one feature's policy
- OSS code and docs inherit benchmark terminology

That is the wrong split if the real product boundary is OSS vs `ee/`.

## Design Goal

Support live eval row injection without teaching OSS what a "needle" is.

OSS should provide a neutral source-decoration mechanism.

`ee/` should provide:

- eval-specific config loading
- injected-row planning
- injected-row provider wrapping
- eval-specific validation and failure policy
- future tracking of whether planted rows were surfaced

## Non-Goals

- Snapshot-based benchmarking
- User-facing CLI or MCP design
- Result scoring or eval dashboards
- Generalizing all runtime behavior behind one abstraction

## Proposed Architecture

### OSS owns source-decoration mechanics

Add an internal decoration pipeline to `crates/coral-engine` that operates on
 a fully compiled `BackendRegistration` before catalog registration.

Suggested shape:

```rust
pub(crate) trait SourceDecorator: Send + Sync {
    fn name(&self) -> &'static str;

    fn prepare(&mut self) -> datafusion::error::Result<()> {
        Ok(())
    }

    fn decorate_source(
        &mut self,
        schema_name: &str,
        registration: BackendRegistration,
    ) -> datafusion::error::Result<BackendRegistration>;

    fn finish(&mut self) -> datafusion::error::Result<()> {
        Ok(())
    }
}
```

This is intentionally narrow. It supports the only part of the lifecycle this
 feature needs:

- one-time setup
- per-source table decoration
- final invariant checks

It is intentionally a success-path seam. A `SourceDecorator` transforms a
 successfully registered `BackendRegistration`; it does not participate in
 source registration failure policy.

### `ee/` owns the live-eval feature

Commercial code under `ee/` implements one concrete source decorator, for example:

- `ee/coral-evals/src/injection/decorator.rs`
- `ee/coral-evals/src/injection/plan.rs`
- `ee/coral-evals/src/injection/provider.rs`
- `ee/coral-evals/src/injection/config.rs`

The important part is ownership, not the exact crate names.

That decorator owns all eval-specific concepts:

- injection manifest format
- "needle" terminology if you still want it internally
- how rows are grouped by `(schema, table)`
- whether an unmatched target is fatal
- future surfacing/tracking hooks

OSS never needs to mention "live eval", "benchmark", or "needle".

## End-to-End Flow

### 1. App/bootstrap layer builds runtime options

`coral-app` should stop placing eval-specific fields into
 `QueryRuntimeContext`.

Instead, runtime assembly should accept a separate internal options object for
 engine-only behavior:

```rust
pub(crate) struct RuntimeBuildOptions {
    pub(crate) source_decorators: Vec<Box<dyn SourceDecorator>>,
}
```

Important split:

- `QueryRuntimeContext`: generic app-owned runtime inputs needed to compile
  sources, such as home-directory resolution
- `RuntimeBuildOptions`: engine assembly options, including optional source
  decorators

`RuntimeBuildOptions` should remain internal to runtime assembly. It is a
 composition-root input owned by `coral-app` and `ee/`, not a second public
 engine contract.

That keeps the public app-to-engine contract clean.

### 2. `ee/` config produces an injection plan

Commercial config loading turns an eval manifest into an in-memory injection
 plan.

The plan should be normalized before runtime registration begins.

Suggested normalized model:

```rust
pub(crate) struct InjectionPlan {
    pub(crate) targets: HashMap<TableTarget, Vec<serde_json::Value>>,
}

pub(crate) struct TableTarget {
    pub(crate) schema: String,
    pub(crate) table: String,
}
```

This is where parsing, schema-independent validation, and ownership of file/env
 handling should live.

### 3. Registry runs source decorators generically

Registration flow becomes:

1. build source decorators
2. call `prepare()`
3. for each source:
   - `source.register(ctx)`
   - pass the `BackendRegistration` through source decorators in order
   - register final tables into the DataFusion catalog
4. if `source.register(ctx)` fails, handle it with the existing generic source
   registration path
5. call `finish()`

`runtime/registry.rs` stays generic. It should not mention:

- needles
- overlays
- evals
- YAML
- targeted-source failure policy

### 4. `ee/` wraps matching providers

The commercial decorator inspects the registered tables for the current source.

For each targeted table:

1. fetch injected rows for `(schema, table)`
2. convert them to `RecordBatch` values against the live provider schema
3. wrap the original provider with a union-style provider

There are two reasonable implementation options:

#### Option A: OSS ships a neutral provider wrapper

OSS can expose internal reusable helpers such as:

- `UnionTableProvider`
- `json_rows_to_record_batches(schema, rows)`

Then the commercial decorator only decides when to apply them.

This is the best choice if you expect more than one feature to reuse the same
 wrapping behavior.

#### Option B: `ee/` owns the wrapper too

If this really is a one-feature mechanism for the near term, keep the wrapper in
 `ee/` and let OSS expose only the source-decoration seam.

This keeps OSS even smaller, at the cost of some duplicated low-level provider
 code later if another feature wants the same pattern.

My recommendation: start with Option B unless you already expect another wrapper
 feature soon. The seam matters more than sharing helper code on day one.

## Error Policy

Keep error ownership narrow for now.

- OSS registry continues to handle source registration failures through the
  existing generic path.
- `SourceDecorator` errors cover only decoration-time behavior on successfully
  registered sources.
- `ee/` owns eval-config validation and unmatched-target policy.

Do not introduce a generic decorator failure-interception hook yet. That would
 widen the seam before there is a proven second use case for it.

If targeted source registration failures later need special eval semantics, add
 a separate neutral policy seam at that point rather than overloading
 `SourceDecorator`.

## Tracking Future "Was The Needle Found?" Logic

Do not put result-tracking in the source-decoration seam.

Injection and tracking are related but different:

- injection modifies what tables expose
- tracking inspects query text, logical plans, results, or post-query evidence

The clean design is:

- source-decoration seam handles row injection
- eval runner or `ee/` query wrapper handles tracking/scoring

That avoids turning the table registration path into a full eval framework.

## Why This Is Better Than The Current PR

- OSS contains only neutral runtime mechanics.
- `ee/` owns all commercial terminology and policy.
- `QueryRuntimeContext` stays small and generic.
- `RuntimeBuildOptions` stays internal instead of becoming another public seam.
- The next commercial decoration feature can reuse the same seam.
- Registry complexity grows linearly with source decorators, not with
  feature-specific
  branches.
- Live evals remain possible without pretending they are an OSS product concept.

## Concrete Rollout

### Phase 1: add the neutral seam in OSS

In `crates/coral-engine`:

- add `runtime/augment.rs`
- define `SourceDecorator`
- define `RuntimeBuildOptions`
- update runtime assembly and registry to execute source decorators
- keep the default OSS path as `source_decorators = []`

Do not add a generic source-failure interception interface in this phase.

No behavior change for OSS users.

### Phase 2: move live injection into `ee/`

In `ee/`:

- add eval config loading
- normalize injection targets into an injection plan
- implement the commercial decorator
- wire bootstrap so live eval runs pass that decorator into runtime assembly

### Phase 3: add tracking separately

Still in `ee/`:

- track whether injected facts were actually surfaced
- keep tracking outside the table-registration lifecycle

## Rejected Alternatives

### 1. Keep the current PR shape and accept the leakage

Rejected because the leakage is not accidental. It changes the contract and
 ownership boundary in exactly the wrong direction.

### 2. Put a generic `extensions: HashMap<String, Value>` on `QueryRuntimeContext`

Rejected because it hides the same leakage behind a bag of untyped data. The
 boundary still becomes feature transport rather than a clean contract.

### 3. Put all of this entirely outside Coral

Rejected for live evals because the union/injection behavior needs to happen at
 table-provider level if you want queries to operate naturally over live plus
 injected rows.

## Recommendation

Do not merge the current needle design as the final shape.

Implement a neutral source-decoration seam in OSS, then build live eval
 injection as a commercial decorator under `ee/`.

If you want the fastest path:

1. extract the source-decoration seam first
2. move all current needle-specific logic behind an `ee/` decorator
3. only then decide whether any union-provider helpers are worth keeping in OSS
