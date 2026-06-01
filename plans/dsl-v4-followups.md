# DSL v4 Follow-up Plan

Direction confidence: high. Implementation-detail confidence after the
Goldfish pass: moderate-high, because the exact runtime package contract and
schema-generation contract must be made explicit before coding.

## Problem

The current DSL v4 slice crosses three boundaries it should not cross:

- The v4 authored manifest is partly a snapshot: it requires a top-level
  `version` and per-surface `sha256`.
- `coral-engine` knows about v4 authored manifests and v4 materialized
  artifacts through a dedicated v4 backend.
- The stable v3 JSON schema was expanded to validate v4, instead of preserving
  the v3 contract and generating a separate v4 schema from Rust.

The desired shape is stricter: v4 YAML declares intent, source add/materialize
records the observed snapshot, `coral-app` assembles runtime-ready source
packages, and `coral-engine` only compiles generic runtime backend components.

## Current Context

- `crates/coral-spec/src/schema/source_manifest.schema.json` is currently the
  v3 schema with v4 additions. It changed `dsl_version` from `const: 3` to
  `[3, 4]`, relaxed the global `backend` requirement, and added `surfaces`.
- `crates/coral-spec/src/schema.rs` embeds that one schema and
  `crates/coral-spec/src/parser.rs` validates against it before dispatching on
  `dsl_version`.
- `crates/coral-spec/src/v4/mod.rs` requires `RawV4SourceManifest.version` and
  `RawV4Surface.sha256`; `SurfaceDescriptor` stores the authored SHA.
- `crates/coral-app/src/sources/materialization.rs` already computes descriptor
  SHA at add time, but rejects the descriptor if it does not match the authored
  SHA. It stores the observed value in `FingerprintSurface.descriptor_sha256`.
- `crates/coral-app/src/sources/manager.rs` imports v4 sources, canonicalizes
  local descriptor paths, enriches missing OpenAPI metadata, materializes
  artifacts, and persists imported manifest YAML.
- `crates/coral-engine/src/contracts/query.rs` adds
  `QuerySource::new_with_v4_materialization`.
- `crates/coral-engine/src/backends/mod.rs` dispatches v4 specially.
- `crates/coral-engine/src/backends/v4/mod.rs` converts v4 projections into
  synthetic HTTP manifests and merges per-surface registrations under one SQL
  schema.

One important secondary smell: `enrich_v4_openapi_manifest_yaml` currently
writes OpenAPI-derived `description` and `base_url` back into the persisted
manifest. That is also snapshot behavior. Removing only `sha256` and `version`
while keeping generated OpenAPI metadata in the manifest would be a half-fix.

## Technical Plan

### 1. Make v4 manifests intent-only

Remove `version` and `sha256` from the authored v4 surface.

- In `crates/coral-spec/src/v4/mod.rs`, remove `version` from
  `RawV4SourceManifest` and remove `sha256` from `RawV4Surface`.
- Stop storing SHA in `SurfaceDescriptor`; it should identify only
  `url` or `file`.
- Stop using `SourceManifestCommon` as the normalized v4 identity if that keeps
  forcing a version. Give `V4SourceManifest` explicit `name`, `description`,
  and `test_queries` fields, or introduce a v4-specific common struct without
  version.
- Remove `source_version` from v4 artifact structs (`SemanticIr`,
  `ProjectionCatalog`, `Fingerprint`) unless there is a concrete runtime use.
  Fingerprint freshness should come from `source_name`, `manifest_sha256`,
  descriptor locations, descriptor SHA, importer version, generator version,
  and input declaration hashes.
- Update `sources/core-v4/github_v4/manifest.yaml` and all v4 test fixtures to
  omit `version` and `sha256`.
- Replace `ValidatedSourceManifest::source_version() -> &str` with an authored
  version accessor that can return `None`, or add a new optional accessor and
  migrate app callers to it. v3 returns `Some`; v4 returns `None`.
- For API compatibility, keep existing proto string fields. Service mapping
  should serialize `None` as an empty string.
- For CLI compatibility, keep the `source list` version column but render `-`
  for missing version, and skip the `Version:` line in `source info` when the
  version is missing. Do not invent a fake v4 manifest version.

At materialization time:

- `write_materialization` reads/fetches the descriptor, computes SHA-256 from
  bytes, and records that in `fingerprint.yaml` and `MaterializedSurface`.
- `load_v4_materialization` verifies the installed manifest hash, surface
  descriptor identity, input declaration hash, artifact schema version,
  importer version, and generator version. It should not re-fetch remote
  descriptors during query load.
- Do not write computed SHA or generated version back into persisted
  `manifest.yaml`.

For OpenAPI-derived metadata:

- Stop persisting generated `description` or derived `base_url` into
  `manifest.yaml`.
- Keep explicit authored `base_url` in the manifest when provided.
- When omitted, derive base URL from the materialized OpenAPI document while
  assembling the runtime package in `coral-app`.
- For this PR, do not derive source descriptions for display. If a v4 source
  wants a description in `source info`, it should author `description`
  explicitly. Derived descriptions can be a later metadata feature.

### 2. Move runtime package assembly to coral-app

Do not replace the current v4 backend with one synthetic HTTP manifest. That is
wrong because different v4 surfaces can have different `base_url`, auth,
headers, and rate-limit settings.

Instead, introduce a generic runtime package at the app-engine seam:

- Change `QuerySource` in `crates/coral-engine/src/contracts/query.rs` to this
  shape in spirit:

```rust
pub struct QuerySource {
    source_name: String,
    authored_version: Option<String>,
    description: String,
    declared_inputs: Vec<ManifestInputSpec>,
    test_queries: Vec<String>,
    components: Vec<RuntimeSourceComponent>,
    variables: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
}

pub enum RuntimeSourceComponent {
    Http(coral_spec::backends::http::HttpSourceManifest),
    File(coral_spec::backends::file::FileSourceManifest),
    Mcp(coral_spec::backends::mcp::McpSourceManifest),
}
```

- Put `RuntimeSourceComponent` in `coral-engine` contracts, not `coral-spec`.
  It is an engine input package type, but its variants wrap existing
  `coral-spec` backend-ready structs.
- Add constructors such as `QuerySource::from_manifest(...)` for existing v3
  callers and `QuerySource::from_runtime_components(...)` for app-assembled
  packages.
- Remove the earlier idea of `ValidatedSourceManifest::from_http_manifest`;
  engine component compilation should accept `RuntimeSourceComponent` directly
  and dispatch to the existing backend compilers.
- Keep `ValidatedSourceManifest` as the author/source-spec parser result. Do
  not force generated runtime components back into that enum.
- `QuerySource` should validate that every component belongs to the same
  logical source/schema. Component-level manifest `version` and `test_queries`
  should not be the public source identity; the package owns source-level test
  queries and source metadata.
- For v3 sources, `coral-app` creates a package with exactly one component
  from the parsed v3 manifest.
- For v4 sources, `coral-app` loads materialized artifacts and creates one HTTP
  component per published surface, using the conversion logic currently in
  `crates/coral-engine/src/backends/v4/mod.rs`.
- Move v4 projection-to-HTTP conversion into a new app-owned module, likely
  `crates/coral-app/src/sources/runtime_package.rs` or a focused sibling of
  `materialization.rs`.
- In `coral-engine`, delete `backends/v4`.
- In `coral-engine`, replace `V4CompiledSource` with a generic composite
  compiled source that compiles every runtime component and merges their
  registrations under one logical source schema.
- Keep duplicate table/function detection generic in the composite source.
  Error messages should not mention v4.
- Treat component registration failure as a failure of the whole logical
  source, matching the current v4 behavior.
- Update `SourceInputResolutionContext::from_query_source` to read declared
  inputs directly from `QuerySource`, not from a single source spec.
- Update source validation in `coral-app/src/query/manager.rs` to call
  `query_source.test_queries()` instead of `query_source.source_spec()`.

After this, this command should return no engine hits:

```sh
rg "coral_spec::v4|as_v4|V4Materialized|backends::v4|new_with_v4_materialization" crates/coral-engine
```

### 3. Keep v3 schema unchanged and generate v4 schema

Restore the stable v3 schema and add a separate generated v4 schema.

- Restore `crates/coral-spec/src/schema/source_manifest.schema.json` to the
  v3-only version from `main`.
- Add `crates/coral-spec/src/schema/source_manifest_v4.schema.json`.
- Add explicit `schemars` dependency wiring. It is already in `Cargo.lock`
  transitively, but `coral-spec`/`xtask` do not depend on it directly.
- Generate the v4 schema inside `coral-spec`, not directly in `xtask`.
  `coral-spec` should expose a public helper such as
  `generated_v4_source_manifest_schema() -> serde_json::Value`; `xtask` only
  writes/checks the returned JSON.
- Use schema-only Rust types colocated with the v4 raw parser, for example
  `crates/coral-spec/src/v4/schema.rs`, if deriving `JsonSchema` on the parser
  structs would drag schema concerns through normalized runtime types.
- Those schema types must describe authored YAML only: `dsl_version`, `name`,
  optional authored `description`, optional `test_queries`, and `surfaces`.
  They must not include materialized fingerprints, projections, semantic IR,
  generated versions, or generated SHA.
- Model `ParsedTemplate` fields as string in the schema types. Do not require
  `ParsedTemplate` itself to implement `JsonSchema` unless that is simpler
  locally.
- Model `inputs` as a typed map in the schema types, not `serde_json::Value`,
  so the generated schema is useful.
- Keep parser and schema types from drifting by adding tests that validate each
  v4 fixture through both the generated schema and `parse_source_manifest_yaml`.
- Add generation/check tooling under `xtask`, for example:

```sh
cargo run --locked -p xtask -- generate-schemas
cargo run --locked -p xtask -- generate-schemas --check
```

- Add `make schema-generate` and `make schema-check`, and include
  `schema-check` in the validation path if generated schema freshness should
  be CI-enforced.
- Mark only the generated v4 schema as `linguist-generated` in
  `.gitattributes`.
- Change `schema.rs` to dispatch validators by raw `dsl_version` before
  backend parsing: v3 uses the unchanged v3 schema, v4 uses the generated v4
  schema.
- Keep the source-authoring skill link to the stable v3 schema unless a v4
  preview authoring path is explicitly documented.

Generated-schema post-processing is acceptable for constraints that schemars
does not express cleanly, but it must live in the `coral-spec` schema helper
and be covered by tests. Candidate constraints include `dsl_version: 4`,
surface `id` pattern, HTTPS URL pattern, and exactly one of `url` or `file`.

### 4. Materialization validation matrix

When loading v4 materialization, validate the artifact as a local snapshot
without re-fetching remote descriptors:

- fingerprint artifact schema version matches `V4_ARTIFACT_SCHEMA_VERSION`;
- fingerprint source name matches the parsed v4 source name;
- fingerprint manifest SHA matches the installed manifest YAML bytes after the
  app's durability normalization;
- fingerprint importer and projection-generator versions match current code;
- fingerprint has exactly the same surface ids as the v4 manifest, no missing
  surfaces and no extras;
- each fingerprint surface's descriptor kind and descriptor location match the
  parsed manifest descriptor after local file canonicalization;
- each fingerprint surface's input declaration hash matches the parsed surface
  inputs;
- each materialized surface directory and raw/normalized/source IR file exists;
- each materialized raw source document hashes to the fingerprint descriptor
  SHA, catching local disk corruption without reaching the network;
- each semantic IR has the expected artifact schema version, source name,
  surface id, surface type, and importer version;
- projection catalog has expected artifact schema version, source name, and
  generator version;
- every projection references an existing surface and an operation present in
  that surface's semantic IR.

## Detailed Implementation

- `sources/core-v4/github_v4/manifest.yaml`: remove top-level `version` and
  per-surface `sha256`.
- `crates/coral-spec/src/schema/source_manifest.schema.json`: restore v3
  schema exactly.
- `crates/coral-spec/src/schema/source_manifest_v4.schema.json`: add generated
  v4 schema.
- `crates/coral-spec/src/schema.rs`: compile and dispatch v3/v4 validators.
- `crates/coral-spec/src/parser.rs`: parse raw `dsl_version` before schema
  validation and route to the correct schema.
- `crates/coral-spec/src/v4/mod.rs`: remove authored version/SHA, add
  schema helper entrypoints, update fingerprint/artifact identity, and update
  tests.
- `crates/coral-spec/src/v4/schema.rs` (new, or an equivalent submodule): hold
  v4 authored-manifest schema types and generated-schema post-processing.
- `crates/coral-spec/src/common.rs`: avoid forcing v4 through
  `SourceManifestCommon` if the version field makes v4 awkward.
- `Cargo.toml`, `crates/coral-spec/Cargo.toml`: add direct `schemars`
  dependency. `xtask` should not need `schemars` if it calls the
  `coral-spec` schema helper.
- `xtask/src/main.rs` plus a new `xtask/src/schemas.rs`: add schema generation
  and freshness check.
- `Makefile`: add schema generation/check targets.
- `.gitattributes`: mark generated v4 schema as generated.
- `crates/coral-app/src/sources/materialization.rs`: compute descriptor SHA
  at materialization time without checking an authored SHA; stop mutating YAML
  with generated metadata; expose enough metadata for app runtime packaging.
- `crates/coral-app/src/sources/manager.rs`: persist intent YAML, still
  canonicalize local file descriptors for durability, and stop relying on v4
  authored version.
- `crates/coral-app/src/sources/catalog.rs` and
  `crates/coral-app/src/sources/model.rs`: make candidate version optional and
  support v4 with no authored version.
- `crates/coral-app/src/query/manager.rs`: build runtime packages, not
  `QuerySource::new_with_v4_materialization`.
- `crates/coral-app/src/sources/runtime_package.rs` (new): own v4
  materialization-to-HTTP-component assembly.
- `crates/coral-engine/src/contracts/query.rs`: replace v4 materialization
  storage with `RuntimeSourceComponent` plus source metadata.
- `crates/coral-engine/src/backends/mod.rs`: compile runtime components
  generically.
- `crates/coral-engine/src/backends/v4/mod.rs`: delete.
- `crates/coral-engine/tests/engine/v4_tests.rs`: delete or move coverage into
  app-level tests.
- `crates/coral-engine/tests/engine.rs`: remove v4 test module include.
- `crates/coral-engine` tests: add generic multi-component package tests.
- `crates/coral-app` tests: extend v4 import/query coverage to prove app
  assembles HTTP runtime components from materialization.
- `docs/reference/cli-reference.mdx`: update `source list`/`source info`
  wording for sources with no authored version (`-` in list output and no
  `Version:` detail line).
- `docs/reference/source-spec-reference.mdx` and
  `plugins/coral/skills/coral-create-source-spec/SKILL.md`: keep the stable v3
  guidance unchanged unless adding an explicit v4 preview authoring path.
- `AGENTS.md`, `crates/coral-app/AGENTS.md`, and
  `crates/coral-engine/AGENTS.md`: update in the same implementation change.
  This is a meta change because app/engine ownership is shifting for runtime
  package assembly.

## Alternatives

- Persist computed `version` and `sha256` back into installed v4 manifest:
  rejected. It preserves the snapshot shape under generated values and makes
  the stored manifest less like authored intent.
- Keep an optional authored SHA as an integrity pin: rejected for this PR
  because the user asked to remove SHA from the manifest and v4 is still
  preview. If integrity pinning becomes necessary later, add a deliberately
  named field such as `integrity.sha256` rather than reviving snapshot-looking
  `sha256`.
- Keep the engine v4 backend and just thin it down: rejected. The engine still
  has to understand v4 materialization and projection semantics.
- Collapse v4 into one generated HTTP manifest: rejected. It breaks the
  multi-surface model where each surface can have different runtime settings.
- Hand-write the v4 JSON schema: rejected. The requirement is to generate v4
  schema from Rust with `schemars`, and hand-written schema would immediately
  drift.
- Change the v3 schema into a polymorphic v3/v4 schema: rejected. The stable v3
  schema should be unchanged in this PR.

## Acceptance Criteria

- `source_manifest.schema.json` has no diff from `main`.
- `source_manifest_v4.schema.json` is generated from Rust and `schema-check`
  fails when it is stale.
- Direct v3 schema validation behavior is unchanged, including rejection of v4
  manifests. The parser still accepts v4 by dispatching to the separate v4
  validator first.
- v4 schema accepts the updated `sources/core-v4/github_v4/manifest.yaml`.
- v4 schema rejects v3-only fields such as `backend`, `tables`, top-level
  `auth`, and `functions`.
- v4 authored manifests do not require or accept top-level `version` or
  per-surface `sha256`.
- `coral source add --file sources/core-v4/github_v4/manifest.yaml` computes
  descriptor SHA and writes it to materialized fingerprint data.
- Persisted imported v4 manifest YAML does not contain generated version,
  generated SHA, or generated OpenAPI base URL/description.
- v4 source info without authored `version` serializes an empty proto version,
  renders `-` in list output, and omits the `Version:` line in detail output.
- v4 source info without authored `description` has an empty description;
  OpenAPI descriptions are not silently persisted or displayed as source
  metadata in this PR.
- v4 materialization load rejects missing, extra, stale, or locally corrupted
  materialized surface artifacts according to the validation matrix above.
- Querying an installed v4 source works through app-assembled runtime
  components.
- `coral-engine` contains no v4-specific backend, types, or imports.
- Engine tests cover generic multi-component packages without importing
  `coral_spec::v4`.
- App tests cover v4 import, materialization, runtime package assembly, and a
  real query through the generated HTTP component.
- `make rust-checks` passes.
- `make schema-check` passes once added.
- `make docs-check` passes if docs or generated docs are touched.
- Relevant `AGENTS.md` files are updated and explicitly describe the new
  app-owned runtime package assembly boundary.
- CLI docs reflect the versionless v4 output behavior.

## Open Questions

- Should schema freshness be part of `make rust-checks`, `make docs-check`, or
  its own CI step? Recommendation: own `schema-check`, then wire CI explicitly.
