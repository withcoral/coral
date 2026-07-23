# AGENTS.md

## Purpose

`coral-app` is the local management plane and internal gRPC server composition
root.

## Owns

- local server bootstrap and service wiring
- app-owned persisted state under `CORAL_CONFIG_DIR`
- workspace identity and validation
- source lifecycle and install/remove persistence
- credential-set identity and credential material persistence
- runtime feature registry semantics for user-facing `[features]` config
- bundled-source manifest description and install-time manifest mapping through
  `coral-spec`
- assembly of query-engine runtime packages from app-owned installed state,
  including DSL v4 materialized artifacts and generated runtime components
- query-time selection of installed sources before calling `coral-engine`
- workspace-scoped catalog discovery behavior over query-visible tables:
  matching, pagination, exact lookup, column filtering, and missing-table
  context

## Does Not Own

- source-spec semantics beyond light request validation and app-facing mapping
- backend-specific compilation or runtime registration
- `DataFusion` session assembly or query planning
- public client-facing rendering helpers
- a high-level public local SDK boundary

## Invariants

- Keep service handlers thin; real behavior belongs in managers, state helpers,
  or credential helpers.
- Keep process environment access in `src/bootstrap/env.rs` or other clearly
  app-owned bootstrap seams. Do not read ambient process environment from
  managers, services, state helpers, or credential helpers.
- Treat `PrincipalId` as an opaque, stable identifier from one collision-free
  namespace spanning every actor kind and identity authority. Principal
  providers own canonicalization and supply the authenticated `PrincipalKind`;
  downstream code must not infer actor kind, ownership, or authorization from
  the identifier. Kind is an input to authorization policy, not a permission or
  role by itself, and providers must not classify the same `PrincipalId` as
  different kinds across requests.
- Keep `state/`, `credentials/`, `workspaces/`, `sources/`, `query/`, and
  `catalog/` as the main internal boundaries. Do not create new sub-boundaries
  unless they own durable, independent behavior.
- Keep RDBMS-backed durable app-state infrastructure under `state/db`. The
  initial DB bootstrap may coexist with filesystem-backed source behavior, but
  repository wiring must keep SQLx pools, transactions, SeaQuery schema
  identifiers, and row structs inside that module.
- Give an independently identified entity one stable ID as its sole primary
  key. Use a composite primary key only when the tuple itself is the durable
  domain identity, not merely because every access is scoped by a parent such
  as `workspace_id`.
- Treat `workspace_id` as the confidentiality and access-control boundary for
  workspace-owned rows. Every externally influenced lookup or mutation must
  match the workspace and entity ID even when the entity ID is globally unique.
- Name event timestamps for the fact they record as
  `<fact>_at_unix_nanos BIGINT`. Name actor attribution
  `<event>_by_principal_id`; attribution does not imply ownership or
  authorization.
- Prefer portable `TEXT` columns plus named `CHECK` constraints for small
  closed value sets shared by SQLite and Postgres. Couple nullable fields with
  a named constraint when they represent one state transition.
- Versioned migrations must fail loudly on schema drift. Do not use
  `IF NOT EXISTS` unless the migration deliberately adopts a documented legacy
  object.
- Design indexes from concrete access paths, including predicate and ordering
  columns. Put multi-repository transaction choreography in a focused
  `state/db/*_state.rs` operation; repositories expose the smallest reusable
  query primitives.
- DB repository behavior should have shared tests that run against SQLite
  locally and Postgres in CI through the repository harness.
- Until the RDBMS migration phases replace the relevant stores, persist
  imported manifests as files under app-owned state; do not inline them into
  `config.toml`.
- Persist DSL v4 imported manifests as authored intent plus durability
  normalization only. Descriptor hashes, generated OpenAPI metadata, semantic
  IR, projections, and package fingerprints belong in materialized artifacts
  or runtime package assembly, not in persisted `manifest.yaml`.
- Treat DSL v4 materialization as a user-chosen lifecycle event: generate at
  source add and never re-fetch descriptors, recompute projections, or rewrite
  artifacts implicitly during query/list/validate. Fingerprints, producer
  versions, identity metadata, and raw-document hashes are tracing diagnostics,
  not runtime gates. Load readable, structurally compatible artifacts and
  isolate source-local compatibility failures while preserving fail-closed
  behavior for operational errors. RDBMS migration machinery must not turn
  load-time compatibility into silent regeneration.
- Treat `projections.yaml` as an immutable materialized snapshot. Effective
  operation-metadata overrides never reconcile projection exposure or lookup
  keys at runtime. Reject incompatible selected artifact combinations instead
  of changing projection fields in memory.
- Store DSL v4 source documents, semantic IR, generated operation metadata,
  fingerprint, diagnostics, and the generated projection catalog directly
  under the materialization root. Store full `operation-metadata.yaml` and
  projection overrides directly under the override root; do not restore
  per-surface directories or fallback paths. A present operation-metadata
  override completely replaces generated metadata. Legacy
  `parameter_metadata.yaml` files are inert and must not be migrated, deleted,
  or interpreted at load time.
- A valid DSL v4 source with no published projections still has a loadable
  semantic IR. Runtime package assembly returns no component for that source;
  it must not treat the empty projection catalog as a corrupt materialization.
- User-facing runtime feature semantics belong in `coral_app::features`; raw
  config-file persistence, locking, and TOML extraction stay in `state/`.
- Bundled installs persist source identity plus configured variables and
  secrets, then resolve their manifest from the current binary at runtime.
- Keep reusable database pools workspace-scoped. `coral-app` owns the
  workspace-to-registry map and removes an entry when its workspace is deleted;
  `coral-engine` owns the registry's provider-specific pool implementation.
- Credential backend selection stays inside `credentials/`. Managers pass
  explicit source credential-storage routes; CLI, MCP, source-spec, and engine
  code must not know backend implementation details.
- An installed source's persisted credential-storage route is authoritative.
  A missing route is legacy file storage, not an instruction to re-run global
  backend selection.
- Source `name` is the canonical installed identifier and SQL schema name.
- `coral-client::local` intentionally depends on `coral-app::ServerBuilder` for
  the explicit local bootstrap seam.
- Prefer documenting `coral-client` as the public local entrypoint and
  `coral-app` as the internal composition root, even when bootstrap types stay
  visible for sibling crates or tests.

## Layering

- `bootstrap/server.rs` is the composition root. It discovers environment and
  layout, constructs stores and managers, wires runtime context, and mounts
  gRPC services.
- `service.rs` files are transport adapters. They should stay thin: decode
  tonic requests, normalize workspace and path identifiers, call managers, and
  map app/core results into protobufs.
- `manager.rs` files own app-level orchestration. They coordinate installed
  state, credential material, manifests, rollback, runtime setup, and engine
  calls. They should not know about tonic request or response types.
- `catalog/discovery.rs` owns provider-independent discovery semantics that
  adapters need to share. CLI, MCP, and UI code should render catalog results,
  not reimplement table matching, column filtering, pagination, or
  missing-table context.
- `sources/runtime_package.rs` owns app-level conversion from installed source
  state and materialized artifacts into the generic runtime components accepted
  by `coral-engine`.
- For all service calls, keep protobuf request/response types confined to the
  service edge. Convert request data into small app-local command, query, or
  binding structs before calling managers; do not pass `coral_api::v1`
  request/response/message types into managers, state helpers, or other
  app-owned domain code.
- `workspaces/name.rs` and `sources/name.rs` own the checked app-local identity
  types. Parse `WorkspaceName` and `SourceName` at persistence and service
  boundaries so managers and state/layout code stay transport-free and do not
  pass raw identifier strings around internally.
- `state/config.rs`, `credentials/store.rs`, and `storage/fs.rs` own
  persistence and filesystem details. Managers may coordinate them, but
  services should not reach into them directly.
- Keep app-owned domain models transport-free. Proto mapping belongs at the
  service edge unless there is a strong reason to centralize it.
