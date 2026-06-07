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
- bundled-source and imported `SourceSpec` lifecycle through `coral-spec`
- source materialization from app-owned installed identity into capability and
  export artifacts
- workspace export composition and app-generated runtime plans
- query-time loading of SQL bindings from installed source exports before
  calling `coral-sql`
- workspace-scoped discovery behavior over generated exports: search,
  describe, pagination, exact lookup, and missing-reference context

## Does Not Own

- source-spec semantics beyond light request validation and app-facing mapping
- backend-specific compilation or runtime registration
- `DataFusion` session assembly or SQL query planning
- public client-facing rendering helpers
- a high-level public local SDK boundary

## Invariants

- Keep service handlers thin; real behavior belongs in managers, state helpers,
  or credential helpers.
- Keep process environment access in `src/bootstrap/env.rs` or other clearly
  app-owned bootstrap seams. Do not read ambient process environment from
  managers, services, state helpers, or credential helpers.
- Keep `state/`, `credentials/`, `workspaces/`, `sources/`, `query/`, and
  `catalog/` as the main internal boundaries. Do not create new sub-boundaries
  unless they own durable, independent behavior.
- Persist imported manifests as files under app-owned state; do not inline
  them into `config.toml`.
- Persist imported SourceSpecs as authored intent plus durability
  normalization only. Provider snapshots, capability sets, generated exports,
  and artifact fingerprints belong in materialized artifacts, not in
  `config.toml`.
- Treat source materialization as a user-chosen lifecycle event: generate at
  source add, never re-fetch descriptors or recompute exports implicitly during
  query/list/validate, and fail with re-add guidance when artifacts are missing
  or incompatible. Do not add migration machinery until the lifecycle is
  explicitly designed.
- User-facing runtime feature semantics belong in `coral_app::features`; raw
  config-file persistence, locking, and TOML extraction stay in `state/`.
- Bundled installs persist source identity plus configured variables and
  secrets, then resolve their manifest from the current binary at runtime.
- Credential backend selection stays inside `credentials/`. Managers pass
  explicit source credential-storage routes; CLI, MCP, SourceSpec, SQL, and
  upstream runtime code must not know backend implementation details.
- An installed source's persisted credential-storage route is authoritative.
  A missing route defaults to file storage, not an instruction to re-run global
  backend selection.
- Installed source identity is app-owned. `SourceSpec.name` is only an
  install-time display/key seed; persisted `source_id` and `source_key` drive
  generated capability ids, binding refs, and SQL namespaces.
- Explicit local bootstrap belongs to `coral-app::ServerBuilder` and
  `RunningServer`; keep `coral-client` focused on API transport and result
  helpers.
- Prefer documenting `coral-client` as the transport entrypoint and
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
  state, credential material, manifests, rollback, capability/export loading,
  and runtime calls. They should not know about tonic request or response
  types.
- `discovery/` owns workspace export loading and provider-independent
  search/describe semantics that adapters need to share. CLI, MCP, and UI code
  should render discovery results, not reimplement export matching,
  pagination, or missing-reference context.
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
