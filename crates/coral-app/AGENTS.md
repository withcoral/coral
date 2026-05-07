# AGENTS.md

## Purpose

`coral-app` is the local management plane and internal gRPC server composition
root.

## Owns

- local server bootstrap and service wiring
- app-owned persisted state under `CORAL_CONFIG_DIR`
- workspace identity and validation
- source lifecycle and install/remove persistence
- bundled-source manifest description and install-time manifest mapping through
  `coral-spec`
- query-time selection of installed sources before calling `coral-engine`

## Does Not Own

- source-spec semantics beyond light request validation and app-facing mapping
- backend-specific compilation or runtime registration
- `DataFusion` session assembly or query planning
- public client-facing rendering helpers
- a high-level public local SDK boundary

## Invariants

- Keep service handlers thin; real behavior belongs in managers or state
  helpers.
- Keep process environment access in `src/bootstrap/env.rs` or other clearly
  app-owned bootstrap seams. Do not read ambient process environment from
  managers, services, or state helpers.
- Keep `state/`, `workspaces/`, `sources/`, and `query/` as the main internal
  boundaries. Do not create new sub-boundaries unless they own durable,
  independent behavior.
- Persist imported manifests as files under app-owned state; do not inline
  them into `config.toml`.
- Bundled installs persist source identity plus configured variables and
  secrets, then resolve their manifest from the current binary at runtime.
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
  state, secrets, manifests, rollback, runtime setup, and engine calls. They
  should not know about tonic request or response types.
- For all service calls, keep protobuf request/response types confined to the
  service edge. Convert request data into small app-local command, query, or
  binding structs before calling managers; do not pass `coral_api::v1`
  request/response/message types into managers, state helpers, or other
  app-owned domain code.
- `workspaces/name.rs` and `sources/name.rs` own the checked app-local identity
  types. Parse `WorkspaceName` and `SourceName` at persistence and service
  boundaries so managers and state/layout code stay transport-free and do not
  pass raw identifier strings around internally.
- `state/config.rs`, `state/secrets.rs`, and `storage/fs.rs` own persistence
  and filesystem details. Managers may coordinate them, but services should not
  reach into them directly.
- Keep app-owned domain models transport-free. Proto mapping belongs at the
  service edge unless there is a strong reason to centralize it.
