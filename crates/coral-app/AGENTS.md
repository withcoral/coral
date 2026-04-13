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
