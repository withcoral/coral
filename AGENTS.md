# AGENTS.md

## Repo Map

- `crates/coral-api`: protobuf contract and generated Rust bindings.
- `crates/coral-app`: local server composition, state, workspaces, and source
  lifecycle.
- `crates/coral-cli`: terminal adapter.
- `crates/coral-client`: intentionally thin local transport bootstrap plus
  Arrow IPC decode/render helpers.
- `crates/coral-engine`: engine-side backend compilation, runtime registration,
  and query execution.
- `crates/coral-mcp`: MCP stdio adapter over `coral-client`.
- `crates/coral-spec`: declarative source-spec parsing, validation,
  input discovery, and normalized source-definition models.

## Rules

- Run `make validate` before finishing.
- Keep adapters thin. If CLI or MCP behavior gets complex, move it inward.
- Keep transport contract concerns in `coral-api`, source-spec concerns in
  `coral-spec`, app/state concerns in `coral-app`, and query/runtime
  concerns in `coral-engine`.
- Keep shared Arrow IPC decoding and result rendering in `coral-client`.
- Treat `coral-app` as an internal composition root even if sibling crates use
  its bootstrap seam today.
- If a caller needs explicit local server control, prefer `coral-client::local`
  over widening the default client surface.
