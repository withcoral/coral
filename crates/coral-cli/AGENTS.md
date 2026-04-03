# AGENTS.md

## Purpose

`coral-cli` is the terminal adapter.

## Owns

- argument parsing and command routing
- interactive prompting for source install/import from `coral-spec`
- terminal rendering

## Does Not Own

- source lifecycle rules
- source-spec parsing or validation semantics
- query execution internals
- Arrow IPC wire handling

## Invariants

- Keep the CLI thin over `coral-client` and app/query internals.
- Decode query responses through `coral-client`; do not reimplement Arrow IPC
  handling here.
- Keep install/import user-friendly, but move reusable behavior inward instead
  of duplicating app or MCP logic.
- Prefer improving prompts and terminal presentation here rather than pushing
  user-facing formatting concerns into `coral-app` or `coral-engine`.
