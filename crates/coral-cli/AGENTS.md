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
- Keep CLI-owned process environment access purpose-specific and locally
  justified with a targeted Clippy allow. Fixed CLI env contracts may live in
  `src/env.rs`, but avoid generic helpers that let arbitrary command code read
  ambient environment without declaring intent.
- Decode query responses through `coral-client`; do not reimplement Arrow IPC
  handling here.
- Keep install/import user-friendly, but move reusable behavior inward instead
  of duplicating app or MCP logic.
- Treat CLI commands, flags, output, and workflows as public surfaces. Update
  `docs/` when a change affects a reader-facing contract or important
  operational knowledge, choose the best existing docs location, and make the
  amount of space match the feature's user-facing weight and visibility.
- Prefer improving prompts and terminal presentation here rather than pushing
  user-facing formatting concerns into `coral-app` or `coral-engine`.
