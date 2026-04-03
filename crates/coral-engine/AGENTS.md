# AGENTS.md

## Purpose

`coral-engine` is the data plane engine: backend compilation, runtime
registration, and query execution.

## Owns

- backend-specific source adapters
- query runtime assembly and system catalog registration
- transport-neutral query results and errors

## Does Not Own

- app bootstrap or local transport wiring
- source-spec parsing, validation, or input discovery
- source CRUD, config persistence, or secret storage policy
- Arrow IPC codecs or result rendering
- CLI or MCP presentation concerns

## Invariants

- Keep the app-to-query seam small and type-focused; do not leak backend or
  `DataFusion` specifics through caller-visible contracts.
- Keep source-spec semantics in `coral-spec`; this crate should only consume
  validated source-spec types and backend-specific spec structs from there.
- Runtime code should work with compiled sources and generic metadata, not app
  policy or transport concerns.
- Keep this crate transport-neutral. Arrow IPC, CLI formatting, and MCP-facing
  shaping belong outside `coral-engine`.
