# AGENTS.md

## Purpose

`coral-api` owns Coral's protobuf transport contract and generated Rust
bindings.

## Owns

- protobuf message and service definitions under `proto/`
- generated transport-visible Rust types
- cross-crate wire-contract stability for app/client communication

## Does Not Own

- app-level error rendering or fallback policy
- query-runtime classification logic
- CLI or MCP-specific shaping
- speculative SDK abstractions

## Invariants

- Keep `coral-api` focused on wire contracts, not consumer-side decode helpers
  or rendering policy.
- Prefer Google AIP guidance for protobuf and API design. When a proposed wire
  shape diverges from AIP best practices, treat that as a design decision that
  should be considered carefully and discussed explicitly in the PR. Reference:
  https://google.aip.dev/general
- Prefer additive protobuf evolution. New fields and messages are fine; avoid
  changes that break older generated clients or change the meaning of existing
  defaults.
- Be careful with proto3 `optional`. Use presence tracking only when unset must
  be distinguished from the default value; do not add `optional` by default to
  strings or booleans just to signal "this may be absent".
- Enums should be forward-compatible and easy to consume. Reserve the zero
  value for the default/unknown state and avoid overlapping sentinel meanings
  unless there is a strong compatibility reason.
