# AGENTS.md

## Purpose

`coral-spec` owns the declarative source-spec DSL: parsing, validation, input
discovery, and normalized source-definition models.

## Owns

- source-spec structs and enums shared across source kinds
- file and HTTP source-spec parsing
- source-spec validation helpers
- install/import-time input discovery

## Does Not Own

- runtime registration or SQL execution
- app bootstrap, source CRUD, or persistence policy
- CLI prompting or user-facing rendering
- transport or protobuf contracts

## Invariants

- Keep source-spec types transport-neutral; do not import protobuf or gRPC
  types.
- Keep runtime execution concerns out of this crate. Engine behavior belongs in
  `coral-engine`.
- Backends that declare SQL relations, including tables and source-scoped table
  functions, must project those names into the shared declared-relation
  namespace validator in `src/validate.rs`; do not hand-roll backend-local
  table/function collision checks.
- Prefer normalized source-spec values over raw YAML plumbing in public
  helpers.
