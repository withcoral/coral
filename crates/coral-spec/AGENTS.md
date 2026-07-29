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
- DSL v4 manifests declare source-wide `inputs:` plus exactly one singular
  `surface:`. The source `name` is the SQL namespace; v4 surfaces and generated
  projections must not introduce surface IDs, namespace suffixes, or redundant
  projection namespaces.
- Keep the DSL v4 materialized model singular too: one semantic IR, one
  complete operation-metadata catalog, and one fingerprint surface, with
  source-wide projection names. Semantic IR contains imported provider facts;
  inferred pagination and lookup-key policy belongs only in operation
  metadata. Consumers must pair both through `ValidatedSurfacePlan`, and
  runtime structural validation must accept a valid source with zero
  operations and zero projections.
- Row-path and pagination inference are heuristics over vendor descriptors, so
  changing either can quietly reshape relations in unrelated sources. Diff
  `cargo run --locked -p xtask -- v4-metadata-report` across the change before
  submitting.
