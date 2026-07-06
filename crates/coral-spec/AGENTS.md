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
- `ManifestDataType` is the single scalar-type vocabulary. Spec structs carry
  it typed — do not add `data_type: String` fields that consumers re-parse,
  and do not hand-list its variants or spellings anywhere except
  `ManifestDataType::ALL` and `as_manifest_str`. Restricted subsets (such as
  `FilePartitionDataType`) are separate enums related to it through
  `TryFrom`/`From`, not parallel-maintained copies.

## Adding a manifest data type

The exhaustive matches and the lattice tests in this crate walk you through
most of the change; the parts they cannot reach are listed last.

- Add the variant, its `as_manifest_str` arm, and its entry in
  `ManifestDataType::ALL` (the witness match inside `ALL`'s initializer
  breaks first).
- Decide whether the variant is partition-legal in
  `TryFrom<ManifestDataType> for FilePartitionDataType`.
- Lower it from v4 in `IrScalarType::lower` if importers can produce it.
- Update the `manifest_data_type` (and, if partition-legal,
  `file_partition_data_type`) enums in
  `src/schema/source_manifest.schema.json`; the golden tests in
  `src/schema.rs` fail until they match.
- Give it an Arrow lowering and value-level handling in `coral-engine`
  (compiler-enforced there; see that crate's AGENTS.md).
- Update the data-type table in `docs/reference/source-spec-reference.mdx` —
  this one is not enforced by any test.
