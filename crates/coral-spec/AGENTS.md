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
- Keep authored function language, generic identity, and declared signatures
  transport-neutral and execution-neutral.
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
- Row-path, pagination and lookup-key inference are heuristics over vendor
  descriptors, so changing any of them can quietly reshape relations in
  unrelated sources. Diff `cargo run --locked -p xtask -- v4-metadata-report`
  across the change before submitting, and review every line of the diff,
  including sources the change was not aimed at. The three are coupled: the
  lookup-key allowlist is whatever query parameters pagination did not claim,
  so a pagination edit that changes which detector wins hands its displaced
  parameters to the dependent-join planner.
- Response-shape inference folds `allOf` and refuses `anyOf`/`oneOf`/`not`.
  Composition that intersects has one property map to ask questions of;
  alternation does not. Wrapped-list inference and response-cursor discovery
  must read the same folded view — if only one of them can see through
  composition, an operation is presented as a paginated table that silently
  stops after its first page.
- Keep the body next-URL name lexicon narrower than the response-header one. A
  header named `Next` is nearly always a URL, so `next`/`nextpage` are accepted
  there. A body field named `next` is very often a continuation token, and a
  token must reach `cursor_query` so it lands in the request parameter that
  expects it — `next_url_body` would request it as a URL. Do not harmonise the
  two lists.
