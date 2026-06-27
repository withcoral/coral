# Virtual Graph Architecture

Virtual graph is a production subsystem for querying existing Coral source
tables as a read-only graph. It must not depend on live credentials for
correctness tests, and it must not couple language frontends directly to SQL
string rendering.

## Core Boundaries

- **Declaration model**: versioned YAML mapping from graph labels and
  relationship mappings to Coral/DataFusion table references. Relationship
  types may be overloaded across distinct endpoint label pairs.
- **Validation**: checks declaration shape, duplicate labels/types, endpoint
  references, duplicate relationship mapping signatures, exposed properties,
  and clear path-qualified diagnostics.
- **Frontend parsers**: Cypher, GraphQL, and future GQL-style frontends compile
  into the shared graph IR.
- **Shared graph IR**: typed nodes, relationships, predicates, projections,
  distinct row selection, ordering, aggregation, offsets, and limits. Predicate
  operands can compare properties against literals or other graph properties,
  and boolean expression trees represent non-conjunctive filters.
- **Plan validation**: resolves IR variables to declaration mappings, selects
  overloaded relationship mappings by type, endpoint labels, and direction,
  checks property references, aggregate restrictions, and connected join shape
  before any SQL is rendered.
- **SQL lowering**: the only layer that renders DataFusion SQL. It owns
  identifier quoting, deterministic join planning for connected patterns,
  predicate placement, and translated SQL.
- **Execution integration**: graph declarations are validated against the
  runtime catalog before translation, and translated SQL executes through the
  existing `CoralQuery::execute_sql` and `CoralQuery::explain_sql` paths.

## Non-Negotiables

- Coral virtual graph is read-only.
- Every supported feature starts with failing tests.
- Unsupported features must fail with explicit diagnostics.
- Synthetic unit, integration, e2e, and generated performance fixtures are the
  source of truth. Live sources are out of scope for validation.
- Performance is part of the contract: SQL shape, projection pruning,
  predicate placement, planning overhead, and execution overhead must remain
  reviewable.

## First Production Slice

The foundation slice establishes:

- v1 graph declaration parsing and validation.
- declaration validation against Coral catalog snapshots for mapped tables,
  columns, and required-filter constraints.
- a typed shared graph query plan.
- a declaration-aware graph plan validator that frontloads user-facing semantic
  diagnostics before SQL lowering.
- SQL lowering for node scans, directed and undirected relationship traversals,
  property and identity projections, connected multi-hop paths, property and
  identity predicates, grouping aggregates, ordering, `SKIP`, and `LIMIT`.
- `CoralQuery::execute_graph_plan` and `CoralQuery::explain_graph_plan`
  wrappers that validate declarations against the built runtime catalog,
  preserve translated SQL and diagnostics, and reuse the existing SQL execution
  path.
- a strict read-only Cypher frontend based on `decypher` that accepts the first
  supported `MATCH ... RETURN` subset, rejects writes and unsupported GQL
  features structurally, and feeds the same shared graph query plan.
- `CoralQuery::execute_cypher` and `CoralQuery::explain_cypher` wrappers for
  text queries that preserve translated SQL and diagnostics.
- synthetic tests that execute translated SQL through Coral's existing engine.

## Cypher Frontend Boundary

The Cypher frontend is only a parser and compiler. It must not render SQL and
must not inspect source manifests or runtime state. Its output is a
`GraphPlan`; declaration validation, catalog validation, SQL lowering, and
execution remain separate layers.

The supported foundation subset is intentionally narrow:

- read-only single-part queries and transparent multi-part `MATCH` queries;
- one or more non-optional `MATCH` clauses with connected path parts;
- anchored `OPTIONAL MATCH` pattern parts lowered as null-preserving left joins,
  including single-hop directed optional-local predicates and inline property
  maps placed in the join scope;
- named node variables where the first binding has one static label and
  repeated bindings may omit the label;
- directed, reverse, and undirected typed relationships;
- connected multi-hop relationship chains;
- `WHERE` comparisons combined with `AND`, `OR`, `NOT`, and parentheses;
- literal-left and chained comparisons normalized into property predicates;
- integer and finite floating-point predicate literals;
- `IN` predicates over scalar literal lists, including numeric and null members;
- typed Cypher parameters bound through the explicit parameter API, where
  scalar parameters are accepted in literal positions and list parameters are
  accepted as `IN` right-hand sides;
- `id(node)`, `id(keyedRelationship)`, `type(relationship)`, `labels(node)`,
  and `keys(variable)` in projections, with optional relationship and node
  function projections preserving null for unmatched optional bindings;
- `id(...)`, `type(relationship)`, and static
  `'<Label>' IN labels(node)` membership in predicates;
- static `node:Label` and `relationship:TYPE` predicates, including grouped
  label-expression conjunction, disjunction, and negation evaluated against
  mapped labels and relationship types;
- string prefix, suffix, and substring predicates lowered to escaped SQL
  `LIKE`;
- inline node property maps normalized to equality predicates;
- inline relationship property maps normalized to equality predicates, with
  internal relationship variables for anonymous edges;
- `IS NULL` and `IS NOT NULL` predicates lowered with SQL null semantics;
- property projections, identity projections, standalone and grouped `count(*)`,
  `count(property)`, `count(DISTINCT property)`, `count(node)`,
  `count(DISTINCT node)`, `count(relationship)` with keyed or keyless mappings,
  `count(DISTINCT relationship)` for keyed mappings, `collect(property)`,
  `collect(DISTINCT property)`, numeric property aggregates, property and
  identity `ORDER BY`, direct aggregate `ORDER BY` expressions that match
  projected aggregates, and projection alias `ORDER BY` including aggregate
  aliases;
- transparent `WITH` pass-through and terminal `WITH` projection subsets;
- integer `SKIP` and `LIMIT`.

Unsupported Cypher/GQL features fail with `UNSUPPORTED_CYPHER` diagnostics.
This includes writes, multi-hop or undirected optional-local predicates, path
variables, variable-length paths, parameterized property maps, keyless
relationship identity operations, non-terminal projection boundaries, subqueries,
procedure calls, and broad expression semantics.
