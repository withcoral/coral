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
- `WHERE` comparisons combined with `AND`, `OR`, `XOR`, `NOT`, and
  parentheses, with `XOR` lowered as a null-preserving boolean rewrite;
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
  `LIKE` for literal and parameter RHS values, or DataFusion string functions
  for scalar expression RHS values;
- regex predicates lowered to DataFusion `regexp_like` for string literals,
  string parameters, and scalar expression RHS values; regex syntax follows
  DataFusion/Rust regex semantics, not Neo4j's Java regex dialect;
- scalar string, numeric, and conversion expressions in projections,
  predicates, and ordering, including arithmetic `+`, `-`, `*`, `/`, `%`,
  and `^`, unary numeric negation, `coalesce`, strict scalar casts lowered to
  DataFusion `CAST`, nullable scalar casts lowered to `TRY_CAST`, string case
  conversion, whitespace trimming, `replace`, character length via `size`,
  `char_length`, and `character_length`, and zero-based `substring` lowered
  to DataFusion `SUBSTRING`, plus `left`, `right`, `reverse`, numeric
  `abs`, `ceil`, `floor`, `round`, `sqrt`, `sign`, `exp`, `log`, `log10`,
  constants `pi` and `e`, and trigonometric `sin`, `cos`, `tan`, `cot`,
  `asin`, `acos`, `atan`, `atan2`, `degrees`, `radians`, and `haversin`;
  Cypher `log` lowers to DataFusion `ln` to preserve natural-log semantics,
  while `pi()` and `e()` compile to deterministic float literals and
  `haversin(x)` lowers as `(1 - cos(x)) / 2`;
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
procedure calls, path/list length via `size`, and broad expression semantics.

## GraphQL Frontend Boundary

The GraphQL frontend follows the same rule as Cypher: parse GraphQL into the
shared graph IR and let validation, catalog checks, SQL lowering, and execution
remain separate. The supported slice is intentionally graph-query oriented:

- exactly one query operation or anonymous selection set;
- exactly one root field whose name is the graph node label;
- scalar property selections with optional GraphQL aliases;
- root `where` object predicates over selected node properties, including
  `and`, `or`, and `not` boolean filter composition;
- `orderBy` object or list of objects using property fields and `ASC` / `DESC`;
- integer `limit`, `offset` / `skip`, and boolean `distinct` root arguments.
- nested relationship fields named `out_TYPE(to: Label)`, `in_TYPE(from:
  Label)`, or `any_TYPE(label: Label)`;
- nested relationship target filters via `where` and relationship property
  filters via `relationshipWhere`, with the same boolean composition support
  as root filters;
- relationship property projections through reserved `_edge { ... }`
  selections inside relationship fields.

Nested relationship fields compile directly to `NodePattern` and
`RelationshipPattern` IR entries. Endpoint labels are checked against the graph
declaration before lowering, and the existing graph validator still resolves the
final relationship mapping. Selected nested node properties and `_edge`
relationship properties are flattened into the tabular result set; GraphQL
object materialization is intentionally out of scope for the DataFusion
execution path.

Fragments, directives, variables, mutations, subscriptions, nested row
modifiers, and optional GraphQL traversals are rejected with GraphQL-specific
diagnostics until their IR contracts are defined.
