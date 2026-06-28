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
  before any SQL is rendered. Runtime execute/explain paths also pass the
  built Coral catalog into the validator so scalar expressions and predicates
  can reject catalog-known type mistakes, such as numeric keys mixed with
  string fallbacks, before DataFusion planning.
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
- catalog-aware scalar type validation for runtime graph execution, including
  `coalesce`, `nullIf`, CASE result branches, string and numeric functions,
  GQL scalar aliases such as `ceiling` and `ln`, arithmetic, and scalar/direct
  predicate operands.
- catalog-aware numeric aggregate target validation for runtime graph
  execution, so non-numeric mapped properties are rejected before DataFusion
  planning for `sum`, `avg`, `median`, `stDev`, and `stDevP`.
- SQL lowering for node scans, directed and undirected relationship traversals,
  property and identity projections, connected multi-hop paths, disconnected
  mandatory components as explicit `CROSS JOIN`s, property and identity
  predicates, grouping aggregates, ordering, `SKIP`, and `LIMIT`.
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

Some `decypher` high-level AST nodes are currently lossy for Cypher constructs
that Coral supports. Coral keeps these recovery paths narrow and source-backed:
for `all` / `any` / `none` / `single` collection predicates, the frontend
recovers the filter variable and collection expression from the lossless CST by
function span, reparses only the collection expression fragment through
`decypher`, and then routes it through the normal static-list compiler. This is
limited to static folded collections and exists so collection predicates remain
semantically correct without introducing SQL-rendering shortcuts in the
frontend.

The supported foundation subset is intentionally narrow:

- read-only single-part queries and transparent multi-part `MATCH` queries;
- one or more non-optional `MATCH` clauses with connected path parts or
  disconnected mandatory parts lowered as explicit cartesian products;
- anchored `OPTIONAL MATCH` pattern parts lowered as null-preserving left joins,
  including single-hop directed optional-local predicates and inline property
  maps placed in the join scope; optional plans still require mandatory
  bindings to stay anchored to the first component;
- non-materialized fixed-length path-variable bindings, including nullable
  `length(path)` over optional relationships via the presence-gated scalar
  expression IR and compiler-generated internal relationship bindings when an
  anonymous optional path needs a presence gate;
- declaration-aware `RETURN *` expansion in runtime Cypher execution/explain
  paths and `compile_cypher*_for_graph` helpers. Because Coral does not
  materialize graph objects, star expansion lowers visible graph variables to
  tabular metadata and property columns such as `service.__id`,
  `service.__labels`, `dependency.__type`, and `service.name`; declaration-free
  compile helpers keep rejecting `RETURN *`;
- explicit graph-variable `RETURN service` / `RETURN service AS svc` expansion
  through the same declaration-aware tabular contract, with aliases used as
  output column prefixes;
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
  accepted as `IN` right-hand sides and static metadata-list comparison
  operands;
- `id(node)`, `id(keyedRelationship)`, `type(relationship)`, `labels(node)`,
  and `keys(variable)` in projections, with optional relationship and node
  function projections preserving null for unmatched optional bindings;
- `isEmpty(labels(...))` and declaration-aware `isEmpty(keys(...))` boolean
  scalar expressions folded from static pattern/declaration metadata, with
  optional endpoint forms using the same presence-gated scalar IR as endpoint
  property and identity expressions;
- declaration-aware `labels(...) = [...]`, `labels(...) <> [...]`,
  `keys(...) = [...]`, `keys(...) <> [...]`, and `tail(...)` static-list
  predicates folded from static label/property metadata or typed folded-list
  expressions, including reversed operands and list-parameter operands;
- zero-based positive and negative index expressions over static
  `labels(...)` and declaration-aware `keys(...)` metadata lists, folded at
  compile time with out-of-range indexes returning `NULL`;
- start-inclusive/end-exclusive slice expressions over static `labels(...)`
  and declaration-aware `keys(...)` metadata lists, folded at compile time
  and preserving optional nulls for nullable bindings. Empty static slices are
  carried through the IR as typed folded lists so DataFusion can render typed
  empty arrays instead of ambiguous `make_array()` values;
- `head(...)`, `last(...)`, and `tail(...)` over literal lists, list
  parameters, and static metadata lists, folded at compile time with `NULL` for
  empty matched lists in `head(...)` / `last(...)`, typed empty-list results for
  `tail(...)`, and optional null preservation for nullable graph bindings;
- `size(labels(...))` and declaration-aware `size(keys(...))` scalar
  expressions folded from static graph metadata, preserving optional nulls;
- static `all` / `any` / `none` / `single` collection predicates over literal
  lists, list parameters, `tail(...)`, `labels(...)`, and declaration-aware
  `keys(...)`, folded at compile time with Cypher unknown/null behavior and
  optional-match presence gates preserved;
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
  DataFusion `CAST`, nullable scalar casts lowered to `TRY_CAST`, `nullIf`
  lowered to DataFusion `NULLIF`, string case
  conversion, whitespace trimming, `replace`, character length via `size`,
  `char_length`, and `character_length`, and zero-based `substring` lowered
  to DataFusion `SUBSTRING`, scalar-string `isEmpty(expr)` predicates lowered
  to `char_length(expr) = 0`, plus `left`, `right`, `reverse`, numeric
  `abs`, `ceil`, `floor`, `round`, `sqrt`, `sign`, `exp`, `log`, `log10`,
  constants `pi` and `e`, and trigonometric `sin`, `cos`, `tan`, `cot`,
  `asin`, `acos`, `atan`, `atan2`, `degrees`, `radians`, and `haversin`;
  `id(variable)`, `elementId(variable)`, and `type(relationship)` can also be
  nested in scalar expressions. `id(variable)` keeps the mapped key's native
  type, `elementId(variable)` lowers to a string cast, and relationship
  metadata preserves null for unmatched optional relationships. Cypher `log`
  lowers to DataFusion `ln` to preserve natural-log semantics, while `pi()`
  and `e()` compile to deterministic float literals and `haversin(x)` lowers
  as `(1 - cos(x)) / 2`;
- projected and ordered searched `CASE` expressions can reference optional
  bindings and preserve SQL null/unknown semantics without moving those
  predicates into row-filtering scope;
- inline node property maps normalized to equality predicates;
- inline relationship property maps normalized to equality predicates, with
  internal relationship variables for anonymous edges;
- `IS NULL` and `IS NOT NULL` predicates lowered with SQL null semantics;
- `EXISTS { MATCH ... }` lowered to SQL semi-joins in `WHERE`; scalar
  `EXISTS` projections lower as correlated `COUNT(*) > 0` expressions so they
  are executable by DataFusion in `RETURN`, can be sorted through their
  projected alias or an exact repeated projected expression, can appear in
  searched `CASE` expressions when a scalar expression has only one correlated
  subquery, and compact pattern
  `EXISTS { ... }` supports inline property maps when no compact `WHERE` clause
  is present;
- property projections, identity projections, standalone and grouped `count(*)`,
  `count(property)`, `count(DISTINCT property)`, `count(node)`,
  `count(DISTINCT node)`, `count(relationship)` with keyed or keyless mappings,
  `count(DISTINCT relationship)` for keyed mappings, `collect(property)`,
  `collect(DISTINCT property)` lowered through null-filtered `ARRAY_AGG`
  with an empty-list fallback, GQL aggregate aliases `collect_list`,
  `stdev_samp`, and `stdev_pop`, numeric property aggregates, property and
  identity `ORDER BY`, direct aggregate `ORDER BY` expressions that match
  projected aggregates, and projection alias `ORDER BY` including aggregate
  aliases;
- transparent `WITH` pass-through, graph-variable aliasing, terminal
  graph-variable `WITH` row modifiers, and terminal `WITH` projection subsets
  whose final `RETURN` can reorder or rename every projected alias;
- top-level `UNION` and `UNION ALL` over independently supported branch queries
  with identical output names, column order, and catalog-compatible output
  types;
- non-materialized path variable bindings in `MATCH p = (...)` when `p` is not
  carried by `WITH *` or used as a graph value;
- integer `SKIP` and `LIMIT`.

Unsupported Cypher/GQL features fail with `UNSUPPORTED_CYPHER` diagnostics.
This includes writes, multi-hop or undirected optional-local predicates, path
value projection or filtering, variable-length paths, parameterized property
maps, keyless relationship identity operations, non-terminal projection
boundaries, post-union result processing, scalar projections containing
multiple correlated `COUNT`/`EXISTS` subqueries, general subqueries with `WITH`,
`RETURN`, `UNION`, or procedure calls,
path/list length via `size`, ordered metadata-list comparisons, dynamic list
comparisons or indexes, and broad expression semantics.

## GraphQL Frontend Boundary

The GraphQL frontend follows the same rule as Cypher: parse GraphQL into the
shared graph IR and let validation, catalog checks, SQL lowering, and execution
remain separate. The supported slice is intentionally graph-query oriented:

- exactly one query operation or anonymous selection set;
- exactly one included root field whose field name is the graph node label, or
  a declaration-aware generated-client alias such as `service`, `Services`, or
  `services` when the alias resolves to exactly one declared label;
- root field aliases, which are accepted for generated-client compatibility but
  do not change Coral's flat tabular result shape;
- scalar property selections with optional GraphQL aliases;
- reserved `_id` and `_elementId` selections on nodes, lowered to mapped key
  and string element-id projections without overloading user properties named
  `id`;
- flat aggregate node fields `_count`, `_count(field:)`,
  `_countDistinct(field:)`, `_collect(field:)`, `_collectDistinct(field:)`,
  `_sum(field:)`, `_avg(field:)`, `_min(field:)`, and `_max(field:)`, lowered
  to the shared aggregate IR; selected non-aggregate properties become SQL
  grouping keys;
- node-level `__typename`, lowered as a static literal projection of the graph
  node label, and edge-level `__typename`, lowered as the static relationship
  type;
- named and inline fragments on node selections when their type condition
  matches the current graph label, and on `_edge` selections when their type
  condition matches the relationship type;
- `@include(if:)` and `@skip(if:)` on fields, fragment spreads, and inline
  fragments, with boolean literals or typed boolean variables;
- root `where` object predicates over selected node properties, including
  equality shorthand values such as `where: { tier: "prod" }`, explicit
  equality, inequality, range, string, regex, list-membership, and null filters
  plus generated-client-friendly operator aliases and negated property filters,
  with `and`, `or`, `xor`, and `not` boolean filter composition;
- reserved `_id` and `_elementId` identity filters and `orderBy` fields, where
  `_id` targets the mapped key and `_elementId` targets the string element id;
- `orderBy` object or list of objects using property or identity fields and
  `ASC` / `ASCENDING` / `DESC` / `DESCENDING`, including single-field
  shorthand objects such as `{ risk: DESC }`; multi-column shorthand ordering
  must use a list of single-field objects because GraphQL input object field
  order is not a stable sort-precedence contract;
- integer `limit` / `first`, `offset` / `skip`, and boolean `distinct` root
  arguments;
- typed GraphQL variables bound through the explicit variable API in supported
  scalar literal, scalar-list `in`, enum/string name, boolean, and
  non-negative integer positions, including scalar variables used as shorthand
  equality filters;
- nested relationship fields named `out_TYPE(to: Label)`, `in_TYPE(from:
  Label)`, or `any_TYPE(label: Label)`;
- nested relationship target filters via `where` and relationship property
  filters via `relationshipWhere`, with the same boolean composition support
  as root filters;
- relationship property and relationship type metadata projections through
  reserved `_edge { ... }` selections inside relationship fields, including
  `_id`, `_elementId`, and named and inline edge fragments.

Nested relationship fields compile directly to `NodePattern` and
`RelationshipPattern` IR entries. Endpoint labels are checked against the graph
declaration before lowering, and the existing graph validator still resolves the
final relationship mapping. Selected nested node properties and `_edge`
relationship properties are flattened into the tabular result set; GraphQL
object materialization is intentionally out of scope for the DataFusion
execution path.

Conflicting response aliases are rejected before SQL lowering; exact duplicate
projections, such as repeated `__typename` through fragments, are suppressed.
Fragment definition directives, operation directives, unknown directives,
mutations, subscriptions, nested row modifiers, and optional GraphQL traversals
are rejected with GraphQL-specific diagnostics until their IR contracts are
defined.

`graphql_schema_sdl_for_graph` generates a GraphQL execution schema from the
same declaration model. It is intentionally a schema view over the supported
compiler contract, not a separate runtime: query execution still compiles the
submitted GraphQL document into the shared graph IR before validation and SQL
lowering. Because v1 graph declarations do not include source column type
metadata, mapped graph properties use a custom `CoralGraphValue` scalar while
reserved identity fields use `_id: CoralGraphValue` and `_elementId: String`.
The schema includes root node fields, node `where` and `orderBy` inputs,
relationship traversal fields, relationship `relationshipWhere` inputs, and
relationship object types for the properties and identity fields available
through `_edge` selections. Standard GraphQL SDL cannot express Coral's
context-specific `_edge` field without changing the query contract to wrapper
objects, so SDL generation exposes the relationship object shapes while the
compiler remains the authority for validating `_edge` placement inside traversal
selections.

SDL generation is stricter than declaration parsing: names must be legal
GraphQL names, graph properties cannot collide with reserved virtual fields
such as `_id`, `_elementId`, or `__typename`, generated type names must be
unique, and relationship overloads must produce unambiguous `out_TYPE`,
`in_TYPE`, and `any_TYPE` fields. Ambiguous overloads remain queryable through
the compiler when an endpoint argument disambiguates them, but they cannot be
losslessly represented by one standard GraphQL field signature without a
broader schema design.
