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
`decypher`, and then routes it through the normal static-list compiler. Static
list comprehensions use the same source-backed recovery for the
`variable IN collection` header and optional `WHERE` filter expression. Their
supported map expressions are evaluated as compile-time literal transforms over
each folded item, currently covering identity maps, scalar literals and
parameters, `toString`, string case conversion, trim variants, and `replace`.
This is limited to static folded collections and exists so collection
predicates and static comprehensions remain semantically correct without
introducing SQL-rendering shortcuts in the frontend.

Some Cypher constructs are blocked before Coral compilation because the current
parser dependency does not accept or fully preserve their standard syntax. Coral
keeps narrow source-compatibility normalizers for parser gaps that can lower
directly into existing IR, currently compact counted patterns and static
`range(start, end[, step])`. Broader frontend blockers, such as unsupported
dynamic list-comprehension sources or map expressions, should be addressed in
the parser frontend or shared expression IR rather than by broad query-string
rewriting in Coral.

The supported foundation subset is intentionally narrow:

- read-only single-part queries and transparent multi-part `MATCH` queries;
- one or more non-optional `MATCH` clauses with connected path parts or
  disconnected mandatory parts lowered as explicit cartesian products;
- anchored `OPTIONAL MATCH` pattern parts lowered as null-preserving left joins,
  including single-hop directed optional-local predicates and inline property
  maps placed in the join scope; optional plans still require mandatory
  bindings to stay anchored to the first component, and later mandatory
  `MATCH` clauses are allowed only when dependency analysis proves their pattern
  and local `WHERE` avoid variables introduced by the optional scope. Exact
  positive optional ranges such as `*2`, `*2..2`, and `{2}` reuse fixed-hop
  expansion inside one nullable optional scope. Same-label exact zero-hop
  optional ranges such as `*0` lower to an identity predicate when no named path
  needs optional presence gating; multi-length optional branch expansion remains
  deferred;
- non-materialized fixed-length path-variable bindings, including nullable
  `length(path)` and `size(path)` over optional relationships via the
  presence-gated scalar expression IR and compiler-generated internal
  relationship bindings when an anonymous optional path needs a presence gate;
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
  repeated bindings may omit the label. In declaration-aware compilation,
  fresh named and anonymous endpoints may omit the label when an exact typed
  relationship pattern, or an untyped exact single-hop relationship pattern,
  and the graph declaration infer one unique endpoint label; declaration-free
  compilation keeps rejecting first-bound named variables and anonymous nodes
  without labels;
- directed, reverse, and undirected typed relationships, with
  `startNode(r)` / `endNode(r)` endpoint functions over cross-label
  undirected relationships when a single graph declaration mapping recovers
  original edge orientation. Declaration-aware compilation may infer the type
  for an untyped exact single-hop relationship when the endpoint labels and
  direction select exactly one relationship type; untyped ranges and ambiguous
  endpoint pairs remain rejected;
- connected multi-hop relationship chains;
- `WHERE` comparisons combined with `AND`, `OR`, `XOR`, `NOT`, and
  parentheses, with `XOR` lowered as a null-preserving boolean rewrite;
- literal-left and chained comparisons normalized into property predicates;
- integer and finite floating-point predicate literals;
- `IN` predicates over scalar literal lists, including numeric and null members,
  and parenthesized static folded list expressions such as
  `property IN (['prod'] + $tiers)`;
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
  `labels(...) < [...]`, `keys(...) = [...]`, `keys(...) <> [...]`,
  `keys(...) >= [...]`, and `tail(...)` static-list predicates folded from
  static label/property metadata or typed folded-list expressions, including
  reversed operands, list-parameter operands, and lexicographic ordered
  comparisons over compatible string or numeric static lists;
- zero-based positive and negative index expressions over static folded lists,
  including `labels(...)`, declaration-aware `keys(...)`, `tail(...)`,
  `reverse(...)`, static `split(...)`, and static list concatenation, folded at
  compile time with out-of-range indexes returning `NULL`;
- start-inclusive/end-exclusive slice expressions over static folded lists,
  including `labels(...)`, declaration-aware `keys(...)`, `tail(...)`,
  `reverse(...)`, static `split(...)`, and static list concatenation, folded at
  compile time and preserving optional nulls for nullable bindings. Empty static
  slices are carried through the IR as typed folded lists so DataFusion can
  render typed empty arrays instead of ambiguous `make_array()` values;
- `head(...)`, `last(...)`, `tail(...)`, and list-valued `reverse(...)` over
  literal lists, list parameters, static `split(...)`, and static metadata
  lists, folded at compile time with `NULL` for empty matched lists in
  `head(...)` / `last(...)`, typed empty-list results for `tail(...)` and
  `reverse(...)`, and optional null preservation for nullable graph bindings;
- static `split(source, delimiter)` over string literals or scalar string
  parameters, folded into typed string lists with a capped expansion size.
  Empty delimiters and dynamic graph-property split arguments remain rejected
  until Coral has a dynamic list IR;
- static list concatenation with `+` over literal lists, list parameters,
  `tail(...)`, static `split(...)`, `labels(...)`, and declaration-aware
  `keys(...)`, plus `list + element` and `element + list` when the element is a
  scalar literal or scalar parameter. Concatenation is folded at compile time
  and preserved as typed list IR in projections, `ORDER BY`, static `UNWIND`,
  `size(...)`, endpoint list functions, static collection predicates, static
  list comprehensions, and parenthesized `IN` right-hand sides. Nullable static
  metadata lists on the right-hand side of `IN` preserve optional-match nulls
  with the same presence gating used by scalar metadata expressions.
  Concatenation rejects mixed non-null element types, unknowable projected
  element types, dynamic operands, and lists from different optional bindings;
- static list cast functions `toStringList(...)`, `toIntegerList(...)`,
  `toFloatList(...)`, and `toBooleanList(...)` over folded static lists. Casts
  use Cypher's nullable per-element conversion semantics and then re-enter the
  same typed static-list IR, so cast outputs compose with projection,
  `ORDER BY`, static `UNWIND`, indexes/slices, endpoint list functions,
  predicates, and list comprehensions. Literal `NULL` list inputs and dynamic
  list values remain rejected until Coral has a nullable dynamic-list IR;
- parser-accepted static list comprehensions such as `[k IN keys(node)]`,
  `[l IN labels(node)]`, `[x IN ['a', 'b']]`, `[x IN $list]`, and
  `[x IN split('a,b', ',')]`, folded as typed static-list expressions in
  projections and `ORDER BY`. Static `WHERE` filters over the item variable,
  literals, scalar parameters, comparisons, string predicates (`STARTS WITH`,
  `ENDS WITH`, `CONTAINS`, and regex), `IN` static lists, `IS NULL`, and
  `AND`/`OR`/`XOR`/`NOT` are evaluated before SQL lowering. Static map
  expressions over folded items support identity, scalar literals and
  parameters, numeric arithmetic, predicate-valued maps, `toString`, string case
  conversion, trim variants, and `replace`;
- `size(labels(...))` and declaration-aware `size(keys(...))` scalar
  expressions folded from static graph metadata, preserving optional nulls;
- static `all` / `any` / `none` / `single` collection predicates over literal
  lists, list parameters, `tail(...)`, `labels(...)`, and declaration-aware
  `keys(...)`, folded at compile time with Cypher unknown/null behavior,
  string predicate comparisons, and optional-match presence gates preserved;
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
- literal-only `WHERE` comparison, `IN`, and null-check predicates over
  supported static scalar expressions folded before SQL lowering, including
  arithmetic, `coalesce`, `nullIf`, character length, casts, string
  case/trim/replace, substring, `left`, `right`, scalar-string `reverse`,
  numeric map functions, and scalar parameters;
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
- `EXISTS { MATCH ... }` and compact `EXISTS { pattern WHERE ... }` lower to SQL
  semi-joins in `WHERE`; scalar `EXISTS` projections lower as correlated
  `COUNT(*) > 0` expressions so they are executable by DataFusion in `RETURN`,
  can be sorted through their projected alias or an exact repeated projected
  expression, and can appear in searched `CASE` expressions when a scalar
  expression has only one correlated subquery. Compact pattern `WHERE` is
  recovered from decypher's lossless CST when the high-level AST classifies the
  `WHERE` as a subquery clause, then rewritten through the same scoped
  `MATCH ... WHERE ... FINISH` planner path as explicit existential subqueries.
  Inline property maps remain compact property predicates, while scoped
  `WHERE` clauses are carried as predicate-expression IR and rendered with
  subquery-local node/relationship aliases. Nested scoped subqueries still
  require staged planning and are rejected before SQL lowering;
- compact `COUNT { pattern WHERE ... }` is normalized before AST construction to
  `COUNT { MATCH pattern WHERE ... FINISH }`, allowing Coral to support GQL-style
  counted pattern syntax without depending on parser-private AST recovery. The
  normalized form then lowers through the same scoped count-subquery planner as
  explicit `COUNT { MATCH ... }`;
- property projections, identity projections, standalone and grouped `count(*)`,
  `count(property)`, `count(DISTINCT property)`, `count(node)`,
  `count(DISTINCT node)`, `count(relationship)` with keyed or keyless mappings,
  `count(DISTINCT relationship)` for keyed mappings, `collect(property)`,
  `collect(DISTINCT property)`, `collect(node)`, `collect(DISTINCT node)`, and
  keyed relationship or endpoint collections lowered through null-filtered
  `ARRAY_AGG` with an empty-list fallback. Graph-variable collection returns
  mapped stable keys rather than materialized graph objects. Aggregate scalar
  expression targets such as `collect(coalesce(n.tier, 'unknown'))`,
  `collect(n.risk > 0.8)`, `count(coalesce(n.tier, 'unknown'))`,
  `count(n.tier IS NULL)`, and `sum(n.risk + 1)` lower through the same
  scalar-expression renderer; correlated scalar subqueries are still rejected
  inside aggregate targets. Static pattern-alternative rewrites project
  aggregate expression targets as hidden per-branch aliases, then apply the
  aggregate over those aliases after `UNION ALL`; graph-variable collections
  from static alternatives use label/type-qualified graph identity values so
  keys from different mappings do not collide. GQL aggregate aliases include
  `collect_list`, `stdev_samp`, and `stdev_pop`; numeric property aggregates,
  property and identity `ORDER BY`, direct aggregate `ORDER BY` expressions
  that match
  projected aggregates, and projection alias `ORDER BY` including aggregate
  aliases;
- transparent `WITH` pass-through, graph-variable aliasing, terminal
  graph-variable `WITH` row modifiers, and terminal `WITH` projection subsets
  whose final `RETURN` can reorder or rename every projected alias, including
  bounded `WITH *` plus explicit scalar aliases when the final `RETURN`
  enumerates those aliases or uses `RETURN *` to expand visible graph variables
  plus the aliases. Aggregate aliases in `WITH * ... RETURN *` remain deferred
  because they require grouped scoped planning;
- top-level `UNION` and `UNION ALL` over independently supported branch queries
  with identical output names, column order, and catalog-compatible output
  types;
- single-part static `UNWIND` over literal lists, list parameters, static
  `range(...)`, static `split(...)`, and folded static list expressions. The
  Cypher frontend expands these into capped
  `UNION ALL` branches and substitutes the unwind variable as a scalar literal
  before normal graph planning. Duplicate list elements are intentionally
  preserved, aggregate projections are hoisted through the same outer union
  aggregation path used by static pattern alternatives, and empty lists compile
  to a forced-empty graph plan. Row-preserving hidden `ORDER BY` expressions are
  evaluated inside each expanded branch and stripped by the outer projection.
  Dynamic list-valued columns and `WITH`-scoped unwinds remain future row-source
  IR work rather than SQL-rendering shortcuts;
- exact fixed relationship ranges greater than one hop lowered as repeated
  fixed-hop joins when the graph declaration yields one unambiguous intermediate
  label sequence, including cross-label paths such as
  `(:Person)-[:ROUTES*2]->(:Incident)` through `Service`. Intermediate-label
  inference builds relationship-type label adjacency once per query pattern and
  stops collecting after two candidate sequences because two is enough to prove
  ambiguity and avoids combinatorial search across cyclic or highly connected
  declarations;
- exact zero-hop relationship ranges lowered as same-node identity predicates,
  and finite non-negative bounded mandatory relationship ranges and GQL
  relationship quantifiers lowered as fixed-hop alternatives, with outer row
  modifiers, aggregates, and `length(path)` / `size(path)` applied after
  expansion. Cross-label bounded ranges use declaration metadata to prune
  impossible hop counts before planning and keep only exact lengths with one
  unambiguous intermediate label sequence; ranges whose exact alternatives are
  all pruned lower to ordinary empty-result plans with `WHERE FALSE` so
  projections and result schemas remain stable;
- non-materialized path variable bindings in `MATCH p = (...)`, including
  `WITH *` pass-through when the path is later used only for supported metadata
  such as `length(path)` or `size(path)`, including in `WITH * WHERE`, and not
  as a graph value. Path metadata scalars can participate in supported scalar
  arithmetic, scalar functions, and `CASE` expressions such as
  `coalesce(length(path), 0)`;
- non-negative integer `SKIP` and `LIMIT` literals, scalar parameters, and
  static scalar expressions such as `(1 + 1)` and `coalesce($limit, 10)`.

Unsupported Cypher/GQL features fail with `UNSUPPORTED_CYPHER` diagnostics.
This includes writes, multi-hop or undirected optional-local predicates, path
value projection or filtering, unbounded variable-length paths, multi-length
branch expansion inside `OPTIONAL MATCH`, cross-label optional zero-hop ranges,
relationship-variable list bindings for zero-hop or multi-hop ranges,
ambiguous cross-label fixed-hop paths,
parameterized property maps, keyless relationship identity operations,
non-terminal projection boundaries, post-union result processing, scalar
projections containing multiple correlated `COUNT`/`EXISTS` subqueries, dynamic
or multipart `UNWIND`, general subqueries with `WITH`, `RETURN`, `UNION`, or
procedure calls, ordered metadata-list comparisons, dynamic list comparisons or
indexes, and broad expression semantics.

## GraphQL Frontend Boundary

The GraphQL frontend follows the same rule as Cypher: parse GraphQL into the
shared graph IR and let validation, catalog checks, SQL lowering, and execution
remain separate. The supported slice is intentionally graph-query oriented:

- exactly one query operation or anonymous selection set by default, with
  generated-client-style multi-operation documents accepted when callers provide
  an `operationName` for one named query operation;
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
- relationship-existence filters inside root or nested node `where` objects,
  using the same `out_TYPE`, `in_TYPE`, and `any_TYPE` names as traversal
  fields. For example, `where: { out_OWNS: { to: Service, where: { tier: { eq:
  "prod" } }, relationshipWhere: { source: { eq: "pagerduty" } } } }` lowers to
  a scoped `EXISTS` predicate and does not add relationship fields to the
  selected result shape;
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
  Label)`, or `any_TYPE(label: Label)`, with the endpoint argument optional
  when the graph declaration has exactly one matching endpoint label for the
  current source label, relationship type, and direction;
- nested relationship target filters via `where` and relationship property
  filters via `relationshipWhere`, with the same boolean composition support
  as root filters;
- relationship property and relationship type metadata projections through
  reserved `_edge { ... }` selections inside relationship fields, including
  `_id`, `_elementId`, and named and inline edge fragments.

Nested relationship fields compile directly to `NodePattern` and
`RelationshipPattern` IR entries. Endpoint labels are checked against the graph
declaration before lowering, or inferred from the declaration only when there is
one possible target; ambiguous relationship overloads must still pass `to`,
`from`, or `label` explicitly. The existing graph validator still resolves the
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
The schema includes exact-label and generated-client alias root node fields,
node `where` and `orderBy` inputs, relationship traversal fields with endpoint
enum defaults for unambiguous mappings, relationship-existence filter inputs
inside node `where` inputs, relationship `relationshipWhere` inputs, and
relationship object types for the properties and identity fields available
through `_edge` selections. Standard
GraphQL SDL cannot express Coral's
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
