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
  including single-hop directed and undirected optional-local predicates and
  inline property maps placed in the join scope. Optional plans still require
  mandatory bindings to stay anchored to the first component, but later
  mandatory `MATCH` clauses may continue from optional-introduced node
  bindings. The SQL lowerer keeps ordinary mandatory joins first when possible,
  then joins optional scopes before a mandatory relationship that is blocked on
  an optional endpoint, so unmatched optional rows are dropped by the following
  inner join. Later global `WHERE` predicates over still-optional bindings
  lower as ordinary row filters, while predicates attached to the `OPTIONAL
  MATCH` clause stay inside the nullable join scope. In branch-expanded plans,
  optional-local predicates receive the same branch-local missing-property
  normalization before validation, preserving nullable join semantics while
  treating properties absent from the selected mapping as `NULL`. Exact
  positive optional
  ranges such as `*2`, `*2..2`, and `{2}` reuse fixed-hop expansion inside one
  nullable optional scope. Same-label exact zero-hop optional ranges such as
  `*0` lower to an identity predicate for newly introduced endpoints when no
  nullable row boundary is needed; deterministic named zero-hop optional paths
  fold `length(path)`, `size(path)`, and `size(relationships(path))` to `0`,
  and `size(nodes(path))` to `1`, when the endpoint is newly introduced or the
  same already-bound variable. Zero-hop optional ranges over
  distinct endpoints that were already bound are row-preserving and do not add
  equality filters when path metadata is unused; named path metadata for those
  endpoints lowers as a searched `CASE` gated by endpoint identity, and
  optional-local `WHERE` predicates over the named zero-hop path are conjoined
  into that same metadata gate. Multi-length optional branch expansion remains
  deferred;
- non-materialized fixed-length path-variable bindings, including nullable
  `length(path)`, `size(path)`, `size(relationships(path))`, and
  `size(nodes(path))` over optional relationships via the presence-gated scalar
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
- named node variables where the first binding has one static or compile-time
  dynamic label, such as `:$($label)` with a scalar string parameter, and
  repeated bindings may omit the label. In declaration-aware compilation,
  fresh named and anonymous endpoints may omit the label when an exact typed
  relationship pattern, or an untyped exact single-hop relationship pattern,
  and the graph declaration infer one unique endpoint label. Declaration-aware
  standalone unlabeled node scans such as `MATCH (n)` and anonymous `MATCH ()`
  are lowered by expanding over the declared node labels through the same
  capped union-branch planner used for explicit label alternatives. After
  branch compilation, missing node or relationship property references
  introduced by those expanded mappings are normalized to branch-local `NULL`
  literals for projections, scalar projection expressions, hidden `ORDER BY`
  columns, aggregate source columns, aggregate scalar expression targets, and
  predicates. Scalar-expression normalization rewrites missing-property leaves
  before validation, so wrappers such as `coalesce(n.tier, 'unknown')` or
  `coalesce(r.since, 'unknown')` keep their normal semantics. Predicate
  normalization lowers direct missing-property comparisons into scalar `NULL`
  comparisons so SQL three-valued logic preserves Cypher unknown behavior under
  `AND` / `OR` / `XOR` / `NOT`, while `IS NULL` / `IS NOT NULL` keep their
  expected null-check semantics. The same branch-local rewrite recurses into
  scoped `EXISTS { MATCH ... WHERE ... }` and existence-style
  `COUNT { MATCH ... WHERE ... }` predicates, combining outer branch bindings
  with subquery-local nodes and relationships before validation. Declaration-free
  compilation keeps rejecting first-bound named variables and anonymous nodes
  without labels. Bounded relationship-range pruning resolves the same
  compile-time dynamic endpoint labels before it consults graph declaration
  topology;
- directed, reverse, and undirected typed relationships, with
  `startNode(r)` / `endNode(r)` endpoint functions over cross-label
  undirected relationships when a single graph declaration mapping recovers
  original edge orientation. Same-label undirected endpoint property reads,
  endpoint identity scalars, endpoint metadata lists, and endpoint identity
  aggregate targets lower through dedicated scalar IR nodes that SQL rendering
  resolves with searched `CASE` expressions over the declared relationship
  `from`/`to` key columns. Materialized same-label endpoint graph values remain
  unsupported until Coral has a first-class graph value representation for
  data-dependent endpoints. Declaration-aware compilation may infer the type
  for an untyped exact single-hop relationship when the endpoint labels and
  direction select exactly one relationship type. Relationship type atoms may
  also be compile-time dynamic, such as `:$($type)` with a scalar string
  parameter, including during bounded relationship-range pruning; untyped
  ranges, row-dependent dynamic types, and ambiguous endpoint pairs remain
  rejected;
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
- list-valued `coalesce(...)` over static lists and `NULL` values. The frontend
  accepts only arguments that already compile through the static-list path,
  infers one compatible element type, and lowers nullable metadata fallbacks as
  scalar `Coalesce(PresenceGated(TypedLiteralList), TypedLiteralList)` instead
  of introducing dynamic list values. Purely static cases whose first non-null
  list is unconditional remain foldable as `StaticListValue` so downstream
  `head(...)`, `tail(...)`, slice, index, and static `UNWIND` handling can reuse
  the same folded-list machinery. Scalar/list mixes, all-null or untyped empty
  list outputs, dynamic list columns, and lists gated by different optional
  bindings remain rejected. Membership predicates such as
  `property IN coalesce(keys(optionalNode), ['fallback'])` lower as branch-local
  scalar predicates, preserving matched optional metadata semantics while letting
  unmatched optional branches fall through to later static-list fallbacks.
  Sliced RHS membership such as
  `property IN coalesce(keys(optionalNode), ['fallback'])[0..1]` uses the same
  branch-local path after slicing each folded branch.
  Collection predicates such as `any(k IN coalesce(...) WHERE ...)` use the same
  branch-local strategy, with each branch evaluated by the existing folded static
  collection predicate evaluator. Sliced collection predicates such as
  `any(k IN coalesce(keys(optionalNode), ['fallback'])[0..1] WHERE ...)` slice
  each folded branch before that evaluator runs.
  Scalar index, slice, sliced-list comparison, and sliced-endpoint expressions such as
  `coalesce(keys(optionalNode), ['fallback'])[0]` and
  `coalesce(keys(optionalNode), ['fallback'])[0..1][0]` are lowered by reducing
  each static branch and wrapping optional metadata branches in presence-gated
  scalar `CASE` expressions. `head(...)`, `last(...)`, `size(...)`, and
  `isEmpty(...)` over list-valued or sliced list-valued `coalesce(...)` are
  handled as scalar reducers over those same static branches, e.g.
  `coalesce(size(branch1), size(branch2))`, so optional metadata fallbacks work
  without adding runtime array endpoint or array-length operators. Because
  reducer outputs do not render an array value, all-empty branches such as
  `size(coalesce([], []))` can compile even though projecting `coalesce([], [])`
  remains rejected as an untyped dynamic-list boundary;
- list-valued `CASE` result branches over static lists and `NULL` values. The
  `CASE` compiler first probes branch result expressions for static-list shapes;
  if any non-null branch is a list, every non-null `THEN` / `ELSE` branch must
  be a static-list expression with one compatible inferred element type. Branch
  outputs are lowered through the existing scalar `CASE` IR as typed literal
  lists, presence-gated metadata lists, or list-valued static `coalesce(...)`.
  This keeps conditional optional-metadata normalization in the same SQL
  renderer and validator path as scalar `CASE` while still rejecting scalar/list
  mixes, mixed element families, all-empty/all-null results, and dynamic list
  columns. `property IN CASE ... END` lowers to a scalar boolean `CASE` whose
  branch results are ordinary folded-list membership predicates, which allows
  empty or null branches in predicate position without rendering an untyped array
  value. Sliced RHS membership over `(CASE ... END)[start..end]` slices each
  folded branch before applying the same membership lowering. Static collection
  predicates over `CASE` collections lower the same way,
  except each branch result is the folded outcome of `all` / `any` / `none` /
  `single` over that branch's list; sliced collection predicates reduce each
  branch's slice before evaluating the quantifier. Indexed, sliced,
  sliced-comparison, and sliced-index CASE
  collections such as `(CASE ... END)[0]`, `(CASE ... END)[0..1]`, and
  `((CASE ... END)[0..1])[0]` also lower branch-locally, so empty or null
  branches become typed empty-list, scalar predicate, or `NULL` branch results while metadata
  branches keep their optional presence gates. `head(CASE ... END)`,
  `last(CASE ... END)`, `size(CASE ... END)`, `isEmpty(CASE ... END)`, and the
  same reducers over sliced CASE collections are scalar reducers over the same
  branch parts, so they compile all-empty branch sets by lowering to scalar
  `CASE` expressions rather than rendering an untyped list;
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
  projections, `ORDER BY`, exact/ordered static-list comparisons, and `IN`
  right-hand sides. Unsliced list-valued `coalesce(...)` and `CASE` sources
  reuse the same branch-local static-list reducers as membership and collection
  predicates: each branch is folded through the comprehension filter/map
  evaluator, then rendered as a typed list-valued `COALESCE` or scalar `CASE`
  expression with existing optional-presence gates intact. When such a
  conditional comprehension appears inside list comparison or on the right-hand
  side of `IN`, the frontend renders branch-local boolean comparison or
  membership expressions instead of emitting SQL predicates over a dynamic array
  expression. Static `WHERE` filters over the item variable, literals, scalar
  parameters,
  comparisons, string predicates (`STARTS WITH`, `ENDS WITH`, `CONTAINS`, and
  regex), `IN` static lists, `IS NULL`, and `AND`/`OR`/`XOR`/`NOT` are evaluated
  before SQL lowering. Static map expressions over folded items support
  identity, scalar literals and parameters, numeric arithmetic, predicate-valued
  maps, `toString`, string case conversion, trim variants, and `replace`.
  Conditional sources with slices remain a parser-front-end gap rather than a
  dynamic-list runtime feature;
- `size(labels(...))` and declaration-aware `size(keys(...))` scalar
  expressions folded from static graph metadata, preserving optional nulls;
- static `all` / `any` / `none` / `single` collection predicates over literal
  lists, list parameters, `tail(...)`, static-list `CASE` / `coalesce(...)`,
  `labels(...)`, and declaration-aware `keys(...)`, folded at compile time or
  lowered through branch-local scalar predicates with Cypher unknown/null
  behavior, string predicate comparisons, and optional-match presence gates
  preserved;
- `id(...)`, `type(relationship)`, static `'<Label>' IN labels(node)`
  membership, and branch-local membership over static-list `CASE` /
  `coalesce(...)` right-hand sides in predicates;
- static and compile-time dynamic `node:Label` / `relationship:TYPE`
  predicates, including grouped label-expression conjunction, disjunction, and
  negation evaluated against mapped labels and relationship types. Dynamic
  predicate atoms such as `node:$($label)` are folded when the expression is a
  string literal or scalar string parameter; row-dependent and list-valued
  dynamic labels remain rejected because Coral does not evaluate graph labels
  per source row;
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
- inline node property maps normalized to equality predicates, including
  direct graph property/key/element-id RHS expressions and prior `WITH` scalar
  aliases when the alias can be represented as a property, key, element-id,
  literal, or literal-list predicate RHS;
- inline relationship property maps normalized to equality predicates, with
  internal relationship variables for anonymous edges and the same direct
  graph-expression and restricted scalar-alias RHS support as node maps;
- `IS NULL` and `IS NOT NULL` predicates lowered with SQL null semantics;
- `EXISTS { MATCH ... }` and compact `EXISTS { pattern WHERE ... }` lower to SQL
  semi-joins in `WHERE`; scalar `EXISTS` projections lower as correlated
  `COUNT(*) > 0` expressions so they are executable by DataFusion in `RETURN`,
  can be sorted through their projected alias or an exact repeated projected
  expression, hidden direct ordering over precomputable single-anchor
  relationship patterns uses the same grouped `LEFT JOIN` path as count
  subqueries, and `EXISTS` can appear in searched `CASE` expressions when a
  scalar expression has only one correlated subquery. Compact pattern `WHERE` is
  recovered from decypher's lossless CST when the high-level AST classifies the
  `WHERE` as a subquery clause, then rewritten through the same scoped
  `MATCH ... WHERE ... FINISH` planner path as explicit existential subqueries.
  Inline property maps remain compact property predicates, while scoped
  `WHERE` clauses are carried as predicate-expression IR and rendered with
  subquery-local node/relationship aliases. Nested scoped
  `EXISTS { MATCH ... }` and `COUNT { MATCH ... }` predicates inside scoped
  subquery `WHERE` clauses are supported recursively for relationship and
  node-only patterns, with endpoint correlations resolved through child-local
  aliases, parent scoped aliases, or outer `MATCH` bindings. Nested `EXISTS`
  scoped boolean/scalar predicates can also reference parent scoped properties
  through the same alias renderer. When these scoped predicates live under a
  branch-expanded pattern, property references missing from the selected branch
  are normalized to branch-local `NULL` before scoped validation, including
  subquery-local relationship properties. `COUNT` predicates whose comparison
  is equivalent to existence, such as `COUNT { ... } > 0`,
  `0 < COUNT { ... }`, or `COUNT { ... } = 0`, lower to `EXISTS` /
  `NOT EXISTS` at top level and inside scoped subqueries, while tautological or
  impossible integer thresholds such as `COUNT { ... } >= 0` and
  `COUNT { ... } < 0` fold to boolean literals. Scoped parent-property
  predicates therefore avoid DataFusion's nested
  correlated scalar-subquery limits. Other nested count comparisons continue
  through the scalar count renderer and will require staged aggregate planning
  for broader parent-property support. `OPTIONAL MATCH`,
  `WITH`, `RETURN`, and `UNION` inside scoped subqueries still require staged
  planning and are rejected before SQL lowering;
- compact `COUNT { pattern WHERE ... }` is normalized before AST construction to
  `COUNT { MATCH pattern WHERE ... FINISH }`, allowing Coral to support GQL-style
  counted pattern syntax without depending on parser-private AST recovery. The
  normalized form then lowers through the same scoped count-subquery planner as
  explicit `COUNT { MATCH ... }`;
- explicit Cypher `ORDER BY ... NULLS FIRST/LAST` is normalized before typed AST
  construction because the current parser version accepts sort direction but
  does not model null placement. The Cypher frontend records null placement per
  sort item from the source text, parses the normalized query, then keys the
  recovered placement by the typed sort expression span. Normal order lowering
  carries that into the shared `OrderKey.nulls` field, so single plans, terminal
  `WITH`, static pattern alternatives, and static `UNWIND` outer ordering all
  render the same DataFusion `NULLS FIRST` / `NULLS LAST` SQL;
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
  inside aggregate targets. Static and compile-time dynamic pattern-alternative
  rewrites resolve dynamic label/type atoms before branch expansion, while
  declaration-aware standalone unlabeled node scans synthesize one node-label
  branch per declaration mapping. Both paths project aggregate expression
  targets as hidden per-branch aliases, then apply the aggregate over those
  aliases after `UNION ALL`; graph-variable collections from branch-expanded
  alternatives use label/type-qualified graph identity values so keys from
  different mappings do not collide. GQL aggregate aliases include
  `collect_list`, `stdev_samp`, and `stdev_pop`; numeric property aggregates,
  property and identity `ORDER BY`, direct aggregate `ORDER BY` expressions
  that match projected aggregates, projection alias `ORDER BY` including
  aggregate aliases, hidden direct `ORDER BY` over precomputable single-anchor
  relationship-pattern `COUNT { ... }` subqueries, and explicit null placement
  on supported sort keys;
- transparent `WITH` pass-through, graph-variable aliasing, non-terminal
  deterministic scalar aliases, including `WITH *` plus deterministic scalar
  aliases, that can be inlined into later `MATCH`, `WHERE`, `RETURN`, and
  `ORDER BY` expressions, terminal graph-variable `WITH DISTINCT` and row
  modifiers, and terminal `WITH` projection subsets whose final `RETURN` can
  reorder or rename every projected alias, including bounded `WITH *` plus
  explicit scalar aliases when the final `RETURN` enumerates those aliases or
  uses `RETURN *` to expand visible graph variables plus the aliases. Terminal
  graph-variable `WITH DISTINCT` preserves
  graph-variable returns and `RETURN *`; scalar projections after that boundary,
  non-terminal aggregate or subquery aliases, and aggregate aliases in
  `WITH * ... RETURN *` remain deferred because they require grouped or staged
  scoped planning;
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
  evaluated inside each expanded branch and stripped by the outer projection;
  explicit null placement is preserved on the final outer order keys.
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
  such as `length(path)`, `size(path)`, `size(relationships(path))`, or
  `size(nodes(path))`, including in `WITH * WHERE`, and not as a graph value.
  Path metadata scalars can participate in supported scalar arithmetic, scalar
  functions, and `CASE` expressions such as `coalesce(length(path), 0)`;
- non-negative integer `SKIP` and `LIMIT` literals, scalar parameters, and
  static scalar expressions such as `(1 + 1)` and `coalesce($limit, 10)`.

Unsupported Cypher/GQL features fail with `UNSUPPORTED_CYPHER` diagnostics.
This includes writes, multi-hop or undirected optional-local predicates, path
value projection or filtering, unbounded variable-length paths, multi-length
branch expansion inside `OPTIONAL MATCH`, cross-label optional zero-hop ranges
that introduce nullable endpoints, relationship-variable list bindings for
zero-hop or multi-hop ranges,
ambiguous cross-label fixed-hop paths,
parameterized property maps, keyless relationship identity operations,
non-terminal projection boundaries, post-union result processing, scalar
projections containing multiple complex or multi-anchor correlated
`COUNT`/`EXISTS` subqueries, dynamic or multipart `UNWIND`, general subqueries
with `WITH`, `RETURN`, `UNION`, or procedure calls, dynamic list comparisons or
indexes, branch-local sliced lists in list-comprehension/`UNWIND` contexts, and
broad expression semantics.

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
Exact duplicate root fields and nested relationship fields with the same
response name, field, and normalized arguments merge their child selections.
Relationship-field merging shares one graph traversal, which avoids accidental
row multiplication when generated clients repeat a traversal through fragments.
Same-response root or relationship fields with different traversal arguments
are rejected rather than guessed or lowered as separate joins.
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
