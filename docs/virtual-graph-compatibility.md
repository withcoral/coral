# Virtual Graph Compatibility Matrix

This matrix tracks production support. It is intentionally stricter than a PoC:
unsupported behavior should be rejected clearly instead of guessed.

## Declaration

| Feature | Status | Notes |
| --- | --- | --- |
| v1 YAML declarations | Supported foundation | Nodes, relationships, table refs, keys, properties |
| Duplicate label rejection | Supported foundation | One node table per graph label |
| Duplicate relationship mapping rejection | Supported foundation | Relationship mappings must have a unique `type + from label + to label` signature |
| Endpoint label validation | Supported foundation | Relationship endpoints must reference declared node labels |
| Catalog validation | Supported foundation | Checks mapped tables, columns, and required-filter constraints during explicit validation and query execution/explain |
| Multiple mappings per relationship type | Supported foundation | The validator selects the mapping by endpoint labels and direction; ambiguous undirected inverse mappings are rejected |

## Shared Graph IR

| Feature | Status | Notes |
| --- | --- | --- |
| Node scans | Supported foundation | One table per label |
| Directed relationship traversals | Supported foundation | Forward and reverse traversal lower to joins |
| Relationship type overloads | Supported foundation | A relationship type can map to multiple edge tables when endpoint labels disambiguate the pattern |
| Connected multi-hop paths | Supported foundation | Linear and closed connected patterns lower to deterministic joins |
| Disconnected mandatory patterns | Supported foundation | Disconnected mandatory node/path components lower to explicit SQL `CROSS JOIN`; disconnected patterns that include `OPTIONAL MATCH` remain rejected until optional lowering is component-aware |
| Property projections | Supported foundation | Node keys and exposed properties |
| Property predicates | Supported foundation | Literal and property-to-property comparisons with boolean expression trees |
| Numeric literals | Supported foundation | Integer and finite floating-point literals, including negated values |
| `COUNT(*)` | Supported foundation | Standalone or grouped by projected properties |
| `COUNT(property)` | Supported foundation | Counts non-null mapped property values; optional `DISTINCT` is supported |
| `COUNT(node)` | Supported foundation | Counts node key occurrences; `COUNT(DISTINCT node)` counts distinct declared node keys |
| `COUNT(relationship)` | Supported foundation | Counts declared relationship key values; keyless relationship mappings are rejected |
| Collection aggregate functions | Supported foundation | `COLLECT(property)` and `COLLECT(DISTINCT property)` lower to DataFusion `ARRAY_AGG`; graph-value collection is deferred |
| Numeric aggregate functions | Supported foundation | `SUM`, `AVG`, `MIN`, and `MAX` over mapped graph properties |
| Grouped aggregate projections | Supported foundation | Property projections become SQL `GROUP BY` keys |
| Distinct projections | Supported foundation | `SELECT DISTINCT` over projected rows |
| Literal projections | Supported foundation | String, integer, finite float, boolean, null, scalar parameter literals, and non-empty homogeneous literal/list-parameter values can be projected |
| Identity projections | Supported foundation | `id(node)`, `id(keyedRelationship)`, `elementId(node)`, `elementId(keyedRelationship)`, and `type(relationship)` lower through mapped keys and fixed relationship types; `elementId` casts mapped keys to strings; optional `type(relationship)` returns null when the relationship is unmatched |
| Node label projections | Supported foundation | `labels(node)` lowers to a one-element DataFusion list containing the statically mapped node label and preserves null for unmatched optional nodes |
| Property key projections | Supported foundation | `keys(node)` and `keys(relationship)` lower to a deterministic list of declared graph property names and preserve null for unmatched optional bindings |
| Identity predicates | Supported foundation | `WHERE id(...)` compares mapped keys; `WHERE elementId(...)` compares string-cast mapped keys and rejects keyless relationships; `WHERE type(r)` is folded from the fixed relationship type |
| Ordering, skip, and limit | Supported foundation | Property order keys, identity order keys, boolean predicate scalar order keys, direct projected aggregate expressions, projection aliases including aggregate aliases, row offset, and row limit |
| Execute/explain wrappers | Supported foundation | Preserves translated SQL and diagnostics |
| Declaration-aware plan validation | Supported foundation | Resolves variables/properties and rejects unsupported plan shapes before SQL rendering |
| Optional matches | Supported foundation | Anchored single-hop optional relationships lower to `LEFT JOIN`; directed and undirected optional-local predicates and inline property maps lower into the nullable join scope |
| Variable-length paths | Deferred | Requires recursive/path expansion semantics |
| Path values | Deferred | Requires graph value representation |

## Frontends

| Feature | Status | Notes |
| --- | --- | --- |
| Cypher parser | Supported foundation | `decypher` AST frontend compiles to shared IR, not directly to SQL |
| Single `MATCH ... RETURN` | Supported foundation | One non-optional MATCH clause with one or more mandatory pattern parts |
| Comma-separated `MATCH` patterns | Supported foundation | Connected parts lower to joins; disconnected mandatory parts lower to explicit SQL `CROSS JOIN` |
| Labeled node patterns | Supported foundation | Named node variables are supported; anonymous node patterns are supported when they resolve to exactly one positive static label; static exclusions such as `:Service&!Team` are accepted, while label alternatives, dynamic labels, multiple required labels, and contradictory label expressions are rejected |
| Typed directed relationships | Supported foundation | Requires one positive static relationship type; static exclusions such as `:OWNS&!DEPENDS_ON` are accepted, while type alternatives, dynamic types, multiple required types, and contradictions are rejected. Exact-one relationship ranges/quantifiers such as `*1`, `*1..1`, `{1}`, and `{1,1}` are accepted as single-hop syntax compatibility |
| Undirected relationships | Supported foundation | Lowers to orientation-aware joins; same-label relationships use disjunctive endpoint conditions; inverse overloaded mappings that both match are rejected as ambiguous |
| Multi-hop relationship chains | Supported foundation | Forward, reverse, and mixed chains compile through the shared graph IR |
| Multiple `MATCH` clauses | Supported foundation | Transparent multi-part read clauses compile into one connected graph plan |
| `WHERE` property comparisons | Supported foundation | String, integer, float, boolean, null literal, property-to-property comparisons, and scalar-expression RHS comparisons |
| `WHERE id(...)` predicates | Supported foundation | Node ids and keyed relationship ids lower to mapped key comparisons and `IN` predicates |
| `WHERE type(r)` predicates | Supported foundation | Folded to boolean predicates because each relationship pattern has one static type; equality, inequality, `IN`, `STARTS WITH`, `ENDS WITH`, `CONTAINS`, and regex matching are supported |
| `WHERE node:Label` / `relationship:TYPE` predicates | Supported foundation | Static label/type expressions fold against the mapped node label or relationship type, including grouped conjunction/disjunction/negation; dynamic labels remain rejected |
| Chained comparisons | Supported foundation | Normalized to conjunctions, e.g. `10 <= n.score < 20` |
| Literal-left comparisons | Supported foundation | Operators are inverted around the property operand where possible |
| Literal-only predicates | Supported foundation | Non-null literal comparisons and literal `IN` membership fold to boolean predicates; null-producing unknown cases are rejected instead of guessed |
| `WHERE` boolean logic | Supported foundation | `AND`, `OR`, `XOR`, `NOT`, and parentheses lower to SQL boolean predicates; `XOR` is rendered as a null-preserving boolean rewrite instead of relying on target-specific SQL syntax |
| `WHERE ... IN [...]` | Supported foundation | Literal scalar lists, including numeric and null members, lower to SQL `IN`; empty lists lower to `FALSE` |
| `WHERE '<Label>' IN labels(node)` | Supported foundation | String-literal and scalar string parameter membership predicates fold against the statically mapped node label |
| `WHERE '<key>' IN keys(variable)` | Supported foundation | String-literal and scalar string parameter membership predicates lower against declared graph property metadata |
| Cypher parameters | Supported foundation | Explicit typed parameter API binds scalar values in literal positions and list values as `IN` right-hand sides before SQL lowering |
| `WHERE ... STARTS WITH` / `ENDS WITH` / `CONTAINS` | Supported foundation | String-literal and string-parameter RHS lowers to escaped SQL `LIKE`; scalar expression RHS lowers to DataFusion `starts_with`, `ends_with`, and `contains` |
| `WHERE ... =~` regex matching | Supported foundation | String-literal, string-parameter, and scalar expression RHS lowers to DataFusion `regexp_like`; semantics follow DataFusion/Rust regex rather than Neo4j's Java regex dialect |
| `WHERE ... IS NULL` / `IS NOT NULL` | Supported foundation | Lowers to SQL `IS NULL` / `IS NOT NULL` |
| Inline node property maps | Supported foundation | Normalized to equality predicates, e.g. `(n:Service {tier: 'prod'})` |
| Inline relationship property maps | Supported foundation | Anonymous relationships get internal variables for property predicates |
| `RETURN` property projections | Supported foundation | Optional aliases are supported |
| `RETURN` literal projections | Supported foundation | Supports string, integer, finite float, boolean, null, scalar parameters, and non-empty homogeneous literal/list-parameter lists; empty, all-null, and mixed-type lists are rejected |
| `RETURN` / `ORDER BY` / `WHERE` scalar expressions | Supported foundation | Arithmetic `+`, `-`, `*`, `/`, `%`, and `^`, unary numeric negation, boolean predicate scalar expressions in `RETURN` and `ORDER BY`, searched `CASE WHEN ... THEN ... ELSE ... END`, generic `CASE expr WHEN value THEN ... ELSE ... END`, plus `coalesce(...)`, `nullIf(...)`, strict casts `toString(...)`, `toInteger(...)`, `toFloat(...)`, `toBoolean(...)`, nullable casts `toStringOrNull(...)`, `toIntegerOrNull(...)`, `toFloatOrNull(...)`, `toBooleanOrNull(...)`, string case/trim/replace functions including GQL-style `lower(...)`, `upper(...)`, and `btrim(...)` aliases, `size(...)`/`char_length(...)`/`character_length(...)`, `substring(...)`, `left(...)`, `right(...)`, `reverse(...)`, numeric `abs(...)`, `ceil(...)`, `floor(...)`, `round(value[, places])`, `sqrt(...)`, `sign(...)`, `exp(...)`, `log(...)`, `log10(...)`, constants `pi()` and `e()`, trigonometric `sin(...)`, `cos(...)`, `tan(...)`, `cot(...)`, `asin(...)`, `acos(...)`, `atan(...)`, `atan2(y, x)`, `degrees(...)`, `radians(...)`, and `haversin(...)`, plus graph metadata `id(variable)`, `elementId(variable)`, and `type(relationship)` over graph properties, scalar literals, scalar parameters, and nested supported scalar expressions. `id(variable)` keeps the mapped key's native type; `elementId(variable)` lowers to a string cast and requires keyed relationships; `type(relationship)` lowers to the static relationship type. Runtime execute/explain validates catalog-known scalar types before SQL planning, so mixed `coalesce`/CASE branches, string functions over numeric values, numeric functions over strings, arithmetic over non-numeric values, and incompatible scalar/direct predicate operands fail with `INVALID_SCALAR_TYPE`; pure translation without a runtime catalog remains conservative for unknown property types. Metadata scalar expressions are valid in forms such as `toString(id(n))`, `coalesce(elementId(r), 'missing')`, `coalesce(type(r), 'missing')`, CASE result branches, scalar predicates, and `ORDER BY`, preserving `NULL` for unmatched optional relationships. Boolean predicate scalar expressions include comparisons, `IN`, null checks, static label/type predicates, `exists(property)`, `isEmpty(scalar)`, and `AND`/`OR`/`XOR`/`NOT`. `nullIf(a, b)` lowers to DataFusion SQL `NULLIF(a, b)`. Strict casts lower to DataFusion `CAST`; nullable casts lower to `TRY_CAST`. Cypher `log(...)` lowers to DataFusion `ln(...)` to preserve natural-log semantics; `pi()` and `e()` compile to deterministic float literals; `haversin(x)` lowers as `(1 - cos(x)) / 2`. Scalar-string `isEmpty(expr)` is supported as a predicate in `WHERE` and searched `CASE WHEN`, lowering to `char_length(expr) = 0`; list, map, and path emptiness are not modeled yet. CASE `WHEN` predicates support property/scalar comparisons, `IN` literal lists, `exists(property)`, `isEmpty(scalar)`, graph variable/id/elementId null checks, static label/type predicates, literal membership in `labels()` / `keys()`, `type(relationship)` comparisons and `IN`, scalar null checks, and `AND`/`OR`/`XOR`/`NOT`; projected and ordered CASE expressions may reference unmatched optional bindings and follow SQL null/unknown semantics without filtering rows. |
| `RETURN DISTINCT` | Supported foundation | Supported for projected rows; `ORDER BY` with `DISTINCT` must use projected properties |
| `RETURN count(*)` | Supported foundation | Supported as a standalone aggregate projection |
| `RETURN count(property)` | Supported foundation | Supports `count(property)` and `count(DISTINCT property)` |
| `RETURN count(node)` | Supported foundation | Supports `count(node)` and `count(DISTINCT node)` over declared node keys |
| `RETURN count(relationship)` | Supported foundation | Counts keyed or keyless relationship rows; `count(DISTINCT relationship)` requires a declared relationship key |
| `RETURN collect(property)` | Supported foundation | Supports property collection with optional `DISTINCT`; collecting nodes, relationships, or paths is rejected until graph values are modeled |
| `RETURN id(...)` / `elementId(...)` / `type(r)` | Supported foundation | Projects mapped keys, string-cast mapped keys, and fixed relationship types; `elementId(relationship)` requires a declared relationship key; optional relationship types preserve nulls |
| `RETURN labels(node)` | Supported foundation | Projects the statically mapped label as a one-element list via DataFusion `make_array` |
| `RETURN keys(variable)` | Supported foundation | Projects declared property keys for node and relationship variables via DataFusion `make_array`; identity keys are included only when declared as graph properties |
| `RETURN sum/avg/min/max/median/stDev/stDevP(property)` | Supported foundation | Numeric aggregate projections over mapped properties; runtime execute/explain rejects catalog-known non-numeric targets for `sum`, `avg`, `median`, `stDev`, and `stDevP`; `median` lowers to DataFusion `MEDIAN`, `stDev` lowers to sample standard deviation, and `stDevP` lowers to population standard deviation |
| `RETURN sum/avg/min/max/median(DISTINCT property)` | Supported foundation | Distinct numeric aggregate projections are rendered through DataFusion distinct aggregate calls; `stDev(DISTINCT ...)` and `stDevP(DISTINCT ...)` are rejected because DataFusion does not execute distinct standard-deviation aggregates |
| `RETURN property, count(...)` | Supported foundation | Uses Cypher-style implicit grouping over projected properties |
| `ORDER BY`, `SKIP`, and `LIMIT` | Supported foundation | Property order keys, identity expressions including `elementId(...)`, static metadata functions including `labels(...)` and `keys(...)`, boolean predicate scalar expressions, direct aggregate expressions that match `RETURN` projections, projection aliases including aggregate aliases, and non-negative integer offsets/limits |
| `WITH` pass-through | Supported foundation | Transparent `WITH var, ...`, `WITH var AS alias, ...`, and `WITH *` carry visible graph variables without staging; omitted graph variables leave scope but their prior joins still constrain rows, graph-variable aliases atomically rename the shared graph plan, `WHERE` predicates over carried variables lower into the normal predicate tree, and terminal graph-variable `WITH` clauses may apply `ORDER BY`, `SKIP`, and `LIMIT` when no later `MATCH` requires staged row semantics |
| Terminal `WITH` projections | Supported foundation | Terminal projection, alias filtering, ordering, skip, limit, pure `RETURN *` alias pass-through, and final `RETURN` reordering or renaming of every `WITH` alias are supported without staging another `MATCH` |
| `OPTIONAL MATCH` | Supported foundation | Requires an already-bound node anchor and one single-hop connected pattern part; preserves unmatched rows with nullable optional bindings |
| Optional-local `WHERE` and inline property maps | Supported foundation | Supported for single-hop directed and undirected optional patterns by placing predicates inside the null-preserving join scope |
| General list-expression predicates | Rejected | Only literal-list `IN` and static `'<Label>' IN labels(node)` are supported; arbitrary list expressions need a richer list IR |
| Multi-hop optional patterns | Rejected | Needs grouped optional-path lowering so a failed later hop nulls the whole optional path instead of leaving earlier optional bindings populated |
| Variable-length paths | Rejected | Exact-one relationship ranges/quantifiers are accepted as single-hop syntax; all other variable-length ranges still need recursive/path expansion semantics |
| Path variables in `MATCH p = (...)` | Supported foundation | Accepted as non-materialized compatibility bindings when `p` is not carried by `WITH *` and is not used as a graph value; explicit `WITH var, ...` drops ignored path bindings |
| Path values | Rejected | Returning, filtering, or otherwise materializing path values needs graph value representation |
| User variables beginning with `__coral_` | Rejected | Prefix reserved for internal planner bindings |
| General `WITH`, `UNION`, subqueries, procedure calls | Rejected | Non-terminal projection boundaries and set/pipeline semantics need staged planning |
| Parameterized property maps | Rejected | The current parser dependency does not lower parameter-map pattern syntax into the typed AST; use inline property maps with scalar parameter values instead |
| GraphQL parser | Supported foundation | Root-node query fields compile directly to shared IR with scalar property selections, reserved node identity fields `_id` and `_elementId`, flat aggregate fields `_count`, `_count(field:)`, `_countDistinct(field:)`, `_collect(field:)`, `_collectDistinct(field:)`, `_sum(field:)`, `_sumDistinct(field:)`, `_avg(field:)`, `_avgDistinct(field:)`, `_median(field:)`, `_medianDistinct(field:)`, `_stDev(field:)`, `_stDevP(field:)`, `_min(field:)`, `_minDistinct(field:)`, `_max(field:)`, and `_maxDistinct(field:)`, declaration-aware generated-client root aliases such as `service`/`Services`/`services` when they resolve to exactly one declared label, root aliases, root-level named/inline fragments with `Query` type conditions, node- and edge-level `__typename`, named and inline fragments on matching node labels or relationship types, `@include(if:)` / `@skip(if:)`, `where`, `orderBy`, `limit`/`first`, `offset`/`skip`, and `distinct`; selected non-aggregate fields group aggregate results; typed GraphQL variables can bind scalar literals, scalar-list `in` values, object-shaped `where`/nested `where`/`relationshipWhere` filters and `orderBy` inputs, enum/string names, booleans, and non-negative integer row modifiers through the explicit variable API, with scalar/list/default enum values used when runtime variables are omitted; `where`, nested `where`, and `relationshipWhere` support scalar shorthand equality values such as `tier: "prod"` and property/reserved identity predicates including `eq`/`equals`, `ne`/`neq`/`notEqual`/`notEquals`, `gt`/`greaterThan`, `gte`/`ge`/`greaterThanEqual`/`greaterThanOrEqual`, `lt`/`lessThan`, `lte`/`le`/`lessThanEqual`/`lessThanOrEqual`, `startsWith`/`starts_with`, `endsWith`/`ends_with`, `contains`, `matches`/`regex`, `in`, `isNull`/`is_null`, `isNotNull`/`is_not_null`, and negated generated-client operators `notIn`, `notStartsWith`, `notEndsWith`, `notContains`, `notMatches`, and `notRegex`, plus `and`, `or`, binary `xor`, and `not` boolean composition; `_id` supports equality/range/list/null filters over mapped keys, while `_elementId` supports string identity filters; canonical `orderBy.field` accepts properties, `_id`, and `_elementId`, with `ASC`/`ASCENDING` and `DESC`/`DESCENDING` directions, and single-field shorthand order objects such as `{ risk: DESC }` are accepted with multi-column shorthand expressed as lists of single-field objects; relationship fields named `out_TYPE(to: Label)`, `in_TYPE(from: Label)`, or `any_TYPE(label: Label)` compile to normal graph patterns; `_edge { ... }` projects relationship properties, `_edge` identity fields `_id` and `_elementId`, and static relationship type metadata |
| GraphQL SDL generation | Supported foundation | `graphql_schema_sdl_for_graph` emits a parseable execution schema from a graph declaration, including root node fields, node and relationship object types, property, identity, and flat aggregate fields, traversal fields, `where` inputs with canonical, underscore, and uppercase boolean composition fields, `relationshipWhere` inputs, `orderBy` inputs, aggregate-field enums, row modifier arguments, and the custom `CoralGraphValue` scalar for untyped mapped properties. SDL generation rejects GraphQL-unsafe declaration names, reserved virtual-field property collisions, non-unique generated type names, and relationship overloads whose `out_TYPE` / `in_TYPE` / `any_TYPE` field shapes cannot be represented unambiguously in standard GraphQL SDL. |
| GraphQL unsupported generated-client features | Rejected | Fragment definition directives, operation directives, unknown directives, mismatched fragment type conditions, fragment cycles, and conflicting response aliases are rejected before SQL lowering |
| GraphQL object defaults | Supported foundation | Object and object-list defaults are recursively coerced for supported filter and `orderBy` inputs; mixed scalar/object default lists are rejected |
| GraphQL nested row modifiers | Rejected | Per-parent `orderBy`, `limit`/`first`, `offset`/`skip`, and `distinct` require collection semantics rather than global SQL modifiers |
| Writes | Rejected by product invariant | Coral virtual graph is read-only |

## Validation

All current and future compatibility checks must use synthetic fixtures only.
Live-source tests are intentionally excluded from product validation.
