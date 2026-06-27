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
| Ordering, skip, and limit | Supported foundation | Property order keys, identity order keys, direct projected aggregate expressions, projection aliases including aggregate aliases, row offset, and row limit |
| Execute/explain wrappers | Supported foundation | Preserves translated SQL and diagnostics |
| Declaration-aware plan validation | Supported foundation | Resolves variables/properties and rejects unsupported plan shapes before SQL rendering |
| Optional matches | Supported foundation | Anchored optional relationships lower to `LEFT JOIN`; single-hop directed optional-local predicates and inline property maps lower into the nullable join scope |
| Variable-length paths | Deferred | Requires recursive/path expansion semantics |
| Path values | Deferred | Requires graph value representation |

## Frontends

| Feature | Status | Notes |
| --- | --- | --- |
| Cypher parser | Supported foundation | `decypher` AST frontend compiles to shared IR, not directly to SQL |
| Single `MATCH ... RETURN` | Supported foundation | One non-optional MATCH clause with one or more connected pattern parts |
| Comma-separated `MATCH` patterns | Supported foundation | Supported when parts are connected by reused node variables |
| Labeled node patterns | Supported foundation | Requires named node variables; first binding needs exactly one static label, repeated bindings may omit the label |
| Typed directed relationships | Supported foundation | Requires one static relationship type |
| Undirected relationships | Supported foundation | Lowers to orientation-aware joins; same-label relationships use disjunctive endpoint conditions; inverse overloaded mappings that both match are rejected as ambiguous |
| Multi-hop relationship chains | Supported foundation | Forward, reverse, and mixed chains compile through the shared graph IR |
| Multiple `MATCH` clauses | Supported foundation | Transparent multi-part read clauses compile into one connected graph plan |
| `WHERE` property comparisons | Supported foundation | String, integer, float, boolean, null literal, and property-to-property comparisons |
| `WHERE id(...)` predicates | Supported foundation | Node ids and keyed relationship ids lower to mapped key comparisons and `IN` predicates |
| `WHERE type(r)` predicates | Supported foundation | Folded to boolean predicates because each relationship pattern has one static type |
| `WHERE node:Label` / `relationship:TYPE` predicates | Supported foundation | Static label/type expressions fold against the mapped node label or relationship type, including grouped conjunction/disjunction/negation; dynamic labels remain rejected |
| Chained comparisons | Supported foundation | Normalized to conjunctions, e.g. `10 <= n.score < 20` |
| Literal-left comparisons | Supported foundation | Operators are inverted around the property operand where possible |
| Literal-only predicates | Supported foundation | Non-null literal comparisons and literal `IN` membership fold to boolean predicates; null-producing unknown cases are rejected instead of guessed |
| `WHERE` boolean logic | Supported foundation | `AND`, `OR`, `NOT`, and parentheses lower to SQL boolean predicates |
| `WHERE ... IN [...]` | Supported foundation | Literal scalar lists, including numeric and null members, lower to SQL `IN`; empty lists lower to `FALSE` |
| `WHERE '<Label>' IN labels(node)` | Supported foundation | String-literal and scalar string parameter membership predicates fold against the statically mapped node label |
| `WHERE '<key>' IN keys(variable)` | Supported foundation | String-literal and scalar string parameter membership predicates lower against declared graph property metadata |
| Cypher parameters | Supported foundation | Explicit typed parameter API binds scalar values in literal positions and list values as `IN` right-hand sides before SQL lowering |
| `WHERE ... STARTS WITH` / `ENDS WITH` / `CONTAINS` | Supported foundation | String-literal RHS lowers to escaped SQL `LIKE` |
| `WHERE ... IS NULL` / `IS NOT NULL` | Supported foundation | Lowers to SQL `IS NULL` / `IS NOT NULL` |
| Inline node property maps | Supported foundation | Normalized to equality predicates, e.g. `(n:Service {tier: 'prod'})` |
| Inline relationship property maps | Supported foundation | Anonymous relationships get internal variables for property predicates |
| `RETURN` property projections | Supported foundation | Optional aliases are supported |
| `RETURN` literal projections | Supported foundation | Supports string, integer, finite float, boolean, null, scalar parameters, and non-empty homogeneous literal/list-parameter lists; empty, all-null, and mixed-type lists are rejected |
| `RETURN` / `ORDER BY` / `WHERE` scalar expressions | Supported foundation | Arithmetic `+`, `-`, `*`, `/`, and `%`, searched `CASE WHEN ... THEN ... ELSE ... END`, generic `CASE expr WHEN value THEN ... ELSE ... END`, plus `coalesce(...)`, casts, string case/trim/replace functions, `size(...)`/`char_length(...)`/`character_length(...)`, `substring(...)`, `left(...)`, `right(...)`, `reverse(...)`, and numeric `abs(...)`, `ceil(...)`, `floor(...)`, and `round(value[, places])` over graph properties, scalar literals, scalar parameters, and nested supported scalar expressions. CASE `WHEN` predicates support property/scalar comparisons, `IN` literal lists, `exists(property)`, null checks, and `AND`/`OR`/`NOT`; `id()`, `elementId()`, `labels()`, `type()`, label predicates, bound-variable null checks, `XOR`, and optional-variable references are not supported inside CASE yet. Exponentiation is not supported yet. |
| `RETURN DISTINCT` | Supported foundation | Supported for projected rows; `ORDER BY` with `DISTINCT` must use projected properties |
| `RETURN count(*)` | Supported foundation | Supported as a standalone aggregate projection |
| `RETURN count(property)` | Supported foundation | Supports `count(property)` and `count(DISTINCT property)` |
| `RETURN count(node)` | Supported foundation | Supports `count(node)` and `count(DISTINCT node)` over declared node keys |
| `RETURN count(relationship)` | Supported foundation | Counts keyed or keyless relationship rows; `count(DISTINCT relationship)` requires a declared relationship key |
| `RETURN collect(property)` | Supported foundation | Supports property collection with optional `DISTINCT`; collecting nodes, relationships, or paths is rejected until graph values are modeled |
| `RETURN id(...)` / `elementId(...)` / `type(r)` | Supported foundation | Projects mapped keys, string-cast mapped keys, and fixed relationship types; `elementId(relationship)` requires a declared relationship key; optional relationship types preserve nulls |
| `RETURN labels(node)` | Supported foundation | Projects the statically mapped label as a one-element list via DataFusion `make_array` |
| `RETURN keys(variable)` | Supported foundation | Projects declared property keys for node and relationship variables via DataFusion `make_array`; identity keys are included only when declared as graph properties |
| `RETURN sum/avg/min/max(property)` | Supported foundation | Numeric aggregate projections over mapped properties |
| `RETURN property, count(...)` | Supported foundation | Uses Cypher-style implicit grouping over projected properties |
| `ORDER BY`, `SKIP`, and `LIMIT` | Supported foundation | Property order keys, identity expressions including `elementId(...)`, direct aggregate expressions that match `RETURN` projections, projection aliases including aggregate aliases, and non-negative integer offsets/limits |
| `WITH` pass-through | Supported foundation | Transparent `WITH var, ...` and `WITH *` preserve bound graph variables |
| Terminal `WITH` projections | Supported foundation | Terminal projection, alias filtering, ordering, skip, and limit are supported without staging another `MATCH` |
| `OPTIONAL MATCH` | Supported foundation | Requires an already-bound node anchor and one connected pattern part; preserves unmatched rows with nullable optional bindings |
| Optional-local `WHERE` and inline property maps | Supported foundation | Supported for single-hop directed optional patterns by placing predicates inside the null-preserving join scope |
| General list-expression predicates | Rejected | Only literal-list `IN` and static `'<Label>' IN labels(node)` are supported; arbitrary list expressions need a richer list IR |
| Multi-hop or undirected optional-local predicates | Rejected | Needs broader optional-scope grouping and orientation-aware predicate placement |
| `WHERE XOR` | Rejected | Not portable across target SQL dialects |
| `WHERE ... =~` regex matching | Rejected | Needs regex dialect compatibility across DataFusion targets |
| Variable-length paths | Rejected | Needs recursive/path expansion semantics |
| Path variables and path values | Rejected | Needs graph value representation |
| User variables beginning with `__coral_` | Rejected | Prefix reserved for internal planner bindings |
| General `WITH`, `UNION`, subqueries, procedure calls | Rejected | Non-terminal projection boundaries and set/pipeline semantics need staged planning |
| Parameterized property maps | Rejected | Full map expansion would need shape-aware parameter semantics; use scalar inline property values instead |
| GraphQL parser | Supported foundation | Root-node query fields compile directly to shared IR with scalar property selections, `where`, `orderBy`, `limit`, `offset`/`skip`, and `distinct`; relationship fields named `out_TYPE(to: Label)`, `in_TYPE(from: Label)`, or `any_TYPE(label: Label)` compile to normal graph patterns; `_edge { ... }` projects relationship properties |
| GraphQL nested row modifiers | Rejected | Per-parent `orderBy`, `limit`, `offset`/`skip`, and `distinct` require collection semantics rather than global SQL modifiers |
| Writes | Rejected by product invariant | Coral virtual graph is read-only |

## Validation

All current and future compatibility checks must use synthetic fixtures only.
Live-source tests are intentionally excluded from product validation.
