# Virtual Graph Compatibility Matrix

This matrix tracks production support. It is intentionally stricter than a PoC:
unsupported behavior should be rejected clearly instead of guessed.

## Declaration

| Feature | Status | Notes |
| --- | --- | --- |
| v1 YAML declarations | Supported foundation | Nodes, relationships, table refs, keys, properties |
| Duplicate label/type rejection | Supported foundation | Prevents ambiguous lowering |
| Endpoint label validation | Supported foundation | Relationship endpoints must reference declared node labels |
| Catalog validation | Supported foundation | Checks mapped tables, columns, and required-filter constraints during explicit validation and query execution/explain |
| Multiple mappings per relationship type | Deferred | Needs disambiguation rules before support |

## Shared Graph IR

| Feature | Status | Notes |
| --- | --- | --- |
| Node scans | Supported foundation | One table per label |
| Directed relationship traversals | Supported foundation | Forward and reverse traversal lower to joins |
| Connected multi-hop paths | Supported foundation | Linear and closed connected patterns lower to deterministic joins |
| Property projections | Supported foundation | Node keys and exposed properties |
| Property predicates | Supported foundation | Literal and property-to-property comparisons with boolean expression trees |
| `COUNT(*)` | Supported foundation | Standalone aggregate projection only |
| Distinct projections | Supported foundation | `SELECT DISTINCT` over projected rows |
| Ordering, skip, and limit | Supported foundation | Property order keys, row offset, and row limit |
| Execute/explain wrappers | Supported foundation | Preserves translated SQL and diagnostics |
| Declaration-aware plan validation | Supported foundation | Resolves variables/properties and rejects unsupported plan shapes before SQL rendering |
| Optional matches | Deferred | Requires nullability-aware IR |
| Variable-length paths | Deferred | Requires recursive/path expansion semantics |
| Path values | Deferred | Requires graph value representation |
| Mixed aggregate/property projections | Rejected | Needs grouping IR |
| Aggregate ordering | Rejected | Needs aggregate/grouping IR |

## Frontends

| Feature | Status | Notes |
| --- | --- | --- |
| Cypher parser | Supported foundation | `decypher` AST frontend compiles to shared IR, not directly to SQL |
| Single `MATCH ... RETURN` | Supported foundation | One non-optional MATCH clause with one or more connected pattern parts |
| Comma-separated `MATCH` patterns | Supported foundation | Supported when parts are connected by reused node variables |
| Labeled node patterns | Supported foundation | Requires named node variables; first binding needs exactly one static label, repeated bindings may omit the label |
| Typed directed relationships | Supported foundation | Requires one static relationship type and one arrowhead |
| Multi-hop relationship chains | Supported foundation | Forward, reverse, and mixed chains compile through the shared graph IR |
| `WHERE` property comparisons | Supported foundation | String, integer, boolean, null literal, and property-to-property comparisons |
| `WHERE` boolean logic | Supported foundation | `AND`, `OR`, `NOT`, and parentheses lower to SQL boolean predicates |
| `WHERE ... IN [...]` | Supported foundation | Literal scalar lists lower to SQL `IN`; empty lists lower to `FALSE` |
| `WHERE ... IS NULL` / `IS NOT NULL` | Supported foundation | Lowers to SQL `IS NULL` / `IS NOT NULL` |
| Inline node property maps | Supported foundation | Normalized to equality predicates, e.g. `(n:Service {tier: 'prod'})` |
| Inline relationship property maps | Supported foundation | Anonymous relationships get internal variables for property predicates |
| `RETURN` property projections | Supported foundation | Optional aliases are supported |
| `RETURN DISTINCT` | Supported foundation | Supported for projected rows; `ORDER BY` with `DISTINCT` must use projected properties |
| `RETURN count(*)` | Supported foundation | Supported as a standalone aggregate projection |
| `ORDER BY`, `SKIP`, and `LIMIT` | Supported foundation | Property order keys, property projection aliases, and non-negative integer offsets/limits |
| `OPTIONAL MATCH` | Rejected | Needs nullability-aware IR and SQL lowering |
| Multiple `MATCH` clauses | Rejected | Needs multi-pattern planning and join ordering rules |
| `WHERE XOR` | Rejected | Not portable across target SQL dialects |
| `WHERE ... IN` with null list values | Rejected | Needs explicit Cypher null-membership semantics |
| Undirected or bidirectional relationships | Rejected | Needs explicit graph-expansion semantics |
| Variable-length paths | Rejected | Needs recursive/path expansion semantics |
| Path variables and path values | Rejected | Needs graph value representation |
| User variables beginning with `__coral_` | Rejected | Prefix reserved for internal planner bindings |
| `WITH`, `UNION`, subqueries, procedure calls | Rejected | Needs scope and pipeline semantics |
| Parameters | Rejected | Needs API/runtime parameter binding contract |
| Grouping | Rejected | Needs aggregate/grouping IR |
| `ORDER BY` aggregate aliases | Rejected | Needs aggregate/grouping IR |
| GraphQL parser | Planned | Must compile to shared IR, not Cypher strings |
| Writes | Rejected by product invariant | Coral virtual graph is read-only |

## Validation

All current and future compatibility checks must use synthetic fixtures only.
Live-source tests are intentionally excluded from product validation.
