# Virtual Graph Compatibility Matrix

This matrix tracks production support. It is intentionally stricter than a PoC:
unsupported behavior should be rejected clearly instead of guessed.

## Declaration

| Feature | Status | Notes |
| --- | --- | --- |
| v1 YAML declarations | Supported foundation | Nodes, relationships, table refs, keys, properties |
| Duplicate label/type rejection | Supported foundation | Prevents ambiguous lowering |
| Endpoint label validation | Supported foundation | Relationship endpoints must reference declared node labels |
| Catalog validation | Supported foundation | Checks mapped tables, columns, and required-filter constraints |
| Multiple mappings per relationship type | Deferred | Needs disambiguation rules before support |

## Shared Graph IR

| Feature | Status | Notes |
| --- | --- | --- |
| Node scans | Supported foundation | One table per label |
| Directed relationship traversals | Supported foundation | Forward and reverse traversal lower to joins |
| Property projections | Supported foundation | Node keys and exposed properties |
| Property predicates | Supported foundation | Conjunctive comparisons |
| `COUNT(*)` | Supported foundation | SQL lowering only in first slice |
| Ordering and limit | Supported foundation | Property order keys and row limit |
| Execute/explain wrappers | Supported foundation | Preserves translated SQL and diagnostics |
| Optional matches | Deferred | Requires nullability-aware IR |
| Variable-length paths | Deferred | Requires recursive/path expansion semantics |
| Path values | Deferred | Requires graph value representation |

## Frontends

| Feature | Status | Notes |
| --- | --- | --- |
| Cypher parser | Supported foundation | `decypher` AST frontend compiles to shared IR, not directly to SQL |
| Single `MATCH ... RETURN` | Supported foundation | One non-optional MATCH clause with one connected path pattern |
| Labeled node patterns | Supported foundation | Requires named node variables and exactly one static label |
| Typed directed relationships | Supported foundation | Requires one static relationship type and one arrowhead |
| `WHERE` property comparisons | Supported foundation | String, integer, boolean, and null literals; comparisons joined by `AND` |
| `WHERE ... IS NULL` / `IS NOT NULL` | Supported foundation | Lowers to SQL `IS NULL` / `IS NOT NULL` |
| Inline node property maps | Supported foundation | Normalized to equality predicates, e.g. `(n:Service {tier: 'prod'})` |
| `RETURN` property projections | Supported foundation | Optional aliases are supported |
| `RETURN count(*)` | Supported foundation | Supported as a standalone aggregate projection |
| `ORDER BY` and `LIMIT` | Supported foundation | Property order keys and non-negative integer limits |
| `OPTIONAL MATCH` | Rejected | Needs nullability-aware IR and SQL lowering |
| Multiple `MATCH` clauses | Rejected | Needs multi-pattern planning and join ordering rules |
| Undirected or bidirectional relationships | Rejected | Needs explicit graph-expansion semantics |
| Variable-length paths | Rejected | Needs recursive/path expansion semantics |
| Path variables and path values | Rejected | Needs graph value representation |
| Inline relationship property maps | Rejected | Needs relationship variable synthesis for anonymous relationships |
| `WITH`, `UNION`, subqueries, procedure calls | Rejected | Needs scope and pipeline semantics |
| Parameters | Rejected | Needs API/runtime parameter binding contract |
| `DISTINCT`, `SKIP`, grouping | Rejected | Needs aggregate/grouping IR |
| GraphQL parser | Planned | Must compile to shared IR, not Cypher strings |
| Writes | Rejected by product invariant | Coral virtual graph is read-only |

## Validation

All current and future compatibility checks must use synthetic fixtures only.
Live-source tests are intentionally excluded from product validation.
