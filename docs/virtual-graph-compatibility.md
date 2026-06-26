# Virtual Graph Compatibility Matrix

This matrix tracks production support. It is intentionally stricter than a PoC:
unsupported behavior should be rejected clearly instead of guessed.

## Declaration

| Feature | Status | Notes |
| --- | --- | --- |
| v1 YAML declarations | Supported foundation | Nodes, relationships, table refs, keys, properties |
| Duplicate label/type rejection | Supported foundation | Prevents ambiguous lowering |
| Endpoint label validation | Supported foundation | Relationship endpoints must reference declared node labels |
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
| Cypher parser | Planned | Must compile to shared IR, not directly to SQL |
| GraphQL parser | Planned | Must compile to shared IR, not Cypher strings |
| Writes | Rejected by product invariant | Coral virtual graph is read-only |

## Validation

All current and future compatibility checks must use synthetic fixtures only.
Live-source tests are intentionally excluded from product validation.
