# Virtual Graph Architecture

Virtual graph is a production subsystem for querying existing Coral source
tables as a read-only graph. It must not depend on live credentials for
correctness tests, and it must not couple language frontends directly to SQL
string rendering.

## Core Boundaries

- **Declaration model**: versioned YAML mapping from graph labels and
  relationship types to Coral/DataFusion table references.
- **Validation**: checks declaration shape, duplicate labels/types, endpoint
  references, exposed properties, and clear path-qualified diagnostics.
- **Frontend parsers**: Cypher, GraphQL, and future GQL-style frontends compile
  into the shared graph IR.
- **Shared graph IR**: typed nodes, relationships, predicates, projections,
  ordering, aggregation, and limits.
- **SQL lowering**: the only layer that renders DataFusion SQL. It owns
  identifier quoting, join shape, predicate placement, and translated SQL.
- **Execution integration**: translated SQL executes through the existing
  `CoralQuery::execute_sql` and `CoralQuery::explain_sql` paths.

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
- SQL lowering for node scans, directed relationship traversals, property
  projections, property predicates, ordering, `COUNT(*)`, and `LIMIT`.
- `CoralQuery::execute_graph_plan` and `CoralQuery::explain_graph_plan`
  wrappers that preserve translated SQL and diagnostics while reusing the
  existing SQL execution path.
- synthetic tests that execute translated SQL through Coral's existing engine.
