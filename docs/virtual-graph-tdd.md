# Virtual Graph TDD Guidelines

Every virtual graph feature follows this loop:

1. Write the feature contract in the compatibility matrix.
2. Add failing unit tests for parser or declaration behavior.
3. Add failing IR and SQL-lowering tests.
4. Add synthetic integration/e2e tests that compare graph-query results with
   equivalent SQL.
5. Add deterministic performance coverage when the feature can affect planning
   or execution cost.
6. Add or update an openCypher TCK-style baseline scenario when the feature maps
   to portable read-only Cypher behavior.
7. Implement the minimum production code needed to pass those tests.

## Required Test Tiers

- **Unit**: declaration validation, frontend acceptance/rejection, IR creation,
  SQL lowering, and diagnostics.
- **Integration**: file-backed synthetic Coral sources, translated SQL
  execution, explain plans, and structured errors.
- **E2E**: feature-level scenarios that compose declarations, frontend query,
  translation, execution, and result assertions.
- **Performance**: generated synthetic datasets and translation/planning budgets
  that do not require secrets or live services.

## Feature Readiness

A feature is not production-ready until:

- unsupported adjacent syntax has explicit rejection tests;
- generated SQL is snapshot-tested or asserted directly;
- equivalent SQL result checks pass on synthetic data;
- diagnostics are stable enough for API/CLI callers;
- performance risks are either measured or explicitly deferred in Linear.

## Cypher Frontend Checklist

For each added Cypher/GQL feature:

1. Add acceptance tests that assert the exact `GraphPlan` shape.
2. Add rejection tests for adjacent unsupported syntax so the subset boundary
   stays reviewable.
3. Add SQL-lowering assertions when the feature changes generated SQL.
4. Add synthetic `execute_cypher` or `explain_cypher` integration coverage.
5. Add a scenario to
   `crates/coral-engine/tests/fixtures/virtual_graph/opencypher_read_baseline.json`
   when the behavior is part of Coral's claimed read-only Cypher compatibility.
6. Update the compatibility matrix before broadening the parser.
