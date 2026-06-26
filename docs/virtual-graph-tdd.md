# Virtual Graph TDD Guidelines

Every virtual graph feature follows this loop:

1. Write the feature contract in the compatibility matrix.
2. Add failing unit tests for parser or declaration behavior.
3. Add failing IR and SQL-lowering tests.
4. Add synthetic integration/e2e tests that compare graph-query results with
   equivalent SQL.
5. Add deterministic performance coverage when the feature can affect planning
   or execution cost.
6. Implement the minimum production code needed to pass those tests.

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
