# Virtual Graph Compatibility Gates

Coral tracks Cypher compatibility with an openCypher TCK-style read baseline.
The gate is synthetic and read-only: each scenario runs through the same
production path as users, from Cypher parsing to virtual graph validation, SQL
lowering, DataFusion execution, and result comparison.

Coral also tracks GraphQL virtual graph compatibility with the same fixture
shape and coverage-floor contract. The GraphQL gate is not an upstream language
TCK; it is Coral's product contract for the GraphQL read adapter over virtual
graphs.

## Why This Exists

The openCypher TCK is the right external model for language conformance, but
Coral virtual graphs are not native mutable property graphs. Coral maps graph
labels and relationship types onto existing tables, so the conformance gate has
to translate graph fixtures into source manifests, graph declarations, and
expected tabular results.

The first Cypher gate is a curated baseline rather than a complete upstream TCK
import. It gives us a CI-backed contract for the read features we already claim
and a clear place to add translated scenarios as support expands. The GraphQL
gate follows the same pattern for adapter behavior that does not have a direct
openCypher equivalent.

## Current Cypher Gate

Run the Cypher gate locally with:

```sh
make virtual-graph-tck
```

The broader focused engine gate also runs it:

```sh
make virtual-graph-checks
```

Generate a machine-readable coverage summary with:

```sh
make virtual-graph-tck-report
```

CI runs the gate in the `Virtual Graph Core` workflow for pull requests and for
pushes to `main` or `prod`. The workflow also runs the JSON coverage report and
writes it into the GitHub step summary for review.

The baseline data lives at:

```text
crates/coral-engine/tests/fixtures/virtual_graph/opencypher_read_baseline.json
```

The runner lives at:

```text
crates/coral-engine/tests/engine/opencypher_tck_tests.rs
```

The report command lives in:

```text
xtask/src/tck.rs
```

## Upstream openCypher Inventory

Coral also maintains an upstream openCypher TCK inventory gate. This gate does
not claim that Coral executes the upstream suite. Instead, it parses the
upstream `tck/features` tree, classifies every feature group into Coral's
read-only product scope, and compares the curated Coral baseline against the
upstream scenario-definition count.

Run it locally with:

```sh
make virtual-graph-upstream-tck-report
```

The target clones the Apache-2.0 openCypher repository at tag `2024.3`, verifies
the pinned revision `677cbafabb8c3c5eed458fd3b1ec0daec8d67d23`, and runs:

```sh
cargo run --locked -p xtask -- virtual-graph-upstream-tck-report \
  --features-dir <openCypher checkout>/tck/features \
  --json
```

For the pinned upstream tree, the inventory currently reports 1,615 scenario
definitions across 220 feature files. Of those, 1,294 are read-candidate
scenario definitions after excluding mutation clauses and procedure calls that
are outside Coral virtual graph's read-only scope. Coral's curated baseline has
301 scenarios, which is 18.64% of the full upstream scenario-definition inventory
and 23.26% of the read-candidate inventory.

The inventory gate fails if:

- the pinned upstream scenario count drops below the recorded floor;
- the read-candidate scenario count drops below the recorded floor;
- upstream adds a new top-level feature group that Coral has not classified.

This gives us a scored backlog target without overstating conformance. A future
runner can promote specific upstream scenarios from inventory into executable
Coral fixtures as graph declarations, source tables, and expected tabular
results are translated.

## Current GraphQL Gate

Run the GraphQL gate locally with:

```sh
make virtual-graph-graphql
```

The broader focused engine gate also runs it:

```sh
make virtual-graph-checks
```

Generate a machine-readable coverage summary with:

```sh
make virtual-graph-graphql-report
```

CI runs the GraphQL gate in the `Virtual Graph Core` workflow and writes the
JSON report into the GitHub step summary next to the Cypher report.

The GraphQL report target uses the generic `virtual-graph-baseline-report`
xtask command. The existing `virtual-graph-tck-report` command is retained for
the Cypher TCK-style gate.

The baseline data lives at:

```text
crates/coral-engine/tests/fixtures/virtual_graph/graphql_read_baseline.json
```

The runner lives at:

```text
crates/coral-engine/tests/engine/graphql_baseline_tests.rs
```

## Cypher Scope

The baseline currently contains 301 representative read-only scenarios:

- `Match`: 15 scenarios for labeled node scans plus forward and reverse
  relationship matches.
- `Where`: 24 scenarios for comparisons, post-`WITH` property and identity
  joins, property-to-property comparisons, boolean conjunction/disjunction,
  negation, `XOR`, arithmetic predicates, chained range predicates,
  relationship-property filtering, bare boolean property filtering, `<>`
  inequality, empty-string filtering, list membership, metadata-list
  membership, string predicates, regex predicates, optional null filtering, and
  `exists(property)`.
- `RelationshipProperties`: 1 scenario for relationship property filtering.
- `OptionalMatch`: 15 scenarios for null-preserving rows, null filtering,
  `coalesce(...)`, optional-local predicates, relationship-property
  projections, zero-count aggregation for unmatched rows, reverse traversal,
  bound-endpoint optional joins, multiple optional clauses, mandatory matches
  after optional scopes, inline relationship property maps, named-path metadata,
  and connected multi-hop optional chains.
- `Aggregation`: 9 scenarios for grouped counts, `count(*)`, numeric
  aggregates, statistical aggregates, distinct statistical aggregates,
  `percentileCont(...)`, `count(DISTINCT ...)`, hidden aggregate ordering, and
  selected static-map aggregate targets.
- `With`: 14 scenarios for transparent scope filtering, scalar alias filters,
  `WITH *` filtering, non-terminal scalar aliases carried into a later `MATCH`,
  multi-stage `WITH ... WHERE` pipelines, bare boolean alias filtering,
  aggregate filtering through terminal `WITH`, terminal `WITH DISTINCT` scalar
  projection and filtering, relationship-property alias filtering, and terminal
  `WITH ORDER BY` / `SKIP` / `LIMIT` row modifiers with filtering.
- `CountSubquery`: 10 scenarios for `COUNT { ... }` projections, predicates,
  hidden ordering, scoped scalar `RETURN` validation, `RETURN DISTINCT scalar`
  row-counting, distinct-count threshold predicates, and reversed
  arbitrary-threshold count predicates.
- `CollectSubquery`: 3 scenarios for `COLLECT { MATCH ... RETURN scalar }`
  projection lists over correlated scoped patterns, including `RETURN DISTINCT`
  scalar collection and count-only `size(...)` / `isEmpty(...)` lowering over
  `COLLECT` subqueries.
- `PatternComprehension`: 2 scenarios for parser-recovered
  `[(pattern) WHERE predicate | scalar]` projection lists over correlated
  scoped relationship patterns, plus count-only `size(...)` / `isEmpty(...)`
  lowering over pattern comprehensions.
- `ExistsSubquery`: 2 scenarios for `EXISTS { ... }` predicates, including
  row-preserving `RETURN DISTINCT` inside the scoped subquery.
- `ListExpressions`: 26 scenarios for static `range(...)`, `split(...)`,
  indexes, slices, endpoint functions, collection predicates, and list
  comprehensions, including folded math-function map expressions and static
  `reduce(...)` folds, direct folded-list `ORDER BY` keys, compile-time
  dynamic label/type literal lists, folded string-list expressions, and
  statically selected list-valued `CASE`, plus legacy static `filter(...)` /
  `extract(...)` functions.
- `LiteralExpressions`: 15 scenarios for scalar literal projections and
  homogeneous literal-list projections.
- `MapExpressions`: 19 scenarios for static literal-map key extraction with
  `keys({ ... })`, static literal-map value lookup, compile-time selection of
  supported graph scalar/property map entries, including composition with
  projections, predicates, ordering, list endpoint functions, and `IN`
  membership, plus `properties(variable).field` and static string-index access
  over mapped graph properties.
- `ScalarExpressions`: 60 scenarios for searched and generic `CASE`, scalar-string
  `isEmpty(...)` predicates and projections, string case conversion, trim
  variants, `replace(...)`, `substring(...)`, `size(...)`, `left(...)`,
  `right(...)`, `contains(...)`, `startsWith(...)`, `endsWith(...)`,
  including bare string predicate functions in `WHERE` / `CASE WHEN`,
  `reverse(...)`, comparison operators over numeric, string, boolean, node
  property, and relationship property scalar expressions, chained numeric and
  string ranges, numeric `abs(...)`, `ceil(...)`, `floor(...)`, `round(...)`,
  `sqrt(...)`, `sign(...)`, and adjacent rejection coverage.
- `NullSemantics`: 14 scenarios for literal, property, static-map, optional,
  arithmetic, and function null propagation; `coalesce(...)` / `nullIf(...)`
  null normalization; null-aware `IN`; and adjacent null comparison / list
  membership rejection coverage.
- `MathematicalFunctions`: 18 scenarios for arithmetic operators, unary
  negation, precedence, `sqrt(...)`, rounding aliases, `exp(...)`, `log(...)` /
  `ln(...)`, `log10(...)`, `pow(...)` / `power(...)`, `pi()`, `e()`,
  trigonometric functions, degree/radian conversion, `atan2(...)`,
  `haversin(...)`, `isNaN(...)`, and adjacent rejection coverage.
- `TypeConversion`: 3 scenarios for strict `toString(...)`,
  `toInteger(...)`, `toFloat(...)`, `toBoolean(...)`, nullable
  `toStringOrNull(...)`, `toIntegerOrNull(...)`, `toFloatOrNull(...)`,
  `toBooleanOrNull(...)` composition, and adjacent rejection coverage.
- `GraphMetadata`: 5 scenarios for `id(...)`, `elementId(...)`, `type(...)`,
  `startNode(...)`, `endNode(...)`, `labels(...)`, and `keys(...)`.
- `RowModifiers`: 14 scenarios for `ORDER BY`, `NULLS FIRST` / `NULLS LAST`,
  `SKIP`, and `LIMIT`.
- `Parameters`: 3 scenarios for scalar/list/limit parameter binding,
  compile-time dynamic label-list parameters, and missing parameter rejection
  through the public parameterized Cypher execution API.
- `Unwind`: 13 scenarios for `UNWIND` list expansion, including range, split,
  list-parameter, concatenated-list, duplicate, distinct, empty-list,
  chained-unwind, filtered, matched, aggregated, sliced static `CASE`, and
  static expansion after transparent `WITH` sources.
- `ReturnDistinct`: 3 scenarios for `RETURN DISTINCT`, including computed
  expressions and grouped aggregate projections.
- `Union`: 4 scenarios for `UNION` duplicate removal and `UNION ALL`
  duplicate preservation, including computed branch projections.
- `PathMetadata`: 1 scenario for `length(path)`.
- `VariableLengthPaths`: 4 scenarios for exact fixed-length relationship
  ranges, bounded ranges, zero-hop bounded ranges, and bounded GQL
  relationship quantifiers.
- `PathValues`: 4 scenarios for fixed-path element id-list projections, static
  path element-list indexes, slices, endpoint functions, list reducers, and
  full materialized path return rejection.

Unsupported scenarios are not silently skipped. If a scenario is part of the
baseline, it must either produce the expected rows or the expected structured
error substring.

The fixture also declares `minimum_feature_counts`. The test runner verifies
that every scenario id is unique, every feature bucket is declared in the floor
map, and each bucket stays at or above its declared floor, so coverage cannot
silently shrink or move into an unreported category.

## GraphQL Scope

The GraphQL baseline currently contains 30 representative read-only scenarios:

- `RootSelection`: 2 scenarios for exact-label and generated root aliases.
- `ScalarFilters`: 8 scenarios for scalar operator objects, shorthand equality,
  list membership, and null checks.
- `BooleanFilters`: 1 scenario for `xor` and `not` composition.
- `RowModifiers`: 1 scenario for root ordering, offset, and limit.
- `Aggregation`: 6 scenarios for grouped `_count`, numeric property
  aggregates, distinct counts, and exact `_percentileCont(field:, percentile:)`.
- `IdentityFields`: 1 scenario for `_id` and `_elementId`.
- `NestedRelationships`: 1 scenario for relationship traversal with endpoint
  and relationship-property filters.
- `RelationshipExistence`: 1 scenario for `EXISTS`-style relationship filters.
- `GeneratedClientShape`: 5 scenarios for fragments, `__typename`, and
  `@skip`/`@include` directives.
- `ErrorHandling`: 4 expected rejections for unknown graph-declared properties,
  `_id` string predicates, and aggregate argument misuse.

The same fixture-level contract applies: ids must be unique, every feature
bucket must be declared, and each bucket must stay at or above its floor.

## Expansion Policy

When adding Cypher or GraphQL support:

1. Add or translate a representative scenario into the relevant baseline
   fixture.
2. Keep the graph fixture synthetic and deterministic.
3. Prefer end-to-end result assertions over parser-only assertions.
4. Document intentionally unsupported read behavior in
   `docs/virtual-graph-compatibility.md`.
5. Only promote scenarios into the gate when Coral intentionally claims that
   behavior.

Future work should extend the upstream inventory into a scenario-level importer
that publishes a scored matrix such as executable, passing, unsupported by
product design, and known gap counts.
