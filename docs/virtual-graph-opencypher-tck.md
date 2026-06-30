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
119 scenarios, which is 7.37% of the full upstream scenario-definition inventory
and 9.20% of the read-candidate inventory.

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

The baseline currently contains 119 representative read-only scenarios:

- `Match`: 3 scenarios for labeled node scans plus forward and reverse
  relationship matches.
- `Where`: 11 scenarios for comparisons, property-to-property comparisons,
  boolean conjunction/disjunction, negation, `XOR`, list membership,
  metadata-list membership, string predicates, regex predicates, and
  `exists(property)`.
- `RelationshipProperties`: 1 scenario for relationship property filtering.
- `OptionalMatch`: 4 scenarios for null-preserving rows, null filtering,
  `coalesce(...)`, and connected multi-hop optional chains.
- `Aggregation`: 7 scenarios for grouped counts, `count(*)`, numeric
  aggregates, statistical aggregates, distinct statistical aggregates,
  `count(DISTINCT ...)`, and selected static-map aggregate targets.
- `With`: 6 scenarios for transparent scope filtering, non-terminal scalar
  aliases carried into a later `MATCH`, bare boolean alias filtering, aggregate
  filtering through terminal `WITH`, terminal `WITH DISTINCT` scalar projection,
  and terminal `WITH ORDER BY` / `SKIP` / `LIMIT` row modifiers.
- `CountSubquery`: 10 scenarios for `COUNT { ... }` projections, predicates,
  hidden ordering, scoped scalar `RETURN` validation, `RETURN DISTINCT scalar`
  row-counting, distinct-count threshold predicates, and reversed
  arbitrary-threshold count predicates.
- `CollectSubquery`: 2 scenarios for `COLLECT { MATCH ... RETURN scalar }`
  projection lists over correlated scoped patterns, including `RETURN DISTINCT`
  scalar collection.
- `ExistsSubquery`: 2 scenarios for `EXISTS { ... }` predicates, including
  row-preserving `RETURN DISTINCT` inside the scoped subquery.
- `ListExpressions`: 9 scenarios for static `range(...)`, `split(...)`,
  indexes, slices, endpoint functions, collection predicates, and list
  comprehensions, including folded math-function map expressions.
- `LiteralExpressions`: 2 scenarios for scalar literal projections and
  homogeneous literal-list projections.
- `MapExpressions`: 5 scenarios for static literal-map key extraction with
  `keys({ ... })`, static literal-map value lookup, compile-time selection of
  supported graph scalar/property map entries, including composition with
  projections, predicates, ordering, list endpoint functions, and `IN`
  membership, plus `properties(variable).field` and static string-index access
  over mapped graph properties.
- `ScalarExpressions`: 23 scenarios for searched `CASE`, scalar-string
  `isEmpty(...)` predicates and projections, string case conversion, trim
  variants, `replace(...)`, `substring(...)`, `size(...)`, `left(...)`,
  `right(...)`, `contains(...)`, `startsWith(...)`, `endsWith(...)`,
  including bare string predicate functions in `WHERE` / `CASE WHEN`,
  `reverse(...)`, numeric `abs(...)`, `ceil(...)`, `floor(...)`, `round(...)`,
  `sqrt(...)`, `sign(...)`, and adjacent rejection coverage.
- `NullSemantics`: 3 scenarios for literal-only null predicate folding,
  `coalesce(...)` / `nullIf(...)` null normalization, and unsafe literal-only
  null comparison rejection.
- `MathematicalFunctions`: 5 scenarios for `exp(...)`, `log(...)` /
  `ln(...)`, `log10(...)`, `pow(...)` / `power(...)`, `pi()`, `e()`,
  trigonometric functions, degree/radian conversion, `atan2(...)`,
  `haversin(...)`, and adjacent rejection coverage.
- `TypeConversion`: 3 scenarios for strict `toString(...)`,
  `toInteger(...)`, `toFloat(...)`, `toBoolean(...)`, nullable
  `toStringOrNull(...)`, `toIntegerOrNull(...)`, `toFloatOrNull(...)`,
  `toBooleanOrNull(...)` composition, and adjacent rejection coverage.
- `GraphMetadata`: 5 scenarios for `id(...)`, `elementId(...)`, `type(...)`,
  `startNode(...)`, `endNode(...)`, `labels(...)`, and `keys(...)`.
- `RowModifiers`: 2 scenarios for `ORDER BY`, `NULLS FIRST` / `NULLS LAST`,
  `SKIP`, and `LIMIT`.
- `Parameters`: 2 scenarios for scalar/list/limit parameter binding and missing
  parameter rejection through the public parameterized Cypher execution API.
- `Unwind`: 2 scenarios for `UNWIND` list expansion, including sliced static
  `CASE` list sources.
- `ReturnDistinct`: 1 scenario for `RETURN DISTINCT`.
- `Union`: 2 scenarios for `UNION` duplicate removal and `UNION ALL`
  duplicate preservation.
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

The GraphQL baseline currently contains 12 representative read-only scenarios:

- `RootSelection`: 2 scenarios for exact-label and generated root aliases.
- `ScalarFilters`: 2 scenarios for scalar operator objects, shorthand equality,
  list membership, and null checks.
- `BooleanFilters`: 1 scenario for `xor` and `not` composition.
- `RowModifiers`: 1 scenario for root ordering, offset, and limit.
- `Aggregation`: 1 scenario for grouped `_count`.
- `IdentityFields`: 1 scenario for `_id` and `_elementId`.
- `NestedRelationships`: 1 scenario for relationship traversal with endpoint
  and relationship-property filters.
- `RelationshipExistence`: 1 scenario for `EXISTS`-style relationship filters.
- `GeneratedClientShape`: 1 scenario for fragments and `__typename`.
- `ErrorHandling`: 1 expected rejection for unknown graph-declared properties.

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
