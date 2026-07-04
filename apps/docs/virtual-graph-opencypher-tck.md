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
647 scenarios, which is 40.06% of the full upstream scenario-definition inventory
and 50.00% of the read-candidate inventory.

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

Generate the schema-driven capability coverage report with:

```sh
make virtual-graph-graphql-schema-coverage
```

This report uses the generated GraphQL schema surface plus the engine's own
capability classifiers as the denominator. It reports headline canonical
capability coverage, secondary alias-spelling coverage, per-category counts
including `RejectionPaths`, and a tagged uncovered list.

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

The baseline currently contains 647 representative read-only scenarios:

- `Match`: 27 scenarios for labeled node scans, forward/reverse relationship
  matches, anonymous endpoints, inline property maps, bound-node reuse, grouped
  pattern counts, self-loop emptiness, and disconnected relationship products.
- `Where`: 30 scenarios for comparisons, post-`WITH` property and identity
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
- `Aggregation`: 29 scenarios for grouped counts, `count(*)`, numeric
  aggregates, empty-input/null-injected aggregate targets, optional null
  grouping, statistical aggregates, distinct statistical aggregates,
  `percentileCont(...)`, `percentileDisc(...)`, `count(DISTINCT ...)`, hidden
  aggregate ordering, and selected static-map aggregate targets.
- `With`: 45 scenarios for transparent scope filtering, scalar alias filters,
  `WITH *` filtering and carry-forward into later `MATCH`, variable dropping,
  node and relationship variable renaming, non-terminal scalar aliases carried
  into a later `MATCH`, interleaved multi-stage `WITH` pipelines, bare boolean
  alias filtering, aggregate filtering through terminal `WITH`, terminal
  `WITH DISTINCT` scalar and graph-variable projections, relationship-property
  alias filtering, terminal `WITH ORDER BY` / `SKIP` / `LIMIT` row modifiers
  with filtering, terminal `WITH *, expr AS alias RETURN *`, staged fixed
  multi-hop final `MATCH` chains after row-limited and aggregate `WITH`,
  staged final `OPTIONAL MATCH` after row-limited `WITH`, relationship-key
  carry into staged optional finals, and explicit rejection coverage for
  aggregate `WITH` chains that require staged planning.
- `CountSubquery`: 10 scenarios for `COUNT { ... }` projections, predicates,
  hidden ordering, scoped scalar `RETURN` validation, `RETURN DISTINCT scalar`
  row-counting, distinct-count threshold predicates, and reversed
  arbitrary-threshold count predicates.
- `CollectSubquery`: 3 scenarios for `COLLECT { MATCH ... RETURN scalar }`
  projection lists over correlated scoped patterns, including `RETURN DISTINCT`
  scalar collection and count-only `size(...)` / `isEmpty(...)` lowering over
  `COLLECT` subqueries.
- `PatternComprehension`: 15 scenarios for parser-recovered
  `[(pattern) WHERE predicate | scalar]` projection lists over correlated
  scoped relationship patterns, plus count-only `size(...)` / `isEmpty(...)`
  lowering over pattern comprehensions.
- `ExistsSubquery`: 13 scenarios for `EXISTS { ... }` predicates, including
  row-preserving `RETURN DISTINCT` inside the scoped subquery.
- `ListExpressions`: 40 scenarios for static `range(...)`, `split(...)`,
  indexes, slices, endpoint functions, collection predicates, and list
  comprehensions, including folded math-function map expressions and static
  `reduce(...)` folds, direct folded-list `ORDER BY` keys, compile-time
  dynamic label/type literal lists, folded string-list expressions, and
  statically selected list-valued `CASE`, plus legacy static `filter(...)` /
  `extract(...)` functions.
- `Quantifiers`: 12 scenarios for `all` / `any` / `none` / `single` list
  quantifier predicates over literal lists, empty ranges, boolean and string
  item predicates, metadata lists, optional static-list fallbacks, parameters,
  static list comprehensions, and dynamic collection rejection.
- `LiteralExpressions`: 15 scenarios for scalar literal projections and
  homogeneous literal-list projections.
- `MapExpressions`: 25 scenarios for static literal-map key extraction with
  `keys({ ... })`, static literal-map value lookup, compile-time selection of
  supported graph scalar/property map entries, including composition with
  projections, predicates, ordering, list endpoint functions, and `IN`
  membership, plus `properties(variable).field` and static string-index access
  over mapped graph properties.
- `ScalarExpressions`: 86 scenarios for searched and generic `CASE`, scalar-string
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
- `MathematicalFunctions`: 30 scenarios for arithmetic operators, unary
  negation, precedence, `sqrt(...)`, rounding aliases, `exp(...)`, `log(...)` /
  `ln(...)`, `log10(...)`, `pow(...)` / `power(...)`, `pi()`, `e()`,
  trigonometric functions, degree/radian conversion, `atan2(...)`,
  `haversin(...)`, `isNaN(...)`, and adjacent rejection coverage.
- `TypeConversion`: 16 scenarios for strict `toString(...)`,
  `toInteger(...)`, `toFloat(...)`, `toBoolean(...)`, nullable
  `toStringOrNull(...)`, `toIntegerOrNull(...)`, `toFloatOrNull(...)`,
  `toBooleanOrNull(...)` composition, and adjacent rejection coverage.
- `Temporal`: 86 scenarios for DATE, LOCALDATETIME, and LOCALTIME map and
  string constructor support, native temporal comparison operators,
  `toString(...)` over native temporal values, constructed temporal component
  access over native `date_part(...)` units, duration construction for temporal
  arithmetic, ISO-8601 duration rendering through bare duration returns and
  `toString(duration)`, DATE / LOCALDATETIME / LOCALTIME `+` and `-` duration
  arithmetic, calendar between/months and total day/second duration functions,
  and adjacent temporal support boundaries, including `date(...)`, `localdatetime(...)`,
  `localtime(...)`, zoned `datetime(...)` / `time(...)`, duration component
  access, same-kind temporal subtraction, non-literal duration multiplication,
  unsupported temporal map forms, deferred component names, kind-mismatched
  components, `duration.between(...)`, and `duration.inMonths(...)`.
- `GraphMetadata`: 19 scenarios for `id(...)`, `elementId(...)`, `type(...)`,
  `startNode(...)`, `endNode(...)`, `labels(...)`, and `keys(...)`.
- `RowModifiers`: 24 scenarios for `ORDER BY`, `NULLS FIRST` / `NULLS LAST`,
  `SKIP`, and `LIMIT`.
- `Parameters`: 14 scenarios for scalar/list/limit parameter binding,
  compile-time dynamic label-list parameters, and missing parameter rejection
  through the public parameterized Cypher execution API.
- `Unwind`: 13 scenarios for `UNWIND` list expansion, including range, split,
  list-parameter, concatenated-list, duplicate, distinct, empty-list,
  chained-unwind, filtered, matched, aggregated, sliced static `CASE`, and
  static expansion after transparent `WITH` sources.
- `ReturnDistinct`: 3 scenarios for `RETURN DISTINCT`, including computed
  expressions and grouped aggregate projections.
- `ReturnProjection`: 12 scenarios for `RETURN *`, aliased projections,
  duplicate expression returns, graph-variable projection expansion, and
  adjacent unsupported path-value rejection.
- `Union`: 17 scenarios for `UNION` duplicate removal and `UNION ALL`
  duplicate preservation, including computed branch projections.
- `PathMetadata`: 1 scenario for `length(path)`.
- `VariableLengthPaths`: 16 scenarios for exact fixed-length relationship
  ranges, bounded ranges, zero-hop bounded ranges, and bounded GQL
  relationship quantifiers.
- `PathValues`: 16 scenarios for fixed-path element id-list projections, static
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

The GraphQL baseline currently contains 154 representative read-only scenarios:

- `RootSelection`: 4 scenarios for exact-label and generated singular/plural
  root aliases.
- `ScalarFilters`: 18 scenarios for scalar operator objects, shorthand equality,
  list membership, int/float coercion boundary filtering, null checks,
  empty-result filters, negated string predicates including `notMatches`, and
  regex `matches`.
- `BooleanFilters`: 8 scenarios for `and`/`or` arrays, nested and/or, deep
  three-level boolean composition, nested `xor` inside `and`, `not`
  composition, `xor`, and uppercase operator aliases.
- `RowModifiers`: 17 scenarios for root ordering, multi-key ordering, offset,
  limit, `first`/`skip` row-modifier aliases, combined skip/first windows,
  pagination edge windows including offset-beyond-count, limit `0`, and
  last-window overshoot, `ASCENDING`/`DESCENDING` and shorthand `orderBy`
  directions, explicit null ordering with `nulls: FIRST` / `nulls: LAST`,
  distinct projection, and distinct projection combined with ordering.
- `Aggregation`: 20 scenarios for grouped `_count`, numeric property
  aggregates including `_median`, DISTINCT aggregate variants, sample and
  population standard deviation with `_stDev` / `_stDevP`, distinct counts, and
  exact `_percentileCont(field:, percentile:)`, plus single-row `_collect`.
- `Temporal`: 5 scenarios for stored `Timestamp` and `Date` scalar selection,
  `orderBy`, and `_min` / `_max` aggregation.
- `IdentityFields`: 32 scenarios for `_id` / `_elementId` selection, `_id`
  equality, range, inequality, list, negated-list, and null filters, `_id` list
  filtering combined with identity ordering, `_id` ordering, and `_elementId`
  equality, range, inequality, string, negated-string, list, negated-list, null
  filters, and ordering.
- `NestedRelationships`: 15 scenarios for out/in/any relationship traversal with
  endpoint predicates, endpoint and `_edge` projections, in/any strength
  relationship-property filters, combined float relationship-property
  predicates, 2-hop out/out and out/in traversals, and empty outgoing
  traversals that flatten to no rows.
- `RelationshipExistence`: 8 scenarios for `EXISTS`-style out/in/any
  relationship filters, endpoint and relationship-property predicates, and
  boolean-composed existence with scalar filters, including negated incoming
  existence.
- `GeneratedClientShape`: 9 scenarios for single, chained, and inline
  fragments, root and traversal `__typename` metadata including `_edge`
  relationship types, and `@skip`/`@include` directives.
- `ErrorHandling`: 18 expected rejections for unknown graph-declared properties,
  `_id` string predicates, aggregate argument misuse, temporal numeric
  aggregate and string-filter boundaries, and multiple included root node
  fields.

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
