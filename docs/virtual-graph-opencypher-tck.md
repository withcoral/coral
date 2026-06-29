# Virtual Graph openCypher TCK Gate

Coral tracks Cypher compatibility with an openCypher TCK-style read baseline.
The gate is synthetic and read-only: each scenario runs through the same
production path as users, from Cypher parsing to virtual graph validation, SQL
lowering, DataFusion execution, and result comparison.

## Why This Exists

The openCypher TCK is the right external model for language conformance, but
Coral virtual graphs are not native mutable property graphs. Coral maps graph
labels and relationship types onto existing tables, so the conformance gate has
to translate graph fixtures into source manifests, graph declarations, and
expected tabular results.

The first gate is a curated baseline rather than a complete upstream TCK import.
It gives us a CI-backed contract for the read features we already claim and a
clear place to add translated scenarios as support expands.

## Current Gate

Run the gate locally with:

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

## Scope

The baseline currently contains 41 representative read-only scenarios:

- `Match`: 3 scenarios for labeled node scans plus forward and reverse
  relationship matches.
- `Where`: 9 scenarios for comparisons, boolean conjunction/disjunction,
  negation, `XOR`, list membership, metadata-list membership, string
  predicates, and regex predicates.
- `RelationshipProperties`: 1 scenario for relationship property filtering.
- `OptionalMatch`: 3 scenarios for null-preserving rows, null filtering, and
  `coalesce(...)`.
- `Aggregation`: 4 scenarios for grouped counts, `count(*)`, numeric
  aggregates, and `count(DISTINCT ...)`.
- `With`: 2 scenarios for transparent scope filtering and aggregate filtering
  through terminal `WITH`.
- `CountSubquery`: 2 scenarios for `COUNT { ... }` projections and predicates.
- `ExistsSubquery`: 1 scenario for `EXISTS { ... }` predicates.
- `ScalarExpressions`: 1 scenario for searched `CASE`.
- `GraphMetadata`: 4 scenarios for `id(...)`, `elementId(...)`, `type(...)`,
  `labels(...)`, and `keys(...)`.
- `RowModifiers`: 2 scenarios for `ORDER BY`, `NULLS FIRST` / `NULLS LAST`,
  `SKIP`, and `LIMIT`.
- `Unwind`: 1 scenario for `UNWIND` list expansion.
- `ReturnDistinct`: 1 scenario for `RETURN DISTINCT`.
- `Union`: 2 scenarios for `UNION` duplicate removal and `UNION ALL`
  duplicate preservation.
- `PathMetadata`: 1 scenario for `length(path)`.
- `VariableLengthPaths`: 3 scenarios for exact fixed-length relationship
  ranges, bounded ranges, and bounded GQL relationship quantifiers.
- `PathValues`: 1 expected rejection for materialized path returns.

Unsupported scenarios are not silently skipped. If a scenario is part of the
baseline, it must either produce the expected rows or the expected structured
error substring.

The fixture also declares `minimum_feature_counts`. The test runner verifies
that every scenario id is unique and that each feature bucket stays at or above
its declared floor, so coverage cannot silently shrink by moving or deleting a
scenario.

## Expansion Policy

When adding Cypher support:

1. Add or translate an openCypher-style scenario into the baseline fixture.
2. Keep the graph fixture synthetic and deterministic.
3. Prefer end-to-end result assertions over parser-only assertions.
4. Document intentionally unsupported read behavior in
   `docs/virtual-graph-compatibility.md`.
5. Only promote scenarios into the gate when Coral intentionally claims that
   behavior.

Future work should add an importer/reporting tool for the upstream openCypher
TCK so we can publish a scored matrix such as applicable, passing, unsupported
by product design, and known gap counts.
