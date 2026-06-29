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

CI runs the gate in the `Virtual Graph Core` workflow for pull requests and for
pushes to `main` or `prod`.

The baseline data lives at:

```text
crates/coral-engine/tests/fixtures/virtual_graph/opencypher_read_baseline.json
```

The runner lives at:

```text
crates/coral-engine/tests/engine/opencypher_tck_tests.rs
```

## Scope

The baseline currently covers representative read-only scenarios for:

- labeled node scans and property projections;
- directed relationship matches;
- `WHERE` comparisons, boolean conjunction, and list membership;
- `OPTIONAL MATCH` null-preserving rows;
- grouped `count(...)`;
- `RETURN DISTINCT`;
- path metadata through `length(path)`;
- exact fixed-length relationship ranges;
- transparent `WITH` scope filtering;
- `UNION ALL` duplicate preservation;
- explicit rejection of materialized path returns.

Unsupported scenarios are not silently skipped. If a scenario is part of the
baseline, it must either produce the expected rows or the expected structured
error substring.

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
