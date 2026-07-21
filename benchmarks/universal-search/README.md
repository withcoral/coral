# Universal Search benchmark

This directory documents the Universal Search relevance workflow. Real catalog
snapshots, generated questions, collected queries, responses, focused corpora,
and replay results belong under `runs/`, which Git ignores. They can contain
private catalog names and descriptions and must not be committed.

The workflow has two boundaries:

1. `prepare`, `generate`, and `collect` create a candidate corpus. Collection
   gives each fresh Codex process only the question, captures its single Coral
   `search` call, and freezes the chosen query.
2. `replay` and `report` run those frozen queries against a selected Coral
   binary. Use replay while tuning ranking weights; do not regenerate queries
   for each candidate.

Build the binaries first:

```shell
cargo build --locked --release -p coral-cli -p coral-benchmarks
```

Create one run directory and prepare 100 schema-balanced samples:

```shell
cargo run --locked -p xtask -- benchmark universal-search prepare \
  --dir benchmarks/universal-search/runs/initial \
  --coral-bin target/release/coral \
  --workspace default \
  --count 100 \
  --seed 1729
```

Generate three questions per sample with ten fresh generator processes:

```shell
cargo run --locked -p xtask -- benchmark universal-search generate \
  --dir benchmarks/universal-search/runs/initial \
  --model MODEL \
  --jobs 10
```

Collect one fresh Coral search call per question, then replay and report:

```shell
cargo run --locked -p xtask -- benchmark universal-search collect \
  --dir benchmarks/universal-search/runs/initial \
  --coral-bin target/release/coral \
  --workspace default \
  --model MODEL \
  --jobs 10

# If some agents time out or violate the one-search protocol, preserve the
# successful trials and rerun only failed or missing cases:
cargo run --locked -p xtask -- benchmark universal-search collect \
  --dir benchmarks/universal-search/runs/initial \
  --coral-bin target/release/coral \
  --workspace default \
  --model MODEL \
  --jobs 10 \
  --retry-failed

cargo run --locked -p xtask -- benchmark universal-search replay \
  --dir benchmarks/universal-search/runs/initial \
  --label baseline \
  --coral-bin target/release/coral \
  --workspace default \
  --jobs 10

cargo run --locked -p xtask -- benchmark universal-search report \
  --dir benchmarks/universal-search/runs/initial \
  --label baseline
```

After replaying the same corpus with a new `--label candidate`, add
`--baseline-label` for a paired comparison:

```shell
cargo run --locked -p xtask -- benchmark universal-search report \
  --dir benchmarks/universal-search/runs/initial \
  --label candidate \
  --baseline-label baseline
```

To tune against a smaller set, copy reviewed cases into a focused corpus inside
the same ignored run directory. This keeps the questions, queries, and catalog
targets out of Git:

```shell
cargo run --locked -p xtask -- benchmark universal-search replay \
  --dir benchmarks/universal-search/runs/initial \
  --corpus benchmarks/universal-search/runs/initial/focused-regressions.jsonl \
  --label baseline-search-regressions \
  --coral-bin target/release/coral \
  --workspace default \
  --jobs 6

cargo run --locked -p xtask -- benchmark universal-search report \
  --dir benchmarks/universal-search/runs/initial \
  --label baseline-search-regressions
```

The focused outputs are `replay-baseline-search-regressions.jsonl` and
`summary-baseline-search-regressions.{json,md}`. Replay labels are immutable;
choose a new label for every candidate build.

Inspect `collected-corpus.jsonl` for ambiguous questions, private custom-source
metadata, and incorrect targets. Keep the 300-case corpus and focused
regressions inside the ignored run directory. They are development data. Use a
separate, sealed benchmark for final evaluation.

Replay records separate the real `limit = 10` result from the diagnostic
`limit = 50` result. A target missing at 50 is marked as censored when Coral
reports truncation or provider `has_more`; it is not proof that no lower-ranked
match exists. Reports show parent rank and target rank separately. Parent rank
measures whether Coral returned the owning table or table function. Target rank
measures whether the requested object is usable: parent targets match that same
surface, while child targets require the exact field in its role-specific parent
section (or the equivalent legacy standalone field result). Alternatives do not
affect scoring. Reports break results down by schema and parent/child target
class, show equal-weight macro summaries, and can compare two replays case by
case with `--baseline-label`. Reports also count the
compact JSON response with the `o200k_base` tokenizer so relevance changes can
be weighed against the context consumed by search results.
