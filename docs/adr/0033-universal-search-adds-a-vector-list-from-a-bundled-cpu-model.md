---
status: accepted
---

# Universal Search adds a vector list from a bundled CPU model

Each kind's retriever runs a vector query beside its lexical query, merges the two lists and marks each entry strong or
weak; the engine's fusion does not change. One embedding model, `BAAI/bge-small-en-v1.5`, runs on the CPU inside the
Coral process on Desktop and on the server. A background Embedder gives each Search Entity a vector of its text, and an
Entity without a current vector ranks by words alone. Search Hits carry no evidence. One setting turns the vector side
off.

The kinds are those of [#2967](https://github.com/withcoral/coral/issues/2967): Provider Tools, Private Tools (which
replace Saved Functions) and Shared Tools, found through `coral.search`.

Universal Search ranked by words only, so a query that names a Tool by what it does, not by the words in its
description, missed it. On the labelled bench, hit@10 goes from 0.668 (words alone) to 0.753 (fused) on the Microsoft
Graph run (Set B, Postgres, 2,385 queries over 19,830 rows), from 0.737 to 0.844 on Set A over SQLite and from 0.761 to
0.852 on Set A over Postgres (998 queries), and no Provider falls below its lexical score at 10. The numbers and the paired tests are in
[the pick](https://github.com/withcoral/coral/issues/2865#issuecomment-6740646173); the design and its measurements are
on the map [#2856](https://github.com/withcoral/coral/issues/2856).

## Three limits set first

[#2845](https://github.com/withcoral/coral/issues/2845) fixed three things before any measurement:

- **No embedding API.** An API model ranks higher (OpenAI `text-embedding-3-large` scored 0.884 vector-only on Set A in
  the first test, against 0.826 for bge-small), but it sends catalog text out of the deployment, needs a key and network
  on every Desktop, and costs money per row. Deferred, not refused.
- **One CPU model inside the Coral process.** No sidecar, no separate inference server (TEI, Ollama), no GPU. A second
  process is one more thing to ship, supervise and update on three Desktop platforms and in the server image. The cost:
  the model's CPU and memory land in the Coral process, and a busy server can slow down. If that happens, inference
  moves out of the process; that is a new decision.
- **Measure first.** An offline harness (`tools/universal-search/prototype-semantic/` in coral-benchmarks) measured 24
  model files across dimensions, index texts, query texts and merge rules on both sets, on an M4 Pro and on one
  Skylake vCPU without VNNI, before any product code.

## The vector list

The vector query carries the lexical query's `WHERE`: the caller's discoverable Providers, complete Generations, policy
exclusions and the Provider filter. Both queries return the window depth (100) from the same read snapshot. The
retriever merges them by reciprocal rank, k=60, with weight 1:1, and cuts the result to the window. An entry is strong
when the retriever's own strong rule holds for it, wherever it came from, or when it is in the top 5 of the vector list.
The engine's strong-first fusion across kinds ([ADR 0011](0011-universal-search-engine-and-retrievers.md)) does not
change.

- **Top 5 is a constant.** It does not follow the request's limit, so the order is the same for every page size and
  offset pages ([ADR 0028](0028-search-pages-by-a-plain-offset.md)) do not repeat or skip rows. Vector hits that are
  never strong lose 2.1 to 3.8 points on Set B. The best similarity threshold was 0.2 to 2.4 points lower, its
  value moved between sets and models, and a threshold relative to the top hit marked all 50 vector hits strong
  on queries with a flat top.
- **Ties break by a stable key**, in the vector query and in the merge (for example `ORDER BY distance, tool_key`).
  Without it, a page can repeat or skip a row when the index has not changed.
- **Query text: the frozen query alone**, with the model's query prefix, embedded once per request by the engine and
  given to every retriever. The Task's goal added at most 0.8 points, none of it significant, and the bench's stand-in
  for it (the writer's full question, 102 characters at the median) is longer than a real Task intent (80 at most).
- **Index text.** A Provider Tool: Call Path, title, description and parameter names. A Private or Shared Tool: Call
  Path, description and input names, by analogy; no bench case targets one yet. Parameter names win on Set B; the signature line costs twice as much to embed for no gain there.

Rejected: weight 1:2 for the vector list (up to 1.4 points lower, and Notion falls below lexical); a strong count tied to
the limit; a merge in the engine across kinds, which would change ADR 0011's fusion for no measured gain.

## Search Hits lose their evidence

`matched_tags`, `matched_inputs`, the Guidance Snippet and the rule that dropped a hit with no matched field go. The
Description Snippet starts at the beginning of the description. A hit from the vector list has no matched word to show,
and the drop rule would remove it. This amends ADR 0011, where evidence crossed into the engine.

[The prototype](https://github.com/withcoral/coral/issues/2864#issuecomment-6685151365) measured no cost: the same hits
on every Set A page and pages 2.2% smaller; in duels the turns, tokens and cost per solve moved less than 9% with no
significant sign test. Reef reads the old fields as optional and keeps doing so, because retained Task history pages
still hold them.

## The model

`BAAI/bge-small-en-v1.5` (MIT, 33.4M parameters), the author's fp32 ONNX file, 384 dimensions, on Desktop (macOS arm64,
Windows x64, Linux x64) and in the server image. bge-base and jina-embeddings-v2-base-code score the same on every set
(p ≥ 0.13) at four times the cost per query and per row, and jina-code puts Notion below lexical. Qwen3-Embedding-0.6B
gains 2.1 points on Set B (p 0.0012), but on one Skylake vCPU a query takes 347 ms and the model needs 3.5 GB. Desktop
picks are judged on that vCPU as the x86 floor, not only on Apple silicon. One model for both targets means one
manifest, one delivery path and one set of bench results.

**Late interaction.** A late-interaction model (one vector per token, scored by MaxSim) earns its runtime only if it
beats the best single-vector model on Set B by at least 2 points of hit@10 with p < 0.01, and fits the target's latency
floor. LateOn, the best measured, beat Qwen3 by 0.8 points (p 0.20) and costs 188 ms a query on the x86 floor. Taking
one later changes the storage (a per-token blob per row, 14 to 34 times one 384-d vector; neither sqlite-vec nor
pgvector holds it as a vector), adds a MaxSim scorer in Rust over the fused top results, and removes the KNN query.

**Runtime.** [`ort`](https://github.com/withcoral/coral/issues/2867#issuecomment-6742949196), statically linked with
ONNX Runtime 1.31.0, in its own crate, `coral-embed`. Every session sets `AutoDevicePolicy::PreferCPU`: `ort`'s default,
`MaxEfficiency`, puts part of the graph on CoreML on macOS, four times slower with about seven times the memory. The
Rust vectors match the harness to 4.2e-7 with the same hit@10. A query embeds in about 3 ms on one M4 core and 12 ms on
one x86 vCPU; an embedding process peaks at 320 to 360 MiB; the `coral` binary grows by 29.6 MB on macOS and 32.9 MB on
Linux. Windows size and speed are not measured yet. Below x86-64-v3 the vector side stays off. The prebuilt runtime's
Windows 10 1903 floor is accepted.

**Manifest.** The model is a manifest compiled into the binary: model id, file names, download URL, SHA-256, size,
dimension, maximum tokens, query and document prefixes, index text, query text, merge weight and strong count. There is
no operator override. The Embedder revision is a digest of the fields that change stored vectors, separate from the
indexer revision, so a model change re-embeds every Entity without re-indexing words.

## The Embedder

The Embedder is the third supervised task beside Observer and Reconcile. It owns one OS thread at low priority with
one intra-op thread. It wakes on every index write, on the reconcile tick and at boot, and drains a predicate, not a
queue table: a vector is stale when it is missing, when its revision differs, or when its source stamp differs from the
row's `indexed_at`. It takes the oldest Generation first, in batches of about 32 rows, with one write per batch under
the Workspace's index lock that checks staleness again. A failed batch logs, backs off and retries; there is no per-row
failure state. Saving a Private Tool or publishing a Shared Tool never calls the model. Query embedding runs on a blocking thread at normal
priority.

A Graph-sized Provider (19,830 rows) becomes searchable by meaning in about 11.5 minutes on one x86 vCPU and 2.9 minutes
on one M4 core; until then it ranks by words. Two Coral Instances that overlap during a rolling update can embed the
same rows; the write checks staleness, so this costs CPU only.

## Storage

One vector table per backend, keyed like the entity table, with the revision and source stamp beside each vector. Coral
creates it at boot from the manifest, before the Embedder starts, and drops and recreates it when the dimension changes. It lives outside
the migration chain, the one exception to that rule: a migration with a vector column fails on a Postgres without
pgvector, and Coral with the vector side off must start there.

- **Postgres:** `halfvec(384)`, with the filter columns (kind, Provider, Generation, Tool key) on the vector table, so
  the vector query reads it alone and scans it exactly. At the sandbox RDS shape the
  [scan](https://github.com/withcoral/coral/issues/2948#issuecomment-6689216657) costs about 1.55 µs a row: a Graph
  Workspace takes 31 to 37 ms, and p50 passes 50 ms at about 32,000 rows per Workspace. Joined to the entity table, a
  Provider filter made the planner sort by distance and nested-loop (p99 about 630 ms at 10 Workspaces), and the plan
  slowed as other Workspaces arrived. The query binds its vector as `halfvec`; a `vector` cast in SQL converts again for
  every row. `halfvec(384)` keeps 26 MB per Graph Workspace against 44 MB for `vector(384)`.
- **pgvector is required on Postgres** while the vector side is on. The storage check creates the extension where its
  role may (the RDS master user may, as for `pg_trgm`) and otherwise refuses to start with a one-line remedy. With the
  setting off it checks nothing.
- **SQLite:** a sqlite-vec `vec0` table of `float[384]` (sqlite-vec has no fp16 type), version pinned, registered for
  the whole process before coral-db opens its first pool. The vector query takes the eligibility SQL through `rowid IN
  (subquery)`. sqlite-vec is best effort: if `EXPLAIN QUERY PLAN` shows the filter does not reach `vec0`, SQLite stays
  lexical, with no BLOB scan or other workaround.
- **No foreign keys**, not even to `workspace_records`. A `vec0` table cannot hold one, and a cascade from a source
  makes a delete wait for the index ([#2684](https://github.com/withcoral/coral/issues/2684)). The Reconcile task removes
  vectors whose Search Entity or Workspace is gone. Retrieval reads only live sources
  ([ADR 0017](0017-universal-search-never-gates-the-catalog.md)), so an orphan never reaches a page. This amends ADR
  0017's rule that each search table keeps its Workspace key.

Rejected for now: HNSW. It makes a large Workspace fast (about 4 ms) but loses 3 to 6% recall, needs an iterative scan,
`ef_search` of 200 or more and a custom plan per Workspace, and its index needs memory too.

## Delivery

One find-or-fetch path in the Coral process; Electron takes no part. The process looks for the manifest's files under a
read-only system path first (the server image puts them there), then under `models/<model id>/` in its state directory.
If they are absent, it downloads them from a model release in `withcoral/coral-releases` through the outbound client, to
a temporary name renamed after the SHA-256 matches, and retries with backoff while the process runs. GitHub Releases is
the one download host Coral already trusts; no Coral bucket exists. Search is lexical until the files land. Folders the
manifest no longer names are deleted after the new model lands. A host without network access gets the files at the
system path, or turns the vector side off.

## The setting and failures

`[search] semantic_search_enabled`, one process-wide boolean read at boot, default true. False means no query
embedding, no Embedder thread, no download and no extension check.

A failure on the vector side at query time (model absent, session error, vector table missing during a rolling update,
vector query error) gives that retriever its lexical list alone. It never puts the retriever in the `Error` state, and
the page says nothing. Telemetry says it ([#2855](https://github.com/withcoral/coral/issues/2855)): the retriever's
span records `fused` or `lexical_only` with a reason (`disabled`, `model_absent`, `model_failed`, `cpu_floor`,
`embed_failed`), a `coral.search.embed_query` span times the query embedding, and the Embedder reports its backlog and
batch durations.

## Consequences

- Search Hits change shape. Programs that read `matched_tags`, `matched_inputs` or the Guidance Snippet get nothing.
- A new or changed Provider ranks by words for minutes. On a server, Workspaces share the one Embedder thread: ten
  Graph-sized Providers take about two hours.
- Vectors cost database memory. On db.t4g.medium (4 GiB) the vectors of about 17 Graph Workspaces stay warm at
  `halfvec(384)` (extrapolated from 10 measured); past that the instance swaps and a query takes 0.8 to 2 s.
- The Coral process carries the model: about 30 MB of binary, a 133 MB model file, and up to about 360 MiB while it
  embeds.
- Self-hosted operators on Postgres install pgvector or turn the setting off.
- A model change is a code change: a new manifest, a new revision, and every Entity re-embeds. A new dimension recreates
  the vector table.
- Not decided here: how Workspaces share the Embedder on a busy server, a cap on embedded Workspaces or a larger
  database class, HNSW past a row count, tuning the merge weight on real queries, query text beyond the frozen query
  (the Task intent, the search `intent` argument), and bench cases for Private and Shared Tools.
