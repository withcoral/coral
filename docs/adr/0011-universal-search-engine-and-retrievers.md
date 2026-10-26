---
status: accepted
---

# Split Universal Search into an engine and one retriever per Search Entity kind

Universal Search in `crates/coral-search` has one engine and one retriever per Search Entity kind: Operations and
Saved Functions. The engine normalizes the query once, asks every retriever, fuses their ranked lists by position,
pages, and encodes cursors. It never branches on kind. A retriever owns everything about its kind: applying the owners' visibility rules, matching, its own
ranking over its own indexed entries, evidence, hydration into hits, and exact resolution. The rules themselves stay
with coral-workspace, coral-provider, coral-policy, and coral-functions; search stores no visibility and decides none. Only positions and evidence cross from a retriever into the engine; raw scores stay inside and are
assumed incomparable across retrievers. `describe` is a separate exact path that asks the retrievers in turn and never
touches relevance.

Universal Search also owns its index end to end. The workflow that commits a Provider Generation or a Saved Function notifies Universal
Search afterward with the entries the owner prepared, the Task command for Task-attributed work; an `EntityIndexer` per kind, the write-side sibling of `EntityRetriever` over the
same identity, stores them in one table that search owns; a periodic reconcile task converges what a lost notification missed.
Publication and save keep deciding what may be searched. Retrieval and ranking run inside the database on both backends, Postgres full-text search with an
approximate BM25 in SQL and SQLite FTS5, with the same field weights; the app loads only a window per retriever
and computes evidence over it. ADR 0017 replaces the sentence on the entries the owner prepared: the decision is
that search prepares its entries from the stored Generation after the commit (#2463 carries it out), and no search
bound changes an Operation's availability.

## Why a seam per kind, not branches

Before the split, the module loaded two lanes of material for every visible Provider on every query, ranked each lane
across all kinds, and fused the lanes by weighted reciprocal rank; Saved Functions were a second branch inside the same
loops. Every new kind, and every kind-specific rule about visibility or evidence, would have added another branch to
each loop. Behind the retriever's four methods, everything a kind knows lives in one place. Rejected:

- **By-hand branches per kind** in the engine: the shape we had, with the cost above.
- **A lanes-only trait**, one supplier of metadata and declaration lanes per kind with the engine still ranking: it
  keeps kind knowledge in the engine and forbids a kind from ranking its own way, for example inside the database later.
- **A two-phase retriever with a per-request prepared object**: an extra type per kind that carried resolved
  visibility from a prepare call into a query call. The same resolution now happens once inside `query`, and a
  retriever may hold its own resources between calls.
- **One ranking per lane across kinds**, the previous model: it assumes lane scores are comparable across kinds, which
  stops being true once each kind ranks its own way.

## Why the engine fuses by rank, and what that changes

Each retriever returns one ranked list. The engine fuses by position with equal weights, a Saved Function before an
Operation at equal rank, then Call Path; Saved Functions were written by people for this Workspace and no model has
them memorised. On a query where both retrievers answer, results come out in a different order than before. That is
deliberate. The guardrail is the mixed-query test in `crates/coral-search`. Two further order changes ride
along: declaration paths rank inside the Operation's entry at the lowest field preference instead of in a separate
lane, and the deprecation flag and the Provider's access mode left searchable text.

## Why search owns its index

The two JSON lane columns on the Generation row put search material under coral-catalog's schema and made every query
re-read and decode it. Rejected: **leaving index storage on the Generation row** and putting an indexer trait over it.
Instead the committing workflow tells search after its commit which Generation became active, search prepares the
entries from the stored Generation ([ADR 0017](0017-universal-search-never-gates-the-catalog.md)) and writes them into its
own table, where the Provider's active-Generation pointer decides which rows are live with no flag of their own.
Indexing is no longer atomic with the Generation: between the pointer moving and the rows landing, that Provider is
absent from relevance and the page reports partial. A Saved Function's row carries a cascading foreign key to the
function record, so its removal needs no notification.
