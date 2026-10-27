---
status: accepted
---

# Split Universal Search into an engine and one retriever per Search Entity kind

Universal Search has one engine and one retriever per Search Entity kind: Operations and Saved Functions. The engine
normalizes the query, asks every retriever, fuses their ranked lists by position and pages the result; it never
branches on kind. A retriever owns everything about its kind: applying its owners' visibility rules, matching, ranking,
evidence, and exact resolution for `describe`. Only positions and evidence cross into the engine, because raw scores are
assumed incomparable across retrievers. Search applies visibility but decides none; those rules stay with the owning
domains.

Before the split, one module ranked lanes of material across every kind, and each new kind or kind-specific rule added
a branch to every loop. Rejected: per-kind branches in the engine; a lanes-only trait that keeps ranking in the engine
and stops a kind from ranking its own way; and one ranking per lane across kinds, which assumes scores stay comparable
once each kind ranks its own way.

Fusion weights retrievers equally and puts a Saved Function before an Operation at equal rank: people wrote Saved
Functions for this Workspace, and no model has them memorised.

## Search owns its index

Search entries live in a table that search owns, not on the Generation row. After a Provider Generation or a Saved
Function commits, the committing workflow tells search, which prepares and writes the entries; a periodic reconcile
converges whatever a lost notification missed. Ranking runs in the database on both backends, Postgres full-text search
and SQLite FTS5, with the same field weights. Indexing is therefore not atomic with publication: until the entries land,
that Provider is missing from relevance and the page reports partial results. ADR 0017 records why that gap is
accepted.
