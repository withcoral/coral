---
status: accepted
---

# Split Universal Search into an engine and one retriever per Search Entity kind

Universal Search has one engine and one retriever per Search Entity kind: Tools and Saved Functions. The engine
normalizes the query, asks every retriever, fuses their ranked lists by position and pages the result; it never
branches on kind. A retriever owns everything about its kind: applying its owners' visibility rules, matching, ranking,
evidence, and exact resolution for exact Call Path lookups. Only positions, evidence and whether each entry is a strong
match cross into the engine; a strong match is an entry that its retriever judges to answer the query by where the
query's words meet it, not only by sharing a word somewhere in its text. Raw scores are assumed incomparable across
retrievers, so each retriever turns its own scores into that one judgment and the engine compares no score. Search
applies visibility but decides none; those rules stay with the owning domains.

Before the split, one module ranked lanes of material across every kind, and each new kind or kind-specific rule added
a branch to every loop. Rejected: per-kind branches in the engine; a lanes-only trait that keeps ranking in the engine
and stops a kind from ranking its own way; and one ranking per lane across kinds, which assumes scores stay comparable
once each kind ranks its own way.

Fusion weights retrievers equally. It puts every strong match before every weak one, each group by position, so a weak
match is demoted, never dropped. Each retriever decides strong or weak by its own rule over its own signals, such as the
query's words in order in a field, a query word that starts a word of its name, or a Provider the query names. A kind
can therefore judge by fields another kind lacks, and each rule can be tested alone; the cost is that the rules can
disagree about how good a match must be, and only measuring them on one labelled set keeps them in step. The rules are
in [#2792](https://github.com/withcoral/coral/pull/2792).

At equal position a strong Tool comes before a strong Saved Function. A Saved Function used to win every tie, because
people wrote Saved Functions for this Workspace and no model has them memorised. But on a labelled replay of real
searches, with that tie and weak matches demoted, an unrelated Saved Function that shared one generic word with the
query took rank 1 on about a third of searches; with Tools first, on about 2%. A Saved Function that the query targets
usually moves from first to second, still in the top three.

Rejected: one kind-neutral match strength that every kind computes and the engine compares. It drifts, because the
kinds hold different fields: a Saved Function has no title or Provider name, so sorting by a shared score pushed
targeted Saved Functions out of the top three on about two thirds of queries.

## Search owns its index

Search entries live in a table that search owns, not on the Generation row. After a Provider Generation or a Saved
Function commits, the committing workflow tells search, which prepares and writes the entries; a periodic reconcile
converges whatever a lost notification missed. Ranking runs in the database on both backends, Postgres full-text search
and SQLite FTS5, with the same field weights. Indexing is therefore not atomic with publication: until the entries land,
that Provider is missing from relevance and the page reports partial results. ADR 0017 records why that gap is
accepted.
