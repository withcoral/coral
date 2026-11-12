---
status: accepted
---

# Split Universal Search into an engine and one retriever per Search Entity kind

Universal Search has one engine and one retriever per Search Entity kind: Provider Tools, Private Tools and Shared Tools
([ADR 0034](0034-every-callable-is-a-tool.md)). The engine normalizes the query, asks every retriever, fuses their
ranked lists by position and pages the result; it never branches on kind. A retriever owns everything about its kind:
applying its owners' visibility rules, matching, ranking, merging its lexical and vector lists
([ADR 0035](0035-universal-search-adds-a-vector-list-from-a-bundled-cpu-model.md)), and exact resolution for exact Call
Path lookups. Only positions and whether each entry is a strong match cross into the engine; Search Hits carry no
evidence. A strong match is an entry that its retriever judges to answer the query, not only to share a word somewhere
in its text. Raw scores are assumed incomparable across retrievers, so each retriever turns its own scores into that one
judgment and the engine compares no score. Search applies visibility but decides none; those rules stay with the owning
domains.

Before the split, one module ranked lanes of material across every kind, and each new kind or kind-specific rule added
a branch to every loop. Rejected: per-kind branches in the engine; a lanes-only trait that keeps ranking in the engine
and stops a kind from ranking its own way; and one ranking per lane across kinds, which assumes scores stay comparable
once each kind ranks its own way.

Fusion weights retrievers equally. It puts every strong match before every weak one, each group by position, so a weak
match is demoted, never dropped. Each retriever decides strong or weak by its own rule over its own signals, such as the
query's words in order in a field, a query word that starts a word of its name, a Provider the query names, or a place
near the top of its vector list. A kind can therefore judge by fields another kind lacks, and each rule can be tested
alone; the cost is that the rules can disagree about how good a match must be, and only measuring them on one labelled
set keeps them in step. The rules are in [#2792](https://github.com/withcoral/coral/pull/2792).

At equal position a strong Provider Tool comes before a strong Private Tool, and a Private Tool before a Shared Tool. A
Private Tool used to win every tie, because people wrote Private Tools for this Workspace and no model has them
memorised. But on a labelled replay of real searches, with that tie and weak matches demoted, an unrelated Private Tool
that shared one generic word with the query took rank 1 on about a third of searches; with Provider Tools first, on
about 2%. A Private Tool that the query targets usually moves from first to second, still in the top three. No replay
measured where Shared Tools go.

Rejected: one kind-neutral match strength that every kind computes and the engine compares. It drifts, because the kinds
hold different fields: a Private Tool has no title or Provider name, so sorting by a shared score pushed targeted
Private Tools out of the top three on about two thirds of queries.

## Search owns its index

Search entries live in a table that search owns, not on the Generation row. After a Provider Generation commits, a
Private Tool is saved or a Shared Tool is published, the committing workflow tells search, which prepares and writes the
entries; a periodic reconcile converges whatever a lost notification missed. Ranking runs in the database on both
backends, Postgres full-text search and SQLite FTS5, with the same field weights. Indexing is therefore not atomic with
publication: until the entries land, that Provider is missing from relevance and the page reports partial results.
ADR 0017 records why that gap is accepted.
