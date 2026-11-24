---
status: accepted
---

# A rerank step decides by distance whether an Authored Tool is strong

Universal Search has a rerank step between the retrievers and fusion. It sets the final mark of each Authored Tool, a
Private Tool or a Shared Tool. An Authored Tool is strong when one of these rules holds:

1. **Phrase:** a field holds the query's words in order.
2. **Rare path word:** a content word of the query starts a word of the Tool's path, and under 10% of the Workspace's
   Provider Tool paths hold a word that starts with it
   ([#3426](https://github.com/withcoral/coral/issues/3426)).
3. **Distance:** the Tool is strictly nearer to the query than the nearest Provider Tool of the same request. The
   Provider Tool window holds only the Tools inside the search's Provider filter, so the comparison is inside it too.

The Authored Tool retriever applies rules 1 and 2, and the rerank step applies rule 3. An Authored Tool that no rule
makes strong gets a third mark, **last**: fusion puts it after every weak entry. Fusion orders strong, weak and last
entries, each group by position and then by retriever order. A Provider Tool keeps the rules of
[ADR 0011](0011-universal-search-engine-and-retrievers.md) and
[ADR 0035](0035-universal-search-adds-a-vector-list-from-a-bundled-cpu-model.md), the first 5 of its vector list
included.

## Why the fixed count fails for Authored Tools

ADR 0035 made the first 5 entries of each kind's vector list strong, and read no distance. A vector list has nearest
entries for every query. So with semantic search on, 5 Private Tools and the nearest Shared Tools were strong on every
search, whatever the query asked. In one team Workspace with real use, Private and Shared Tools became half of all hits,
and the called Provider Tool fell out of the first 3 places of the page: 93.2% before, 87.0% after
([#3412](https://github.com/withcoral/coral/issues/3412)). The one Shared Tool of that Workspace was on 97.6% of the
pages. For Provider Tools, ADR 0035 measured that the first 5 gain 2.1 to 3.8 points. The Authored Tools of one caller
are few (up to 38 for one principal in that Workspace), and their first 5 are only the least far of them.

Removing the strong mark alone does not end the crowding. Fusion merges by position, so a weak Authored Tool at
position 1 still comes before a weak Provider Tool at position 2. Hence the third mark.

Rule 3 compares two distances from one query vector and one model, so it needs no tuned limit. A label test on the same
Workspace (#3412) measured each rule: how often it marks one of the caller's Authored Tools strong on 698 real searches
after which the agent called a Provider Tool (false alarm), and how often it marks the target strong for 81 Authored
Tools with three written queries each.

| Strong rule for an Authored Tool                       | False alarm | Hit, 3 words | Hit, sentence | Hit, no shared word |
| ------------------------------------------------------ | ----------: | -----------: | ------------: | ------------------: |
| First 5 of its own vector list (before)                |      100.0% |       100.0% |        100.0% |               84.0% |
| Rare path word                                         |        9.5% |        90.1% |         95.1% |                0.0% |
| Nearer than the nearest Provider Tool                  |        1.3% |        67.9% |         82.7% |               11.1% |
| Rare path word, or nearer than the nearest Provider Tool |      9.6% |        93.8% |         97.5% |               11.1% |

The bench of [coral-benchmarks#698](https://github.com/withcoral/coral-benchmarks/issues/698) measured the change on
Set A and Set B with stubs of the same Workspace's Authored Tools; the scoreboard is on #3412.

<!-- BENCH -->

## What changes in ADR 0011 and ADR 0035

- **A distance crosses into the engine.** ADR 0011 lets only a position and a mark cross. A window entry now also
  carries its distance from the query when its retriever's vector list holds it. Both stores compute the cosine
  distance from one query vector, so two kinds' distances compare. Only the rerank step reads them. Scores, ranks and
  merge scores still never compare across kinds.
- **One place in the engine reads the kind.** ADR 0011 says the engine never branches on kind. The rerank step reads
  the kind of an entry directly, and it is the only place in the engine that does. Rejected: a role that each
  retriever gives its entries, which adds a concept for one exception.
- **Fusion does not weigh Authored Tools equally.** ADR 0011 weights retrievers equally. A last Authored Tool now
  follows every weak Provider Tool. Strong entries of every kind still merge by position, and at equal position the
  retriever order of ADR 0011 still decides.
- **The fixed count is for Provider Tools only.** ADR 0035 makes an entry strong when it is among the first 5 of its
  vector list. That rule no longer applies to Authored Tools. The vector list still adds Authored Tools to the window,
  and the merge of the two lists does not change.
- **Still no merge across kinds.** ADR 0035 rejects a merge in the engine across kinds. The rerank step changes marks
  only: it does not reorder a window, drops no entry, and does not order the page by distance across kinds.

## Details

- **No Provider distance, no strong by distance.** When no Provider Tool entry has a distance, no Authored Tool is strong
  by distance; rules 1 and 2 still apply. This happens when no Provider Tool in scope has a current vector, the nearest
  vectors are stale, the Provider Tool vector query fails, or semantic search is off.
- **An equal distance is not strong.** An Authored Tool exactly as near as the nearest Provider Tool is last.
- **An Authored Tool outside its vector list has no distance.** The list holds the window's depth (100) of the nearest
  Authored Tools of one kind.
- **The Provider filter keeps its rule.** It does not narrow Authored Tools, as the `coral.search` description says. An
  Authored Tool that does not fit the filter's Providers is last unless a rule makes it strong.
- **Paging does not change.** The window size, `has_more`, the coverage and `next_offset` stay. The order is the same
  for every limit and offset ([ADR 0028](0028-search-pages-by-a-plain-offset.md)). At `limit` 50, a last Authored Tool
  follows up to 100 Provider Tools of the window, so it starts on page 2 or 3.
- **Telemetry.** The `coral.search` span records how many Authored Tools each rule made strong
  (`coral.search.authored_strong.phrase`, `.rare_path_word`, `.distance`; an entry counts under its first rule in that
  order) and whether a Provider Tool distance was there (`coral.search.provider_tool_distance`).

## Known losses

- **An Authored Tool that shares no word with the query** is strong in about 11% of the label test's cases, where the
  fixed count made it strong in 84% (and on every other search too). When the page has room, it stays as a last entry.
  The model often finds the right Authored Tool (first of its own list in 53.1% of these cases), but it cannot tell that
  the Tool beats the Provider Tools.
- **At `limit` 50** a last Authored Tool starts on page 2 or 3.

## Candidates not chosen

- **A0, the fallback if this step fails its bars:** no vector query for Authored Tools, with the rare-word rule. On
  worded queries its hits are 2 to 4 points lower, and it has no hit with no shared word.
- **A:** the vector list only orders the lexical matches. No case can measure its gain over A0.
- **B, the last resort:** one vector order across kinds by distance. Not measured, and it reverses the merge across kinds
  that ADR 0011 and ADR 0035 reject.
- **C, rejected:** a fixed distance limit. The model card of `bge-small-en-v1.5` says that the order of scores matters,
  not their value, and the best limit for Provider Tools moved between sets (0.64 to 0.70).
- **Mark only:** take the strong mark away and keep fusion as it is. It gives back the first 3 places, but leaves
  Authored Tools in the last 3 places of a page of 8.

## Direction

The step only sets marks now. Its name says what it can grow into: a real reranker that scores the query against the
entries of every kind. That is a new decision, with its own measurement.
