---
status: accepted
---

# A rerank step decides by distance whether an Authored Tool is strong

Universal Search has a rerank step between the retrievers and fusion. It sets the final mark of each Authored Tool, a
Private Tool or a Shared Tool. An Authored Tool is strong when one of these rules holds:

1. **Phrase:** a field holds the query's words in order.
2. **Rare path word, near enough:** a content word of the query starts a word of the Tool's path, few of the
   Workspace's Provider Tool paths hold a word that starts with it
   ([#3426](https://github.com/withcoral/coral/issues/3426)), and the Tool is no farther from the query than the
   Provider Tool of a fixed rank by distance, or the farthest Provider Tool with a distance when fewer have one. Words
   such as `get` and `list` start a word of most Provider Tool paths, so they make no Authored Tool strong.
3. **Distance:** the Tool is strictly nearer to the query than the nearest Provider Tool of the same request.

The Provider Tool window holds only the Tools inside the search's Provider filter, so each comparison is inside it too.
A distance condition applies only when both distances exist. When the Tool has no distance of its own, or no Provider
Tool has one, the rare path word decides alone, as it did before this step, and rule 3 makes nothing strong.

The Authored Tool retriever applies rule 1 and finds the rare path word; the rerank step applies the distance
conditions. An Authored Tool that no rule makes strong gets a third mark, **last**: fusion puts it after every weak
entry. Fusion orders strong, weak and last
entries, each group by position and then by retriever order. A Provider Tool keeps the rules of
[ADR 0011](0011-universal-search-engine-and-retrievers.md) and
[ADR 0035](0035-universal-search-adds-a-vector-list-from-a-bundled-cpu-model.md), its place near the top of its vector
list included.

## Why the fixed count fails for Authored Tools

ADR 0035 made a fixed number of entries at the top of each kind's vector list strong, and read no distance. A vector
list has nearest entries for every query. So with semantic search on, that many Private Tools and Shared Tools were
strong on every search, whatever the query asked. In one team Workspace with real use, Private and Shared Tools became half of all hits,
and the called Provider Tool fell out of the first 3 places of the page: 93.2% before, 87.0% after
([#3412](https://github.com/withcoral/coral/issues/3412)). The one Shared Tool of that Workspace was on 97.6% of the
pages. For Provider Tools, ADR 0035 measured that the fixed count gains 2.1 to 3.8 points. The Authored Tools of one
caller are few (up to 38 for one principal in that Workspace), and the nearest of them are only the least far.

Removing the strong mark alone does not end the crowding. Fusion merges by position, so a weak Authored Tool at
position 1 still comes before a weak Provider Tool at position 2. Hence the third mark.

Rule 3 compares two distances from one query vector and one model, so it needs no tuned limit. A label test on the same
Workspace (#3412) measured each rule: how often it marks one of the caller's Authored Tools strong on 698 real searches
after which the agent called a Provider Tool (false alarm), and how often it marks the target strong for 81 Authored
Tools with three written queries each.

| Strong rule for an Authored Tool                       | False alarm | Hit, 3 words | Hit, sentence | Hit, no shared word |
| ------------------------------------------------------ | ----------: | -----------: | ------------: | ------------------: |
| Fixed count at the top of its own vector list (before) |      100.0% |       100.0% |        100.0% |               84.0% |
| Rare path word                                         |        9.5% |        90.1% |         93.8% |                0.0% |
| Nearer than the nearest Provider Tool                  |        1.3% |        67.9% |         82.7% |               11.1% |
| Rare path word, or nearer than the nearest Provider Tool |      9.6% |        93.8% |         97.5% |               11.1% |

## Why a rare path word needs a distance too

The bench of [coral-benchmarks#698](https://github.com/withcoral/coral-benchmarks/issues/698) measured rules 1 to 3 on
Set A and Set B, with stubs of the same Workspace's Authored Tools; the scoreboard is on #3412. With the rare path word
alone, an Authored Tool was still in the first 3 places of 47% to 60% of the Provider pages, and the target Provider
Tool lost 3.5 to 5.2 points of hit@3 against the same catalog without Authored Tools. Almost every such page matched a
rare path word: the bench queries are long, and in a catalog of a few thousand Provider Tools many everyday words are
rare. Without the rare path word, the Provider pages kept their places, but worded queries for
an Authored Tool lost 5.7 to 8.6 points of hit@3. No cut-off for "rare" passed both. Rule 2 asks a rare path word to be
near the query as well, because a Tool that a word matches by chance is seldom near the query:

| Bench, `main` → this decision | Provider crowding, hit@3 | An Authored Tool in the first 3 places | Worded Authored queries, hit@3 against `main` |
| ----------------------------- | -----------------------: | -------------------------------------: | --------------------------------------------: |
| Set A                         |  −20.5 → −0.8 points     |                           100% → 9.5%  |                    −2.1 (3 words), 0.0 (sentence) |
| Set B, one / two writers      |  −17.3 / −16.5 → −0.1 / −0.5 |                 100% → 4.6% / 4.3% |                    −5.0 (3 words), +3.6 (sentence) |

Crowding is the target Provider Tool's hit@3 with the Authored Tools in the catalog minus without them, paired by
case; the bench's bar was 2 points. The bar for worded Authored queries was 5 points below `main`, and Set B's
3-word queries sit on it.

The rank is relative, as rule 3 is, so it needs no limit on the distance's value.

## Exceptions to ADR 0011 and ADR 0035

This decision amends ADR 0011 (what crosses into the engine, kind in the engine, equal fusion) and ADR 0035 (the fixed
count for every kind). Both now state the current rules; this section gives the reasons.

- **A distance crosses into the engine.** A window entry carries its distance from the query when its retriever's
  vector list holds it. Both stores compute the cosine distance from one query vector and one model, so the distances
  of two kinds compare, where their scores do not. Only the rerank step reads them. Scores, ranks and merge scores
  still never compare across kinds.
- **One place in the engine reads the kind.** The rerank step reads the kind of an entry directly. Rejected: a role
  that each retriever gives its entries, which adds a concept for one exception.
- **Fusion does not weigh Authored Tools equally.** A last Authored Tool follows every weak Provider Tool. Strong
  entries of every kind still merge by position, and at equal position the retriever order still decides.
- **The fixed count is for Provider Tools only.** The vector list still adds Authored Tools to the window, and the
  merge of the two lists does not change.
- **Still no merge across kinds.** The rerank step changes marks only: it does not reorder a window, drops no entry,
  and does not order the page by distance across kinds.

## Details

- **A missing distance.** No Provider Tool entry has a distance when no Provider Tool in scope has a current vector,
  the Provider Tool vector query fails, or the request has no query vector (semantic search off, the model not ready,
  the query not embedded). An Authored Tool has none when its vector is missing or stale, its vector query fails, or
  its vector list ends before it; the list holds as many of the nearest Authored Tools of one kind as the window holds.
  In each case the rare path word decides alone and rule 3 makes nothing strong, so a search without the vector side
  marks Authored Tools as it did before this step.
- **Equal distances.** An Authored Tool exactly as near as the nearest Provider Tool is not strong by rule 3. One
  exactly as near as the Provider Tool of the rank of rule 2 is near enough for its rare path word.
- **The Provider filter keeps its rule.** It does not narrow Authored Tools, as the `coral.search` description says. An
  Authored Tool that does not fit the filter's Providers is last unless a rule makes it strong.
- **Paging does not change.** The window size, `has_more`, the coverage and `next_offset` stay. The order is the same
  for every limit and offset ([ADR 0028](0028-search-pages-by-a-plain-offset.md)). A last Authored Tool follows every
  Provider Tool of the window, so at the largest page size it starts on page 2 or 3.
- **Telemetry.** The search span records how many Authored Tools each rule made strong (an entry counts under its first
  rule: phrase, rare path word near enough, distance) and whether a Provider Tool distance was there.

## Known losses

- **An Authored Tool that shares no word with the query** is strong in about 11% of the label test's cases, where the
  fixed count made it strong in 84% (and on every other search too). When the page has room, it stays as a last entry.
  The model often finds the right Authored Tool (first of its own list in 53.1% of these cases), but it cannot tell that
  the Tool beats the Provider Tools.
- **At the largest page size** a last Authored Tool starts on page 2 or 3.

## Candidates not chosen

- **The rare path word alone,** as #3426 built it: the Provider pages of the bench kept an Authored Tool in their first
  3 places, as above.
- **No rare path word,** rules 1 and 3 only: worded queries for an Authored Tool lost 5.7 to 8.6 points on the bench.
- **A nearer rank for rule 2** (the 1st to 3rd Provider Tool), estimated from the bench replays: worded queries lost
  up to 8 points on Set B. **The nearest Provider Tool's distance plus a margin**, estimated the same way: it passed
  at one margin only, and a margin is a limit on the distance's value, as C.
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
