---
status: accepted
---

# Search pages by a plain offset

A search page continues from a plain count of ranked results to skip. The count binds nothing: no format version, and
no digest of the caller, Workspace, query or Provider filter. Agents read the paging state on most search results and
almost never page, so the cost of an opaque cursor is in reading it on every result, not in the rare page that uses it
([#2809](https://github.com/withcoral/coral/issues/2809)).

The digest bought little. Each page authorizes again, ranks again in a new read snapshot and applies current
visibility. A forged offset can skip results, but it cannot show a Tool that the caller cannot see. Pages were never a
snapshot: an index write between two pages moves positions, with or without a format check. An offset that an agent
reuses with a different query or filter pages that query's results from the same position, so the agent misses its
first results. Paging is rare, so we accept this.

Task history keeps the offset of each search. No migration changes the pages that Task history kept before this
change: such a page shows as a first page, without its mark that more results remain.

Rejected: a short cursor that keeps the version and a part of the digest. It costs less than the full cursor and keeps
every check, but each page authorizes again, so the checks protect nothing.
