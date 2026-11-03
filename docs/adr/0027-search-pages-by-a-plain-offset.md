---
status: accepted
---

# Search pages by a plain offset

`search` pages by `offset`, the count of ranked results to skip. A page carries `next_offset` only while more results
remain. The offset binds nothing: it has no format version and no digest of the caller, Workspace, query or Provider
filter. Agents read the paging state on most search results and almost never page, so its cost is in reading. The earlier
cursor, base64url JSON with a SHA-256 digest, cost about 140 Claude tokens and 95 o200k tokens. An offset costs one or
two (#2809).

The digest bought little. Each page authorizes again, ranks again in a new read snapshot and applies current visibility.
A forged offset can skip results, but it cannot show a Tool that the caller cannot see. Pages were never a snapshot: an
index write between two pages moves positions, with or without a format check. An offset that the agent reuses with a
different query or filter pages that query's results from the same position, so the agent misses its first results.
Paging is rare, so we accept this.

Task history keeps the offset of each search. No migration changes the pages that Task history kept before this change.
Their cursor text stays, and nothing reads it: such a page shows as a first page, without its mark that more results
remain.

Rejected: `<version>.<position>.<first 8 hex of the digest>`. It costs about 10 tokens and keeps every check, but each
page authorizes again, so the checks protect nothing.
