---
status: accepted
---

# Lists page by a plain key

A list that can grow without limit returns one page at a time, forward only. The module that owns the list reads the
page. It orders the rows by a key that is unique within the list's scope, and reads the rows after the key that the
request names, in one read snapshot. It reads one row more than the page holds, to learn if another page follows, and
returns a typed page: the items, and the key of the last item when more follow. Where a list has a key that does not
change while its row exists, it pages by that key.

The key type owns its text form, so every Adapter spells a cursor the same way. The Adapter validates the page size
and calls the key type to read the cursor and to write the next one. It does nothing else for the page.

Before this, the gRPC Adapter made an opaque token for each page. The token held the position and a scope string made
of the caller, the parent and the collection, and the Adapter refused a token whose scope did not match the request.
Some lists also read every row and cut the page in the Adapter, so the cost of one page grew with the collection.

The cursor is now the key as plain text, and it binds nothing: no caller, parent, filter, format version or signature.
The reasoning is the same as for search ([ADR 0028](0028-search-pages-by-a-plain-offset.md)). Each request names its
own scope and filter, each page authorizes again, and the query keeps the scope predicate beside the key predicate. A
key from another Workspace, another Principal or another filter can only position the read inside the request's own
scope. It cannot show a row that the caller cannot list. So the binding protected nothing, and its cost was a token
that nobody can read in a log or write by hand in a test.

A client passes only a value that the list returned. Coral does not check this: it accepts any text in the key's
format as a position, and that format can change. Text that is not in the key's format is an invalid argument.

A text key is compared in byte order on both databases, so SQLite and Postgres return the same pages.

## What a caller can rely on

- Each page reads the state at the time of its own request. The pages of one read are not a snapshot.
- A row that exists, and keeps its key, for the whole read appears exactly once. A row whose key changes between two
  pages can appear twice or not at all.
- A row created behind the cursor does not appear. A row created ahead of the cursor appears.
- The read does not need the row that the cursor came from. If that row is deleted between two pages, the next page
  still follows its key.

## When a list does not page

A list may return all its rows in one response only when configuration, or another bounded list, bounds its size. A
list that a User or an Agent can grow without limit pages.

## What stays as it is

- Search pages by a count of ranked results to skip (ADR 0028). Ranked results have no stable key to continue from.
- The cursor of the tool call that observes a Run
  ([ADR 0029](0029-mcp-requests-are-served-without-sessions-and-a-required-action-reaches-its-user-through-the-assistants-link.md))
  is a position in that Run's output, not a list key.
- A read that also pages backward, or around an anchor, is outside this decision.

## Considered options

- **Keep the token bound to its scope.** Rejected: each page authorizes again and the query is scoped, so the check
  protects nothing.
- **Sign the cursor.** Rejected for the same reason, and it adds a key to keep.
- **Page by offset, as search does.** Rejected: a row created or deleted before the offset moves every later row, so a
  page repeats or skips rows, and the database reads the skipped rows on every page. Search accepts this because a
  ranking has no key.

## Consequences

- Lists move to this contract one at a time. A list that has not moved keeps its token until it does.
- A token from before a list moved is not migrated. The list refuses it, or reads it as a key and returns a page from
  that position. A client keeps a cursor only for one read, so we accept this.
- On Postgres, an index serves the order of a text key only when the index has the `C` collation. Until a list has
  such an index, each of its pages sorts the rows of its scope.
