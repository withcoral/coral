---
status: accepted
---

# Tasks and Runs are named with four words, stored as the identifier

A new Task or Program Run identifier is four words from a fixed list, drawn at random and joined by hyphens, such as
`height-word-database-library`. The text is the row's primary key, with no UUID behind it. Rows written earlier keep
their UUIDs, and parsers accept both shapes. [Issue #2412](https://github.com/withcoral/coral/issues/2412) holds the
measurements.

## Why

Agents copy Task and Run identifiers into nearly every MCP call. A UUID costs 22 to 32 tokens per copy, and its 32 hex
digits invite transcription errors. Four single-token words cost about 8 tokens on Claude and 3.4 on GPT, and agents
repeat them without error. Rejected along the way: 13 characters of base32, which is still a random string at 11.9
tokens on Claude; and Docker-style adjective and animal names, at 15.3 tokens because tokenizers fuse only common
words.

## Consequences

- **Uniqueness is the primary key's,** per deployment. An insert that collides draws again, a bounded number of times.
  Rejected: per-Workspace uniqueness, which needs a composite key and lets two Runs share a name in logs and support
  requests.
- **An identifier grants nothing.** Four words are guessable, so a request for another Principal's Task or Run answers
  exactly as one for a Task or Run that does not exist.
- **The list can change.** Parsers never check identifiers against it, so editing it never invalidates a stored one.
  The [identifier README](../../crates/coral-identifier/README.md) covers what the list admits.
- **Growth adds a fifth word.** Parsers and schemas already accept five, so a very large deployment changes a
  generation setting, not a contract.
