---
status: accepted
---

# Tasks and Runs are named with four words, stored as the identifier

A new Task or Program Run identifier is four words from a fixed list of 246, drawn at random and joined by hyphens:
`height-word-database-library`. The text is the row's primary key. There is no UUID behind it, no second column, no
migration and no codec. Rows written before this decision keep their UUID text, and every parser accepts both shapes.
The decision was made on [issue #2412](https://github.com/withcoral/coral/issues/2412), whose comments hold the
measurements; this record states the shape and the contract.

## Why

Agents copy Task and Run identifiers into every `search`, `describe`, `exec`, `wait` and `cancel` call. A UUID costs
22 to 32 tokens per copy, and its 32 hex digits are a transcription hazard. Four words that are each one token cost 8.0
tokens on Claude 4.7 and later and 3.4 on GPT, and an agent reads and repeats them without error. Two forms were
measured and rejected on the way: 13 characters of base32 (11.9 tokens on Claude, and still a random string), and
readable "Docker-style" adjectives and animals (15.3 tokens on Claude, because its vocabulary fuses only common words).

## The shape

- **Form.** Four words, any order, lowercase, joined by hyphens, from `crates/coral-identifier/words.txt`. The list is
  hand-curated from the 822 whole lowercase words in Claude's vocabulary that are also one token as a bare word on
  the tokenizers of GPT, Llama 3, Qwen3, Gemma 3, Mistral and DeepSeek. It holds no ordinals, directions, numbers,
  months, names, places, plurals, participles, code tokens, Coral terms, or sensitive, political or instruction-like
  words. A first plan of two adjectives and two nouns was dropped: the vocabulary holds only 52 clean adjectives, too
  few for the space below. `validate_words.py` beside the list is the tokenizer check, run by hand after an edit; it
  is not a CI gate.
- **Storage.** The words are the `id` column of `task_records` and `program_run_records`, which is `TEXT` on SQLite
  and Postgres and already the sole primary key, with `UNIQUE (workspace_id, id)` beside it for Workspace-scoped
  reads. Nothing else changes in the schema.
- **Uniqueness.** The primary key proves it, per deployment, per table. A new row is inserted with
  `ON CONFLICT DO NOTHING`; a row count of zero means the words are taken, and the caller draws again, up to eight
  times, then fails with `IdentifierIssuance`. The space is 246⁴ = 3.66 × 10⁹. The retry rate with n rows is n / S:
  1 % at 37 million, 10 % at 366 million, which one Task per second reaches after 11.6 years. The chance that eight
  draws in a row are taken at 10 % is 10⁻⁸ per insert.
- **One form everywhere.** MCP arguments and results, gRPC resource names, Reef URLs and the Run Status Page,
  `run_status_url` and logs all carry the same text. The MCP schema pattern is `coral_identifier::SCHEMA_PATTERN`,
  and Reef's stable Run link checks its name against the same pattern before it asks Coral.
- **Both shapes parse.** `TaskId::parse` and `RunId::parse` accept four or five lowercase words joined by hyphens, or
  a canonical lowercase UUID. They do not check the words against the list, so a later edit to the list never
  invalidates a stored identifier.
- **Run cursors.** The cursor token carried the Run's 16 UUID bytes. It now carries the first 16 bytes of the SHA-256
  of the identifier text, so it stays fixed-size for every identifier length, and its format byte is 4. A cursor from
  before this release is rejected as invalid once; the caller observes again without one and replays its output.
- **No authority.** A 32-bit identifier is guessable. A request for another Principal's Task or Run answers exactly
  as a request for one that does not exist; the owning modules' scoping tests hold that line.

## Growth

A very large deployment adds a fifth word: 246⁵ = 9.0 × 10¹¹, two more tokens on Claude, seven more characters. The
parsers, the MCP schema and the gRPC names accept five words from the start, so the switch is a generation setting,
not a contract change, and mixed lengths in one table are fine because the key is the text. The word list cannot
grow much: Claude's vocabulary caps it, and curation keeps about a third of it.

## Not taken

- **Per-Workspace uniqueness.** It would need a migration that replaces the primary key with `(workspace_id, id)`
  and would let two Runs in one deployment share a name in logs and support requests. The space is not needed.
- **A stricter list rule.** Keeping only words whose hyphen merges with them on every BPE tokenizer saves 0.4 tokens
  on GPT and nothing on Claude, at the price of a 152-word list.
- **A fifth word now.** Four words serve every deployment Coral runs today; see Growth.

The token costs are a 2026 measurement of nine tokenizers. New models change them; the stored identifiers and the
uniqueness rule do not depend on them.
