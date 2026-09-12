# Claude Code Sessions

**Version:** 0.1.0
**Backend:** File (JSONL)
**Tables:** 2

Query Claude Code session history and project memories through SQL. Search past prompts, share project context, track decisions across sessions, and transfer knowledge between collaborators.

## Installation

Install the source via the CLI:

```bash
coral source add --file sources/community/claude_code_sessions/manifest.yaml
```

The `history` table reads directly from `~/.claude/history.jsonl` (no converter). The `memories` table needs a one-time converter run:

```bash
python3 sources/community/claude_code_sessions/scripts/memories-to-jsonl.py
```

## Prerequisites

- [Claude Code](https://docs.anthropic.com/en/docs/claude-code) installed and used at least once (creates `~/.claude/history.jsonl`)

## Quick Start

```sql
-- Recent prompts
SELECT display, project, timestamp
FROM claude_code_sessions.history
ORDER BY timestamp DESC
LIMIT 10;

-- Search prompts by keyword
SELECT display, project
FROM claude_code_sessions.history
WHERE display LIKE '%deploy%'
LIMIT 10;

-- Find sessions for a project
SELECT DISTINCT "sessionId", project
FROM claude_code_sessions.history
WHERE project LIKE '%coral%';

-- Count prompts per project
SELECT project, COUNT(*) as prompt_count
FROM claude_code_sessions.history
GROUP BY project
ORDER BY prompt_count DESC;

-- List all project memories
SELECT name, type, description, project
FROM claude_code_sessions.memories;

-- Search memories by keyword
SELECT name, description, body
FROM claude_code_sessions.memories
WHERE body LIKE '%coral%';

-- Share context: what decisions were made?
SELECT name, description
FROM claude_code_sessions.memories
WHERE type = 'feedback';
```

## Tables

### `history`

Claude Code prompt history. Each row is a user prompt sent to Claude Code, with the project path, session ID, and timestamp. Data is written automatically by Claude Code at `~/.claude/history.jsonl`.

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `display` | Utf8 | User prompt text sent to Claude Code |
| `timestamp` | Int64 | Unix timestamp in milliseconds |
| `project` | Utf8 | Absolute path to the project directory |
| `sessionId` | Utf8 | UUID of the Claude Code session (quote as `"sessionId"` in SQL) |

**Note:** `sessionId` uses camelCase (matching Claude Code's JSONL format). Quote it in SQL: `"sessionId"`.

---

### `memories`

Claude Code project memories — decisions, preferences, context, and learnings stored across sessions. Share this table to transfer project knowledge to collaborators or other agents. Run the converter script first.

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `name` | Utf8 | Slug identifier for the memory |
| `description` | Utf8 | One-line summary of the memory |
| `type` | Utf8 | Memory type (user, project, feedback, reference) |
| `project` | Utf8 | Project directory path slug |
| `origin_session_id` | Utf8 | Session that created this memory |
| `body` | Utf8 | Full memory content (markdown) |

## Source scope

- File-backed source reading directly from `~/.claude/history.jsonl`.
- No credentials, no API key needed.
- `history` reads directly from Claude Code's native JSONL — no converter.
- `memories` needs a one-time converter: `python3 sources/community/claude_code_sessions/scripts/memories-to-jsonl.py`
- Any Claude Code user has this file after their first session.
- 2 declared test queries require no filters.
- Read-only access to prompt history. Full conversation content (responses, tool calls) is not in this file.

## Limitations

- Only user prompts are stored in `history.jsonl` — Claude's responses, tool calls, and file edits are not included.
- `sessionId` is camelCase (matching the JSONL key). Use quoted identifiers in SQL: `"sessionId"`.
- `timestamp` is Unix milliseconds (Int64), not a formatted date. Use `timestamp / 1000` for Unix seconds.
- The `pastedContents` field from the JSONL is not exposed as a column (complex nested JSON).
- Data is append-only — Claude Code adds new entries but doesn't remove old ones.

## Provider docs

- Claude Code: https://docs.anthropic.com/en/docs/claude-code

## Live validation output

Validated against a live Claude Code installation with 915 history entries.

```bash
$ coral source lint sources/community/claude_code_sessions/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/claude_code_sessions/manifest.yaml
Added source claude_code_sessions

  ✓ claude_code_sessions connected successfully

    claude_code_sessions (2 tables)
    ├─ history
    └─ memories
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT display, project, "sessionId" FROM claude_code_sessions.history LIMIT 3
      3 rows
```

**Live history proof:**

```sql
SELECT display, project, "sessionId"
FROM claude_code_sessions.history LIMIT 3;
```

```text
+--------------------------------------------------------------+--------------------------------------+--------------------------------------+
| display                                                      | project                              | sessionId                            |
+--------------------------------------------------------------+--------------------------------------+--------------------------------------+
| now analysis this project and tell where it lacking curren...| /Users/user/AI Autopsy Engine        | 0a2ea000-0000-0000-0000-40c2c917d96b |
| analysis this project goal is making ai black box into tra...| /Users/user/AI Autopsy Engine        | 14d19100-0000-0000-0000-9d6f635c3deb |
| brew upgrade claude                                          | /Users/user                          | e1439900-0000-0000-0000-7f781fc11e52 |
+--------------------------------------------------------------+--------------------------------------+--------------------------------------+
```

**Live search proof:**

```sql
SELECT display, project
FROM claude_code_sessions.history
WHERE display LIKE '%coral%' LIMIT 3;
```

```text
+--------------------------------------------------------------+-----------------------------+
| display                                                      | project                     |
+--------------------------------------------------------------+-----------------------------+
| see i want to make make an chatbot on my data and also us... | /Volumes/algsoch/algsoch    |
| see hackathon has completed no i do not need demo one but... | /Volumes/algsoch/careops    |
| see i am not just use claude to develop this withcoral...    | /Volumes/algsoch/terminal 3 |
+--------------------------------------------------------------+-----------------------------+
```

**Live project count proof:**

```sql
SELECT project, COUNT(*) as prompt_count
FROM claude_code_sessions.history
GROUP BY project
ORDER BY prompt_count DESC
LIMIT 5;
```

```text
+--------------------------------------+--------------+
| project                              | prompt_count |
+--------------------------------------+--------------+
| /Volumes/algsoch/terminal-3          | 453          |
| /Volumes/algsoch/coral               | 120          |
| /Users/user/AI Autopsy Engine        | 85           |
| /Volumes/algsoch/algsoch             | 72           |
| /Volumes/algsoch/careops             | 45           |
+--------------------------------------+--------------+
```
