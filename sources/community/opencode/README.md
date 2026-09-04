# OpenCode

**Version:** 0.3.0
**Backend:** File (JSONL)
**Tables:** 20

Query local OpenCode data — sessions, messages, message parts, todos, user prompts, shared sessions, projects, project directories, workspaces, the full domain event log, event sequencing, accounts and control accounts, stored credentials, data/migration history, permissions, and per-session context epochs — through Coral SQL. The included converter script reads OpenCode's local SQLite database (`~/.local/share/opencode/opencode.db`) and writes JSONL files that Coral's `file` backend can serve with no running server.

## Installation

1. Run the converter script to export OpenCode data to JSONL:

```bash
python3 sources/community/opencode/scripts/opencode-to-jsonl.py
```

2. Install the source:

```bash
coral source add --file sources/community/opencode/manifest.yaml
```

The script opens the OpenCode database in read-only mode (`mode=ro`) and never writes to it. Re-run any time the on-disk database changes to refresh Coral's view.

## Prerequisites

- **Python 3.8+** — the converter uses only stdlib (`sqlite3`). No external dependencies.
- **OpenCode installed** and run at least once — the database file `~/.local/share/opencode/opencode.db` must exist. Linux/macOS only by default; use `--db-path` to override.
- The `~/.local/share/opencode/` path is XDG-style on every platform; on macOS that is where OpenCode stores its data, not `~/Library/Application Support/opencode/`.

## Quick Start

```bash
python3 sources/community/opencode/scripts/opencode-to-jsonl.py
coral source add --file sources/community/opencode/manifest.yaml
```

```sql
-- Sessions by recent activity, with model + cost breakdown
SELECT id, title, agent, model_provider, cost, tokens_input, tokens_output
FROM opencode.sessions
ORDER BY time_updated DESC
LIMIT 10;

-- Pull the transcript text of one session in chronological order
SELECT message_id, time_created,
       json_as_text(data, 'type')  AS part_type,
       json_as_text(data, 'text')  AS body
FROM opencode.parts
WHERE session_id = 'ses_...'
ORDER BY time_created ASC;

-- Incomplete todos for one session
SELECT content, priority, position
FROM opencode.todos
WHERE session_id = 'ses_...'
  AND status <> 'completed'
ORDER BY position ASC;

-- Most expensive sessions this month
SELECT id, title, model_provider, cost
FROM opencode.sessions
ORDER BY cost DESC
LIMIT 10;

-- Aggregate token usage by model provider
SELECT model_provider,
       count(*)                  AS sessions,
       sum(tokens_input)         AS total_in,
       sum(tokens_output)        AS total_out,
       sum(tokens_cache_read)     AS total_cache_read
FROM opencode.sessions
GROUP BY model_provider
ORDER BY total_in DESC;
```

## Converter usage

```bash
# Export every table from the default location
python3 sources/community/opencode/scripts/opencode-to-jsonl.py

# Override database path (e.g. on a different machine or a copied snapshot)
python3 sources/community/opencode/scripts/opencode-to-jsonl.py \
  --db-path /mnt/snapshot/opencode.db

# Override output directory
python3 sources/community/opencode/scripts/opencode-to-jsonl.py \
  --output /path/to/output

# Export only one table
python3 sources/community/opencode/scripts/opencode-to-jsonl.py \
  --only parts
```

Default database path: `~/.local/share/opencode/opencode.db`.
Default output directory: `~/.coral/opencode/`.

**Note:** the `--output` option writes to a custom path, but the manifest reads from `~/.coral/opencode/`. Update the manifest `source.location` blocks if you use a custom output path.

## Tables

| Table | Description |
| --- | --- |
| `opencode.sessions` | One row per OpenCode session, with flattened model / token / cost / timestamp columns and a `metadata` JSON column for the raw JSON-shaped fields. |
| `opencode.messages` | One row per message header stored in the OpenCode `message` table, with the full payload as a JSON column. |
| `opencode.session_messages` | The same messages projected through the per-session sequence table, ordered by `seq` rather than `time_created`. |
| `opencode.parts` | One row per message part — the actual transcript content (text, tool calls, images, step-start markers). This is the table to read for full session content. |
| `opencode.todos` | One row per session todo list entry (status, priority, position). |
| `opencode.session_inputs` | One row per user prompt admitted to a session, in admission order. |
| `opencode.session_shares` | One row per shared session URL. Treat `secret` as sensitive. |
| `opencode.projects` | One row per OpenCode project, with worktree, display name, and timestamps. |
| `opencode.project_directories` | One row per directory attached to a project. |
| `opencode.workspaces` | One row per workspace. |
| `opencode.events` | The full OpenCode domain event log — the largest table (~1M rows). Every state change, with per-aggregate sequence and a `type`-typed `data` blob. |
| `opencode.event_sequences` | Per-aggregate event sequencing counters. |
| `opencode.accounts` | OpenCode account rows (email, url, token expiry, timestamps). Tokens not exposed. |
| `opencode.account_states` | Currently active account / org. |
| `opencode.control_accounts` | Control-plane account rows (auth). Tokens not exposed. |
| `opencode.credentials` | Stored provider credentials (label, connector, method, active). Secret `value` not exposed. |
| `opencode.data_migrations` | Applied data migrations. |
| `opencode.migrations` | Applied schema migrations. |
| `opencode.permissions` | Per-project permission rows (action, resource). |
| `opencode.session_context_epochs` | Per-session context-compaction baselines and snapshots. |

### `opencode.sessions`

The session index. Use this as the entry point for any query.

**Columns**

| Column | Type | Description |
| --- | --- | --- |
| `id` | Utf8 | Session id (`ses_...`) |
| `project_id` | Utf8 | OpenCode project id |
| `parent_id` | Utf8 | Parent session id for forks / sub-agents |
| `workspace_id` | Utf8 | Workspace id when set |
| `slug` | Utf8 | Short slug derived from the title |
| `title` | Utf8 | Session title (often the first user prompt) |
| `directory` | Utf8 | Working-directory path |
| `path` | Utf8 | Project path when set |
| `agent` | Utf8 | Agent name (`build`, `general`, `explore`, ...) |
| `model` | Utf8 | Full model identifier as JSON (`{"id","providerID","variant"}`) |
| `model_id` | Utf8 | Model id parsed from `model` |
| `model_provider` | Utf8 | Provider id parsed from `model` |
| `version` | Utf8 | OpenCode schema/version string |
| `share_url` | Utf8 | Public share URL when shared |
| `tokens_input` | Int64 | Total input tokens |
| `tokens_output` | Int64 | Total output tokens |
| `tokens_reasoning` | Int64 | Reasoning tokens when reported |
| `tokens_cache_read` | Int64 | Prompt-cache tokens read |
| `tokens_cache_write` | Int64 | Prompt-cache tokens written |
| `cost` | Float64 | Cumulative session cost in USD |
| `time_created` | Int64 | Session creation timestamp (epoch ms) |
| `time_updated` | Int64 | Last activity timestamp (epoch ms) |
| `time_compacting` | Int64 | Last context-compaction timestamp |
| `time_archived` | Int64 | Archive timestamp when set |
| `metadata` | Json | Raw `metadata`, `summary_diffs`, `revert`, and `permission` blobs |

---

### `opencode.parts`

The transcript content. Use `json_as_text(data, 'text')` to extract the message body.

**Columns**

| Column | Type | Description |
| --- | --- | --- |
| `id` | Utf8 | Part id (`prt_...`) |
| `message_id` | Utf8 | Parent message id |
| `session_id` | Utf8 | Parent session id |
| `time_created` | Int64 | Creation timestamp (epoch ms) |
| `time_updated` | Int64 | Last-updated timestamp (epoch ms) |
| `data` | Json | Full part payload (`type`, `text`, `tool_call`, file changes, etc.) |

---

### `opencode.todos`

Session-scoped todo lists. Composite primary key `(session_id, position)`.

**Columns**

| Column | Type | Description |
| --- | --- | --- |
| `session_id` | Utf8 | Session id this todo belongs to |
| `content` | Utf8 | Todo text |
| `status` | Utf8 | `pending`, `in_progress`, `completed`, `cancelled`, ... |
| `priority` | Utf8 | `high`, `medium`, `low`, ... |
| `position` | Int64 | Position in the session's todo list |
| `time_created` | Int64 | Creation timestamp (epoch ms) |
| `time_updated` | Int64 | Last-updated timestamp (epoch ms) |

(Other tables follow the same column shape as their declared schema; see `manifest.yaml` for the full list.)

## Source scope

- File-backed source reading from `~/.coral/opencode/*.jsonl`. **No running server required.**
- The converter opens the SQLite database read-only (`mode=ro`) so it never touches OpenCode's live state.
- The converter uses Python stdlib only (`sqlite3`). No external dependencies.
- Data is static — re-run the converter script after the on-disk database changes to refresh.
- `model` columns in OpenCode are JSON objects `{"id","providerID","variant"}`; the converter parses them into `model_id` and `model_provider` flat columns and also keeps the raw `model` JSON for transparency.
- `events.jsonl` is the largest export on long-running installs (OpenCode's event log can hold ~1M rows / hundreds of MB). `parts.jsonl` is also large. The source intentionally excludes `events` and `parts` from the manifest's `test_queries` so `coral source add` / `coral source test` finish in seconds; query `opencode.events` / `opencode.parts` after install as needed.
- 4 declared test queries (`sessions`, `messages`, `todos`, `projects`) cover the most common access patterns without forcing a full events- or parts-table scan during install.
- Live secrets are intentionally **not** exported at all: the converter drops `accounts.access_token` and `accounts.refresh_token`, `control_accounts.access_token` and `control_accounts.refresh_token`, `credentials.value`, and `session_shares.secret`. The source exposes only non-secret metadata for these rows.

## Limitations

- The converter fetches the entire SQLite database on each run — there is no incremental / streaming export. On a very large install (millions of message parts), the export may take a few seconds and produce multi-hundred-MB JSONL files.
- The `parts.data` JSON column contains the full transcript content. JSON parsing in SQL requires the `json_as_text` (and related) helpers and is not pushdown-friendly, so `SELECT * FROM opencode.parts WHERE json_as_text(data, 'text') LIKE '%foo%'` will scan every row. For pattern matching across many parts, export the parts table to a structured store or use a downstream tool that supports JSON column indexes.
- `session_inputs` and `session_shares` may be empty on a fresh install — OpenCode only writes to them once you share a session or admit a user prompt in a way that records an input row. The manifest declares the columns regardless.
- The OpenCode database schema is **not** a stable public contract; column meanings and JSON payload shapes can change between OpenCode versions. The converter guards the obviously-fragile fields (model, permission) and stores the rest under `metadata.data` as opaque JSON so a future schema change degrades gracefully rather than failing the source.
- `opencode serve` is **not** consulted. If you want live agent telemetry, query the `opencode serve` HTTP API directly — this source is a read-only, offline mirror of the on-disk database.

## Provider docs

- **[OpenCode CLI](https://opencode.ai/docs/cli)** — local CLI installation, where the SQLite database lives, how sessions are stored.
- **`opencode session` subcommands** — share, list, and delete sessions. The `opencode session share` command writes to `session_shares`; the converter reads it back here.
- **`opencode` TUI and ACP server** — the runtime that produces the rows in `session`, `message`, `part`, and `event`. Out of scope for this HTTP-less source.

## Validation output

The output below was produced by running the converter against a real OpenCode installation and pointing Coral at the exported JSONL files.

### Regression tests

```bash
python3 sources/community/opencode/tests/validate-fixtures.py
```

```text
OK parse_model: JSON object, missing keys, non-JSON, None
OK open_readonly: clear error when the database is missing
OK open_readonly: write attempts are rejected (read-only enforced)
OK fixture sessions.jsonl: 3 row(s), all required keys present
OK fixture messages.jsonl: 3 row(s), all required keys present
OK fixture session_messages.jsonl: 3 row(s), all required keys present
OK fixture parts.jsonl: 4 row(s), all required keys present
OK fixture todos.jsonl: 3 row(s), all required keys present
OK fixture session_inputs.jsonl: 2 row(s), all required keys present
OK fixture session_shares.jsonl: 1 row(s), all required keys present
OK fixture projects.jsonl: 3 row(s), all required keys present
OK fixture project_directories.jsonl: 4 row(s), all required keys present
OK fixture workspaces.jsonl: 2 row(s), all required keys present
OK foreign keys: every session_id in child tables exists in sessions.jsonl
OK foreign keys: every project_id in child tables exists in projects.jsonl
OK foreign keys: every message_id in parts exists in messages.jsonl
All opencode converter checks passed
```

### `coral source lint`

```text
$ coral source lint sources/community/opencode/manifest.yaml
Manifest is valid
```

### `coral source add`

```text
$ coral source add --file sources/community/opencode/manifest.yaml
Added source opencode (secrets: none)
Validating source...

  ✓ opencode connected successfully
  Secrets: none

    opencode (10 tables)
    ├─ messages
    ├─ parts
    ├─ project_directories
    ├─ projects
    ├─ session_inputs
    ├─ session_messages
    ├─ session_shares
    ├─ sessions
    ├─ todos
    └─ workspaces
    Query tests
    4 declared · 4 passed · 0 failed

    ✓ SELECT id, title, agent, model_provider, cost FROM opencode.sessions LIMIT 3
      3 rows

    ✓ SELECT id, session_id, time_created FROM opencode.messages LIMIT 3
      3 rows

    ✓ SELECT session_id, content, status, position FROM opencode.todos LIMIT 3
      3 rows

    ✓ SELECT id, worktree, name FROM opencode.projects LIMIT 3
      3 rows
```

### Live query proofs

```sql
SELECT id, title, agent, model_provider, tokens_input, tokens_output, cost
FROM opencode.sessions
ORDER BY tokens_input DESC
LIMIT 3;
```

```text
+--------------------------------+-----------------------------------------------------+-------+----------------+--------------+---------------+--------+
| id                             | title                                               | agent | model_provider | tokens_input | tokens_output | cost   |
+--------------------------------+-----------------------------------------------------+-------+----------------+--------------+---------------+--------+
| ses_0150f217cffeGq9tAm4scX8k63 | Complete remaining tasks from session               | build | samagama       | 180068861    | 1035057       | 0.0    |
| ses_048e8fca1ffeEzXkR2H7LmiSMN | MS Graph 733-table battery re-run                   | build | samagama       | 67834918     | 1633900       | 0.0    |
| ses_000c159f2ffeOoNzxoUMIfpfD3 | Undo changes and align to git                       | build | samagama       | 62123231     | 417843        | 0.0    |
+--------------------------------+-----------------------------------------------------+-------+----------------+--------------+---------------+--------+
```

```sql
SELECT count(*) AS sessions, sum(cost) AS total_cost, sum(tokens_input) AS total_in_tokens
FROM opencode.sessions;
```

```text
+----------+-------------------+-----------------+
| sessions | total_cost        | total_in_tokens |
+----------+-------------------+-----------------+
| 459      | 623.2679184999996 | 1439697280      |
+----------+-------------------+-----------------+
```

```sql
SELECT model_provider, count(*) AS sessions, sum(cost) AS total_cost
FROM opencode.sessions
GROUP BY model_provider
ORDER BY total_cost DESC;
```

```text
+----------------+----------+-------------------+
| model_provider | sessions | total_cost        |
+----------------+----------+-------------------+
| samagama       | 459      | 623.2679184999996 |
+----------------+----------+-------------------+
```

```sql
SELECT session_id, content, status, position
FROM opencode.todos
LIMIT 3;
```

```text
+--------------------------------+---------------------------------------------------------------------------------------------------+-----------+
| session_id                     | content                                                                                           | status    |
+--------------------------------+---------------------------------------------------------------------------------------------------+-----------+
| ses_000c159f2ffeOoNzxoUMIfpfD3 | C1: Fix broken Show-first-20 toggle (line 639 calls setShowAllSessions(true))                     | completed |
| ses_000c159f2ffeOoNzxoUMIfpfD3 | C2: Fix InlineConversationHint lying — text says 'appears below' but modal opens                  | completed |
| ses_000c159f2ffeOoNzxoUMIfpfD3 | C3: Fix HeatmapSection period subtitle using capped recentSessions instead of full heatmap totals | completed |
+--------------------------------+---------------------------------------------------------------------------------------------------+-----------+
```

### Catalog introspection

```sql
SELECT table_name FROM coral.tables WHERE schema_name = 'opencode' ORDER BY table_name;
```

```text
+---------------------+
| table_name          |
+---------------------+
| messages            |
| parts               |
| project_directories |
| projects            |
| session_inputs      |
| session_messages    |
| session_shares      |
| sessions            |
| todos               |
| workspaces          |
+---------------------+
```