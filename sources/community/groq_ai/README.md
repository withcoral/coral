# Groq AI community source

Query GroqCloud model metadata and run simple chat completions through Coral SQL.
This source adds Groq's OpenAI-compatible API to the community catalog so users
and agents can inspect available models, verify model configuration, and smoke
test prompts without leaving the Coral workflow.

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 3
**Base URL:** `https://api.groq.com/openai/v1`

## Why this source

Groq is a common inference provider for fast LLM experiments, agent prototypes,
and production chat workloads. Coral did not have a Groq source yet, so this
community spec gives the reef a focused read/query surface for:

- Discovering active GroqCloud models from SQL.
- Looking up metadata for one model before using it in an agent or workflow.
- Running a bounded chat-completion prompt as an integration smoke test.
- Joining model metadata with other Coral sources in local analysis workflows.

The v1 surface is intentionally narrow and read-oriented. It proves Coral can
authenticate against Groq, call Groq's OpenAI-compatible endpoints, map JSON
responses into tables, and validate the source with declared test queries.

## Installation

Community sources are not bundled with the Coral binary. Clone the Coral
repository and add the manifest from this directory:

```bash
coral source add --file sources/community/groq_ai/manifest.yaml
```

You can also copy `manifest.yaml` into another workspace and pass that path to
`coral source add --file`.

## Authentication

Create or copy an API key from the GroqCloud console:

https://console.groq.com/keys

Set the key as `GROQ_API_KEY` before adding or testing the source. Coral sends
it as a bearer token to Groq's OpenAI-compatible API.

```bash
export GROQ_API_KEY="your_groq_api_key"
coral source add --file sources/community/groq_ai/manifest.yaml
```

Interactive install also works:

```bash
coral source add --interactive --file sources/community/groq_ai/manifest.yaml
```

## Provider docs

- Groq API reference: https://console.groq.com/docs/api-reference
- Groq models: https://console.groq.com/docs/models
- Groq text/chat guide: https://console.groq.com/docs/text-chat
- Groq rate limits: https://console.groq.com/docs/rate-limits
- Groq model permissions: https://console.groq.com/docs/model-permissions

## Tables

| Table | Description | Required filters |
| --- | --- | --- |
| `groq_ai.models` | Active GroqCloud models returned by the Models API. | None |
| `groq_ai.model` | Metadata for one Groq model ID. | `model_id` |
| `groq_ai.chat_completions` | Run one chat completion request using SQL filters. | `model`, `prompt` |

### `groq_ai.models`

Lists models available from `GET /models`.

```sql
SELECT id, object, owned_by, active, context_window, max_completion_tokens
FROM groq_ai.models
LIMIT 20;
```

### `groq_ai.model`

Fetches metadata for one model from `GET /models/{model_id}`. Slash-containing
model IDs such as `groq/compound-mini` are supported by Coral's HTTP request
builder and are covered by the validation output below.

```sql
SELECT id, object, owned_by, active, context_window, max_completion_tokens
FROM groq_ai.model
WHERE model_id = 'groq/compound-mini';
```

### `groq_ai.chat_completions`

Runs a single user-message chat completion through `POST /chat/completions`.
Use `max_completion_tokens` when you want to keep validation output small.

```sql
SELECT content, finish_reason, max_completion_tokens
FROM groq_ai.chat_completions
WHERE model = 'llama-3.3-70b-versatile'
  AND prompt = 'What is Python? Reply in one short line under 15 words.'
  AND max_completion_tokens = 40
LIMIT 1;
```

## Validation

Run the source-level checks with a valid `GROQ_API_KEY` before opening or
updating a PR. The API key is required for `source add`, `source test`, and live
SQL queries, but it should never be printed or committed.

```bash
coral source lint sources/community/groq_ai/manifest.yaml

export GROQ_API_KEY="your_groq_api_key"
coral source add --file sources/community/groq_ai/manifest.yaml
coral source test groq_ai
```

The declared test queries cover model discovery, two chat-completion smoke
tests, and a detail lookup for a slash-containing model ID:

```sql
SELECT * FROM groq_ai.models LIMIT 5;

SELECT content
FROM groq_ai.chat_completions
WHERE model = 'llama-3.3-70b-versatile'
  AND prompt = 'Reply with exactly: Coral Groq works'
LIMIT 1;

SELECT content
FROM groq_ai.chat_completions
WHERE model = 'llama-3.3-70b-versatile'
  AND prompt = 'What is Python?'
LIMIT 1;

SELECT id, owned_by, active
FROM groq_ai.model
WHERE model_id = 'groq/compound-mini'
LIMIT 1;
```

### Live validation output

The following output was captured from a live validation run using a real
GroqCloud API key.

#### Manifest lint

Command:

```bash
coral source lint sources/community/groq_ai/manifest.yaml
```

Output:

```text
Manifest is valid
```

#### Add source and run declared tests

Command:

```bash
coral source add --file sources/community/groq_ai/manifest.yaml
```

Output:

```text
Added source groq_ai

  PASS groq_ai connected successfully

    groq_ai (3 tables)
    - chat_completions
    - model
    - models
    Query tests
    4 declared - 4 passed - 0 failed

    PASS SELECT * FROM groq_ai.models LIMIT 5
      5 rows

    PASS SELECT content FROM groq_ai.chat_completions WHERE model = 'llama-3.3-70b-versatile' AND prompt = 'Reply with exactly: Coral Groq works' LIMIT 1
      1 row

    PASS SELECT content FROM groq_ai.chat_completions WHERE model = 'llama-3.3-70b-versatile' AND prompt = 'What is Python?' LIMIT 1
      1 row

    PASS SELECT id, owned_by, active FROM groq_ai.model WHERE model_id = 'groq/compound-mini' LIMIT 1
      1 row
```

#### Re-run source tests

Command:

```bash
coral source test groq_ai
```

Output:

```text
  PASS groq_ai connected successfully

    groq_ai (3 tables)
    - chat_completions
    - model
    - models
    Query tests
    4 declared - 4 passed - 0 failed

    PASS SELECT * FROM groq_ai.models LIMIT 5
      5 rows

    PASS SELECT content FROM groq_ai.chat_completions WHERE model = 'llama-3.3-70b-versatile' AND prompt = 'Reply with exactly: Coral Groq works' LIMIT 1
      1 row

    PASS SELECT content FROM groq_ai.chat_completions WHERE model = 'llama-3.3-70b-versatile' AND prompt = 'What is Python?' LIMIT 1
      1 row

    PASS SELECT id, owned_by, active FROM groq_ai.model WHERE model_id = 'groq/compound-mini' LIMIT 1
      1 row
```

#### Confirm table discovery

Command:

```bash
coral sql "SELECT table_name FROM coral.tables WHERE schema_name = 'groq_ai' ORDER BY table_name"
```

Output:

```text
+------------------+
| table_name       |
+------------------+
| chat_completions |
| model            |
| models           |
+------------------+
```

#### Confirm column discovery

Command:

```bash
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'groq_ai' ORDER BY table_name, ordinal_position"
```

Output:

```text
+------------------+-----------------------+-----------+
| table_name       | column_name           | data_type |
+------------------+-----------------------+-----------+
| chat_completions | model                 | Utf8      |
| chat_completions | prompt                | Utf8      |
| chat_completions | max_completion_tokens | Int64     |
| chat_completions | index                 | Int64     |
| chat_completions | finish_reason         | Utf8      |
| chat_completions | content               | Utf8      |
| chat_completions | message_role          | Utf8      |
| model            | model_id              | Utf8      |
| model            | id                    | Utf8      |
| model            | object                | Utf8      |
| model            | owned_by              | Utf8      |
| model            | active                | Boolean   |
| model            | context_window        | Int64     |
| model            | max_completion_tokens | Int64     |
| model            | public_apps           | Json      |
| models           | id                    | Utf8      |
| models           | object                | Utf8      |
| models           | owned_by              | Utf8      |
| models           | active                | Boolean   |
| models           | context_window        | Int64     |
| models           | max_completion_tokens | Int64     |
| models           | public_apps           | Json      |
+------------------+-----------------------+-----------+
```

#### Confirm input discovery

Command:

```bash
coral sql "SELECT key, kind, required FROM coral.inputs WHERE schema_name = 'groq_ai' ORDER BY key"
```

Output:

```text
+--------------+--------+----------+
| key          | kind   | required |
+--------------+--------+----------+
| GROQ_API_KEY | secret | true     |
+--------------+--------+----------+
```

#### Run a live chat completion query

Command:

```bash
coral sql "SELECT content, max_completion_tokens FROM groq_ai.chat_completions WHERE model = 'llama-3.3-70b-versatile' AND prompt = 'What is Python? Reply in one short line under 15 words.' AND max_completion_tokens = 40 LIMIT 1"
```

Output:

```text
+----------------------------------------------+-----------------------+
| content                                      | max_completion_tokens |
+----------------------------------------------+-----------------------+
| Python is a high-level programming language. | 40                    |
+----------------------------------------------+-----------------------+
```

#### Query Groq model metadata

Command:

```bash
coral sql "SELECT id, object, owned_by FROM groq_ai.models LIMIT 5"
```

Output:

```text
+-------------------------------------------+--------+-------------+
| id                                        | object | owned_by    |
+-------------------------------------------+--------+-------------+
| canopylabs/orpheus-arabic-saudi           | model  | Canopy Labs |
| meta-llama/llama-4-scout-17b-16e-instruct | model  | Meta        |
| whisper-large-v3                          | model  | OpenAI      |
| meta-llama/llama-prompt-guard-2-22m       | model  | Meta        |
| groq/compound-mini                        | model  | Groq        |
+-------------------------------------------+--------+-------------+
```

#### Query one slash-containing model ID

Command:

```bash
coral sql "SELECT id, object, owned_by, active, context_window, max_completion_tokens FROM groq_ai.model WHERE model_id = 'groq/compound-mini' LIMIT 1"
```

Output:

```text
+--------------------+--------+----------+--------+----------------+-----------------------+
| id                 | object | owned_by | active | context_window | max_completion_tokens |
+--------------------+--------+----------+--------+----------------+-----------------------+
| groq/compound-mini | model  | Groq     | true   | 131072         | 8192                  |
+--------------------+--------+----------+--------+----------------+-----------------------+
```

## Implementation notes

- Uses Coral source-spec DSL v3 with the HTTP backend.
- Uses `HeaderAuth` with `Authorization: Bearer {{input.GROQ_API_KEY}}`.
- Maps Groq's `data` array from `GET /models` into `groq_ai.models`.
- Maps Groq model metadata, including `public_apps` and
  `max_completion_tokens`, onto `groq_ai.models` and `groq_ai.model`.
- Maps `choices[*].message.content` from `POST /chat/completions` into
  `groq_ai.chat_completions.content`.
- Sends the current Groq chat parameter `max_completion_tokens`.
- Echoes required SQL filters such as `model`, `prompt`, and `model_id` back as
  virtual columns so query results keep their request context.
- Does not require runtime, CLI, MCP, or UI changes.

## Limitations

- This source is read/query oriented and does not manage Groq account settings.
- `chat_completions` performs a live API call for each query.
- The chat table supports one user message per query. It is intended for
  validation and lightweight SQL workflows, not as a full chat client.
- Responses, available models, permissions, rate limits, and errors depend on
  the Groq account, API key permissions, selected model, and current provider
  limits.

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md), keep the manifest focused,
and include the validation commands plus proof output in the PR description.
