# LM Studio community source

Query a local LM Studio server from Coral SQL using LM Studio's
OpenAI-compatible API. This source lists models visible to the local server,
runs bounded response and single-turn chat-completion smoke tests, and
generates embedding vectors from an embedding-capable model.

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 4
**Base URL:** `http://localhost:1234/v1`

## Why this source

LM Studio is a popular local model runtime for testing LLMs on a workstation.
Coral did not have an LM Studio source yet, so this community spec adds a
focused read/query surface for:

- Discovering models visible to the LM Studio OpenAI-compatible server.
- Running bounded response and single-turn chat-completion checks through SQL.
- Generating an embedding vector from an embedding-capable local model.
- Joining local model inventory with other Coral sources during agent or
  workflow debugging.

The v1 surface is intentionally narrow. It uses LM Studio's documented
OpenAI-compatible endpoints and avoids model-detail path lookups so model IDs
containing slashes remain safe.

## Installation

Start the LM Studio server from the Developer tab, then add the manifest:

```bash
coral source add --file sources/community/lm_studio/manifest.yaml
```

If your LM Studio server uses a different OpenAI-compatible base URL, set
`LM_STUDIO_BASE_URL` before adding the source:

```bash
export LM_STUDIO_BASE_URL="http://localhost:1234/v1"
coral source add --file sources/community/lm_studio/manifest.yaml
```

Interactive install also works:

```bash
coral source add --interactive --file sources/community/lm_studio/manifest.yaml
```

## Authentication

This first version targets the default local LM Studio server shape. LM Studio
does not require API authentication by default. If the server is configured to
require API tokens, token-authenticated requests are out of scope for this
initial source.

## Provider Docs

- LM Studio local server: https://lmstudio.ai/docs/developer/core/server
- LM Studio API quickstart: https://lmstudio.ai/docs/developer/rest/quickstart
- LM Studio authentication: https://lmstudio.ai/docs/developer/core/authentication
- OpenAI-compatible list models: https://lmstudio.ai/docs/developer/openai-compat/models
- OpenAI-compatible responses: https://lmstudio.ai/docs/developer/openai-compat/responses
- OpenAI-compatible chat completions: https://lmstudio.ai/docs/developer/openai-compat/chat-completions
- OpenAI-compatible embeddings: https://lmstudio.ai/docs/developer/openai-compat/embeddings
- OpenAI-compatible completions: https://lmstudio.ai/docs/developer/openai-compat/completions

## Tables

| Table | Description | Required filters |
| --- | --- | --- |
| `lm_studio.models` | Models visible to the OpenAI-compatible server. | None |
| `lm_studio.responses` | Run one non-streaming Responses API request. | `model`, `input`, `max_output_tokens` |
| `lm_studio.chat_completions` | Run one non-streaming single-turn chat completion. | `model`, `prompt`, `max_tokens` |
| `lm_studio.embeddings` | Generate one embedding vector from input text. | `model`, `input` |

### `lm_studio.models`

Lists models returned by `GET /models`.

```sql
SELECT id, object, owned_by
FROM lm_studio.models
LIMIT 20;
```

### `lm_studio.responses`

Runs a non-streaming request through `POST /responses`. Always pass a positive
`max_output_tokens` value so the request is bounded. The full `output` field is
JSON because LM Studio may return message items, reasoning items, tool events,
or other Responses API output shapes depending on model and request.

```sql
SELECT status, max_output_tokens, output
FROM lm_studio.responses
WHERE model = 'model-identifier'
  AND input = 'Reply with exactly: Coral Responses works'
  AND max_output_tokens = 20
LIMIT 1;
```

### `lm_studio.chat_completions`

Runs a single user-message chat completion through `POST /chat/completions`.
Always pass a positive `max_tokens` value so the request is bounded.

```sql
SELECT content, reasoning_content, finish_reason, max_tokens
FROM lm_studio.chat_completions
WHERE model = 'model-identifier'
  AND prompt = 'Reply with exactly: Coral LM Studio works'
  AND max_tokens = 20
LIMIT 1;
```

This table is single-turn only. It does not expose chat history, image messages,
tool calls, or structured-output payloads in this first version.
Reasoning models may return early thinking text in `reasoning_content` before
answer text appears in `content`.

### `lm_studio.embeddings`

Generates an embedding vector through `POST /embeddings`. Use an
embedding-capable model loaded or visible in LM Studio. Select `embedding` when
you need the full vector; validation examples show a short vector preview so
terminal output stays readable.

```sql
SELECT model, index, substr(CAST(embedding AS VARCHAR), 1, 80) AS embedding_preview
FROM lm_studio.embeddings
WHERE model = 'embedding-model-identifier'
  AND input = 'Coral source validation'
LIMIT 1;
```

## Validation

Run the source-level checks after starting the LM Studio server:

```bash
coral source lint sources/community/lm_studio/manifest.yaml

export LM_STUDIO_BASE_URL="http://localhost:1234/v1"
coral source add --file sources/community/lm_studio/manifest.yaml
coral source test lm_studio
```

The declared test query covers model discovery:

```sql
SELECT id, object, owned_by
FROM lm_studio.models
LIMIT 5;
```

Before opening a PR, also capture live output for one chat-completion query and
one embedding query against real local models. A Responses API smoke test is
also useful when the loaded model supports it.

### Live validation output

The following output was captured against a local LM Studio server with a
chat-capable model and an embedding-capable model loaded.

#### Manifest lint

Command:

```bash
coral source lint sources/community/lm_studio/manifest.yaml
```

Output:

```text
Manifest is valid
```

#### Add source and run declared tests

Command:

```bash
coral source add --file sources/community/lm_studio/manifest.yaml
```

Output:

```text
Added source lm_studio

  PASS lm_studio connected successfully

    lm_studio (4 tables)
    - chat_completions
    - embeddings
    - models
    - responses
    Query tests
    1 declared - 1 passed - 0 failed

    PASS SELECT id, object, owned_by FROM lm_studio.models LIMIT 5
      4 rows
```

#### Re-run source tests

Command:

```bash
coral source test lm_studio
```

Output:

```text
  PASS lm_studio connected successfully

    lm_studio (4 tables)
    - chat_completions
    - embeddings
    - models
    - responses
    Query tests
    1 declared - 1 passed - 0 failed

    PASS SELECT id, object, owned_by FROM lm_studio.models LIMIT 5
      4 rows
```

#### Model inventory query

Command:

```bash
coral sql "SELECT id, object, owned_by FROM lm_studio.models LIMIT 5"
```

Output:

```text
+----------------------------------------+--------+--------------------+
| id                                     | object | owned_by           |
+----------------------------------------+--------+--------------------+
| text-embedding-nomic-embed-text-v1.5   | model  | organization_owner |
| google/gemma-4-e4b                     | model  | organization_owner |
| text-embedding-nomic-embed-text-v1.5:2 | model  | organization_owner |
| google/gemma-4-e4b:2                   | model  | organization_owner |
+----------------------------------------+--------+--------------------+
```

#### Bounded chat-completion query

Command:

```bash
coral sql "SELECT content, reasoning_content, finish_reason, max_tokens FROM lm_studio.chat_completions WHERE model = 'google/gemma-4-e4b' AND prompt = 'Reply with exactly: Coral LM Studio works' AND max_tokens = 20 LIMIT 1"
```

Output:

```text
+-----------------------+-------------------+---------------+------------+
| content               | reasoning_content | finish_reason | max_tokens |
+-----------------------+-------------------+---------------+------------+
| Coral LM Studio works |                   | stop          | 20         |
+-----------------------+-------------------+---------------+------------+
```

#### Bounded Responses API query

Command:

```bash
coral sql "SELECT status, max_output_tokens FROM lm_studio.responses WHERE model = 'google/gemma-4-e4b' AND input = 'Reply with exactly: Coral Responses works' AND max_output_tokens = 20 LIMIT 1"
```

Output:

```text
+-----------+-------------------+
| status    | max_output_tokens |
+-----------+-------------------+
| completed | 20                |
+-----------+-------------------+
```

#### Embedding query

Command:

```bash
coral sql "SELECT model, index, substr(CAST(embedding AS VARCHAR), 1, 80) AS embedding_preview FROM lm_studio.embeddings WHERE model = 'text-embedding-nomic-embed-text-v1.5' AND input = 'Coral LM Studio source validation' LIMIT 1"
```

Output:

```text
+--------------------------------------+-------+----------------------------------------------------------------------------------+
| model                                | index | embedding_preview                                                                |
+--------------------------------------+-------+----------------------------------------------------------------------------------+
| text-embedding-nomic-embed-text-v1.5 | 0     | [0.01817111112177372,-0.007743862923234701,-0.1390269100666046,-0.05625997856259 |
+--------------------------------------+-------+----------------------------------------------------------------------------------+
```

## Scope and Limitations

- Targets LM Studio's OpenAI-compatible `/v1` endpoints.
- Uses the default unauthenticated local server contract.
- Does not include token-authenticated server mode in this first version.
- Does not include the legacy `/v1/completions` endpoint.
- Does not include the native `/api/v1` stateful chat, model load, model unload,
  download, or MCP integration endpoints.
- `responses` performs one live API call for each query.
- `chat_completions` performs one live model call for each query.
- `chat_completions` is single-turn and non-streaming.
- `embeddings` requires an embedding-capable model.
- Model availability depends on the LM Studio server configuration and loaded
  or visible local models.
