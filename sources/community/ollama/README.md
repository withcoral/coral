# Ollama community source

Query a local Ollama API through Coral SQL. This source exposes model
inventory, running model state, server version, model metadata, and bounded
non-streaming generate/single-turn chat smoke checks.

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 6
**Default base URL:** `http://localhost:11434/api`

## Provider docs

- API introduction: https://docs.ollama.com/api/introduction
- Authentication: https://docs.ollama.com/api/authentication
- Generate: https://docs.ollama.com/api/generate
- Chat: https://docs.ollama.com/api/chat
- Embed: https://docs.ollama.com/api/embed
- List models: https://docs.ollama.com/api/tags
- Running models: https://docs.ollama.com/api/ps
- Show model details: https://docs.ollama.com/api-reference/show-model-details
- Version: https://docs.ollama.com/api-reference/get-version

## Installation

Start Ollama first, then add the source:

```bash
coral source add --file sources/community/ollama/manifest.yaml
```

The source uses `OLLAMA_BASE_URL` as a non-secret variable. If unset, it defaults
to local Ollama:

```bash
export OLLAMA_BASE_URL="http://localhost:11434/api"
coral source add --file sources/community/ollama/manifest.yaml
```

This first version targets local or self-hosted Ollama APIs that do not require
an API key. Direct `https://ollama.com/api` access requires `Authorization:
Bearer ...` authentication and is intentionally out of scope for this source
version.

If Coral runs inside WSL while Ollama runs on Windows, `localhost` points at
WSL instead of the Windows host. Use the Windows host or WSL gateway address for
`OLLAMA_BASE_URL` in that setup.

## Tables

| Table | Description | Required filters |
| --- | --- | --- |
| `ollama.version` | Ollama server version from `GET /version`. | None |
| `ollama.models` | Locally available models from `GET /tags`. | None |
| `ollama.running_models` | Models currently loaded in memory from `GET /ps`. | None |
| `ollama.model_details` | Metadata for one model from `POST /show`. | `model` |
| `ollama.generate` | One bounded non-streaming generation request from `POST /generate`. | `model`, `prompt`, `num_predict` |
| `ollama.chat` | One bounded non-streaming single-turn chat request from `POST /chat`. | `model`, `prompt`, `num_predict` |

## Example queries

List installed models:

```sql
SELECT name, model, size, family, parameter_size, quantization_level
FROM ollama.models
LIMIT 10;
```

Check running models:

```sql
SELECT name, size_vram, expires_at, context_length
FROM ollama.running_models
LIMIT 10;
```

Inspect one model:

```sql
SELECT model, family, parameter_size, quantization_level, capabilities
FROM ollama.model_details
WHERE model = '<installed-model>';
```

Run a bounded generate query:

```sql
SELECT response, done_reason, eval_count, num_predict
FROM ollama.generate
WHERE model = '<installed-model>'
  AND prompt = 'Reply with exactly: Coral Ollama works'
  AND num_predict = 12
LIMIT 1;
```

Run a bounded single-turn chat query:

```sql
SELECT content, done_reason, eval_count, num_predict
FROM ollama.chat
WHERE model = '<installed-model>'
  AND prompt = 'What is Python? Reply in one short line.'
  AND num_predict = 20
LIMIT 1;
```

## Validation

Run these checks with Ollama running:

```bash
coral source lint sources/community/ollama/manifest.yaml
coral source add --file sources/community/ollama/manifest.yaml
coral source test ollama
```

The declared tests are model-independent:

```sql
SELECT version FROM ollama.version LIMIT 1;

SELECT name, model, size
FROM ollama.models
LIMIT 5;
```

For live model proof in a PR, include sanitized output for the commands above
plus at least one representative model query and one bounded generate or chat
query against a model installed on the test machine.

### Live validation output

The following output was captured against Ollama `0.23.2` with Coral pointed at
a running local Ollama API. The test machine used
`qwen2.5-coder:1.5b-base` for bounded generate/chat proof because it was small
enough to run in the available memory.

```text
$ coral source lint sources/community/ollama/manifest.yaml
Manifest is valid
```

```text
$ coral source add --file sources/community/ollama/manifest.yaml
Added source ollama

  ✓ ollama connected successfully

    ollama (6 tables)
    ├─ chat
    ├─ generate
    ├─ model_details
    ├─ models
    ├─ running_models
    └─ version
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT version FROM ollama.version LIMIT 1
      1 row

    ✓ SELECT name, model, size FROM ollama.models LIMIT 5
      5 rows
```

```text
$ coral source test ollama
ollama

  ✓ ollama connected successfully

    ollama (6 tables)
    ├─ chat
    ├─ generate
    ├─ model_details
    ├─ models
    ├─ running_models
    └─ version
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT version FROM ollama.version LIMIT 1
      1 row

    ✓ SELECT name, model, size FROM ollama.models LIMIT 5
      5 rows
```

```sql
SELECT version FROM ollama.version LIMIT 1;
```

```text
+---------+
| version |
+---------+
| 0.23.2  |
+---------+
```

```sql
SELECT name, model, size, family, parameter_size, quantization_level
FROM ollama.models
LIMIT 5;
```

```text
+-------------------------+-------------------------+------------+------------+----------------+--------------------+
| name                    | model                   | size       | family     | parameter_size | quantization_level |
+-------------------------+-------------------------+------------+------------+----------------+--------------------+
| nomic-embed-text:latest | nomic-embed-text:latest | 274302450  | nomic-bert | 137M           | F16                |
| qwen2.5-coder:1.5b-base | qwen2.5-coder:1.5b-base | 986060385  | qwen2      | 1.5B           | Q4_K_M             |
| llama3.1:8b             | llama3.1:8b             | 4920753328 | llama      | 8.0B           | Q4_K_M             |
| minimax-m2.7:cloud      | minimax-m2.7:cloud      | 375        | minimax    |                |                    |
| qwen2.5-coder:7b        | qwen2.5-coder:7b        | 4683087561 | qwen2      | 7.6B           | Q4_K_M             |
+-------------------------+-------------------------+------------+------------+----------------+--------------------+
```

```sql
SELECT model, family, parameter_size, quantization_level, capabilities
FROM ollama.model_details
WHERE model = 'qwen2.5-coder:1.5b-base'
LIMIT 1;
```

```text
+-------------------------+--------+----------------+--------------------+--------------------+
| model                   | family | parameter_size | quantization_level | capabilities       |
+-------------------------+--------+----------------+--------------------+--------------------+
| qwen2.5-coder:1.5b-base | qwen2  | 1.5B           | Q4_K_M             | completion, insert |
+-------------------------+--------+----------------+--------------------+--------------------+
```

```sql
SELECT response, done_reason, eval_count, num_predict
FROM ollama.generate
WHERE model = 'qwen2.5-coder:1.5b-base'
  AND prompt = 'Reply with exactly: Coral Ollama works'
  AND num_predict = 12
LIMIT 1;
```

```text
+-----------------------------------------------------------------------------+-------------+------------+-------------+
| response                                                                    | done_reason | eval_count | num_predict |
+-----------------------------------------------------------------------------+-------------+------------+-------------+
|  for a multinational corporation that focuses on the production and sale of | length      | 12         | 12          |
+-----------------------------------------------------------------------------+-------------+------------+-------------+
```

```sql
SELECT content, done_reason, eval_count, num_predict
FROM ollama.chat
WHERE model = 'qwen2.5-coder:1.5b-base'
  AND prompt = 'What is Python? Reply in one short line.'
  AND num_predict = 20
LIMIT 1;
```

```text
+------------------------------------------------------------------+-------------+------------+-------------+
| content                                                          | done_reason | eval_count | num_predict |
+------------------------------------------------------------------+-------------+------------+-------------+
|  Python is an interpreted, object-oriented programming language. | stop        | 11         | 20          |
+------------------------------------------------------------------+-------------+------------+-------------+
```

## Implementation notes

- Uses Coral source-spec DSL v3 with the HTTP backend.
- Uses `OLLAMA_BASE_URL` as a configurable variable.
- Does not require authentication for local Ollama.
- Sets `stream = false` for `generate` and `chat` so Coral receives one JSON
  response instead of a streaming response.
- Requires `num_predict` on `generate` and `chat`, then sends it as
  `options.num_predict` so live inference queries are bounded.
- Models `chat` as a single-turn user prompt. Chat history, tool calls, and
  image-message inputs are not exposed in this first version.
- Does not include create, copy, pull, push, delete, or embeddings operations.

## Limitations

- `generate` and `chat` perform live inference calls and depend on local model
  availability, machine resources, and model load time.
- `model_details`, `generate`, and `chat` require a model name that exists on
  the configured Ollama server.
- The source is read/query oriented. It intentionally excludes model management
  endpoints that mutate local state or download/delete models.
- The non-mutating `POST /embed` embeddings endpoint is intentionally omitted
  from this first version.
- Direct `https://ollama.com/api` access is not supported yet because Ollama
  requires an API key and Authorization header for that mode.
- Localhost access from WSL, Docker, or remote machines may require a different
  `OLLAMA_BASE_URL`, such as a LAN IP or `host.docker.internal`.
