# NVIDIA NIM Coral Source

## Summary

This source lets Coral query NVIDIA's hosted NIM API for available models and
run bounded synchronous OpenAI-compatible chat-completion checks through SQL.

## Authentication

Create or copy an API key from NVIDIA build:

https://build.nvidia.com/settings/api-keys

Set the key as `NVIDIA_API_KEY` before adding or testing the source:

```bash
export NVIDIA_API_KEY="your_nvidia_api_key"
coral source add --file sources/community/nvidia_nim/manifest.yaml
```

The key is sent as an `Authorization: Bearer ...` header to NVIDIA's hosted NIM
API at `https://integrate.api.nvidia.com/v1`.

## Live request costs

`nvidia_nim.models` is a metadata read. `nvidia_nim.chat_completions` performs
live NVIDIA NIM API calls whenever selected, so it can consume NVIDIA API
quota, credits, or rate limits. Keep validation queries bounded and small.

## Provider docs

- NVIDIA API Catalog quickstart: https://docs.api.nvidia.com/nim/docs/api-quickstart
- NVIDIA hosted NIM LLM APIs: https://docs.api.nvidia.com/nim/reference/llm-apis
- NVIDIA NIM LLM API reference: https://docs.nvidia.com/nim/large-language-models/latest/reference/api-reference.html
- NVIDIA retrieval and embedding APIs: https://docs.api.nvidia.com/nim/reference/retrieval-apis
- NVIDIA API keys: https://build.nvidia.com/settings/api-keys

## Tables

| Table | Description | Required filters |
| --- | --- | --- |
| `nvidia_nim.models` | Model catalog from `GET /models`. | None |
| `nvidia_nim.chat_completions` | Run one bounded synchronous non-streaming chat completion. | `model`, `prompt`, `max_tokens` |

### `nvidia_nim.models`

Lists models returned by NVIDIA's hosted NIM OpenAI-compatible `GET /models`
endpoint.

```sql
SELECT id, object, owned_by
FROM nvidia_nim.models
LIMIT 10;
```

### `nvidia_nim.chat_completions`

Runs a single user-message chat completion through `POST /chat/completions`
for synchronous OpenAI-compatible NVIDIA NIM models. Always pass a positive
`max_tokens` value so the request is bounded. NVIDIA rejects non-positive
`max_tokens` values.

```sql
SELECT content, finish_reason, max_tokens, returned_model, total_tokens
FROM nvidia_nim.chat_completions
WHERE model = 'meta/llama-3.1-8b-instruct'
  AND prompt = 'Reply with exactly: Coral NVIDIA works.'
  AND max_tokens = 20
LIMIT 1;
```

This table is single-turn only. It preserves top-level response metadata such
as response ID, returned model, raw `choices`, `usage`, token counts, and
NVIDIA `nvext` timing metadata when returned. It does not expose chat history,
tool calls, structured-output payloads, multimodal message payloads, or
streaming in this first version.

## Validation

Run the source-level checks with a valid `NVIDIA_API_KEY` before opening or
updating a PR. The API key is required for `source add`, `source test`, and live
SQL queries, but it should never be printed or committed.

```bash
coral source lint sources/community/nvidia_nim/manifest.yaml

export NVIDIA_API_KEY="your_nvidia_api_key"
coral source add --file sources/community/nvidia_nim/manifest.yaml
coral source test nvidia_nim
```

The declared test query covers model discovery:

```sql
SELECT id, object, owned_by FROM nvidia_nim.models LIMIT 5;
```

Before opening a PR, also capture live output for one bounded chat-completion
query against a synchronous OpenAI-compatible chat model.

### Live validation output

The following output was captured against NVIDIA NIM with a valid API key.

#### Manifest lint

Command:

```bash
coral source lint sources/community/nvidia_nim/manifest.yaml
```

Output:

```text
Manifest is valid
```

#### Add source and run declared tests

Command:

```bash
coral source add --file sources/community/nvidia_nim/manifest.yaml
```

Output:

```text
Added source nvidia_nim

  PASS nvidia_nim connected successfully

    nvidia_nim (2 tables)
    - chat_completions
    - models
    Query tests
    1 declared - 1 passed - 0 failed

    PASS SELECT id, object, owned_by FROM nvidia_nim.models LIMIT 5
      5 rows
```

#### Re-run source tests

Command:

```bash
coral source test nvidia_nim
```

Output:

```text
  PASS nvidia_nim connected successfully

    nvidia_nim (2 tables)
    - chat_completions
    - models
    Query tests
    1 declared - 1 passed - 0 failed

    PASS SELECT id, object, owned_by FROM nvidia_nim.models LIMIT 5
      5 rows
```

#### Model inventory query

Command:

```bash
coral sql "SELECT id, object, owned_by FROM nvidia_nim.models LIMIT 10"
```

Output:

```text
+------------------------------------------+--------+-------------+
| id                                       | object | owned_by    |
+------------------------------------------+--------+-------------+
| 01-ai/yi-large                           | model  | 01-ai       |
| abacusai/dracarys-llama-3.1-70b-instruct | model  | abacusai    |
| adept/fuyu-8b                            | model  | adept       |
| ai21labs/jamba-1.5-large-instruct        | model  | ai21labs    |
| aisingapore/sea-lion-7b-instruct         | model  | aisingapore |
| baai/bge-m3                              | model  | baai        |
| bigcode/starcoder2-15b                   | model  | bigcode     |
| bytedance/seed-oss-36b-instruct          | model  | bytedance   |
| databricks/dbrx-instruct                 | model  | databricks  |
| deepseek-ai/deepseek-coder-6.7b-instruct | model  | deepseek-ai |
+------------------------------------------+--------+-------------+
```

#### Bounded chat-completion query

Command:

```bash
coral sql "SELECT content, finish_reason, max_tokens, returned_model, total_tokens FROM nvidia_nim.chat_completions WHERE model = 'meta/llama-3.1-8b-instruct' AND prompt = 'Reply with exactly: Coral NVIDIA works.' AND max_tokens = 20 LIMIT 1"
```

Output:

```text
+---------------------+---------------+------------+----------------------------+--------------+
| content             | finish_reason | max_tokens | returned_model             | total_tokens |
+---------------------+---------------+------------+----------------------------+--------------+
| Coral NVIDIA works. | stop          | 20         | meta/llama-3.1-8b-instruct | 49           |
+---------------------+---------------+------------+----------------------------+--------------+
```

#### Chat metadata query

Command:

```bash
coral sql "SELECT id, object, created, prompt_tokens, completion_tokens, total_tokens FROM nvidia_nim.chat_completions WHERE model = 'meta/llama-3.1-8b-instruct' AND prompt = 'Reply with exactly: Coral NVIDIA works.' AND max_tokens = 20 LIMIT 1"
```

Output:

```text
+-----------------------------------------------+-----------------+------------+---------------+-------------------+--------------+
| id                                            | object          | created    | prompt_tokens | completion_tokens | total_tokens |
+-----------------------------------------------+-----------------+------------+---------------+-------------------+--------------+
| chatcmpl-00cc3dd3-da23-46a3-b6bc-9e1f31d5a2d3 | chat.completion | 1779980907 | 43            | 6                 | 49           |
+-----------------------------------------------+-----------------+------------+---------------+-------------------+--------------+
```

## Scope and limitations

- Targets NVIDIA's hosted NIM API at `https://integrate.api.nvidia.com/v1`.
- Requires `NVIDIA_API_KEY` bearer authentication.
- `chat_completions` uses required positive `max_tokens`; NVIDIA rejects
  non-positive values.
- `chat_completions` is single-turn, synchronous, and non-streaming.
- `chat_completions` is a live execution table and may consume NVIDIA API
  quota, credits, or rate limits.
- Asynchronous model-specific endpoints that return HTTP 202 with `requestId`
  and require `/v1/status/{requestId}` polling are out of scope because Coral
  source specs do not currently model provider polling flows.
- Embeddings are out of scope for this first version because NVIDIA embedding
  request bodies are model-specific. For example, some asymmetric embedding
  models require `input_type`, while other documented embedding endpoints use
  different request fields.
- Does not expose streaming, tool calls, structured outputs, reranking,
  embeddings, multimodal/image/video/audio model payloads, local/self-hosted
  NIM management endpoints, metrics, license, manifest, health, tokenize,
  detokenize, or Responses API in this first version.
- Does not include model-detail path lookups, avoiding model-ID path issues for
  IDs that contain `/`.
