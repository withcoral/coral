# OpenRouter community source

Query OpenRouter from Coral SQL. This source lists model catalogs, checks API
key usage and limits, runs bounded non-streaming chat completions, and generates
embedding vectors from embedding-capable models.

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 5
**Base URL:** `https://openrouter.ai/api/v1`

## Why this source

OpenRouter is a popular model gateway for routing requests across many LLM
providers with a single OpenAI-compatible API. Coral did not have an
OpenRouter source yet, so this community spec adds a focused SQL surface for:

- Discovering available OpenRouter chat/text models.
- Discovering embedding-capable OpenRouter models.
- Checking API key usage, remaining limits, and rate-limit metadata.
- Running bounded chat-completion smoke tests through SQL.
- Generating embedding vectors for local analysis or workflow checks.

The first version is intentionally narrow. It uses documented OpenRouter API
endpoints, requires positive token bounds for live chat calls, and avoids
model-detail path lookups so model IDs containing `/` remain safe.

## Installation

Community sources are not bundled with the Coral binary. Clone the Coral
repository and add the manifest from this directory:

```bash
coral source add --file sources/community/openrouter/manifest.yaml
```

## Authentication

Create or copy an API key from:

https://openrouter.ai/settings/keys

Set the key as `OPENROUTER_API_KEY` before adding or testing the source. Coral
sends it as a bearer token to OpenRouter.

```bash
export OPENROUTER_API_KEY="your_openrouter_api_key"
coral source add --file sources/community/openrouter/manifest.yaml
```

Interactive install also works:

```bash
coral source add --interactive --file sources/community/openrouter/manifest.yaml
```

## Live request costs

`openrouter.models`, `openrouter.embedding_models`, and `openrouter.key` are
metadata reads. `openrouter.chat_completions` and `openrouter.embeddings`
perform live OpenRouter API calls whenever selected, so they can consume
OpenRouter credits, model-provider quota, and rate limits. Keep validation
queries bounded and small.

## Provider docs

- OpenRouter API reference: https://openrouter.ai/docs/api-reference/overview
- Authentication: https://openrouter.ai/docs/api-reference/authentication
- List models: https://openrouter.ai/docs/api-reference/list-available-models
- Get current key: https://openrouter.ai/docs/api-reference/get-current-api-key
- Chat completions: https://openrouter.ai/docs/api-reference/chat-completion
- Embeddings: https://openrouter.ai/docs/api-reference/create-embedding
- List embedding models: https://openrouter.ai/docs/api-reference/list-embedding-models
- Rate limits and credits: https://openrouter.ai/docs/api-reference/limits

## Tables

| Table | Description | Required filters |
| --- | --- | --- |
| `openrouter.models` | Chat/text model catalog from `GET /models`. | None |
| `openrouter.embedding_models` | Embedding model catalog from `GET /embeddings/models`. | None |
| `openrouter.key` | Current API key usage, limits, and rate-limit metadata. | None |
| `openrouter.chat_completions` | Run one bounded non-streaming chat completion. | `model`, `prompt`, `max_tokens` |
| `openrouter.embeddings` | Generate one embedding vector. | `model`, `input` |

### `openrouter.models`

Lists models returned by `GET /models`.

```sql
SELECT id, name, context_length, architecture_modality, prompt_price, completion_price
FROM openrouter.models
LIMIT 20;
```

### `openrouter.embedding_models`

Lists embedding-capable models returned by `GET /embeddings/models`.

```sql
SELECT id, name, context_length, prompt_price
FROM openrouter.embedding_models
LIMIT 20;
```

### `openrouter.key`

Fetches current API key metadata from `GET /key`.

```sql
SELECT label, usage, limit, limit_remaining, is_free_tier, rate_limit
FROM openrouter.key
LIMIT 1;
```

### `openrouter.chat_completions`

Runs a single user-message chat completion through `POST /chat/completions`.
Always pass a positive `max_tokens` value so the request is bounded.

```sql
SELECT content, reasoning, finish_reason, max_tokens, returned_model, total_tokens, cost
FROM openrouter.chat_completions
WHERE model = 'google/gemini-3.1-flash-lite'
  AND prompt = 'Reply with exactly: Coral OpenRouter works'
  AND max_tokens = 20
LIMIT 1;
```

This table is single-turn only. It preserves top-level response metadata such
as response ID, returned model, raw `choices`, and `usage` when OpenRouter
returns it. It does not expose chat history, tool calls, structured-output
payloads, provider routing options, transforms, or streaming in this first
version.

### `openrouter.embeddings`

Generates an embedding vector through `POST /embeddings`. Select `embedding`
when you need the full vector; validation examples show a short vector preview
so terminal output stays readable. This table preserves top-level response
metadata such as response ID, returned model, raw `data`, and `usage` when OpenRouter
returns it.

```sql
SELECT model, index, substr(CAST(embedding AS VARCHAR), 1, 80) AS embedding_preview
FROM openrouter.embeddings
WHERE model = 'embedding-model-id'
  AND input = 'Coral OpenRouter source validation'
LIMIT 1;
```

## Validation

Run the source-level checks with a valid `OPENROUTER_API_KEY` before opening or
updating a PR. The API key is required for `source add`, `source test`, and live
SQL queries, but it should never be printed or committed.

```bash
coral source lint sources/community/openrouter/manifest.yaml

export OPENROUTER_API_KEY="your_openrouter_api_key"
coral source add --file sources/community/openrouter/manifest.yaml
coral source test openrouter
```

The declared test queries cover model discovery and key metadata:

```sql
SELECT id, name, context_length FROM openrouter.models LIMIT 5;

SELECT label, usage, limit_remaining, is_free_tier
FROM openrouter.key
LIMIT 1;
```

Before opening a PR, also capture live output for one bounded chat-completion
query and one embedding query against real models.

### Live validation output

The following output was captured against OpenRouter with a valid API key.

#### Manifest lint

Command:

```bash
coral source lint sources/community/openrouter/manifest.yaml
```

Output:

```text
Manifest is valid
```

#### Add source and run declared tests

Command:

```bash
coral source add --file sources/community/openrouter/manifest.yaml
```

Output:

```text
Added source openrouter

  PASS openrouter connected successfully

    openrouter (5 tables)
    - chat_completions
    - embedding_models
    - embeddings
    - key
    - models
    Query tests
    2 declared - 2 passed - 0 failed

    PASS SELECT id, name, context_length FROM openrouter.models LIMIT 5
      5 rows

    PASS SELECT label, usage, limit_remaining, is_free_tier FROM openrouter.key LIMIT 1
      1 row
```

#### Re-run source tests

Command:

```bash
coral source test openrouter
```

Output:

```text
  PASS openrouter connected successfully

    openrouter (5 tables)
    - chat_completions
    - embedding_models
    - embeddings
    - key
    - models
    Query tests
    2 declared - 2 passed - 0 failed

    PASS SELECT id, name, context_length FROM openrouter.models LIMIT 5
      5 rows

    PASS SELECT label, usage, limit_remaining, is_free_tier FROM openrouter.key LIMIT 1
      1 row
```

#### Model inventory query

Command:

```bash
coral sql "SELECT id, name, context_length FROM openrouter.models LIMIT 10"
```

Output:

```text
+--------------------------------+-----------------------------------+----------------+
| id                             | name                              | context_length |
+--------------------------------+-----------------------------------+----------------+
| qwen/qwen3.7-max               | Qwen: Qwen3.7 Max                 | 1000000        |
| x-ai/grok-build-0.1            | xAI: Grok Build 0.1               | 256000         |
| google/gemini-3.5-flash        | Google: Gemini 3.5 Flash          | 1048576        |
| anthropic/claude-opus-4.7-fast | Anthropic: Claude Opus 4.7 (Fast) | 1000000        |
| perceptron/perceptron-mk1      | Perceptron: Perceptron Mk1        | 32768          |
| inclusionai/ring-2.6-1t        | inclusionAI: Ring-2.6-1T          | 262144         |
| google/gemini-3.1-flash-lite   | Google: Gemini 3.1 Flash Lite     | 1048576        |
| baidu/cobuddy:free             | Baidu Qianfan: CoBuddy (free)     | 131072         |
| openai/gpt-chat-latest         | OpenAI: GPT Chat Latest           | 400000         |
| x-ai/grok-4.3                  | xAI: Grok 4.3                     | 1000000        |
+--------------------------------+-----------------------------------+----------------+
```

#### Key metadata query

The live output below omits `label` intentionally. The source exposes the
mapped `label` column, but validation avoids printing key-like identifiers.

Command:

```bash
coral sql "SELECT usage, limit_remaining, is_free_tier FROM openrouter.key LIMIT 1"
```

Output:

```text
+-----------+-----------------+--------------+
| usage     | limit_remaining | is_free_tier |
+-----------+-----------------+--------------+
| 0.0000405 |                 | true         |
+-----------+-----------------+--------------+
```

#### Embedding model inventory query

Command:

```bash
coral sql "SELECT id, name, context_length FROM openrouter.embedding_models LIMIT 10"
```

Output:

```text
+-----------------------------------------------+------------------------------------------------+----------------+
| id                                            | name                                           | context_length |
+-----------------------------------------------+------------------------------------------------+----------------+
| google/gemini-embedding-2-preview             | Google: Gemini Embedding 2 Preview             | 8192           |
| perplexity/pplx-embed-v1-4b                   | Perplexity: Embed V1 4B                        | 32000          |
| perplexity/pplx-embed-v1-0.6b                 | Perplexity: Embed V1 0.6B                      | 32000          |
| nvidia/llama-nemotron-embed-vl-1b-v2:free     | NVIDIA: Llama Nemotron Embed VL 1B V2 (free)   | 131072         |
| thenlper/gte-base                             | Thenlper: GTE-Base                             | 8192           |
| thenlper/gte-large                            | Thenlper: GTE-Large                            | 8192           |
| intfloat/e5-large-v2                          | Intfloat: E5-Large-v2                          | 8192           |
| intfloat/e5-base-v2                           | Intfloat: E5-Base-v2                           | 8192           |
| intfloat/multilingual-e5-large                | Intfloat: Multilingual-E5-Large                | 8192           |
| sentence-transformers/paraphrase-minilm-l6-v2 | Sentence Transformers: paraphrase-MiniLM-L6-v2 | 8192           |
+-----------------------------------------------+------------------------------------------------+----------------+
```

#### Bounded chat-completion query

Command:

```bash
coral sql "SELECT content, reasoning, finish_reason, max_tokens, returned_model, total_tokens, cost FROM openrouter.chat_completions WHERE model = 'google/gemini-3.1-flash-lite' AND prompt = 'Reply with exactly: Coral OpenRouter works' AND max_tokens = 20 LIMIT 1"
```

Output:

```text
+------------------------+-----------+---------------+------------+---------------------------------------+--------------+---------+
| content                | reasoning | finish_reason | max_tokens | returned_model                        | total_tokens | cost    |
+------------------------+-----------+---------------+------------+---------------------------------------+--------------+---------+
| Coral OpenRouter works |           | stop          | 20         | google/gemini-3.1-flash-lite-20260507 | 13           | 8.25e-6 |
+------------------------+-----------+---------------+------------+---------------------------------------+--------------+---------+
```

#### Embedding query

Command:

```bash
coral sql "SELECT id, returned_model, index, total_tokens, cost, substr(CAST(embedding AS VARCHAR), 1, 80) AS embedding_preview FROM openrouter.embeddings WHERE model = 'nvidia/llama-nemotron-embed-vl-1b-v2:free' AND input = 'Coral OpenRouter source validation' LIMIT 1"
```

Output:

```text
+-----------------------------------------+---------------------------------------------------------+-------+--------------+------+----------------------------------------------------------------------------------+
| id                                      | returned_model                                          | index | total_tokens | cost | embedding_preview                                                                |
+-----------------------------------------+---------------------------------------------------------+-------+--------------+------+----------------------------------------------------------------------------------+
| gen-emb-1779968767-UB9SzA0JqjtCaZ9ynMMz | private/openrouter/nvidia/llama-nemotron-embed-vl-1b-v2 | 0     | 8            | 0.0  | [0.0161285400390625,0.0282745361328125,0.006008148193359375,0.0091094970703125,0 |
+-----------------------------------------+---------------------------------------------------------+-------+--------------+------+----------------------------------------------------------------------------------+
```

## Scope and limitations

- Targets OpenRouter's documented `/api/v1` endpoints.
- Requires `OPENROUTER_API_KEY` bearer authentication.
- `chat_completions` uses required positive `max_tokens`.
- `chat_completions` is single-turn and non-streaming.
- `chat_completions` and `embeddings` are live execution tables and may
  consume OpenRouter credits or rate limits.
- `embeddings` requires an embedding-capable model.
- Does not expose streaming, provider routing preferences, transforms,
  structured outputs, tool calls, or multimodal message payloads in this first
  version.
- Does not include model-detail path lookups, avoiding model-ID path issues for
  IDs that contain `/`.
