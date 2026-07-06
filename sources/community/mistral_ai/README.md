# Mistral AI Source

Query Mistral AI model metadata and run bounded live chat-completion and embedding checks through Coral SQL.

## Summary

This source lets Coral query Mistral AI for available models, run one bounded non-streaming chat-completion request, and generate one embedding vector through SQL. It preserves top-level response metadata for live request tables, including response IDs, returned models, raw response arrays, and token usage where Mistral returns it.

## Provider docs

- Mistral AI API overview: https://docs.mistral.ai/api
- Models: https://docs.mistral.ai/api/endpoint/models
- Chat completions: https://docs.mistral.ai/api/endpoint/chat
- Embeddings: https://docs.mistral.ai/api/endpoint/embeddings
- Rate limits and usage tiers: https://docs.mistral.ai/deployment/ai-studio/tier
- API keys: https://console.mistral.ai/api-keys

## Authentication

Create a Mistral AI API key in the Mistral console, then add the source:

```bash
coral source add --interactive --file sources/community/mistral_ai/manifest.yaml
```

For scripted setup, provide the key as an environment variable:

```bash
MISTRAL_API_KEY=... coral source add --file sources/community/mistral_ai/manifest.yaml
```

The key is stored locally by Coral and sent as a Bearer token to `https://api.mistral.ai/v1`.

## Live request costs

`mistral_ai.chat_completions` and `mistral_ai.embeddings` are live Mistral AI API calls. Selecting from these tables can consume API credits, quota, and rate limits. Use small prompts and positive token bounds for validation queries.

## Source shape

- `mistral_ai.models` lists model catalog rows from `GET /models`.
- `mistral_ai.chat_completions` runs one bounded synchronous non-streaming chat completion with required `model`, `prompt`, and `max_tokens` filters.
- `mistral_ai.embeddings` generates one embedding vector with required `model` and `input` filters.

## Source scope

- `mistral_ai.models` is a metadata read from `GET /models`.
- `mistral_ai.chat_completions` performs a live Mistral AI API call when selected and may consume API credits, quota, or rate limits.
- `mistral_ai.embeddings` performs a live Mistral AI API call when selected and may consume API credits, quota, or rate limits.
- Chat calls require `model`, `prompt`, and a positive `max_tokens` filter so examples and validation are bounded.
- Chat is single-turn and non-streaming in this first version.
- Embeddings use the documented `model` and `input` request body. Optional model-specific embedding controls such as output dimension, dtype, and encoding format are intentionally omitted from the first version.
- Streaming chat, FIM, classifiers, files, batch, OCR, audio, fine-tuning management, agents, conversations, libraries, observability, and beta workflow endpoints are intentionally out of scope.
- No model-detail path lookup is included, avoiding model-ID path issues for IDs that contain `/`.

## Limitations

- Chat is single-turn only. The table maps one `prompt` filter to one user message and does not model chat history.
- Chat is non-streaming only. Streaming responses are intentionally omitted because the source returns final SQL rows.
- Tool calls are not requested by this source. The nullable `tool_calls` column only preserves tool-call metadata if Mistral returns it for a request shape supported later.
- Embeddings accept one string input in this first version. Batch input arrays are intentionally omitted to keep one SQL row aligned with one request.
- Optional embedding controls such as `output_dimension`, `output_dtype`, and `encoding_format` are not exposed in this first version.
- Mistral endpoints outside model listing, chat completions, and embeddings are out of scope for this source.

## Tables

### `mistral_ai.models`

Lists models available to the API key.

```sql
SELECT id, object, owned_by, max_context_length, completion_chat
FROM mistral_ai.models
LIMIT 10;
```

Useful columns include:

| Column | Notes |
|---|---|
| `id` | Model ID to use in chat or embedding requests |
| `owned_by` | Owner metadata when returned |
| `max_context_length` | Context length metadata when returned |
| `capabilities` | Raw model capabilities JSON |
| `completion_chat` | Whether the model supports chat completions |
| `function_calling` | Whether the model supports function calling |
| `vision` | Whether the model supports vision inputs |

### `mistral_ai.chat_completions`

Runs one bounded, single-turn, non-streaming chat completion.

```sql
SELECT content, finish_reason, max_tokens, returned_model, total_tokens
FROM mistral_ai.chat_completions
WHERE model = 'mistral-small-latest'
  AND prompt = 'Reply with exactly: Coral Mistral works'
  AND max_tokens = 20
LIMIT 1;
```

This table keeps top-level response metadata, not only the first choice:

```sql
SELECT id, object, created, prompt_tokens, completion_tokens, total_tokens
FROM mistral_ai.chat_completions
WHERE model = 'mistral-small-latest'
  AND prompt = 'Reply with exactly: Coral Mistral works'
  AND max_tokens = 20
LIMIT 1;
```

It also preserves the raw `choices` array, `usage` JSON, prompt token detail metadata, and assistant `tool_calls` when Mistral returns them.

### `mistral_ai.embeddings`

Generates one embedding vector.

```sql
SELECT returned_model, index, total_tokens, substr(CAST(embedding AS VARCHAR), 1, 80) AS embedding_preview
FROM mistral_ai.embeddings
WHERE model = 'mistral-embed'
  AND input = 'Coral Mistral source validation'
LIMIT 1;
```

This table keeps the top-level embedding response metadata and raw `data` array while also exposing the first vector as `embedding`.
It also preserves the documented shared `usage` object and token counts when returned.

## Validation checklist

```bash
coral source lint sources/community/mistral_ai/manifest.yaml
coral source add --file sources/community/mistral_ai/manifest.yaml
coral source test mistral_ai
```

Then inspect the registered schema:

```sql
SELECT table_name
FROM coral.tables
WHERE schema_name = 'mistral_ai'
ORDER BY table_name;
```

```sql
SELECT table_name, column_name, data_type
FROM coral.columns
WHERE schema_name = 'mistral_ai'
ORDER BY table_name, ordinal_position;
```

```text
+------------------+-------------------------------+-----------+
| table_name       | column_name                   | data_type |
+------------------+-------------------------------+-----------+
| chat_completions | model                         | Utf8      |
| chat_completions | prompt                        | Utf8      |
| chat_completions | max_tokens                    | Int64     |
| chat_completions | id                            | Utf8      |
| chat_completions | object                        | Utf8      |
| chat_completions | created                       | Int64     |
| chat_completions | returned_model                | Utf8      |
| chat_completions | usage                         | Json      |
| chat_completions | prompt_tokens                 | Int64     |
| chat_completions | completion_tokens             | Int64     |
| chat_completions | total_tokens                  | Int64     |
| chat_completions | prompt_tokens_details         | Json      |
| chat_completions | choices                       | Json      |
| chat_completions | index                         | Int64     |
| chat_completions | finish_reason                 | Utf8      |
| chat_completions | content                       | Utf8      |
| chat_completions | message_role                  | Utf8      |
| chat_completions | tool_calls                    | Json      |
| embeddings       | model                         | Utf8      |
| embeddings       | input                         | Utf8      |
| embeddings       | id                            | Utf8      |
| embeddings       | returned_model                | Utf8      |
| embeddings       | object                        | Utf8      |
| embeddings       | usage                         | Json      |
| embeddings       | prompt_tokens                 | Int64     |
| embeddings       | completion_tokens             | Int64     |
| embeddings       | total_tokens                  | Int64     |
| embeddings       | data                          | Json      |
| embeddings       | index                         | Int64     |
| embeddings       | embedding_object              | Utf8      |
| embeddings       | embedding                     | Json      |
| models           | id                            | Utf8      |
| models           | object                        | Utf8      |
| models           | created                       | Int64     |
| models           | owned_by                      | Utf8      |
| models           | name                          | Utf8      |
| models           | description                   | Utf8      |
| models           | root                          | Utf8      |
| models           | max_context_length            | Int64     |
| models           | aliases                       | Utf8      |
| models           | deprecation                   | Utf8      |
| models           | deprecation_replacement_model | Utf8      |
| models           | default_model_temperature     | Float64   |
| models           | type                          | Utf8      |
| models           | archived                      | Boolean   |
| models           | capabilities                  | Json      |
| models           | completion_chat               | Boolean   |
| models           | completion_fim                | Boolean   |
| models           | function_calling              | Boolean   |
| models           | fine_tuning                   | Boolean   |
| models           | vision                        | Boolean   |
| models           | classification                | Boolean   |
+------------------+-------------------------------+-----------+
```

```sql
SELECT key, kind, required
FROM coral.inputs
WHERE schema_name = 'mistral_ai'
ORDER BY key;
```

Run representative live queries:

```sql
SELECT id, object, owned_by, max_context_length, completion_chat
FROM mistral_ai.models
LIMIT 10;
```

```sql
SELECT content, finish_reason, max_tokens, returned_model, total_tokens
FROM mistral_ai.chat_completions
WHERE model = 'mistral-small-latest'
  AND prompt = 'Reply with exactly: Coral Mistral works'
  AND max_tokens = 20
LIMIT 1;
```

```sql
SELECT returned_model, index, total_tokens, substr(CAST(embedding AS VARCHAR), 1, 80) AS embedding_preview
FROM mistral_ai.embeddings
WHERE model = 'mistral-embed'
  AND input = 'Coral Mistral source validation'
LIMIT 1;
```

## Live validation output

```bash
$ coral source lint sources/community/mistral_ai/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/mistral_ai/manifest.yaml
Added source mistral_ai

  PASS mistral_ai connected successfully

    mistral_ai (3 tables)
    - chat_completions
    - embeddings
    - models
    Query tests
    1 declared - 1 passed - 0 failed

    PASS SELECT id, object, owned_by FROM mistral_ai.models LIMIT 5
      5 rows
```

```bash
$ coral source test mistral_ai
  PASS mistral_ai connected successfully

    mistral_ai (3 tables)
    - chat_completions
    - embeddings
    - models
    Query tests
    1 declared - 1 passed - 0 failed

    PASS SELECT id, object, owned_by FROM mistral_ai.models LIMIT 5
      5 rows
```

```sql
SELECT table_name
FROM coral.tables
WHERE schema_name = 'mistral_ai'
ORDER BY table_name;
```

```text
+------------------+
| table_name       |
+------------------+
| chat_completions |
| embeddings       |
| models           |
+------------------+
```

```sql
SELECT key, kind, required
FROM coral.inputs
WHERE schema_name = 'mistral_ai'
ORDER BY key;
```

```text
+-----------------+--------+----------+
| key             | kind   | required |
+-----------------+--------+----------+
| MISTRAL_API_KEY | secret | true     |
+-----------------+--------+----------+
```

```sql
SELECT id, object, owned_by, max_context_length, completion_chat
FROM mistral_ai.models
LIMIT 10;
```

```text
+-----------------------------+--------+-----------+--------------------+-----------------+
| id                          | object | owned_by  | max_context_length | completion_chat |
+-----------------------------+--------+-----------+--------------------+-----------------+
| mistral-medium-2505         | model  | mistralai | 131072             | true            |
| mistral-medium-2508         | model  | mistralai | 131072             | true            |
| mistral-medium-latest       | model  | mistralai | 131072             | true            |
| mistral-medium              | model  | mistralai | 131072             | true            |
| mistral-vibe-cli-with-tools | model  | mistralai | 131072             | true            |
| open-mistral-nemo           | model  | mistralai | 131072             | true            |
| open-mistral-nemo-2407      | model  | mistralai | 131072             | true            |
| mistral-tiny-2407           | model  | mistralai | 131072             | true            |
| mistral-tiny-latest         | model  | mistralai | 131072             | true            |
| codestral-2508              | model  | mistralai | 256000             | true            |
+-----------------------------+--------+-----------+--------------------+-----------------+
```

```sql
SELECT content, finish_reason, max_tokens, returned_model, total_tokens
FROM mistral_ai.chat_completions
WHERE model = 'mistral-small-latest'
  AND prompt = 'Reply with exactly: Coral Mistral works'
  AND max_tokens = 20
LIMIT 1;
```

```text
+---------------------+---------------+------------+----------------------+--------------+
| content             | finish_reason | max_tokens | returned_model       | total_tokens |
+---------------------+---------------+------------+----------------------+--------------+
| Coral Mistral works | stop          | 20         | mistral-small-latest | 29           |
+---------------------+---------------+------------+----------------------+--------------+
```

```sql
SELECT id, object, created, prompt_tokens, completion_tokens, total_tokens
FROM mistral_ai.chat_completions
WHERE model = 'mistral-small-latest'
  AND prompt = 'Reply with exactly: Coral Mistral works'
  AND max_tokens = 20
LIMIT 1;
```

```text
+----------------------------------+-----------------+------------+---------------+-------------------+--------------+
| id                               | object          | created    | prompt_tokens | completion_tokens | total_tokens |
+----------------------------------+-----------------+------------+---------------+-------------------+--------------+
| a4b3b88bba9c49ab8e0d99e7b701e313 | chat.completion | 1780038283 | 23            | 6                 | 29           |
+----------------------------------+-----------------+------------+---------------+-------------------+--------------+
```

```sql
SELECT id, returned_model, index, total_tokens, substr(CAST(embedding AS VARCHAR), 1, 80) AS embedding_preview
FROM mistral_ai.embeddings
WHERE model = 'mistral-embed'
  AND input = 'Coral Mistral source validation'
LIMIT 1;
```

```text
+----------------------------------+----------------+-------+--------------+----------------------------------------------------------------------------------+
| id                               | returned_model | index | total_tokens | embedding_preview                                                                |
+----------------------------------+----------------+-------+--------------+----------------------------------------------------------------------------------+
| 36fcfd3a0ccd41d09301b41442d4dabe | mistral-embed  | 0     | 8            | [-0.00481414794921875,0.003612518310546875,0.007099151611328125,-0.0125350952148 |
+----------------------------------+----------------+-------+--------------+----------------------------------------------------------------------------------+
```
