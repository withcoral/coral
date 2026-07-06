# Cohere AI Source

Query Cohere model metadata and run bounded live chat-completion and embedding checks through Coral SQL.

## Summary

This source lets Coral query Cohere for model catalog metadata, run one bounded non-streaming single-turn chat request, and generate one text embedding vector through SQL. It preserves top-level live response metadata, including response IDs, assistant message objects, embedding response metadata, usage objects, and billed token counts where Cohere returns them.

## Provider docs

- Cohere API overview: https://docs.cohere.com/v2/reference/about
- Models: https://docs.cohere.com/v2/reference/list-models
- Chat: https://docs.cohere.com/v2/reference/chat
- Embeddings: https://docs.cohere.com/v2/reference/embed
- Model guide: https://docs.cohere.com/v2/docs/models
- Rate limits: https://docs.cohere.com/v2/docs/rate-limits
- API keys: https://dashboard.cohere.com/api-keys

## Authentication

Create a Cohere API key in the Cohere dashboard, then add the community source:

```bash
coral source add --interactive --file sources/community/cohere_ai/manifest.yaml
```

For scripted setup, provide the key as an environment variable:

```bash
COHERE_API_KEY=... coral source add --file sources/community/cohere_ai/manifest.yaml
```

The key is stored locally by Coral and sent as a Bearer token to Cohere's API.

## Live request costs

`cohere_ai.chat_completions` and `cohere_ai.embeddings` are live Cohere API calls. Selecting from these tables can consume API credits, quota, and rate limits. Use small prompts and positive token bounds for validation queries.

## Source shape

- `cohere_ai.models` lists model catalog rows from `GET /v1/models`.
- `cohere_ai.chat_completions` runs one bounded synchronous non-streaming chat request with required `model`, `prompt`, and `max_tokens` filters.
- `cohere_ai.embeddings` generates one text embedding vector with required `model`, `input`, and `input_type` filters.

## Source scope

- Targets Cohere's hosted API at `https://api.cohere.com`.
- Requires `COHERE_API_KEY` bearer authentication.
- `cohere_ai.models` is a metadata read.
- `cohere_ai.chat_completions` and `cohere_ai.embeddings` perform live Cohere API calls when selected, so they can consume API credits, quota, or rate limits.
- Chat calls require a positive `max_tokens` filter so examples and validation are bounded.
- Chat is single-turn and non-streaming in this first version.
- Embeddings use one text input and request `float` embeddings.
- Embeddings require a text `input_type` such as `search_query`, `search_document`, `classification`, or `clustering`, matching Cohere's requirement for Embed v3 and newer models.

## Limitations

- Streaming chat is intentionally omitted because the source returns final SQL rows.
- Tool calls, documents, citations, JSON schema response formats, safety controls, logprob requests, and reasoning configuration are not requested by this first version.
- The nullable `tool_calls` and `logprobs` columns only preserve metadata if Cohere returns those fields for a request shape supported later.
- Embeddings accept one text input in this first version. Batch text arrays, image embeddings, image `input_type`, and non-float embedding types are intentionally omitted.
- Rerank, classify, tokenize, detokenize, audio transcriptions, fine-tuning, datasets, connectors, and evaluation endpoints are intentionally out of scope.
- Model detail lookup is intentionally omitted because the list endpoint already provides useful metadata without needing path-sensitive model IDs.

## Tables

### `cohere_ai.models`

Lists Cohere models visible to the API key.

```sql
SELECT name, endpoints, context_length, features
FROM cohere_ai.models
WHERE endpoint = 'chat'
LIMIT 10;
```

Useful columns:

| Column | Notes |
|---|---|
| `name` | Model name to use in chat or embedding requests |
| `endpoints` | Comma-separated supported endpoint names; use the optional `endpoint` filter to find chat or embed models |
| `context_length` | Context length returned by Cohere |
| `features` | Comma-separated feature flags when returned |

### `cohere_ai.chat_completions`

Runs one bounded single-turn chat request.

```sql
SELECT content, finish_reason, max_tokens, billed_input_tokens, billed_output_tokens
FROM cohere_ai.chat_completions
WHERE model = 'command-a-03-2025'
  AND prompt = 'Reply with exactly: Coral Cohere works'
  AND max_tokens = 20
LIMIT 1;
```

This table preserves the raw assistant `message`, `usage`, response `id`, and token counts where Cohere returns them.

### `cohere_ai.embeddings`

Generates one text embedding vector.

```sql
SELECT id, api_version, billed_input_tokens,
       substr(CAST(float_embedding AS VARCHAR), 1, 80) AS embedding_preview
FROM cohere_ai.embeddings
WHERE model = 'embed-v4.0'
  AND input = 'Coral Cohere source validation'
  AND input_type = 'search_query'
LIMIT 1;
```

This table preserves the raw `embeddings` object, raw `meta` object, response `id`, API version metadata, and billed token counts where Cohere returns them.

## Live validation output

```bash
$ coral source lint sources/community/cohere_ai/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/cohere_ai/manifest.yaml
Added source cohere_ai

  PASS cohere_ai connected successfully

    cohere_ai (3 tables)
    - chat_completions
    - embeddings
    - models
    Query tests
    1 declared - 1 passed - 0 failed

    PASS SELECT name, endpoints, context_length FROM cohere_ai.models LIMIT 5
      5 rows
```

```bash
$ coral source test cohere_ai
  PASS cohere_ai connected successfully

    cohere_ai (3 tables)
    - chat_completions
    - embeddings
    - models
    Query tests
    1 declared - 1 passed - 0 failed

    PASS SELECT name, endpoints, context_length FROM cohere_ai.models LIMIT 5
      5 rows
```

```sql
SELECT table_name
FROM coral.tables
WHERE schema_name = 'cohere_ai'
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
SELECT table_name, column_name, data_type
FROM coral.columns
WHERE schema_name = 'cohere_ai'
ORDER BY table_name, ordinal_position;
```

```text
+------------------+----------------------+-----------+
| table_name       | column_name          | data_type |
+------------------+----------------------+-----------+
| chat_completions | model                | Utf8      |
| chat_completions | prompt               | Utf8      |
| chat_completions | max_tokens           | Int64     |
| chat_completions | id                   | Utf8      |
| chat_completions | finish_reason        | Utf8      |
| chat_completions | message              | Json      |
| chat_completions | message_role         | Utf8      |
| chat_completions | content_type         | Utf8      |
| chat_completions | content              | Utf8      |
| chat_completions | tool_calls           | Json      |
| chat_completions | usage                | Json      |
| chat_completions | billed_input_tokens  | Int64     |
| chat_completions | billed_output_tokens | Int64     |
| chat_completions | input_tokens         | Int64     |
| chat_completions | output_tokens        | Int64     |
| chat_completions | cached_tokens        | Int64     |
| chat_completions | logprobs             | Json      |
| embeddings       | model                | Utf8      |
| embeddings       | input                | Utf8      |
| embeddings       | input_type           | Utf8      |
| embeddings       | id                   | Utf8      |
| embeddings       | texts                | Json      |
| embeddings       | returned_text        | Utf8      |
| embeddings       | embeddings           | Json      |
| embeddings       | float_embedding      | Json      |
| embeddings       | meta                 | Json      |
| embeddings       | api_version          | Utf8      |
| embeddings       | billed_input_tokens  | Int64     |
| embeddings       | billed_image_tokens  | Int64     |
| models           | endpoint             | Utf8      |
| models           | default_only         | Boolean   |
| models           | name                 | Utf8      |
| models           | is_deprecated        | Boolean   |
| models           | endpoints            | Utf8      |
| models           | finetuned            | Boolean   |
| models           | context_length       | Int64     |
| models           | tokenizer_url        | Utf8      |
| models           | default_endpoints    | Utf8      |
| models           | features             | Utf8      |
| models           | sampling_defaults    | Json      |
+------------------+----------------------+-----------+
```

```sql
SELECT key, kind, required
FROM coral.inputs
WHERE schema_name = 'cohere_ai'
ORDER BY key;
```

```text
+----------------+--------+----------+
| key            | kind   | required |
+----------------+--------+----------+
| COHERE_API_KEY | secret | true     |
+----------------+--------+----------+
```

```sql
SELECT name, endpoints, context_length, features
FROM cohere_ai.models
WHERE endpoint = 'chat'
LIMIT 5;
```

```text
+-----------------------------+----------------+----------------+-----------------------------------------------------------------------------------------------------+
| name                        | endpoints      | context_length | features                                                                                            |
+-----------------------------+----------------+----------------+-----------------------------------------------------------------------------------------------------+
| c4ai-aya-expanse-32b        | generate, chat | 128000         |                                                                                                     |
| c4ai-aya-vision-32b         | chat           | 16384          | logprobs, vision                                                                                    |
| command-a-03-2025           | chat           | 288000         | json_mode, json_schema, strict_tools, safety_modes, tools, tool_choice                              |
| command-a-plus-05-2026      | generate, chat | 128000         | logprobs, json_mode, json_schema, strict_tools, safety_modes, tools, reasoning, vision, tool_images |
| command-a-reasoning-08-2025 | chat           | 288768         | json_mode, json_schema, strict_tools, safety_modes, tools, reasoning                                |
+-----------------------------+----------------+----------------+-----------------------------------------------------------------------------------------------------+
```

```sql
SELECT content, finish_reason, max_tokens, billed_input_tokens, billed_output_tokens
FROM cohere_ai.chat_completions
WHERE model = 'command-a-03-2025'
  AND prompt = 'Reply with exactly: Coral Cohere works'
  AND max_tokens = 20
LIMIT 1;
```

```text
+--------------------+---------------+------------+---------------------+----------------------+
| content            | finish_reason | max_tokens | billed_input_tokens | billed_output_tokens |
+--------------------+---------------+------------+---------------------+----------------------+
| Coral Cohere works | COMPLETE      | 20         | 8                   | 5                    |
+--------------------+---------------+------------+---------------------+----------------------+
```

```sql
SELECT id, message_role, content_type, input_tokens, output_tokens, cached_tokens
FROM cohere_ai.chat_completions
WHERE model = 'command-a-03-2025'
  AND prompt = 'Reply with exactly: Coral Cohere works'
  AND max_tokens = 20
LIMIT 1;
```

```text
+--------------------------------------+--------------+--------------+--------------+---------------+---------------+
| id                                   | message_role | content_type | input_tokens | output_tokens | cached_tokens |
+--------------------------------------+--------------+--------------+--------------+---------------+---------------+
| 48463565-9a32-44e9-8ab9-2cad9ef98c51 | assistant    | text         | 503          | 7             | 0             |
+--------------------------------------+--------------+--------------+--------------+---------------+---------------+
```

```sql
SELECT id, api_version, billed_input_tokens,
       substr(CAST(float_embedding AS VARCHAR), 1, 80) AS embedding_preview
FROM cohere_ai.embeddings
WHERE model = 'embed-v4.0'
  AND input = 'Coral Cohere source validation'
  AND input_type = 'search_query'
LIMIT 1;
```

```text
+--------------------------------------+-------------+---------------------+----------------------------------------------------------------------------------+
| id                                   | api_version | billed_input_tokens | embedding_preview                                                                |
+--------------------------------------+-------------+---------------------+----------------------------------------------------------------------------------+
| 1a911b6d-e340-47f3-9b7c-b19a0867f72d | 2           | 6                   | [-0.0059665833,0.017109653,-0.0004938097,-0.043809433,-0.040975988,-0.0053672004 |
+--------------------------------------+-------------+---------------------+----------------------------------------------------------------------------------+
```

## Validation checklist

```bash
coral source lint sources/community/cohere_ai/manifest.yaml
coral source add --file sources/community/cohere_ai/manifest.yaml
coral source test cohere_ai
```

Then inspect the registered schema:

```sql
SELECT table_name
FROM coral.tables
WHERE schema_name = 'cohere_ai'
ORDER BY table_name;
```

```sql
SELECT table_name, column_name, data_type
FROM coral.columns
WHERE schema_name = 'cohere_ai'
ORDER BY table_name, ordinal_position;
```

```sql
SELECT key, kind, required
FROM coral.inputs
WHERE schema_name = 'cohere_ai'
ORDER BY key;
```

Run representative live queries:

```sql
SELECT name, endpoints, context_length, features
FROM cohere_ai.models
WHERE endpoint = 'chat'
LIMIT 10;
```

```sql
SELECT content, finish_reason, max_tokens, billed_input_tokens, billed_output_tokens
FROM cohere_ai.chat_completions
WHERE model = 'command-a-03-2025'
  AND prompt = 'Reply with exactly: Coral Cohere works'
  AND max_tokens = 20
LIMIT 1;
```

```sql
SELECT id, api_version, billed_input_tokens,
       substr(CAST(float_embedding AS VARCHAR), 1, 80) AS embedding_preview
FROM cohere_ai.embeddings
WHERE model = 'embed-v4.0'
  AND input = 'Coral Cohere source validation'
  AND input_type = 'search_query'
LIMIT 1;
```
