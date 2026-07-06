# Sarvam AI Source

Query Sarvam AI language detection, translation, and bounded live chat-completion responses through Coral SQL.

## Summary

This source lets Coral identify the language and script of text, translate one text input between supported Indian language codes, and run one bounded non-streaming single-turn Sarvam AI chat completion through SQL. It preserves Sarvam response IDs, detected language metadata, translated text, chat choices, returned model metadata, and token usage fields where the API returns them.

## Provider docs

- Sarvam API introduction: https://docs.sarvam.ai/api-reference-docs/introduction
- Authentication: https://docs.sarvam.ai/api-reference-docs/authentication
- Language detection: https://docs.sarvam.ai/api-reference-docs/text/identify-language
- Translation: https://docs.sarvam.ai/api-reference-docs/text/translate-text
- Chat completions: https://docs.sarvam.ai/api-reference-docs/chat/chat-completions
- Credits and rate limits: https://docs.sarvam.ai/api-reference-docs/ratelimits
- API keys: https://dashboard.sarvam.ai/

## Authentication

Create a Sarvam AI API key in the Sarvam dashboard, then add the community source:

```bash
coral source add --interactive --file sources/community/sarvam_ai/manifest.yaml
```

For scripted setup, provide the key as an environment variable:

```bash
SARVAM_AI_API_KEY=... coral source add --file sources/community/sarvam_ai/manifest.yaml
```

The key is stored locally by Coral and sent in the `api-subscription-key` header.

## Live request costs

All tables in this source call live Sarvam APIs when selected. Language detection, translation, and chat completion queries can consume Sarvam credits, quota, and rate limits. Use short inputs and positive token bounds for validation queries.

## Source shape

- `sarvam_ai.language_detection` identifies the language and script for one text input.
- `sarvam_ai.translations` translates one text input with required source language, target language, and model filters.
- `sarvam_ai.chat_completions` runs one bounded synchronous non-streaming chat completion with required `model`, `prompt`, and `max_tokens` filters.

## Source scope

- Targets Sarvam's hosted API at `https://api.sarvam.ai`.
- Requires `SARVAM_AI_API_KEY` authentication.
- Language detection accepts one text `input`.
- Translation requires one text `input`, `source_language_code`, `target_language_code`, and `model`.
- Chat calls require `model`, `prompt`, and a positive `max_tokens` filter so examples and validation are bounded.
- Chat is single-turn, non-streaming, and requests one choice in this first version.

## Limitations

- Speech-to-text, speech-to-text translate, text-to-speech, pronunciation dictionary, document intelligence, streaming, WebSocket, and batch endpoints are intentionally out of scope.
- Translation exposes the core text fields only. Optional speaker gender, mode, output script, and numeral-format controls are intentionally omitted from this first version.
- Chat history, tools, tool choice, stop sequences, wiki grounding, seed, sampling controls, and multi-choice generation are intentionally omitted.
- The source does not expose a model catalog table because the documented Sarvam pages list the relevant model IDs in each endpoint's request schema rather than a general models endpoint.
- `sarvam_ai.chat_completions` sends `stream=false` and expects a final synchronous response row.

## Tables

### `sarvam_ai.language_detection`

Identifies the language and script for one text input.

```sql
SELECT request_id, language_code, script_code
FROM sarvam_ai.language_detection
WHERE input = 'Hello, how are you?'
LIMIT 1;
```

Useful columns:

| Column | Notes |
|---|---|
| `request_id` | Sarvam request identifier |
| `language_code` | Detected language code such as `en-IN` or `hi-IN` |
| `script_code` | Detected script code such as `Latn` or `Deva` |

### `sarvam_ai.translations`

Translates one text input.

```sql
SELECT request_id, returned_source_language_code, target_language_code, translated_text
FROM sarvam_ai.translations
WHERE input = 'Hello, how are you?'
  AND source_language_code = 'en-IN'
  AND target_language_code = 'hi-IN'
  AND model = 'sarvam-translate:v1'
LIMIT 1;
```

Useful columns:

| Column | Notes |
|---|---|
| `model` | Translation model supplied in SQL, such as `sarvam-translate:v1` |
| `source_language_code` | Source language code supplied in SQL |
| `target_language_code` | Target language code supplied in SQL |
| `returned_source_language_code` | Source language code returned by Sarvam |
| `translated_text` | Translated text |

### `sarvam_ai.chat_completions`

Runs one bounded single-turn chat completion.

```sql
SELECT id, finish_reason, max_tokens, returned_model,
       prompt_tokens, completion_tokens, total_tokens
FROM sarvam_ai.chat_completions
WHERE model = 'sarvam-30b'
  AND prompt = 'Reply in one short sentence about Coral.'
  AND max_tokens = 30
LIMIT 1;
```

This table preserves the raw `choices` array, `usage` object, response `id`, returned model, first choice fields, and token counts where Sarvam returns them.

## Live validation output

Run these checks after setting `SARVAM_AI_API_KEY`.

```bash
$ coral source lint sources/community/sarvam_ai/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/sarvam_ai/manifest.yaml
Added source sarvam_ai

  PASS sarvam_ai connected successfully

    sarvam_ai (3 tables)
    - chat_completions
    - language_detection
    - translations
    Query tests
    1 declared - 1 passed - 0 failed

    PASS SELECT language_code, script_code FROM sarvam_ai.language_detection WHERE input = 'Hello, how are you?' LIMIT 1
      1 row
```

```bash
$ coral source test sarvam_ai
  PASS sarvam_ai connected successfully

    sarvam_ai (3 tables)
    - chat_completions
    - language_detection
    - translations
    Query tests
    1 declared - 1 passed - 0 failed

    PASS SELECT language_code, script_code FROM sarvam_ai.language_detection WHERE input = 'Hello, how are you?' LIMIT 1
      1 row
```

```sql
SELECT table_name
FROM coral.tables
WHERE schema_name = 'sarvam_ai'
ORDER BY table_name;
```

```text
+--------------------+
| table_name         |
+--------------------+
| chat_completions   |
| language_detection |
| translations       |
+--------------------+
```

```sql
SELECT table_name, column_name, data_type
FROM coral.columns
WHERE schema_name = 'sarvam_ai'
ORDER BY table_name, ordinal_position;
```

```text
+--------------------+-------------------------------+-----------+
| table_name         | column_name                   | data_type |
+--------------------+-------------------------------+-----------+
| chat_completions   | model                         | Utf8      |
| chat_completions   | prompt                        | Utf8      |
| chat_completions   | max_tokens                    | Int64     |
| chat_completions   | id                            | Utf8      |
| chat_completions   | object                        | Utf8      |
| chat_completions   | created                       | Int64     |
| chat_completions   | returned_model                | Utf8      |
| chat_completions   | service_tier                  | Utf8      |
| chat_completions   | system_fingerprint            | Utf8      |
| chat_completions   | choices                       | Json      |
| chat_completions   | index                         | Int64     |
| chat_completions   | finish_reason                 | Utf8      |
| chat_completions   | message_role                  | Utf8      |
| chat_completions   | content                       | Utf8      |
| chat_completions   | reasoning_content             | Utf8      |
| chat_completions   | refusal                       | Utf8      |
| chat_completions   | tool_calls                    | Json      |
| chat_completions   | logprobs                      | Json      |
| chat_completions   | usage                         | Json      |
| chat_completions   | prompt_tokens                 | Int64     |
| chat_completions   | completion_tokens             | Int64     |
| chat_completions   | total_tokens                  | Int64     |
| chat_completions   | prompt_tokens_details         | Json      |
| chat_completions   | completion_tokens_details     | Json      |
| language_detection | input                         | Utf8      |
| language_detection | request_id                    | Utf8      |
| language_detection | language_code                 | Utf8      |
| language_detection | script_code                   | Utf8      |
| translations       | input                         | Utf8      |
| translations       | source_language_code          | Utf8      |
| translations       | target_language_code          | Utf8      |
| translations       | model                         | Utf8      |
| translations       | request_id                    | Utf8      |
| translations       | translated_text               | Utf8      |
| translations       | returned_source_language_code | Utf8      |
+--------------------+-------------------------------+-----------+
```

```sql
SELECT key, kind, required
FROM coral.inputs
WHERE schema_name = 'sarvam_ai'
ORDER BY key;
```

```text
+-------------------+--------+----------+
| key               | kind   | required |
+-------------------+--------+----------+
| SARVAM_AI_API_KEY | secret | true     |
+-------------------+--------+----------+
```

```sql
SELECT request_id, language_code, script_code
FROM sarvam_ai.language_detection
WHERE input = 'Hello, how are you?'
LIMIT 1;
```

```text
+-----------------------------------------------+---------------+-------------+
| request_id                                    | language_code | script_code |
+-----------------------------------------------+---------------+-------------+
| 20260601_945a5775-43b8-4b31-bec8-2257a0046078 | en-IN         | Latn        |
+-----------------------------------------------+---------------+-------------+
```

```sql
SELECT request_id, returned_source_language_code, target_language_code,
       length(translated_text) AS translated_length
FROM sarvam_ai.translations
WHERE input = 'Hello, how are you?'
  AND source_language_code = 'en-IN'
  AND target_language_code = 'hi-IN'
  AND model = 'sarvam-translate:v1'
LIMIT 1;
```

```text
+-----------------------------------------------+-------------------------------+----------------------+-------------------+
| request_id                                    | returned_source_language_code | target_language_code | translated_length |
+-----------------------------------------------+-------------------------------+----------------------+-------------------+
| 20260601_6d8fb69b-c96f-4aba-bef2-3c3a3101d629 | en-IN                         | hi-IN                | 20                |
+-----------------------------------------------+-------------------------------+----------------------+-------------------+
```

```sql
SELECT id, finish_reason, max_tokens, returned_model,
       prompt_tokens, completion_tokens, total_tokens
FROM sarvam_ai.chat_completions
WHERE model = 'sarvam-30b'
  AND prompt = 'Reply in one short sentence about Coral.'
  AND max_tokens = 30
LIMIT 1;
```

```text
+-----------------------------------------------+---------------+------------+----------------+---------------+-------------------+--------------+
| id                                            | finish_reason | max_tokens | returned_model | prompt_tokens | completion_tokens | total_tokens |
+-----------------------------------------------+---------------+------------+----------------+---------------+-------------------+--------------+
| 20260601_c7618c0b-89af-4a5e-8106-8217cc929d3d | length        | 30         | sarvam-30b     | 18            | 30                | 48           |
+-----------------------------------------------+---------------+------------+----------------+---------------+-------------------+--------------+
```
