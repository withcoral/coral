# elevenlabs community source

Query ElevenLabs voices and text-to-speech models through Coral SQL. This source exposes
available TTS models and voices so you can discover model capabilities, compare
pricing, list voices, and join voice metadata into wider analysis workflows.

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 2
**Base URL:** `https://api.elevenlabs.io`

## Why this source

ElevenLabs is a text-to-speech API provider offering expressive AI voices across 70+
languages. Coral did not have an ElevenLabs source yet, so this community spec gives
users a focused read/query surface for:

- Discovering available TTS models and their capabilities (TTS, voice conversion, style,
  speaker boost, fine-tuning support).
- Listing voices with their categories, labels, and preview URLs.
- Comparing model pricing via `token_cost_factor` and `model_rates`.
- Joining voice metadata with other Coral sources in local analysis workflows.

The v1 surface is intentionally narrow and read-oriented. It proves Coral can authenticate
against ElevenLabs, call the Models and Voices APIs, map JSON responses into tables, and
validate the source with declared test queries. The `voices` table uses the v2 list endpoint
with cursor pagination and optional category/search/voice-type filters.

## Installation

Community sources are not bundled with the Coral binary. Clone the Coral repository and add
the manifest from this directory:

```bash
coral source add --file sources/community/elevenlabs/manifest.yaml
```

You can also copy `manifest.yaml` into another workspace and pass that path to
`coral source add --file`.

## Authentication

Create or copy an API key from the ElevenLabs console:

https://elevenlabs.io/app/settings/api-keys

API keys are project-scoped. Use an API key with access to the Models and Voices APIs.
See: https://elevenlabs.io/docs/api-reference/authentication

Set the key as `ELEVENLABS_API_KEY` before adding or testing the source. Coral sends it
as a `xi-api-key` header to the ElevenLabs API.

```bash
export ELEVENLABS_API_KEY="your_elevenlabs_api_key"
coral source add --file sources/community/elevenlabs/manifest.yaml
```

Interactive install also works:

```bash
coral source add --interactive --file sources/community/elevenlabs/manifest.yaml
```

## Provider docs

- ElevenLabs API reference: https://elevenlabs.io/docs/api-reference
- ElevenLabs models: https://elevenlabs.io/docs/api-reference/models/list
- ElevenLabs voices (v2 list): https://elevenlabs.io/docs/api-reference/voices/search
- ElevenLabs voices (legacy): https://elevenlabs.io/docs/api-reference/legacy/voices/get-all
- ElevenLabs authentication: https://elevenlabs.io/docs/api-reference/authentication
- ElevenLabs API key settings: https://elevenlabs.io/app/settings/api-keys

## Tables

| Table | Description | Required filters |
| --- | --- | --- |
| `elevenlabs.models` | Available TTS models from the Models API. | None |
| `elevenlabs.voices` | Available voices from the v2 Voices API (cursor pagination). | Optional: `category`, `voice_type`, `search`, `sort`, `sort_direction`, `fine_tuning_state`, `collection_id` |

### `elevenlabs.models`

Lists available text-to-speech models from `GET /v1/models`.

```sql
SELECT model_id, name, token_cost_factor, can_do_text_to_speech
FROM elevenlabs.models
WHERE can_do_text_to_speech = true
ORDER BY token_cost_factor ASC
LIMIT 10;
```

### `elevenlabs.voices`

Lists available voices from `GET /v2/voices` with cursor pagination and optional filters.

```sql
SELECT voice_id, name, category, labels, preview_url
FROM elevenlabs.voices
LIMIT 10;
```

Optional filters include `category`, `voice_type`, and `search`:

```sql
SELECT voice_id, name, category
FROM elevenlabs.voices
WHERE category = 'premade'
LIMIT 10;
```

## Validation

```bash
$ coral source lint sources/community/elevenlabs/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/elevenlabs/manifest.yaml
Added source elevenlabs

  ✓ elevenlabs connected successfully

    elevenlabs (2 tables)
    ├─ models
    └─ voices
    Query tests
    3 declared · 3 passed · 0 failed

    ✓ SELECT model_id, name, can_do_text_to_speech, can_do_voice_conversion FROM elevenlabs.models LIMIT 5
      5 rows

    ✓ SELECT model_id, name, token_cost_factor, concurrency_group FROM elevenlabs.models WHERE can_do_text_to_speech = true LIMIT 5
      5 rows

    ✓ SELECT voice_id, name, category FROM elevenlabs.voices LIMIT 5
      5 rows
```

```bash
$ coral source test elevenlabs
  ✓ elevenlabs connected successfully

    elevenlabs (2 tables)
    ├─ models
    └─ voices
    Query tests
    3 declared · 3 passed · 0 failed

    ✓ SELECT model_id, name, can_do_text_to_speech, can_do_voice_conversion FROM elevenlabs.models LIMIT 5
      5 rows

    ✓ SELECT model_id, name, token_cost_factor, concurrency_group FROM elevenlabs.models WHERE can_do_text_to_speech = true LIMIT 5
      5 rows

    ✓ SELECT voice_id, name, category FROM elevenlabs.voices LIMIT 5
      5 rows
```

```sql
SELECT table_name, description, required_filters
FROM coral.tables
WHERE schema_name = 'elevenlabs'
ORDER BY table_name;
```

```text
+------------+-----------------------------------------------------------------+------------------+
| table_name | description                                                     | required_filters |
+------------+-----------------------------------------------------------------+------------------+
| models     | Available ElevenLabs text-to-speech models from GET /v1/models. |                  |
| voices     | Available ElevenLabs voices from GET /v2/voices. Supports       |                  |
|            | cursor pagination and optional filters for category, search,    |                  |
|            | and voice type.                                                 |                  |
+------------+-----------------------------------------------------------------+------------------+
```

```sql
SELECT column_name, data_type, is_virtual, is_required_filter
FROM coral.columns
WHERE schema_name = 'elevenlabs' AND table_name = 'models'
ORDER BY ordinal_position;
```

```text
+----------------------------------------+-----------+------------+--------------------+
| column_name                            | data_type | is_virtual | is_required_filter |
+----------------------------------------+-----------+------------+--------------------+
| model_id                               | Utf8      | false      | false              |
| name                                   | Utf8      | false      | false              |
| can_be_finetuned                       | Boolean   | false      | false              |
| can_do_text_to_speech                  | Boolean   | false      | false              |
| can_do_voice_conversion                | Boolean   | false      | false              |
| can_use_style                          | Boolean   | false      | false              |
| can_use_speaker_boost                  | Boolean   | false      | false              |
| serves_pro_voices                      | Boolean   | false      | false              |
| token_cost_factor                      | Float64   | false      | false              |
| description                            | Utf8      | false      | false              |
| requires_alpha_access                  | Boolean   | false      | false              |
| max_characters_request_free_user       | Int64     | false      | false              |
| max_characters_request_subscribed_user | Int64     | false      | false              |
| maximum_text_length_per_request        | Int64     | false      | false              |
| languages                              | Json      | false      | false              |
| model_rates                            | Json      | false      | false              |
| concurrency_group                      | Utf8      | false      | false              |
+----------------------------------------+-----------+------------+--------------------+
```

```sql
SELECT column_name, data_type, is_virtual, is_required_filter
FROM coral.columns
WHERE schema_name = 'elevenlabs' AND table_name = 'voices'
ORDER BY ordinal_position;
```

```text
+-----------------------------+-----------+------------+--------------------+
| column_name                 | data_type | is_virtual | is_required_filter |
+-----------------------------+-----------+------------+--------------------+
| voice_id                    | Utf8      | false      | false              |
| name                        | Utf8      | false      | false              |
| category                    | Utf8      | false      | false              |
| description                 | Utf8      | false      | false              |
| labels                      | Json      | false      | false              |
| preview_url                 | Utf8      | false      | false              |
| fine_tuning                 | Json      | false      | false              |
| high_quality_base_model_ids | Json      | false      | false              |
| verified_languages          | Json      | false      | false              |
+-----------------------------+-----------+------------+--------------------+
```

```sql
SELECT key, kind, required, is_set
FROM coral.inputs
WHERE schema_name = 'elevenlabs'
ORDER BY key;
```

```text
+--------------------+--------+----------+--------+
| key                | kind   | required | is_set |
+--------------------+--------+----------+--------+
| ELEVENLABS_API_KEY | secret | true     | true   |
+--------------------+--------+----------+--------+
```

```sql
SELECT model_id, name, token_cost_factor, can_do_text_to_speech, can_do_voice_conversion
FROM elevenlabs.models
ORDER BY token_cost_factor ASC
LIMIT 10;
```

```text
+----------------------------+------------------------+-------------------+-----------------------+-------------------------+
| model_id                   | name                   | token_cost_factor | can_do_text_to_speech | can_do_voice_conversion |
+----------------------------+------------------------+-------------------+-----------------------+-------------------------+
| eleven_multilingual_v2     | Eleven Multilingual v2 | 1.0               | true                  | false                   |
| eleven_flash_v2_5          | Eleven Flash v2.5      | 1.0               | true                  | false                   |
| eleven_turbo_v2_5          | Eleven Turbo v2.5      | 1.0               | true                  | false                   |
| eleven_turbo_v2            | Eleven Turbo v2        | 1.0               | true                  | false                   |
| eleven_flash_v2            | Eleven Flash v2        | 1.0               | true                  | false                   |
| eleven_english_sts_v2      | Eleven English v2      | 1.0               | false                 | true                    |
| eleven_monolingual_v1      | Eleven English v1      | 1.0               | true                  | false                   |
| eleven_multilingual_sts_v2 | Eleven Multilingual v2 | 1.0               | false                 | true                    |
| eleven_multilingual_v1     | Eleven Multilingual v1 | 1.0               | true                  | false                   |
| eleven_v3                  | Eleven v3              | 1.0               | true                  | false                   |
+----------------------------+------------------------+-------------------+-----------------------+-------------------------+
```

```sql
SELECT voice_id, name, category
FROM elevenlabs.voices
LIMIT 5;
```

```text
+----------------------+----------------------------------------+----------+
| voice_id             | name                                   | category |
+----------------------+----------------------------------------+----------+
| CwhRBWXzGAHq8TQ4Fs17 | Roger - Laid-Back, Casual, Resonant    | premade  |
| EXAVITQu4vr4xnSDxMaL | Sarah - Mature, Reassuring, Confident  | premade  |
| FGY2WhTYpPnrIDTdsKH5 | Laura - Enthusiast, Quirky Attitude    | premade  |
| IKne3meq5aSn9XLyUdCD | Charlie - Deep, Confident, Energetic   | premade  |
| JBFqnCBsd6RMkjVDRZzb | George - Warm, Captivating Storyteller | premade  |
+----------------------+----------------------------------------+----------+
```

```sql
SELECT category, count(*) AS voice_count
FROM elevenlabs.voices
GROUP BY category
ORDER BY voice_count DESC;
```

```text
+----------+-------------+
| category | voice_count |
+----------+-------------+
| premade  | 21          |
+----------+-------------+
```

## Implementation notes

- Uses Coral source-spec DSL v3 with the HTTP backend.
- Uses `HeaderAuth` with `xi-api-key: {{input.ELEVENLABS_API_KEY}}`.
- Maps `GET /v1/models` top-level array into `elevenlabs.models` via `row_strategy: direct`.
- Maps `GET /v2/voices` response `voices` array into `elevenlabs.voices` via `rows_path: [voices]`.
- The models endpoint returns small datasets (10 models) so no pagination is needed.
- The voices endpoint uses the v2 API with cursor pagination (`next_page_token`) and
  supports optional query filters (`category`, `voice_type`, `search`, `sort`,
  `sort_direction`, `fine_tuning_state`, `collection_id`).
- Exposes `model_rates` and `languages` as JSON for flexible inspection when querying.
- Does not require runtime, CLI, MCP, or UI changes.

## Limitations

- This source is read/query oriented and does not generate audio.
- Text-to-speech synthesis, voice cloning, and audio generation are not included.
- Responses, available models, pricing, rate limits, and errors depend on the ElevenLabs
  account tier and API key permissions.
- The `voices` table uses the v2 list endpoint with cursor pagination and optional filters
  for `category`, `voice_type`, `search`, `sort`, `sort_direction`, `fine_tuning_state`,
  and `collection_id`.
- The v1 `GET /v1/voices` endpoint (legacy) stops working above 500 voices and has no
  pagination or filtering support. The v2 endpoint replaces it.
- Streaming TTS, SSML input, pronunciation dictionaries, and voice design (VoiceLab) are
  not included.

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md), keep the manifest focused,
and include the validation commands plus proof output in the PR description.
