# Deepgram community source

Query Deepgram speech-to-text model metadata and run audio transcriptions through
Coral SQL. This source exposes available STT models and a live transcription table
that accepts an audio URL and model name, returning transcript text, confidence
scores, word-level timing, and response metadata.

**Version:** 0.2.0
**Backend:** HTTP
**Tables:** 2
**Base URL:** `https://api.deepgram.com`

## Why this source

Deepgram is a speech-to-text API provider offering both batch and streaming
transcription with features like diarization, sentiment analysis, and topic
extraction. Coral did not have a Deepgram source yet, so this community spec
gives users a focused read/query surface for:

- Discovering available Deepgram STT models from SQL.
- Running bounded transcription queries against publicly accessible audio URLs.
- Extracting word-level timing, confidence scores, and response metadata.
- Joining model metadata with other Coral sources in local analysis workflows.

The v1 surface is intentionally narrow and read-oriented. It proves Coral can
authenticate against Deepgram, call the transcription API, map JSON responses
into tables, and validate the source with declared test queries.

## Installation

Community sources are not bundled with the Coral binary. Clone the Coral
repository and add the manifest from this directory:

```bash
coral source add --file sources/community/deepgram/manifest.yaml
```

You can also copy `manifest.yaml` into another workspace and pass that path to
`coral source add --file`.

## Authentication

Create or copy an API key from the Deepgram console:

https://console.deepgram.com/

API keys are project-scoped. Use a project API key with a role that can
call the Models and Listen APIs.
See: https://developers.deepgram.com/docs/create-additional-api-keys

Set the key as `DEEPGRAM_API_KEY` before adding or testing the source. Coral sends
it as a `Token` header to Deepgram's API (not Bearer).

```bash
export DEEPGRAM_API_KEY="your_deepgram_api_key"
coral source add --file sources/community/deepgram/manifest.yaml
```

Interactive install also works:

```bash
coral source add --interactive --file sources/community/deepgram/manifest.yaml
```

## Provider docs

- Deepgram API reference: https://developers.deepgram.com/
- Deepgram models: https://console.deepgram.com/
- Deepgram listen endpoint: https://developers.deepgram.com/reference/speech-to-text/listen-pre-recorded
- Deepgram rate limits: https://developers.deepgram.com/docs/rate-limits
- API key roles and permissions: https://developers.deepgram.com/docs/create-additional-api-keys

## Tables

| Table | Description | Required filters |
| --- | --- | --- |
| `deepgram.models` | Available Deepgram STT models from the Models API. | None |
| `deepgram.transcriptions` | Run one transcription request against an audio URL. | `model`, `url` |

### `deepgram.models`

Lists available STT models from `GET /v1/models`.

```sql
SELECT name, architecture, version, languages
FROM deepgram.models
WHERE architecture = 'polaris'
LIMIT 10;
```

### `deepgram.transcriptions`

Runs a single transcription through `POST /v1/listen`. The audio URL must be
publicly accessible. Use the `nova-3` model for most workloads; fall back to `nova-2` if `nova-3` is unavailable for your region.

```sql
SELECT transcript, confidence, duration, request_id
FROM deepgram.transcriptions
WHERE model = 'nova-2'
  AND url = 'https://example.com/audio.wav'
LIMIT 1;
```

Optional filters include booleans for feature toggles (`punctuate`, `smart_format`,
`summarize`, `sentiment`, `diarize`, `topics`, `intents`, `detect_entities`, `multichannel`,
`paragraphs`), strings for language, search/replace/redact, and encoding values, and
numeric (`sample_rate`) for raw-audio encoding. Channel-specific columns use
`channels[0]`; query `channels_raw` to inspect all channels when `multichannel` is enabled:

```sql
SELECT transcript, summary, sentiment
FROM deepgram.transcriptions
WHERE model = 'nova-2'
  AND url = 'https://example.com/audio.wav'
  AND punctuate = true
  AND smart_format = true
  AND summarize = true
  AND sentiment = true
  AND language = 'en'
LIMIT 1;
```

## Validation

```bash
$ coral source lint sources/community/deepgram/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/deepgram/manifest.yaml
Added source deepgram

  ✓ deepgram connected successfully

    deepgram (2 tables)
    ├─ models
    └─ transcriptions
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT name, architecture, version FROM deepgram.models LIMIT 5
      5 rows

    ✓ SELECT name, canonical_name, languages FROM deepgram.models WHERE architecture = 'polaris' LIMIT 5
      5 rows
```

```bash
$ coral source test deepgram
  ✓ deepgram connected successfully

    deepgram (2 tables)
    ├─ models
    └─ transcriptions
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT name, architecture, version FROM deepgram.models LIMIT 5
      5 rows

    ✓ SELECT name, canonical_name, languages FROM deepgram.models WHERE architecture = 'polaris' LIMIT 5
      5 rows
```

```sql
SELECT table_name, description, required_filters
FROM coral.tables
WHERE schema_name = 'deepgram'
ORDER BY table_name;
```

```text
+----------------+-------------------------------------------------------------------------------------------------------------------------------------------+------------------+
| table_name     | description                                                                                                                               | required_filters |
+----------------+-------------------------------------------------------------------------------------------------------------------------------------------+------------------+
| models         | Available Deepgram speech-to-text models from GET /v1/models.                                                                             |                  |
| transcriptions | Transcribe audio from a URL using Deepgram POST /v1/listen. One SQL row per transcription request; preserves top-level response metadata. | model,url        |
+----------------+-------------------------------------------------------------------------------------------------------------------------------------------+------------------+
```

```sql
SELECT column_name, data_type, is_virtual, is_required_filter
FROM coral.columns
WHERE schema_name = 'deepgram' AND table_name = 'models'
ORDER BY ordinal_position;
```

```text
+------------------+-----------+------------+--------------------+
| column_name      | data_type | is_virtual | is_required_filter |
+------------------+-----------+------------+--------------------+
| name             | Utf8      | false      | false              |
| canonical_name   | Utf8      | false      | false              |
| architecture     | Utf8      | false      | false              |
| languages        | Json      | false      | false              |
| version          | Utf8      | false      | false              |
| uuid             | Utf8      | false      | false              |
| batch            | Boolean   | false      | false              |
| streaming        | Boolean   | false      | false              |
| formatted_output | Boolean   | false      | false              |
| multilingual     | Boolean   | false      | false              |
+------------------+-----------+------------+--------------------+
```

```sql
SELECT column_name, data_type, is_virtual, is_required_filter
FROM coral.columns
WHERE schema_name = 'deepgram' AND table_name = 'transcriptions'
ORDER BY ordinal_position;
```

```text
+--------------+-----------+------------+--------------------+
| column_name  | data_type | is_virtual | is_required_filter |
+--------------+-----------+------------+--------------------+
| model        | Utf8      | true       | true               |
| url          | Utf8      | true       | true               |
| language     | Utf8      | true       | false              |
| request_id   | Utf8      | false      | false              |
| channels     | Int64     | false      | false              |
| channels_raw | Json      | false      | false              |
| duration     | Float64   | false      | false              |
| transcript   | Utf8      | false      | false              |
| confidence   | Float64   | false      | false              |
| words        | Json      | false      | false              |
| paragraphs   | Json      | false      | false              |
| search       | Json      | false      | false              |
| entities     | Json      | false      | false              |
| summary      | Json      | false      | false              |
| topics       | Json      | false      | false              |
| intent       | Json      | false      | false              |
| sentiment    | Json      | false      | false              |
+--------------+-----------+------------+--------------------+
```

```sql
SELECT key, kind, required, is_set
FROM coral.inputs
WHERE schema_name = 'deepgram'
ORDER BY key;
```

```text
+------------------+--------+----------+--------+
| key              | kind   | required | is_set |
+------------------+--------+----------+--------+
| DEEPGRAM_API_KEY | secret | true     | true   |
+------------------+--------+----------+--------+
```

```sql
SELECT name, architecture, version, languages
FROM deepgram.models
LIMIT 10;
```

```text
+------------------+--------------+------------------+----------------+
| name             | architecture | version          | languages      |
+------------------+--------------+------------------+----------------+
| conversationalai | base         | 2021-11-10.1     | ["en","en-US"] |
| automotive       | polaris      | 1983-02-23.4285  | ["en","en-US"] |
| drivethru        | polaris      | 1983-05-08.23433 | ["en","en-US"] |
| finance          | polaris      | 2022-07-27.30495 | ["en","en-US"] |
| general          | polaris      | 2023-11-14.0     | ["taq"]        |
| general          | polaris      | 2023-07-13.28732 | ["en","en-US"] |
| general          | polaris      | 2022-12-08.27973 | ["pt-PT"]      |
| general          | polaris      | 2022-12-08.27925 | ["pt-BR"]      |
| general          | polaris      | 2022-12-08.27689 | ["pt"]         |
| general          | polaris      | 2022-12-08.24015 | ["de"]         |
+------------------+--------------+------------------+----------------+
```

```sql
SELECT architecture, count(*) AS model_count
FROM deepgram.models
GROUP BY architecture
ORDER BY model_count DESC;
```

```text
+--------------+-------------+
| architecture | model_count |
+--------------+-------------+
| nova-2       | 147         |
| nova-3       | 124         |
| base         | 90          |
| polaris      | 27          |
| whisper      | 9           |
| nova         | 9           |
| unknown      | 2           |
+--------------+-------------+
```

```sql
SELECT request_id, channels, duration, transcript, confidence
FROM deepgram.transcriptions
WHERE model = 'nova-2'
  AND url = 'https://dpgr.am/spacewalk.wav'
LIMIT 1;
```

```text
+--------------------------------------+----------+-----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+
| request_id                           | channels | duration  | transcript                                                                                                                                                                                                                                                                                                                        | confidence |
+--------------------------------------+----------+-----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+
| 019e8781-10d5-73c0-9132-ce7591874576 | 1        | 25.933313 | yeah as as much as it's worth celebrating the first spacewalk with an all female team i think many of us are looking forward to it just being normal and i think if it signifies anything it is to honor the the women who came before us who were skilled and qualified and didn't get the same opportunities that we have today | 0.99853516 |
+--------------------------------------+----------+-----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+
```

## Implementation notes

- Uses Coral source-spec DSL v3 with the HTTP backend.
- Uses `HeaderAuth` with `Authorization: Token {{input.DEEPGRAM_API_KEY}}`.
- Maps Deepgram's `stt` array from `GET /v1/models` into `deepgram.models`.
- Maps transcription response onto `deepgram.transcriptions`, including
  `results.channels.[0].alternatives.[0]` fields for transcript and
  `metadata` fields for request metadata. Intelligence features
  (`summary`, `topics`, `intents`, `sentiments`) map to `results.*` at the
  response root level per current Deepgram API shape.
- Transcription request parameters (`model`, `language`, `punctuate`, etc.) are
  sent as query parameters per Deepgram's API contract. Only the `url` is sent
  in the JSON body.
- Sets `fetch_limit_default: 1` on `transcriptions` to prevent accidental API calls.
- Requires `model` and `url` filters on `transcriptions`; audio URL must be publicly accessible.
- Uses current Deepgram parameter names: `detect_entities` (not `ner`), `numerals` (not `numericalize`). Exposes `sample_rate` and `encoding` for raw-audio encoding cases; both should be omitted for containerized audio and used together for headerless raw audio.
- `multichannel` is exposed alongside a `channels_raw` JSON column containing the full `results.channels` array so multi-channel data is queryable even though per-channel columns reference `channels[0]`.
- Rate limits apply per Deepgram account tier. See provider docs for details.
- Does not require runtime, CLI, MCP, or UI changes.

## Limitations

- This source is read/query oriented and does not manage Deepgram account settings.
- `transcriptions` performs a live API call for each query and consumes transcription credits.
- The table requires a publicly accessible audio URL; private URLs or localhost will fail.
- Streaming transcription, custom language models (NLU), and webhook callbacks are not included.
- Responses, available models, pricing, rate limits, and errors depend on the Deepgram
  account, API key permissions, selected model, and current provider limits.

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md), keep the manifest focused,
and include the validation commands plus proof output in the PR description.