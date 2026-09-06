# AssemblyAI

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 1
**Functions:** 1

Query speech-to-text transcripts from AssemblyAI. List transcripts with status and metadata, or retrieve full transcript text with confidence scores, speaker labels, and audio duration.

## Installation

Install the source via the CLI:

```bash
coral source add --file sources/community/assemblyai/manifest.yaml
```

## Credentials

To use this source, you will need an AssemblyAI API key.

1. Sign up at [assemblyai.com](https://www.assemblyai.com) (free, no credit card required).
2. Navigate to your [account page](https://www.assemblyai.com/app/account).
3. Copy your API key.
4. Provide it when prompted by `coral source add` or set it as an environment variable:

```bash
export ASSEMBLYAI_API_KEY="your-api-key"
```

The free tier includes 100 hours of transcription.

## Quick Start

```sql
-- List all transcripts
SELECT id, status, audio_url, created
FROM assemblyai.transcripts
LIMIT 10;

-- List only completed transcripts
SELECT id, audio_url, created, completed
FROM assemblyai.transcripts
WHERE status = 'completed'
LIMIT 10;

-- Get full transcript text for a specific job
SELECT id, status, text, audio_duration, confidence, language_code
FROM assemblyai.transcript(id => 'your-transcript-id');

-- Get word-level data with timestamps
SELECT id, words
FROM assemblyai.transcript(id => 'your-transcript-id');
```

## Tables

### `transcripts`

Speech-to-text transcripts created in your AssemblyAI account. Sorted from newest to oldest. Transcripts are available for the last 90 days.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `status` | Utf8 | | Filter by status: `queued`, `processing`, `completed`, `error` |
| `created_on` | Utf8 | | Only transcripts created on this date (YYYY-MM-DD) |
| `before_id` | Utf8 | | Get transcripts created before this transcript ID (cursor pagination) |
| `after_id` | Utf8 | | Get transcripts created after this transcript ID (cursor pagination) |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `id` | Utf8 | Unique identifier for the transcript |
| `status` | Utf8 | Status: queued, processing, completed, error |
| `audio_url` | Utf8 | URL of the audio/video file that was transcribed |
| `resource_url` | Utf8 | URL to retrieve the full transcript resource |
| `created` | Utf8 | When the transcript was created (datetime string) |
| `completed` | Utf8 | When the transcript was completed (datetime string) |
| `error` | Utf8 | Error message if the transcript failed |

## Functions

### `assemblyai.transcript`

Retrieve the full transcript for a specific transcription job. Pass the transcript ID as a named argument with `id => '<transcript_id>'`.

**Arguments**

| Argument | Type | Description |
|----------|------|-------------|
| `id` | Utf8 | (Required) Transcript ID |

**Result columns**

| Column | Type | Description |
|--------|------|-------------|
| `id` | Utf8 | Unique identifier for the transcript |
| `status` | Utf8 | Status: queued, processing, completed, error |
| `text` | Utf8 | Full transcribed text of the audio/video file |
| `audio_url` | Utf8 | URL of the audio/video file |
| `audio_duration` | Int64 | Duration of the audio in seconds |
| `confidence` | Float64 | Confidence score (0.0 to 1.0) |
| `language_code` | Utf8 | Language code (e.g. en_us, en) |
| `language_confidence` | Float64 | Confidence score for detected language (0.0 to 1.0) |
| `words` | Json | Word-level data with timestamps and confidence (JSON array) |
| `utterances` | Json | Speaker-labeled utterances with timestamps (JSON array) |
| `error` | Utf8 | Error message if the transcript failed |

## Live request costs

Each query performs live API calls to `https://api.assemblyai.com`. Listing transcripts (`GET /v2/transcript`) does not consume transcription credits. Retrieving a transcript (`GET /v2/transcript/{id}`) also does not consume credits — transcription credits are consumed only when submitting new audio via `POST /v2/transcript`, which is not modeled in this source. SQL `LIMIT` is pushed to the API via `limit` query param (default 10, max 200).

## Source scope

- Targets the AssemblyAI API at `https://api.assemblyai.com`. The EU endpoint (`https://api.eu.assemblyai.com`) is not supported in this version.
- Requires `ASSEMBLYAI_API_KEY` authentication via raw API key in the `Authorization` header (not Bearer).
- The `transcripts` table lists transcript metadata sorted newest to oldest, available for the last 90 days.
- The `transcript` function retrieves full transcript details including text, word-level data, and speaker utterances.
- Read-only access. Submitting new transcripts (`POST /v2/transcript`) is intentionally out of scope.
- 1 declared test query (`transcripts`) is source-independent.

## Limitations

- The source provides read-only access only. Submitting audio for transcription, deleting transcripts, and other write operations are out of scope.
- The `transcripts` table returns list metadata only (id, status, audio_url, created, completed, error). Full text requires the `transcript` function.
- Pagination uses `before_id`/`after_id` cursor filters (exposed as optional filters). A single query returns at most 200 transcripts. Use `before_id` or `after_id` to page through older/newer results.
- The `words` and `utterances` columns in the `transcript` function return large JSON arrays for long audio. Use `text` for plain transcript content.
- Speaker diarization data (`utterances`) is only populated when `speaker_labels` was enabled during transcription.
- Transcripts older than 90 days are not available via the API.
- Rate limits apply based on your AssemblyAI plan.

## Provider docs

- AssemblyAI quickstart: https://www.assemblyai.com/docs/getting-started/transcribe-an-audio-file
- List transcripts API: https://www.assemblyai.com/docs/api-reference/transcripts/list
- Get transcript API: https://www.assemblyai.com/docs/api-reference/transcripts/get
- API keys: https://www.assemblyai.com/app/account

## Live validation output

Validated against a live AssemblyAI account with a valid `ASSEMBLYAI_API_KEY`.

```bash
$ coral source lint sources/community/assemblyai/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/assemblyai/manifest.yaml
Added source assemblyai

  ✓ assemblyai connected successfully

    assemblyai (1 table)
    └─ transcripts
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT id, status, audio_url, created FROM assemblyai.transcripts LIMIT 3
      1 row
```

**Table introspection:**

```sql
SELECT table_name, description, required_filters
FROM coral.tables
WHERE schema_name = 'assemblyai'
ORDER BY table_name;
```

```text
+-------------+----------------------------------------------------------------------------------------------------------------------------------------------+------------------+
| table_name  | description                                                                                                                                  | required_filters |
+-------------+----------------------------------------------------------------------------------------------------------------------------------------------+------------------+
| transcripts | Speech-to-text transcripts created in your AssemblyAI account. Sorted from newest to oldest. Transcripts are available for the last 90 days. |                  |
+-------------+----------------------------------------------------------------------------------------------------------------------------------------------+------------------+
```

**Function introspection:**

```sql
SELECT function_name, kind, arguments_json
FROM coral.table_functions
WHERE schema_name = 'assemblyai';
```

```text
+---------------+-------+---------------------------------------------+
| function_name | kind  | arguments_json                              |
+---------------+-------+---------------------------------------------+
| transcript    | table | [{"name":"id","required":true,"values":[]}] |
+---------------+-------+---------------------------------------------+
```

**Inputs introspection:**

```sql
SELECT key, kind, required, is_set
FROM coral.inputs
WHERE schema_name = 'assemblyai'
ORDER BY key;
```

```text
+--------------------+--------+----------+--------+
| key                | kind   | required | is_set |
+--------------------+--------+----------+--------+
| ASSEMBLYAI_API_KEY | secret | true     | true   |
+--------------------+--------+----------+--------+
```

```bash
$ coral source test assemblyai
  ✓ assemblyai connected successfully
  Secrets: keychain
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT id, status, audio_url, created FROM assemblyai.transcripts LIMIT 3
      1 row
```

**Live transcripts proof:**

```sql
SELECT id, status, audio_url, created
FROM assemblyai.transcripts LIMIT 3;
```

```text
+--------------------------------------+-----------+-----------------------------------+-----------------------------+
| id                                   | status    | audio_url                         | created                     |
+--------------------------------------+-----------+-----------------------------------+-----------------------------+
| e6846d4e-0000-0000-0000-50130cac87f4 | completed | https://assembly.ai/wildfires.mp3 | 2026-07-09T23:55:35.034339Z |
+--------------------------------------+-----------+-----------------------------------+-----------------------------+
```

**Live transcript detail proof:**

```sql
SELECT id, status, audio_duration, confidence, language_code
FROM assemblyai.transcript(id => 'e6846d4e-0000-0000-0000-50130cac87f4');
```

```text
+--------------------------------------+-----------+----------------+------------+---------------+
| id                                   | status    | audio_duration | confidence | language_code |
+--------------------------------------+-----------+----------------+------------+---------------+
| e6846d4e-0000-0000-0000-50130cac87f4 | completed | 282            | 0.98088217 | en            |
+--------------------------------------+-----------+----------------+------------+---------------+
```
