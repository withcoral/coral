# Vapi

Query [Vapi](https://vapi.ai) Voice AI data — call logs, assistants, and phone numbers — using SQL.

## Tables

| Table | Endpoint | Description |
|---|---|---|
| `vapi.calls` | `GET /call` | Call logs with status, cost, transcript, and AI analysis |
| `vapi.assistants` | `GET /assistant` | Configured voice AI assistants |
| `vapi.phone_numbers` | `GET /phone-number` | Provisioned phone numbers |

## Setup

1. Go to [https://dashboard.vapi.ai/org/api-keys](https://dashboard.vapi.ai/org/api-keys).
2. Create or copy an existing API key. Any key with read access works.
3. Set the environment variable:

```sh
export VAPI_API_KEY=<your-api-key>
```

4. Add the source:

```sh
coral source add vapi
```

## Example Queries

### Recent calls

```sql
SELECT id, type, status, cost, created_at
FROM vapi.calls
LIMIT 20;
```

### Calls in a time window (date-cursor pagination)

Vapi uses date-cursor pagination. Use `created_at_lt` and `created_at_gt` to page
through large call histories:

```sql
-- Calls from the last 7 days
SELECT id, type, status, cost, started_at, ended_at
FROM vapi.calls
WHERE created_at_gt = '2025-01-01T00:00:00Z'
  AND created_at_lt = '2025-01-08T00:00:00Z'
LIMIT 100;
```

To page forward, set `created_at_lt` to the `created_at` of the oldest row in the
previous result.

### Calls by assistant

```sql
SELECT c.id, c.status, c.cost, a.name AS assistant_name
FROM vapi.calls c
JOIN vapi.assistants a ON c.assistant_id = a.id
LIMIT 20;
```

### Call transcripts

```sql
SELECT id, status, artifact__transcript
FROM vapi.calls
WHERE artifact__transcript IS NOT NULL
LIMIT 5;
```

### Assistants and their models

```sql
SELECT id, name, model__provider, model__model, voice__provider
FROM vapi.assistants;
```

### Phone numbers

```sql
SELECT id, name, number, assistant_id
FROM vapi.phone_numbers;
```

### Calls per phone number

```sql
SELECT p.number, COUNT(*) AS call_count
FROM vapi.calls c
JOIN vapi.phone_numbers p ON c.phone_number_id = p.id
GROUP BY p.number
ORDER BY call_count DESC;
```

## Key Columns

### vapi.calls

| Column | Type | Notes |
|---|---|---|
| `id` | Utf8 | Unique call ID |
| `type` | Utf8 | `inboundPhoneCall`, `outboundPhoneCall`, or `webCall` |
| `status` | Utf8 | `queued`, `ringing`, `in-progress`, `forwarding`, or `ended` |
| `ended_reason` | Utf8 | Why the call ended |
| `cost` | Float64 | Total cost in USD |
| `assistant_id` | Utf8 | Links to `vapi.assistants.id` |
| `phone_number_id` | Utf8 | Links to `vapi.phone_numbers.id` |
| `analysis__summary` | Utf8 | AI-generated call summary (may be null) |
| `artifact__transcript` | Utf8 | Full transcript (may be null) |
| `created_at_gt` | virtual filter | Lower bound for date-cursor pagination |
| `created_at_lt` | virtual filter | Upper bound for date-cursor pagination |

### vapi.assistants

| Column | Type | Notes |
|---|---|---|
| `id` | Utf8 | Unique assistant ID |
| `name` | Utf8 | Human-readable name |
| `model__provider` | Utf8 | LLM provider (e.g. `openai`, `anthropic`) |
| `model__model` | Utf8 | LLM model name (e.g. `gpt-4o`) |
| `voice__provider` | Utf8 | TTS provider (e.g. `11labs`, `deepgram`) |

### vapi.phone_numbers

| Column | Type | Notes |
|---|---|---|
| `id` | Utf8 | Unique phone number ID |
| `number` | Utf8 | E.164 format (e.g. `+14155552671`) |
| `assistant_id` | Utf8 | Default assistant for inbound calls |

## Pagination

Vapi does not use standard offset or cursor pagination. Instead, it accepts
`createdAtGt` and `createdAtLt` query parameters to bound results by creation
time. The maximum page size is 1000 rows.

To page through a large call history:

1. Query with `created_at_lt = <end>` and `created_at_gt = <start>`.
2. Take the `created_at` of the last (oldest) row as the new `created_at_lt`.
3. Repeat until fewer than the requested number of rows are returned.

This source sets `pagination: mode: none` because Coral cannot drive this
date-cursor pattern automatically. Callers control pagination via WHERE filters.
