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
coral source add --file sources/community/vapi/manifest.yaml
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

### Calls by phone number

```sql
-- Scope calls to a specific phone number without scanning all calls
SELECT id, type, status, cost, created_at
FROM vapi.calls
WHERE phone_number_id_filter = '<your-phone-number-id>'
LIMIT 50;
```

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
FROM vapi.assistants
LIMIT 100;
```

### All assistants (page through large accounts)

```sql
-- First page
SELECT id, name, created_at FROM vapi.assistants LIMIT 100;

-- Next page: use the created_at of the last row as created_at_lt
SELECT id, name, created_at
FROM vapi.assistants
WHERE created_at_lt = '<created_at of last row>'
LIMIT 100;
```

### Phone numbers

```sql
SELECT id, name, number, assistant_id
FROM vapi.phone_numbers
LIMIT 100;
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
| `type` | Utf8 | Known values include `inboundPhoneCall`, `outboundPhoneCall`, `webCall`, `vapi.websocketCall`; Vapi may add more |
| `status` | Utf8 | Known values include `queued`, `ringing`, `in-progress`, `forwarding`, `ended`; additional non-happy-path values exist |
| `ended_reason` | Utf8 | Why the call ended |
| `cost` | Float64 | Total cost in USD |
| `assistant_id` | Utf8 | Links to `vapi.assistants.id` |
| `phone_number_id` | Utf8 | Links to `vapi.phone_numbers.id` |
| `analysis__summary` | Utf8 | AI-generated call summary (may be null) |
| `artifact__transcript` | Utf8 | Full transcript (may be null) |
| `created_at_gt` | virtual filter | Lower bound for date-cursor pagination |
| `created_at_lt` | virtual filter | Upper bound for date-cursor pagination |
| `assistant_id_filter` | virtual filter | Filter by assistant ID |
| `phone_number_id_filter` | virtual filter | Filter by phone number ID |
| `limit` | virtual filter | Max rows (default 100, max 1000) |

### vapi.assistants

| Column | Type | Notes |
|---|---|---|
| `id` | Utf8 | Unique assistant ID |
| `name` | Utf8 | Human-readable name |
| `model__provider` | Utf8 | LLM provider (e.g. `openai`, `anthropic`) |
| `model__model` | Utf8 | LLM model name (e.g. `gpt-4o`) |
| `voice__provider` | Utf8 | TTS provider (e.g. `11labs`, `deepgram`) |
| `created_at_gt` | virtual filter | Lower bound for pagination |
| `created_at_lt` | virtual filter | Upper bound for pagination |
| `limit` | virtual filter | Max rows (default 100, max 1000) |

### vapi.phone_numbers

| Column | Type | Notes |
|---|---|---|
| `id` | Utf8 | Unique phone number ID |
| `number` | Utf8 | E.164 format (e.g. `+14155552671`) |
| `assistant_id` | Utf8 | Default assistant for inbound calls |
| `created_at_gt` | virtual filter | Lower bound for pagination |
| `created_at_lt` | virtual filter | Upper bound for pagination |
| `limit` | virtual filter | Max rows (default 100, max 1000) |

## Pagination

Vapi does not use standard offset or cursor pagination. Instead, it accepts
`createdAtGt` and `createdAtLt` query parameters to bound results by creation
time. All three list endpoints default to **100 rows** per request and accept up
to **1000** via the `limit` filter.

To page through a large call history:

1. Query with `created_at_lt = <end>` and `created_at_gt = <start>`.
2. Take the `created_at` of the last (oldest) row as the new `created_at_lt`.
3. Repeat until fewer than the requested number of rows are returned.

The same pattern applies to `vapi.assistants` and `vapi.phone_numbers`.

This source sets `pagination: mode: none` because Coral cannot drive this
date-cursor pattern automatically. Callers control pagination via WHERE filters.
