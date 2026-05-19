# Postmark

Query server metadata, message streams, messages, bounces, templates, and
outbound delivery statistics from Postmark.

## Setup

### Get Your Postmark Server Token

Create or copy a server API token from the API Tokens tab for the Postmark
server you want to query. This source uses server-level read endpoints with the
`X-Postmark-Server-Token` header.

Postmark documents API authentication at
https://postmarkapp.com/developer/api/overview.

### Add the Source

```bash
POSTMARK_SERVER_TOKEN=server_api_token \
coral source add --file sources/community/postmark/manifest.yaml
```

## Authentication

The source sends `POSTMARK_SERVER_TOKEN` in the
`X-Postmark-Server-Token` request header. The token determines which Postmark
server Coral can query.

This v1 source does not use Postmark account tokens and does not query
account-level server inventory.

## Tables

### `server`

Returns one row with metadata for the Postmark server associated with the
server token. Use this table to verify credentials and identify the server.

The source intentionally does not expose server API token values returned by the
provider.

**Example:**

```sql
SELECT id, name, delivery_type, track_opens, track_links
FROM postmark.server
LIMIT 1;
```

### `message_streams`

Returns message streams configured for the authenticated Postmark server.

Optional filters:

- `message_stream_type`, such as `All`, `Inbound`, `Transactional`, or
  `Broadcasts`
- `include_archived_streams`

**Example:**

```sql
SELECT id, name, message_stream_type, created_at, archived_at
FROM postmark.message_streams
WHERE include_archived_streams = true
ORDER BY name;
```

### `outbound_messages`

Returns outbound message search results. Postmark searches are offset-paginated
with `count` and `offset`, and a single search can return up to 10,000
messages. Use date and other filters for larger investigations.

Optional filters:

- `recipient`
- `from_email`
- `tag`
- `status`
- `to_date`
- `from_date`
- `subject`
- `message_stream`

**Example:**

```sql
SELECT message_id, message_stream, from_email, subject, status, received_at
FROM postmark.outbound_messages
WHERE from_date = '2026-05-01'
  AND to_date = '2026-05-16'
  AND message_stream = 'outbound'
ORDER BY received_at DESC
LIMIT 100;
```

### `inbound_messages`

Returns inbound message search results. The inbound message `date` column
preserves Postmark's email Date header value as text.

Optional filters:

- `recipient`
- `from_email`
- `tag`
- `subject`
- `mailbox_hash`
- `status`
- `to_date`
- `from_date`

**Example:**

```sql
SELECT message_id, from_email, to_email, subject, status, date
FROM postmark.inbound_messages
WHERE status = 'processed'
LIMIT 100;
```

### `bounces`

Returns bounce records from Postmark bounce search.

Optional filters:

- `type`
- `inactive`
- `email_filter`
- `tag`
- `message_id`
- `from_date`
- `to_date`
- `message_stream`

**Example:**

```sql
SELECT id, type, email, from_email, bounced_at, inactive, can_activate
FROM postmark.bounces
WHERE inactive = true
ORDER BY bounced_at DESC
LIMIT 100;
```

### `templates`

Returns templates associated with the authenticated Postmark server.

Optional filters:

- `template_type`, such as `All`, `Standard`, or `Layout`
- `layout_template`

**Example:**

```sql
SELECT template_id, name, alias, template_type, active
FROM postmark.templates
WHERE template_type = 'Standard'
ORDER BY name;
```

### `outbound_stats`

Returns one row with aggregate outbound email statistics for the authenticated
server. Postmark stats use EST timezone and return all-time stats when no date
filters are supplied.

Optional filters:

- `tag`
- `from_date`
- `to_date`
- `message_stream`

**Example:**

```sql
SELECT sent, bounced, bounce_rate, spam_complaints, opens, total_clicks
FROM postmark.outbound_stats
WHERE from_date = '2026-05-01'
  AND to_date = '2026-05-16'
  AND message_stream = 'outbound';
```

## Limits

- This source is read-only.
- `outbound_messages`, `inbound_messages`, and `bounces` are Postmark search
  endpoints capped at 10,000 records per search. Use `from_date`, `to_date`,
  and other filters to inspect larger histories.
- Postmark messages expire after the server's retention period. The default
  retention period is 45 days, but Postmark can be configured from 7 to 365
  days.
- Raw message dumps, bounce dumps, message details, inbound bypass/retry,
  sending, template writes, server edits, stream writes, webhooks,
  suppressions, and data removal operations are not included in v1.
- Postmark's dynamic outbound `metadata_` search parameters are not exposed in
  v1 because Coral source-spec query parameter names are static.
- Nested fields such as recipients, attachments, metadata, sender/recipient
  objects, and subscription management configuration are exposed as `Json`.

## Public API Mapping

| Table | Postmark endpoint | Response rows | Pagination |
|---|---|---|---|
| `server` | `GET /server` | response object | none |
| `message_streams` | `GET /message-streams` | `MessageStreams` | none documented |
| `outbound_messages` | `GET /messages/outbound` | `Messages` | `count` + `offset` |
| `inbound_messages` | `GET /messages/inbound` | `InboundMessages` | `count` + `offset` |
| `bounces` | `GET /bounces` | `Bounces` | `count` + `offset` |
| `templates` | `GET /templates` | `Templates` | `count` + `offset` |
| `outbound_stats` | `GET /stats/outbound` | response object | none |

Primary Postmark docs:

- https://postmarkapp.com/developer/api/overview
- https://postmarkapp.com/developer/api/server-api
- https://postmarkapp.com/developer/api/message-streams-api
- https://postmarkapp.com/developer/api/messages-api
- https://postmarkapp.com/developer/api/bounce-api
- https://postmarkapp.com/developer/api/templates-api
- https://postmarkapp.com/developer/api/stats-api
