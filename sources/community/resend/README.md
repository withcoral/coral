# Resend

Query sent emails, domains, API keys, contacts, segments, topics,
broadcasts, webhooks, API request logs, and legacy audiences from Resend.

## Setup

### Get Your API Key

1. Log in to the [Resend Dashboard](https://resend.com)
2. Navigate to **API Keys** at https://resend.com/api-keys
3. Create a new API key with **Full access** permission
4. Copy the key (starts with `re_`)

### Add the Source

```bash
export RESEND_API_KEY="re_your_api_key_here"
coral source add --file sources/community/resend/manifest.yaml
```

## Tables

### `emails`

Lists emails sent by the authenticated team. Returns sender,
recipients, subject, delivery status, and timestamps.

**Useful for:**

- Monitoring email delivery status
- Auditing sent emails and recipients
- Tracking open and click rates via `last_event`

**Example:**

```sql
SELECT id, "from", subject, last_event, created_at
FROM resend.emails
LIMIT 20;
```

### `domains`

Lists all domains registered in the Resend account. Returns domain
name, verification status, region, and capabilities.

**Useful for:**

- Domain inventory and verification status
- Checking sending and receiving capabilities
- Reviewing regional deployment

### `api_keys`

Lists all API keys for the authenticated user. Returns key name,
creation time, and last usage timestamp. The actual key value is
never returned after creation.

**Useful for:**

- Auditing API key usage and identifying unused keys
- Security review of active keys

**Example:**

```sql
SELECT id, name, created_at, last_used_at
FROM resend.api_keys;
```

### `audiences`

Lists legacy audiences for accounts that still have audience resources.
Resend's current model uses global contacts plus segments.

**Useful for:**

- Legacy audience inventory
- Migration checks when moving from audiences to segments

### `segments`

Lists all segments for the authenticated account. Segments group contacts
for broadcast targeting.

**Useful for:**

- Segment inventory
- Resolving `segment_id` values from broadcasts
- Reviewing campaign targeting groups

### `contacts`

Lists global contacts for the authenticated account. Returns email
address, name, subscription status, and creation timestamp.

**Example:**

```sql
SELECT id, email, first_name, last_name, unsubscribed
FROM resend.contacts
LIMIT 20;
```

### `broadcasts`

Lists all broadcasts (bulk email campaigns). Returns status,
audience/segment targeting, schedule, and send timestamps.

**Useful for:**

- Monitoring broadcast campaign status
- Auditing scheduled and sent campaigns
- Reviewing segment and topic targeting

**Example:**

```sql
SELECT id, status, segment_id, topic_id, scheduled_at, sent_at
FROM resend.broadcasts;
```

### `topics`

Lists subscription topics for the authenticated account. Topics let
contacts manage preferences for different kinds of email.

**Useful for:**

- Topic inventory
- Resolving `topic_id` values from broadcasts
- Auditing public subscription preferences

**Example:**

```sql
SELECT id, name, default_subscription, visibility
FROM resend.topics;
```

### `webhooks`

Lists all webhook endpoints configured in the account. Returns
endpoint URL, enabled/disabled status, and subscribed event types.

**Useful for:**

- Auditing webhook integrations
- Identifying disabled or misconfigured endpoints

**Example:**

```sql
SELECT id, endpoint, status, events, created_at
FROM resend.webhooks;
```

### `logs`

Lists API request logs for the account. Returns endpoint, HTTP method,
response status code, user agent, and timestamp.

**Useful for:**

- Debugging API integration issues
- Auditing API request patterns and error rates
- Identifying unexpected clients or user agents

**Example:**

```sql
SELECT id, endpoint, method, response_status, created_at
FROM resend.logs
LIMIT 20;
```

## Authentication

The source uses Bearer token authentication with your Resend API key.
The key is sent as a `secret` input and never exposed in query results.

**Important:** Resend requires a `User-Agent` header on all API
requests. Coral's HTTP client includes one by default.

## Limits

- Resend's paginated list endpoints support up to 100 items per request.
- Resend uses cursor-based pagination (`after`/`before` with item IDs),
  which requires extracting the last item's ID from the response data
  array. This is not currently supported by Coral's `cursor_query`
  pagination mode. Each table returns the first page of results.
- For `emails` and `contacts`, this means the most recent 100 items.
  Most accounts will have fewer than 100 domains, legacy audiences,
  segments, topics, API keys, webhooks, and broadcasts.
- Timestamps are returned as ISO 8601 strings.
- The default rate limit is 5 requests per second per team.

## Example Queries

### List all domains with their verification state

```sql
SELECT id, name, status, region, capabilities
FROM resend.domains;
```

### Review recent email delivery status

```sql
SELECT id, "from", subject, last_event, created_at
FROM resend.emails
LIMIT 20;
```

### Find bounced emails

```sql
SELECT id, "from", subject, created_at
FROM resend.emails
WHERE last_event = 'bounced';
```

### Audit API key usage

```sql
SELECT name, created_at, last_used_at
FROM resend.api_keys;
```

### List recent contacts

```sql
SELECT email, first_name, last_name, unsubscribed, created_at
FROM resend.contacts
LIMIT 20;
```

### Check unsubscribed contacts

```sql
SELECT email, first_name, last_name, created_at
FROM resend.contacts
WHERE unsubscribed = true;
```

### Review broadcast campaigns

```sql
SELECT id, status, segment_id, topic_id, scheduled_at, sent_at, created_at
FROM resend.broadcasts;
```

### Resolve broadcast segments and topics

```sql
SELECT id, name, created_at
FROM resend.segments;

SELECT id, name, default_subscription, visibility
FROM resend.topics;
```

### Audit webhooks

```sql
SELECT endpoint, status, events, created_at
FROM resend.webhooks;
```

### Check API error rates

```sql
SELECT endpoint, method, response_status, user_agent, created_at
FROM resend.logs
WHERE response_status >= 400
LIMIT 20;
```

## Notes

- The source is read-only — no send, create, update, or delete
  operations
- API keys are never exposed in query results; the `api_keys` table
  only shows metadata (name, creation time, last usage)
- The `from` column in the `emails` table is a reserved SQL keyword;
  use double quotes (`"from"`) in queries
- Resend returns timestamps as ISO 8601 strings with timezone offsets
- The `to`, `cc`, `bcc`, and `reply_to` fields in emails are JSON
  arrays since emails can have multiple recipients
- Cursor pagination support may be added in a future version to
  enable paginating through large email and contact lists
