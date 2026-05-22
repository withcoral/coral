# Mailgun

Query sending domains, SMTP credentials, inbound routes, and email
suppression lists (bounces, unsubscribes, complaints) from Mailgun.

## Setup

### Get Your API Key

1. Log in to the [Mailgun Dashboard](https://app.mailgun.com)
2. Navigate to **Account Settings → API Security**
3. Copy your **Private API Key**

### Add the Source

```bash
export MAILGUN_API_KEY="key-your-private-api-key"
coral source add --file sources/community/mailgun/manifest.yaml
```

For EU-hosted domains, also set the base URL before adding the source:

```bash
export MAILGUN_BASE_URL="https://api.eu.mailgun.net"
```

## Tables

### `domains`

Lists all sending domains for the Mailgun account. Returns domain
verification state, type, SMTP login, security, and tracking settings.

**Useful for:**

- Domain inventory and verification status
- Reviewing automatic sender security and tracking settings
- Checking TLS requirements and spam action policies

### `credentials`

Lists SMTP credentials for a specific sending domain. Returns login
usernames and creation timestamps. Passwords are never returned by
the API.

**Requires:** `domain` filter (from `domains`)

**Example:**

```sql
SELECT domain, login, mailbox, created_at
FROM mailgun.credentials
WHERE domain = 'example.com';
```

### `routes`

Lists all inbound routing rules for the Mailgun account. Routes are
global (not domain-specific) and define match expressions and actions
for incoming messages.

**Useful for:**

- Auditing inbound email routing rules
- Reviewing forwarding destinations and priorities
- Checking for stale or unused routes

### `bounces`

Lists bounced email addresses for a specific sending domain. Bounces
are addresses that have permanently failed delivery and are
automatically suppressed from future sends.

**Requires:** `domain` filter (from `domains`)

**Useful for:**

- Reviewing delivery failures and SMTP error codes
- Auditing suppression lists for email deliverability
- Identifying problematic recipient addresses

**Example:**

```sql
SELECT address, code, error, created_at
FROM mailgun.bounces
WHERE domain = 'example.com';
```

### `unsubscribes`

Lists email addresses that have unsubscribed from a specific sending
domain. These addresses are automatically suppressed from future
sends.

**Requires:** `domain` filter (from `domains`)

**Useful for:**

- Monitoring unsubscribe rates
- Auditing suppression lists
- Reviewing tag-specific unsubscribes

**Example:**

```sql
SELECT address, tags, created_at
FROM mailgun.unsubscribes
WHERE domain = 'example.com';
```

### `complaints`

Lists email addresses that have filed spam complaints for a specific
sending domain. These addresses are automatically suppressed from
future sends.

**Requires:** `domain` filter (from `domains`)

**Example:**

```sql
SELECT address, created_at
FROM mailgun.complaints
WHERE domain = 'example.com';
```

## Authentication

The source uses HTTP Basic Authentication with username `api` and
your Private API Key as the password. The key is sent as a `secret`
input and never exposed in query results.

## Region Support

Mailgun operates in two regions. Set the `MAILGUN_BASE_URL` input
to match your domain's region:

| Region | Base URL |
|---|---|
| US (default) | `https://api.mailgun.net` |
| EU | `https://api.eu.mailgun.net` |

## Limits

- `domains`, `credentials`, and `routes` are account-wide; no domain filter required
  (except `credentials` which is per-domain).
- `bounces`, `unsubscribes`, `complaints`, and `credentials` require
  a `domain` filter — they query one domain at a time.
- All list endpoints use `skip`/`limit` offset pagination. The source
  defaults to 100 per page with a max of 1000.
- Timestamps are returned as strings in RFC 2822 format, not as
  epoch milliseconds.

## Example Queries

### List all domains with their verification state

```sql
SELECT name, id, state, type, smtp_login,
       require_tls, use_automatic_sender_security
FROM mailgun.domains;
```

### Find unverified domains

```sql
SELECT name, state, created_at
FROM mailgun.domains
WHERE state != 'active';
```

### List all inbound routes sorted by priority

```sql
SELECT id, priority, expression,
       actions, description
FROM mailgun.routes
ORDER BY priority;
```

### Review bounce errors for a domain

```sql
SELECT address, code, error, created_at
FROM mailgun.bounces
WHERE domain = 'example.com';
```

### Check unsubscribes for a domain

```sql
SELECT address, tags, created_at
FROM mailgun.unsubscribes
WHERE domain = 'example.com';
```

### List spam complaints

```sql
SELECT address, created_at
FROM mailgun.complaints
WHERE domain = 'example.com';
```

### Check SMTP credentials

```sql
SELECT login, mailbox, created_at
FROM mailgun.credentials
WHERE domain = 'example.com';
```

## Notes

- All endpoints use the Mailgun v3 REST API with HTTP Basic
  Authentication (`api:<private_key>`), except domains inventory which
  uses Mailgun's current v4 domains endpoint
- The `base_url` is configurable via the `MAILGUN_BASE_URL` input
  to support both US and EU regions
- Timestamps are returned as Utf8 strings in RFC 2822 format (e.g.
  "Thu, 15 May 2025 12:00:00 UTC") because Mailgun does not use
  Unix epoch timestamps
- The Events API (email delivery, open, click tracking) uses
  cursor-based pagination with full URL cursors, which is not yet
  supported. It may be added in a future version
- SMTP passwords are never exposed — the `credentials` table only
  returns login usernames
- The `domains` table does not expose `smtp_password` to prevent
  credential leakage through SQL results
