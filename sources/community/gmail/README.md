# Gmail Source

Query your Gmail mailbox using SQL via the Gmail REST API v1. Designed for
inbox discovery, provider-native search, and cross-source joins with bundled
**Stripe**, **Linear**, and **Intercom** on sender email.

## Setup

### 1. Create a Google OAuth Desktop App

1. Go to https://console.cloud.google.com
2. Create a new project or select existing
3. Go to **APIs & Services** → **Enable APIs**
4. Search and enable **Gmail API**
5. Go to **APIs & Services** → **Credentials**
6. Click **Create Credentials** → **OAuth Client ID**
7. Select **Desktop App**
8. Copy the **Client ID** and **Client Secret**

### 2. Add the Source

```bash
coral source add --interactive --file sources/community/gmail/manifest.yaml
```

When prompted:

- Choose **"Connect Gmail"** for interactive OAuth flow
- Enter your **Client ID** and **Client Secret**
- A browser window will open — sign in and approve access
- Coral stores the access token (and a refresh token when Google returns one)

Or choose **"Paste access token"** if you already have a token from
https://developers.google.com/oauthplayground using scope:
`https://www.googleapis.com/auth/gmail.readonly`

The manifest requests `access_type=offline` and `prompt=consent` so Google
may issue a refresh token on first connect. If queries fail with an expired
token, run `coral source add` again and choose **Connect Gmail** to re-authenticate.

## Tables and functions

| Name | Kind | Description |
| --- | --- | --- |
| `profile` | table | Mailbox email address and counts |
| `labels` | table | System and user labels |
| `messages` | table | List message IDs (optional `label_ids`, `q` filters) |
| `message_details` | table | Per-message From/Subject/Date metadata (`message_id` required) |
| `threads` | table | List threads with snippet |
| `drafts` | table | Draft IDs |
| `search_messages` | search function | Gmail-native search via `q` argument |

`messages` and `drafts` return IDs for discovery. Use `message_details` for
join-friendly headers, or `search_messages` for provider-native search syntax.

## Example queries

### Profile and labels

```sql
SELECT email_address, messages_total, threads_total
FROM gmail.profile;

SELECT id, name, type
FROM gmail.labels;
```

### Inbox discovery

```sql
SELECT id, thread_id
FROM gmail.messages
WHERE label_ids = 'INBOX'
LIMIT 20;
```

### Provider-native search

```sql
SELECT id, thread_id
FROM gmail.search_messages(q => 'from:stripe.com newer_than:7d')
LIMIT 20;

SELECT id, thread_id
FROM gmail.search_messages(q => 'is:unread subject:invoice')
LIMIT 10;
```

### Message metadata (single message)

```sql
SELECT message_id, from_header, subject, internal_date
FROM gmail.message_details
WHERE message_id = '<message-id-from-gmail.messages>'
LIMIT 1;
```

### Join discovery IDs to metadata

```sql
SELECT m.id, d.from_header, d.subject, d.internal_date
FROM gmail.messages m
JOIN gmail.message_details d ON d.message_id = m.id
WHERE m.label_ids = 'INBOX'
LIMIT 20;
```

Extract an email from `from_header` (angle-bracket, bare address, or embedded):

```sql
SELECT
  m.id,
  d.subject,
  COALESCE(
    regexp_match(d.from_header, '<([^>]+)>')[1],
    regexp_match(d.from_header, '([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,})')[1],
    TRIM(d.from_header)
  ) AS from_email
FROM gmail.messages m
JOIN gmail.message_details d ON d.message_id = m.id
WHERE m.label_ids = 'INBOX'
LIMIT 20;
```

## Cross-source joins

`from_header` may be `Name <addr@domain>`, a bare `addr@domain`, or text with an
embedded address. Examples below use `COALESCE` so joins do not silently drop
rows when Gmail omits angle brackets.

Reference GitHub issue #1080 when contributing changes. Example relationships:

```text
gmail.message_details.from_header (parsed email)
  → stripe.customers.email
  → linear.users.email
  → intercom.contacts.email
```

### Stripe customers who emailed you recently

Requires bundled Stripe source and parsed `from_email`:

```sql
WITH mail AS (
  SELECT
    m.id AS message_id,
    d.subject,
    COALESCE(
      regexp_match(d.from_header, '<([^>]+)>')[1],
      regexp_match(d.from_header, '([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,})')[1],
      TRIM(d.from_header)
    ) AS from_email
  FROM gmail.search_messages(q => 'newer_than:30d') m
  JOIN gmail.message_details d ON d.message_id = m.id
)
SELECT from_email, subject, s.id AS stripe_customer_id, s.name
FROM mail
JOIN stripe.customers s ON LOWER(s.email) = LOWER(mail.from_email)
WHERE mail.from_email IS NOT NULL
LIMIT 20;
```

### Linear users matching Gmail senders

```sql
WITH mail AS (
  SELECT
    m.id AS message_id,
    COALESCE(
      regexp_match(d.from_header, '<([^>]+)>')[1],
      regexp_match(d.from_header, '([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,})')[1],
      TRIM(d.from_header)
    ) AS from_email
  FROM gmail.messages m
  JOIN gmail.message_details d ON d.message_id = m.id
  WHERE m.label_ids = 'INBOX'
)
SELECT from_email, u.name AS linear_name, u.email AS linear_email
FROM mail
JOIN linear.users u ON LOWER(u.email) = LOWER(mail.from_email)
WHERE mail.from_email IS NOT NULL
LIMIT 20;
```

### Intercom contacts not in recent inbox (illustrative)

```sql
SELECT i.email, i.name
FROM intercom.contacts i
WHERE i.email IS NOT NULL
LIMIT 50;
```

Combine with Gmail search results in your workspace to find contacts who have
not appeared in recent mail.

## Auth scopes

This source uses `gmail.readonly`, a **restricted** Gmail scope.

**Why not `gmail.metadata`?** The `messages` and `threads` tables and
`search_messages` use the Gmail `q` parameter, which requires at least
`gmail.readonly` per [Gmail API scopes](https://developers.google.com/workspace/gmail/api/auth/scopes).

Public apps need Google OAuth verification. Personal or internal use can stay
unverified.

## Rate limits

| Limit type | Quota units |
| --- | --- |
| Per minute per project | 1,200,000 |
| Per minute per user per project | 6,000 |

| Method | Quota units |
| --- | --- |
| `messages.list` / `search_messages` | 5 |
| `messages.get` / `message_details` | 5 |
| `drafts.list` | 5 |
| `threads.list` | 10 |
| `labels.list` | 1 |
| `getProfile` | 1 |

Each `message_details` row costs one `messages.get` call. Prefer `LIMIT` on
joins over large unbounded scans.

Full details: https://developers.google.com/workspace/gmail/api/reference/quota

## Limitations

- Read-only (`gmail.readonly`); no send, delete, or label changes
- No full MIME body or attachment bytes in v1
- `from_header` is the raw From header; use `COALESCE` + `regexp_match` in SQL for joins
- `message_details` requires an explicit `message_id` filter per fetch

## Validation

```bash
make lint-sources
coral source lint sources/community/gmail/manifest.yaml
coral source add --interactive --file sources/community/gmail/manifest.yaml
coral source test gmail
```

## Provider docs

- Gmail API: https://developers.google.com/workspace/gmail/api/reference/rest
- Auth scopes: https://developers.google.com/workspace/gmail/api/auth/scopes
- Search operators: https://support.google.com/mail/answer/7190
