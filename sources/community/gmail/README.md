# Gmail Source

Query your Gmail mailbox using SQL via the Gmail REST API v1. Designed for
inbox discovery, provider-native search, and cross-source workflows with bundled
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

`messages` and `drafts` return IDs for discovery. Use `search_messages` for
provider-native search, then `message_details` with a **literal** `message_id`
to read From/Subject metadata for one message at a time.

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

### Message metadata (two-step workflow)

Gmail uses `messages.list` / `search_messages` for IDs and `messages.get` for
one message at a time. Coral's `message_details` table maps to `messages.get` and
requires a **literal** `message_id` in `WHERE` (not a value from a `JOIN`).

**Step 1 — discover ids:**

```sql
SELECT id, thread_id
FROM gmail.search_messages(q => 'in:inbox')
LIMIT 10;
```

**Step 2 — metadata for one id (paste from step 1):**

```sql
SELECT message_id, from_header, subject, internal_date
FROM gmail.message_details
WHERE message_id = '0000000000000001'
LIMIT 1;
```

Parse `from_header` in SQL when you have a single metadata row:

```sql
SELECT
  message_id,
  subject,
  COALESCE(
    regexp_match(from_header, '<([^>]+)>')[1],
    regexp_match(from_header, '([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,})')[1],
    TRIM(from_header)
  ) AS from_email
FROM gmail.message_details
WHERE message_id = '0000000000000001'
LIMIT 1;
```

## Cross-source workflows

After step 2, use the parsed email in a **separate** Coral query against bundled
sources. Coral cannot `JOIN` from `gmail.search_messages` into `message_details`
because `message_id` must be literal.

Example relationships:

```text
gmail.search_messages / gmail.messages  →  ids
gmail.message_details (literal message_id)  →  from_header
parsed from_email  →  stripe.customers.email / linear.users.email
```

### Stripe lookup for a known sender email

Requires bundled Stripe. Run after you know `from_email` from `message_details`:

```sql
SELECT id, email, name
FROM stripe.customers
WHERE LOWER(email) = LOWER('billing@example.com')
LIMIT 5;
```

### Linear lookup for a known sender email

```sql
SELECT name, email
FROM linear.users
WHERE LOWER(email) = LOWER('billing@example.com')
LIMIT 5;
```

### Intercom contacts (illustrative)

```sql
SELECT email, name
FROM intercom.contacts
WHERE email IS NOT NULL
LIMIT 50;
```

To correlate Gmail with Intercom, compare ids from `search_messages` and emails
from per-message `message_details` fetches against this contact list in your
workspace.

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
| `messages.get` / `message_details` | 20 |
| `drafts.list` | 5 |
| `threads.list` | 10 |
| `labels.list` | 1 |
| `getProfile` | 1 |

Each `message_details` row costs one `messages.get` call (20 quota units each).
Use `LIMIT` on list/search queries; fetch details only for message ids you need.

Full details: https://developers.google.com/workspace/gmail/api/reference/quota

## Limitations

- Read-only (`gmail.readonly`); no send, delete, or label changes
- This source does not expose full MIME bodies or attachment bytes in v1 (the
  Gmail API supports them via other methods)
- `from_header` is the raw From header; use `COALESCE` + `regexp_match` in SQL for joins
- `message_details` requires a **literal** `message_id` filter per fetch (no
  join-derived ids; two-step list/get workflow)

## Validation

```bash
make lint-sources
coral source lint sources/community/gmail/manifest.yaml
coral source add --interactive --file sources/community/gmail/manifest.yaml
coral source test gmail
```

## Live validation

Community sources require evidence of a successful OAuth-backed run. After
`coral source add --interactive`, record sanitized output from `coral source test
gmail` and from queries that exercise `search_messages` and `message_details`.

```bash
coral source test gmail

coral sql "SELECT id, thread_id FROM gmail.search_messages(q => 'in:inbox') LIMIT 3"
# Use a literal id from the result above (JOIN-derived ids are not supported):
coral sql "SELECT message_id, from_header, subject, internal_date FROM gmail.message_details WHERE message_id = '0000000000000001' LIMIT 1"
```

Example output shape (synthetic ids and headers; live OAuth evidence in PR discussion):

```text
$ coral source test gmail

  ✓ gmail connected successfully
  Secrets: keychain

    gmail (6 tables)
    ├─ drafts
    ├─ labels
    ├─ message_details
    ├─ messages
    ├─ profile
    └─ threads
    Query tests
    5 declared · 5 passed · 0 failed

$ coral sql "SELECT id, thread_id FROM gmail.search_messages(q => 'in:inbox') LIMIT 1"
+------------------+------------------+
| id               | thread_id        |
+------------------+------------------+
| 0000000000000001 | 0000000000000001 |
+------------------+------------------+

$ coral sql "SELECT message_id, from_header, subject, internal_date FROM gmail.message_details WHERE message_id = '0000000000000001' LIMIT 1"
+------------------+-----------------------------+----------------------+----------------------------+
| message_id       | from_header                 | subject              | internal_date              |
+------------------+-----------------------------+----------------------+----------------------------+
| 0000000000000001 | Example Sender <user@example.com> | Example subject line | 2026-01-15T10:00:00Z       |
+------------------+-----------------------------+----------------------+----------------------------+
```

## Provider docs

- Gmail API: https://developers.google.com/workspace/gmail/api/reference/rest
- Auth scopes: https://developers.google.com/workspace/gmail/api/auth/scopes
- Search operators: https://support.google.com/mail/answer/7190
