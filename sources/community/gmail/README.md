# Gmail Source

Query your Gmail mailbox using SQL via the Gmail REST API v1.

## Setup

### 1. Get an OAuth2 Access Token

1. Go to https://developers.google.com/oauthplayground
2. In the scopes box enter:
https://www.googleapis.com/auth/gmail.readonly
3. Click **Authorize APIs** and sign in with your Google account
4. Click **Exchange authorization code for tokens**
5. Copy the **Access token**

> Note: Access tokens expire after 1 hour. Repeat the steps above to get a fresh token when needed.

### 2. Add the Source

```bash
coral source add --interactive --file sources/community/gmail/manifest.yaml
```

When prompted enter your access token.

## Tables

| Table | Description |
|-------|-------------|
| `gmail.profile` | Mailbox info — email address, message count, thread count |
| `gmail.labels` | All labels including INBOX, SENT, DRAFT, SPAM, TRASH |
| `gmail.messages` | List messages by label or search query (returns IDs) |
| `gmail.threads` | List threads by label or search query |
| `gmail.drafts` | List all saved drafts |

> Note: `messages` and `threads` tables are ID/discovery tables.
> The Gmail list endpoints return message IDs and thread IDs.
> Use the IDs to fetch full message details via the Gmail API directly.

## Example Queries

```sql
-- Get your mailbox stats
SELECT email_address, messages_total, threads_total
FROM gmail.profile;

-- List all labels
SELECT id, name, type
FROM gmail.labels;

-- List inbox messages
SELECT id, thread_id
FROM gmail.messages
WHERE label_ids = 'INBOX'
LIMIT 20;

-- Search messages
SELECT id, thread_id
FROM gmail.messages
WHERE q = 'from:someone@gmail.com'
LIMIT 10;

-- Include spam and trash
SELECT id, thread_id
FROM gmail.messages
WHERE include_spam_trash = true
LIMIT 10;

-- List threads
SELECT id, snippet
FROM gmail.threads
LIMIT 20;

-- List drafts
SELECT id, message_id, message_thread_id
FROM gmail.drafts
LIMIT 10;
```

## Auth Scopes

This source uses `gmail.readonly` which is a **restricted Gmail scope**.
Google marks this scope as restricted because it grants read access to
all message content and metadata.

**Why not `gmail.metadata`?**
The narrower `gmail.metadata` scope is not sufficient for this source
because the `messages` and `threads` tables support a `q` search filter.
Gmail's API explicitly states that the `q` parameter cannot be used with
`gmail.metadata` — it requires at least `gmail.readonly` to work correctly.

Users publishing an app using this source publicly will need to go through
Google's OAuth verification process. For personal or internal use,
unverified access is fine.

Scope reference: https://developers.google.com/workspace/gmail/api/auth/scopes

## Rate Limits

Gmail API quota limits per minute:

| Limit type | Quota units |
|------------|-------------|
| Per minute per project | 1,200,000 |
| Per minute per user per project | 6,000 |

Per-method costs for this source:

| Method | Quota units |
|--------|-------------|
| `messages.list` | 5 |
| `drafts.list` | 5 |
| `threads.list` | 10 |
| `labels.list` | 1 |
| `getProfile` | 1 |

Full details: https://developers.google.com/workspace/gmail/api/reference/quota

## Provider Docs

- Gmail API: https://developers.google.com/workspace/gmail/api/reference/rest
- Auth Scopes: https://developers.google.com/workspace/gmail/api/auth/scopes
- OAuth Playground: https://developers.google.com/oauthplayground
