# Gmail Source

Query your Gmail mailbox using SQL via the Gmail REST API v1.

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
- Token is stored automatically!

Or choose **"Paste access token"** if you already have a token from
https://developers.google.com/oauthplayground using scope:
`https://www.googleapis.com/auth/gmail.readonly`

> Note: Access tokens expire after 1 hour. Re-run the add command to refresh.

## Tables

| Table | Description |
|-------|-------------|
| `gmail.profile` | Mailbox info — email address, message count, thread count |
| `gmail.labels` | All labels including INBOX, SENT, DRAFT, SPAM, TRASH |
| `gmail.messages` | List messages by label or search query (returns IDs) |
| `gmail.threads` | List threads by label or search query |
| `gmail.drafts` | List all saved drafts |

> Note: `messages` and `drafts` are ID/discovery tables that return IDs only.
> The `threads` table also returns `snippet` and `history_id` columns.
> Use message or draft IDs to fetch full details via the Gmail API directly.

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
- Gmail API Console: https://console.cloud.google.com
