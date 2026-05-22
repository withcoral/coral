# Gmail

Query Gmail messages, threads, labels, and drafts using SQL via the [Gmail REST API v1](https://developers.google.com/gmail/api/reference/rest).

## Authentication

The source uses OAuth 2.0 Bearer Token authentication. `GMAIL_ACCESS_TOKEN` is required at install time.

To generate an access token:

1. Go to [Google Cloud Console](https://console.cloud.google.com/apis/credentials).
2. Create a project and enable the **Gmail API**.
3. Create an **OAuth 2.0 Client ID** credential (Desktop or Web app).
4. Authorize the scope `https://www.googleapis.com/auth/gmail.readonly`.
5. Obtain an access token via OAuth 2.0 flow or use the [OAuth 2.0 Playground](https://developers.google.com/oauthplayground/).

## Configuration

| Input | Kind | Required | Default | Description |
|---|---|---|---|---|
| `GMAIL_ACCESS_TOKEN` | secret | yes | — | OAuth 2.0 access token with `gmail.readonly` scope. |

## Schema

### `labels`

One row per Gmail label (folder/category). Requires `user_id` filter. Use `'me'` for the authenticated user. Use `label_id` values as filters in the `messages` and `threads` tables.

| Column | Type | Description |
|---|---|---|
| `label_id` | text | Unique label ID (e.g. `INBOX`, `SENT`, or a custom ID). |
| `name` | text | Display name of the label. |
| `type` | text | `system` for built-in labels, `user` for custom labels. |

### `messages`

One row per Gmail message. Requires `user_id` filter. Optionally filter by `label_id` or Gmail search `q`.

| Column | Type | Description |
|---|---|---|
| `message_id` | text | Unique message ID. Use as filter in `message_detail`. |
| `thread_id` | text | Thread this message belongs to. |

### `message_detail`

Full metadata for a single message. Requires `user_id` and `message_id` filters.

| Column | Type | Description |
|---|---|---|
| `message_id` | text | Unique message ID. |
| `thread_id` | text | Thread ID. |
| `snippet` | text | Short plain-text preview of the message body. |
| `history_id` | text | History record ID. |
| `internal_date` | text | Message timestamp in milliseconds since epoch. |
| `label_ids` | text | JSON array of label IDs applied to this message. |

### `threads`

One row per conversation thread. Requires `user_id` filter. Optionally filter by `label_id` or `q`.

| Column | Type | Description |
|---|---|---|
| `thread_id` | text | Unique thread ID. |
| `snippet` | text | Preview of the most recent message in the thread. |
| `history_id` | text | History record ID. |

## Example Queries

```sql
-- List all labels
SELECT label_id, name, type
FROM gmail.labels
WHERE user_id = 'me';

-- List messages in INBOX
SELECT message_id, thread_id
FROM gmail.messages
WHERE user_id = 'me'
  AND label_id = 'INBOX';

-- Search for unread messages
SELECT message_id, thread_id
FROM gmail.messages
WHERE user_id = 'me'
  AND q = 'is:unread';

-- Search for messages from a specific sender
SELECT message_id, thread_id
FROM gmail.messages
WHERE user_id = 'me'
  AND q = 'from:someone@example.com';

-- Get full details of a specific message
SELECT message_id, snippet, internal_date, label_ids
FROM gmail.message_detail
WHERE user_id = 'me'
  AND message_id = 'your-message-id';

-- List threads in INBOX
SELECT thread_id, snippet
FROM gmail.threads
WHERE user_id = 'me'
  AND label_id = 'INBOX';
```

## Limitations

- **Read-only**: This source only supports `SELECT` operations. Sending or modifying emails is not supported.
- **Access token expiry**: OAuth 2.0 access tokens expire after 1 hour. Refresh the token and update the secret as needed.
- **message_detail requires one call per message**: Fetching details for many messages requires repeated queries with different `message_id` filters.
- **Pagination not yet supported**: Results are limited to the API's default page size (100 messages per request).

## Notes

- Use `'me'` as the `user_id` to query the authenticated user's mailbox.
- Gmail search syntax is supported in the `q` filter — see [Gmail search operators](https://support.google.com/mail/answer/7190) for the full reference.
