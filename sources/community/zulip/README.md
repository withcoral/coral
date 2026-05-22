# Zulip

Query users, channels, topics, and bounded message windows from Zulip.

## Setup

### Get Your Zulip API Key

1. Open your Zulip organization.
2. Go to personal settings and copy your Zulip API email and API key, or use a
   bot account's email and API key.
3. Copy the organization base URL, for example
   `https://example.zulipchat.com`.

Zulip documents API key setup at https://zulip.com/api/api-keys.

### Add the Source

```bash
ZULIP_SITE=https://example.zulipchat.com \
ZULIP_EMAIL=bot@example.zulipchat.com \
ZULIP_API_KEY=zulip_api_key \
coral source add --file sources/community/zulip/manifest.yaml
```

## Authentication

The source uses Zulip HTTP Basic authentication. Coral sends
`ZULIP_EMAIL` as the Basic auth username and `ZULIP_API_KEY` as the Basic auth
password.

The authenticated user or bot determines which users, channels, topics, and
messages are visible. Private channels, protected history, guest permissions,
bot subscriptions, and organization email-visibility settings can affect the
rows and fields returned by the Zulip API.

## Tables

### `me`

Returns one row for the authenticated Zulip user or bot account. Use this table
to verify credentials and identify which account Coral is using.

**Example:**

```sql
SELECT user_id, email, full_name, is_bot, is_admin
FROM zulip.me
LIMIT 1;
```

### `users`

Returns Zulip organization users visible to the authenticated account.

Optional filters:

- `client_gravatar`
- `include_custom_profile_fields`
- `user_ids`, as a Zulip JSON array string such as `[1,2,3]`

**Example:**

```sql
SELECT user_id, full_name, email, is_bot, is_active
FROM zulip.users
WHERE include_custom_profile_fields = true
LIMIT 50;
```

### `channels`

Returns Zulip channels visible to the authenticated account. Zulip's public API
path still uses the older term `streams`; this source exposes the current
product term `channels`.

Optional filters:

- `include_public`
- `include_subscribed`
- `exclude_archived`
- `include_all`
- `include_all_active`
- `include_default`
- `include_web_public`
- `include_owner_subscribed`
- `include_can_access_content`

**Example:**

```sql
SELECT stream_id, name, description, is_archived, invite_only
FROM zulip.channels
WHERE include_public = true
LIMIT 50;
```

### `topics`

Returns topics in one Zulip channel.

Required filter:

- `stream_id`

Optional filter:

- `allow_empty_topic_name`

**Example:**

```sql
SELECT stream_id, name, max_id
FROM zulip.topics
WHERE stream_id = 1
ORDER BY max_id DESC
LIMIT 50;
```

### `messages`

Returns a bounded window of Zulip messages around an anchor. Zulip's message API
uses `anchor`, `num_before`, and `num_after` instead of cursor/page pagination,
so this table requires those filters.

Required filters:

- `anchor`, such as `newest`, `oldest`, `first_unread`, or a message ID
- `num_before`
- `num_after`

Optional filters:

- `narrow`, as a Zulip JSON narrow string
- `include_anchor`
- `apply_markdown`
- `client_gravatar`
- `allow_empty_topic_name`

**Latest visible messages:**

```sql
SELECT id, sender_full_name, type, stream_id, subject, content, timestamp
FROM zulip.messages
WHERE anchor = 'newest'
  AND num_before = 100
  AND num_after = 0
ORDER BY id DESC;
```

**Messages from a channel and topic:**

```sql
SELECT id, sender_full_name, subject, content, timestamp
FROM zulip.messages
WHERE anchor = 'newest'
  AND num_before = 100
  AND num_after = 0
  AND narrow = '[{"operator":"channel","operand":"general"},{"operator":"topic","operand":"announcements"}]'
ORDER BY id DESC;
```

**Messages matching a search term:**

```sql
SELECT id, sender_full_name, subject, content, timestamp
FROM zulip.messages
WHERE anchor = 'newest'
  AND num_before = 100
  AND num_after = 0
  AND narrow = '[{"operator":"search","operand":"deploy"}]'
ORDER BY id DESC;
```

## Limits

- `messages` returns bounded windows, not automatic full-history scans.
- Zulip recommends requesting at most 1000 messages in a batch; the documented
  maximum is 5000 messages per request.
- Message visibility depends on subscriptions, private-channel access,
  protected history, and bot/user permissions.
- Email fields can be redacted or replaced with API-only email addresses
  depending on organization visibility settings.
- Nested fields such as message flags, reactions, recipients, edit history,
  submessages, topic links, and user profile data are exposed as `Json`.
- Zulip rate limits API clients. The source maps Zulip rate-limit headers so
  Coral can classify rate-limit responses.

## Public API Mapping

| Table | Zulip endpoint | Response rows |
|---|---|---|
| `me` | `GET /api/v1/users/me` | response object |
| `users` | `GET /api/v1/users` | `members` |
| `channels` | `GET /api/v1/streams` | `streams` |
| `topics` | `GET /api/v1/users/me/{stream_id}/topics` | `topics` |
| `messages` | `GET /api/v1/messages` | `messages` |

Primary Zulip docs:

- https://zulip.com/api/http-headers
- https://zulip.com/api/api-keys
- https://zulip.com/api/get-own-user
- https://zulip.com/api/get-users
- https://zulip.com/api/get-streams
- https://zulip.com/api/get-stream-topics
- https://zulip.com/api/get-messages
