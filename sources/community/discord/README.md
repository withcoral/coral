# Discord

Query Discord bot identity, guilds, channels, messages, members, and roles from the [Discord REST API v10](https://discord.com/developers/docs/intro).

## Prerequisites

### 1. Create a Discord application and bot

1. Go to https://discord.com/developers/applications and create a new application.
2. Navigate to the **Bot** section and click **Reset Token** to generate a bot token.
3. Invite the bot to your server using the OAuth2 URL Generator with the `bot` scope and the permissions listed below. Use the **Bot Permissions** text-field calculator in the Developer Portal to compute the integer.

Configure the following by table:

| Table | Requires | Privileged intent | Bot permissions |
|---|---|---|---|
| `current_user` | Token only | none | none |
| `guilds` | Token only | none | none |
| `channels` | `guild_id` filter | none | `VIEW_CHANNEL` (`0x400`) |
| `messages` | `channel_id` filter | `MESSAGE_CONTENT` | `VIEW_CHANNEL` (`0x400`), `READ_MESSAGE_HISTORY` (`0x10000`) |
| `members` | `guild_id` filter | `GUILD_MEMBERS` | none |
| `roles` | `guild_id` filter | none | `VIEW_CHANNEL` (`0x400`) |

**Privileged intents** are enabled in the Developer Portal under **Bot → Privileged Gateway Intents**. Without `GUILD_MEMBERS`, `GET /guilds/{guild.id}/members` returns an empty result. Without `MESSAGE_CONTENT`, the `content`, `embeds`, and `attachments` columns return empty values.

The minimal bot permission integer for read-only queries across all tables is `0x10400` (`VIEW_CHANNEL` + `READ_MESSAGE_HISTORY`). No bot permission is needed for reading guild members — only the `GUILD_MEMBERS` privileged intent controls access.

The `bot` scope is always required. The `applications.commands` scope is optional and only needed if the bot uses slash commands alongside Coral.

### 2. Set the bot token

```shell
export DISCORD_BOT_TOKEN=your_bot_token_here
```

When adding the source, the token must be marked as `kind: secret` in the manifest (already configured). Coral will pass it as `Authorization: Bot {{token}}` on every request.

### 3. Discover guild and channel IDs

After adding the source, discover IDs by querying:

```sql
-- Find guilds the bot can see
SELECT id, name FROM discord.guilds LIMIT 10;

-- Find channels in a guild
SELECT id, name, type FROM discord.channels WHERE guild_id = 'GUILD_ID';
```

Use the returned IDs as required filters for the `channels`, `messages`, `members`, and `roles` tables.

## Tables

| Table | Description | Required filter |
|-------|-------------|----------------|
| `current_user` | The Discord bot user associated with the configured token | none |
| `guilds` | Discord guilds that the configured bot can see | none |
| `channels` | Channels in a Discord guild | `guild_id` |
| `messages` | Recent messages in a Discord channel | `channel_id` |
| `members` | Members in a Discord guild | `guild_id` |
| `roles` | Roles in a Discord guild | `guild_id` |

## Setup

```shell
export DISCORD_BOT_TOKEN=your_bot_token_here
coral source add --file sources/community/discord/manifest.yaml
coral source test discord
```

## Quick-start queries

Start with a minimal query that requires no filters:

```sql
-- List guilds the bot can see (no filter needed)
SELECT id, name FROM discord.guilds LIMIT 10;
```

Once you have a guild ID and channel ID, query recent messages:

```sql
SELECT id, content, timestamp
FROM discord.messages
WHERE channel_id = 'YOUR_CHANNEL_ID'
ORDER BY timestamp DESC
LIMIT 20;
```

## Example queries

```sql
-- Confirm the token works and identify the bot account
SELECT id, username, global_name FROM discord.current_user;

-- List all guilds with approximate member and presence counts
SELECT id, name, approximate_member_count, approximate_presence_count
FROM discord.guilds WHERE with_counts = true;

-- List text channels in a specific guild
SELECT id, name, position, topic
FROM discord.channels
WHERE guild_id = '123456789012345678'
  AND type = 0;

-- Recent messages in a channel
SELECT id, author__username, content, timestamp
FROM discord.messages
WHERE channel_id = '123456789012345678'
ORDER BY timestamp DESC
LIMIT 20;

-- Members who are actively boosting
SELECT user__username, nick, premium_since, joined_at
FROM discord.members
WHERE guild_id = '123456789012345678'
  AND premium_since IS NOT NULL;

-- Roles sorted by hierarchy
SELECT id, name, color, position, permissions
FROM discord.roles
WHERE guild_id = '123456789012345678'
ORDER BY position DESC;
```

## Column naming

Nested API fields use double-underscore (`__`) flattening:

- `author__id` → `author.id` in the API response
- `author__username` → `author.username`
- `user__global_name` → `user.global_name`

This matches the convention used across bundled Coral sources.

## Pagination, filtering, and rate limits

### Cursor-based pagination

Discord uses Snowflake-based cursor pagination via `before`, `after`, and `around` parameters rather than page numbers. These endpoints accept optional filter columns:

| Table | Pagination filters | Max per page |
|---|---|---|
| `guilds` | `before`, `after` (by guild ID), `with_counts` | 200 |
| `messages` | `before`, `after`, `around` (message ID, mutually exclusive) | 100 |
| `members` | `after` (by user ID) | 1000 |

Messages are returned newest-first. The source returns at most one page per query (up to `page_size` rows, default 50 for messages and 1000 for members). To fetch subsequent pages, pass the last row's ID as a cursor filter in a separate query:

```sql
-- Get the first page
SELECT id, content, timestamp
FROM discord.messages
WHERE channel_id = '123456789012345678'
ORDER BY timestamp DESC LIMIT 50;

-- Get the next page (using the last message ID from the previous result)
SELECT id, content, timestamp
FROM discord.messages
WHERE channel_id = '123456789012345678'
  AND before = 'LAST_MESSAGE_ID'
ORDER BY timestamp DESC LIMIT 50;
```

### Rate limits

The source declares the standard Discord rate-limit response headers in its manifest:

| Header | Purpose |
|---|---|
| `X-RateLimit-Remaining` | Number of requests remaining in the current window |
| `X-RateLimit-Reset` | Unix timestamp when the current bucket resets |
| `Retry-After` | Seconds to wait before retrying (on 429 responses) |

Coral reads these headers and automatically pauses or retries when a rate limit is encountered, so you don't need to manage backoff manually.

## Privileged intents vs bot permissions

Privileged gateway intents and bot permissions are distinct controls. Both must be
configured, but they are set in different places:

- **Bot permissions** are granted during OAuth2 invite (via the `bot` scope) and
  control what the bot is allowed to do in a guild. The `bot` scope alone grants
  zero permissions — you must specify the `VIEW_CHANNEL` and
  `READ_MESSAGE_HISTORY` flags explicitly.
- **Privileged intents** are toggled in the Developer Portal under
  **Bot → Privileged Gateway Intents** and control access to sensitive API
  endpoints and gateway events. `MESSAGE_CONTENT` and `GUILD_MEMBERS` are
  privileged intents.

See the per-table requirements table in [Prerequisites](#prerequisites) for which
permissions and intents each table needs.

## Validation

The following output was captured from a live Discord bot at setup time. The manifest
test queries (`current_user`, `guilds`) passed validation. The `coral sql` examples
below demonstrate manual queries against all six tables using a bot that was a
member of one guild. IDs, names, and message content are anonymized.

```shell
# Add the source
$ DISCORD_BOT_TOKEN="your_bot_token_here" \
  coral source add --file sources/community/discord/manifest.yaml

Added source discord

  ✓ discord connected successfully

    discord (6 tables)
    ├─ channels
    ├─ current_user
    ├─ guilds
    ├─ members
    ├─ messages
    └─ roles
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT * FROM discord.current_user LIMIT 1
      1 row
    ✓ SELECT id, name FROM discord.guilds LIMIT 10
      0 rows (no guilds found for this token)

# Test the source
$ coral source test discord

  ✓ discord connected successfully

    discord (6 tables)
    ├─ channels
    ├─ current_user
    ├─ guilds
    ├─ members
    ├─ messages
    └─ roles
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT * FROM discord.current_user LIMIT 1
      1 row
    ✓ SELECT id, name FROM discord.guilds LIMIT 10
      0 rows (no guilds found for this token)

# Current user identity
$ coral sql "SELECT * FROM discord.current_user LIMIT 1"
+--------------------+-----------+---------------+-------------+--------+-----+-------------+----------+--------+-------+
| id                 | username  | discriminator | global_name | avatar | bot | mfa_enabled | verified | locale | email |
+--------------------+-----------+---------------+-------------+--------+-----+-------------+----------+--------+-------+
| 123456789012345678 | bot_user  | 0000          |             |        | true | false       | true     | en-US  |       |
+--------------------+-----------+---------------+-------------+--------+-----+-------------+----------+--------+-------+

# Guilds
$ coral sql "SELECT id, name, approximate_member_count FROM discord.guilds WHERE with_counts = true LIMIT 5"
+--------------------+-------------+--------------------------+
| id                 | name        | approximate_member_count |
+--------------------+-------------+--------------------------+
| 123456789012345679 | Test Server | 2                        |
+--------------------+-------------+--------------------------+

# Channels
$ coral sql "SELECT id, name, type, position FROM discord.channels WHERE guild_id = '123456789012345679' LIMIT 10"
+--------------------+----------------+------+----------+
| id                 | name           | type | position |
+--------------------+----------------+------+----------+
| 123456789012345680 | Text Channels  | 4    | 0        |
| 123456789012345681 | Voice Channels | 4    | 0        |
| 123456789012345682 | general        | 0    | 0        |
| 123456789012345683 | General        | 2    | 0        |
+--------------------+----------------+------+----------+

# Messages
$ coral sql "SELECT id, author__username, content, timestamp, flags FROM discord.messages WHERE channel_id = '123456789012345682' LIMIT 5"
+--------------------+------------------+------------------------+--------------------------+-------+
| id                 | author__username | content                | timestamp                | flags |
+--------------------+------------------+------------------------+--------------------------+-------+
| 123456789012345684 | bot_user         |                        | 2026-05-23T12:58:54.844Z | 0     |
| 123456789012345685 | server_member    | https://example.com/1  | 2026-02-24T08:03:37.653Z | 0     |
| 123456789012345686 | server_member    | https://example.com/2  | 2025-08-16T17:52:41.876Z | 0     |
| 123456789012345687 | server_member    | Announcement text      | 2025-07-06T20:47:17.386Z | 0     |
| 123456789012345688 | server_member    |                        | 2025-07-06T20:45:26.785Z | 16384 |
+--------------------+------------------+------------------------+--------------------------+-------+

# Members
$ coral sql "SELECT user__username, nick, joined_at, premium_since FROM discord.members WHERE guild_id = '123456789012345679' LIMIT 10"
+----------------+------+--------------------------+---------------+
| user__username | nick | joined_at                | premium_since |
+----------------+------+--------------------------+---------------+
| server_member  |      | 2024-10-26T07:37:01.352Z |               |
| bot_user       |      | 2026-05-23T12:58:54.758Z |               |
+----------------+------+--------------------------+---------------+

# Roles
$ coral sql "SELECT id, name, color, position FROM discord.roles WHERE guild_id = '123456789012345679' ORDER BY position DESC LIMIT 10"
+--------------------+------------+-------+----------+
| id                 | name       | color | position |
+--------------------+------------+-------+----------+
| 123456789012345689 | bot_role   | 0     | 1        |
| 123456789012345679 | @everyone  | 0     | 0        |
+--------------------+------------+-------+----------+
```
