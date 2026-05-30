# Discord

Query Discord bot identity, guilds, channels, messages, members, and roles from the [Discord REST API v10](https://discord.com/developers/docs/intro).

## Prerequisites

### 1. Create a Discord application and bot

1. Go to https://discord.com/developers/applications and create a new application.
2. Navigate to the **Bot** section and click **Reset Token** to generate a bot token.
3. Under **Privileged Gateway Intents**, enable the intents your queries need:
   - **`GUILD_MEMBERS`** — required for the `members` table. Without it, `GET /guilds/{guild.id}/members` returns an empty result.
   - **`MESSAGE_CONTENT`** — required to read `content`, `embeds`, and `attachments` from the `messages` table. Without it, those fields return empty values regardless of permissions.

### 2. Required bot permissions

When inviting the bot to your server, use the OAuth2 URL Generator with:
- **Scope**: `bot` (and `applications.commands` if you plan to use slash commands alongside Coral)
- **Bot permissions**: the following are needed depending on which tables you query:

| Permission | Flag | Required for |
|---|---|---|
| Read Messages / View Channels | `0x400` | All guild-scoped tables (channels, messages, members, roles) |
| Read Message History | `0x10000` | `messages` table |
| Read Members | `0x20` | `members` table (in addition to the `GUILD_MEMBERS` privileged intent) |
| Send Messages | `0x800` | Not required for querying; only needed if the bot posts messages |

The minimal permission integer for read-only queries is `0x10420` (Read Members + Read Messages / View Channels + Read Message History).

### 3. Set the bot token

```shell
export DISCORD_BOT_TOKEN=your_bot_token_here
```

When adding the source, the token must be marked as `kind: secret` in the manifest (already configured). Coral will pass it as `Authorization: Bot {{token}}` on every request.

### 4. Discover guild and channel IDs

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

-- Messages within a time window
SELECT id, author__username, content, timestamp
FROM discord.messages
WHERE channel_id = '123456789012345678'
  AND timestamp >= '2026-05-01'
  AND timestamp < '2026-06-01'
ORDER BY timestamp DESC;

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

Messages are returned newest-first. To page through results, use the last row's ID as a cursor:

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

### Snowflake time filtering

Discord Snowflakes embed timestamps. When you set `LIMIT` and `OFFSET`, the source automatically handles cursor continuation behind the scenes.

You can also filter messages by their `timestamp` column directly, which maps to the ISO 8601 creation timestamp from the API:

```sql
SELECT id, content, timestamp
FROM discord.messages
WHERE channel_id = '123456789012345678'
  AND timestamp >= '2026-05-01T00:00:00Z'
ORDER BY timestamp DESC;
```

For Snowflake-range filtering, use the `after`/`before` cursor filters with Snowflake IDs corresponding to approximate timestamps.

### Rate limits

The source declares the standard Discord rate-limit response headers in its manifest:

| Header | Purpose |
|---|---|
| `X-RateLimit-Remaining` | Number of requests remaining in the current window |
| `X-RateLimit-Reset` | Unix timestamp when the current bucket resets |
| `Retry-After` | Seconds to wait before retrying (on 429 responses) |

Coral reads these headers and automatically pauses or retries when a rate limit is encountered, so you don't need to manage backoff manually.

## Privileged intents vs bot permissions

Privileged gateway intents are distinct from bot permissions and must be enabled in the Discord Developer Portal under **Bot → Privileged Gateway Intents**:

- **`GUILD_MEMBERS`** — required to query the `members` table. Granting the `Read Members` bot permission alone is insufficient.
- **`MESSAGE_CONTENT`** — required to read `content`, `embeds`, and `attachments` from the `messages` table. Without this intent, those fields will be empty regardless of the `Read Message History` permission.

## Validation

The following output was captured from a live Discord bot at setup time to confirm that
the source connects, authenticates, and returns real data from all six tables. IDs,
names, and message content are anonymized to placeholders.

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
