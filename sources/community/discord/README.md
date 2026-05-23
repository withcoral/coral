# Discord

Query Discord bot identity, guilds, channels, messages, members, and roles from the [Discord REST API v10](https://discord.com/developers/docs/intro).

## Prerequisites

1. Create a Discord application at https://discord.com/developers/applications
2. Navigate to the **Bot** section and click **Reset Token** to generate a bot token
3. Enable the **Privileged Gateway Intents** your queries need:
   - **`GUILD_MEMBERS`** — required for the `members` table. Without this privileged intent, the endpoint returns an empty member list for guilds with 250k+ members and may be restricted in smaller guilds.
   - **`MESSAGE_CONTENT`** — required to read `content`, `embeds`, and `attachments` from the `messages` table. Without this intent, those fields will be empty (`""` or `[]`).
4. Invite the bot to your server using the OAuth2 URL Generator with the `bot` scope and the required bot permissions (e.g., Read Messages / View Channels, Read Message History).

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

## Pagination and cursors

Discord uses cursor-based pagination with `before`, `after`, and `around` parameters rather than page numbers. These endpoints support manual pagination via optional filters:

- **`guilds`** — Supports `before` and `after` filters for paging by guild ID, and `with_counts` to request approximate member and presence counts (max 200 per page).
- **`messages`** — Supports mutually exclusive `before`, `after`, and `around` filters for message ID-based pagination (max 100 per page). Messages are returned newest-first.
- **`members`** — Supports `after` filter for user ID-based pagination (max 1000 per page).

## Privileged intents vs bot permissions

Privileged gateway intents are distinct from bot permissions and must be enabled in the Discord Developer Portal under **Bot → Privileged Gateway Intents**:

- **`GUILD_MEMBERS`** — required to query the `members` table. Granting the `Read Members` bot permission alone is insufficient.
- **`MESSAGE_CONTENT`** — required to read `content`, `embeds`, and `attachments` from the `messages` table. Without this intent, those fields will be empty regardless of the `Read Message History` permission.
