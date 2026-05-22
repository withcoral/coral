# Discord

Query guilds, channels, messages, members, and roles from a Discord server via the [Discord REST API v10](https://discord.com/developers/docs/intro).

## Prerequisites

1. Create a Discord application at https://discord.com/developers/applications
2. Navigate to the **Bot** section and click **Reset Token** to generate a bot token
3. Under **Privileged Gateway Intents**, enable **Server Members Intent** if you want to query the `members` table for guilds with 250k+ members
4. Invite the bot to your server using the OAuth2 URL Generator with `bot` scope and the required permissions:
   - `Read Messages / View Channels` — channels, messages
   - `Read Message History` — message content
   - `Request Server Members` — members

## Tables

| Table | Description | Required filter |
|-------|-------------|----------------|
| `guilds` | Servers the bot has access to | none |
| `channels` | Channels in a guild | `guild_id` |
| `messages` | Messages in a channel | `channel_id` |
| `members` | Guild members | `guild_id` |
| `roles` | Guild roles | `guild_id` |

## Setup

```shell
export DISCORD_BOT_TOKEN=your_bot_token_here
coral source add --file sources/community/discord/manifest.yaml
coral source test discord
```

## Example queries

```sql
-- List all guilds with member counts
SELECT id, name, member_count, premium_tier FROM discord.guilds;

--- List text channels in a specific guild
SELECT id, name, position, topic
FROM discord.channels
WHERE guild_id = '123456789012345678'
  AND type = 0;

--- Recent messages in a channel
SELECT id, author__username, content, timestamp
FROM discord.messages
WHERE channel_id = '123456789012345678'
ORDER BY timestamp DESC
LIMIT 20;

--- Members who are actively boosting
SELECT user__username, nickname, premium_since, joined_at
FROM discord.members
WHERE guild_id = '123456789012345678'
  AND premium_since IS NOT NULL;

--- Roles sorted by hierarchy
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
