# Discord community source

The `discord` community source exposes read-only Discord guild, channel,
message, and member data through Coral SQL using the
[Discord REST API v10](https://discord.com/developers/docs/reference).

## Setup

Create a bot in the Discord Developer Portal:

1. Open [Discord Developer Portal](https://discord.com/developers/applications)
2. Create a **New Application** and add a **Bot**
3. Copy the bot token
4. Under **Privileged Gateway Intents**, enable **Server Members Intent**
   (required for `discord.members` on larger guilds)
5. Invite the bot to your server with **View Channels** and
   **Read Message History** permissions

Then install the source:

```sh
export DISCORD_BOT_TOKEN="<bot_token>"
coral source add --file sources/community/discord/manifest.yaml
```

Or interactively:

```sh
coral source add --file sources/community/discord/manifest.yaml --interactive
```

## Tables

| Table | Purpose | Required filters |
| --- | --- | --- |
| `discord.current_user` | Authenticated bot identity (`GET /users/@me`) | None |
| `discord.guilds` | Guilds the bot belongs to (`GET /users/@me/guilds`) | None |
| `discord.channels` | Channels in a guild (`GET /guilds/{guild_id}/channels`) | `guild_id` |
| `discord.messages` | Recent messages in a channel (`GET /channels/{channel_id}/messages`) | `channel_id` |
| `discord.members` | Guild members (`GET /guilds/{guild_id}/members`) | `guild_id` |

All tables are read-only. This source does not send messages or modify
Discord resources.

## Example queries

Verify bot credentials:

```sql
SELECT id, username, bot
FROM discord.current_user;
```

List guilds with member counts:

```sql
SELECT id, name, approximate_member_count
FROM discord.guilds;
```

List channels in a guild:

```sql
SELECT id, name, type, topic
FROM discord.channels
WHERE guild_id = 'YOUR_GUILD_ID';
```

Recent messages in a text channel:

```sql
SELECT author__username, content, timestamp
FROM discord.messages
WHERE channel_id = 'YOUR_CHANNEL_ID'
ORDER BY timestamp DESC
LIMIT 20;
```

Guild members for contact matching:

```sql
SELECT user__username, user__global_name, nick, joined_at
FROM discord.members
WHERE guild_id = 'YOUR_GUILD_ID'
LIMIT 100;
```

## Validation

Lint the manifest:

```sh
coral source lint sources/community/discord/manifest.yaml
```

Install and test with real credentials:

```sh
export DISCORD_BOT_TOKEN="<bot_token>"
coral source add --file sources/community/discord/manifest.yaml
coral source test discord
```

Inspect the registered source:

```sh
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'discord'"
coral sql "SELECT table_name, column_name FROM coral.columns WHERE schema_name = 'discord' ORDER BY table_name, ordinal_position"
```

## API reference

- [Discord REST API reference](https://discord.com/developers/docs/reference)
- [User resource](https://discord.com/developers/docs/resources/user)
- [Guild resource](https://discord.com/developers/docs/resources/guild)
- [Channel resource](https://discord.com/developers/docs/resources/channel)

## Notes

- Authentication uses `Authorization: Bot <token>` on all requests.
- Discord requires a `User-Agent` header; this source sets one automatically.
- `discord.messages` returns recent channel history only; Discord does not
  expose a global message search endpoint for bots in this source.
- `discord.members` may require the Server Members Intent and appropriate
  bot permissions for large guilds.
- Nested API fields are flattened with double underscores (for example
  `author__username` maps to `author.username`).
- Full upstream objects are preserved in each table's `raw` JSON column.
