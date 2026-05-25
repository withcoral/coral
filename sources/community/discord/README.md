# Discord

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 6
**Base URL:** `https://discord.com/api/v10`

Query servers, channels, messages, members, roles, and invites from Discord — the popular voice, video, and text chat app.

## Authentication

Requires a `DISCORD_BOT_TOKEN`. Generate one from:
**Discord Developer Portal → Applications → [Your App] → Bot → Reset Token**

### Required Permissions & Intents
To ensure full data access, generate a **Bot Token** and invite your bot to the target servers. 
Additionally, you must enable the following **Privileged Gateway Intents** in the Developer Portal:
- `Server Members Intent` (required to query the `discord.guild_members` table)
- `Message Content Intent` (required to read the `content` field in `discord.messages`)

Tokens start with `MT...` or similar and are tied to your specific Bot application.

```bash
coral source add --file sources/community/discord/manifest.yaml
```

You will be prompted to enter your bot token interactively.

API docs: https://discord.com/developers/docs/intro

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `guilds` | Servers the bot is a member of | — | `limit` |
| `channels` | Channels within a specific server | `guild_id` | — |
| `messages` | Messages within a specific channel | `channel_id` | `limit` |
| `guild_members` | Members within a specific server | `guild_id` | `limit` |
| `roles` | Roles within a specific server | `guild_id` | — |
| `invites` | Active invites for a specific server | `guild_id` | — |

### Key design notes

- **Discord Snowflake IDs.** Almost all IDs in Discord (servers, users, roles) are highly unique 64-bit integers known as "Snowflakes", but are queried and returned as strings to prevent precision loss.
- **`messages` is the richest table.** It includes the `content`, `author`, `attachments`, `embeds`, and full timestamps directly in the list response.
- **Filters are required.** Discord's API requires strict hierarchical fetching. You cannot fetch "all messages" across all servers. You must first find the `guild_id`, then query `channels`, then query `messages` using the `channel_id`.

```text
guilds         → servers (no filter required)
channels       → requires guild_id
messages       → requires channel_id
guild_members  → requires guild_id
roles          → requires guild_id
invites        → requires guild_id
```

### channels filter values

| Filter | Description |
|---|---|
| `guild_id` | **(Required)** Filter by server ID (e.g., `1234567890`) |

### messages filter values

| Filter | Description |
|---|---|
| `channel_id` | **(Required)** Filter by channel ID (e.g., `0987654321`) |

### guild_members filter values

| Filter | Description |
|---|---|
| `guild_id` | **(Required)** Filter by server ID |

### Rate Limits & Fetch Limits
Discord heavily rate-limits its REST API. It returns a `429 Too Many Requests` status code with `X-RateLimit-*` headers indicating when you can retry.

To prevent unbounded queries that could exhaust your rate limit or get your bot banned, tables like `guilds`, `messages`, and `guild_members` utilize cursor-based pagination under the hood. 

## Quick start

```bash
# Step 1 — list all servers the bot is in
coral sql "
  SELECT id, name, approximate_member_count 
  FROM discord.guilds 
  LIMIT 10
"

# Step 2 — list all channels in a server
coral sql "
  SELECT id, name, type 
  FROM discord.channels 
  WHERE guild_id = '1234567890'
"

# Step 3 — list recent messages in a text channel
coral sql "
  SELECT id, author__username, content, timestamp 
  FROM discord.messages 
  WHERE channel_id = '0987654321'
  LIMIT 20
"

# Step 4 — list all roles in a server
coral sql "
  SELECT id, name, permissions 
  FROM discord.roles 
  WHERE guild_id = '1234567890'
"
```

## Example queries

### Top servers by member count

```sql
SELECT
  id,
  name,
  owner,
  approximate_member_count,
  approximate_presence_count
FROM discord.guilds
ORDER BY approximate_member_count DESC
LIMIT 50;
```

### Fetch latest messages with attachments

```sql
SELECT
  id,
  author__username,
  content,
  attachments,
  timestamp
FROM discord.messages
WHERE channel_id = '1234567890'
ORDER BY timestamp DESC
LIMIT 100;
```

### Find all server members who joined recently

```sql
SELECT
  user__id,
  user__username,
  nick,
  joined_at,
  premium_since
FROM discord.guild_members
WHERE guild_id = '1234567890'
ORDER BY joined_at DESC
LIMIT 50;
```

### List all invites and their usage stats

```sql
SELECT
  code,
  inviter__username,
  uses,
  max_uses,
  expires_at,
  created_at
FROM discord.invites
WHERE guild_id = '1234567890';
```
