# Discord Source for Coral

A production-ready custom source spec that connects [Coral](https://coral.so) to Discord's REST API v10 via a Bot Token, exposing guilds, channels, messages, and guild members as queryable SQL tables. 

This custom source enables ContextOS and the Coral community to run real-time SQL queries over Discord data with zero setup friction.

---

## Contributors

Built and contributed to the Coral community by:

- [Ganesh Bamalwa](https://github.com/GaneshBamalwa)
- [Siddhant Shivam](https://github.com/sidshivam625)
- [Vishal Kumar](https://github.com/Vishy-MK)

We hope this source empowers you to build awesome conversational intelligence tools!

---

## 🛠 Setup Guide

### 1. Create a Discord Bot in the Developer Portal
1. Navigate to the [Discord Developer Portal](https://discord.com/developers/applications).
2. Click **New Application** and give it a name (e.g., `ContextOS Bot`).
3. Select **Bot** from the left-hand sidebar menu, then click **Add Bot** and confirm.
4. Locate the **Token** section and click **Reset Token**. Copy this token immediately and save it securely. This is your `DISCORD_BOT_TOKEN`.
5. Under **Privileged Gateway Intents**, enable:
   - **Message Content Intent** (required to read the text content of messages in servers).
   - **Server Members Intent** (required to retrieve the member list for the `guild_members` table).
6. Click **Save Changes**.

### 2. Generate an OAuth2 Invite Link
To add the bot to your Discord server:
1. In the Discord Developer Portal, navigate to the **OAuth2** -> **URL Generator** tab.
2. Under **Scopes**, select **bot**.
3. Under **Bot Permissions**, select the following permissions (their combined bitmask is `66560`):
   - **Read Messages / View Channels**
   - **Read Message History**
4. Copy the generated **Invite URL** at the bottom of the page.
5. Paste this URL into your browser, choose a server you manage, and click **Authorize**.

---

## 💾 Installation

Ensure you have [Coral CLI](https://coral.so/docs) installed. Register the custom source spec file by running:

```bash
coral source add --file ./manifest.yaml
```

When prompted, paste your Discord Bot Token:
```bash
# Paste your bot token when prompted
```

Alternatively, you can provide the token non-interactively using your environment:
```bash
export DISCORD_BOT_TOKEN="your_bot_token"
coral source add --file ./discord.yaml
```

---

## 📊 Available Tables

| Table Name | Required Filters | Description |
| :--- | :--- | :--- |
| **`discord.guilds`** | *None* | All Discord guilds (servers) the bot is a member of. |
| **`discord.channels`** | `guild_id` | All channels (text, voice, category, announcement, etc.) in a given guild. |
| **`discord.messages`** | `channel_id` | Recent messages in a specific channel. Supports an optional `limit` parameter. |
| **`discord.guild_members`** | `guild_id` | Full list of members within a given guild. |

---

## 🔍 Example Queries

### Step 1: Find your Guild (Server) ID
Get the ID and basic statistics for all servers your bot has joined:
```sql
SELECT id, name, approximate_member_count FROM discord.guilds LIMIT 5;
```

### Step 2: Retrieve Channels
List channels in a specific server. **Note:** Discord returns all types of channels (text, voice, category). It is highly recommended to filter by `type = 0` to query text channels exclusively:
```sql
-- 0=text, 2=voice, 4=category, 5=announcement, 13=stage, 15=forum
SELECT id, name, topic FROM discord.channels 
WHERE guild_id = 'YOUR_GUILD_ID' AND type = 0 
LIMIT 10;
```

### Step 3: Query Recent Messages
Fetch recent messages from a text channel. You can optionally supply a `limit` filter to override the default count:
```sql
SELECT author__username, content, timestamp 
FROM discord.messages 
WHERE channel_id = 'YOUR_CHANNEL_ID' AND limit = 25;
```

### Step 4: Retrieve Server Members
Retrieve the list of users in a guild. This query will return an empty result or error if **Server Members Intent** is not enabled in the developer portal:
```sql
SELECT user__username, nick, joined_at 
FROM discord.guild_members 
WHERE guild_id = 'YOUR_GUILD_ID' 
LIMIT 10;
```

---

## ⚠️ Known Limitations

1. **Approximate Member Count**: The `approximate_member_count` field is normally returned as `null` by Discord's `/users/@me/guilds` endpoint unless specified. This spec automatically appends `?with_counts=true` to the request query parameters to ensure this field is populated correctly.
2. **Server Members Intent**: The `guild_members` table requires your bot to have the **Server Members Intent** enabled under the **Bot** tab of your Discord Application. Without this, the table will return a `401 Unauthorized` or an empty result set.
3. **Read Message History**: The bot must have **Read Message History** permissions in the target channel to query `discord.messages`.
4. **Pagination**: Discord's REST API caps messages at a maximum of 100 per request. This spec retrieves up to the specified `limit` in a single request and does not currently implement cursor-based pagination.
5. **Token Refresh**: Bot tokens are static and do not expire. If you need to rotate your token, click **Reset Token** in the Discord Developer Portal and re-add the source: `coral source add --file ./discord.yaml`.
