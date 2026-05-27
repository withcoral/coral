# Telegram Bot API Source

[Telegram Bot API](https://core.telegram.org/bots/api) is an HTTP-based interface for developers that allows you to interact with the Telegram messaging platform via bots.

This community source allows querying bot user details, updates/incoming messages, chat profiles, members, and administrators using Coral SQL.

---

## Setup

### 1. Create a Bot and Get a Bot Token

All requests to the Telegram Bot API require a Bot Token. To create a bot and get a token:

1. Open Telegram and search for the official **`@BotFather`** account.
2. Send the command `/newbot` to start the bot creation wizard.
3. Choose a friendly name for your bot (e.g., `My Coral Bot`).
4. Choose a unique username for your bot. It **must** end in `bot` (e.g., `my_coral_bot`).
5. BotFather will output your authorization token (e.g., `123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ`). Copy this token.

### 2. Add the Source to Coral

Run the following command to add the source. Pass your bot token when prompted:

```bash
coral source add --file sources/community/telegram/manifest.yaml --interactive
```

### 3. Verify Connection

```bash
coral source test telegram
```

---

## Tables

| Table | Description | Required Filters |
|---|---|---|
| `telegram.me` | Get basic information about the bot itself (ID, username, permissions). | None |
| `telegram.updates` | Retrieve incoming updates (messages, callback queries, posts) for the bot. | None |
| `telegram.chats` | Get detailed information about a specific chat (profile, title, bio). | `chat_id` |
| `telegram.chat_administrators` | List administrators of a group, supergroup, or channel. | `chat_id` |
| `telegram.chat_member` | Look up info about a specific chat member. | `chat_id`, `user_id` |
| `telegram.chat_member_count` | Get the total number of members in a chat. | `chat_id` |

---

## Key API Limitations

### ⚠️ Destructive Update Queue
Telegram's `/getUpdates` endpoint functions as a queue that is **consumed destructively**. 
Once Coral queries `telegram.updates`, the returned updates are acknowledged and flushed from Telegram's cache. If you run the query twice in a row, the second query will return empty results `[]` unless new messages are sent to the bot in the interim. 
* **Tip**: Send a test message to your bot in Telegram immediately before querying `telegram.updates` to ensure data is returned.

### ⚠️ No Historical Messages
The Telegram Bot API does **not** support querying or searching the historical log of messages in a chat. Bots can only observe messages in real-time as they arrive via the `telegram.updates` queue.

### ⚠️ No Bulk Member Listing
The Telegram Bot API does **not** provide any endpoint to retrieve a list of all members in a group or channel. Consequently, a general `members` table is not possible. You must use `telegram.chat_member` to look up individual users by their ID, or use `telegram.chat_member_count` to get the count.

---

## SQL Examples

### Get Bot Profile
```sql
SELECT id, username, first_name, can_join_groups 
FROM telegram.me;
```

### Get Recent Messages
```sql
SELECT update_id, chat_id, from_username, text, date 
FROM telegram.updates 
LIMIT 10;
```

### Get Chat Profile Details
Supports both username strings (for public channels/groups) and numeric IDs:
```sql
SELECT id, type, title, username, bio, description 
FROM telegram.chats 
WHERE chat_id = '@telegram';
```

### List Administrators of a Group
```sql
SELECT user_id, username, status, is_anonymous 
FROM telegram.chat_administrators 
WHERE chat_id = -1001234567890;
```

### Get Chat Member Status & Count
```sql
-- Check a user's role
SELECT user_id, status, custom_title 
FROM telegram.chat_member 
WHERE chat_id = -1001234567890 AND user_id = 987654321;

-- Get total member count
SELECT member_count 
FROM telegram.chat_member_count 
WHERE chat_id = -1001234567890;
```
