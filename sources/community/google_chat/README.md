# Google Chat community source

The `google_chat` community source exposes read-only Google Chat space, member, and message data through Coral SQL.

## Setup

Google Chat API supports both Workspace admin app authentication and user OAuth authentication.

To use the Google Chat source:
1. Ensure the Google Chat API is enabled in your Google Cloud Project.
2. Obtain an OAuth 2.0 access token requesting the following read-only scopes:
   * `https://www.googleapis.com/auth/chat.spaces.readonly`
   * `https://www.googleapis.com/auth/chat.memberships.readonly`
   * `https://www.googleapis.com/auth/chat.messages.readonly`
3. Install the source:

```sh
export GOOGLE_CHAT_ACCESS_TOKEN="<oauth-access-token>"
cargo run -p coral-cli -- source add --file sources/community/google_chat/manifest.yaml
```

Alternatively, you can run the interactive setup flow in the Coral UI by selecting **Connect Google Chat** and providing your Google Cloud OAuth Client ID and Secret.

## Tables

| Table | Purpose | Required Filters |
| --- | --- | --- |
| `google_chat.spaces` | Lists named spaces, group chats, and direct messages the caller is in. | None |
| `google_chat.members` | Members of a specific space (users, bots, or groups). | `space_id` |
| `google_chat.messages` | Messages within a specific space. | `space_id` |

All tables are read-only. This source does not create, update, delete, or send messages.

### Important Limitations & Behaviors

* **Required Filters**: Both the `members` and `messages` tables require a `space_id` filter in the `WHERE` clause. This is an upstream requirement of the Google Chat API.
* **Invisible Card Text**: App-generated messages or bot notifications utilizing layout blocks/cards do not populate the root `text` field. Content for these messages must be queried from the `annotations` or `raw` JSON columns.
* **Rate Limiting**: Long-standing chat rooms can contain massive message histories. Bounding queries using `create_time` via the virtual `filter` column is strongly recommended (e.g. `WHERE filter = 'createTime > "2026-05-01T00:00:00Z"'`) to prevent 429 rate limit exceptions.
* **No last_update_time filtering**: The upstream Google Chat list messages API only supports filtering by `createTime` and `thread.name`. Therefore, incremental syncs must rely on `create_time` intervals.

## Example queries

Discover spaces:

```sql
SELECT id, display_name, space_type, create_time
FROM google_chat.spaces
LIMIT 20;
```

List members in a specific space:

```sql
SELECT member_id, member_display_name, member_type, role
FROM google_chat.members
WHERE space_id = 'space_abc123'
ORDER BY member_display_name;
```

List messages within a space:

```sql
SELECT name, sender_display_name, text, create_time
FROM google_chat.messages
WHERE space_id = 'space_abc123'
  AND filter = 'createTime > "2026-05-01T00:00:00Z"'
LIMIT 50;
```

Query raw card structure or annotations:

```sql
SELECT id, text, annotations, raw
FROM google_chat.messages
WHERE space_id = 'space_abc123'
LIMIT 5;
```

## Validation

Lint the manifest:

```sh
cargo run -p coral-cli -- source lint sources/community/google_chat/manifest.yaml
```

Install and test with a real or mock token:

```sh
export GOOGLE_CHAT_ACCESS_TOKEN="<token>"
cargo run -p coral-cli -- source add --file sources/community/google_chat/manifest.yaml
cargo run -p coral-cli -- source test google_chat
```

Inspect the registered source metadata:

```sh
cargo run -p coral-cli -- sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'google_chat'"
cargo run -p coral-cli -- sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'google_chat' ORDER BY table_name, ordinal_position"
```
