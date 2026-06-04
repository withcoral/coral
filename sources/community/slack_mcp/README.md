# Slack MCP Connector

**Version:** 0.1.0
**Source:** Slack official remote MCP server
**Backend:** MCP (Streamable HTTP, native)
**Server URL:** `https://mcp.slack.com/mcp`
**Surface:** 2 tables + 9 functions wrapping read-oriented MCP tools

This connector exposes Slack's official MCP server as a Coral source. It is
separate from the bundled `slack` HTTP source and keeps the MCP output intact:
most functions return a `result` text column plus a `raw` JSON column.

## Slack app setup

Slack requires MCP clients to use a Slack app that is either internal or
published in the Slack Marketplace.

1. Open https://api.slack.com/apps.
2. Select **Create New App**.
3. Select **From an app manifest**.
4. Select the workspace where you want to test the connector.
5. Paste this manifest in the **JSON** tab:

```json
{
  "display_information": {
    "name": "Coral Slack MCP"
  },
  "oauth_config": {
    "pkce_enabled": true,
    "redirect_urls": [
      "http://localhost:53684/oauth/callback"
    ],
    "scopes": {
      "user": [
        "search:read.public",
        "search:read.private",
        "search:read.mpim",
        "search:read.im",
        "search:read.files",
        "search:read.users",
        "channels:history",
        "groups:history",
        "mpim:history",
        "im:history",
        "channels:read",
        "groups:read",
        "mpim:read",
        "users:read",
        "users:read.email",
        "files:read",
        "emoji:read",
        "canvases:read"
      ]
    }
  },
  "settings": {
    "org_deploy_enabled": false,
    "socket_mode_enabled": false,
    "is_hosted": false,
    "is_mcp_enabled": true,
    "token_rotation_enabled": false
  }
}
```

6. Create the app and review Slack's generated permission summary.
7. Open **Basic Information** and copy **Client ID** and **Client Secret**
   from **App Credentials**.

The manifest registers the fixed local OAuth redirect URL used by this source:

```text
http://localhost:53684/oauth/callback
```

It also enables Slack's MCP app setting. If Slack's app creation flow ignores
or rejects the MCP setting for your workspace, enable **Model Context
Protocol** manually under **Agents & AI Apps** after creating the app.

## Coral setup

Register the source interactively:

```bash
coral source add --file sources/community/slack_mcp/manifest.yaml --interactive
```

When prompted for `SLACK_MCP_ACCESS_TOKEN`, choose **Connect with Slack MCP**.
Provide:

- `SLACK_MCP_OAUTH_CLIENT_ID`: your Slack app client ID
- `SLACK_MCP_OAUTH_CLIENT_SECRET`: your Slack app client secret

Coral opens the Slack OAuth page, exchanges the authorization code for a user
access token, and stores that token as the source secret.

## Tables

| Table | MCP tool | Description |
|---|---|---|
| `channels` | `list_channels` | Channels visible to the authenticated user |
| `emoji` | `list_emoji` | Custom emoji available in the workspace |

## Functions

All functions require named arguments.

| Function | MCP tool | Required args | Description |
|---|---|---|---|
| `search_messages` | `search_messages` | `query` | Search messages visible to the authenticated user |
| `search_channels` | `search_channels` | `query` | Search channels by name or description |
| `search_users` | `search_users` | `query` | Search users by name, email, or user ID |
| `search_files` | `search_files` | `query` | Search Slack files |
| `read_channel` | `read_channel` | `channel_id` | Read recent messages from a channel or conversation |
| `read_thread` | `read_thread` | `channel_id`, `message_ts` | Read replies in a thread |
| `read_user_profile` | `read_user_profile` | `user_id` | Read a user profile |
| `list_channel_members` | `list_channel_members` | `channel_id` | List channel members |
| `canvas_read` | `canvas_read` | `canvas_id` | Read a canvas |

## Examples

```sql
SELECT result
FROM slack_mcp.search_channels(query => 'general');

SELECT result
FROM slack_mcp.read_channel(
  channel_id => 'C0123456789',
  limit => 25
);

SELECT result
FROM slack_mcp.read_thread(
  channel_id => 'C0123456789',
  message_ts => '1712345678.123456',
  limit => 100
);
```

## Notes

Slack's MCP server is optimized for agents, so responses may be human-readable
Markdown instead of stable row objects. Use `result` for the rendered response
and `raw` when the server returns structured data.

This source intentionally exposes read-oriented tools only. Slack's MCP server
also includes write-capable tools such as message posting and canvas updates,
but those are not modeled here because SQL queries should not unexpectedly
mutate Slack state.

Slack's official MCP documentation:
https://docs.slack.dev/ai/slack-mcp-server/
