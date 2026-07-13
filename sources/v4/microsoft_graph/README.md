# Microsoft Graph Connector

Preview DSL v4 source for the full
[Microsoft Graph v1.0 OpenAPI description](https://github.com/microsoftgraph/msgraph-metadata/tree/master/openapi/v1.0).
It exposes Microsoft Graph endpoints for Teams chats, Teams/channel metadata,
SharePoint/OneDrive files, and other Microsoft 365 resources as generated SQL
tables and table functions.

## Status

This is a preview DSL v4 source. The full Microsoft Graph `OpenAPI`
descriptor is about 38 MB and produces a large generated catalog, so `source
add` can take longer than curated sources.

Generated Graph tables usually expose Microsoft Graph's OData response envelope.
For collection endpoints, use `json_get_array(value)` and `unnest(...)` to turn
the response's `value` array into rows.

## Requirements

- A Coral build with large DSL v4 descriptor/source-response support.
- A Microsoft work/school account in an Entra tenant.
- Tenant policy must allow consent for the requested delegated Graph scopes, or
  an admin must approve the app.

Personal Microsoft accounts such as Outlook.com or personal/free Teams accounts
are not the target path for this source.

## Auth

Use Coral's interactive OAuth flow to connect Microsoft Graph:

```bash
coral source add --interactive --file sources/core-v4/microsoft_graph_v4/manifest.yaml
```

During setup:

- Press enter for `MS_GRAPH_TENANT_ID` to use `organizations`.
- Choose **Sign in with Microsoft** for `MS_GRAPH_ACCESS_TOKEN`.
- Press enter for `MS_GRAPH_OAUTH_CLIENT_ID [source default]` to use Coral's
  default multitenant Microsoft Graph app.
- Complete the browser OAuth flow.

The default client ID is a Coral-owned multitenant Microsoft Entra app. Some
Microsoft tenants block user consent for newly registered or unverified
third-party multitenant apps. If consent is blocked, ask a tenant admin to
approve the Coral app, or re-run setup with a customer-owned Entra app client ID
when prompted for `MS_GRAPH_OAUTH_CLIENT_ID`.

To target a specific tenant, set `MS_GRAPH_TENANT_ID` to the tenant GUID instead
of `organizations`.

The OAuth flow requests delegated Microsoft Graph scopes:

```text
User.Read
Chat.Read
Chat.ReadBasic
Team.ReadBasic.All
Channel.ReadBasic.All
Files.Read
Files.Read.All
Sites.Read.All
offline_access
```

Coral reads what the signed-in user can access. This source does not request
`ChannelMessage.Read.All` by default; reading full Teams channel message history
requires that additional Microsoft Graph permission and typically admin
approval.

Verify the connection:

```bash
coral source test microsoft_graph_v4
```

## Example queries

List chats visible to the signed-in user:

```sql
WITH chats AS (
  SELECT unnest(json_get_array(value)) AS chat
  FROM microsoft_graph_v4.me_chat_me_listchats
  WHERE top = 20
)
SELECT
  json_get_str(chat, 'id') AS id,
  coalesce(json_get_str(chat, 'topic'), json_get_str(chat, 'chatType')) AS topic,
  json_get_str(chat, 'chatType') AS chat_type,
  json_get_str(chat, 'lastUpdatedDateTime') AS last_updated,
  json_get_str(chat, 'webUrl') AS web_url
FROM chats
ORDER BY last_updated DESC;
```

List recent messages from one chat:

```sql
WITH raw AS (
  SELECT unnest(json_get_array(value)) AS msg
  FROM microsoft_graph_v4.chats_chatmessage_chats_listmessages(
    chat_id => '19:example@thread.v2',
    top => 50
  )
)
SELECT
  json_get_str(msg, 'createdDateTime') AS created,
  json_get_str(msg, 'from', 'user', 'displayName') AS sender,
  regexp_replace(
    replace(json_get_str(msg, 'body', 'content'), '&nbsp;', ' '),
    '<[^>]+>',
    '',
    'g'
  ) AS body
FROM raw
ORDER BY created ASC;
```

List joined Teams:

```sql
WITH teams AS (
  SELECT unnest(json_get_array(value)) AS team
  FROM microsoft_graph_v4.me_team_me_listjoinedteams
)
SELECT
  json_get_str(team, 'id') AS id,
  json_get_str(team, 'displayName') AS display_name,
  json_get_str(team, 'tenantId') AS tenant_id
FROM teams
ORDER BY display_name;
```

Inspect OneDrive/SharePoint drives visible through `/me`:

```sql
WITH drives AS (
  SELECT unnest(json_get_array(value)) AS drive
  FROM microsoft_graph_v4.me_drive_me_listdrives
)
SELECT
  json_get_str(drive, 'id') AS id,
  json_get_str(drive, 'name') AS name,
  json_get_str(drive, 'driveType') AS drive_type,
  json_get_str(drive, 'webUrl') AS web_url
FROM drives
ORDER BY name;
```

Find generated Teams/SharePoint table names:

```sql
SELECT table_name, description
FROM coral.tables
WHERE schema_name = 'microsoft_graph_v4'
  AND (
    table_name LIKE '%chat%'
    OR table_name LIKE '%team%'
    OR table_name LIKE '%channel%'
    OR table_name LIKE '%drive%'
    OR table_name LIKE '%site%'
  )
ORDER BY table_name
LIMIT 100;
```
