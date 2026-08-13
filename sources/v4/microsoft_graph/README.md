# Microsoft Graph Connector

Preview DSL v4 source for the full
[Microsoft Graph v1.0 OpenAPI description](https://github.com/microsoftgraph/msgraph-metadata/tree/master/openapi/v1.0).
It exposes Microsoft Graph endpoints for Teams chats, Teams/channel metadata,
SharePoint/OneDrive files, and other Microsoft 365 resources as generated SQL
tables and table functions.

## Status

This is a preview DSL v4 source. The full Microsoft Graph OpenAPI
descriptor is about 38 MB and produces a large generated catalog, so `source
add` can take longer than curated sources.

## Collections and pagination

Collection endpoints are ordinary row tables: one row per resource, with a
column per declared property. Coral unwraps Microsoft Graph's OData envelope,
so there is no `value` array to `unnest`.

Coral also follows `@odata.nextLink` until it stops appearing, so a query
returns the whole collection rather than the first page. Some Graph collections
are very large — a busy Teams chat can hold hundreds of thousands of messages —
so add a `LIMIT`, a `WHERE` filter, or `top => n` when you do not need all of
it.

Inherited properties are declared as columns however deep the `allOf` chain
goes: `microsoft.graph.drive` reaches `id`, `name`, and `webUrl` through
`baseItem` and `entity`, and all three are columns.

One known gap, tracked separately:

- Graph declares `$count` as a boolean and `$top` as an integer on the same
  endpoints, and page-size detection stops at the first candidate name it
  recognizes. `$top` is therefore not detected, so Coral accepts Graph's
  server-side default page size. `top` stays available as an ordinary filter if
  you want to set it yourself.

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
coral source add --interactive --file sources/v4/microsoft_graph/manifest.yaml
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

Column names are the resource's property names, lowercased with punctuation
removed: `lastUpdatedDateTime` becomes `lastupdateddatetime`. Properties that
hold an object or an array stay JSON, so reach into them with `json_get_str`
and friends.

List chats visible to the signed-in user:

```sql
SELECT
  id,
  coalesce(topic, chattype) AS topic,
  chattype,
  lastupdateddatetime,
  weburl
FROM microsoft_graph_v4.me_chat_me_listchats
ORDER BY lastupdateddatetime DESC
LIMIT 20;
```

List recent messages from one chat. `from` is a SQL keyword, so quote it:

```sql
SELECT
  createddatetime,
  json_get_str("from", 'user', 'displayName') AS sender,
  regexp_replace(
    replace(json_get_str(body, 'content'), '&nbsp;', ' '),
    '<[^>]+>',
    '',
    'g'
  ) AS body
FROM microsoft_graph_v4.chats_chatmessage_chats_listmessages(
  chat_id => '19:example@thread.v2'
)
ORDER BY createddatetime DESC
LIMIT 50;
```

List joined Teams:

```sql
SELECT id, displayname, tenantid
FROM microsoft_graph_v4.me_team_me_listjoinedteams
ORDER BY displayname;
```

Count the messages in a chat — the question that motivated following
`@odata.nextLink`, since it needs every page:

```sql
SELECT count(*) AS message_count
FROM microsoft_graph_v4.chats_chatmessage_chats_listmessages(
  chat_id => '19:example@thread.v2'
);
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
