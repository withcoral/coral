# Slack connector (DSL v4 preview)

Read-only access to the Slack Web API: channels, messages, thread replies,
search, files, users, and team metadata.

## Status

Preview DSL v4 source. Unlike every other v4 source, the OpenAPI descriptor here
is **generated and committed** rather than fetched from the vendor. Slack
publishes no usable machine-readable description of its Web API — the official
[`slackapi/slack-api-specs`](https://github.com/slackapi/slack-api-specs) is
Swagger 2.0, was last updated in August 2020, and the repository was archived in
March 2024 — so `openapi.yaml` is built by `tools/openapi-forge` from Slack's
reference documentation, the response samples recorded in
`slackapi/java-slack-sdk`, and the request types in `@slack/web-api`.

Do not edit `openapi.yaml` or `catalog-preview.md` by hand. Regenerate them:

```bash
make slack-spec         # rebuild from the pinned snapshot
make slack-spec-fetch   # refresh the snapshot from upstream first
```

Corrections belong in `tools/openapi-forge/apis/slack/overlay.yaml`.

## Scope

37 operations across the thirteen non-admin families. Two filters select them:
Slack must document the method as GET, since Coral hides every non-GET
operation, **and** the method must actually read.

That second filter is not redundant. Slack documents `auth.revoke`,
`users.deletePhoto`, `conversations.declineSharedInvite` and
`files.remote.share` as GET, and all four mutate. They are excluded
deliberately. `admin.*` is out of scope entirely: 106 methods needing Enterprise
Grid and admin scopes, almost all of them writes.

## Auth

```bash
coral source add --interactive --file sources/v4/slack/manifest.yaml
```

The pre-filled app links in the setup prompts request the 21 read scopes these
relations use, plus `search:read` on the user-token app. Slack only grants
`search:read` to user tokens, so a bot token covers every relation except
`search_all`, `search_files` and `search_messages`.

Four relations need scopes deliberately left out of the defaults, because
requesting them by default would be wrong for a read-only connector:

| Relation | Extra scope | Why it is not requested |
| --- | --- | --- |
| `team_accesslogs`, `team_billableinfo`, `team_integrationlogs` | `admin` | Very broad; grants far more than reading |
| `team_externalteams_list` | `conversations.connect:manage` | A write scope |
| `users_identity` | `identity:read` | Belongs to Slack's separate Sign in with Slack flow |

Add them to your app by hand if you need those tables. A relation whose scope is
missing returns `ok = false` with `error = missing_scope` rather than failing
the query — see the first known limitation below.

Verify with `coral source test slack_v4`.

## What the catalog looks like

`catalog-preview.md` in this directory is generated from the descriptor by
`cargo run -p xtask -- v4-preview` and lists, per relation, the request it
makes, the row path and pagination Coral inferred, and every derived column. It
is the fastest way to see what a descriptor change did.

```sql
-- conversations_list has no required arguments, so it is a table and its
-- filters go in a WHERE clause. catalog-preview.md says which is which.
SELECT id, name, num_members
FROM slack_v4.conversations_list
WHERE types = 'public_channel'
LIMIT 20;

SELECT ts, "user", text
FROM slack_v4.conversations_history(channel => 'C012AB3CD')
LIMIT 20;

SELECT table_name, description
FROM coral.tables
WHERE schema_name = 'slack_v4'
ORDER BY table_name;
```

## Known limitations

**Failed calls do not raise errors.** Slack answers a failed request with HTTP
200 and `{"ok": false, "error": "..."}`. The DSL v3 source handles this with
`response.ok_path` and `error_path`; the v4 OpenAPI surface has no equivalent,
so a failure arrives as a row with `ok = false` and a populated `error` column
rather than as a query error. Both fields are modelled so the failure is at
least visible. Check `ok` when a query returns unexpectedly little.

**Four list relations do not expose rows.** `pins_list`, `reminders_list`,
`usergroups_list` and `usergroups_users_list` return `{ok, <rows>, error}` with
no pagination and no metadata sibling, and Coral's row-path inference requires
one or the other before it will treat a property as the rows. The rows are still
there, one level in:

```sql
SELECT json_get_str(reminder, 'text') AS text
FROM (SELECT unnest(json_get_array(reminders)) AS reminder FROM slack_v4.reminders_list);
```

**The three search relations nest their rows further.** `search.messages`,
`search.files` and `search.all` return matches at `messages.matches`, which
inference does not reach — it recurses only through `items`, `data`, `results`
and `rows`. Use `json_get_array(messages, 'matches')` with `unnest`.

**Nested detail is JSON.** Coral types only a row's direct properties, so
`message.blocks`, `user.profile` and similar are JSON columns. Use
`json_get_str(...)` and `json_get_array(...)` to read into them.

**Page sizes are capped at 100 unless a larger default is documented.** Coral
derives a page-size maximum of `max(declared_default, 100)`, and most Slack
methods document a default of 100 even where the real maximum is 999 or 1000.
`users_list` documents 1000 and gets it; the rest fetch 100 per request.

**`files_list` fetches a single page.** Slack's reference page for `files.list`
omits the `count` and `page` arguments that `@slack/web-api` accepts, so there
is nothing for pagination detection to find. The generator reports this as a
build warning. The same check flags a missing `before` argument on
`team.accessLogs`.
