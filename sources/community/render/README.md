# Render source

Query Render services, deploys, events, logs, PostgreSQL databases, Key Value
instances, projects, and environments through Coral using the Render API.

This source is designed for operational debugging as well as infrastructure
inventory. It can answer questions like which services exist, what deployed
recently, what events happened around an incident, which log lines mention an
error, and which datastore resources are attached to the account. Because it
exposes `deploys.commit_id`, it is built to **join against a source-control
source** (GitHub/GitLab) so you can trace a failed deploy back to the commit or
pull request that caused it.

## Authentication

This source uses a Render API key sent as:

```text
Authorization: Bearer <RENDER_API_KEY>
```

Create an API key under **Account Settings → API Keys**
(<https://dashboard.render.com/u/settings#api-keys>). A read-only key is
sufficient for every table here. The key authorizes access to every workspace
your Render account can see.

## Setup

```bash
RENDER_API_KEY=<token> coral source add --file sources/community/render/manifest.yaml
```

Or add it interactively:

```bash
coral source add --interactive --file sources/community/render/manifest.yaml
```

## Validation

Validated live against a real Render account on 2026-05-31 (Coral 0.3.0). All
rows below come from disposable `coral-validate-throwaway` fixtures.

`coral source test render`:

```text
  ✓ render connected successfully

    render (8 tables)
    ├─ deploys
    ├─ environments
    ├─ events
    ├─ key_value
    ├─ logs
    ├─ postgres
    ├─ projects
    └─ services
    Query tests
    2 declared · 2 passed · 0 failed
```

Representative `coral sql` results:

```text
coral sql "SELECT id, name, type FROM render.services LIMIT 5"
+--------------------------+--------------------------+-------------+
| id                       | name                     | type        |
+--------------------------+--------------------------+-------------+
| srv-d8cqi8l7vvec73bbnq3g | coral-validate-throwaway | static_site |
+--------------------------+--------------------------+-------------+

coral sql "SELECT id, status, trigger, finished_at FROM render.deploys WHERE service_id='srv-d8cqi8l7vvec73bbnq3g' LIMIT 3"
+--------------------------+--------+---------+-----------------------------+
| id                       | status | trigger | finished_at                 |
+--------------------------+--------+---------+-----------------------------+
| dep-d8cqi8t7vvec73bbnqa0 | live   | manual  | 2026-05-29T14:58:03.892097Z |
+--------------------------+--------+---------+-----------------------------+

coral sql "SELECT type, timestamp FROM render.events WHERE service_id='srv-d8cqi8l7vvec73bbnq3g' LIMIT 4"
+----------------+-----------------------------+
| type           | timestamp                   |
+----------------+-----------------------------+
| deploy_ended   | 2026-05-29T14:58:04.003146Z |
| build_ended    | 2026-05-29T14:58:00Z        |
| deploy_started | 2026-05-29T14:56:35.939837Z |
| build_started  | 2026-05-29T14:56:35.917761Z |
+----------------+-----------------------------+

coral sql "SELECT id, name, plan, region, status FROM render.postgres LIMIT 3"
+----------------------------+-----------------------------+------+--------+-----------+
| id                         | name                        | plan | region | status    |
+----------------------------+-----------------------------+------+--------+-----------+
| dpg-d8cr7ppkh4rs7387a340-a | coral-validate-throwaway-db | free | oregon | available |
+----------------------------+-----------------------------+------+--------+-----------+

coral sql "SELECT id, name, plan, region, status FROM render.key_value LIMIT 3"
+--------------------------+-----------------------------+------+--------+-----------+
| id                       | name                        | plan | region | status    |
+--------------------------+-----------------------------+------+--------+-----------+
| red-d8cr8h77f7vs73elq340 | coral-validate-throwaway-kv | free | oregon | available |
+--------------------------+-----------------------------+------+--------+-----------+
```

## Tables

| Table | Description | Required filters | Other useful filters |
| --- | --- | --- | --- |
| `services` | Render services and their Git/deploy metadata | — | `name`, `type`, `environment_id`, `region`, `suspended`, `owner_id`, `created_after/before`, `updated_after/before`, `include_previews` |
| `deploys` | Deploy history for one service, newest first | `service_id` | `status`, `created_after/before`, `updated_after/before`, `finished_after/before` |
| `events` | Lifecycle events for one service | `service_id` | — |
| `logs` | Application/build/request log lines | `owner_id`, `resource` | `text`, `level`, `type`, `start_time`, `end_time`, `direction` |
| `postgres` | Render PostgreSQL databases | — | `name`, `region`, `suspended`, `owner_id`, `environment_id`, `include_replicas`, `created_after/before`, `updated_after/before` |
| `key_value` | Render Key Value (Redis-compatible) instances | — | `name`, `region`, `owner_id`, `environment_id`, `created_after/before`, `updated_after/before` |
| `projects` | Render projects | — | — |
| `environments` | Environments for one project | `project_id` | — |

> **Required filters matter.** `deploys`, `events`, `logs`, and `environments`
> each require the filter(s) above and will error without them. Discover the IDs
> from `services` and `projects` first (see **Query flow**).

## Example queries

### List services

`services` has no `region` column — region lives in the raw `service_details`
JSON. Pull it out with `json_get_str`:

```sql
SELECT id, name, type, repo, branch,
       json_get_str(service_details, 'region') AS region,
       updated_at
FROM render.services
ORDER BY updated_at DESC
LIMIT 20;
```

### Find recent failed deploys

Deploy `status` values are: `created`, `queued`, `build_in_progress`,
`update_in_progress`, `live`, `deactivated`, `build_failed`, `update_failed`,
`canceled`, `pre_deploy_in_progress`, `pre_deploy_failed`. There is **no plain
`failed`** status — filter on the specific failure state:

```sql
SELECT id, status, trigger, commit_id, commit_message, finished_at
FROM render.deploys
WHERE service_id = 'srv-xxxxxxxx'
  AND status = 'build_failed'
ORDER BY created_at DESC
LIMIT 20;
```

### Look at events around an incident

`events` requires a `service_id`:

```sql
SELECT id, type, timestamp
FROM render.events
WHERE service_id = 'srv-xxxxxxxx'
ORDER BY timestamp DESC
LIMIT 50;
```

The `details` column is the raw provider JSON for the event; inspect fields with
`json_get_str(details, '<key>')`.

### Search logs for an error

`logs` requires both `owner_id` (workspace, `tea-xxxx`) and `resource` (the
service/postgres/key-value ID, e.g. `srv-xxxx`). The filter is `resource`, **not**
`resource_id`:

```sql
SELECT timestamp, level, message
FROM render.logs
WHERE owner_id = 'tea-xxxxxxxx'
  AND resource = 'srv-xxxxxxxx'
  AND level = 'error'
ORDER BY timestamp DESC
LIMIT 100;
```

Defaults to the last hour; widen with `start_time`/`end_time` (RFC3339, within
the last 30 days). The `labels` JSON holds the resource, level, type, host, and
status code for each line.

### Inventory PostgreSQL databases

```sql
SELECT id, name, region, plan, status, environment_id, created_at
FROM render.postgres
ORDER BY created_at DESC
LIMIT 50;
```

### Inventory Key Value instances

```sql
SELECT id, name, region, plan, status, environment_id, created_at
FROM render.key_value
ORDER BY created_at DESC
LIMIT 50;
```

### Map projects to environments

`environments` requires a `project_id`, so resolve the project first, then query
its environments (a blind cross-join will not supply the required filter):

```sql
-- 1. find the project
SELECT id, name FROM render.projects LIMIT 20;

-- 2. list that project's environments
SELECT id, name, protected_status
FROM render.environments
WHERE project_id = 'prj-xxxxxxxx';
```

### Cross-source: trace a failed deploy to its commit

The point of putting Render in Coral is the join. `deploys.commit_id` is the SHA
that shipped, so you can correlate a broken deploy with the pull request that
introduced it:

```sql
SELECT d.id AS deploy_id, d.status, d.commit_id, pr.title, pr.html_url
FROM render.deploys d
JOIN github.pull_requests pr
  ON pr.merge_commit_sha = d.commit_id
WHERE d.service_id = 'srv-xxxxxxxx'
  AND d.status = 'build_failed';
```

## Query flow

Start with `render.services` to discover service IDs and metadata. Use a service
ID with `render.deploys` to inspect deployment history, `render.events` to see
control-plane changes, and `render.logs` (with the workspace `owner_id`) to read
runtime behavior around the same time window.

For infrastructure inventory, use `render.projects` and `render.environments` to
understand organization, then query `render.postgres` and `render.key_value`.

## Pagination

Render returns the next-page cursor on **each row** of a page rather than once
per response, and Coral's path walker has no "last element" selector. The
manifest works around this by reading the cursor from the last slot of a full
100-row page (`response_cursor_path: ["99", "cursor"]`); a partial final page has
no index `99`, which cleanly stops pagination. This is intentional — not a bug.

## Rate limits and result size

Render API calls are paginated. Use filters and SQL `LIMIT` to keep queries
targeted, especially for logs and deploy history. Log queries should always
include `owner_id` + `resource` plus a time window; broad log searches can
consume API quota quickly.

## Known limitations

- Focuses on read-only infrastructure, deploy, event, log, and datastore
  visibility.
- Does not expose custom domains, one-off jobs, metrics, secrets, billing,
  teams, or write operations.
- Some columns expose provider-specific status strings exactly as Render returns
  them — notably `suspended` (`suspended`/`not_suspended`) and `auto_deploy`
  (`yes`/`no`) are **strings, not booleans**, because that is what the Render API
  returns.
- Log coverage depends on Render retention and the API key's permissions.
