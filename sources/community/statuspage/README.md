# Statuspage

Query public Atlassian Statuspage Status API feeds through Coral SQL.

This source is for pages that expose the public `/api/v2/*.json`
Statuspage endpoints documented on each Statuspage-powered site, such as:

- `https://www.githubstatus.com`
- `https://status.atlassian.com`
- `https://status.twilio.com`
- `https://www.cloudflarestatus.com`
- `https://status.datadoghq.com`
- `https://status.openai.com`
- `https://www.vercel-status.com`
- `https://discordstatus.com`
- `https://status.zoom.us`

Always verify a vendor before adding it. Some vendor status sites are not
Atlassian Statuspage Status API sites. For example, Stripe and PagerDuty do
not expose this `/api/v2/status.json` shape, and Slack uses its own
`https://slack-status.com/api/v2.0.0/...` API.

## API Scope

This manifest models the public page-level Status API feeds:

| Table | What it returns |
|-------|-----------------|
| `status` | One-row page health rollup |
| `components` | Current component status |
| `incidents` | Recent incident feed |
| `active_incidents` | Unresolved incidents |
| `scheduled_maintenances` | 50 most recent maintenance windows |
| `active_maintenances` | In-progress or verifying maintenance |
| `upcoming_maintenances` | Future scheduled maintenance |

Endpoint paths are the matching `/api/v2/*.json` Status API feeds.

The Status API feed endpoints are not paginated. The all-maintenance feed is
documented as the 50 most recent scheduled maintenances.

## Requirements

- Coral CLI installed and available as `coral`
- A public paid Statuspage page URL, without a trailing slash
- No authentication for public paid pages

Private Statuspage pages and public trial pages require an `Authorization`
header with a full Statuspage API key. This manifest intentionally does not
model that authenticated flow because Statuspage does not provide a separate
read-only API key for those pages.

Atlassian documents the public Status API as not rate limited.

## Verify A Vendor URL

Before adding a vendor, confirm that the URL exposes the public Statuspage
API shape:

```bash
curl -fsSL "https://status.openai.com/api/v2/status.json"
```

A compatible response includes top-level `page` and `status` fields.

Use the base URL when registering the source:

```text
https://status.openai.com
```

Do not use a trailing slash or a full endpoint URL:

```text
https://status.openai.com/
https://status.openai.com/api/v2/status.json
```

## Add The Source

From this directory:

```bash
STATUSPAGE_BASE_URL="https://status.openai.com" \
  coral source add --file ./manifest.yaml
```

Or use the interactive prompt:

```bash
coral source add --file ./manifest.yaml --interactive
```

The schema name is the top-level `name:` in `manifest.yaml`, which defaults
to `statuspage`. Query the default source as `statuspage.status`,
`statuspage.components`, and so on.

## Validate The Source

The manifest includes representative `test_queries`. During
`coral source add`, Coral runs those queries against the newly registered
source and reports how many passed.

You can also run individual checks after registration:

```bash
coral sql "SELECT indicator, description FROM statuspage.status"
coral sql "SELECT id, name, status FROM statuspage.components LIMIT 5"
coral sql "SELECT id, name, status FROM statuspage.active_maintenances"
```

## Query Examples

### Current Overall Status

```sql
SELECT indicator, description
FROM statuspage.status;
```

`indicator` values include `none`, `minor`, `major`, `critical`, and
`maintenance`.

### Degraded Components

```sql
SELECT name, status, updated_at
FROM statuspage.components
WHERE status != 'operational'
ORDER BY updated_at DESC;
```

Component statuses include `operational`, `degraded_performance`,
`partial_outage`, `major_outage`, and `under_maintenance`.

### Active Incidents

```sql
SELECT id, name, impact, status, created_at, shortlink
FROM statuspage.active_incidents
ORDER BY created_at DESC;
```

This table returns unresolved incidents only.

### Recent Incident Feed

```sql
SELECT name, impact, status, created_at, resolved_at, shortlink
FROM statuspage.incidents
ORDER BY created_at DESC
LIMIT 10;
```

### Active Maintenance

```sql
SELECT name, status, impact, scheduled_for, scheduled_until, shortlink
FROM statuspage.active_maintenances
ORDER BY scheduled_for ASC;
```

Treat both `in_progress` and `verifying` as active maintenance states.

### Upcoming Maintenance

```sql
SELECT name, status, impact, scheduled_for, scheduled_until, shortlink
FROM statuspage.upcoming_maintenances
ORDER BY scheduled_for ASC;
```

### Recent Scheduled Maintenance Feed

```sql
SELECT name, status, scheduled_for, scheduled_until, updated_at
FROM statuspage.scheduled_maintenances
ORDER BY updated_at DESC
LIMIT 20;
```

## Multiple Vendors

`coral source add --file` uses the literal top-level `name:` from the YAML
file. It does not support `--name` with `--file`, and `name:` is not prompted
as an input.

To register multiple Statuspage vendors, create one manifest copy per vendor
and change the top-level `name:`.

```bash
cp manifest.yaml github_status.yaml
cp manifest.yaml openai_status.yaml
```

Edit the first line in each copy:

```yaml
# github_status.yaml
name: github_status
```

```yaml
# openai_status.yaml
name: openai_status
```

Register both:

```bash
STATUSPAGE_BASE_URL="https://www.githubstatus.com" \
  coral source add --file ./github_status.yaml

STATUSPAGE_BASE_URL="https://status.openai.com" \
  coral source add --file ./openai_status.yaml
```

Query across both schemas:

```sql
SELECT 'GitHub' AS vendor, indicator, description
FROM github_status.status
UNION ALL
SELECT 'OpenAI' AS vendor, indicator, description
FROM openai_status.status;
```

If you copy the manifest and rename the source, also update the copied
manifest's `test_queries` from `statuspage.*` to the new schema name before
using `coral source test`.

## Troubleshooting

### `unexpected argument '--name' found`

This Coral CLI does not accept `--name` with `--file`:

```bash
coral source add --file ./manifest.yaml --name openai_status
```

Create a manifest copy and change the top-level `name:` instead.

### `No such file or directory`

The file passed to `--file` must already exist. Create copied manifests before
adding them:

```bash
cp manifest.yaml vercel_status.yaml
```

Then edit `name:` and run `coral source add`.

### `Table ... not found`

The schema in your SQL must match the manifest `name:` that was registered.

List registered schemas:

```bash
coral sql "SELECT DISTINCT schema_name FROM coral.tables"
```

### Empty Results

Empty arrays are normal when the vendor has no unresolved incident, active
maintenance, or upcoming maintenance.

### Incompatible URL

Verify the URL directly:

```bash
curl -fsSL "https://status.example.com/api/v2/status.json"
```

If the response does not include `page` and `status`, the URL is not compatible
with this manifest. Slack, Stripe, PagerDuty, AWS Health, and Google Cloud
Service Health require different APIs.

## References

- [Atlassian Status API page](https://status.atlassian.com/api)
- [Statuspage API types][statuspage-api-types]
- [Slack Status API](https://docs.slack.dev/reference/slack-status-api/)

[statuspage-api-types]: https://support.atlassian.com/statuspage/docs/what-are-the-different-apis-under-statuspage/
