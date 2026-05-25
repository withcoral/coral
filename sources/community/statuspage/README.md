# Statuspage

Query public Atlassian Statuspage instances through Coral SQL.

This source exposes current status, components, incidents, and scheduled maintenance windows from any vendor that serves the public Statuspage `/api/v2/*.json` endpoints. It works without authentication.

Examples of compatible vendors include GitHub, Stripe, Atlassian, PagerDuty, Twilio, Cloudflare, Datadog, Slack, OpenAI, Vercel, Discord, and Zoom.

## What This Source Provides

| Table | Description |
|-------|-------------|
| `{source}.status` | One-row page-level health summary |
| `{source}.components` | Current health of each service component |
| `{source}.incidents` | Recent incidents, including resolved and unresolved incidents |
| `{source}.active_incidents` | Currently unresolved incidents |
| `{source}.scheduled_maintenances` | Scheduled maintenance windows |
| `{source}.active_maintenances` | Maintenance windows currently in progress |
| `{source}.upcoming_maintenances` | Future scheduled maintenance windows |

The `{source}` prefix is the Coral schema name. For the default `manifest.yaml`, the schema is `statuspage` because the manifest starts with:

```yaml
name: statuspage
```

If you copy the manifest and change `name: vercel_status`, the schema becomes `vercel_status`.

## Requirements

- Coral CLI installed and available as `coral`
- A public Statuspage URL, without a trailing slash
- No API token or credentials

Quick CLI check:

```bash
coral source add --help
```

The current Coral CLI registers file-based sources using the top-level `name:` from the YAML file. It does not support `coral source add --file ./manifest.yaml --name some_name`.

## Verify A Vendor URL

Before adding a vendor, confirm that the URL exposes the public Statuspage API:

```bash
curl -fsSL "https://status.openai.com/api/v2/status.json"
```

A compatible response includes top-level `page` and `status` fields.

Use the base URL without `/api/v2/...` and without a trailing slash when registering the source:

```text
https://status.openai.com
```

Do not use:

```text
https://status.openai.com/
https://status.openai.com/api/v2/status.json
```

## Add One Vendor

Use this flow when you only need one Statuspage vendor, or when using the default schema name `statuspage` is acceptable.

From the directory that contains `manifest.yaml`:

```bash
coral source add --file ./manifest.yaml --interactive
```

When prompted for `STATUSPAGE_BASE_URL`, enter a compatible Statuspage base URL:

```text
https://status.openai.com
```

Coral registers the source as `statuspage`, because the manifest contains `name: statuspage`.

Check that it was added:

```bash
coral source list
```

Test the source:

```bash
coral source test statuspage
```

Query it:

```bash
coral sql "SELECT indicator, description FROM statuspage.status"
```

## Add One Vendor Non-Interactively

Coral reads manifest inputs from environment variables when `--interactive` is not used:

```bash
STATUSPAGE_BASE_URL="https://status.openai.com" \
  coral source add --file ./manifest.yaml
```

This still registers the source as `statuspage`.

## Add Multiple Vendors

To register multiple vendors at the same time, each vendor needs its own manifest file with a unique top-level `name:`.

This is required because `coral source add --file` takes the schema name from the YAML file. The schema name is not prompted as an input.

### Example: GitHub And OpenAI

Create one manifest copy per vendor:

```bash
cp manifest.yaml github_status.yaml
cp manifest.yaml openai_status.yaml
```

Edit only the first line in each copied file:

```yaml
# github_status.yaml
name: github_status
```

```yaml
# openai_status.yaml
name: openai_status
```

Register each vendor with its own URL:

```bash
STATUSPAGE_BASE_URL="https://www.githubstatus.com" \
  coral source add --file ./github_status.yaml

STATUSPAGE_BASE_URL="https://status.openai.com" \
  coral source add --file ./openai_status.yaml
```

Confirm the schemas:

```bash
coral sql "SELECT DISTINCT schema_name FROM coral.tables"
```

Query both vendors:

```sql
SELECT 'GitHub' AS vendor, indicator, description
FROM github_status.status
UNION ALL
SELECT 'OpenAI' AS vendor, indicator, description
FROM openai_status.status;
```

## Add Another Vendor Later

To add Vercel later, first create a manifest file for it:

```bash
cp manifest.yaml vercel_status.yaml
```

Change the top-level `name:` in `vercel_status.yaml`:

```yaml
name: vercel_status
```

Then register it:

```bash
STATUSPAGE_BASE_URL="https://www.vercel-status.com" \
  coral source add --file ./vercel_status.yaml
```

Query it:

```bash
coral sql "SELECT indicator, description FROM vercel_status.status"
```

## Query Examples

Replace `statuspage` with your schema name if you registered a copied manifest such as `github_status`, `openai_status`, or `vercel_status`.

### Current Overall Status

```sql
SELECT indicator, description
FROM statuspage.status;
```

Common `indicator` values are `none`, `minor`, `major`, `critical`, and `maintenance`.

### Degraded Components

```sql
SELECT name, status, updated_at
FROM statuspage.components
WHERE status != 'operational'
ORDER BY updated_at DESC;
```

Component statuses can include `operational`, `degraded_performance`, `partial_outage`, `major_outage`, and `under_maintenance`.

### Active Incidents

```sql
SELECT id, name, impact, status, created_at, shortlink
FROM statuspage.active_incidents
ORDER BY created_at DESC;
```

This table returns only unresolved incidents. If it returns zero rows, the vendor has no active incident in Statuspage.

### Recent Incident History

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

### Upcoming Maintenance

```sql
SELECT name, status, impact, scheduled_for, scheduled_until, shortlink
FROM statuspage.upcoming_maintenances
ORDER BY scheduled_for ASC;
```

### Multi-Vendor Status Dashboard

```sql
SELECT 'GitHub' AS vendor, indicator, description
FROM github_status.status
UNION ALL
SELECT 'OpenAI' AS vendor, indicator, description
FROM openai_status.status
UNION ALL
SELECT 'Vercel' AS vendor, indicator, description
FROM vercel_status.status;
```

### Multi-Vendor Active Outages

```sql
SELECT 'GitHub' AS vendor, name, impact, status, created_at, shortlink
FROM github_status.active_incidents
UNION ALL
SELECT 'OpenAI' AS vendor, name, impact, status, created_at, shortlink
FROM openai_status.active_incidents
UNION ALL
SELECT 'Vercel' AS vendor, name, impact, status, created_at, shortlink
FROM vercel_status.active_incidents;
```

## Compatible Vendor URLs

These vendors expose Statuspage-style `/api/v2/*.json` endpoints:

| Vendor | Base URL |
|--------|----------|
| GitHub | `https://www.githubstatus.com` |
| Stripe | `https://status.stripe.com` |
| Atlassian | `https://status.atlassian.com` |
| PagerDuty | `https://status.pagerduty.com` |
| Twilio | `https://status.twilio.com` |
| Cloudflare | `https://www.cloudflarestatus.com` |
| Datadog | `https://status.datadoghq.com` |
| Slack | `https://status.slack.com` |
| OpenAI | `https://status.openai.com` |
| Vercel | `https://www.vercel-status.com` |
| Discord | `https://discordstatus.com` |
| Zoom | `https://status.zoom.us` |

AWS Health and Google Cloud Service Health use different APIs, so they are not drop-in `STATUSPAGE_BASE_URL` values for this manifest.

## Validate The Manifest

Run:

```bash
coral source lint ./manifest.yaml
```

Expected result:

```text
Manifest is valid
```

## Troubleshooting

### `unexpected argument '--name' found`

`--name` is not supported together with `--file` in this Coral CLI:

```bash
coral source add --file ./manifest.yaml --name openai_status
```

Use a copied manifest with a different top-level `name:` instead:

```bash
cp manifest.yaml openai_status.yaml
```

Edit `openai_status.yaml`:

```yaml
name: openai_status
```

Then add it:

```bash
STATUSPAGE_BASE_URL="https://status.openai.com" \
  coral source add --file ./openai_status.yaml
```

### `No such file or directory`

The file passed to `--file` must already exist.

This fails if `vercel_status.yaml` has not been created:

```bash
coral source add --file ./vercel_status.yaml
```

Create it first:

```bash
cp manifest.yaml vercel_status.yaml
```

Then edit the top-level `name:` and run `coral source add`.

### `Table ... not found`

The schema in your SQL must match the top-level `name:` in the manifest that was registered.

If you added the default `manifest.yaml`, query:

```bash
coral sql "SELECT indicator, description FROM statuspage.status"
```

If you added a copied manifest with `name: vercel_status`, query:

```bash
coral sql "SELECT indicator, description FROM vercel_status.status"
```

List registered schemas:

```bash
coral sql "SELECT DISTINCT schema_name FROM coral.tables"
```

### `coral query` Is Not A Command

Use `coral sql`:

```bash
coral sql "SELECT DISTINCT schema_name FROM coral.tables"
```

### Empty Results

Empty arrays are normal for active incidents and maintenance tables when the vendor has no current outage or scheduled maintenance.

For example, this can legitimately return zero rows:

```sql
SELECT *
FROM statuspage.active_incidents;
```

### Incompatible URL

Verify the URL directly:

```bash
curl -fsSL "https://status.example.com/api/v2/status.json"
```

If the response does not include `page` and `status`, the vendor probably does not use Atlassian Statuspage at that URL.

### Trailing Slash In URL

Use:

```text
https://status.openai.com
```

Avoid:

```text
https://status.openai.com/
```

The manifest appends endpoint paths such as `/api/v2/status.json`.

## Data Notes

- Incidents are recent historical incidents. The exact retention window depends on the vendor.
- Components represent current state only, not historical component status.
- Maintenance endpoints include upcoming, active, and recently completed maintenance windows depending on the vendor.
- Public Statuspage APIs are usually generous with rate limits, but high-frequency polling should still include reasonable delays.

## API Documentation

- [Atlassian Statuspage API Docs](https://developer.statuspage.io/)
- All endpoints used by this source are public read-only endpoints and require no authentication.
