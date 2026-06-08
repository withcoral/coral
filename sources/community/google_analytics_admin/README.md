# Google Analytics Admin API (Community)

**Version:** 0.1.0
**Backend:** HTTP (Analytics Admin API v1beta)
**Tables:** 3
**Base URL:** `https://analyticsadmin.googleapis.com`

Inventory Google Analytics GA4 accounts, properties, and data streams via the
Analytics Admin API v1beta. Authenticates with Google OAuth
(authorization_code + PKCE).

## Install

Community sources are not bundled with the Coral binary. Add the manifest from
this directory:

```bash
coral source add --file sources/community/google_analytics_admin/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to
`coral source add --file`.

## Authentication and setup

Requires a Google OAuth client ID and client secret with the
`https://www.googleapis.com/auth/analytics.readonly` scope.

1. In [Google Cloud Console](https://console.cloud.google.com), create an
   OAuth 2.0 client ID (Desktop app type) under **APIs & Services → Credentials**.
2. Enable the **Google Analytics Admin API** under **APIs & Services → Library**.
3. Run the interactive setup:

```bash
coral source add --interactive --file sources/community/google_analytics_admin/manifest.yaml
```

Choose **Connect with Google**, then paste your `GOOGLE_OAUTH_CLIENT_ID` and
`GOOGLE_OAUTH_CLIENT_SECRET` when prompted. Coral completes the PKCE OAuth
flow locally and stores the token. The flow requests offline access and fresh
consent so Google can return a refresh token; Coral uses it to refresh
short-lived access tokens automatically.

To paste an access token directly instead:

```bash
export GOOGLE_ANALYTICS_TOKEN=ya29.a0...
coral source add --file sources/community/google_analytics_admin/manifest.yaml
```

## Tables

| Table | Required filters | Description |
| --- | --- | --- |
| `accounts` | none | All GA4 accounts accessible to the authenticated user |
| `properties` | `account_id` | GA4 properties under an account, including descendant subproperties |
| `data_streams` | `property_id` | Data streams attached to a property |

IDs use the resource name form:

- `account_id` → `accounts/123456789`
- `property_id` → `properties/987654321`

## Example queries

### List all GA4 accounts

```sql
SELECT account_id, display_name, region_code, create_time
FROM google_analytics_admin.accounts
LIMIT 10;
```

### Properties under an account

```sql
SELECT property_id, display_name, time_zone, currency_code, industry_category, service_level
FROM google_analytics_admin.properties
WHERE account_id = 'accounts/123456789'
ORDER BY display_name
LIMIT 20;
```

### Data streams for a property

```sql
SELECT stream_id, stream_type, display_name, web_uri, measurement_id
FROM google_analytics_admin.data_streams
WHERE property_id = 'properties/987654321'
LIMIT 10;
```

### Web streams for a property

`data_streams` requires a `property_id` filter — Coral cannot derive it from
a join predicate. To browse streams across multiple properties, run one query
per property ID returned by the step above.

```sql
SELECT stream_id, stream_type, display_name, web_uri, measurement_id
FROM google_analytics_admin.data_streams
WHERE property_id = 'properties/987654321'
  AND stream_type = 'WEB_DATA_STREAM'
LIMIT 20;
```

## Cross-source examples

### GA4 properties with open Sentry projects (join on slug/name similarity)

```sql
SELECT p.display_name AS ga4_property, sp.slug AS sentry_project
FROM google_analytics_admin.properties p
JOIN sentry.projects sp
  ON LOWER(sp.slug) = LOWER(REPLACE(p.display_name, ' ', '-'))
WHERE p.account_id = 'accounts/123456789'
LIMIT 20;
```

## Validation

```bash
# YAML style check
make lint-sources
coral source lint sources/community/google_analytics_admin/manifest.yaml

# Add interactively and run test queries
coral source add --interactive --file sources/community/google_analytics_admin/manifest.yaml
coral source test google_analytics_admin
```

The following sanitized live output was captured before the source was renamed
from `google_analytics` to `google_analytics_admin`. The provider requests and
authentication scope are unchanged; rerun the commands above with the final
source name in your environment.

```text
  ✓ google_analytics connected successfully
  Secrets: keyring

    google_analytics (3 tables)
    ├─ accounts
    ├─ data_streams
    └─ properties

    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT account_id, display_name FROM google_analytics.accounts LIMIT 1
      1 row
```

```bash
# Representative query — list accounts (output sanitized)
coral sql "SELECT account_id, display_name, region_code FROM google_analytics_admin.accounts LIMIT 3"
```

```text
+--------------------+-------------------------+-------------+
| account_id         | display_name            | region_code |
+--------------------+-------------------------+-------------+
| accounts/123456789 | Acme Corp Analytics     | US          |
| accounts/234567890 | Staging Account         | US          |
| accounts/345678901 | Mobile App Analytics    | GB          |
+--------------------+-------------------------+-------------+
3 rows
```

## Limitations

- **Admin API only** — account, property, and stream metadata only. GA4
  reporting data (sessions, events, conversions) requires the GA4 Data API
  (`analyticsdata.googleapis.com`). Coral HTTP sources use one base URL, so
  reporting belongs in a separate source.
- **No UA properties** — Universal Analytics (UA-*) properties are sunset and
  not supported; this source targets GA4 only.
- **Soft-deleted items hidden** — `showDeleted: false` is the default; deleted
  accounts and properties are excluded from results.
- **data_streams requires property_id** — Coral cannot derive required filters
  from join predicates. Query `google_analytics_admin.properties` first, then
  query `google_analytics_admin.data_streams` with a specific `property_id`.
- **Resource name IDs** — all IDs use the API resource name format
  (`accounts/123456789`, `properties/987654321`), not bare numeric IDs.
- **Rate limits and quotas** — the Analytics Admin API applies per-project and
  per-user quotas. The documented defaults are 1,200 requests/minute/project
  and 600 requests/minute/user. If you hit quota, wait for the 60-second refill
  window, reduce `LIMIT` values, or add delays between queries. See the
  [Admin API quota guide](https://developers.google.com/analytics/devguides/config/admin/v1/quotas)
  for current limits.
- Community sources are maintained separately from bundled core sources.

## API reference

- [Accounts.list](https://developers.google.com/analytics/devguides/config/admin/v1/rest/v1beta/accounts/list)
- [Properties.list](https://developers.google.com/analytics/devguides/config/admin/v1/rest/v1beta/properties/list)
- [DataStreams.list](https://developers.google.com/analytics/devguides/config/admin/v1/rest/v1beta/properties.dataStreams/list)
- [Admin API quotas](https://developers.google.com/analytics/devguides/config/admin/v1/quotas)

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md): discuss on the linked issue
first, sign the CLA if this is your first contribution, run `make lint-sources`,
and open a focused PR titled
`feat(sources/community/google_analytics_admin): add Google Analytics Admin source`.
