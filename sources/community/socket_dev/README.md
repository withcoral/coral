# Socket.dev source

This source queries [Socket.dev](https://socket.dev/) package supply-chain
intelligence through Coral SQL. It is read-only and exposes organization
discovery, API quota, dependency inventory, package metadata, package scores,
and package alerts.

## Authentication

Set `SOCKET_API_TOKEN` to a Socket organization API token:

```bash
export SOCKET_API_TOKEN="your_socket_org_token"
coral source add --file sources/community/socket_dev/manifest.yaml
```

Socket API tokens are organization-scoped. Package PURL lookups require the
`packages:list` scope. Organization discovery and quota checks require
authentication but do not require an additional scope.

The source defaults to `https://api.socket.dev/v0`. Override
`SOCKET_API_BASE` only if Socket documents a different API host for your
environment.

## Provider docs

- [Socket API authentication](https://docs.socket.dev/reference/authentication)
- [Socket OpenAPI document](https://api.socket.dev/v0/openapi)
- [Get Packages by PURL](https://docs.socket.dev/reference/batchpackagefetch)
- [Get Packages by PURL, org scoped](https://docs.socket.dev/reference/batchpackagefetchbyorg)
- [Get quota](https://docs.socket.dev/reference/getquota)
- [Search dependencies](https://docs.socket.dev/reference/searchdependencies)
- [Package scores](https://docs.socket.dev/docs/package-scores)
- [Alert categories](https://docs.socket.dev/docs/package-issues)
- [Ecosystem support](https://docs.socket.dev/docs/language-support)

## Tables

| Table | Required filters | Description |
|---|---|---|
| `quota` | none | Current Socket API quota for the configured token |
| `organizations` | none | Socket organizations visible to the configured token |
| `dependency_search` | `limit`, `offset` | Dependencies observed in the authenticated organization |
| `package_artifacts` | `org_slug`, `purl` | Package metadata, scores, alert summaries, and raw artifact data |
| `package_alerts` | `org_slug`, `purl` | One row per Socket alert for a package artifact |

Use `socket_dev.organizations` first to discover the `org_slug` required by
the package lookup tables.

## Example queries

Check API quota:

```sql
SELECT quota, max_quota, next_window_refresh
FROM socket_dev.quota
LIMIT 1;
```

Find the Socket organization slug:

```sql
SELECT slug, name, plan
FROM socket_dev.organizations
LIMIT 10;
```

Fetch package scores and metadata for one npm package:

```sql
SELECT
  name,
  version,
  score_overall,
  score_supply_chain,
  score_vulnerability,
  score_quality,
  score_maintenance,
  score_license
FROM socket_dev.package_artifacts
WHERE org_slug = 'your-org-slug'
  AND purl = 'pkg:npm/express@4.19.2'
LIMIT 1;
```

To wait for package analysis, include the optional package lookup filters:

```sql
SELECT name, version, score_overall
FROM socket_dev.package_artifacts
WHERE org_slug = 'your-org-slug'
  AND purl = 'pkg:npm/express@4.19.2'
  AND poll = 'true'
  AND timeout_sec = '120'
LIMIT 1;
```

Fetch alerts for one package:

```sql
SELECT type, severity, category, action, props
FROM socket_dev.package_alerts
WHERE org_slug = 'your-org-slug'
  AND purl = 'pkg:npm/express@4.19.2'
LIMIT 20;
```

Query a scoped npm package with PURL syntax:

```sql
SELECT name, version, score_overall, score_supply_chain
FROM socket_dev.package_artifacts
WHERE org_slug = 'your-org-slug'
  AND purl = 'pkg:npm/%40babel/core@7.24.0'
LIMIT 1;
```

Search dependencies observed by Socket:

```sql
SELECT repository, branch, type, namespace, name, version, direct
FROM socket_dev.dependency_search
WHERE limit = 50
  AND offset = 0
LIMIT 50;
```

`limit` must be between 1 and 100. `offset` must be 0 or greater.

## Notes

- Socket package lookups use Package URLs (PURLs), such as
  `pkg:npm/express@4.19.2`, `pkg:pypi/django@5.0.6`, or
  `pkg:maven/log4j/log4j@1.2.17`.
- Scoped npm packages should use encoded scope syntax in the PURL path, such
  as `pkg:npm/%40babel/core@7.24.0`.
- `package_artifacts` and `package_alerts` call Socket's org-scoped PURL
  endpoint, which returns newline-delimited JSON. The manifest decodes that
  response with Coral's `json_each_row` response format.
- Socket's org-scoped PURL endpoint consumes 100 quota units per request.
  Check `socket_dev.quota` before package lookups, keep package queries narrow,
  and expect provider rate-limit responses when quota is exhausted.
- Package lookups accept optional `poll` and `timeout_sec` filters. Set
  `poll = 'true'` to wait for Socket package analysis; `timeout_sec` maps to
  Socket's `timeoutSec` query parameter and must be between 1 and 1200 seconds.
  `package_artifacts` also accepts `include_alerts = 'true'` to request alert
  metadata in the raw package payload.
- Socket's older npm score and npm issues endpoints are deprecated. This source
  uses the org-scoped PURL endpoint instead so it supports multiple package
  ecosystems.
- `alerts`, `alert_priorities`, `authors`, `dependencies`, `manifest_files`,
  `props`, `fix`, `reachability`, and `action_source` are JSON columns because
  the upstream payloads are nested or alert-type-specific.

## Out of scope

- Creating full scans or diff scans
- Uploading manifests or SBOMs
- Mutating security policy, license policy, alert resolutions, or API tokens
- Webhook management
- Deprecated Socket report and npm-specific package score endpoints
