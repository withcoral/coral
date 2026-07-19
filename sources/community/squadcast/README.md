# Squadcast (Community)

**Version:** 0.1.0
**Backend:** HTTP (Squadcast API v3)
**Tables:** 2
**Base URL:** `https://api.squadcast.com` (US) or `https://api.eu.squadcast.com` (EU)

Query Squadcast services and users via API v3. Join Squadcast users with
Linear issues by email for cross-source on-call team intelligence.

## Install

```bash
coral source add --file sources/community/squadcast/manifest.yaml
```

## Authentication and setup

Requires a Squadcast access token, your account owner ID, and the API base
URL for your region (US or EU).

Generate an API token (refresh token) in the Squadcast web app, then exchange
it for an access token using the authentication endpoint for your region:

```bash
curl -X GET https://auth.squadcast.com/oauth/access-token \
  -H "X-Refresh-Token: <your-refresh-token>"
```

EU accounts must use `https://auth.eu.squadcast.com/oauth/access-token`.

Copy `data.access_token` from the response, note your owner ID from
**Settings -> General Settings -> Organization ID**, then run:

```bash
SQUADCAST_TOKEN=eyJ0eXAi... \
SQUADCAST_API_BASE_URL=https://api.squadcast.com \
SQUADCAST_OWNER_ID=your-owner-id \
coral source add --file sources/community/squadcast/manifest.yaml
```

For EU accounts set `SQUADCAST_API_BASE_URL=https://api.eu.squadcast.com`.

Use `data.expires_at` from the authentication response to determine when the
access token needs to be refreshed.

## Tables

| Table | Description |
| --- | --- |
| `services` | Services with ownership, escalation policy, maintenance state, and timestamps |
| `users` | Account users with display names, email, and role |

`SQUADCAST_OWNER_ID` is passed automatically as the required `owner_id`
parameter on `services` requests.

## Example queries

### Services overview

```sql
SELECT id, name, slug, escalation_policy, on_maintenance
FROM squadcast.services
ORDER BY name;
```

### Service ownership

```sql
SELECT name, escalation_policy, owner, maintainer, on_maintenance
FROM squadcast.services
ORDER BY name;
```

## Cross-source examples

### Engineers with open Linear issues

Requires the `linear` source.

```sql
SELECT u.username_for_display, u.email, COUNT(li.id) AS open_issues
FROM squadcast.users u
JOIN linear.issues li ON LOWER(li.assignee_email) = LOWER(u.email)
WHERE li.state_type != 'completed'
GROUP BY u.username_for_display, u.email
ORDER BY open_issues DESC
LIMIT 20;
```

## Validation

The `services` endpoint and representative query below were live-tested on an
earlier branch revision. This contract-alignment pass removed the unvalidated
`incidents` table and maps only fields present in the provider's current
OpenAPI schema.

```bash
# Previously captured representative live query (output sanitized)
coral sql "SELECT id, name, escalation_policy FROM squadcast.services LIMIT 3"
```

```text
+----------+-------------------+-----------------------------+
| id       | name              | escalation_policy           |
+----------+-------------------+-----------------------------+
| svc-0001 | Payment Service   | Engineering On-Call Policy  |
| svc-0002 | Auth Service      | Backend SRE                 |
| svc-0003 | API Gateway       | Platform Team               |
+----------+-------------------+-----------------------------+
3 rows
```

## Limitations

- **Access-token expiry** — use `data.expires_at` from the authentication
  response to determine when to exchange the refresh token again.
- **No incidents table** — the documented incident APIs expose individual
  incident operations and file-export workflows, not a provider-documented
  JSON list response that Coral can map safely.
- **No webhook or postmortem tables** — only services and users are exposed in
  v1.
- Community sources are maintained separately from bundled core sources.

## API reference

- [API overview and authentication](https://developers.incidents.cloud.solarwinds.com/api-reference)
- [Services list](https://developers.incidents.cloud.solarwinds.com/api-reference/services#get-v3-services)
- [Users list](https://developers.incidents.cloud.solarwinds.com/api-reference/users#get-v3-users)

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md): discuss on the linked issue
first, sign the CLA if this is your first contribution, run `make lint-sources`,
and open a focused PR titled
`feat(sources/community/squadcast): add squadcast community source`.
