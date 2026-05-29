# Tableau

Query Tableau Server or Tableau Cloud metadata from Coral. The source exposes
read-only project, workbook, view, data source, and user inventory using the
Tableau REST API.

## Authentication

This source expects a Tableau REST API auth token from the Tableau sign-in
endpoint. A minimal first-success path is to sign in with a personal access
token (PAT) and capture the returned credentials token and site LUID:

```bash
curl -X POST "$TABLEAU_SERVER_URL/api/$TABLEAU_API_VERSION/auth/signin" \
  -H "Content-Type: application/json" \
  -d '{
    "credentials": {
      "personalAccessTokenName": "coral",
      "personalAccessTokenSecret": "TABLEAU_PAT_SECRET",
      "site": {"contentUrl": "TABLEAU_SITE_CONTENT_URL"}
    }
  }'
```

Use the returned `credentials.token` as `TABLEAU_AUTH_TOKEN`. Use the returned
`credentials.site.id` as `TABLEAU_SITE_ID`; this is the site LUID used in REST
paths, not the browser-visible site content URL.

| Input | Description |
| --- | --- |
| `TABLEAU_SERVER_URL` | Tableau base URL, for example `https://prod-useast-a.online.tableau.com`. |
| `TABLEAU_API_VERSION` | REST API version supported by your Tableau site, such as `3.24`. |
| `TABLEAU_SITE_ID` | Tableau site LUID returned by sign-in and used in REST paths. |
| `TABLEAU_AUTH_TOKEN` | REST API token sent as `X-Tableau-Auth`. |

Use a least-privilege Tableau user with metadata read permissions. The
`tableau.users` table has a higher provider requirement: Tableau's Get Users on
Site endpoint requires server/site-admin permissions for username/password or
PAT sign-in, and JWT-connected apps require `tableau:users:read`.

## Tables

| Table | Description |
| --- | --- |
| `tableau.projects` | Project inventory. |
| `tableau.workbooks` | Workbook metadata. Supports Tableau REST `filter` syntax. |
| `tableau.views` | View metadata. Supports Tableau REST `filter` syntax. |
| `tableau.datasources` | Published data source metadata. Supports Tableau REST `filter` syntax. |
| `tableau.users` | Site users. Requires Tableau admin permissions or JWT `tableau:users:read`. |

## Examples

List recently updated workbooks:

```sql
SELECT id, name, project_name, owner_id, updated_at
FROM tableau.workbooks
ORDER BY updated_at DESC
LIMIT 25;
```

Find views in a workbook:

```sql
SELECT id, name, content_url, updated_at
FROM tableau.views
WHERE filter = 'workbookId:eq:workbook_luid';
```

Review user roles:

```sql
SELECT id, name, full_name, site_role, last_login
FROM tableau.users;
```

## Notes

- Tableau list endpoints use `pageNumber` and `pageSize` pagination.
- The source reads metadata only and does not download workbook, view, or data
  source content.
- `TABLEAU_AUTH_TOKEN` is short-lived in many Tableau deployments. Refresh it
  before running long-lived workflows.
- `tableau.users` may fail for non-admin metadata readers even when content
  tables work, because Tableau applies stronger permissions to user inventory.
- Live API tests passed against a Tableau Cloud site. The source used a
  short-lived Tableau REST auth token generated from the sign-in endpoint.
- For `tableau.views`, the tested Tableau Cloud REST response used a wrapper
  object with rows under `views.view[]`, so the source maps
  `rows_path: [views, view]`.

Redacted `GET /api/3.25/sites/{site_id}/views?pageSize=1` response shape:

```json
{
  "pagination": {
    "pageNumber": "1",
    "pageSize": "1",
    "totalAvailable": "<redacted>"
  },
  "views": {
    "view": [
      {
        "id": "<view-luid>",
        "name": "<view-name>",
        "contentUrl": "<content-url>",
        "workbook": {
          "id": "<workbook-luid>"
        },
        "owner": {
          "id": "<owner-luid>"
        },
        "createdAt": "<timestamp>",
        "updatedAt": "<timestamp>"
      }
    ]
  }
}
```

## Validation

- YAML parsing: passed
- Coral manifest schema validation: passed
- `git diff --check`: passed
- `make lint-sources`: passed
- Live API tests: passed against a Tableau Cloud site

Live Coral evidence:

```text
✓ tableau connected successfully
Secrets: keychain

tableau (5 tables)
├─ datasources
├─ projects
├─ users
├─ views
└─ workbooks
Query tests
3 declared · 3 passed · 0 failed

✓ SELECT id, name FROM tableau.projects LIMIT 1
  1 row

✓ SELECT id, name, project_name FROM tableau.workbooks LIMIT 1
  1 row

✓ SELECT id, name, workbook_id FROM tableau.views LIMIT 1
  1 row
```

Representative query:

```sql
SELECT id, name, project_name, updated_at
FROM tableau.workbooks
LIMIT 3;
```
