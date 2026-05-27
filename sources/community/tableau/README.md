# Tableau

Query Tableau Server or Tableau Cloud metadata from Coral. The source exposes
read-only project, workbook, view, data source, and user inventory using the
Tableau REST API.

## Authentication

This source expects a Tableau REST API auth token that was already obtained from
the Tableau sign-in endpoint.

| Input | Description |
| --- | --- |
| `TABLEAU_SERVER_URL` | Tableau base URL, for example `https://prod-useast-a.online.tableau.com`. |
| `TABLEAU_API_VERSION` | REST API version supported by your Tableau site, such as `3.24`. |
| `TABLEAU_SITE_ID` | Tableau site LUID used in REST paths. |
| `TABLEAU_AUTH_TOKEN` | REST API token sent as `X-Tableau-Auth`. |

Use a least-privilege Tableau user with metadata read permissions.

## Tables

| Table | Description |
| --- | --- |
| `tableau.projects` | Project inventory. |
| `tableau.workbooks` | Workbook metadata. Supports Tableau REST `filter` syntax. |
| `tableau.views` | View metadata. Supports Tableau REST `filter` syntax. |
| `tableau.datasources` | Published data source metadata. Supports Tableau REST `filter` syntax. |
| `tableau.users` | Site users visible to the authenticated user. |

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
- Live API tests passed against a Tableau Cloud site. The source used a
  short-lived Tableau REST auth token generated from the sign-in endpoint.

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
2 declared · 2 passed · 0 failed

✓ SELECT id, name FROM tableau.projects LIMIT 1
  1 row

✓ SELECT id, name, project_name FROM tableau.workbooks LIMIT 1
  1 row
```

Representative query:

```sql
SELECT id, name, project_name, updated_at
FROM tableau.workbooks
LIMIT 3;
```
