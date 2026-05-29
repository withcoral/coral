# Superset Coral source

Query Apache Superset metadata with Coral for BI governance, dashboard
inventory, and analytics workspace cleanup.

## Setup

Provide a Superset base URL and JWT bearer access token. Create the token with
`POST /api/v1/security/login`, then use the returned `access_token` value
without the `Bearer` prefix.

```bash
SUPERSET_BASE_URL=http://localhost:8088 \
SUPERSET_ACCESS_TOKEN=... \
coral source add --file sources/community/superset/manifest.yaml
```

The token user needs read access to dashboards, charts, datasets, databases,
SQL Lab saved queries, and query history. The `superset.users` and
`superset.roles` tables call `/api/v1/security/users/` and
`/api/v1/security/roles/`, so they require Admin or a custom role with the
corresponding security user and role read permissions.

Run validation:

```bash
coral source test superset
```

## Tables

| Table | Description |
| --- | --- |
| `superset.dashboards` | Dashboard metadata. |
| `superset.charts` | Chart metadata. |
| `superset.datasets` | Dataset metadata. |
| `superset.databases` | Database connection metadata. |
| `superset.saved_queries` | SQL Lab saved queries. |
| `superset.queries` | SQL Lab query history. |
| `superset.users` | Security users. |
| `superset.roles` | Security roles. |
| `superset.dashboard_charts` | Charts attached to a required dashboard ID or slug. |
| `superset.dashboard_datasets` | Datasets attached to a required dashboard ID or slug. |

## Example queries

```sql
SELECT dashboard_title, published, changed_on, url
FROM superset.dashboards
ORDER BY changed_on DESC
LIMIT 20;
```

```sql
SELECT slice_name, viz_type, datasource_name, changed_on
FROM superset.charts
ORDER BY changed_on DESC
LIMIT 20;
```

```sql
SELECT label, database__database_name, schema, changed_on
FROM superset.saved_queries
ORDER BY changed_on DESC
LIMIT 20;
```

```sql
SELECT slice_name, changed_on, slice_url
FROM superset.dashboard_charts
WHERE dashboard_id_or_slug = 'sales-overview';
```

## Notes

- Superset list endpoints accept pagination inside the `q` parameter. The
  current Coral HTTP pagination DSL cannot safely synthesize that Rison/JSON
  shape, so list tables expose the provider's default page instead of claiming
  complete pagination.
- `superset.queries` is kept conservative with a default fetch limit because it
  represents SQL Lab audit history.
- Query history timestamps use Superset's numeric `start_time` and `end_time`
  fields. Dashboard, chart, and dataset list timestamps use the documented
  UTC changed fields.
- `superset.dashboard_charts` follows the chart-definitions response shape:
  `id`, `slice_name`, `changed_on`, and `slice_url`.

## Validation evidence

Static validation run locally:

```bash
coral source lint sources/community/superset/manifest.yaml
make lint-sources
yamllint sources/community/superset/manifest.yaml
git diff --check origin/main..HEAD
gitleaks detect --no-banner --redact --source . --log-opts=origin/main..HEAD
```

Credentialed `coral source add --file`, `coral source test superset`, and
representative live queries require a Superset instance access token and were
not run in this workspace.

## API references

- https://superset.apache.org/developer-docs/api
- https://superset.apache.org/user-docs/6.0.0/api/
- https://superset.apache.org/developer-docs/api/queries/
- https://superset.apache.org/developer-docs/api/security-users/
- https://superset.apache.org/developer-docs/api/security-roles/
