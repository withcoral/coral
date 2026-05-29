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
SELECT slice_name, viz_type
FROM superset.dashboard_charts
WHERE dashboard_id_or_slug = 'sales-overview';
```

## API references

- https://superset.apache.org/developer-docs/api
- https://superset.apache.org/user-docs/6.0.0/api/
- https://superset.apache.org/developer-docs/api/queries/
- https://superset.apache.org/developer-docs/api/security-users/
- https://superset.apache.org/developer-docs/api/security-roles/
