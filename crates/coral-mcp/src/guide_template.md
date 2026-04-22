# Coral Query Guide

{{SOURCES_SECTION}}

## Discovery Workflow

Always inspect queryable tables and table metadata before writing queries:

```sql
-- List visible tables, descriptions, and required filters
SELECT schema_name, table_name, description, required_filters FROM coral.tables ORDER BY schema_name, table_name;

-- Inspect columns for one visible table
{{COLUMNS_EXAMPLE}}
```

## Per-Source Configuration

Per-source config values (e.g. Datadog site, Sentry org slug, GitHub API base URL) are exposed via `coral.inputs`. Use it to compose absolute URLs or account-scoped identifiers from source variables. Secret values are never exposed — secret rows always have `value IS NULL`, but `is_set` tells you whether the secret is configured.

```sql
-- Look up a variable value
SELECT value FROM coral.inputs
WHERE schema_name = 'datadog' AND kind = 'variable' AND key = 'DD_SITE';

-- Check which secrets are configured (without revealing values)
SELECT schema_name, key FROM coral.inputs
WHERE kind = 'secret' AND is_set;
```

## Query Guidance

- Fully qualify tables in SQL, for example `slack.messages`.
- Check `coral.tables.required_filters` and `coral.columns.is_required_filter` before querying tables that depend on filter-only inputs.
- Cross-source joins work with standard SQL after source scans complete.
- `list_tables` and `coral://tables` show queryable fully qualified tables; `coral.tables`, `coral.columns`, and `coral.inputs` provide richer SQL metadata.
