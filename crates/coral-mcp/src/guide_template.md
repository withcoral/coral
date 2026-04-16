# Coral Query Guide

{{SOURCES_SECTION}}

## Discovery Workflow

Always inspect queryable tables and table metadata before writing queries:

```sql
-- List visible tables, descriptions, and required filters
SELECT schema_name, table_name, description, required_filters FROM coral.tables ORDER BY schema_name, table_name;

-- Inspect columns for one visible table
{{COLUMNS_EXAMPLE}}

-- Discover visible source variables and effective values
SELECT schema_name, variable_key, variable_value FROM coral.source_variables ORDER BY schema_name, variable_key;
```

Use `coral.source_variables` when you need source-level context such as site domains, org slugs, or other non-secret config values. Secrets are not exposed there.

## Query Guidance

- Fully qualify tables in SQL, for example `slack.messages`.
- Check `coral.tables.required_filters` and `coral.columns.is_required_filter` before querying tables that depend on filter-only inputs.
- Use `coral.source_variables` to find effective non-secret source variables before constructing source-specific URLs or API paths.
- Cross-source joins work with standard SQL after source scans complete.
- `list_tables` and `coral://tables` show queryable fully qualified tables; `coral.tables`, `coral.columns`, and `coral.source_variables` provide richer SQL metadata.
