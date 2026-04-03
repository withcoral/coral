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

## Query Guidance

- Fully qualify tables in SQL, for example `slack.messages`.
- Check `coral.tables.required_filters` and `coral.columns.is_required_filter` before querying tables that depend on filter-only inputs.
- Cross-source joins work with standard SQL after source scans complete.
- `list_tables` and `coral://tables` show queryable fully qualified tables; `coral.tables` and `coral.columns` provide richer SQL metadata.
