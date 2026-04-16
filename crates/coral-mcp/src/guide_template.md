# Coral Query Guide

{{SOURCES_SECTION}}

## Discovery Workflow

Always inspect queryable tables and table metadata before writing queries:

```sql
-- List visible tables, descriptions, SQL-ready refs, and required filters
SELECT schema_name, table_name, sql_table_ref, description, required_filters FROM coral.tables ORDER BY schema_name, table_name;

-- Inspect columns for one visible table, including SQL-ready refs
{{COLUMNS_EXAMPLE}}
```

## Query Guidance

- Always use `coral.tables.sql_table_ref`, `coral.columns.sql_column_ref`, and `coral.columns.sql_qualified_column_ref` when writing SQL. Copy them verbatim; never reconstruct or qualify identifiers yourself.
- Check `coral.tables.required_filters` and `coral.columns.is_required_filter` before querying tables that depend on filter-only inputs.
- Cross-source joins work with standard SQL after source scans complete.
- `list_tables` and `coral://tables` show queryable fully qualified tables; `coral.tables` and `coral.columns` provide richer SQL metadata.
