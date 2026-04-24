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

## JSON Columns

Some source tables expose JSON payloads as `Utf8` columns. Extract fields with the `json_*` functions — path segments are variadic, e.g. `json_get(payload, 'user', 'id')`.

- `json_get(json, path…)` returns a union. Casting to `Boolean`, `Int32/64`, `Float32/64`, or `Utf8` is rewritten to the matching typed function; casts to `Decimal*` stay on the normal cast path and preserve the requested precision/scale.
- Typed shortcuts: `json_get_bool`, `json_get_int`, `json_get_float`, `json_get_str` return the named type directly and yield NULL when the path is missing or the shape doesn't match.
- `json_get_json` returns nested JSON as text for further extraction; `json_get_array` returns `List<Utf8>`.
- `json_as_text` renders any value as text (scalars as their text form, objects/arrays as JSON).
- `json_contains` tests path existence; `json_length` returns array/object size; `json_object_keys` lists keys.

```sql
SELECT json_get_str(payload, 'event')              AS event,
       json_get(payload, 'user', 'id')::bigint     AS user_id,
       json_get(payload, 'amount')::decimal(18, 2) AS amount
FROM source.events;
```

## Query Guidance

- Fully qualify tables in SQL, for example `slack.messages`.
- Check `coral.tables.required_filters` and `coral.columns.is_required_filter` before querying tables that depend on filter-only inputs.
- Cross-source joins work with standard SQL after source scans complete.
- `list_tables` and `coral://tables` show queryable fully qualified tables; `coral.tables`, `coral.columns`, and `coral.inputs` provide richer SQL metadata.
