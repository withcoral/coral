# Coral Query Guide

{{SOURCES_SECTION}}

## Discovery Workflow

Always inspect queryable tables, source-scoped table functions, and table metadata before writing queries. Call table functions from `FROM` with named arguments, for example `github.search_issues(q => 'repo:withcoral/coral deploy failure')`.

```sql
-- List visible tables, descriptions, and required filters
SELECT schema_name, table_name, description, required_filters FROM coral.tables ORDER BY schema_name, table_name;

-- List source-scoped table functions, such as provider-native search
SELECT schema_name, function_name, description, arguments_json, result_columns_json FROM coral.table_functions ORDER BY schema_name, function_name;

-- Inspect columns for one visible table, including nullability and filter-only virtual columns
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
- `json_get_json` returns nested JSON as text for further extraction; `json_get_array` returns `List<Utf8>` where each element is JSON text. String array elements therefore include JSON quotes, e.g. `["\"phoebe-org\""]`. For plain string comparisons, prefer `json_get_str(json, ..., <index>)` when the index is known, or compare against JSON text.
- `json_as_text` renders any value as text (scalars as their text form, objects/arrays as JSON).
- `json_contains` tests path existence; `json_length` returns array/object size; `json_object_keys` lists keys.

```sql
SELECT json_get_str(payload, 'event')              AS event,
       json_get(payload, 'user', 'id')::bigint     AS user_id,
       json_get(payload, 'amount')::decimal(18, 2) AS amount
FROM source.events;
```

```sql
-- json_get_array returns JSON text elements, so string values include quotes.
SELECT *
FROM launchdarkly.flag_environments
WHERE json_get_str(rules, 0, 'clauses', 0, 'values', 0) = 'phoebe-org';
```

## Query Guidance

- Use each table's `sql_reference` from `list_tables` or `coral://tables` in `FROM` and `JOIN` clauses, for example `slack.messages`.
- Do not quote the whole `schema.table` string. Write `github.pulls` or `"github"."pulls"`, not `"github.pulls"`.
- Check `coral.tables.required_filters` and `coral.columns.is_required_filter` before querying tables that depend on filter-only inputs.
- Cross-source joins work with standard SQL after source scans complete.
- Use `LIKE` or `ILIKE` for SQL wildcard matching with `%` and `_`. `SIMILAR TO` uses regex-shaped patterns, so write `.*` instead of `%`, `.` instead of `_`, or escape literal percent/underscore characters as `\%` and `\_`.
- Regex operators such as `~` and `~*` treat `%` and `_` as ordinary literal characters.
- `list_tables` shows queryable fully qualified tables in pages; pass `schema`, `limit`, and `offset` to narrow large catalogs.
- `search_tables` searches table names, descriptions, guides, and required filters with a Rust regex; use it before broad SQL metadata scans when you know part of the table name or required filters.
- `describe_table` returns one compact table detail with guide text, required filters, and column count; use `coral.columns` when you need full column details.
- `list_columns` lists columns for one table; pass `pattern`, `required_only`, `limit`, and `offset` to inspect large schemas progressively.
- `coral://tables` shows table summaries for all installed sources; `coral.tables`, `coral.columns`, and `coral.inputs` provide richer SQL metadata.
