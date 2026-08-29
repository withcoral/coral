---
name: coral
description: "Query live sources through Coral MCP. Use when the task needs GitHub, Jira, Slack, Linear, Datadog, Sentry, files, or connected data."
---

# Coral

## Overview

Use this as the Coral entrypoint for external context. Query Coral before answering from assumptions or changing code when live external state matters.

- Use Coral MCP tools/resources for discovery and query.
- Do not use the `coral` CLI, compile Coral, copy binaries, or bootstrap a server unless explicitly asked.
- Do not switch to vendor tools for the same read unless the user asks to continue without Coral or Coral does not cover the source.

## Support Checks

- Confirm Coral MCP tools/resources before making external-system claims.
- If Coral MCP is unavailable, state the blocker and stop; no local recovery.
- Distinguish missing source config, missing credentials, query errors, and empty results.
- If scope is missing, inspect guidance first, then ask for the smallest missing identifier.

## Workflow

1. Identify the needed source, entity, and scope from the user request.
2. Discover relevant tables, table functions, columns, and filters with `search`; use `list_catalog` when you need a paged catalog view narrowed by catalog, schema, or kind.
3. Use `describe` with `schema` and a bare `surface` name when you need exact metadata; add `catalog` for a three-part table. Coral resolves whether the surface is a table or table function. Keep the request target or earlier discovery result because an exact response returns metadata without repeating the target. Read `kind`, `guide`, `required_filters`, function arguments, and result columns; use `coral://guide` for query patterns and `coral://tables` for table summaries.
4. Inspect `coral.columns` for table columns, required filters, virtual columns, and descriptions.
5. Inspect `coral.table_functions` for source-scoped function guides, arguments, and result columns.
6. Inspect `coral.inputs` when source configuration affects the answer.
7. Query with `sql`: select useful columns, include required filters or function arguments, and add `LIMIT` unless complete output is requested.
8. Summarize evidence, gaps, and next action. If editing code, use the Coral result to guide changes.

## Worked Example

The Workflow above, demonstrated for one canonical request.

User: "What open issues are in `withcoral/coral` right now?"

1. `search_catalog` with `query: "github issues"` → returns `github.issues`.
2. `describe_table` with `name: "github.issues"` → reports required filters `owner`, `repo`.
3. `sql`:
   ```sql
   SELECT number, title, html_url, created_at
   FROM github.issues
   WHERE owner = 'withcoral' AND repo = 'coral' AND state = 'open'
   ORDER BY created_at DESC
   LIMIT 20
   ```
4. Lead with count and most recent row; cite `github.issues`; omit the SQL unless the user wants to reuse it.

## Cross-Source JOIN

A single statement spans multiple installed sources; each source scans locally before the join executes. Provider-direct MCPs cannot reproduce this.

Example — GitHub issues that still have no Linear attachment:

```sql
SELECT i.number, i.title, i.html_url
FROM github.issues i
LEFT JOIN linear.attachments l ON l.url = i.html_url
WHERE i.owner = 'withcoral' AND i.repo = 'coral'
  AND i.state = 'open'
  AND l.id IS NULL
```

- Confirm each schema is installed (`coral.tables` per schema) before composing the join.
- Apply the tightest filter on each side; sources scan independently before joining.
- Use `LEFT JOIN ... WHERE B.id IS NULL` for "missing on the other side" reports; `INNER JOIN` for symmetric correlation on a stable identifier.
- Reach for a cross-source JOIN when the user's question implies correlating two systems ("which X already has a Y", "which Y is missing an X"); do not synthesize across sources by running two queries and merging in prose.

## Query Rules

- Use each table's `sql_reference` when available. Empty `catalog_name` means `schema_name.table_name`; otherwise use `catalog_name.schema_name.table_name`. Quote identifiers separately, never the whole qualified reference.
- Use each table function's `sql_call_example`, filling in required arguments before querying it.
- Keep metadata discovery bounded: page catalog discovery, query `coral.columns` for one table or `coral.table_functions` for one source when possible, and add `LIMIT` when reading broad metadata directly.
- Virtual columns are filter-only and return `NULL`; check `is_virtual`.
- Required filters must appear in `WHERE`; inspect `required_filters` and `is_required_filter`.
- Secret inputs always return `value = NULL`; use `is_set`.
- Cross-source joins work and execute locally after source scans complete; see *Cross-Source JOIN* above.
- Keep answers compact: name the source, table, required filters, and query shape. Avoid exhaustive column dumps unless requested.
- Lead with the answer or blocker. Include SQL only when it helps the user trust or reuse the result.

## Boundaries

- Manifest fallback is only by request; inspect the smallest relevant sections and summarize table/filter shape.
- Do not paste large manifest excerpts, present source-wide conclusions without query coverage, or treat query failures as empty results.

## Feedback

If the MCP `feedback` tool is available, file feedback when Coral blocks progress, pushes an unproductive pattern, or a vendor tool was easier for the same read.

Include `trying_to_do`, `tried`, and `stuck`, with table/source names, query snippets, and error text. Do not file feedback for ordinary empty results or missing credentials unless Coral made the problem unclear.
