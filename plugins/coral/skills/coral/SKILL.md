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

For clear source intent:

2. Use targeted catalog discovery first: `search_catalog` with `schema` when possible, or `list_catalog` with `schema` or `kind`.
3. Read only the catalog item or items needed for the likely query: `sql_reference`, `sql_call_example`, and `required_filters`.
4. Use `list_columns` or `coral.columns` only for the specific table you plan to query, and only when the catalog result does not provide enough detail.
5. Query with `sql` as soon as the table or function reference, required filters or arguments, and useful columns are known.

When the source or entity is unclear:

2. Read `coral://guide` or use catalog discovery to identify available schemas and likely query surfaces.
3. Then switch back to source-scoped discovery.

For deeper discovery:

- Inspect `coral.table_functions` when table-function arguments or result columns are not clear from `search_catalog` or `list_catalog`.
- Inspect `coral.inputs` when source configuration affects the answer.
- Inspect broader `coral.tables`, `coral.columns`, `coral.filters`, or `coral.table_functions` metadata only when the source is unclear, a query fails, or the user asks for inventory.

Summarize evidence, gaps, and next action. If editing code, use the Coral result to guide changes.

## Query Rules

- Use each table's `sql_reference`; write `github.pulls` or `"github"."pulls"`, not `"github.pulls"`.
- Use each table function's `sql_call_example`, filling in required arguments before querying it.
- Keep metadata discovery bounded: page catalog discovery, query `coral.columns` for one table or `coral.table_functions` for one source when possible, and add `LIMIT` when reading broad metadata directly.
- Do not inspect columns, functions, inputs, or broad metadata just to be complete. Stop discovery when you can write a safe first SQL query.
- Virtual columns are filter-only and return `NULL`; check `is_virtual`.
- Required filters must appear in `WHERE`; inspect `required_filters` and `is_required_filter`.
- Secret inputs always return `value = NULL`; use `is_set`.
- Cross-source joins work and execute locally after source scans complete.
- Keep answers compact: name the source, table, required filters, and query shape. Avoid exhaustive column dumps unless requested.
- Lead with the answer or blocker. Include SQL only when it helps the user trust or reuse the result.

## Boundaries

- Manifest fallback is only by request; inspect the smallest relevant sections and summarize table/filter shape.
- Do not paste large manifest excerpts, present source-wide conclusions without query coverage, or treat query failures as empty results.

## Feedback

If the MCP `feedback` tool is available, file feedback when Coral blocks progress, pushes an unproductive pattern, or a vendor tool was easier for the same read.

Include `trying_to_do`, `tried`, and `stuck`, with table/source names, query snippets, and error text. Do not file feedback for ordinary empty results or missing credentials unless Coral made the problem unclear.
