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
2. Read `coral://catalog` once when the task needs source context, a broad all-source index, or likely table functions and no focused pattern exists. Prefer `search_catalog` with a focused pattern, `schema`, and `kind` when the task names an entity; use `list_catalog` only for paged browsing.
3. Read summary results or `coral://catalog` for `sql_reference`, `sql_call_example`, and `required_filters`; request `detail: "full"` only for a small catalog result set that needs guides or table-function result columns.
4. Use `search_columns` when you know a field, column, or data type but not the exact table; this avoids probing several tables with `describe_table` and `list_columns`.
5. For a candidate table, call `describe_table` first; it includes up to 50 compact column summaries. Call `list_columns` only for needed columns using `pattern`, `required_only`, and pagination.
6. Query `coral.sources`, `coral.columns`, `coral.table_functions`, `coral.filters`, or `coral.inputs` only for deeper multi-table introspection, source-authored context, filter modes, source configuration, or full table-function JSON.
7. Use `coral://guide` for query patterns, `coral://catalog` for source context plus table and table-function summaries, and `coral://tables` only when a table-only summary is enough.
8. Query with `sql`: select useful columns, include required filters or function arguments, and add `LIMIT` unless complete output is requested.
9. Summarize evidence, gaps, and next action. If editing code, use the Coral result to guide changes.

## Query Rules

- Use each table's `sql_reference`; write `github.pulls` or `"github"."pulls"`, not `"github.pulls"`.
- Use each table function's `sql_call_example`, filling in required arguments before querying it.
- Use the `sql` result's `row_count` and `columns` before issuing extra count or schema-discovery calls. Treat `row_count` as rows returned by the statement, not total rows beyond a `LIMIT`.
- Read source-authored context from `coral://guide` or `coral.sources` before probing provider identity, auth scope, or provider-specific search qualifiers.
- Keep metadata discovery bounded: prefer compact catalog summaries, focused `search_catalog` patterns, `search_columns` for cross-table field discovery, `describe_table`, and filtered `list_columns`; `search_catalog` ranks strong identifier matches first, so inspect early hits before broadening the search. Add `LIMIT` when reading broad metadata directly.
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
