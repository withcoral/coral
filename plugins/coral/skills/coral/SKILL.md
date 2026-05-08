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
2. Discover tables with `list_tables`; page large catalogs and narrow by schema when useful.
3. Read `list_tables`, `coral://guide`, or `coral://tables` for `sql_reference`, `required_filters`, and examples.
4. Inspect `coral.columns` for candidate columns, required filters, virtual columns, and descriptions.
5. Inspect `coral.inputs` when source configuration affects the answer.
6. Query with `sql`: select useful columns, include required filters, and add `LIMIT` unless complete output is requested.
7. Summarize evidence, gaps, and next action. If editing code, use the Coral result to guide changes.

## Query Rules

- Use each table's `sql_reference`; write `github.pulls` or `"github"."pulls"`, not `"github.pulls"`.
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
