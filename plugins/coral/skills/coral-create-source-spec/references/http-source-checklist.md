# HTTP Source Checklist

Use this checklist when authoring an HTTP-backed Coral source.

## Start Small

- Begin with one collection endpoint.
- Add only a few columns first.
- Lint, add and query before expanding coverage.

## Source Header

Include:

- `name`
- `version`
- `dsl_version`
- `backend: http`
- `base_url`
- `auth`

Use:

- variables for non-secret configuration such as API base URLs
- secrets for API keys, tokens, and client secrets

## Description and Input Hints

- Start `description` with `Query ...`.
- Make description capability-first: list the core entities users can query.
- Keep setup/auth/scopes out of `description`; place those details in input hints.
- Avoid generic metadata text (`REST API`, `OpenAPI provider`, `... and more`).

For each input hint, include:

- expected value type
- where/how to get it
- minimum permission/scope guidance
- one concrete example when useful

Additional hint guidance:

- Base URL inputs should clarify default behavior and self-hosted alternatives.
- Secret inputs should name token type and any format constraints (for example token prefixes).
- For encoded credentials, include a short shell example (for example `printf ... | base64`).
- Prefer official docs links and stable settings pages over brittle click-path instructions.

## Table Design

- Prefer one table per collection endpoint.
- Add detail routes only when item fetches are actually needed.
- Keep table names stable and SQL-friendly.
- Preserve provider semantics when filter behavior matters.
- Add `test_queries` once you know which simple query or queries should confirm the source basically works.

## Response Extraction

- Set `rows_path` to the array Coral should read as rows.
- Use the default direct row strategy unless the payload shape requires something else.
- Keep the first pass simple; add special handling only after validating the payload shape.

## Filters

- Mark filters as required only when the upstream API requires them.
- Use seed queries to discover real IDs for child tables.
- If a table keeps failing with a missing required filter, inspect `coral.columns` and match the exact filter name.
- Use `test_queries` for the small set of queries you want `coral source test` to run as a smoke/connection check.

## Pagination

Prefer explicit pagination when the provider pattern is known.

- `limit` + `offset` APIs:
  - use offset pagination
- numbered-page APIs:
  - use page pagination
- cursor/token APIs:
  - use cursor pagination

Do not treat `COUNT(*)` as sufficient pagination proof. Fetch actual rows and confirm that results extend beyond one page.

## Validation Loop

Use this loop while iterating:

```sh
# `coral source add` reads each input from an env var named after its `key` by
# default. Export them first, or pass `--interactive` to be prompted.
coral source lint ./my-source.yaml
coral source add --file ./my-source.yaml
coral source test my_source
coral sql "SELECT table_name, description, required_filters FROM coral.tables WHERE schema_name = 'my_source' ORDER BY table_name LIMIT 50 OFFSET 0"
coral sql "SELECT table_name, column_name, data_type, is_virtual, is_required_filter, description FROM coral.columns WHERE schema_name = 'my_source' ORDER BY table_name, ordinal_position LIMIT 100 OFFSET 0"
```

If the source is named or lives in the Coral repo, add representative `test_queries` for a basic smoke/connection check and run:

```sh
coral source test my_source
```

Then run targeted table queries with real filters.
