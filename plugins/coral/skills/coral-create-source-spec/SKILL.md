---
name: coral-create-source-spec
description: Create or update a Coral source spec YAML for a custom HTTP API or local dataset. Use when authoring a standalone source for `coral source add --file`, or when adapting that spec into a Coral repo source under `sources/core` or `sources/community`.
---

# Coral Create Source Spec

Use this skill when the task is to author or repair a Coral source spec.

## Goal

Produce a valid, queryable Coral source spec that works with:

- `coral source lint <path>`
- `coral source add --file <path>`
- `coral source test <name>`
- `coral sql`
- `coral.tables` and `coral.columns`
- `coral.inputs` for source variables and secrets

## Default Mode

Default to standalone source authoring for external developers.

That means:

- create a YAML source spec file
- lint it early with `coral source lint <path>`
- add it to Coral with `coral source add --file <path>` when you need to exercise it as a source
- validate by querying it
- iterate until the shape is correct

Only switch to Coral repo layout when the user is explicitly editing the Coral repo.

## Output Modes

- External authoring:
  - create a standalone source spec such as `./my-source.yaml`
  - validate structure with `coral source lint ./my-source.yaml`
  - load it with `coral source add --file ./my-source.yaml` when you need to query it through Coral
- Coral repo contribution:
  - write community source specs to `sources/community/<name>/manifest.yaml`
  - write core source specs to `sources/core/<name>/manifest.yaml` only when the user is intentionally changing bundled core sources
  - add representative `test_queries` for a basic smoke/connection check of the source
  - validate with `coral source test <name>` and repo checks

## Workflow

1. Read the provider API docs or inspect the local dataset.
2. Start with one small table and a few columns.
3. Define:
   - source metadata
   - backend
   - base URL or file location
   - auth
   - variables and secrets
   - tables
   - filters
   - response extraction
   - pagination
   - typed columns
4. Lint the source spec:
   - `coral source lint <path>`
5. Validate the source in the right mode:
   - standalone specs: `coral source add --file <path>` and inspect with `coral sql`
   - `coral source add` is non-interactive by default: each input `key` is read from the matching environment variable. Export required variables and secrets before running, or pass `--interactive` to be prompted.
   - repo sources or already-named sources: `coral source test <name>`
6. Inspect the exposed shape:
   - inspect `coral.tables` for visible tables, descriptions, guides, and required filters; keep metadata queries bounded with `LIMIT`/`OFFSET`
   - inspect `coral.columns` for canonical column metadata, including `is_virtual` and `is_required_filter`; filter by one table or page large column sets
   - inspect `coral.inputs` to verify variables, secrets, defaults, hints, and required flags
7. Query representative tables with `coral sql`.
8. If you are relying on `coral source test`, make sure `test_queries` gives you a basic smoke/connection check for the source.
9. Refine the spec and repeat.

## Authoring Rules

- Start small and expand table coverage incrementally.
- Use the source manifest schema as both inspiration for authoring and validation of structure: https://github.com/withcoral/coral/blob/main/crates/coral-spec/src/schema/source_manifest.schema.json
- Use source variables for non-secret configuration.
- Use source secrets for credentials.
- Keep table names stable and SQL-friendly.
- Mark filters as required only when the API truly requires them.
- Prefer explicit pagination when the API shape is known.
- Verify pagination with actual row fetches, not only `COUNT(*)`.
- Add or update `test_queries` when you want `coral source test` to perform a basic smoke/connection check.

## Metadata UX Rules

Use these rules for top-level source metadata so source discovery and setup are consistent.

### `description`

- Start with `Query ...`.
- Make the first sentence capability-first: list the key entities users can query.
- Preferred template:
  - `Query <entities> from <Provider> (<Cloud or self-hosted when relevant>).`
- Keep `description` focused on data coverage, not setup steps.
- Do not use vague phrasing such as:
  - `REST API v3`
  - `OpenAPI provider`
  - `... and more`
- Move auth/setup/permission details to input hints, not description text.

### Input hints (`inputs.<KEY>.hint`)

Each hint should tell the user:

- what value is expected
- how to obtain it
- minimum scope/permission guidance
- one concrete format/example when useful

Specific guidance:

- For URL/base inputs:
  - say what the default means
  - include at least one concrete example
  - include self-hosted guidance when supported
- For secrets:
  - name the exact credential type (API key, PAT, application key, etc.)
  - include format constraints when relevant (for example, token prefixes)
  - include least-privilege scope guidance
- For derived secrets (for example Basic auth blobs):
  - include a short shell example (for example a Base64 command)
- Prefer stable documentation links.
  - Use official docs links and stable settings pages.
  - Avoid brittle click-path instructions as the primary guidance.

Keep hints concise and directly actionable.

## Validation Loop

Use this loop during authoring:

```sh
# Export any required inputs first (key matches the input `key` in the spec),
# or pass --interactive to be prompted.
coral source lint ./my-source.yaml
coral source add --file ./my-source.yaml
coral source test my_source
coral sql "SELECT schema_name, table_name, description, required_filters FROM coral.tables WHERE schema_name = 'my_source' ORDER BY schema_name, table_name LIMIT 50 OFFSET 0"
coral sql "SELECT table_name, column_name, data_type, is_virtual, is_required_filter, description FROM coral.columns WHERE schema_name = 'my_source' ORDER BY table_name, ordinal_position LIMIT 100 OFFSET 0"
coral sql "SELECT key, kind, value, default_value, hint, required, is_set FROM coral.inputs WHERE schema_name = 'my_source' ORDER BY key"
```

For repo sources or already-named sources, add `test_queries` for a basic smoke/connection check and run:

```sh
coral source test my_source
```

Then run targeted table queries until the source behaves correctly.

## HTTP Sources

For HTTP-backed sources:

- define `backend: http`
- define `base_url`
- define auth headers
- define request path, query, and body only where needed
- define response `rows_path`
- define pagination explicitly when the provider pattern is known
- define typed columns
- add `test_queries` once you know which simple query or queries should confirm the source basically works

Read `references/http-source-checklist.md` when you need table-shape and pagination guidance.

If your HTTP source uses an Authorization header with a prefix (e.g. `Authorization: Bearer <token>`), you can use a secret input for the token and define the header as a template:

```yaml
auth:
  headers:
    - name: Authorization
      from: template
      template: Bearer {{input.FOOBAR_API_KEY}}
```

## Local Data Sources

For local file-backed sources:

- define the file backend
- define the source location
- define file selection patterns if applicable
- define typed columns

## Deliverable

Report:

- source spec path
- lint / add / test commands used
- validation commands run
- assumptions made
- blocked or unverified endpoints
