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
   - credential retrieval methods for secrets, including OAuth when the provider supports browser-based setup
   - tables
   - table functions for source-scoped parameterized endpoints
   - filters
   - response extraction
   - pagination
   - typed columns
4. Lint the source spec:
   - `coral source lint <path>`
5. Validate the source in the right mode:
   - standalone specs: `coral source add --file <path>` and inspect with `coral sql`
   - `coral source add` is non-interactive by default: each input `key` is read from the matching environment variable. Export required variables and secrets before running, or pass `--interactive` to be prompted.
   - for OAuth credential methods, run `coral source add --interactive --file <path>` with no environment value for the target secret so Coral offers the authored credential choices
   - repo sources or already-named sources: `coral source test <name>`
6. Inspect the exposed shape:
   - inspect `coral.tables` for visible tables, descriptions, guides, and required filters; keep metadata queries bounded with `LIMIT`/`OFFSET`
   - inspect `coral.table_functions` for source-scoped functions, arguments, result columns, kind, and search limits
   - inspect `coral.columns` for canonical column metadata, including `is_virtual` and `is_required_filter`; filter by one table or page large column sets
   - inspect `coral.filters` for normalized table filter names, types, modes, required flags, and descriptions
   - inspect `coral.inputs` to verify variables, secrets, defaults, hints, and required flags
7. Query representative tables with `coral sql`.
8. If you are relying on `coral source test`, make sure `test_queries` gives you a basic smoke/connection check for the source.
9. Refine the spec and repeat.

## Authoring Rules

- Start small and expand table coverage incrementally.
- Use the source manifest schema as both inspiration for authoring and validation of structure: https://github.com/withcoral/coral/blob/main/crates/coral-spec/src/schema/source_manifest.schema.json
- Use source variables for non-secret configuration.
- Use source secrets for credentials.
- For OAuth-backed services, model setup with `inputs.<TOKEN>.credential.methods[]` using `type: oauth`; keep the runtime `auth` or request header pointing at the same secret input.
- OAuth credential methods support device-code flow and authorization-code flow. For authorization-code flow, set `flow.type: authorization_code`, set `flow.pkce` explicitly to `required` or `disabled`, use a loopback `http://127.0.0.1` or `http://localhost` redirect URI, choose `redirect_uri_port_mode: random` for provider apps that allow variable localhost ports, and choose `fixed` only when users can register the exact non-zero redirect URI. The CLI also accepts the final loopback redirect URL pasted into the terminal when the browser cannot reach the machine running Coral, so do not reject authorization-code OAuth solely because users may run Coral over SSH, in a VM, or in another split-browser environment. For device-code flow, declare `flow.type: device_code`, `endpoints.device_authorization_url`, `endpoints.token_url`, and a public client ID; omit redirect URI fields and do not declare a client secret.
- OAuth endpoint URLs may template declared `kind: variable` inputs with `{{input.KEY}}` for non-secret endpoint components such as tenant IDs or domains. Do not reference secret inputs, filters, function arguments, state, or inline defaults from OAuth endpoint URLs.
- If a provider also supports manually pasted tokens, include a `type: source_config` fallback after the OAuth method. When the provider's token endpoint requires client authentication with a client secret, prompt for both OAuth client values: declare `client.id.input`, `client.secret.input`, and `client.secret.transport` (`basic_auth` or `request_body`).
- Do not add top-level source inputs solely for OAuth client credentials; `client.id.input` and `client.secret.input` are collected during OAuth setup.
- Each credential method accepts optional `label`, `description`, and `hint` fields, surfaced during interactive install and in the generated source docs. When an input offers more than one method, put the how-to-get-it guidance in each method's `hint` (rendered next to that method's fields) instead of in one long input-level `hint`, and scope each hint to the inputs that method collects.
- For short-lived OAuth access tokens, make sure the OAuth method can obtain refresh tokens when the provider supports them, and document any scopes, consent prompts, or client settings required for refresh-token issuance. If the provider will not issue refresh tokens, call out that users must reconnect when access tokens expire unless the source has another supported long-lived credential path.
- Keep table and table-function names stable, SQL-friendly, and unique within
  the source's case-insensitive relation namespace. Prefer plain `snake_case`
  table names. Table-function names must start with an ASCII letter or
  underscore and then use only ASCII letters, numbers, or underscores.
- Mark filters as required only when the API truly requires them.
- Use default table functions for parameterized non-retrieval operations, such as scoped child collections, time-range logs, metrics queries, or detail operations that do not map cleanly to a stable table.
- Use `kind: search` table functions for provider endpoints that accept query text and return ranked candidates.
- Do not model provider search as a table filter. Use `mode: contains` only for ordinary provider-side substring filters. Provider-ranked retrieval belongs in a `kind: search` function.
- Include `search_limits` on every `kind: search` function and expose stable result identifiers for follow-up detail queries.
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
- For OAuth methods:
  - use a user-facing `label` such as `Connect with GitHub`
  - keep `description` to a short one-line blurb; put the setup detail in the method's `hint`
  - in the method's `hint`, list the required OAuth scopes and explain whether users must register a fixed loopback redirect URI or provide their own OAuth client ID/secret; when the method collects `client.id.input`/`client.secret.input`, say where to obtain those values
  - for authorization-code flow, note in the method's `hint` that users can paste the final localhost redirect URL into the terminal if their browser cannot reach Coral's loopback listener directly
  - when a secret offers multiple methods, scope each method's `hint` to the inputs that method collects instead of writing one broad input-level hint that mixes guidance for every method
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
coral sql "SELECT function_name, kind, arguments_json, result_columns_json, search_limits_json FROM coral.table_functions WHERE schema_name = 'my_source' ORDER BY function_name LIMIT 50 OFFSET 0"
coral sql "SELECT table_name, filter_name, filter_mode, is_required, data_type, description FROM coral.filters WHERE schema_name = 'my_source' ORDER BY table_name, filter_name LIMIT 100 OFFSET 0"
coral sql "SELECT table_name, column_name, data_type, is_virtual, is_required_filter, filter_mode, description FROM coral.columns WHERE schema_name = 'my_source' ORDER BY table_name, ordinal_position LIMIT 100 OFFSET 0"
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
- define auth headers or other runtime auth fields
- define `credential.methods` on secret inputs when setup should offer OAuth or another retrieval choice
- define request path, query, and body only where needed
- define source-scoped table functions for provider-native operations that require invocation arguments
- define response `rows_path`
- define pagination explicitly when the provider pattern is known
- define typed columns
- add `test_queries` once you know which simple query or queries should confirm the source basically works

Read `references/http-source-checklist.md` when you need table-shape and pagination guidance.

If your HTTP source uses an Authorization header with a prefix (e.g. `Authorization: Bearer <token>`), use a secret input for the raw token and define the header with `from: bearer`:

```yaml
inputs:
  FOOBAR_API_TOKEN:
    kind: secret
    hint: Bearer token for the Foobar API.
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: bearer
      key: FOOBAR_API_TOKEN
```

For an OAuth-backed HTTP source, add the retrieval method to that same secret input:

```yaml
inputs:
  FOOBAR_API_TOKEN:
    kind: secret
    hint: Connect with Foobar OAuth or paste a token with read access.
    credential:
      methods:
        - type: oauth
          label: Connect with Foobar
          description: Open a browser and authorize Coral to read Foobar data.
          hint: |
            Signs you in through Foobar and requests the `read` scope. To
            use your own app, set FOOBAR_OAUTH_CLIENT_ID to its Client ID.
          oauth:
            flow:
              type: authorization_code
              pkce: required
            redirect_uri: http://127.0.0.1:0/oauth/callback
            redirect_uri_port_mode: random
            endpoints:
              authorization_url: https://foobar.example.com/oauth/authorize
              token_url: https://foobar.example.com/oauth/token
            client:
              id:
                input: FOOBAR_OAUTH_CLIENT_ID
            scopes:
              scope:
                delimiter: space
                values:
                  - read
        - type: source_config
          label: Paste token
          hint: Paste a Foobar API token with read access to the data you query.
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: bearer
      key: FOOBAR_API_TOKEN
```

When a provider accepts either a full pasted API-key header or an OAuth access token, declare both credential inputs as optional secrets, then use `from: one_of` and put the complete header value first, followed by a `from: bearer` OAuth fallback:

```yaml
inputs:
  FOOBAR_API_KEY:
    kind: secret
    required: false
  FOOBAR_OAUTH_ACCESS_TOKEN:
    kind: secret
    required: false
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: one_of
      values:
        - from: input
          key: FOOBAR_API_KEY
        - from: bearer
          key: FOOBAR_OAUTH_ACCESS_TOKEN
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
