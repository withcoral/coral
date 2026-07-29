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
- `credential.methods` on secret inputs when setup should offer OAuth or a manual token choice
- separate `kind: identity` manifests for reusable identity setup contracts, when the task explicitly calls for one

## Authentication Setup

- Keep credential collection separate from runtime auth. `inputs` stores values; `auth`, request headers, query params, or body fields decide where values are sent.
- Use `from: bearer` for raw access tokens that must be sent as `Authorization: Bearer <token>`. Use `from: one_of` when runtime auth should choose the first present credential, such as a pasted API-key header before an OAuth bearer token.
- When `from: one_of` models alternate stored credentials, mark each branch's secret input `required: false` so either credential can satisfy auth.
- Use `type: source_config` when the user should provide a stored secret directly through an environment variable or prompt.
- Use `type: oauth` when the provider should issue the source secret through OAuth device-code or authorization-code flow during `coral source add --interactive`.
- OAuth methods require `flow.type: device_code` or `flow.type: authorization_code`. Authorization-code methods require an explicit `flow.pkce` of `required` or `disabled`; device-code methods require `endpoints.device_authorization_url`, `endpoints.token_url`, and either a public client ID or `client.dynamic_registration`, and omit redirect URI fields and static client secrets.
- OAuth redirect URIs must use `http://127.0.0.1` or `http://localhost`. Use `redirect_uri_port_mode: random` with no port or port `0` when the provider accepts variable loopback ports. Use `fixed` with a non-zero port when the OAuth app must pre-register the exact URI. Authorization-code setup can still work when the browser cannot reach Coral's loopback listener directly, because the CLI lets users paste the final localhost redirect URL into the terminal.
- For public clients, declare `client.id.default`, `client.id.input`, or both. When the provider's token endpoint requires client authentication with a client secret, prompt for both OAuth client values: declare `client.id.input`, `client.secret.input`, and `client.secret.transport` (`basic_auth` or `request_body`).
- For Dynamic Client Registration, Coral always registers a native client; source specs cannot configure a `web` Dynamic Client Registration application type. Set `request_refresh_token_grant: true` only when the provider supports registering clients for refresh-token grants.
- OAuth URL fields may use `{{input.KEY}}` only for declared `kind: variable` inputs. Use this for non-secret tenant, site, region, or domain URL components in `endpoints.*`, `resource`, or `client.dynamic_registration.registration_url`; never use secrets or runtime tokens in OAuth URLs.
- Do not add top-level source inputs solely for OAuth client credentials; `client.id.input` and `client.secret.input` are collected during OAuth setup.
- If the provider supports pasted tokens too, put the OAuth method first and add a `source_config` fallback.
- For short-lived OAuth access tokens, document the scopes, consent prompts, or client settings required for refresh-token issuance. If the provider will not issue refresh tokens, call out that users must reconnect when access tokens expire.
- For `kind: identity` manifests, provider `issuer`, identity `type`, setup inputs, OAuth method config, and audience metadata belong on the identity spec. Identity OAuth client IDs may be defaults or variable inputs; client secrets must be declared secret inputs; OAuth URLs may template only variable inputs; identity inputs do not declare source credential methods.

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
- OAuth setup guidance — required scopes, client ID/secret expectations, and redirect URI registration — belongs in the OAuth method's `hint` (`credential.methods[].hint`), not the input-level hint, since that text renders next to the fields the method collects.
- When a secret declares multiple `credential.methods`, write a focused `hint` on each method (`credential.methods[].hint`) instead of one long input-level hint; the fields shown change with the selected method, so each method's hint should cover only the inputs it collects.
- For encoded credentials, include a short shell example (for example `printf ... | base64`).
- Prefer official docs links and stable settings pages over brittle click-path instructions.

## Table Design

- Prefer one table per collection endpoint.
- Add detail routes only when item fetches are actually needed.
- Keep table and table-function names stable, SQL-friendly, and unique within
  the source's case-insensitive relation namespace. Prefer plain `snake_case`
  table names. Table-function names must start with an ASCII letter or
  underscore and then use only ASCII letters, numbers, or underscores.
- Preserve provider semantics when filter behavior matters.
- Add `test_queries` once you know which simple query or queries should confirm the source basically works.

## Search and Retrieval

- Use default table functions for parameterized non-retrieval operations, such as scoped child collections, time-range logs, metrics queries, or detail operations that do not map cleanly to a stable table.
- Use `functions` with `kind: search` for provider-native search endpoints that accept query text and return provider-ranked candidates.
- Add `search_limits` to every `kind: search` function.
- Keep search function arguments close to the provider API, and use `bind.arg` when the SQL argument name should differ from the request argument name.
- Set a function arg `type` when the provider argument is not string-shaped; omitted arg types default to `Utf8`.
- Search result columns should include stable identifiers and useful candidate metadata such as title, URL, score, rank, or timestamps when the provider returns them.
- Do not model provider-native search as a table filter. Use `mode: contains` only for ordinary substring filters on normal list/detail tables. Provider-ranked retrieval belongs in a `kind: search` function.
- If a search result is not a complete entity, make sure the returned identifier can be used with ordinary detail tables or required filters.

## Universal Search Authorization

- Universal Search fan-out is DSL v4-only. Reject
  `functions[].universal_search` in DSL v3 sources; they do not participate in
  native fan-out.
- Keep authorization attached to source-authored DSL v4 identity. Use top-level
  `universal_search.routes.<route_id>` with an exact `target.operation_id` on
  the source's singular surface. Make every authored route ID match
  `[a-z][a-z0-9_]*`.
- For an allowed DSL v4 route, select the original imported input with
  `query_input.location` (`path`, `query`, or `tool_arg`) and
  `query_input.name`. Never authorize a generated projection or function name.
- The selected DSL v4 query input must import as an exposed `Utf8` argument
  with no default. Every other exposed input must already have a
  type-compatible default in the imported OpenAPI or MCP operation. These
  defaults are imported facts, not fields authored in the DSL v4 manifest.
- Use optional `result` mapping only for identity, display, and explicitly
  selected `result.attributes`. DSL v4 mappings use RFC 6901 JSON Pointers to
  original imported output fields. Pointers must be syntactically valid in the
  manifest and resolve exactly once during route resolution. Identity and
  display fields must be scalar; an attribute may select a structured value
  only when its schema permits bounded canonical JSON.
- Use `execute: false` for a denial. A denied DSL v4 route keeps its target and
  omits `query_input`. If the source contains any authored Universal Search
  policy, treat unlisted routes as unauthorized.
- Verify every explicit allow is retrieval-only and idempotent upstream. Do not
  infer authorization from legacy `mode: search`, a mutating HTTP method, or an
  unannotated MCP tool. Missing targets, ambiguous inputs, missing or
  type-incompatible imported defaults, duplicate targets, and unresolved
  result fields must fail closed rather than retargeting another operation.

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
coral sql "SELECT function_name, kind, arguments_json, result_columns_json, search_limits_json FROM coral.table_functions WHERE schema_name = 'my_source' ORDER BY function_name LIMIT 50 OFFSET 0"
coral sql "SELECT table_name, filter_name, filter_mode, is_required, data_type, description FROM coral.filters WHERE schema_name = 'my_source' ORDER BY table_name, filter_name LIMIT 100 OFFSET 0"
coral sql "SELECT table_name, column_name, data_type, is_virtual, is_required_filter, filter_mode, description FROM coral.columns WHERE schema_name = 'my_source' ORDER BY table_name, ordinal_position LIMIT 100 OFFSET 0"
```

If the source is named or lives in the Coral repo, add representative `test_queries` for a basic smoke/connection check and run:

```sh
coral source test my_source
```

Then run targeted table queries with real filters.
