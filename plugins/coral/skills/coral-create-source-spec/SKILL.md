---
name: coral-create-source-spec
description: Create or update a Coral SourceSpec YAML for a custom OpenAPI, upstream MCP, GraphQL, or file source. Use when authoring a standalone source for `coral source add --file`, or when adapting that spec into the Coral repo.
---

# Coral Create Source Spec

Use this skill when the task is to author or repair a Coral SourceSpec.

## Goal

Produce a valid SourceSpec that works with:

- `coral source lint <path>`
- `coral source add --file <path>`
- `coral source test <name>` when SQL validation queries are declared
- MCP `search` / `describe`
- Code Mode `coral.sql.query(...)` when SQL bindings exist
- Code Mode generated TypeScript bindings when callable exports exist

## Default Mode

Default to standalone source authoring for external developers.

That means:

- create a YAML source spec file such as `./my-source.yaml`
- lint it early with `coral source lint <path>`
- add it to Coral with `coral source add --file <path>` when you need to
  exercise it
- inspect generated exports with MCP `search` and `describe`
- iterate until generated capabilities and bindings match the provider

Only switch to Coral repo layout when the user is explicitly editing the Coral
repo.

## SourceSpec Shape

Use SourceSpec v1 only:

```yaml
spec_version: 1
kind: source
name: my_source
description: Query useful entities from My Source.
inputs:
  - key: API_TOKEN
    kind: secret
interfaces:
  - id: rest
    type: openapi
    url: https://api.example.com/openapi.json
    auth:
      kind: bearer_input
      key: API_TOKEN
```

Supported interface types:

- `openapi`
- `mcp`
- `graphql`
- `file`

Do not author tables, columns, table functions, filters, runtime selectors, or
callable operation lists. Coral generates capabilities and exports from provider
snapshots.

## Workflow

1. Read the provider API docs, OpenAPI document, MCP server docs, GraphQL
   schema docs, or inspect the local dataset.
2. Start with one interface and the smallest useful provider document/file.
3. Define:
   - source metadata
   - `inputs` for variables and secrets
   - one or more `interfaces`
   - interface auth/env bindings
   - `test_queries` only when you expect SQL bindings
4. Lint the spec with `coral source lint <path>`.
5. Add the source:
   - `coral source add --file <path>`
   - use `--interactive` when inputs should be prompted
6. Inspect the generated shape:
   - MCP `search` for relevant exports
   - MCP `describe` for typed refs, capability ids, schemas, effect profile,
     TypeScript binding metadata, SQL projections, and diagnostics
7. Validate behavior:
   - Code Mode generated TypeScript methods for callable capabilities
   - Code Mode `coral.sql.query(...)` or CLI `coral sql` for SQL bindings
   - `coral source test <name>` for declared `test_queries`
8. Refine and repeat.

## Authoring Rules

- `spec_version` must be `1` and `kind` must be `source`.
- `name` is an install-time proposal for display/source key, not identity.
- Interface ids must match `[a-z][a-z0-9_]*`.
- Use source variables for non-secret configuration.
- Use `allowed_values` for non-secret variables that choose constrained
  provider sites, regions, tenant hosts, or other values used in credentialed
  provider URLs.
- Use source secrets for credentials. API keys, bearer tokens, OAuth tokens,
  passwords, private keys, and authorization header values must be `kind:
  secret`.
- Put interactive credential retrieval on the secret input with
  `credential.methods`. Use `type: oauth` for OAuth retrieval and
  `type: source_config` only when paste/env-token collection should remain a
  visible option.
- OAuth credential methods declare `flow`, `redirect_uri`,
  `redirect_uri_port_mode`, `endpoints`, `client`, and optional `scopes`.
  Use PKCE for public desktop clients and only declare `client.secret` when the
  provider requires confidential OAuth.
- Auth descriptors reference input keys:
  - `kind: bearer_input`
  - `kind: header_input`
  - `kind: headers`
  - `kind: none`
- Descriptor/schema fetches receive interface auth only when the acquisition URL
  is same-origin with the runtime OpenAPI `base_url` or GraphQL `endpoint`.
  Cross-origin public descriptors must not depend on provider credentials.
- Provider, descriptor, schema, and OAuth credential endpoint URLs use HTTPS.
  Localhost or loopback HTTP is only for development fixtures, and OAuth
  endpoint templates must not render to non-loopback HTTP.
- OpenAPI `url`/`base_url`, MCP Streamable HTTP `server.transport.url`,
  GraphQL `endpoint`, and GraphQL live-introspection endpoint overrides may use
  `{{input.KEY}}` URL templates. Provider URL templates must reference declared
  `kind: variable` inputs only; never use secret inputs or inline defaults in
  those URL templates. Constrain provider-host variables with
  `allowed_values`.
- Add `test_queries` only for cheap read-only SQL checks.
- Do not assume every capability gets a SQL binding. Mutations, actions,
  ambiguous output, and most upstream MCP tools are TypeScript-callable only.

## OpenAPI Sources

Use an `openapi` interface with exactly one `url` or `file` descriptor:

```yaml
interfaces:
  - id: rest
    type: openapi
    file: ./openapi.yaml
    overlays:
      - file: ./openapi-fixes.overlay.yaml
    base_url: https://api.example.com
    auth:
      kind: bearer_input
      key: API_TOKEN
```

Use `overlays` only for repeatable OpenAPI Overlay corrections to provider
descriptors. Coral applies overlay files before import and currently supports
`update` and `remove` actions with simple JSONPath targets; do not use `copy`
actions or JSONPath filters.

Read `references/http-source-checklist.md` for OpenAPI import checks.

## Upstream MCP Sources

Use an `mcp` interface for provider MCP tools:

```yaml
interfaces:
  - id: mcp
    type: mcp
    server:
      transport:
        type: streamable_http
        url: https://mcp.example.com/mcp
      auth:
        kind: bearer_input
        key: API_TOKEN
```

Use `auth.kind: headers` when a provider requires multiple custom headers for
one interface.

Stdio MCP is a trusted local-source path and must use direct command/args, not a
shell string.

## GraphQL Sources

Use a `graphql` interface with an endpoint and one schema source:

```yaml
interfaces:
  - id: graph
    type: graphql
    endpoint: https://api.example.com/graphql
    schema:
      kind: introspection_query
```

Supported schema kinds are `sdl_url`, `sdl_file`, `introspection_json_url`,
`introspection_json_file`, and `introspection_query`.

Coral generates root `Query` and `Mutation` capabilities with GraphQL variables
for field arguments and shallow scalar selections for object/connection returns.

Do not author GraphQL operation documents, selection overrides, pagination
profiles, or row-shape hints in SourceSpec.

## File Sources

Use a `file` interface with explicit files:

```yaml
interfaces:
  - id: messages
    type: file
    files:
      - /absolute/path/to/messages.jsonl
    format:
      kind: jsonl
```

Supported formats are `json`, `jsonl`, `csv`, and `parquet`.

## Validation Loop

```sh
coral source lint ./my-source.yaml
coral source add --file ./my-source.yaml
coral source test my_source
```

Then inspect generated exports through Coral MCP:

```text
search query="my_source"
describe reference="typescript:my_source.some.binding"
```

For SQL bindings, run bounded SQL:

```sh
coral sql "SELECT * FROM my_source.some_table LIMIT 20"
```

For callable bindings, run Code Mode through MCP `exec` and `wait`.
