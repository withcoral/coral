---
name: coral-review-source-spec
description: Review new or updated Coral SourceSpec manifests and source PRs for correctness, safety, product fit, generated export quality, documentation quality, and consistency with Coral's capability-first source model.
---

# Coral Review Source Spec

## Review Goal

Review the source as product surface. Focus on whether the SourceSpec gives
Coral enough truthful provider facts to generate useful capabilities, TypeScript
bindings, SQL bindings when appropriate, and diagnostics.

Do not review it as a hand-authored table manifest. Tables/functions/columns are
generated downstream from provider snapshots and capabilities.

## Workflow

1. Identify the target source spec or PR changes.
2. Inspect the YAML, source docs, provider docs, and relevant generated
   artifacts when present.
3. Compare against the active SourceSpec v1 contract: `spec_version: 1`,
   `kind: source`, declared inputs, and `openapi`, `mcp`, `graphql`, or `file`
   interfaces.
4. Produce a code-review style result: findings first, ordered by severity, with
   file and line references. Include open questions only after findings.

## Review Checklist

### Scope And Fit

- Source belongs in the right tree for the change being reviewed.
- The spec does not duplicate an existing source without a clear reason.
- Source `name` is clear, stable, lowercase, and suitable as an initial
  source-key seed.
- No real credentials, customer data, private URLs, or internal fixtures are
  committed.
- The source uses SourceSpec v1. Removed source-contract keys are blockers.

### Inputs And Credentials

- Credentials are `kind: secret`, never variables. This includes API keys,
  bearer tokens, OAuth access tokens, passwords, private keys, and
  authorization header values.
- Non-secret configuration such as base URLs, tenant ids, regions, account ids,
  and organization slugs may be `kind: variable`.
- Input hints tell users what value is expected, where to get it, and the
  minimum scopes/permissions needed.
- Secret inputs that offer interactive credential retrieval declare
  `credential.methods`; OAuth methods include the flow, redirect URI/port mode,
  provider endpoints, client id/secret metadata, and scopes needed by the
  provider.
- Auth descriptors reference declared inputs and place values correctly:
  `bearer_input`, `header_input`, or `none`.
- Descriptor/schema URLs rely on interface auth only when same-origin with the
  runtime OpenAPI `base_url` or GraphQL `endpoint`; cross-origin descriptors
  must be public, anonymous, or local files.
- OAuth credential endpoint URLs use HTTPS except localhost or loopback HTTP
  development fixtures; endpoint templates must not render to non-loopback HTTP.
- Secret values do not appear in examples, generated artifacts, README text,
  diagnostics, logs, or committed fixtures.

### Interface Semantics

- Interface ids match `[a-z][a-z0-9_]*` and are stable because they participate
  in capability ids.
- OpenAPI interfaces declare exactly one `url` or `file`, preserve provider
  auth/server expectations, and point at an authoritative provider document.
- OpenAPI descriptor `url` values hosted away from the runtime `base_url` do
  not require provider credentials to fetch.
- MCP interfaces clearly distinguish upstream provider MCP ingestion from
  Coral's own MCP server. Stdio commands are trusted local configuration and are
  direct command/args, not shell snippets.
- GraphQL interfaces declare an endpoint plus one schema source. They do not
  smuggle authored operations, selection overrides, pagination profiles, or row
  shape hints into SourceSpec.
- GraphQL schema URLs hosted away from the runtime `endpoint` do not require
  provider credentials to fetch.
- File interfaces list explicit files and a supported format. They do not grant
  broad directory traversal or hide arbitrary path expansion in templates.

### Capability And Export Quality

- Supported provider operations should become capabilities even when they are
  not SQL-projectable.
- Mutations/actions are not hidden merely because SQL cannot model them.
- SQL bindings should exist only for read-like, row-shaped capabilities.
- TypeScript binding metadata and search/describe text should make the useful
  operations discoverable without turning aliases into identity.
- Unsupported provider shapes should surface diagnostics rather than silently
  disappearing.
- `test_queries` are present only when SQL bindings are expected, and they are
  cheap, read-only, bounded checks.

### Provider-Specific Checks

- OpenAPI import preserves operation ids, parameter locations/serialization,
  request media types, response variants, security inheritance, and source
  pointers needed for invocation.
- MCP import maps `ToolAnnotations` with MCP defaults, preserves
  `tools.listChanged` as snapshot evidence, and does not infer effects from
  tool names or descriptions.
- GraphQL import generates bounded operations for supported root `Query` and
  `Mutation` fields; subscriptions and unsupported shapes become diagnostics.
- File import keeps file refs scoped to installed-source metadata and does not
  leak arbitrary absolute paths through describe/MCP/Code Mode.

### Documentation

- Description says what users can do with the source, not just which protocol
  it uses.
- Setup docs explain required credentials/scopes and any provider-specific
  prerequisites.
- Examples use `search`/`describe`, generated TypeScript bindings, or SQL only
  when SQL bindings are expected.
- Behavior changes, setup changes, source semantics, and examples are updated
  in the same PR.

## Output Shape

Lead with concrete findings:

```text
Findings
- High: `sources/community/foo/manifest.yaml:12` declares `API_TOKEN` as a variable, but it is sent as an Authorization header...
- Medium: `sources/community/foo/manifest.yaml:31` uses an MCP stdio shell wrapper; SourceSpec stdio must be direct command/args...

Open questions
- Is endpoint X intentionally omitted from the first version?

Review notes
- I treated generated SQL absence as acceptable for non-read capabilities.
```

If no issues are found, say that directly and include limits such as not having
live credentials or not inspecting generated artifacts.

When citing provider behavior, link to the exact provider documentation page.
