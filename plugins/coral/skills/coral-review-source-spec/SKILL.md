---
name: coral-review-source-spec
description: Review new or updated Coral source manifests and source PRs for content, style, product fit, query ergonomics, documentation quality, and consistency with existing Coral sources. Use when Codex is asked to review a sources/core/name or sources/community/name source directory, a manifest.yaml, or a GitHub PR that adds or changes a Coral source.
---

# Coral Review Source Spec

## Review Goal

Review the source as product surface. Do not spend the review mainly restating CI, schema validation, or YAML lint results unless those failures are visible and relevant. Focus on whether the source will be understandable, useful, safe, and consistent for Coral users and agents.

## Workflow

1. Identify the target source directory or PR changes.
   - For a path request, inspect `manifest.yaml`, any README, and related source docs or tests.
   - For a PR request, find changed files under `sources/core/` or `sources/community/`. If the PR does not add or update a source, say the skill is not applicable and review normally.
2. Read guidance in the [Coral repo](https://github.com/withcoral/coral) before judging style:
   - `CONTRIBUTING.md`, especially "Source contributions".
   - The repo `AGENTS.md` and any nearer `AGENTS.md`.
   - Similar existing sources in `sources/core/` and the community example in `sources/community/hn/`.
3. Compare against existing source patterns, not a generic API-wrapper ideal. Look at nearby sources with similar shape: public no-auth APIs, token APIs, OAuth-backed APIs, GraphQL APIs, search-heavy APIs, log/time-series APIs, or generated large API sources.
4. Produce a code-review style result: findings first, ordered by severity, with file and line references. Include open questions only after findings. If there are no substantive issues, say so and mention residual risks or review gaps.

## Review Checklist

These checks should be based on the authoritative API docs for the API the source exposes.

### Scope and Fit

- Source belongs in the right tree: `sources/community/` for community sources; new `sources/core/` additions need prior discussion per `CONTRIBUTING.md`. PRs from external contributors should almost always be in `sources/community`.
- If reviewing a new source, ensure it doesn't replicate the functionality of an existing source.
- Source name is clear, stable, lowercase, and matches the SQL schema users will type.
- Scope is narrow enough to be coherent. A small source should expose the main user workflows, not every marginal endpoint.
- No real credentials, customer data, internal fixtures, or private URLs are committed.
- Updated sources bump `version` when user-visible behavior, tables, columns, inputs, or semantics change.

### Setup and Documentation

- Top-level `description` says what a user can query, not just what vendor API is wrapped.
- `inputs` distinguish secrets from variables correctly, use clear environment-style names, and include enough hints for first success. Environment-style names are prefixed with a service-specific prefix (e.g. `GITHUB_API_TOKEN`, not `API_TOKEN`.)
- Treat credential-like inputs as secrets, regardless of read-only scope or optional auth mode. Inputs named or described as `API_KEY`, `TOKEN`, `ACCESS_TOKEN`, `PASSWORD`, `SECRET`, `APPLICATION_KEY`, `READ_KEY`, `ADMIN_KEY`, private keys, bearer values, or authorization header values must be `kind: secret`; endpoint/base URL/site/region/domain/org/account/user/email values may be `kind: variable`.
- Do not make a credential a variable with an empty default to simulate optional authentication. For the current source-spec surface, require the secret or call out the missing optional-auth design explicitly; never expose a token just to support anonymous installs.
- Auth docs mention required token type, scopes or permissions, and where to get credentials.
- If a secret declares `credential.methods`, each method matches the provider's supported setup path. OAuth methods use authorization-code flow, an explicit `pkce` value, loopback redirect URI, correct endpoint URLs, appropriate client ID/default/input behavior, required `client.id.input` when the token endpoint needs client-secret authentication, correct client-secret transport when needed, and least-privilege scopes.
- OAuth methods do not replace runtime auth. The stored secret is still referenced by `auth`, request headers, query params, or body fields where the provider expects it.
- OAuth setup docs tell users whether they need their own OAuth client, which redirect URI to register, and which scopes to grant. If access tokens are short-lived, the source or docs call out that automatic token refresh is not implemented.
- When both OAuth and pasted-token setup are supported, the method ordering and labels make the preferred path obvious, usually OAuth first and `source_config` fallback second.
- Non-trivial sources include README or manifest guides with setup, schema orientation, and example queries.
- Behavior changes, setup changes, source semantics, and examples are documented in the same PR.

### Query Ergonomics

- Tables model useful user concepts: dimension tables such as users, projects, channels, services, teams, repositories, or metadata are easy starting points.
- High-cardinality or expensive endpoints require filters or have conservative `fetch_limit_default` values.
- Required filters are explicit and described in table `description` or `guide`.
- Guides tell users how to start, which IDs to join through, and any provider-specific timestamp or query syntax traps.
- Provider endpoints that accept query text and return ranked candidates use `kind: search` table functions with `search_limits`, stable result identifiers, and useful candidate metadata. Non-retrieval table functions keep the default kind for parameterized operations such as scoped child collections, time-range logs, metrics queries, or detail operations. Ordinary table filters are for exact lookup, scoping, or provider-side filtering; `mode: contains` is only substring matching. Flag provider-native search modeled as a filter and require a `kind: search` function.
- Table and column names are snake_case, stable, and obvious. Avoid leaking odd provider operation names unless the source is intentionally generated.

### HTTP and API Semantics

- `base_url` and input-derived URLs handle hosted, cloud, region, or enterprise variants without making the common case painful.
- Auth headers and request headers match provider expectations, including API version or Accept headers when needed.
- Pagination mode, cursor paths, page size limits, and result paths reflect the actual API response, not a guessed pattern.
- `ok_path`, `error_path`, `allow_404_empty`, and rate-limit hints are present when the provider's API behavior needs them.
- Required API permissions are not broader than needed for the exposed read-only surface.

### Column Design

- Columns preserve stable identifiers and include human-readable names where available.
- Opaque IDs and very large numeric IDs are usually `Utf8`; use numeric types for values users should compare or aggregate numerically.
- `Timestamp` columns are exposed for important times when Coral can reliably parse or derive them; keep raw provider timestamp fields only when useful.
- `nullable` matches provider reality. Do not mark fields non-null just because examples happened to contain them.
- Nested objects are flattened only when the fields are broadly useful; otherwise expose JSON/text columns rather than creating brittle, low-value columns.
- Column descriptions are concise and user-facing.

### Style Consistency

- YAML is readable and follows existing manifest ordering: identity, inputs/auth/base URL, test queries, functions/tables.
- Existing core sources use short table descriptions plus `guide` blocks for usage advice; prefer that split.
- README structure, when present, should resemble existing source READMEs: authentication, rate limits when relevant, table categories or schema overview, and examples.
- Wording should be clear to a new user. Avoid internal Coral implementation terms unless they are part of the user-facing source-spec surface.

## Output Shape

Lead with concrete findings. Use severity labels only when helpful, but do not bury issues under a summary. Prefer:

```text
Findings
- High: `sources/community/foo/manifest.yaml:42` marks `created_at` non-null, but the provider omits it for imported records...
- Medium: `sources/community/foo/README.md:18` shows setup but never states the required token scope...

Open questions
- Is endpoint X intentionally omitted from the first version?

Review notes
- I treated CI lint/schema checks as out of scope unless visible in the diff.
```

If no issues are found, say that directly and include any limits, such as not having live credentials or not inspecting CI logs.

When highlighting a discrepancy between the source and the API documentation, always include links to the exact API documentation page.
