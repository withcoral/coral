# Coral source model — design spec

A capability-centric model for Coral sources, sitting between provider specs (OpenAPI, GraphQL) and agent-facing surfaces (SQL, tools, codemode).

## Pre-read

James’s [Manifest-o](https://www.notion.so/phoebeai/Manifest-o-350e4c1e4f5380a7bea2f4e536940c43) describes the problems with our current source spec manifests that this spec is attempting to solve.

## Goals

- Bootstrap sources from standards (OpenAPI, GraphQL) instead of hand-authoring.
- Keep provider truth, Coral's interpretation, and the agent-facing surface as separate concerns rather than collapsing them into one manifest.
- Support multiple specs per provider (e.g. REST v3 + REST v4 + GraphQL).
- Project the same internal model into multiple agent surfaces.
- Make every Coral decision traceable back to its source evidence.

## Out of scope

- Webhooks and other server-initiated events. The model below covers reads, writes, and actions; async event delivery (push, fan-out, dead-lettering, signature verification) is a separate concern.

## Layered model

```
┌─────────────────────────────────────────────────────────────┐
│ Source: github                                              │
│                                                             │
│   Specs (snapshotted): rest_v3, rest_v4_preview, graphql    │
│                                                             │
│   Capabilities ──────────────► Entities                     │
│   ├─ inputs/output                ├─ fields                 │
│   ├─ kind, side_effects           ├─ aliases                │
│   ├─ idempotency                  ├─ related                │
│   ├─ aliases, provenance          └─ primitive_mappings ─┐  │
│   └─ bindings ──► (refer up to specs)                    │  │
│                                                          │  │
└──────────────────────────────────────────────────────────┼──┘
              │                                            │
              ▼                                            ▼
       Projections                                  Type registry
       (SQL · tools · codemode)                     (global, additive)
```

The source is the unit. Specs sit inside it as snapshotted inputs. Capabilities and entities are siblings, not nested. Bindings live inside capabilities and reference specs. The two outbound edges from the model are projections (consume capabilities) and the type registry (referenced by entities for cross-provider correlation).

## Source

Top-level container for one provider. Carries metadata, the snapshotted specs it was compiled from, and the lifecycle policy.

```yaml
source:
  id: github
  namespace: github
  specs:
    - id: rest_v3
      kind: openapi
      url: …/api.github.com.yaml
      version_hash: sha256:8f3a…
      status: stable
      preferred_for: [reads, writes]
    - id: rest_v4_preview
      kind: openapi
      url: …/api.github.com.v4-preview.yaml
      version_hash: sha256:c12d…
      status: incomplete
      preferred_for: []
    - id: graphql
      kind: graphql
      url: …/schema.graphql
      version_hash: sha256:a771…
      status: stable
      preferred_for: [field_selection, graph_traversal]
  lifecycle:
    strategy: replace_not_migrate
  rate_limits:
    classes:
      core: { limit: 5000, window: 1h, applies_to: [rest_v3, rest_v4_preview] }
      search: { limit: 30, window: 1m, applies_to: [rest_v3] }
      graphql:
        { limit: 5000, window: 1h, currency: points, applies_to: [graphql] }
```

### Lifecycle

`replace_not_migrate` — when the upstream spec changes, recompile from scratch. No version migration logic. The snapshot's `version_hash` makes the input reproducible.

### Multiple specs per provider

A source can carry multiple specs simultaneously. Common cases: REST + GraphQL, stable v1 + incomplete v2. Specs are not capabilities themselves — they're inputs to the compiler. Capability-to-spec mapping happens through bindings.

TODO: What happens if the specs fundamentally disagree on the share of a resource, or the interface to a capability? Which one wins? How do we reconcile that in the capability?

## Capability

A logical operation the provider can perform, described independently of which spec implements it.

### Kinds

- **Reads:** `get`, `list`, `search`, `get_related`, `aggregate`, `blob`
- **Writes:** `create`, `update`, `replace`, `delete`, `batch_write`
- **Actions:** `trigger`, `transition`, `invoke`
- **Meta:** `introspect`, `auth_probe`, `rate_limit_probe`

### Properties

```yaml
- id: github.pull_request.list
  kind: list
  entity: github.pull_request
  inputs:
    required:
      owner: { type: string, origin: path_param }
      repo: { type: string, origin: path_param }
    optional:
      state: { type: enum<open|closed|all>, default: open }
      base: { type: string }
      head: { type: string }
  output:
    shape: list<github.pull_request>
  side_effects: read_only
  idempotency: safe
  auth:
    schemes: [oauth, pat, github_app]
    scopes_any_of: [repo, public_repo]
  support: supported
  aliases:
    - { name: list_prs, source: heuristic, confidence: 0.9 }
    - { name: list_pull_requests, source: openapi_tag, confidence: 1.0 }
  projection_hints:
    sql: table_function
    tool: list_github_pull_requests
  provenance:
    source: openapi://rest_v3#/paths/~1repos~1{owner}~1{repo}~1pulls/get
  bindings:
    - …
```

### What lives at the capability level

Properties that must hold a coherent contract across all bindings.

- `kind`, `entity`, `inputs`, `output`
- `side_effects` (read_only, safe_write, destructive_external, external_trigger)
- `idempotency` (safe, requires_idempotency_key, not_safe) — same operation invoked twice has the same real-world effect regardless of which binding the runtime selected
- `auth` requirements (logical scopes; binding may translate to spec-specific scheme)
- `support` (supported, unsupported_in_runtime, deprecated, blocked) with `support_reason`
- `policy_anchors` (risk_level, confirmation, audit) — hooks for the policy engine, not policy itself
- `aliases` — multiple names with provenance and confidence (see "Aliases")
- `projection_hints` — descriptive hints to projections, not prescriptive decisions
- `provenance` — primary source the capability was derived from

### What does NOT live at the capability level

- **Pagination** — pagination is wire-shaped. Cursors minted by one binding are meaningless to another, so there's no cross-binding contract worth preserving. Lives entirely in the binding.
- **Wire-format details** — request/response shape, field naming, transport mechanics.

## Binding

How a capability is implemented against a specific spec. A capability has zero or more bindings; each binding references exactly one spec.

```yaml
bindings:
  - id: rest_v3.list_pulls
    spec: rest_v3
    operation: "GET /repos/{owner}/{repo}/pulls"
    input_mapping:
      owner: path_param.owner
      repo: path_param.repo
      state: query.state
    output_mapping: body
    pagination:
      mechanism: link_header
      page_size_param: per_page
      max_page_size: 100
      features: [forward, backward]
    rate_limit_class: core
    capabilities_extra: []
    support: supported
    provenance:
      source: openapi://rest_v3#/paths/~1repos~1{owner}~1{repo}~1pulls/get

  - id: graphql.repository_pull_requests
    spec: graphql
    operation_template: |
      query($owner:String!,$repo:String!,$state:[PullRequestState!],$first:Int!,$after:String) {
        repository(owner:$owner,name:$repo) {
          pullRequests(states:$state, first:$first, after:$after) {
            nodes { …pullRequestFields }
            pageInfo { endCursor hasNextPage }
          }
        }
      }
    input_mapping: { owner: var.owner, repo: var.repo, state: var.state }
    output_mapping: data.repository.pullRequests.nodes
    pagination:
      mechanism: relay_cursor
      page_size_param: first
      cursor_param: after
      max_page_size: 100
      features: [forward]
    rate_limit_class: graphql
    capabilities_extra: [field_selection, fragment_composition]
    support: supported
```

### What lives at the binding level

- Spec reference and operation pointer
- Input/output mapping (path params, query params, body, GraphQL variables)
- Pagination mechanism, page-size params, max page size, features
- Rate-limit class
- Binding-specific capabilities (`capabilities_extra`) — e.g. `field_selection` for GraphQL
- `support` (a binding can be unsupported even when the capability is supported)
- Per-binding provenance

### Capability gaps are visible

A capability can have bindings missing for some specs. That's data, not absence: `github.repo.transfer` having only a `rest_v3` binding tells you the capability exists but isn't reachable for v4-only consumers. The build step produces a coverage matrix (capability × spec) and flags gaps.

### GraphQL escape hatch

In addition to structured capabilities with GraphQL bindings, there's a raw passthrough capability for agent-composed queries:

```yaml
- id: github.graphql.query
  kind: invoke
  inputs:
    required: { query: string }
    optional: { variables: map<string, any> }
  output: shape<dynamic>
  bindings:
    - {
        id: graphql.raw,
        spec: graphql,
        operation: "POST /graphql",
        passthrough: true,
      }
```

## Entity

A type the provider operates on. Entities are referenced by capability inputs and outputs.

```yaml
entity:
  id: github.pull_request
  aliases: [pull, pr]
  fields:
    id: { type: integer, role: id }
    number: { type: integer, role: display_id }
    title: { type: string }
    state: { type: enum<open|closed|merged> }
    author: { type: ref<github.user> }
    base_ref: { type: string }
    head_sha: { type: string }
    created_at: { type: timestamp }
    merged_at: { type: timestamp, nullable: true }
  primitive_mappings:
    created_at: registry.timestamp
    head_sha: registry.git_sha
  provenance: { source: openapi://components/schemas/pull-request }

related:
  - { entity: github.review, relation: many, via: pull_number }
  - { entity: github.review_comment, relation: many, via: pull_number }
```

`related` makes the entity graph explicit so projections can offer progressive discovery (point #7) and SQL projections can derive joins. `primitive_mappings` is the only point where entities touch the global type registry.

## Aliases

Both capabilities and entities carry alias lists. The model deliberately rejects naming canonicalisation — provider names are first-class, never debated.

```yaml
aliases:
  - { name: list_prs, source: heuristic, confidence: 0.9 }
  - { name: list_pull_requests, source: openapi_tag, confidence: 1.0 }
  - {
      name: list_merge_requests,
      source: cross_provider_alias,
      confidence: 0.5,
      scope: github.*,
    }
```

Each alias carries:

- `source` — where it came from (openapi_tag, heuristic, agent_usage, hand_authored, cross_provider_alias)
- `confidence` — for ranking and conflict resolution
- `scope` — when the alias should match (e.g. only inside `github.*`)

TODO: Help me understand `scope` here a bit better. I can see the example is motivated by mapping Gitlab terminology (“merge requests”) to Github (“pull requests”). But what is the `scope` field achieving?

Aliases grow over time. New ones can be learned from agent usage; low-confidence ones can be demoted or removed when they cause collisions.

## Type registry

Small, slow-moving, additive registry of cross-provider primitives.

- Email, URL, timestamp, namespaced user ID, money, etc.
- Only types that genuinely need cross-provider correlation
- Names are normalised here, unlike everywhere else
- Entities reference registry types via `primitive_mappings`

## Provenance

Every capability, every entity, every alias, every binding carries a `source` field pointing to the spec section it was derived from, plus optional `evidence` describing the reasoning.

```yaml
pagination:
  mechanism: link_header
  evidence:
    - "RFC 5988 Link header in response"
    - "per_page query param present"
```

Provenance is not a sidecar. It lives next to the data it describes. The moment provenance is stored separately, it stops being maintained. This is what answers point #4 (recovering the reason behind decisions).

## Support model

Every capability and binding declares `support`:

- `supported` — usable
- `unsupported_in_runtime` — capability exists in provider, Coral can't execute it yet
- `deprecated` — provider has marked deprecated
- `blocked` — policy or governance prevents use

Plus `support_reason` — a string explaining why. Unsupported things stay visible in the model (so agents and humans can see they exist) but are not materialised into projections.

## Projection hints, not decisions

Capabilities and bindings can express preferences:

```yaml
projection_hints:
  sql: table_function # owner/repo are required path params
  tool: list_github_pull_requests
```

These are advisory. The projection layer is free to override based on its own rules, the agent's request, or org policy. The capability stays descriptive; projection stays prescriptive.

## Policy anchors, not policy

Destructive capabilities expose hooks for the policy engine:

```yaml
policy_anchors:
  risk_level: high
  confirmation: recommended
  audit: required
```

The capability says "this is destructive and high-risk." The deployment decides "and therefore requires two approvers in production." Source files stay portable across orgs with different policy regimes.

## File layout

For small providers, one file per source. For large providers (GitHub, Salesforce), one file per entity, with cross-cutting capabilities in `_*.yaml` files.

```
sources/github/
  source.yaml                 # metadata, specs, lifecycle, rate limits
  types/
    primitives.yaml
  entities/
    repository.yaml           # entity + capabilities owned by repo
    pull_request.yaml
    issue.yaml
    review.yaml
    user.yaml
    workflow.yaml
    workflow_run.yaml
    _search.yaml              # cross-entity: code/issue/repo search
    _graphql.yaml             # GraphQL passthrough
  .compiled/                  # build artifact, runtime-loaded
    capabilities.json
    types.json
    indexes.json
```

Capabilities live with their primary entity (the thing they produce or principally affect), not their input entity. `list reviews for a PR` lives in `review.yaml`, not `pull_request.yaml`.

The `.compiled/` directory is the runtime contract. Humans never edit it. The on-disk layout is purely an authoring affordance — the runtime sees a flat, indexed, denormalised artefact.

## Build & lint

Per-entity authoring requires real tooling to prevent drift. The linter must enforce, at minimum:

1. All `ref<X>` and `entity:` references resolve to a single definition in the namespace.
2. All capability IDs are unique and follow `<namespace>.<entity>.<verb>` (or `<namespace>._<area>.<verb>` for cross-cutting).
3. Every capability declares `support`, `side_effects`, `idempotency`, and `provenance`.
4. `kind: list/search/get_related` capabilities have at least one binding with a pagination mechanism declared. `kind: get` capabilities have none.
5. Idempotency is consistent across bindings of the same capability.
6. `related` edges are bidirectional or explicitly marked one-way.
7. Provenance source pointers actually exist in the snapshotted spec.
8. No two aliases collide within a namespace at high confidence.
9. Coverage report: capability × spec matrix showing where bindings are missing.

## Open questions

These are deferred — design decisions worth pinning down with real use cases before committing.

- **GraphQL fusion** — exposing enough structure for the runtime to fuse multiple capabilities into one GraphQL round trip. Probably requires bindings to declare fragments and root paths rather than full queries. Defer until a use case demands it.
- **Deprecation lifecycle granularity** — binding-level vs capability-level vs spec-level. Current instinct: binding-level for normal deprecation, spec-level for whole-API sunsets.
- **Partial-failure shape for batch writes** — "3 of 5 succeeded" representation that fits both REST and GraphQL bindings. Defer until a real batch capability exists.
- **Variants vs separate capabilities** — when does "list issues for org" vs "list issues for repo" become two capabilities vs one with parameter variants? Current instinct: separate when the required scoping differs materially.
- **Resource-hierarchy carrying** — does the capability model carry the provider's nesting tree, or stay flat with foreign-key-style references via `related`? Currently flat-with-refs.
- **Runtime binding selection logic** — how the runtime picks which binding to invoke when multiple are valid. Inputs include: agent-requested features (e.g. field selection → GraphQL), spec-level `preferred_for`, support flags, rate-limit headroom, org policy. Worth a full design pass.
