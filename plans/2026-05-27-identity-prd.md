# Coral Identity PRD

## Summary

Coral needs an identity model that is orthogonal to source specs. Sources define
what can be queried. Identities define who or what Coral can act as. They meet
inside a workspace, where each source surface that needs authority is bound to an
identity.

An identity has an owner:

- **Workspace-owned** identities are shared by the whole workspace — every member
  queries as the same identity. They fit org-tied authority: bots, service
  accounts, trusted roles, OIDC.
- **Member-owned** identities are user-scoped and private to one member. A
  workspace declares a per-member slot, and each member binds their own identity
  into it, so each member queries as themselves. They fit user-tied authority:
  OAuth for Gmail or personal GitHub.

A workspace is the shared curation and membership context: a tailored set of
sources, projections, shared identity assignments, and open per-member slots.

The model is:

```text
Source spec        -> materialized source
Identity spec      -> identity (workspace-owned or member-owned)
Workspace source   -> a workspace's use of a materialized source
Shared assignment  -> workspace source surface uses one workspace identity
Per-member slot    -> workspace source surface needs each member's own identity
Binding            -> a member fills a slot with one of their own identities
Availability       -> who may query the workspace source
```

There is no global `Connection` object, no source-owned identity, no global-pool
identity selection at query time, and no first-wave per-identity ACL. Query-time
identity resolution still happens, but only as a deterministic lookup of the
surface binding for the current member — never as a policy that chooses among
candidate identities. Those concepts either duplicate the surface binding or
create a second permission model.

Users should experience this as a simple authentication UX: source, connected
as, available to, status, permissions needed, affected sources, and fix. Users
should not manage credential material, injection methods, or source IR during
ordinary setup and recovery.

## Goals

- Define identities as materialized objects owned by a workspace (shared) or a
  member (user-scoped).
- Define identity specs as the identity-side equivalent of source specs.
- Add `identity_requirements` to DSL v4 surfaces.
- Bind identities through shared workspace assignments or per-member slots.
- Let one workspace serve both org-tied (shared) and user-tied (per-member)
  authority without changing access control.
- Support private user workspaces, shared workspaces, and local CLI defaults.
- Allow the same source surface to use different identities in different
  workspaces.
- Allow one source to have multiple surfaces, each with its own identity
  requirements.
- Keep matching provider-native: issuer, capabilities, injection method, and
  audience.
- Keep authority changes explicit, especially sharing, reuse, broader
  capabilities, principal changes, and audience changes.
- Support multi-user workspace sharing through workspace-owned identities,
  per-workspace-source availability, and the three-permission model. The user
  authentication layer that identifies which member is querying is a separate
  dependency, not defined here.

## Non-Goals

- Redesign DSL v4 source IR, projections, OpenAPI import, or source
  materialization internals.
- Make source specs own identity materialization.
- Select among multiple candidate identities at query time. Resolution is a
  deterministic assignment lookup, not a policy.
- Add cross-workspace reuse of workspace-owned (shared) identities. Member-owned
  identities are the deliberate exception: they are user-scoped and reusable
  across that member's own workspace slots.
- Add a first-wave per-identity ACL model.
- Normalize provider permissions into a universal Coral taxonomy.
- Define every provider-specific identity spec in this PRD.
- Define the user authentication / principal layer that identifies which member
  is querying. This PRD assumes that layer and composes with it, but a later
  effort builds it.
- Apply to or migrate DSL v3 sources. This model is DSL v4-only; v3 sources keep
  their existing source-scoped credentials.
- Specify per-member query attribution, audit logging, or at-rest credential
  encryption for shared identities. Storage will evolve to cover these
  separately.

## Core Model

### Source Spec

A source spec defines how Coral materializes a queryable source. In DSL v4, it
declares surfaces and projections. Source specs do not choose identities.

### Materialized Source

A materialized source is the result of running a source spec. It contains the
source model, surfaces, and projections that workspaces can use. It is not owned
by one workspace.

### Surface

A surface is a provider interface declared by a source spec, such as GitHub
REST, GitHub GraphQL, Slack Web API, or an AWS service API family. A surface
declares identity requirements. It does not choose a concrete identity.

### Identity Spec

An identity spec defines how Coral materializes provider-facing authority.
Examples:

- `github-oauth`
- `slack-bot-oauth`
- `aws-profile`
- `aws-trusted-role`
- `aws-oidc-trust`
- `manual-bearer-token`

An identity spec owns setup, principal discovery, audience discovery,
capability request or validation, runtime injection method, refresh, recovery,
and supported non-interactive setup.

### Identity

An identity is a materialized authority created by running an identity spec. It
has an owner:

- A **workspace-owned identity** is created in and owned by a workspace. It is
  shared: any member querying through a surface assigned to it acts as that one
  identity. It is strictly workspace-local and never reused by another workspace.
- A **member-owned identity** is created and owned by a member and lives in that
  member's user-scoped identity store. It is private to the member and reusable
  across any of that member's workspace slots that it satisfies. No other member
  can see, pick, or use it.

Examples:

- GitHub OAuth identity for `saul@work` (member-owned)
- Slack bot identity for an Engineering Slack workspace (workspace-owned)
- AWS STS identity for `arn:aws:sts::123456789012:assumed-role/ReadOnly/saul`
- Google OAuth identity for a Google Workspace user (member-owned)

An identity records non-secret metadata:

- identity spec
- owner: a workspace, or a member
- issuer or authority service
- connected principal
- audience
- capabilities
- supported injection method
- health and recovery state
- credential material reference, when credential material exists

Credential material is an implementation detail. Users should not manage it
directly during normal use.

In single-user local mode the member is the local user, so member-owned
identities are simply that user's connected accounts, reusable across all their
local workspaces. Multi-user mode adds more members on the same model; the
member-identifying authentication layer is a separate dependency (see Non-Goals).

### Identity Requirements

Identity requirements are declared by source surfaces. They describe what shape
of identity can be assigned to that surface:

- **Issuer / authority service:** who minted or vouches for the identity, such
  as GitHub, Google, Slack, AWS, or Coral OIDC.
- **Capabilities:** provider-native permissions required by the surface, such
  as OAuth scopes, app permissions, IAM actions, or product permissions.
- **Injection method:** how Coral adds the identity to provider requests, such
  as bearer token, API key header, basic auth, or AWS SigV4. Injection method is
  an identity property, not a source property: one provider is often split across
  several sources, so the identity — not the source — determines how its
  credential is injected.
- **Audience constraints:** where the identity must be valid, such as GitHub
  host, Slack workspace, AWS account, AWS partition, region set, Datadog site,
  Google Workspace domain, or provider base URL.

### Workspace

A workspace is the shared curation and membership context. It is a tailored set
of sources and projections for a use case, plus the members who can use it. It
owns its workspace sources, its workspace-owned (shared) identities, the shared
assignments and per-member slots on their surfaces, and availability.

A workspace does not own member identities. Those live in each member's
user-scoped store; the workspace only holds the slot declaration and a per-member
binding reference. Deleting a workspace never deletes a member's identities, and a
member leaving only drops that member's bindings.

This expands the current Coral notion of a workspace — today an isolated local
source collection — into a multi-user sharing context with members. The layer
that authenticates members and tells Coral which member is querying is a separate
dependency this PRD assumes but does not define.

### Workspace Source

A workspace source is a workspace's use of a materialized source. It owns:

- availability
- per-surface binding: a shared assignment or a per-member slot
- readiness status, which for slots is evaluated per member

### Surface Binding: Shared Assignment or Per-Member Slot

Each surface of a workspace source is bound one of two ways. The binding mode is
a property of the surface in that workspace, set by whoever has Manage workspace
source, and is often inferred from the issuer or archetype (human-tied issuers
default to a slot; app/bot/service-account/trusted-role/OIDC default to shared).

A **shared assignment** binds the surface to one workspace-owned identity:

```text
workspace source + surface -> workspace identity
```

Every member queries as that identity.

A **per-member slot** declares that the surface uses each member's own identity:

```text
workspace source + surface -> per-member slot (requirements only)
```

The slot carries the requirements but no concrete identity. Each member fills it
with a **binding**:

```text
(workspace source + surface, member) -> that member's member-owned identity
```

A shared assignment is valid only when:

- the identity is workspace-owned by the same workspace as the workspace source
- the identity issuer matches an accepted issuer
- the identity capabilities satisfy the surface requirements
- the identity supports the required injection method
- the identity audience satisfies the surface audience constraints

A per-member binding is valid under the same issuer, capability, injection, and
audience rules, except the identity must be member-owned by the binding member.

Bindings and assignments, not identities, determine where an identity is used. A
slot with no binding for a member is not an error; it is that member's "needs
action" until they bind.

### Availability

Availability describes who may query a workspace source. It belongs to the
workspace source, not to the source spec or identity.

Authorization and identity are separate questions. Availability decides who may
query; the surface binding decides whose credential runs the query. A shared
assignment runs every member's query as the workspace identity; a per-member slot
runs each member's query as their own bound identity. Binding mode adds nobody to
the availability set and creates no per-identity ACL.

## Capability Matching

Capabilities are provider-native facts, not Coral-invented generic
permissions. A universal taxonomy would look tidy and be wrong, because
providers do not expose equivalent semantics.

Matching rules:

- Required capabilities are matched as provider-native facts.
- An identity satisfies a requirement only when Coral can prove the identity has
  that capability or the provider's auth model makes it inherent.
- Unknown, unvalidated, or ambiguous capabilities do not satisfy requirements.
- Capabilities for manually supplied credentials come from one of two paths: the
  user declares them explicitly at setup, or the identity spec extracts and
  validates them from the credential when its format allows it, such as parsing
  scopes or claims from a JWT. User-declared capabilities carry asserted-by-user
  provenance, not provider-proven provenance.
- A stronger capability satisfies a weaker requirement only when the identity
  spec explicitly models that provider-specific implication.
- Requesting additional capabilities is an authority-broadening action and
  requires explicit confirmation.

Specs may still include user-facing labels:

```yaml
capabilities:
  - id: repo:read
    kind: github_app_permission
    label: Repository read access
```

Compatibility uses `id` and provider semantics. UX uses `label`.

## DSL v4 Compatibility

This PRD is authoritative for identity and credentials. DSL v4 did not model
identity; its surface-level `auth:` block and source-scoped credential `inputs`
are carryover from v3. For v4 sources, those are superseded by identity binding:
a surface declares `identity_requirements`, and the bound identity — workspace-
owned or member-owned — owns credential material and injection. DSL v3 sources
are unaffected and keep their existing source-scoped credentials.

DSL v4 keeps source specs focused on source materialization. Identity adds one
surface-level contract: `identity_requirements`.

Proposed first-wave shape:

```yaml
surfaces:
  - id: github-rest
    type: open-api
    url: https://example.com/github-openapi.yaml
    sha256: ...
    base_url: https://api.github.com
    identity_requirements:
      accepts:
        - id: github-rest-read
          issuer: github
          injection_method: bearer_authorization_header
          audience:
            host: github.com
          capabilities:
            - id: repo:read
              kind: github_permission
              label: Repository read access
            - id: org:read
              kind: github_permission
              label: Organization read access
```

Contract:

- `accepts` has OR semantics.
- Capabilities inside one accepted shape have AND semantics.
- Matching dimensions are issuer, audience, capabilities, and injection method.
- A shared assignment binds a compatible workspace-owned identity; a per-member
  slot is filled by each member's compatible member-owned identity. The surface's
  `identity_requirements` are identical either way — only the binding's owner
  differs.

DSL v4 projections remain SQL exposure choices. They do not choose identities.

## Examples

### Personal Data Source in One Shared Workspace

A per-member slot lets one shared workspace serve user-tied data without sharing
anyone's credential.

```text
Materialized source: gmail
Surface: gmail-api

Workspace: mail (shared; members: Saul, Andrea)
Workspace source: gmail
gmail.gmail-api -> per-member slot (issuer: google)
Available to: Saul, Andrea

Bindings (member-owned, user-scoped):
  Saul   -> google-saul
  Andrea -> google-andrea
```

The workspace, its curated source, and any projections are shared. The Gmail
surface is a per-member slot, so each member binds their own Google identity.
Saul's query resolves to `google-saul` and Andrea's to `google-andrea` — each
sees only their own mail — through a deterministic per-member lookup, not a global
resolver. Neither member can see or use the other's identity. (Earlier drafts
modeled this as two separate private workspaces; the per-member slot removes that
need and keeps the curation shareable.)

### Same Source, Different Workspace Identity

```text
Materialized source: github
Surface: github-rest

Workspace: saul-private
github.github-rest -> github-saul-work        (member-owned)

Workspace: eng-shared
github.github-rest -> github-coral-app-acme   (workspace-owned, shared)
```

The source and surface are the same. The binding — its owner and identity —
differs per workspace.

### One Source, Multiple Surfaces

```text
Workspace: eng-shared
Workspace source: github

github.github-rest    -> github-coral-app-acme       (shared assignment)
github.github-graphql -> per-member slot (issuer: github)
                           Saul  -> github-graphql-saul
                           Priya -> github-graphql-priya
```

Each surface declares its own identity requirements, and the workspace binds each
independently: a shared workspace-owned app for REST, and a per-member slot for
GraphQL so each member queries as their own GitHub identity. Mixed binding within
one workspace source is exactly what lets a shared workspace serve org-tied and
user-tied authority at once.

## Materialization and Setup

Source and identity materialization are separate flows.

Source materialization follows DSL v4. Running a source spec produces or
refreshes a materialized source with surfaces and projections. It does not
create identities or decide which workspace will use the source.

Identity materialization runs an identity spec to create or refresh one identity
— workspace-owned (into the workspace) or member-owned (into the acting member's
user-scoped store) — and returns enough non-secret metadata for UX: connected
label, audience, capabilities, status, and recovery action.

Adding a source to a workspace composes the two:

1. Ensure the materialized source exists.
2. Create or select the workspace source.
3. Read identity requirements for each required surface.
4. Decide each surface's binding mode: shared assignment or per-member slot
   (default inferred from issuer/archetype, overridable by a workspace manager).
5. For shared surfaces, find or materialize a compatible workspace-owned identity
   and assign it; suggest safe reuse only when allowed.
6. For per-member surfaces, declare the slot. The acting member binds one of
   their compatible member-owned identities, or materializes a new one.
7. Validate and report ready, partially ready, or blocked. Slot readiness is per
   member: a workspace can be ready for one member and need action for another
   who has not bound yet.

Setup should use the smallest safe user-visible decision. Compact setup is fine
when there is one obvious private identity path. Reuse, shared access, broader
capabilities, principal changes, audience changes, or availability changes must
show impact before confirmation.

### Non-Interactive Setup

Non-interactive setup is required for tests, benchmarks, CI, MCP-driven flows,
and sandboxed agent environments.

Identity specs may support non-interactive materialization when the provider
allows it safely, such as selecting an existing AWS profile, using configured
cloud trust, or reading a supplied secret reference. Three-legged OAuth may
still require human authorization.

When non-interactive setup cannot proceed safely, Coral should return a
structured, non-secret fix instead of launching a browser or prompting from a
background context:

```text
GitHub identity required.

Workspace source: github
Surface: github-rest
Required access: repo read, org read
Fix: run interactive identity setup for GitHub, then retry.
```

## Query Execution and Recovery

At query time, Coral does not choose identities from a global pool. It resolves
the surface binding for the current member:

1. Resolve the current workspace and the querying member.
2. Resolve the workspace source for the queried source namespace.
3. Resolve the surface needed by the table or function.
4. Load the bound identity: for a shared assignment, the workspace identity; for
   a per-member slot, the member's binding for that slot.
5. Confirm the identity still satisfies the surface requirements.
6. Inject the identity using the declared injection method.
7. Execute the query.

If no binding exists — no shared assignment, or no per-member binding for this
member — the query is blocked with setup guidance for that member. If the bound
identity is unhealthy, Coral follows the identity spec recovery rules. If it
lacks a required capability, Coral reports an authorization failure and does not
silently request broader access.

Recovery messages stay non-secret, and their scope follows the identity's owner.
A workspace-owned identity affects only its workspace, so its message is
workspace-local. A member-owned identity can be bound in several of the member's
workspaces, so its message is user-scoped and lists every affected workspace:

```text
GitHub access expired.

Connected as: saul@work (your identity)
Affected workspaces and sources:
  mail-eng -> github
  oncall   -> github
Fix: refresh the GitHub identity once; all bindings recover.
```

Authorization failures should distinguish access from source health:

```text
Slack denied access to slack.messages.

Workspace source: slack
Surface: slack-web-api
Connected as: Coral Slack App
Reason: message history access has not been granted.
Fix: grant message history access or assign an identity with that capability.
```

## UX Contract

Normal users should see:

- **Source:** what they query.
- **Connected as:** the bound identity in the current workspace — the shared
  workspace identity, or, for a per-member slot, your own bound identity.
- **Available to:** who may query the workspace source.
- **Status:** ready, partially ready, blocked, or needs action. For a per-member
  slot this is evaluated for you specifically.
- **Permissions needed:** missing access in provider language.
- **Affected sources/surfaces:** what else uses the identity — within the
  workspace for a shared identity, or across your workspaces for your own
  member-owned identity.
- **Fix:** exact next action.

Coral may act silently only when it preserves the same authority:

- refresh credential material using an existing refresh grant
- renew temporary credentials for the same identity spec and audience
- continue using the identity already assigned to the workspace source surface
- revalidate after provider-side recovery

Coral must ask before:

- creating a new identity
- changing principal
- changing audience
- requesting broader capabilities
- changing injection method in a way that changes trust or credential handling
- assigning an identity to another workspace source surface
- changing a surface from a per-member slot to a shared assignment, so all
  members query as one identity
- sharing a workspace that contains identities or workspace sources
- making a workspace source available to more users
- switching from provider-managed auth to manual token entry

## Sharing, Reuse, and Permissions

Safe defaults:

- New workspaces default to private membership.
- New workspace sources default to private availability.
- A workspace-owned identity belongs to the workspace where it is created and is
  never reused by another workspace.
- A member-owned identity belongs to the member's user-scoped store and is
  reusable across that member's own workspace slots that it satisfies.
- Human-tied surfaces default to per-member slots, so each member uses their own
  identity rather than sharing one.
- Making a workspace source available to more users requires explicit
  confirmation.
- Sharing a workspace previews what new members get: the shared sources and
  projections, the workspace-owned identities they will query as, and the
  per-member slots they must fill with their own identity.
- A workspace-owned identity backed by a human principal requires a warning;
  prefer app, bot, service account, trusted-role, or OIDC identities for shared
  assignments, and per-member slots for human-tied access.

The first-wave permission model has three permissions, not per-identity ACLs:

- **Query workspace source:** run queries when availability includes the user.
  Query users may see non-secret status, missing permissions, and fix guidance.
- **Manage workspace source:** add or remove a workspace source, set each
  surface's binding mode, create or assign workspace-owned identities for shared
  surfaces, declare per-member slots, recover shared identities, and change
  availability. Managers declare slots but cannot see or fill another member's
  identities.
- **Manage workspace:** add or remove members, share the workspace, delegate
  source management, and delete or archive the workspace.

Reuse follows ownership. A workspace-owned identity may be suggested for another
compatible surface in the same workspace only when it satisfies the requirements,
the audience matches, no broader capability is needed, and availability does not
change. A member-owned identity may be suggested to its owner for any compatible
slot across that member's workspaces, but only to that member; it is never offered
to or usable by anyone else.

Even then, reuse is a choice:

```text
Use your existing GitHub identity?

> github-saul-work
  Connected as: saul@work
  Already bound in: oncall
  Access: repo read, org read

  Connect another identity
```

If reuse changes blast radius, Coral must show the affected workspaces and
workspace sources.

## Representative Identity Archetypes

The model should fit these archetypes:

- **User OAuth identity:** Google user identity for Gmail, GitHub user OAuth
  identity for GitHub REST.
- **App or bot identity:** Slack bot identity, GitHub App installation identity.
- **Local provider profile identity:** AWS profile resolved through the AWS SDK
  credential chain.
- **Cloud trust identity:** AWS trusted role or AWS OIDC trust.

User OAuth identities are typically member-owned and bound through per-member
slots; app/bot, profile, and cloud-trust identities are typically workspace-owned
and bound through shared assignments. Binding mode is the workspace's choice, so
either archetype can be bound either way when a use case requires it.

The first implementation does not need to ship all of them, but it must not
choose concepts that break any of them.

## Acceptance Criteria

- DSL v4 surfaces declare `identity_requirements`; projections do not choose
  identities.
- A surface can be bound as a shared assignment or a per-member slot, with the
  default inferred from the issuer/archetype and overridable by a manager.
- `coral source add gmail` in a local default workspace can declare a per-member
  slot on `gmail.gmail-api`, let the local user bind their own member-owned Google
  identity, and keep availability private.
- One shared workspace with a Gmail per-member slot resolves Saul's query to his
  identity and Andrea's to hers — each seeing only their own data — through a
  deterministic per-member lookup with no global resolver.
- A member-owned identity is reusable across that member's own workspace slots and
  is never visible or usable to another member.
- A shared Engineering workspace can use Slack or GitHub through a workspace-owned
  app, bot, service account, trusted-role, or OIDC identity shared by all members.
- One workspace-owned identity can back multiple compatible surfaces in the same
  workspace only after compatibility checks pass and the user confirms any
  increased blast radius.
- Another workspace cannot silently reuse or inherit a workspace-owned identity.
- New workspaces and workspace sources start private; sharing previews the shared
  sources, projections, workspace-owned identities, and per-member slots new
  members receive.
- Provider-native capability facts drive matching; unknown or unvalidated
  capabilities do not satisfy requirements.
- Background, MCP, CI, and sandboxed flows do not launch interactive auth. They
  use a supported non-interactive path or return a structured fix.
- Recovery messages are non-secret and scoped to the identity's owner:
  workspace-local for workspace-owned identities, user-scoped (listing affected
  workspaces) for member-owned identities, each with surface, connected label,
  reason, affected scope, and fix.

## Open Questions

- What final DSL v4 field names should replace
  `identity_requirements.accepts`, `issuer`, `injection_method`, `audience`,
  and `capabilities`?
- Which provider-specific `capabilities.kind` values should ship first?
- Which representative identity archetypes should the first implementation ship
  versus validate in fixtures?
- What is the CLI vocabulary: `coral identity`, `coral access`, or another
  surface?
- Which first-wave identity specs support non-interactive materialization?
- Should a surface's default binding mode (shared assignment vs per-member slot)
  be declared by the identity spec archetype, inferred from the issuer, or always
  an explicit workspace choice?
- Where do member-owned identities and slot bindings live in app state, and how
  does that storage key by member without a full principal layer in the first
  local wave?
- What is the minimal first implementation of the workspace-first model, given
  that the user authentication / principal layer lands as a separate effort?
- How does Coral detect or declare a non-interactive execution context, such as
  CI, MCP, or a sandboxed agent, so it can choose a non-interactive path instead
  of launching interactive auth?
- How should server roles package query, manage workspace source, and manage
  workspace permissions?
