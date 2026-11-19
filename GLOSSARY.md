# Coral

Coral exposes connected data to agents and attributes their work. It does not conflate current product concepts with
retired experimental terms. Coral gives a single way to discover and use the Tools that Upstream Services expose.

## Language

**Organization**: The administrative owner above Workspaces. Each Coral deployment contains exactly one Organization.
Isolating several Organizations means running separate deployments, so the boundary between Organizations is physical,
not a filter in the queries of one deployment.

An Organization Owner governs Workspaces but has no authority inside them. The Owner can list every Workspace with its
name, state, and designated Owners. The Owner can also assign a designated Owner, archive or restore a Workspace, and
grant a Secret Store to it or revoke that grant. Everything else in a Workspace needs a Workspace Membership there. A
local deployment has one implicit Organization whose implicit local User is its Organization Owner.

_Avoid_: Mutable Organization resource, inherited Workspace authority, Organization-wide access to another User's
private work

**Organization Membership**: The Coral-owned relationship that gives a User an `owner` or `member` role and an `enabled`
or `disabled` state in one Organization. An eligible first sign-in makes the User an enabled Member. Later sign-ins do
not overwrite the current role and do not re-enable a disabled Membership. Organization Owners manage roles and
disablement, and Coral protects the last enabled Organization Owner. Workspace authority needs an enabled Membership,
but the Membership never grants that authority. The `owner` role adds only Organization administration and Workspace
governance.

Removing a Membership removes the User's Workspace Memberships, stored Provider Account credentials, and current Private
Tools from every Workspace in the Organization. It does not delete Administrative Lifecycle Events, retained Run
snapshots, or historical attribution. When the User is the sole designated Owner of a Workspace, removal needs a
transfer target. The target is one enabled Organization Member, who becomes the designated Owner of every such Workspace
in the same change.

_Avoid_: Workspace Membership, administrator, inherited Workspace authority, role inferred from login metadata

**User Disablement**: An Organization Owner's suspension of a User's Organization Membership. It prevents new
authenticated requests and later upstream dispatches by that User, including those from already accepted Runs. It does
not delete the User's stable identity, Workspace Memberships, current Private Tools, or retained history. A disabled
designated Owner is still the Owner on record. Disablement is distinct from Membership removal, and signing in again
cannot undo it. It does not disable a Workspace-owned Agent merely because the User created the Agent or provisioned its
Provider Accounts. _Avoid_: User deletion, automatic session-based reactivation, Agent disablement

**Usage Report**: The anonymous JSON that a self-hosted Coral deployment sends once for each UTC day, while sharing is
on. It holds only dates, counts and values from fixed lists. A process that an environment variable or a source build
blocks never makes or sends one, and a day that only blocked processes saw gets none. _Avoid_: telemetry (Coral's traces
and metrics), analytics event, ping

**Edition**: Whether a Coral deployment runs as the Community Edition or the Enterprise Edition. A License sets it when
Coral starts. Since a deployment holds one Organization, the Edition is also the Organization's.
_Avoid_: tier, plan, license (for the Edition itself)

**Community Edition**: The Edition with no Enterprise Features enabled. Every build runs as Community until a License
says otherwise. _Avoid_: OSS edition, core, free edition

**Enterprise Edition**: The Edition that enables the Enterprise Features a build contains. _Avoid_: EE (outside code
paths), commercial edition, pro

**Enterprise Feature**: A product feature that only the Enterprise Edition enables, such as reading credentials from a
customer's Secret Store. A build that contains it enables it only while the Edition is Enterprise. A build that lacks it
does not know that it exists. _Avoid_: capability (a Program term), entitlement, add-on

**License**: A grant signed by Coral that sets a self-hosted deployment's Edition to Enterprise on every build whose
Release Date is on or before the License's expiry. A License is a Commercial License or an Evaluation License. An
Evaluation License also needs the clock to be on or before its expiry. A License names its customer for people to read,
and binds to no deployment. _Avoid_: licence, license key, token, entitlement, license file

**Commercial License**: A License that a customer paid for. After its expiry passes, it still covers the builds released
before that date. A build released later refuses to start with it. _Avoid_: paid license, production license (a
production key signs both kinds)

**Evaluation License**: A License for a trial. It sets Enterprise only until its expiry passes on the clock. Then Coral
stops, and does not start again with it. _Avoid_: trial license

**Release Date**: The date a Coral build counts as released, fixed into the build when it is made. A License compares
its expiry with this date. Only an Evaluation License also compares it with the clock. A source or development build
counts as released on its own commit's date, or on the current date when git is unavailable. _Avoid_: build date, build
time

**OAuth Client**: The client software through which a User signs in to Coral. A DCR client ID or a CIMD URL identifies
it. A public client proves only that it holds the PKCE verifier for a code issued to its redirect. A confidential client
also proves that it holds the secret issued when it registered. A client grants no authority, and its secret is never a
Coral credential.
_Avoid_: Agent Registration, client credential, app

**Refresh Grant**: The durable right that one sign-in gives one OAuth Client to get new User access tokens without
another sign-in. The right lasts until an absolute expiry that is fixed at the sign-in. Only a client that declares the
`refresh_token` grant gets one. The grant fixes the User, client, resource, private audience, and Workspace of the
sign-in, and a refresh cannot change them.

Each refresh spends one single-use refresh token and returns its successor. Presenting a spent token ends the grant. A
refresh rechecks enabled Organization Membership and, for a Workspace-scoped grant, Workspace Membership. It never
consults the upstream identity provider. Removing either Membership ends the grants that it supported, and rejoining
revives none. Ending a grant stops only refresh: access tokens already issued keep their expiry.

_Avoid_: session, offline access, sliding expiry
_In Reef_: never named

**Web Session**: Coral's hold on the Refresh Grant of one Reef sign-in. An opaque handle names the session, and the
browser's Reef session cookie carries that handle. Reef resolves the handle into a fresh Reef access token on each
request, so the session outlives its access tokens. Resolving changes nothing, so any number of requests and Reef
processes share one session. A Web Session ends at the earlier of its own maximum age and its grant's absolute expiry,
and never extends. Signing out ends the session and its grant. So do revocation, replay of its spent refresh token, and
Membership removal. Only a Reef grant can back one. _Avoid_: session token, browser session, refresh cookie
_In Reef_: never named

**User Profile**: A User's mutable name and email. Neither is guaranteed unique. The profile is distinct from the stable
User identity and never grants authority. Conflicting emails do not merge Users or transfer access. Workspace Owners can
see names and emails in authorized people and Agent-creator views. Members can see their own profile and the names and
emails of the designated Owners, but not the profiles or private work of other Members. Organization Owners can see the
designated Owners of every Workspace. _Avoid_: Email as identity, authentication record, Organization-wide directory
access through Workspace Membership

**Workspace**: The ownership root, confidentiality boundary, and execution boundary for Coral data and agent work within
one Organization. It has one immutable identity and a mutable display name. Authority inside a Workspace comes only from
a Workspace Membership, and a User must have an enabled Organization Membership to use one. Any enabled Organization
Member can create a Workspace and becomes its first designated Owner.

A Workspace owns its Providers and contains Principals and access rules. Each Provider has its own namespace, catalog,
authentication setting, and Tool policy. A Workspace contains its Program Runs, Tool Invocations, requests, and
execution history. A Workspace Owner can inspect the full retained history for the Owner's own User and for
Workspace-owned Agents, but gets no Run access for another User.

A Workspace is `active` or `archived`. Archiving rejects new Runs and later upstream dispatches, and permits no
configuration change except restoration. An accepted Run whose Program execution has started can finish secret-free
computation. If execution has not started, Coral fails the Run without executing the Program. A Workspace Owner or an
Organization Owner can archive and restore the Workspace. Final data removal belongs to host retention policy, not
ordinary Workspace administration.

_Avoid_: Immediate history deletion, using a display name as identity

**Workspace Membership**: The relationship that makes a User a designated `owner` or `member` of one Workspace, and the
only source of authority inside it. Coral creates one only for a User with a current enabled Organization Membership. A
Member can inspect their own Membership and a safe directory of the designated Owners, but cannot list other Members or
Agents. An Owner can list and manage all Workspace Memberships and Agents.

Every Workspace keeps at least one designated Owner: Coral denies leave, removal, and demotion of the last one. The rule
counts Owners on record, whatever the state of their Organization Membership. Thus a disabled Owner is still the Owner
and User Disablement changes no Membership. An Organization Owner can assign a designated Owner to any active Workspace,
including themselves. Assignment promotes a current Member.

Membership removal always ends the User's authority inside that Workspace and clears the User's stored Provider Account
credentials and current Private Tools. A User who rejoins after that cleanup must reconnect.

_Avoid_: Organization Membership, inherited Membership, effective Owner, ownerless Workspace

**Principal**: An authenticated actor to whom Coral attributes work. Its kind, User or Agent, is stable. The kind
informs authorization but does not itself grant permission. A Principal does not privately own a Task.

**Agent Principal**: An Agent Principal belongs immutably to exactly one Workspace and always acts as itself. A
Workspace Owner creates and administers it in v1. The Workspace owns it, and Coral records the User who created it
without assigning a separate Agent Owner. Its authority comes from current Workspace policy, its enabled state, and its
Providers. It has no Principal-specific Tool policy: an Agent that needs a different Tool set belongs in a different
Workspace.

A `fixed` Credential Route uses the Provider's shared Account for every caller. A `caller` route uses the Agent's own
Provider Account. A Workspace Owner provisions that Account beforehand through secret entry, Secret References or OAuth,
for that exact Agent and Provider. The Agent does not sign in interactively, and the human who provisions the Account is
not the invoking Principal. Account ownership does not become Agent authority.

An Agent is `enabled` or `disabled`. Disabling it rejects new Runs and later upstream dispatches, including pending
Invocations. It also disables the Agent's Provider Accounts, and it preserves attribution. An accepted Run whose Program
execution has started can finish secret-free computation. If execution has not started, Coral fails the Run without
executing the Program.

Moving the Agent requires a new Agent Principal and Agent Registration. Deletion removes its Registration, Provider
Account credentials, and current Private Tools under host retention policy. Deletion keeps its stable identifier, safe
attribution metadata, Administrative Lifecycle Events, retained Run source snapshots, Invocations, approvals, and
historical account identifiers.

An Agent is a caller Principal only in v1. It is not a protected resource that another Agent can call. An authenticated
Agent can observe and cancel its own accepted Runs by Run identifier and cursor. It cannot list general execution
history or access another Principal's Run. A Run identifier grants no authority.

_Avoid_: Delegated Agent, Cross-Workspace Agent, Agent Workspace Grant, movable Agent

**Principal Access Surface**: The Principal-specific view of one Workspace exposed through Streamable HTTP. V1 has no
stdio MCP transport. Interactive clients and autonomous Agents use the Workspace's non-secret endpoint or the
Organization MCP Endpoint. An interactive client authenticates its User through OAuth, and local mode maps this to the
implicit local User. An autonomous Agent authenticates with the bearer credential of its Agent Registration.

The endpoint or the authenticating credential fixes the Workspace before Program execution, and nothing later changes
it. The authenticating credential selects the Principal and attribution. Client software and MCP `clientInfo` do not.
The same Workspace can contain both kinds. Each surface applies that Principal's policy, Provider compatibility,
discovery, and attribution. A dedicated Agent Workspace can be a creation preset for isolation, but is not a separate
Workspace kind.

_Avoid_: User Workspace, Agent Workspace, registration-specific MCP endpoint, shared Principal credential,
Program-selected Workspace, session-selected Workspace

**Organization MCP Endpoint**: The deployment's one fixed MCP endpoint, which names no Workspace. A User picks one of
their active Workspaces when they sign in, and that sign-in's credential fixes the Workspace until it expires. To use
another Workspace, the User signs in again. An Agent's Registration fixes its Workspace here as it does everywhere, so
its credential works at both endpoints. A User's credential for a Workspace MCP endpoint does not work here, and a
User's credential for this endpoint does not work at a Workspace MCP endpoint. The Workspace MCP endpoint remains for a
client that uses two Workspaces at once, for a Provider that reaches another Workspace, and for the implicit local User,
who has no sign-in to pick with. _Avoid_: default Workspace, Workspace switcher, deployment endpoint

**Reef**: Coral's authenticated human interface for administration, authoring, inspection, and Invocation Approval.
Every supported local and hosted deployment provides it. Reef is the only v1 human administration surface. Its server
uses private first-party management and Run interfaces and applies normal Organization and Workspace authorization.
Reef's browser presentation receives minimum view models through server loaders, actions, and explicit resource routes,
not through a generic Coral transport or Desktop proxy. Reef's playground and Tool forms build normal TypeScript
Programs and start Runs through the private Run interface. They do not invoke Tools directly or create a second public
execution protocol. _Avoid_: Principal Access Surface, public management API, direct Tool invocation, Desktop proxy

**Reel**: The standalone developer tool for recording and reviewing browser-test scenarios and their evidence. It is
separate from Reef. It reviews test outcomes, recordings, screenshots, and source snapshots, not Coral Program Runs.
_Avoid_: Recorded UI, Reef feature, Program Run viewer

**Invoking Principal**: The authenticated Principal that submits a Coral Program Run. This Principal stays accountable
for each Tool Invocation in the Run. An approver or the owner of upstream credentials does not replace the Invoking
Principal. _Avoid_: Approver, credential owner

**Agent Registration**: The single host-owned registration that authenticates one Agent Principal and fixes one
Workspace for its Coral access. Coral creates the Agent Principal, Registration, and initial bearer credential
atomically, with no separate activation ceremony. The Workspace MCP endpoint does not select the Registration. A
separate strongly random bearer credential identifies and authenticates it. Coral shows that credential only when it
creates or rotates it and stores only a verifier. Rotation invalidates the old credential without changing the
Registration, Agent Principal, Workspace, policy, attribution, or history.

The Registration can have an optional expiration that stops authentication without deleting the Agent, Provider
Accounts, or history. A Workspace Owner can disable it or set a new expiration. Expiration has the same authority effect
as disabling the Agent, without deleting it, its Provider Accounts, or its history. An Agent cannot rotate its
credential, reactivate itself, or change its policy.

_Avoid_: MCP clientInfo, Program identity, secret URL, recoverable credential, activation ceremony

**Administrative Lifecycle Event**: An immutable, non-secret record of an accepted control-plane or authoring change
that can affect future authority, configuration, or code. It covers these changes:

- Organization Membership
- Workspace and Workspace Membership changes
- Agents and Agent Registrations
- Provider setup, publication, Credential Route and lifecycle changes
- Provider Account authorization, connection and disconnection
- Secret Store grants and revocations
- Tool policy
- approval decisions
- Private Tool saves and deletions
- Shared Tool publishes and removals

It records an actor, which is either the accountable Principal or Coral itself. It also records the action, safe target,
outcome, changed field paths, timestamp, and trace identifier. It does not copy credentials, before-and-after values, or
Invocation arguments. It also records whether a Workspace Membership or Organization Owner governance authorized the
change.

Organization Owners can inspect Organization Membership events and the events for their own governance actions: Owner
assignment, archive, restore, and Secret Store grants and revocations. A Workspace Owner can inspect that Workspace's
Workspace, Membership, Agent, Agent Private Tool, Provider, Credential Route, policy, and shared or Agent Provider
Account events. A User can inspect events for their own Provider Accounts, Membership, and Private Tools. Every Member
User can inspect Shared Tool events. Ownership does not expose another User's private work, and Agents cannot browse
these events.

Each event belongs to the domain that owns the changed state. It is not reconstructed from telemetry or duplicated as a
Program Run Event.

_Avoid_: Program Run Event, mutable audit row, secret log

**Task**: A Workspace-owned unit of agent work toward a goal. It has a declared intent, the Principal that started it, a
history of attributed attempts, and an independently declared terminal outcome of success or failure. That outcome
closes the goal to new attempts but does not cancel work already underway. A User can inspect their own Tasks, and a
Workspace Owner can inspect Tasks attributed to Workspace-owned Agents. Ownership or ordinary Workspace membership
grants no access to another User's private Task, intent, or history. Optional completion feedback belongs to the Task
and shares its visibility and retention. It is not a separate report or interaction. An agent starts a Task with a
Program that states the goal instead of naming a Task. Coral never infers which Task a call belongs to.

_Avoid_: Single call, synthetic Run wrapper, Principal-owned Task, Workspace-shared Task, using Episode or session as
aliases for Task

**Sub-agent**: A model that Coral runs to do data work for an agent that calls it. A Sub-agent has no identity and no
permissions of its own. It runs with the calling agent's permissions, inside the calling agent's Task, and every Run it
starts is recorded as the calling agent's Run. It keeps its conversation between messages, and the caller addresses it
by its own ID. _Avoid_: delegate, data agent, worker, treating a Sub-agent as a Principal

**Turn**: The work a Sub-agent does between receiving one message and sending its response, including every model call
and tool call. A Sub-agent runs at most one Turn at a time. A caller addresses the Sub-agent and never a Turn. _Avoid_:
Sub-agent Invocation, Run (for a Turn), step

**Task identifier**: The name Coral gives a Task when it starts it: four words from Coral's word list, lowercase and
joined by hyphens, such as `random-big-business-store`. Coral draws the words at random and inserts the Task with them
as its primary key. When the name is already in use, Coral draws again. Thus no two Tasks in one deployment share one. A
Task started before Coral issued words keeps its UUID text. The identifier names the Task in MCP, in gRPC resource
names, in Reef and in logs. It grants no authority: a Principal who guesses it sees nothing more than for a Task that
does not exist. _Avoid_: task id, task key, task handle, task alias, short identifier, agent identifier

**Task Interaction**: An individually identified, Workspace-owned child record in a Task's history. It records one
authorized attempt to use Coral and keeps the participating Principal, the separate per-call intent, the timing, and the
attempt outcome. A search, save or delete made by a Program is part of that Program's Run, not a separate interaction.
Task history keeps the search, describe, save and delete interactions recorded before Programs made them (ADRs 0026 and
0031). A User can inspect their own interactions, and a Workspace Owner can inspect the interactions of Workspace-owned
Agents. These rights give no access to another User's private work and give Agents no general history access.
_Avoid_: Task, independent goal, Program Run, Tool Invocation, Program Run Event, Administrative Lifecycle Event,
telemetry, replay checkpoint

**Episode**: A retired predecessor to Task that is not part of Coral's current product model.

_Avoid_: Using episode as an alias for Task

**Upstream Service**: An external system, such as Gmail or Linear, that receives calls from a Provider. Several
Providers can connect to the same Upstream Service through different interfaces or configurations. For a Built-in
Provider, Coral itself is the Upstream Service. _Avoid_: Provider, Provider Account

**Provider Recipe**: A distributed, secret-free description of one Upstream Service that pre-fills Provider Setup and
Provider Configuration. With it, adding a well-known Provider needs no hand-entered descriptor location, destinations,
OAuth settings, or scope list. It offers one or more authentication options, and the owner confirms exactly one. It
records only facts about the Upstream Service, never Coral's opinions about how to use that Service. It thus selects no
Tools, renames nothing, and pins no Provider Generation. Runtime never evaluates a Recipe. _Avoid_: Source, Source Spec,
source manifest, Provider Definition, Provider Configuration

**Provider Definition**: The normalized, secret-free definition from which Coral configures a Provider. It defines the
Tool surface, execution behavior, and supported credential contract without selecting a Workspace, Principal, or
upstream account. In v1, Coral derives it from one exact OpenAPI document or MCP discovery snapshot and the confirmed
Provider Configuration accepted through Provider Setup. It is a materialization input, not a public management resource
or a v3 compatibility manifest. _Avoid_: Provider, Provider Account, Source Spec, stored setup form

**Provider Configuration**: The owner's confirmed display name, authentication method, OAuth application settings and
secret, and destination choices for a Provider. It is separate from the Provider's unchanged input document or discovery
snapshot and from individual account grants. Its non-secret settings stay pinned with the Provider Generation.
Application secrets are write-only, stay absent from inspection, and can rotate independently of account authorization.
The source's own title is the suggested display name, and the owner can replace it.
_Avoid_: Provider Account, credential material, edited OpenAPI document
_In Reef_: "Name" in the Configuration step, with "As in the document", "As in the recipe" or "Changed by you"
provenance

**Provider Setup**: A durable preparation of one exact OpenAPI document or MCP server discovery for Provider creation or
regeneration. It pins the accepted descriptor or discovery snapshot, holds Provider Configuration for explicit review
before sign-in, and records safe compatibility diagnostics. Authentication used to read the catalog is temporary,
setup-scoped material, not a permanent discovery account. Keeping it as a Provider Account for execution requires an
explicit choice. Failed or abandoned creation leaves no usable Provider Account. Creating or regenerating a Provider
consumes the prepared Setup, and Coral does not fetch a different descriptor silently.

A Setup is `awaiting_configuration`, `awaiting_authorization`, `awaiting_discovery`, `ready`, `consumed`, `expired`, or
`canceled`. Only a ready Setup feeds Publication. Every open Setup has a host deadline one hour after it is created. A
Setup that is still open then, ready or not, expires, and Publication refuses it. Expiry and explicit cancellation scrub
setup-scoped credential material.

A Setup that `coral.addProvider` prepares records the Provider Namespace and Credential Route that the Program asked
for. The person who finishes it can change either. A Setup that regenerates a Provider, including one that
`coral.updateProvider` prepares, starts from the Provider's saved Provider Configuration. In that Setup, a source of the
other kind (an MCP server for an OpenAPI document, or the reverse) brings its own servers.

_Avoid_: Provider Definition inspection, live Provider, implicit refresh
_In Reef_: never named. It is preparation within "Add provider", "Refresh tools", "Edit configuration", and "Change
account setting".

**Provider Publication**: The durable terminal result of one synchronous attempt to create or regenerate a Provider from
one Provider Setup. Coral materializes the pinned input before one short final transaction and exposes no pending
Publication lifecycle. That transaction consumes the Setup and has one of two results. It records a safe failure and
publishes nothing, or it atomically publishes the Provider, the active catalog, and any explicitly requested initial
Provider Account. Failed regeneration leaves the existing Provider unchanged. An exact replay of a management request
recovers a committed response without duplicating resources or evidence. Loss before a terminal commit leaves no
accepted Publication. A Publication exposes safe diagnostics but no private Provider generation. _Avoid_: Google
long-running Operation, Provider generation, pending publication, request-independent import
_In Reef_: "Add provider" (in flight "Adding…", result "Added"). Regeneration commits with "Apply changes" (in flight
"Applying…", result "Updated").

**Provider Generation**: The private, immutable publication result that contains one Provider's exact Materialized Tool
Catalog, executable Tool definitions, confirmed Provider Configuration, Provider Credential Contract, model-facing
TypeScript view, and safe provenance. A Provider selects one active generation. A later Invocation selects the active
generation. An admitted Invocation keeps its selected generation until the Invocation is terminal or past its deadline.
A publication with no canonical change reuses the active generation. Coral exposes no generation list, selector, restore
operation, comparison, or caller pin. _Avoid_: Tool revision, public catalog version, Program Run catalog snapshot

**Provider**: One published, configured Tool catalog that exactly one Workspace owns. It has one immutable resource
identity and its own immutable, Workspace-unique Program namespace, authentication setting, Tool policy, and lifecycle.
Providers are independent even when they are configured from the same upstream document or service.
_Avoid_: Source, Upstream Service, Provider Definition, provider account
_In Reef_: "provider". Revoke and restore are "Disable" and "Enable". Hard deletion is "Remove".

**Built-in Provider**: The one Provider in every Workspace whose Tool catalog Coral itself defines and whose Tools Coral
itself performs, with no upstream dispatch. Its Call Root is `coral` itself, so its Tools sit directly on it, such as
`coral.search`. It shares Universal Search, Tool Access State, Approval Requests, and Tool Invocation records with every
other Provider. An Owner can disable and enable it and set its Tool policy. Nobody can rename, reconfigure, or remove
it, and it has no Provider Accounts. When Coral boots, it updates every Workspace's Built-in Provider to the catalog it
serves. A Workspace whose update fails keeps its catalog until the next boot.

Each Built-in Tool declares the Workspace action that it needs. A Principal discovers, invokes, and inspects only the
Tools whose action its authority permits:

- Every Principal reaches `listProviders`, `getPolicy` and `search`.
- Every Principal that authors in the Workspace, Agents included, reaches `addCustomTool`, `updateCustomTool` and
  `removeCustomTool`.
- Only a User with effective Workspace Owner authority reaches `addProvider`, `getProvider`, `updateProvider` and
  `updatePolicy`.

The Built-in Tools do these things:

- `addProvider` adds a Provider.
- `listProviders` lists the Workspace's Providers.
- `getProvider` reads the Provider Configuration of one Owner-created Provider, in the shapes that `addProvider` takes.
- `updateProvider` changes the configuration or source of one Owner-created Provider.
- `getPolicy` reads the exact Tool rules that a Workspace Owner set and that apply to these Tools: every Provider's
  Tools, the Built-in Tools, the Shared Tools and the caller's own Private Tools. The rules are keyed by each Tool's
  complete Call Path.
- `updatePolicy` sets the exact Tool Access State of 1 to 100 Tools by complete Call Path, all or none.
- `addCustomTool`, `updateCustomTool` and `removeCustomTool` add, update or remove one of the caller's own Private
  Tools.

These Tools never change the content of another Principal's Private Tool or of any Shared Tool. An Owner removes an
Agent's Private Tool, and publishes a Shared Tool, in Reef only. `updatePolicy` reaches an Owner-created Provider's
Tools, the Shared Tools and the Owner's own Private Tools. It refuses a Built-in Tool's path, so a Program cannot loosen
the approval on the Tools that change the Workspace.

Coral itself never publishes a Setup that a Program prepares, for a new Provider or an update, on any Credential Route.
A person previews it and publishes it. A credential-requiring Setup, on the `fixed` or the `caller` route, also needs
that person to sign in. If the Run is still current, the Tool returns the Reef page and does not wait. The page names
the Setup and, for an update, the Provider. A change to a Provider's display name alone needs no Setup and keeps its
Generation. `listProviders` reports the open Setups that the invoking User's `addProvider` calls prepared, until each is
published, expires or is canceled. Coral ends a Setup it prepared that nobody will finish.

The Built-in Tools never return to the Program a secret, a fixed credential value, or the OAuth sign-in link that Coral
prepares for one person. They never take a secret from the Program: a person enters a confidential OAuth application's
secret in Reef. A Provider's OAuth endpoints, such as its authorization URL, are configuration, not that link. A Reef
link that one of these Tools returns may name a Provider Setup. That Setup's identity grants nothing, and only the User
who started the Setup can publish it.

_Avoid_: admin API, administrative calls, coral.admin surface, system provider
_In Reef_: "Coral", with the Coral logo, listed first

**Private Provider**: The one Provider in every Workspace whose Tools are its Private Tools, under the Call Root
`coral.private`. Each Principal sees only their own Tools in it. Coral performs its Tools, with no upstream dispatch and
no Provider Accounts. A Workspace Owner can disable it, which stops every Private Tool, and can set exact rules on the
Private Tools the Owner can read: the Owner's own and every Agent's. _Avoid_: Function provider, personal provider

**Shared Provider**: The one Provider in every Workspace whose Tools are its Shared Tools, under the Call Root
`coral.shared`. Coral performs its Tools, with no upstream dispatch and no Provider Accounts, and only a Shared Tool
Publish or a removal changes its catalog. _Avoid_: Code Provider, function provider

**Provider Namespace**: The short JavaScript identifier an Owner chooses for a Provider, such as `github`, and the name
Reef shows and accepts. It is one segment of the Provider's Call Root. _Avoid_: Call Root, display name

**Provider Call Root**: The complete, immutable, Workspace-unique JavaScript path under which one Provider's Tools are
called: `coral.providers.<provider-namespace>` for an Owner-created Provider, `coral` for the Built-in Provider,
`coral.private` for the Private Provider and `coral.shared` for the Shared Provider. _Avoid_: namespace, prefix

**Tool**: A named callable that a Program calls with data and that returns data: a Provider Tool, a Private Tool, or a
Shared Tool. Every Tool call is asynchronous, takes one object of data and returns data, and is one Tool Invocation with
a Tool Access State. Data is what JSON holds: `null`, booleans, finite numbers, strings, arrays and plain objects.
_Avoid_: Function, operation, action

**Provider Tool**: A single callable action that an Upstream Service exposes through a Provider and that Coral makes
available. For the Built-in Provider, Coral itself exposes the action. A Provider Tool has named inputs and returns one
upstream-native result. "Tool" alone means any Tool, and a term that applies only to Provider Tools says so. Coral's own
MCP interface, such as `exec` and `wait`, consists of MCP tools, not Provider Tools. _Avoid_: Operation, Capability,
function, MCP tool (for a Provider Tool)
_In Reef_: "tool"

**Tool Key**: The Upstream Service-authored or deterministically derived key that addresses a Tool within one Provider's
catalog, such as an OpenAPI `operationId`, or the method and path of an Operation that has none. A Tool stays the same
Tool across Provider Generations while its Key stays the same, even when its definition changes. Its Tool Access State
and call history follow the Key. A changed Key is a new Tool that inherits neither. The Key is not a public client name.
Management represents the Provider-plus-Key address as a deterministic, transport-safe Tool resource name. That
representation does not add another Tool identity or pin a Provider generation. _Avoid_: Operation Key, Tool revision,
public Call Path, independent management id

**Tool-relative Path**: The part of a Tool Call Path after its Provider Call Root, such as `issues.get`. Provider
Publication assigns it from the Tool Key and carries it forward while that Key stays active. It names a Tool within one
Provider but does not identify it: a removed path can later name another Tool. _Avoid_: Tool Key
_In Reef_: "tool name"

**Tool Call Path**: The collision-free JavaScript path for one Provider Tool in Coral Programs and Principal-facing
discovery. It is the only v1 client name for the Tool: v1 has no separate language-neutral Tool Logical Name. Every Call
Path is its Provider's Call Root followed by the Tool-relative Path, and Provider Publication makes that Tool-relative
Path collision-free. The projection is idiomatic JavaScript. `/` and `.` in the Tool Key separate dotted namespace
segments, and each segment is lowerCamelCase, so `actions/add-repo-access` projects as `actions.addRepoAccess`. Each
Provider prefixes that projection with its immutable Call Root.

The path persists outside an MCP session and is distinct from the internal Tool Key. A removed Tool-relative path or
Provider namespace is not reserved and can be reused later. V1 provides no alias, and it does not guarantee
compatibility for a historical Call Path. A Call Path does not pin a Provider Generation. A Tool Invocation keeps the
Call Path that its Program called as a label. Activity counts and attributes the call by its Provider and Tool Key, so
calls made under a reused path never count toward the Tool that holds the path now.

_Avoid_: Operation Call Path, Tool Key, language-neutral intermediate name, Provider name, session-local function name,
Provider-specific MCP tool

**Provider Credential Contract**: The normalized, secret-free requirements for authorizing and authenticating one
Provider generation. It contains no credential values. It records these facts:

- the selected credential method
- its Credential Fields and where each is sent (header and parameter names, prefixes, literal values and path prefix)
- the OAuth flow and endpoints
- the audience
- client ownership
- PKCE behavior
- the exact requested scopes
- the normalized set of credential-receiving base URLs, including base paths

A Provider Account is compatible with a Generation only when these authority-bearing facts match its authorization.
These things do not affect compatibility: labels, descriptions, whether a field is masked while typed, schemas, Call
Paths, and Tools added at an already-authorized destination.

_Avoid_: Provider Account, credential material, scope compatibility check

**Provider Account Authorization**: A durable host-owned process that gets credentials through secret entry, Secret
References or OAuth. The credentials are for temporary Provider Setup use, for an initial Provider Account, or for the
explicit replacement of one exact Account. Its safe progress and completion metadata contain no credentials. Completion
cannot silently retarget another caller, account setting, or changed authentication requirement.
_Avoid_: Account Connection Request, request-bound OAuth stream, credential response

**Provider Account**: The durable, Provider-local authorization record and protected credentials for a Provider's shared
account or for one exact caller Principal. Each such scope has at most one current account. It is not a verified
upstream identity, selectable inventory item, or reusable account across Providers.
_Avoid_: Principal, verified upstream identity, reusable account
_In Reef_: "account". A User manages their own `caller` accounts on the workspace's personal "Accounts" page.

**Reported Grant**: The OAuth `scope` value exactly as a token endpoint returned it for a Provider Account, kept for
people to read. Coral does not compare it with the requested scopes, and the grant does not decide whether the Account
is usable. A grant that the service finds insufficient surfaces as that Tool's upstream failure.
_Avoid_: granted scopes, scope check, parsed scopes
_In Reef_: "granted permissions"

**Credential Field**: One named value in a Provider Account's credentials, such as Jira's account email or Datadog's
application key. It belongs to that one account, is never a Provider-wide setting, and holds a value entered in Coral or
named by a Secret Reference.
_Avoid_: credential input, secret (a field need not be sensitive), credential parameter

**Secret Store**: An operator-defined connection to a secrets system outside Coral, such as HashiCorp Vault, with its
own identity there. That system's policy decides what the store can read. Its kind names the secrets system. A kind may
be an Enterprise Feature.
_Avoid_: vault, credential vault, external credential store

**Secret Reference**: The address of one Credential Field's value in a Secret Store granted to the Account's Workspace:
the store, a path, and a key where the store has keys. The value stays in the store and Coral never stores a copy.
_Avoid_: vault path, secret pointer, linked secret

**Credential Route**: The Provider setting that determines whether Invocations use no credentials (`none`), its single
shared Provider Account (`fixed`), or the invoking Principal's exact Provider Account (`caller`). A Program cannot
choose the route or account for an Invocation, and Coral never falls back to another route or credential. A route is
Provider configuration. An Owner chooses it in Reef, or an Owner's Program chooses it for the Provider that
`coral.addProvider` adds. A Program can ask `coral.updateProvider` for a new route, which a person confirms in Reef.

A User's missing, disconnected, expired, or incompatible `caller` Account does not hide Tools from discovery. The User
connects a missing or disconnected Account during Invocation, before approval and dispatch. The User reconnects an
expired or incompatible Account the same way. Coral never replaces a connected Account's credentials on its own. Before
discovery, a `fixed` route and an Agent's `caller` route need a usable Account, and a `none` route needs a
credential-free Provider.

A route must suit the Provider: `none` suits only a Provider that takes no credential, and `fixed` and `caller` suit
only one that takes a credential. Coral decides this one rule for a Setup, a Publication and an Invocation. Reef offers
the routes that Coral allows.

_Avoid_: Authentication method, runtime account selection, route fallback
_In Reef_: "Whose account does it use?": "No account" (`none`), "One shared account" (`fixed`), "Individual accounts"
(`caller`). Users connect their own accounts, and a Workspace Owner connects accounts for each Agent.

**Account Connection Request**: A durable host-owned request for one exact User to connect a required Provider Account.
Compatible waiting Invocations share it, but they do not share their Run deadlines. It is private to that User and
pinned to the Provider's account setting and credential contract. It is not retargeted when either changes, and it is
never created for a shared Account or an Agent. Only an Invocation creates it, and discovery never does. Any completed
connection that leaves that User's exact Account compatible with its pin satisfies it, wherever the User connects.
_Avoid_: Provider Account Authorization, Program resume payload, automatic Program replay, Run preflight, Agent OAuth
prompt

**Materialized Tool Catalog**: The published, Provider-scoped definition of Tools produced when an Owner creates or
regenerates a Provider. It indexes summaries separately from lazily loaded Tool definitions and authoritative provider
schemas. It publishes active Tool-relative paths and a derived TypeScript view under the Provider's namespace for model
and Reef guidance. A Tool that Coral cannot invoke faithfully stays in the catalog as unavailable, with a safe
structural reason. Principal-facing discovery omits unavailable and policy-disabled Tools, and those whose Credential
Route cannot serve the Principal. It exposes no Provider Account identifier, owner, account label, or credential state.
Reef can show omitted Tools and their diagnostic reasons to authorized humans. Runtime never interprets the retained raw
OpenAPI document or MCP discovery snapshot as the catalog. _Avoid_: Raw provider descriptor, live provider catalog,
account inventory

**Authoring Description**: The complete Principal-facing TypeScript call view of one currently visible Tool, Private
Tool, or Shared Tool. It identifies the kind and call path. It contains a safe summary and one self-contained TypeScript
declaration with every public type that the callable signature references. Coral can project the declaration from
authoritative Provider schemas or emit it from Private or Shared Tool source. Callers do not receive the different
internal authority formats. Coral stores a Tool's own declaration and declares each named type of its Provider once per
Provider Generation. A Tool's complete declaration is its own declaration plus every named type that it reaches. Coral
compiles Programs against it and builds Search Hit signature lines from it.

Exact Principal-facing description returns the complete inputs, the callable, a Tool's result wrapper, and the top level
of the success data. It returns the other result types within a fixed byte budget: in full, as field-name stubs, or
listed by name. Describing the same Call Path with `types` returns the named types of its declaration whole, with their
own nested types under the same budget. Exact description of the Principal's own Private Tool without `types` also
returns its source and saved diagnostics for later editing. Exact description of a Shared Tool without `types` returns
the source, diagnostics and number of its current revision, and never who published it.

_Avoid_: JSON Schema discovery result, shared storage format

**Tool Description**: The Tool-specific Authoring Description. It also contains the Provider name, identity, safe
presentation metadata, Tool Hints, and current approval requirement. Shared types belong to the Provider namespace, such
as `coral.providers.github.SimpleUser`, not to one Tool group or a global type namespace. Authoritative provider schemas
stay private catalog material, and Coral does not also return them as JSON Schema. When a Tool's own description,
without the shared types it names, cannot fit the discovery limit, the Tool is unavailable to Principal-facing
discovery. Reef can show its safe diagnostic with the observed and permitted sizes. Within that limit, exact description
fits a Tool's result types to the describe budget and returns the rest through `types`. _Avoid_: JSON Schema discovery
result, global provider type

**Discoverable Tool**: A Tool that Principal-facing discovery returns to one Principal now. Current Workspace authority,
Principal state, Provider lifecycle, Tool Availability, and Tool Access State permit it, and its Credential Route can
serve that Principal. Discoverability is not Account readiness: a User's Discoverable Tool can still require that User
to connect an Account before dispatch. Search indexes and page offsets confer no discoverability. _Avoid_: Ready Tool,
discovery readiness

**Universal Search**: Principal-filtered relevance discovery over the Principal's Discoverable Tools and Private Tools
and the Workspace's Shared Tools. Programs reach it through the Built-in Tool `coral.search`. A query returns ranked
Search Hits, matched by words and by semantic search. Exact Call Paths return their Authoring Descriptions fitted to the
describe budget, or the types named in `types`. An exact lookup also names the paths that match nothing, with nearby
Discoverable Call Paths.

Universal Search searches only secret-free catalog material. It does not execute a Private or Shared Tool or dispatch to
an Upstream Service. Provider-native search is an ordinary Tool that only a Coral Program can invoke. V1 Universal
Search stores no Tool Result, Program Value, Run data, or observed memory. Reef exact catalog inspection is a separate
administration projection.

_Avoid_: Provider search fanout, observed-value memory, exact catalog administration

**Search Entity**: One thing Universal Search can find: a Discoverable Tool of one current Provider, one of the
Principal's current Private Tools, or one of the Workspace's live Shared Tools. Every Principal of the Workspace can
find a live Shared Tool. Each Search Entity has one complete Call Path, one Authoring Description, and one set of
secret-free fields that Universal Search indexes for it. Those fields are its metadata and its declaration-field paths.
A Tool whose declaration-field paths exceed the index bound keeps only its shallower paths, and the index logs a
warning. No search bound makes a Tool unavailable.

Universal Search returns a Search Hit for a Search Entity, and an exact lookup returns its Authoring Description.
Neither returns the Entity's private material. The Authoring Description of a Private or Shared Tool carries its source
and current revision. The Authoring Description of a Shared Tool never names who published it.

_Avoid_: Authoring Kind, search document

**Search Hit**: One item of a search page, cut to what an agent needs to choose and call a Search Entity. It holds the
Entity's Call Path, its signature line, whether that line is complete enough to call from, and its Description Snippet.
For a Tool, it also holds the declared Tool Hints, and the approval requirement and deprecation flag only when they are
set. The signature line puts required inputs first and always keeps the result. It states plainly when the result is
text, usually JSON. Whether the line is complete depends on its inputs alone. The Call Path names the kind and the
Provider, the rank reflects why the hit matched, and an exact lookup gives the rest. A Search Hit never holds the
declaration.

**Search Coverage**: What one search page states, per kind of Search Entity, about how much of the Principal's Search
Entities it examined and why results may be missing. It states how many Providers, Private Tools or Shared Tools were
eligible, searched, and failed, how many results the kind returned, and whether more remain. A kind that the Provider
filter leaves out is skipped, not incomplete. A page states coverage only for a kind whose results may be missing.
Coverage describes one page at the time of the search, not the current catalog.

**Search Index State**: Whether Universal Search has indexed a Provider's active Generation: pending until the first
attempt completes, then indexed or failed by its outcome. A later attempt can turn failed into indexed. A superseded
Generation's state is never the Provider's. Reef shows pending and failed on the Provider and its Tools. The state never
changes what a caller may invoke. _Avoid_: search readiness, index health

**Description Snippet**: The beginning of an Entity's description, cut to a bound.

**Embedder**: The Universal Search component that gives each Search Entity a vector of its text, so a query can match
meaning as well as words. It runs after indexing, never on the catalog's path, and a Search Entity without a current
vector ranks by words alone. _Avoid_: semantic retriever, embedding service, vector index

**Tool Invocation**: One child record of a Program Run and one attempt to call one Tool. It makes at most one upstream
dispatch in v1. A call to a Private Tool or Shared Tool makes none. Each Tool call that its body makes is a child
Invocation linked to it. The parent ends only after its children, and Coral refuses a child call made after its parent
ended. Repeating a call is a new Invocation, not an automatic retry.

Coral persists the Invocation and appends `invocation.started` before account or approval work. Started means admitted,
not dispatched, and the Invocation's duration includes gate waiting. An Invocation is `pending` before its durable
dispatch-start record, `dispatching` after that record and before it ends, and `ended` from then on.

An ended Invocation records `upstream_outcome`, `coral_failure`, `run_terminated`, or `indeterminate`. Run-level
termination bypasses the Program's Tool interface. `indeterminate` means dispatch started but Coral could not durably
save a complete local result, or the Coral Instance that held the Run's stream was lost with the dispatch in flight. The
Program then sees a Tool Error with code `upstream_incomplete`. A Private or Shared Tool call whose body still runs when
its Run ends records `run_terminated`, even after a lost Coral Instance. The calls that its body made keep their own
outcomes.

At admission the Invocation keeps the exact Provider identity and account-setting version, Credential Route, private
Provider Generation, and Tool Key that it resolved. The Program Run stays `running` during waits but does not pin a
generation for later Invocations. Workspaces and callers cannot list, select, restore, or pin Provider generations. The
Invocation references the higher-level Program Run and its stored Program context instead of copying the Program source.

Immediately before dispatch, Coral rechecks these things:

- current Organization Membership and Workspace Membership
- the Invoking Principal, Agent Registration, and Workspace
- the Provider lifecycle and account-setting version
- the Tool and the exact Provider policy
- the retained Credential Route
- the Provider Account and authorization version
- approval
- current token material

Coral checks authority, Provider, Tool, and policy before it resolves `none`, `fixed`, or `caller`. It resolves a
required User caller Account before it creates any required Approval Request, because approval binds the exact Account
authorization. It then rechecks all authority and configuration, refreshes or resolves credentials, and dispatches.
Approval bypasses none of these checks.

Authority loss or Run termination settles a pending Invocation and closes its exact Approval Request without dispatch or
replay. A Run timeout expires that Request instead, because the Run's deadline bounds the Request's deadline. Detaching
from a concurrent Account Connection Request does not cancel another Invocation's wait or the connection itself.
Account-setting changes, explicit Account replacement, and Disconnect stop affected pending work. Coral never switches
accounts. A Provider update alone does not stop an already-authorized Invocation that kept an older Generation and a
compatible Account, but changed authentication requirements end obsolete connection requests and their waiting
Invocations.

Routine token refresh within the same authorization permits continuation after normal checks. It does not replace the
selected authorization or invalidate approval. A permanent refresh rejection requires reconnection. A temporary refresh
failure is a coded Coral Failure and does not change Account state.

The Invocation's full authorized detail and its Approval Request can show its exact logical arguments. Summaries, logs,
notifications, and telemetry omit them. Credentials and host-generated authentication data are never Invocation
arguments.

_Avoid_: Retry attempt, query execution, stored Program

**Policy Enforcement Point**: The one authoritative check that lets a Tool Invocation start its dispatch. Each pass of
it is an enforcement check. The Coral Instance that holds the Run's stream runs it for every ready Invocation after any
wait ends. The check runs against the current Workspace, Provider, policy and account facts, the User's current
authority, and the Run state it reads, including each Approval Request's decision. A decision counts if it was made
before the Request's deadline.

A change to those facts either completes before the enforcement point reads them, or waits until the enforcement point
records its outcome. A revocation that commits first thus always wins. The enforcement point records the dispatch start.
A change that commits after that record does not stop a dispatch that already started. A cached or earlier decision
cannot authorize dispatch.

_Avoid_: Dispatch Fence, dispatch recheck, held fence, versioned decision, authorization cache

**Fan-out Limit**: The deployment-wide number of Tool Invocations of one Coral Program Run that may be `dispatching` at
the same time. Coral admits every call as an Invocation at once. A ready Invocation above the limit stays `pending`. It
starts its dispatch when an earlier one ends, in the order in which the Invocations became ready. Waits for account
connection or approval do not count toward the limit, so every Approval Request a Run needs is visible at once. A
Private or Shared Tool call does not count either: Coral performs it by running its body in the Program, and the calls
that body makes count instead. A waiting Invocation has no separate deadline. Run termination ends it as
`run_terminated`.
_Avoid_: fan-out budget, admitted-count quota, dispatch rate, per-Workspace fan-out

**Tool Availability**: Whether a cataloged Tool can be invoked while preserving its upstream-native semantics. Caller
credentials, approval, quotas, and upstream outages do not change it. Principal-facing discovery omits an unavailable
Tool. Reef can show it and its structural reason for diagnostics. _Avoid_: Caller readiness, best-effort support

**Effectful Tool**: A Tool that may change provider state or cause another external effect. Its outcome may remain
uncertain when communication fails after dispatch. Coral treats every Tool that does not declare itself read-only (see
Tool Hints) as possibly effectful. _Avoid_: Effectful Operation, write tool, mutation

**Tool Hints**: The effect hints a Tool's provider source declares: read-only, destructive, idempotent, and open-world.
Coral takes each hint at catalog materialization, and only when the source declares it. In v1 it takes them from an MCP
tool's annotations. Coral never assumes a protocol default, so an undeclared hint says nothing. OpenAPI/HTTP-sourced
Tools declare none in v1, and their HTTP method is not a hint. Coral's MCP resource read is read-only by protocol
definition. Coral declares the hints of each Built-in Provider Tool in its catalog. Hints are provider claims, and a
caller cannot configure them. Among them, only a declared read-only hint affects policy. _Avoid_: Effect classification,
`unknown` effect, tool annotations
_In Reef_: one label per hint declared true: "Read-only", "Destructive", "Idempotent", "Open world". None when
undeclared.

**Tool Access State**: The Workspace decision that one Tool is `enabled`, `approval_required`, or `disabled`. An exact
rule identifies only a Workspace, Provider, Tool Key, and state. A Workspace Owner manages it. Organization policy does
not set Tool access in v1. There are no Principal-, Agent-, Account-, or Credential Route-specific rules, patterns,
ordered rules, or globs.

A reusable Program in one Workspace thus has one Tool policy for one Provider, whatever its Invoking Principal. A
Principal can still have different discoverability and readiness, because Credential Route, Provider Accounts, and Agent
state are not policy (see Discoverable Tool). A stale Invocation fails safely. Policy for one Provider does not affect
another Provider. A Principal that needs a different Tool set belongs in a different Workspace.

If no exact rule matches, a default applies:

- A Tool that declares itself read-only defaults to `enabled`.
- An OpenAPI/HTTP-sourced Tool with a safe HTTP method (GET, HEAD, OPTIONS, or TRACE) defaults to `enabled`.
- A Built-in Tool whose catalog declares `enabled` as its default also defaults to `enabled`, as `addCustomTool`,
  `updateCustomTool` and `removeCustomTool` do, because they change only the caller's own Private Tools (ADR 0031).
- Every other Tool defaults to `approval_required`.

Coral's own catalog, a Private Tool's author, and the Owner at a Shared Tool Publish can declare a Tool's default, and
each retained Generation keeps it. The default applies to every Tool without an exact rule, including the Tools of
existing Providers. A Workspace Owner overrides it with an exact rule, for example for an upstream that changes state on
GET. Provider signature and execution-binding changes do not make an exact rule stale in v1. A changed Tool Key does.

_Avoid_: Principal policy, Agent policy, Account-specific policy, Credential Route policy, Provider permission, Tool
Availability, allow/ask/deny
_In Reef_: "policy". The states are "Allow", "Ask" and "Block".

**Context Impact**: The net payload bytes one ended Program Run adds to its caller's context: the bytes of its
Emissions, Logs and Final Value, minus the bytes of its Invocations' retained Tool Results. Negative means saved. A Run
that is running or has incomplete Invocation evidence has none. _Avoid_: token savings

**Required Action**: A request of one Program Run that a person must decide before the Run can go on. That person is the
invoking User for a Run that a User invoked, or any Workspace Owner for a Run that an Agent invoked. Today it is an
Approval Request or an Account Connection Request. Any later kind (a Provider's own question to the User, #2447) keeps
the same shape: one request with a deadline, decided by the person or role the Run names, and the first answer counts. A
Run Update's `required_actions` lists the Run's current Required Actions.

A Required Action of a Run that a User invoked reaches that User one way:

- The observing tool call returns it with the link to the page where the User decides it (an Approval Request's own
  address on the Approvals Page, or the person's account page for the Provider for a connection request).
- The assistant passes the link on.
- The waiting call shows it to a client that asks for progress or renders it as an MCP App.

Reef holds the request from the moment it exists, so a page is never a second way in, and the assistant has no path to
decide. Coral sends no MCP elicitation for a Required Action, and no notification apart from the waiting call's progress
message. An Account Connection Request that several Runs wait on is one Required Action in each of their Run Updates.
Each assistant passes it on, and the first answer settles it for all of them.

_Avoid_: pending action, human action, elicitation, prompt, in-band prompt, notification, per-client route, assistant
approval, allow-tool prompt as approval, Run preflight

**Run Status Page**: The directly linked detail of one Program Run within Reef's activity interface, at one stable,
non-secret URL. It is not a required standalone page. Every supported local or hosted deployment provides this
interface. Its link opens the exact Run independently of the activity list's current filters. Opening it requires User
OAuth and normal Run authorization, and the URL grants no authority. The view shows the Run status, Events, Invocations,
and every Approval Request and Account Connection Request that the User can inspect. It groups all current human actions
for the Run and receives Run, approval, and account-connection changes without a manual refresh. Authorized Workspace
Owners find Agent approval actions on the Approvals Page. _Avoid_: Per-Invocation action URL, bearer URL, MCP session
state

**Approval Request**: A durable host-owned request for a human decision on one pending Tool Invocation. Coral creates a
Request whenever an Invocation requires approval. It records the exact logical Tool arguments for review on the
Approvals Page and the Run Status Page independently of the agent transport. Reef is the v1 human resolution interface
that every Request can use, in local and remote deployments. A deployment without its local or hosted Reef interface is
not ready.

A Request that is undecided at its deadline settles the Invocation with `approval_expired`. A decision made before the
deadline counts whenever the Run's stream dispatches the Invocation. Declining settles the Invocation with
`approval_declined`. The first durable terminal decision wins. A later decision returns that result without changing it.
If policy becomes `disabled`, Coral settles the Invocation with `tool_disabled`. If policy becomes `enabled`, Coral
closes the Request and continues after its other checks. A later Invocation can create a new request.

The person that the Request names decides it on the Approvals Page. For a Run that a User invoked, that person is the
invoking User, and the assistant's link opens the page with the Request in review. For a Run that an Agent invoked, that
person is a Workspace Owner. Reef remains available for every Request. An active browser is not required, and existing
Requests survive a temporary Reef outage.

_Avoid_: per-Invocation approval URL, Program pause payload

**Approvals Page**: The Workspace page in Reef where a person decides the Approval Requests waiting on them and reads
those already approved, declined or expired. It shows only Requests of Runs that person can inspect, for as long as the
Run is retained. _Avoid_: approval queue, action queue, inbox

**Invocation Approval**: A human decision that satisfies an `approval_required` Tool Access State for one exact Tool
Invocation. It is bound to the Workspace, Provider and account-setting version, Invoking Principal, private Provider
Generation, Tool, Credential Route, Provider Account and authorization version when present, and arguments. A User
approves their own Invocation. Any Workspace Owner can approve an Agent Invocation. An Agent never approves, and
credential provisioning alone grants no approval authority. Coral keeps the approver Principal, decision, timestamp, and
optional short comment. A later restrictive policy change still applies at dispatch. The approval remains historical
evidence and cannot authorize a later Invocation or change policy. _Avoid_: Always enable, policy rule, reusable
approval token

**Upstream Job**: An asynchronous process owned by an Upstream Service and started or observed through separate Tools.
Coral does not merge those Tools into a workflow or automatically poll the job. _Avoid_: Provider Job, Coral workflow,
Program Run

**Upstream MCP Task**: An Upstream Service-owned asynchronous invocation lifecycle required or offered by an MCP-backed
Tool. It is distinct from Coral's Program Run lifecycle. _Avoid_: Program Run, Coral task

**TypeScript-only Coral**: A Coral product contract in which Tools, Private Tools, and Coral Programs are presented and
authored as TypeScript and SQL is absent from every layer. JavaScript remains valid TypeScript source, and Coral
Programs execute with JavaScript semantics. Type information guides authoring but does not override observed upstream
values or gate Program admission. _Avoid_: TypeScript facade, SQL hidden behind TypeScript, runtime type safety

**Coral TypeScript Profile**: The single explicit authoring and admission profile shared by Reef, Private Tool save, MCP
`exec`, and server-side Run compilation. It pins one official `typescript.js` version, compiler options, source-map
behavior, and the ECMAScript declaration libraries supported by Coral's embedded V8. It accepts JavaScript as TypeScript
source. The profile exposes no DOM, Web Worker, Node, npm, filesystem, authored import, or module-resolution surface.

Reef and Private Tool save report strict semantic diagnostics without making them admission failures. Private Tool Save
types each Tool, Private Tool and Shared Tool that the source names from the declarations that the caller can discover.
A Shared Tool Publish types its source the same way from what the publishing Owner can discover, except the Private
Tools it names, which are each caller's own. V1 one-time Program admission parses and emits source but does not compute
a full semantic check.

_Avoid_: Runtime TypeScript, client-selected compiler, Node TypeScript profile

**Program Authoring Limits**: The fixed v1 bounds applied before a Private Tool save, a Shared Tool Publish, or a
Program Run is accepted. A Private Tool Path or Shared Tool Path is at most 256 UTF-8 bytes and 16 segments. A submitted
Program or one Private or Shared Tool source is at most 64 KiB. One Run links at most 64 Private and Shared Tools
together, and its submitted plus linked TypeScript source is at most 512 KiB. Its emitted JavaScript is at most 1 MiB
and its source maps are at most 2 MiB. One Private Tool Declaration is at most 256 KiB. One save or Run compilation has
a two-second deadline. Exceeding a bound returns a safe error, creates no partial Run, and leaves an existing Private
Tool unchanged. _Avoid_: Deployment-selected authoring limit, unbounded compiler input, partial save

**Program Diagnostic**: Bounded information about an authored TypeScript issue, a Private Tool linking issue, or the
error that caused a Program Run to fail. Its purpose is to help the caller understand the cause and determine the
appropriate correction. A diagnostic makes no claim that repeating a Run is safe. Syntax and linking failures identify
the authored Program or Private Tool with a line, column, and clear message. Reef can show advisory semantic diagnostics
without making them Run failures.

Runtime diagnostics expose an existing error, not a separate diagnostic execution. Authorized Run readers may receive
Program-controlled error messages and mapped authored stacks, including bounded cause chains and grouped errors.
Detailed runtime content is limited to information visible inside the Program's isolate. Tool Errors keep their defined
shape, and trusted Coral error codes stay distinguishable from Program-controlled text. This content stays out of
telemetry, and host internals and credentials stay private. Runtime stacks map emitted JavaScript locations back to
authored TypeScript. They hide generated wrappers, emitted JavaScript, compiler internals, and V8 frames. Emitted
JavaScript stays private diagnostic data, not a product authoring view.

_Avoid_: Raw V8 stack, generated wrapper location, emitted JavaScript display

**Schema Type**: A TypeScript declaration derived from a Tool's input or output schema for authoring guidance. It does
not imply runtime TypeScript type checking, and disagreement with an observed upstream value does not prevent a Coral
Program from handling that value. _Avoid_: Runtime type, executable type

**Agent-owned Pagination**: The calling agent reads continuation state from a Tool Result and explicitly invokes each
later page. Coral returns one provider response per invocation and does not automatically traverse or combine pages.
_Avoid_: Transparent pagination, automatic pagination

**Execution Binding**: The upstream-specific invocation recipe attached to a Tool behind Coral's common Tool interface.
A v1 binding describes HTTP, MCP, a Built-in Provider Tool, or code. A Private Tool's code binding names the definition
that Coral performs, and a Shared Tool's code binding names its Shared Tool Revision, with no upstream. No binding
exposes routing or credential details to Programs. _Avoid_: Execution attachment, client transport, File Collection
binding

**Upstream Outcome**: A complete upstream result received for one Tool Invocation and represented according to its
Execution Binding. Coral keeps it whole, whether the Program receives it as a Tool Result or through a Tool Error.
_Avoid_: Provider Outcome, Upstream Error, Coral Failure, normalized response

**Declared Success**: An Upstream Outcome that its Tool declares as a success, and the only one a Program receives as a
Tool Result. A body that misses its schema is still a Declared Success. _Avoid_: ok result, upstream success state

**Coral Failure**: A safe, coded inability to produce a complete Upstream Outcome for one Tool Invocation. It excludes
Run-level termination and unexpected host defects. It makes no public claim about dispatch, external effects, or retry
safety in v1. When Coral can identify an invalid argument, an `invalid_input` failure names it and gives a Coral-written
reason, which can list the argument names that the Tool declares. Otherwise the failure may give only a general message.
A `tool_not_found` failure names the Call Path the Program wrote and the nearest Discoverable Call Paths. Apart from
that path, which is code rather than data, it never repeats a submitted value. _Avoid_: Upstream failure, transport
failure, Program failure

**Upstream Result**: The Upstream Service-owned value and safe upstream metadata carried by an Upstream Outcome. Coral
does not reshape the value into normalized rows or make upstream schema conformance a condition of success. _Avoid_:
Provider-native Result, row projection, normalized query result

**Tool Result**: The caller-facing projection of one Declared Success: `{ status, data, metadata }` for HTTP, and
`{ data, metadata }` for MCP. Its mandatory metadata carries safe execution-specific response facts. _Avoid_: Program
outcome, failure union, Upstream Error

**Tool Result Shape**: A sketch of where data lives in what a Program received from one Call Path: its field names and
value types, never its values. An object keyed by data, such as identifiers, emails or names a person chose, shows the
union of its values' shapes and not its keys. Only a Run that failed because its Program threw gets them. The final
output page of its Run Update carries one for each distinct Call Path and shape among the latest Tool Results and Tool
Errors that the Program received, in the order in which it first received each. The sketch is bounded, so it may leave
some undescribed, and it counts them. Coral derives it for that failure alone. It is not a Schema Type and makes no
claim about other Runs.
_Avoid_: Observed schema, inferred output schema, result sample

**Tool Error (`ToolError`)**: The non-constructible JavaScript `Error` subclass, `coral.ToolError`, that rejects a Tool
call that produced no Declared Success. When Coral rejects an Upstream Outcome, the error carries it. For a Coral
Failure it carries only a stable code and an optional trace identifier, and makes no claim about whether the call took
effect. _Avoid_: `OperationError`, Upstream Error, Program failure, provider response

**MCP Content Block (`McpContentBlock`)**: One ordered member of an MCP-backed Tool's content sequence, distinguished by
its content type. Binary image, audio, and embedded-resource payloads carry their bounded provider-native base64 data
inline. _Avoid_: Return value, attachment

**MCP Tool Result**: The complete result of one upstream MCP tool call. It keeps its ordered MCP Content Blocks separate
from its tool-typed structured content and metadata. An explicit `isError: true` is not a Declared Success, so the
Program receives the result through a Tool Error. _Avoid_: Flattened tool result, preferred structured content

**HTTP Tool Result**: The Tool Result of one HTTP Declared Success, which carries its status beside its data. A Tool
whose schema Coral cannot type declares one untyped result in full.

**MCP Resource Reference (`McpResourceRef`)**: An opaque, Provider-bound reference created from an upstream MCP resource
link. Reading it is a separate Provider Tool, and Coral never auto-fetches it. _Avoid_: Raw resource URI, Binary Value

**Authored Tool**: A Tool that Coral performs from TypeScript source that a Principal wrote: a Private Tool or a Shared
Tool. The two kinds share one source format, one declaration format, the same limits, and the same data-only contract.
They differ in their Call Root, their owner, and how their source changes: a Private Tool Save replaces a Private Tool,
and a Shared Tool Publish adds a Shared Tool Revision. A Provider Tool is not an Authored Tool: an Upstream Service or
Coral itself performs it. Use the term only for what both kinds share. In the README, docs and Reef, "custom tool" is
the plain-English name for an Authored Tool, and the Built-in Tools that author Private Tools carry it (`addCustomTool`,
`updateCustomTool`, `removeCustomTool`). Domain code keeps Private Tool and Shared Tool.
_Avoid_: Saved Function, code tool, user-defined tool

**Private Tool Path**: The Principal-owned dotted JavaScript path relative to `coral.private` that names one Private
Tool, such as `internal.finance.calculateTax`. Each segment is a valid JavaScript path segment, and Coral rejects
reserved internal segments. Parent paths are virtual namespaces, not folders or separate resources. The complete path
identifies one current definition for that Principal in one Workspace. _Avoid_: Filesystem path, public package name,
Private Tool version

**Private Tool Save**: The atomic creation (`coral.addCustomTool`) or replacement (`coral.updateCustomTool`) of one
current Private Tool at an exact Private Tool Path. A call to either Built-in Tool, from a Program or from a Private
Tool that the Program links, supplies the path, an explicit description, and the function to save. That function is one
that the calling source declares at its top level. The saved source is that declaration and every top-level declaration
that it reaches, as written, with the function as its default export. Saving never executes the saved source.

`addCustomTool` fails when the path exists. `updateCustomTool` fails when the path is missing or, given an expected
revision, when the current revision differs. Without an expected revision, the latest accepted save wins. Either Tool
can set the Private Tool's default Tool Access State. When `addCustomTool` omits it, the default is `enabled`. An update
that omits it keeps the current one. A save replaces the source, description, declaration, and diagnostics together, and
a failure leaves the current Private Tool unchanged. `coral.removeCustomTool` removes one.

Each call is a Tool Invocation of its Run, not a separate Task Interaction. MCP has no authoring tools. A save outlives
its Run. A Program can call the function that it saves, but not by its Private Tool Path, because its Run linked its
Private Tools at admission. Updating or deleting a Private Tool cannot change an already accepted Run.

Only the owning Principal can save or replace a Private Tool. A Workspace Owner can list, inspect, and delete an
Agent-owned Private Tool but cannot save or replace one as the Agent. Reef uses a private first-party service for the
same behavior, with the source as text. V1 exposes no public versions, edit token, or rename operation.

_Avoid_: Draft save, partial publication, Private Tool generation, source string, saveFunction

**Private Tool Declaration**: The exact, self-contained TypeScript declaration that Coral's pinned TypeScript compiler
emits from one accepted Private Tool source. It is the callable interface that Universal Search, exact description, Reef
authoring, and Program admission use. A Private Tool is an accepted callable, not an editor draft. Saving fails on
syntax, declaration emission, forbidden capabilities, invalid exports, unresolved static Private Tool paths, or
authoring limits. Ordinary TypeScript semantic diagnostics stay advisory, and Coral stores them with the accepted
source. The declaration can use TypeScript interfaces that have no faithful JSON Schema projection. Coral stores it
atomically with the source that produced it and does not make JSON Schema the Private Tool's interface authority.
_Avoid_: Private Tool schema, hand-maintained declaration, runtime type

**Private Tool**: A reusable named TypeScript callable that a User or Agent saves at a Private Tool Path within one
Workspace, under that Principal's `coral.private` namespace. A User's current Private Tools are private to that User. A
Workspace Owner can read the source, description, declaration, and diagnostics of an Agent-owned Private Tool and can
delete it, but cannot author code as the Agent. Disabling a Principal keeps its current Private Tools. Deleting an Agent
or removing a User from the Workspace removes that Principal's current Private Tools and does not transfer them.
Administrative Lifecycle Events and retained Run source snapshots remain under their own retention rules.

Its source is a no-import TypeScript module with exactly one default-exported callable. Coral owns the external name and
call path. The module can contain local types, classes, constants, helper functions, and top-level initialization, but
it cannot use packages or Node APIs. Its authored TypeScript is its behavior authority, and its Private Tool Declaration
is its interface authority. Its type information guides callers but does not create runtime TypeScript semantics. A
Private Tool is not a Provider Tool and can call several Tools. Like every Tool, it takes and returns only data: no
callback, function, or class instance crosses a call. Coral does not publish the Private Tool as JSON Schema or a
separately retained JavaScript executable.

Top-level initialization can use the normal Program Runtime Surface, including Tools, Private Tools, `coral.emit`, and
`console`. It runs once for each accepted Run package, before the submitted Program body. Every resulting call, output,
and failure uses the same attribution, gates, limits, deadline, durability, and failure rules as the rest of the Run.
Coral applies no special static effect analysis to this code, and a linked module can invoke a Tool even when the
submitted Program never calls its exported function. The Tool calls that a Private Tool's top-level initialization makes
belong to its module, under no parent Invocation.

At Program admission, Coral statically identifies literal Private Tool references, rejects computed Private Tool Paths,
and links the exact current source closure into the Program compilation. From the Program and from each linked source,
it also extracts the module that each literal `coral.addCustomTool` call saves (Private Tool Save). V1 does not preserve
reverse-reference integrity when a Private Tool is later deleted. A missing source in the closure blocks a later Run at
admission.

Within an accepted Run, each call to a Private Tool is one asynchronous Tool Invocation of that Run and creates no
separate Run. Its Tool calls, emissions, logs, time, and resource use belong to the enclosing Run. A throw in its body
reaches the caller as a Tool Error with code `tool_failed`. A Tool Error from a nested call keeps its own code.

_Avoid_: Saved Function, UDF, Operation, Private Tool JSON Schema, dynamic function lookup

**Shared Tool**: A Workspace-owned Tool under `coral.shared` that a Workspace Owner published from a Private Tool, and
that every Principal in the Workspace can find, read, and call. It runs with the caller's own authority, policy, and
Provider Accounts. It takes and returns only data, and it returns its own value, not a Tool Result. It belongs to the
Workspace's one Shared Provider, which Coral performs, as it does the Built-in Provider. Its author is attribution only:
removing, disabling, or deleting the author never affects it.

Only a Workspace Owner removes a Shared Tool. Removal frees its path and keeps its revisions. Coral refuses a removal
while another Shared Tool's current revision calls the Tool. The Tool's path names it only while it is live. Its
identity names it for good. By that identity, Member Users, but not Agents, read its history, after a removal too. The
history is every revision and every publish and removal event.

_Avoid_: Shared function, public function, library function, package

**Shared Tool Revision**: One immutable, numbered publication of a Shared Tool's source, description, and declaration.
Exactly one revision is current. Each Run links the current one and records which. Program admission selects it.
Acceptance refuses the Run if, in between, a publish moved it or a removal took its Shared Tool. _Avoid_: Version, pin,
Provider Generation

**Shared Tool Publish**: A Workspace Owner's act in Reef that copies one of their own or an Agent's Private Tools into a
new Shared Tool Revision and makes it current. The Owner reviews the exact content that is published and chooses the
Shared Tool's default Tool Access State, and the Private Tool stays as it was. The review shows the declaration that the
source emits, which follows the Tools that the source names: Coral refuses a publish that would store another
declaration, because one of those Tools changed since the review. Restoring publishes an earlier revision's content
again. No Program, Tool, or Agent publishes.
_Avoid_: Promote, move, deploy

**Coral Program**: Principal-authored TypeScript, lowered to JavaScript, that a Coral-owned, secret-free runtime
executes. It runs against schema-typed Provider Tools under their Call Roots (`coral.providers` and `coral`), Private
Tools under `coral.private`, and Shared Tools under `coral.shared`. JavaScript remains valid source, and type
information guides authoring without gating admission. A Program receives Tool Results, catchable Tool Errors,
`coral.emit`, and a bounded global `console`. Run cancellation and deadlines stay host-owned and have no Program-visible
signal. A Program receives no Workspace, Principal, Provider Account, policy, approval, credential, connection URL, or
connection-request data. A Tool Error can identify a safe condition such as a required account, required approval, or
disabled Tool, but it does not reveal authority configuration. A Coral Program can reach Upstream Services only through
host-mediated Tools. _Avoid_: Trusted local script, arbitrary Node process, ambient authority

**Program Compiler**: The component of a Run Executor that applies the Coral TypeScript Profile, links Private Tool
source, emits JavaScript and source maps, and produces safe Program Diagnostics. Each Run Executor keeps an official
`typescript.js` compiler warm in one compiler-only V8 isolate. That isolate receives only packaged compiler assets and
the virtual source files that Coral sends. It exposes no Program host binding or ambient authority. It is recycled after
memory growth and replaced after the compiler fails. Diagnostics are not a failure.

Program execution never shares a compiler isolate. Every accepted Run uses a fresh execution isolate that contains only
emitted JavaScript and the Program Runtime Surface. The Coral process runs no compiler.

_Avoid_: Runtime compiler, Node compiler process, compiler process, shared compiler and Program isolate

**Program Runtime Surface**: The secret-free ECMAScript environment in which V8 executes an accepted Program package. It
provides ordinary objects, arrays, classes, closures, generators, Promises, `async` and `await`, maps, sets, regular
expressions, dates, JSON, `Intl`, BigInt, Symbol, the bounded Coral `console`, and the `coral` host namespace. BigInt
and other non-Program Values can be used internally but cannot be emitted or returned as Program Values. The surface
provides no timers, DOM, browser, Node, Deno, npm, import, `require`, worker, subprocess, shared-memory, Atomics,
filesystem, environment, direct-network, credential, or WebAssembly surface. V8 disables code generation from strings,
including `eval`, `new Function`, and indirect access through `function.constructor`. _Avoid_: Browser sandbox, Node
sandbox, ambient standard library, dynamic code

**Program Admission**: The host step that accepts a submitted TypeScript Program. Coral accepts the Program only after
it does these things:

- resolves the Program's statically named Private Tools
- builds the exact source closure
- enforces capability and size rules
- checks that every literal Tool Call Path names a Tool that the Principal can call
- emits executable JavaScript and source maps

Syntax, missing source, unknown Call Path, forbidden capability, emission, and limit failures return safe diagnostics
and create no Program Run. V1 does not compute ordinary TypeScript semantic diagnostics during one-time Program
admission. After admission succeeds, later JavaScript or Tool failures belong to the durable Program Run.

_Avoid_: Runtime compilation, partial Run, semantic type gate

**Program Run**: The higher-level Workspace record for one accepted execution of a Coral Program. MCP work associates it
with a Task, but standalone Reef execution does not require one. The Run is identified independently of the request used
to start or observe it. Caller disconnection does not end the Run: completion, explicit cancellation, or Coral policy
does. A handled Tool Error does not fail the Run. An unhandled one does, including one that carries an Upstream Outcome.

The Run stores the exact accepted Program source, the identities and exact source snapshots of linked Private Tools, and
enough private compiler output and provenance to keep the code that V8 executed. Every new Run compiles its submitted
source and linked Private Tool closure with Coral's currently pinned TypeScript profile, even when a Private Tool was
saved under an older profile. Coral stores the selected profile and emitted output with the Run. It does not keep or
select old compilers per Private Tool. The Run identity identifies this stored source and executable package, and v1
adds no separate source or executable digest. A later profile update can affect future Runs but cannot change an
accepted Run.

The accepted Run fixes its linked Private Tool code but does not fix one Provider generation for all later Tool calls.
Each Tool Invocation keeps selecting the active generation when that call starts. Child Tool Invocations reference this
stored Program context.

Immediately before Program execution starts, Coral rechecks the current Organization Membership, Workspace, Principal,
Agent, and Agent Registration authority. Authority loss after acceptance but before execution starts fails the Run
without executing the Program or creating a Tool Invocation. After execution starts, authority loss blocks later
upstream dispatch but does not stop secret-free computation.

The Run is attributed to its Invoking Principal, but that Principal does not own it. Sharing a Workspace shares
Providers and policy, not Run history. A User can list, inspect, and cancel the Runs that the User invoked. A Workspace
Owner can inspect and cancel Runs invoked by the Owner's User and by Workspace-owned Agents. The Owner receives no
access to another User's Runs. Other Members cannot inspect another Principal's Runs.

An authenticated Agent can observe and cancel only its own accepted Run, by Run identifier and cursor. It cannot list
Run history or access another Principal's Run. An Organization Owner without a Workspace Membership sees no Runs there.
Credential provisioning alone grants no Run access. Loss of Workspace access stops future reads but does not delete
history. Authorized human Run detail can show the exact submitted Program source, linked Private Tool sources, and
logical Invocation arguments. It cannot show credentials, generated authentication fields, or internal host values.

_Avoid_: Program request, connection-scoped execution, Principal-owned Run

**Run identifier**: The name Coral gives a Program Run when it accepts it: four words from Coral's word list, lowercase
and joined by hyphens, such as `height-word-database-library`. Coral draws the words at random and inserts the Run with
them as its primary key. When the name is already in use, Coral draws again. Thus no two Runs in one deployment share
one. A Run accepted before Coral issued words keeps its UUID text. The identifier names the Run in MCP, in gRPC resource
names, in the Run Status Page URL, and in logs. It grants no authority: a Principal who guesses it sees nothing more
than for a Run that does not exist. _Avoid_: run id, run key, run handle, run alias, short identifier, agent identifier

**Program Run Event**: An immutable, sequenced observation recorded during a Coral Program Run for incremental
consumption and audit. It describes Run execution, including cancellation and recovery effects, but does not duplicate
control-plane, authoring, or approval decisions recorded as Administrative Lifecycle Events. Run Events are not replay
checkpoints. _Avoid_: Administrative Lifecycle Event, stream chunk, workflow checkpoint

**Coral Instance**: One running Coral server process, identified by a random id it generates each time it starts. Any
Coral Instance admits Run Executors, takes waiting Runs for them, serves a Run's stream and commits its chunks, and
receives an outside Run Request for any Run. The Run row records the Coral Instance that holds the Run's stream, so a
wake-up for the Run reaches it. Any Coral Instance also serves any MCP request, since Coral keeps no MCP session.
_Avoid_: replica, node, worker, pod, process

**Run Executor**: The runtime outside the Coral process that executes accepted Coral Program packages, each in a fresh
V8 isolate, and compiles Programs with the Program Compiler. It dials Coral at one address and presents a credential:
one the operator gave it, or, for a local child, one Coral wrote to its stdin at start. Coral admits it by that
credential, never by the network it dials from. It is identified by a random id it generates each time it starts.

On one pull stream it reports its room, a count of the Runs and compiles it is willing to hold, and Coral sends it Runs
and compiles against that count. It opens one stream per Run it holds. A Run starts when Coral records that the Run
Executor loaded it, and its deadline runs from its start. The Run Executor numbers its Program's requests, keeps the
ones Coral has not yet committed, and reconnects a Run to any Coral Instance after a lost link. A lost link or a lapsed
lease never ends a Program. The Program runs until it finishes, exceeds a limit of its own, reaches the Run's deadline,
or Coral terminates it or answers that the Run ended.

The Run Executor holds no other secret and no database connection. Its only connections are the ones it opens to Coral,
and any connection to a remote runtime that it drives. Its isolates run in its own process, or on a remote runtime that
it drives.

_Avoid_: worker, runner, sandbox host, Execution Host

**Run Ledger**: The durable record of one Coral Program Run, appended to in chunks: the Program Run Events, the
Run-scoped records they describe, and the Run's head. Every chunk advances the head by one and commits only if it
extends the head it was built on, so the Ledger has one order and the first commit wins. The Coral Instance that holds
the Run's stream commits a chunk whenever none is in flight and something is pending. A chunk thus holds what arrived
during the previous commit, up to a fixed size. The Coral Instance that receives an outside Run Request, and the sweep,
write their chunks under the same head. Reading the head again resolves an uncertain commit. _Avoid_: event log, stream,
journal, checkpoint

**Run Request**: A message that asks Coral to decide something about one Coral Program Run: start a Tool Invocation,
publish a Program Emission or Program Log, finish the Run, decide an Approval Request, or cancel. A Run Request passes
through three stages:

1. **Applied**, when the Coral Instance that holds the Run's stream has decided it in memory.
2. **Recorded**, when that decision is durable.
3. **Settled**, when what it started has ended.

The Program continues on an Applied answer. Every later effect of the Run is recorded only after that answer is. A
requester outside the Run acts on an answer only once it is Recorded.

A Run Request that decides something is identified by the thing it decides (the Approval Request, the Run, the
Invocation), so the first request wins and a repeat receives the recorded answer. A Program's requests carry the number
its Run Executor gave them, so an Emission or Log has an identity too: a repeat carries the same number and lands once.
When the output applied but not yet recorded exceeds the **Applied cap**, the Run Executor holds the Program's next
request. The output that a Run can lose or hold in memory thus stays bounded. Deadlines and changes outside the Run
reach the Run's stream as wake-ups, not Run Requests.

_Avoid_: command, signal, mailbox message, request token, idempotency key

**Run Trace**: The Coral-owned trace that every span of one Coral Program Run belongs to. It starts at Run acceptance
and is separate from the request that started the Run, which it links to. _Avoid_: Run context, worker trace

**Run Output Budget**: The limit on the bytes of Program Emissions, Program Logs, Tool Invocation results, and the Final
Value that one Coral Program Run may produce. The Run Executor counts each Emission, Log and Final Value against the
exact total as the Program produces it. It counts each Invocation result against that total by the bytes that Coral
reports with it. Thus a Run never exceeds its budget by a byte. Coral confirms the total when it records the chunk. On a
Run Executor, an Emission, a Log or an Invocation result that does not fit ends the Program, and the Run fails with
`resource_limit_exceeded`. A Program in Coral's own process observes a refused Emission or Log, and a refused Invocation
result ends that Invocation with a Tool Error. A Final Value that does not fit fails the Run. Tool arguments do not
count. _Avoid_: approximate budget, soft limit, output quota, bounded overshoot

**Run CPU Budget**: The CPU time that one Coral Program Run may use while its Program executes. Time spent waiting for a
Tool, an approval or an answer from Coral does not count. A Run that exceeds its budget fails, and the other Runs on its
Run Executor go on. By default the budget equals the Run's time limit, from its start to its deadline, so only a lower
budget set by the operator ends a Run before its deadline. _Avoid_: CPU timeout, CPU quota

**Program Emission**: A bounded, structured value that a Coral Program explicitly publishes during its Run. An Emission
is durable caller-facing output, does not suspend execution, and remains observable if the Run later fails. _Avoid_:
Yield, console output

**Program Log**: A diagnostic message produced through a Coral Program's console during its Run. A Program Log aids
observation but is not caller-facing Program output. _Avoid_: Emission, result

**Binary Value (`BinaryValue`)**: A bounded, transport-neutral inline representation of finite binary data in an
HTTP-backed Tool or Program Value. It carries validated base64 data and safe descriptive metadata but has no durable
identity, storage lifetime, or stable cross-Run reference. MCP-backed Tools keep provider-native content blocks.
_Avoid_: Artifact, file path, byte stream, stable cross-Run reference

**Program Value**: The bounded, interoperable JSON published when a Coral Program crosses a durable output boundary. A
Program can use any supported JavaScript value internally. When `coral.emit` or the successful return value publishes
output, native ECMAScript JSON property access, `toJSON`, omission, ordering, and coercion semantics apply. A root
`undefined` return means there is no Final Value, while an Emission must produce interoperable JSON. A serialization
failure or a shared JSON-model failure produces no Program Value. Bounded Binary Values and provider-native MCP Content
Blocks are explicit tagged extensions that keep their binary forms. A Tool Error follows ordinary native serialization,
so a Program must project its safe fields to a plain value when it wants to publish them. _Avoid_: Treating an internal
JavaScript value as already durable, assuming every runtime value has a portable JSON form

**Final Value**: The optional Program Value produced when a Coral Program Run succeeds. It is distinct from the Run's
Emissions and Program Logs. _Avoid_: Transcript, accumulated output

**Run Deadline**: The instant a Program Run was accepted plus its timeout. It is the one clock that every wait for a
person obeys. Nothing pauses or extends it. A Run that waits past it ends `TimedOut`, and a decision that arrives later
does nothing for that Run. An Approval Request has no deadline of its own. An Account Connection Request keeps its own
expiry across Runs, and the deadline the Run Update shows for it is the earlier of that expiry and the Run Deadline.
_Avoid_: approval deadline, human-wait limit, paused Run

**Run Update**: An observation of a Coral Program Run. It contains the Run's current state, a current Principal-filtered
`required_actions` list, a bounded ordered batch of authored Program Emissions and Program Logs after an opaque cursor,
and the next cursor. Automatic execution Events remain in authorized human history, not Run Updates. A terminal Run's
outcome appears on the final output page. `has_more` means authored records or terminal details remain to be delivered.
The cursor identifies authored-record delivery progress and the last visible required-action revision. Internal
execution bookkeeping alone is not a meaningful update. Adapters may project a Run Update into an MCP tool result or
HTTP response without changing its semantics.

The required actions are a current snapshot, not a Run state or Program Run Events. Several can exist for concurrent
Invocations. Each visible Request appears once, even if several Invocations wait on it. A required action identifies
these things:

- an account connection or Invocation approval
- its required actor
- its deadline
- its opaque non-authorizing request identifier
- the stable non-secret URL of the page where the person decides it: the Approvals Page with the Request in review, or,
  for a connection request, the person's account page for the Provider
- the waiting Tool's Call Path, which is absent when Invocations of different Tools share one connection request
- the display name of that Tool's Provider

An Account Connection Request appears only to its invoking User. An Agent can see that its own Run requires a Workspace
Owner action, but cannot inspect or resolve the request. A terminal Run has no required actions.

_Avoid_: Full execution history, stream chunk, transport result, treating a required action as Run state or a Program
Run Event
