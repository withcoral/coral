# Coral

Coral exposes connected data to agents and attributes their work without conflating current product concepts with
retired experimental terminology. Coral provides a unified way to discover and use Tools exposed by Upstream
Services.

## Language

**Organization**: The administrative owner above Workspaces. Each Coral deployment contains exactly one Organization.
Isolating multiple Organizations means running separate deployments, so the cross-Organization boundary is physical
rather than an in-query filter within one deployment. An Organization Owner governs Workspaces without authority inside
them: the Owner can list every Workspace with its name, state, and designated Owners, assign a designated Owner, and
archive or restore a Workspace. Everything else in a Workspace needs a Workspace Membership there. A local deployment
has one implicit Organization whose implicit local User is its Organization Owner. _Avoid_: Mutable Organization
resource, inherited Workspace authority, Organization-wide access to another User's private work

**Organization Membership**: The Coral-owned relationship that gives a User an `owner` or `member` role and an
`enabled` or `disabled` state in one Organization. An eligible first sign-in establishes an enabled Member; subsequent
sign-ins neither overwrite the current role nor re-enable a disabled Membership. Organization Owners manage roles and
disablement, and Coral protects the last enabled Organization Owner. Enabled Membership is a prerequisite for
Workspace authority and never grants it; the `owner` role adds Organization administration and Workspace governance
only. Removing Membership removes the User's Workspace Memberships, stored Provider Account credentials, and current
Saved Functions from every Workspace in the Organization without deleting Administrative Lifecycle Events, retained
Run snapshots, or historical attribution. When the User is the sole designated Owner of a Workspace, removal needs a
transfer target: one enabled Organization Member who becomes the designated Owner of every such Workspace in the same
change. _Avoid_: Workspace Membership, administrator, inherited Workspace authority, role inferred from login metadata

**User Disablement**: An Organization Owner's suspension of a User's Organization Membership, preventing new
authenticated requests and later upstream dispatches by that User, including those from already accepted Runs, without
deleting the User's stable identity, Workspace Memberships, current Saved Functions, or retained history. A disabled
designated Owner is still the Owner on record. Disablement is distinct from
Membership removal, cannot be undone by signing in again, and does not disable Workspace-owned Agents merely because
the User created them or provisioned their Provider Accounts. _Avoid_: User deletion, automatic session-based
reactivation, Agent disablement

**User Profile**: A User's mutable name and email, neither guaranteed unique, distinct from the stable User identity and
never a grant of authority. Conflicting emails do not merge Users or transfer access. Workspace Owners can see names
and emails in authorized people and Agent-creator views; Members can see their own profile and the designated Owners'
names and emails, but not other Members' profiles or private work. Organization Owners can see the designated Owners of
every Workspace. _Avoid_: Email as identity, authentication record, Organization-wide directory access through
Workspace Membership

**Workspace**: The ownership root, confidentiality boundary, and execution boundary for Coral data and agent work
within one Organization. It has one immutable identity and a mutable display name. Authority inside a Workspace comes
only from a Workspace Membership, and a User must have enabled Organization Membership to use one. Any enabled
Organization Member can create a Workspace and becomes its first designated Owner. A Workspace owns its Providers and
contains Principals and access rules. Each Provider has its own namespace, catalog, authentication setting, and
Tool policy. A Workspace contains its Program Runs, Tool Invocations, requests, and execution history. A
Workspace is `active` or `archived`. Archiving rejects new Runs and later upstream dispatches and permits no
configuration change other than restoration. An accepted Run whose Program execution has started can finish
secret-free computation; if execution has not started, Coral fails it without executing the Program. A Workspace Owner
can inspect full retained history for the Owner's own User and for Workspace-owned Agents, but receives no Run access
for another User. A Workspace Owner or an Organization Owner can archive and restore the Workspace. Final data removal
belongs to host retention policy, not ordinary Workspace administration. _Avoid_: Immediate history deletion, using a
display name as identity

**Workspace Membership**: The relationship that makes a User a designated `owner` or `member` of one Workspace, and the
only source of authority inside it. Coral creates one only for a User with a current enabled Organization Membership.
A Member can inspect that Member's Membership and a safe directory of the designated Owners, but cannot list other
Members or Agents. An Owner can list and manage all Workspace Memberships and Agents. Every Workspace keeps at least
one designated Owner: leave, removal, and demotion of the last one are denied. The rule counts Owners on record,
whatever the state of their Organization Membership, so a disabled Owner is still the Owner and User Disablement
changes no Membership. An Organization Owner can assign a designated Owner to any active Workspace, including
themselves; assignment promotes a current Member. Membership removal always ends the User's authority inside that Workspace and clears
the User's stored Provider Account credentials and current Saved Functions. Rejoining after that cleanup requires
reconnecting. _Avoid_: Organization Membership, inherited Membership, effective Owner, ownerless Workspace

**Principal**: An authenticated actor, stably classified as a User or Agent, to whom Coral attributes work. Kind informs
authorization but does not itself grant permission; a Principal does not privately own a Task.

**Agent Principal**: An Agent Principal belongs immutably to exactly one Workspace and always acts as itself. A
Workspace Owner creates and administers it in v1; the Workspace owns it, and Coral records its creating User without
assigning a separate Agent Owner. Its authority comes from current Workspace policy, its enabled state, and its
Providers. It has no Principal-specific Tool policy; an Agent that needs a different Tool set
belongs in a different Workspace. A `fixed` Credential Route uses the Provider's shared Account for every caller.
A `caller` route uses the Agent's own Provider Account, provisioned beforehand by a Workspace Owner through
secret entry or OAuth for that exact Agent and Provider. The Agent does not sign in interactively; the provisioning
human is not the invoking Principal. Account ownership does not become Agent authority. An Agent is `enabled` or
`disabled`. Disabling it rejects new Runs and later upstream
dispatches, including pending Invocations, and disables its Provider Accounts while preserving attribution. An
accepted Run whose Program execution has started can finish secret-free computation; if execution has not started, Coral
fails it without executing the Program. Moving the Agent requires a new Agent Principal and Agent Registration. Deletion
removes its Registration, Provider Account credentials, and current Saved Functions under host retention policy,
but keeps its stable identifier, safe attribution metadata, Administrative Lifecycle Events, retained Run source
snapshots, Invocations, approvals, and historical account identifiers. An Agent is a caller Principal only in v1; it is
not a protected resource that another Agent can call. An authenticated Agent can observe and cancel its own accepted
Runs by Run identifier and cursor, but cannot list general execution history or access another Principal's Run. A Run
identifier grants no authority. _Avoid_: Delegated Agent, Cross-Workspace Agent, Agent Workspace Grant, movable Agent

**Principal Access Surface**: The Principal-specific view of one Workspace exposed through Streamable HTTP; v1 has no
stdio MCP transport. Interactive clients and autonomous Agents use the Workspace's non-secret endpoint or the
Organization MCP Endpoint. An interactive
client authenticates its User through OAuth; local mode maps this to the implicit local User. An autonomous Agent
authenticates with its Agent Registration bearer credential. The endpoint or the authenticating credential fixes the
Workspace before Program execution; nothing later changes it. The authenticating credential, not client software or
MCP `clientInfo`, selects the Principal and attribution. The same
Workspace can contain both kinds. Each surface applies that Principal's policy, Provider compatibility, discovery, and
attribution. A dedicated Agent Workspace can be a creation preset for isolation, but is not a separate Workspace kind.
_Avoid_: User Workspace, Agent Workspace, registration-specific MCP endpoint, shared Principal credential,
Program-selected Workspace, session-selected Workspace

**Organization MCP Endpoint**: The deployment's one fixed MCP endpoint, which names no Workspace. A User picks one of
their active Workspaces when they sign in, and that sign-in's credential fixes the Workspace until it expires; to use
another Workspace, the User signs in again. An Agent's Registration fixes its Workspace here as it does everywhere,
so its credential works at both endpoints. A User's credential for a Workspace MCP endpoint does not work here, and a
User's credential for this endpoint does not work at a Workspace MCP endpoint. The Workspace MCP endpoint remains for a client that uses two Workspaces at once, for a Provider that
reaches another Workspace, and for the implicit local User, who has no sign-in to pick with. _Avoid_: default
Workspace, Workspace switcher, deployment endpoint

**Reef**: Coral's authenticated human interface for administration, authoring, inspection, and Invocation Approval.
Every supported local and hosted deployment provides it. Reef is the only v1 human administration surface; its server
uses private first-party management and Run interfaces and applies normal Organization and Workspace authorization.
Reef's browser presentation receives minimum view models through server loaders, actions, and explicit resource routes
rather than a generic Coral transport or Desktop proxy. Reef's playground and Tool forms build normal TypeScript
Programs and start Runs through the private Run interface; they do not invoke Tools directly or create a second
public execution protocol. _Avoid_: Principal Access Surface, public management API, direct Tool invocation,
Desktop proxy

**Reel**: The standalone developer tool for recording and reviewing browser-test scenarios and their evidence. It is
separate from Reef and reviews test outcomes, recordings, screenshots, and source snapshots—not Coral Program Runs.
_Avoid_: Recorded UI, Reef feature, Program Run viewer

**Invoking Principal**: The authenticated Principal that submits a Coral Program Run. This Principal stays accountable
for each Tool Invocation in the Run. An approver or the owner of upstream credentials does not replace the Invoking
Principal. _Avoid_: Approver, credential owner

**Agent Registration**: The single host-owned registration that authenticates one Agent Principal and fixes one
Workspace for its Coral access. Coral creates the Agent Principal, Registration, and initial bearer credential
atomically, with no separate activation ceremony. The Workspace MCP endpoint does not select the Registration. A
separate strongly random bearer credential identifies and authenticates it. Coral shows that credential only when it
creates or rotates it and stores only a verifier. Rotation invalidates the old credential without changing the
Registration, Agent Principal, Workspace, policy, attribution, or history. The Registration can have an optional
expiration that stops authentication without deleting the Agent, Provider Accounts, or history. A Workspace Owner can
disable it or set a new expiration. Expiration has the same authority effect as disabling the Agent, without deleting
it, its Provider Accounts, or its history. An Agent cannot rotate its credential, reactivate itself, or change its
policy. _Avoid_: MCP clientInfo, Program identity, secret URL, recoverable credential, activation ceremony

**Administrative Lifecycle Event**: An immutable, non-secret record of an accepted control-plane or authoring change
that can affect future authority, configuration, or code. It covers Organization Membership, Workspace and Workspace
Membership changes, Agents and Agent Registrations, Provider setup, publication, Credential Route and lifecycle changes,
Provider Account authorization, connection and disconnection, Tool policy, approval decisions, and Saved Function
saves and deletions. It records an actor that is either the accountable Principal or Coral itself, together with the
action, safe target, outcome, changed field paths, timestamp, and trace identifier, without copying credentials,
before-and-after values, or Invocation arguments. It also records whether a Workspace Membership or Organization Owner
governance authorized the change. Organization Owners can inspect Organization Membership events and the events for
their own governance actions: Owner assignment, archive, and restore. A Workspace Owner can inspect that Workspace's
Workspace, Membership, Agent, Agent Saved Function, Provider, Credential Route, policy, and shared or Agent Provider
Account events. A User can inspect events for their own Provider Accounts, Membership, and Saved Functions. Ownership
does not expose another User's private work, and Agents cannot browse these events. Each event belongs to the domain
that owns the changed state; it is not reconstructed from telemetry or duplicated as a Program Run Event. _Avoid_:
Program Run Event, mutable audit row, secret log

**Task**: A Workspace-owned unit of agent work toward a goal, with declared intent, the Principal that started it,
a history of attributed attempts, and an independently declared terminal success or failure outcome that closes the goal
to new attempts without canceling work already underway. A User can inspect that User's Tasks and a Workspace
Owner can inspect Tasks attributed to Workspace-owned Agents, but ownership or ordinary Workspace membership grants no
access to another User's private Task, intent, or history. Optional completion feedback belongs to the Task and shares its
visibility and retention; it is not a separate report or interaction.

_Avoid_: Single call, synthetic Run wrapper, Principal-owned Task, Workspace-shared Task, using Episode or session as
aliases for Task

**Task identifier**: The name Coral gives a Task when it starts it: four words from Coral's word list, lowercase and
joined by hyphens, such as `random-big-business-store`. Coral draws the words at random, inserts the Task with them as
its primary key, and draws again when the name is already in use, so no two Tasks in one deployment share one. A Task
started before Coral issued words keeps its UUID text. The identifier names the Task in MCP, in gRPC resource names, in
Reef and in logs. It grants no authority: a Principal who guesses it sees nothing more than for a Task that does not
exist. _Avoid_: task id, task key, task handle, task alias, short identifier, agent identifier

**Task Interaction**: An individually identified, Workspace-owned child record in a Task's history of one authorized
attempt to use Coral, retaining the participating Principal, separate per-call intent, timing, attempt outcome, and any
search page or exact description as returned at that time. A User can inspect that User's interactions and a
Workspace Owner can inspect Workspace-owned Agent interactions, without gaining another User's private work or giving
Agents general history access.
_Avoid_: Task, independent goal, Program Run, Tool Invocation, Program Run Event, Administrative Lifecycle Event,
telemetry, replay checkpoint

**Episode**: A retired predecessor to Task that is not part of Coral's current product model.

_Avoid_: Using episode as an alias for Task

**Upstream Service**: An external system, such as Gmail or Linear, that receives calls from a Provider. Several
Providers can connect to the same Upstream Service through different interfaces or configurations. For a Built-in
Provider, Coral itself is the Upstream Service. _Avoid_: Provider,
Provider Account

**Provider Recipe**: A distributed, secret-free description of one Upstream Service that pre-fills Provider Setup and
Provider Configuration, so that adding a well-known Provider needs no hand-entered descriptor location, destinations,
OAuth settings, or scope list. It offers one or more authentication options, and the owner confirms exactly one. It
records only facts about the Upstream Service and never Coral's opinions about how that Service should be used, so it
selects no Tools, renames nothing, and pins no Provider Generation. Runtime never evaluates a Recipe. _Avoid_:
Source, Source Spec, source manifest, Provider Definition, Provider Configuration

**Provider Definition**: The normalized, secret-free definition from which Coral configures a Provider. It defines the
Tool surface, execution behavior, and supported credential contract without selecting a Workspace, Principal, or
upstream account. In v1, Coral derives it from one exact OpenAPI document or MCP discovery snapshot and the confirmed
Provider Configuration accepted through Provider Setup. It is a materialization input, not a public management resource or a v3 compatibility manifest. _Avoid_:
Provider, Provider Account, Source Spec, stored setup form

**Provider Configuration**: The owner's confirmed display name, authentication method, OAuth application settings and
secret, and destination choices for a Provider, separate from its unchanged input document or discovery snapshot and
individual account grants. Its non-secret settings remain pinned with the Provider Generation; application secrets are
write-only, remain absent from inspection, and can rotate independently of account authorization. The source's own title
is the suggested display name; the owner can replace it.
_Avoid_: Provider Account, credential material, edited OpenAPI document
_In Reef_: "Name" in the Configuration step, with "As in the document", "As in the recipe" or "Changed by you" provenance

**Provider Setup**: A durable preparation of one exact OpenAPI document or MCP server discovery for Provider creation or
regeneration. It pins the accepted descriptor or discovery snapshot, holds Provider Configuration for explicit review
before sign-in, and records safe compatibility diagnostics. Authentication used to read the catalog is temporary,
setup-scoped material rather than a permanent discovery account; retaining it as a Provider Account for execution
requires an explicit choice. Failed or abandoned creation leaves no usable Provider Account. Creating or regenerating a
Provider consumes the prepared setup; Coral does not fetch a different descriptor silently. A Setup is `awaiting_configuration`, `awaiting_authorization`, `awaiting_discovery`, `ready`, `consumed`, `expired`,
or `canceled`. Only a
ready Setup feeds Publication. Every open Setup has a host deadline one hour after it is created; a Setup still open
then, ready or not, expires, and Publication refuses it. Expiry and explicit cancellation scrub setup-scoped credential
material. A Setup that `coral.workspace.addProvider` prepares records the Provider Namespace and Credential Route the
Program asked for; the person who finishes it can change either. A Setup that regenerates a Provider, including one
that `coral.workspace.updateProvider` prepares, starts from the Provider's saved Provider Configuration; a source of the other kind,
an MCP server for an OpenAPI document or the reverse, brings its own servers. _Avoid_: Provider Definition inspection, live Provider,
implicit refresh
_In Reef_: never named; preparation within "Add provider", "Refresh tools", "Edit configuration", and "Change account setting"

**Provider Publication**: The durable terminal result of one synchronous attempt to create or regenerate a Provider
from one Provider Setup. Coral materializes the pinned input before one short final transaction and exposes no pending
Publication lifecycle. That transaction consumes the Setup and either records a safe failure while publishing nothing or
atomically publishes the Provider, active catalog, and any explicitly requested initial Provider Account;
failed regeneration leaves the existing Provider unchanged. An exact management-request replay recovers a
committed response without duplicating resources or evidence, while loss before a terminal commit leaves no accepted
Publication. It exposes safe diagnostics but no private Provider generation. _Avoid_: Google long-running Operation,
Provider generation, pending publication, request-independent import
_In Reef_: "Add provider" (in flight "Adding…", result "Added"); regeneration commits with "Apply changes" (in flight "Applying…", result "Updated")

**Provider Generation**: The private, immutable publication result that contains one Provider's exact Materialized
Tool Catalog, executable Tool definitions, confirmed Provider Configuration, Provider Credential Contract, model-facing TypeScript view, and
safe provenance. A Provider selects one active generation. A later Invocation selects the active generation, while an
admitted Invocation retains its selected generation until it is terminal or past its deadline. A publication with no
canonical change reuses the active generation. Coral exposes no generation list, selector, restore operation,
comparison, or caller pin. _Avoid_: Tool revision, public catalog version, Program Run catalog snapshot

**Provider**: One published, configured Tool catalog owned by exactly one Workspace, with one immutable resource
identity and its own immutable, Workspace-unique Program namespace, authentication setting, Tool policy, and
lifecycle. Providers are independent even when configured from the same upstream document or service.
_Avoid_: Source, Upstream Service, Provider Definition, provider account
_In Reef_: "provider"; revoke and restore are "Disable" and "Enable"; hard deletion is "Remove"

**Built-in Provider**: The one Provider in every Workspace whose Tool catalog Coral itself defines and whose
Tools Coral itself performs, with no upstream dispatch. Its Call Root is `coral.workspace`. It shares search,
describe, Tool Access State, Approval Requests, and Tool Invocation records with every other Provider. Each of
its Tools declares the Workspace action it needs, and a Principal discovers, invokes, and inspects only the
Tools whose action its authority permits: every Principal reaches `listProviders` and `getPolicy`, and only a User
with effective Workspace Owner authority reaches `addProvider`, `getProvider`, `updateProvider` and `updatePolicy`. An
Owner can disable and enable it and set its Tool policy, but nobody can rename, reconfigure, or remove it, and it
has no Provider Accounts. Its Tools add a Provider (`addProvider`), list the Workspace's Providers
(`listProviders`), read one Owner-created Provider's Provider Configuration in the shapes `addProvider` takes
(`getProvider`), change one Owner-created Provider's configuration or source (`updateProvider`), read the exact
Tool rules a Workspace Owner set for one Owner-created Provider, keyed by each Tool's Call Path relative to
its Call Root (`getPolicy`), and set the exact Tool Access State of 1–100 of its Tools by those paths, all or
none (`updatePolicy`). `updatePolicy` names a Provider under `coral.providers`, so it never reaches a Built-in
Tool, and a Program cannot loosen the approval on the Tools that change the Workspace. When Coral boots, it
updates every Workspace's Built-in Provider to the catalog it serves; a Workspace whose update fails keeps its catalog
until the next boot. Coral itself publishes a new Provider only from a ready Setup on the `none` route. A
credential-requiring Setup, on the `fixed` or the `caller` route, always needs a person to sign in. Coral itself
publishes an update only from a ready Setup that keeps the Provider's Credential Route, its credential contract, and
every Tool Key of its active Generation; any other update needs a person to confirm it. If a person must finish the
Setup and the Run is still current, the Tool returns the Reef page, which names the Setup and, for an update, the
Provider, and does not wait; `listProviders` reports the open Setups that the invoking User's `addProvider` calls
prepared, until each is published, expires or is canceled. Coral ends a Setup it prepared that nobody will finish. Its
Tools never return a secret, a fixed credential value, or the OAuth sign-in link Coral prepares for one person to
the Program, and never take a secret from it: a person enters a confidential OAuth application's secret in Reef. A
Provider's OAuth endpoints, such as its authorization URL, are configuration, not that link. A Reef link one returns may
name a Provider Setup; that Setup's identity grants nothing, and only the User who started the Setup can publish it.
_Avoid_: admin API, administrative calls, coral.admin surface, system provider
_In Reef_: "Coral", with the Coral logo, listed first

**Provider Namespace**: The short JavaScript identifier an Owner chooses for a Provider, such as `github`, and the
name Reef shows and accepts. It is one segment of the Provider's Call Root. _Avoid_: Call Root, display name

**Provider Call Root**: The complete, immutable, Workspace-unique JavaScript path under which one Provider's Tools
are called: `coral.providers.<provider-namespace>` for an Owner-created Provider and `coral.workspace` for the Built-in
Provider. Uniqueness applies to the Call Root, so a Provider named `workspace` (`coral.providers.workspace`) never clashes with
the Built-in Provider. _Avoid_: namespace, prefix

**Provider Tool**: A single callable action exposed by an Upstream Service, or by Coral for the Built-in Provider,
through a Provider and made available by Coral.
It has named inputs and returns one upstream-native result. "Tool" alone, and in the terms below such as Tool Key and
Tool Invocation, means a Provider Tool. Coral's own MCP interface, such as `exec` and `search`, consists of MCP tools,
not Provider Tools. _Avoid_: Operation, Capability, function, MCP tool (for a Provider Tool)
_In Reef_: "tool"

**Tool Key**: The Upstream Service-authored or deterministically derived key that addresses a Tool within one
Provider's active catalog. It is not a cross-generation identity or a public client name. Management represents the
current Provider-plus-Key address as a deterministic, transport-safe Tool resource name; that representation does
not add another Tool identity or pin a Provider generation. A changed Key produces a changed resource name, and
policy does not transfer silently. _Avoid_: Operation Key, Tool revision, public Call Path, independent management
id

**Tool Call Path**: The collision-free JavaScript path for one Provider Tool in Coral Programs and
Principal-facing discovery. It is the only v1 client name for the Tool; v1 has no separate
language-neutral Tool Logical Name. Every Call Path starts with its Provider's Call Root. Provider
Publication assigns and collision-resolves the Tool-relative projection, then carries it forward while the same
Tool Key stays active. The projection is idiomatic JavaScript: `/` and `.` in the Tool Key separate dotted
namespace segments and each segment is lowerCamelCase, so `actions/add-repo-access` projects as
`actions.addRepoAccess`. Each Provider prefixes that projection with its immutable Call Root. The path
persists outside an MCP session and is distinct from the internal Tool Key.
A removed Tool-relative path or Provider namespace is not reserved and can be reused later. V1 provides
no alias or historical Call Path compatibility guarantee. A Call Path does not pin a Provider Generation. _Avoid_:
Operation Call Path, Tool Key, language-neutral intermediate name, Provider name, session-local function name,
Provider-specific MCP tool

**Provider Credential Contract**: The normalized, secret-free requirements for authorizing and authenticating one
Provider generation. It records the selected credential method, its Credential Fields and where each is sent (header
and parameter names, prefixes, literal values and path prefix), OAuth flow and
endpoints, audience, client ownership, PKCE behavior, exact requested scopes, and the normalized set of credential-receiving
base URLs, including base paths, without containing credential values. A Provider Account is compatible with a
Generation only when these authority-bearing facts match its authorization; labels, descriptions, whether a field is
masked while typed, schemas, Call Paths, and Tools added at an already-authorized destination do not affect
compatibility.
_Avoid_: Provider Account, credential material, scope compatibility check

**Provider Account Authorization**: A durable host-owned process that obtains credentials through secret entry or OAuth
for temporary Provider Setup use, an initial Provider Account, or explicit replacement of one exact Account. Its safe
progress and completion metadata contain no credentials, and completion cannot silently retarget another caller,
account setting, or changed authentication requirement.
_Avoid_: Account Connection Request, request-bound OAuth stream, credential response

**Provider Account**: The durable, Provider-local authorization record and protected credentials for its shared account
or one exact caller Principal, with at most one current account for that scope. It is not a verified upstream identity,
selectable inventory item, or reusable account across Providers.
_Avoid_: Principal, verified upstream identity, reusable account
_In Reef_: "account"; a User manages their own `caller` accounts on the workspace's personal "Accounts" page

**Reported Grant**: The OAuth `scope` value exactly as a token endpoint returned it for a Provider Account, kept for
people to read. It is not compared with the requested scopes and does not decide whether the Account is usable; a
grant the service finds insufficient surfaces as that Tool's upstream failure.
_Avoid_: granted scopes, scope check, parsed scopes
_In Reef_: "granted permissions"

**Credential Field**: One named value in a Provider Account's credentials, such as Jira's account email or Datadog's
application key. It belongs to that one account and is never a Provider-wide setting.
_Avoid_: credential input, secret (a field need not be sensitive), credential parameter

**Credential Route**: The Provider setting that determines whether Invocations use no credentials (`none`), its single
shared Provider Account (`fixed`), or the invoking Principal's exact Provider Account (`caller`). A Program cannot
choose the route or account for an Invocation, and Coral never falls back to another route or credential. A route is
Provider configuration: an Owner chooses it in Reef, or an Owner's Program chooses it for the Provider that
`coral.workspace.addProvider` adds. A Program can ask `coral.workspace.updateProvider` for a new route, which a
person confirms in Reef. A User's missing,
disconnected, expired, or incompatible `caller` Account does not hide Tools from discovery. The User connects a
missing or disconnected Account during Invocation, before approval and dispatch, and reconnects an expired or
incompatible one the same way; Coral never replaces a connected Account's credentials on its own. Before discovery, a
`fixed` route and an Agent's `caller` route need a usable Account, and a `none` route needs a credential-free Provider.
A route must suit the Provider: `none` suits only a Provider that takes no credential, and `fixed` and `caller` suit
only one that takes a credential. Coral decides this one rule for a Setup, a Publication and an Invocation; Reef offers
the routes Coral allows.
_Avoid_: Authentication method, runtime account selection, route fallback
_In Reef_: "Whose account does it use?": "No account" (`none`), "One shared account" (`fixed`), "Individual accounts"
(`caller`); Users connect their own accounts, and a Workspace Owner connects accounts for each Agent.

**Account Connection Request**: A durable host-owned request for one exact User to connect a required Provider Account,
shared by compatible waiting Invocations without sharing their Run deadlines. It is private to that User and pinned to
the Provider's account setting and credential contract; it is not retargeted when either changes and is never created
for a shared Account or an Agent. Only an Invocation creates it; discovery never does. Any completed connection that
leaves that User's exact Account compatible with its pin satisfies it, wherever the User connects.
_Avoid_: Provider Account Authorization, Program resume payload, automatic Program replay, Run preflight, Agent OAuth prompt

**Materialized Tool Catalog**: The published, Provider-scoped definition of Tools produced when an Owner
creates or regenerates a Provider. It indexes summaries separately from lazily loaded Tool definitions and
authoritative provider schemas. It publishes active Tool-relative paths and a derived TypeScript view under the
Provider's namespace for model and Reef guidance. A Tool that Coral cannot invoke
faithfully remains cataloged as unavailable with a safe structural reason. Principal-facing discovery omits unavailable
and policy-disabled Tools, and those whose Credential Route cannot serve the Principal. It exposes no Provider
Account identifier, owner, account label, or credential state. Reef can show omitted Tools and their diagnostic
reasons to authorized humans. Runtime never interprets the retained raw OpenAPI document or MCP discovery snapshot as
the catalog. _Avoid_: Raw provider descriptor, live provider catalog, account inventory

**Authoring Description**: The complete Principal-facing TypeScript call view of one currently visible Tool or
Saved Function. It identifies the kind and call path and contains a safe summary plus one self-contained TypeScript
declaration with every public type that the callable signature references. Its declaration can be projected from
authoritative Provider schemas or emitted from Saved Function source; callers do not receive the different internal
authority formats. Coral stores a Tool's own declaration and declares each named type of its Provider once per
Provider Generation; a Tool's complete declaration is its own declaration plus every named type it reaches.
Coral compiles Programs against it and builds Search Hit signature lines from it. Exact Principal-facing description
returns the complete inputs, the callable, an
Tool's result wrapper, and the top level of the success data, and the other result types within a fixed byte
budget: in full, as field-name stubs, or listed by name. Describing the same Call Path with `types` returns the named
types of its declaration whole, with their own nested types under the same budget. Exact description of the Principal's
own Saved Function without `types` also returns its source and saved diagnostics for later editing. _Avoid_: JSON Schema discovery result,
shared storage format

**Tool Description**: The Tool-specific Authoring Description. It also contains the Provider name, identity,
safe presentation metadata, Tool Hints, and current approval requirement.
Shared types belong to the Provider namespace, such as `coral.providers.github.SimpleUser`, rather than to one
Tool group or a global type namespace. Authoritative provider schemas remain private catalog material and are not
also returned as JSON Schema. A Tool whose own description, without the shared types it names, cannot fit the
discovery limit is unavailable to Principal-facing discovery, while Reef can show its safe diagnostic with the observed
and permitted sizes; within that limit, describe fits
its result types to the describe budget and returns the rest through `types`. _Avoid_: JSON Schema discovery result,
global provider type

**Discoverable Tool**: A Tool that Principal-facing discovery returns to one Principal now. Current
Workspace authority, Principal state, Provider lifecycle, Tool Availability, and Tool Access State permit it,
and its Credential Route can serve that Principal. Discoverability is not Account readiness: a User's Discoverable
Tool can still require that User to connect an Account before dispatch. Search indexes and cursors confer no
discoverability. _Avoid_: Ready Tool, discovery readiness

**Universal Search**: Principal-filtered relevance discovery over the Principal's Discoverable Tools and Saved
Functions. It returns Search Hits with evidence; describe returns an Authoring Description fitted to the describe
budget, or the types of it named in `types`. It searches only
secret-free catalog material and does not execute a Saved Function or dispatch to an Upstream Service. Provider-native
search is an ordinary Tool that only a Coral Program can invoke. V1 Universal Search stores no Tool Result,
Program Value, Run data, or observed memory. Reef exact catalog inspection is a separate administration projection. _Avoid_: Provider search fanout, observed-value memory, exact catalog administration

**Search Entity**: One thing Universal Search can find: a Discoverable Tool of one current Provider, or one of
the Principal's current Saved Functions. Each Search Entity has one complete Call Path, one Authoring Description,
and one set of secret-free fields, its metadata and declaration-field paths, that Universal Search indexes for it. An
Tool whose declaration-field paths exceed the index bound keeps only its shallower paths, and the index logs a
warning; no search bound makes a Tool unavailable. Universal Search returns a Search Hit for it, and describe its
Authoring Description, never the Entity's private material. _Avoid_: Authoring Kind, search document

**Search Hit**: One item of a search page, cut to what an agent needs to choose and call a Search Entity: its Call
Path, signature line, and Description Snippet; a Guidance Snippet and matched tags only where a query term matched
them, and the matched arguments that its cut signature line does not show; for a Tool, its declared Tool
Hints, and its approval requirement and deprecation flag only when set.
The Call Path names the kind and the Provider, the rank reflects why the hit matched, and describe gives the rest.
Never the declaration.

**Search Coverage**: What one search page states, per kind of Search Entity, about how much of the Principal's Search
Entities it examined and why results may be missing: how many Providers or Saved Functions were eligible, searched, and
failed, how many results the kind returned, and whether more remain. A kind that the Provider filter leaves out is
skipped, not incomplete. A page states coverage only for a kind whose results may be missing. Coverage describes one
page at the time of the search, not the current catalog.

**Search Index State**: Whether Universal Search has indexed a Provider's active Generation: pending until the first
attempt completes, then indexed or failed by its outcome. A later attempt can turn failed into indexed. A superseded
Generation's state is never the Provider's. Reef shows pending and failed on the Provider and its Tools. The state never
changes what a caller may invoke. _Avoid_: search readiness, index health

**Description Snippet**: The bounded window of an Entity's description that a Search Hit shows: around the first
matched query term when the description matched, otherwise its beginning.

**Guidance Snippet**: The same bounded window over an Entity's guidance, shown only when a query term matched in it.

**Tool Invocation**: One child record of a Program Run and one attempt to call one Tool, making at most one
upstream dispatch in v1. Coral persists it and appends `invocation.started` before account or approval work; started
means admitted, not dispatched, and its duration includes gate waiting. An Invocation is `pending` before its durable
dispatch-start record, `dispatching` after that record and before it ends, and `ended` thereafter. An ended Invocation
records `upstream_outcome`, `coral_failure`, `run_terminated`, or `indeterminate`; Run-level termination bypasses the
Program's Tool interface, while `indeterminate` means dispatch started but Coral could not durably save a complete
local result. At admission it retains the exact Provider identity and account-setting version, Credential Route, private
Provider Generation, and Tool Key that it resolved. The Program Run remains `running` during waits but does not pin
a generation for later Invocations. Workspaces and callers cannot list, select, restore, or pin Provider generations.
The Invocation references the higher-level Program Run and its stored Program context instead of copying the Program
source. Immediately before dispatch, Coral rechecks current Organization Membership and Workspace Membership, the Invoking Principal, Agent Registration, Workspace, Provider lifecycle and account-setting version,
Tool, exact Provider policy, retained Credential Route, Provider Account and authorization version, approval,
and current token material. Coral checks authority, Provider, Tool, and policy before it resolves `none`, `fixed`,
or `caller`. It resolves a required User caller Account before creating any required Approval Request, because approval
binds the exact Account authorization. It then rechecks all
authority and configuration, refreshes or resolves credentials, and dispatches. Approval bypasses none of these checks.
Authority loss or Run termination settles a pending Invocation and closes its exact Approval Request without dispatch or
replay. Detaching from a concurrent Account Connection Request does not cancel another Invocation's wait or the
connection itself. Account-setting changes, explicit Account replacement, and Disconnect stop affected pending work;
Coral never switches accounts. A Provider update alone does not stop an already-authorized Invocation that retained an
older Generation and compatible Account, but changed authentication requirements end obsolete connection requests and
their waiting Invocations. Routine token refresh within the same authorization permits continuation after normal checks;
it does not replace the selected authorization or invalidate approval. A permanent refresh rejection requires
reconnection; a temporary refresh failure is a coded Coral Failure and does not change Account state. Its full
authorized detail and Approval Request can show its exact logical arguments; summaries, logs, notifications, and
telemetry omit them. Credentials and host-generated authentication data are never Invocation arguments. Repeating a call
is a new Invocation rather than an automatic retry. _Avoid_: Retry attempt, query execution, stored Program

**Policy Enforcement Point**: The one authoritative check that lets a Tool Invocation start its dispatch. Each pass of
it is an enforcement check. The Run Owner runs it for every ready Invocation after any wait has ended, against the
current Workspace, Provider, policy and account facts, the User's current authority, and the Run state it holds,
including each Approval Request's decision. A change to those facts either completes before the enforcement point reads
them or waits until it has recorded its outcome, so a revocation that commits first always wins. The enforcement point
records the dispatch start; a change that commits after it does not stop a dispatch already started. A cached or earlier
decision cannot authorize dispatch. _Avoid_: Dispatch Fence, dispatch recheck, held fence, versioned decision,
authorization cache

**Fan-out Limit**: The deployment-wide number of Tool Invocations of one Coral Program Run that may be
`dispatching` at the same time. Coral admits every call as an Invocation at once; a ready Invocation above the limit
stays `pending` and starts its dispatch, in the order it became ready, when an earlier one ends. Waits for account
connection or approval do not count toward the limit, so every Approval Request a Run needs is visible at once. A
waiting Invocation has no separate deadline; Run termination ends it as `run_terminated`. _Avoid_: fan-out budget,
admitted-count quota, dispatch rate, per-Workspace fan-out

**Tool Availability**: Whether a cataloged Tool can be invoked while preserving its upstream-native semantics.
Caller credentials, approval, quotas, and upstream outages do not change it. Principal-facing discovery omits an
unavailable Tool; Reef can show it and its structural reason for diagnostics. _Avoid_: Caller readiness,
best-effort support

**Effectful Tool**: A Tool that may change provider state or cause another external effect. Its outcome may
remain uncertain when communication fails after dispatch. Coral treats every Tool that does not declare itself
read-only (see Tool Hints) as possibly effectful. _Avoid_: Effectful Operation, write tool, mutation

**Tool Hints**: The effect hints a Tool's provider source declares: read-only, destructive, idempotent, and
open-world. Coral takes each hint at catalog materialization only when the source declares it, from an MCP tool's
annotations in v1, and never assumes a protocol default, so an undeclared hint says nothing. OpenAPI/HTTP-sourced
Tools declare none in v1, and their HTTP method is not a hint; Coral's MCP resource read is read-only by protocol
definition. Coral declares the hints of each Built-in Provider Tool in its catalog. Hints are provider claims, not
caller-configurable, and only a declared read-only hint affects policy among them. _Avoid_: Effect classification,
`unknown` effect, tool annotations
_In Reef_: one label per hint declared true: "Read-only", "Destructive", "Idempotent", "Open world"; none when undeclared

**Tool Access State**: The Workspace decision that one Tool is `enabled`, `approval_required`, or `disabled`.
An exact rule identifies only a Workspace, Provider, Tool Key, and state. A Workspace Owner manages it.
Organization policy does not set Tool access in v1, and there are no Principal-, Agent-, Account-, or
Credential Route-specific rules, patterns, ordered rules, or globs. A reusable Program in one Workspace therefore has
one Tool policy for one Provider regardless of its Invoking Principal. A Principal can still have different
discoverability and readiness because Credential Route, Provider Accounts, and Agent state are not policy (see
Discoverable Tool); a stale Invocation fails safely. If no exact rule matches, a default applies: a Tool that
declares itself read-only, or an OpenAPI/HTTP-sourced Tool with a safe HTTP method (GET, HEAD, OPTIONS, or TRACE),
defaults to `enabled`; every other Tool defaults to `approval_required`. The default applies to every Tool
without an exact rule, including those of existing Providers, and a Workspace Owner overrides it with an exact rule, for
example for an upstream that changes state on GET. Provider signature and execution-binding changes do not
stale an exact rule in v1. A changed Tool Key does. Policy for one Provider does not affect another Provider.
A Principal that needs a different Tool set belongs in a different Workspace.
_Avoid_: Principal policy, Agent policy, Account-specific policy, Credential Route policy, Provider permission,
Tool Availability, allow/ask/deny

**Context Impact**: The net payload bytes one ended Program Run adds to its caller's context: the bytes of its
Emissions, Logs and Final Value, minus the bytes of its Invocations' retained Tool Results. Negative means saved. A
Run that is running or has incomplete Invocation evidence has none. _Avoid_: token savings

**Run Status Page**: The directly linked detail of one Program Run within Reef's activity interface, at one stable,
non-secret URL, rather than a required standalone page. Every supported local or hosted deployment provides this
interface, and its link opens the exact Run independently of the activity list's current filters. Opening it requires
User OAuth and normal Run authorization; the URL grants no authority. The view shows the Run status, Events,
Invocations, and every Approval Request and Account Connection Request that the User can inspect. It groups all current
human actions for the Run and receives Run, approval, and account-connection changes without a manual refresh.
Authorized Workspace Owners find Agent approval actions through Reef's queue and notifications. _Avoid_: Per-Invocation
action URL, bearer URL, MCP session state

**Action Prompt**: A negotiated MCP elicitation that asks the invoking User about one current Approval Request or
Account Connection Request of their own Run, while an observation of that Run is in progress. Each observing tool call
asks about each current request at most once; a later call asks again while the request is still pending. An approval
prompt is a form: accepting it approves that exact Invocation and declining it declines the Invocation, with the same
first-wins rules as Reef. A connection prompt is a URL elicitation to the User's Reef page for that Provider Account, sent
only to clients that support URL mode; declining it declines the request, and accepting it only opens the page. Dismissing a
prompt without an answer decides nothing. An in-band prompt whose request no longer needs an answer is withdrawn; a
later answer to an obsolete prompt decides nothing. The prompt keeps no durable state and grants no authority beyond the
authenticated User's own. Agent bearer sessions, other Users, and Owners observing Agent Runs are never prompted.
_Avoid_: one prompt per Run, form connection prompt

**Approval Request**: A durable host-owned request for a human decision on one pending Tool Invocation. Reef is
the v1 human resolution interface that every Request can use, in local and remote deployments; a deployment without its
local or hosted Reef interface is not ready. Coral creates a Request whenever an Invocation requires approval. It records the exact logical
Tool arguments for review on the Run Status Page independently of the agent transport. Its deadline settles the
Invocation with `approval_expired`; denial settles it with `approval_declined`. The first durable terminal decision
wins. A later decision returns that result without changing it. If policy becomes `disabled`, Coral settles the
Invocation with `tool_disabled`; if it becomes `enabled`, Coral closes the Request and continues after its other
checks. A later Invocation can create a new request. The invoking User can also decide it through an Action Prompt in
their MCP client; Reef remains available for every Request. An active browser is not required, and existing Requests
survive a temporary Reef outage. _Avoid_: per-Invocation approval URL, Program pause payload

**Invocation Approval**: A human decision that satisfies an `approval_required` Tool Access State for one exact
Tool Invocation. It is bound to the Workspace, Provider and account-setting version, Invoking Principal, private
Provider Generation, Tool, Credential Route, Provider Account and authorization version when present, and arguments. A User approves their own
Invocation; any Workspace Owner can approve an Agent Invocation. An Agent never approves, and credential provisioning alone grants no approval authority. Coral retains the
approver Principal, decision, timestamp, and optional short comment. A later restrictive policy change still applies at
dispatch; the approval remains historical evidence and cannot authorize a later Invocation or change policy. _Avoid_:
Always enable, policy rule, reusable approval token

**Upstream Job**: An asynchronous process owned by an Upstream Service and started or observed through separate
Tools. Coral does not merge those Tools into a workflow or automatically poll the job. _Avoid_: Provider Job,
Coral workflow, Program Run

**Upstream MCP Task**: An Upstream Service-owned asynchronous invocation lifecycle required or offered by an MCP-backed
Tool. It is distinct from Coral's Program Run lifecycle. _Avoid_: Program Run, Coral task

**TypeScript-only Coral**: A Coral product contract in which Tools, Saved Functions, and Coral Programs are
presented and authored as TypeScript and SQL is absent from every layer. JavaScript remains valid TypeScript source, and
Coral Programs execute with JavaScript semantics. Type information guides authoring but does not override observed
upstream values or gate Program admission. _Avoid_: TypeScript facade, SQL hidden behind TypeScript, runtime type safety

**Coral TypeScript Profile**: The single explicit authoring and admission profile shared by Reef, Saved Function save,
MCP `exec`, and server-side Run compilation. It pins one official `typescript.js` version, compiler options, source-map
behavior, and the ECMAScript declaration libraries supported by Coral's embedded V8. It accepts JavaScript as TypeScript
source. Reef and Saved Function save report strict semantic diagnostics without making them admission failures. Saved
Function Save types each Tool and Saved Function that the source names from the declarations that the caller can
discover. V1 one-time Program admission parses and emits source but does not compute a full semantic check. The profile
exposes no DOM, Web Worker, Node, npm, filesystem, authored import, or module-resolution surface. _Avoid_: Runtime
TypeScript, client-selected compiler, Node TypeScript profile

**Program Authoring Limits**: The fixed v1 bounds applied before a Saved Function save or Program Run is accepted. A
Saved Function Path is at most 256 UTF-8 bytes and 16 segments. A submitted Program or one Saved Function source is at
most 64 KiB. One Run links at most 64 Saved Functions, and its submitted plus linked TypeScript source is at most 512
KiB. Its emitted JavaScript is at most 1 MiB and its source maps are at most 2 MiB. One Saved Function Declaration is at
most 256 KiB. One save or Run compilation has a two-second deadline. Exceeding a bound returns a safe error, creates no
partial Run, and leaves an existing Saved Function unchanged. _Avoid_: Deployment-selected authoring limit, unbounded
compiler input, partial save

**Program Diagnostic**: Bounded information about an authored TypeScript issue, Saved Function linking issue, or the
error that caused a Program Run to fail, intended to help the caller understand the cause and determine the appropriate
correction. Runtime diagnostics expose an existing error rather than a separate diagnostic execution. Authorized Run
readers may receive Program-controlled error messages and mapped authored stacks, including bounded cause chains and
grouped errors. Detailed runtime content is limited to information visible inside the
Program's isolate; Tool Errors retain their defined shape, and trusted Coral error codes remain distinguishable
from Program-controlled text. This content stays out of telemetry, and host internals and credentials remain private.
A diagnostic makes no claim that repeating a Run is safe. Syntax and linking failures identify the authored Program or
Saved Function with a line, column, and clear message. Runtime
stacks map emitted JavaScript locations back to authored TypeScript and hide generated wrappers, emitted JavaScript,
compiler internals, and V8 frames. Reef can show advisory semantic diagnostics without making them Run failures. Emitted
JavaScript remains private diagnostic data rather than a product authoring view. _Avoid_: Raw V8 stack, generated
wrapper location, emitted JavaScript display

**Schema Type**: A TypeScript declaration derived from a Tool's input or output schema for authoring guidance. It
does not imply runtime TypeScript type checking, and disagreement with an observed upstream value does not prevent a
Coral Program from handling that value. _Avoid_: Runtime type, executable type

**Agent-owned Pagination**: The calling agent reads continuation state from a Tool Result and explicitly
invokes each subsequent page. Coral returns one provider response per invocation and does not automatically traverse or
combine pages. _Avoid_: Transparent pagination, automatic pagination

**Execution Binding**: The upstream-specific invocation recipe attached to a Tool behind Coral's common Tool
interface. A v1 binding describes HTTP, MCP, or a Built-in Provider Tool without exposing routing or credential details to
Programs. _Avoid_:
Execution attachment, client transport, File Collection binding

**Upstream Outcome**: A complete upstream result received for one Tool Invocation and represented according to its
Execution Binding. Coral retains it whole, whether the Program receives it as a Tool Result or through an
Tool Error. _Avoid_: Provider Outcome, Upstream Error, Coral Failure, normalized response

**Declared Success**: An Upstream Outcome that its Tool declares as a success, and the only one a Program receives
as a Tool Result. A body that misses its schema is still a Declared Success. _Avoid_: ok result, upstream success
state

**Coral Failure**: A safe, coded inability to produce a complete Upstream Outcome for one Tool Invocation. It
excludes Run-level termination and unexpected host defects and makes no public claim about dispatch, external effects,
or retry safety in v1. When Coral can identify an invalid argument, an `invalid_input` failure names it and gives a
Coral-written reason; otherwise it may give only a general message. It never repeats a submitted value. _Avoid_:
Upstream failure, transport failure, Program failure

**Upstream Result**: The Upstream Service-owned value and safe upstream metadata carried by an Upstream Outcome. Coral
does not reshape the value into normalized rows or make upstream schema conformance a condition of success. _Avoid_:
Provider-native Result, row projection, normalized query result

**Tool Result**: The caller-facing projection of one Declared Success: `{ status, data, metadata }` for HTTP, and
`{ data, metadata }` for MCP. Its mandatory metadata carries safe execution-specific response facts. _Avoid_: Program
outcome, failure union, Upstream Error

**Tool Error (`ToolError`)**: The non-constructible JavaScript `Error` subclass, `coral.ToolError`, that
rejects a Tool call that produced no Declared Success. When Coral rejects an Upstream Outcome, the error carries
it. For a Coral Failure it carries only a stable code and an optional trace identifier, and makes no claim about whether
the call took effect. _Avoid_: `OperationError`, Upstream Error, Program failure, provider response

**MCP Content Block (`McpContentBlock`)**: One ordered member of an MCP-backed Tool's content sequence,
distinguished by its content type. Binary image, audio, and embedded-resource payloads carry their bounded
provider-native base64 data inline. _Avoid_: Return value, attachment

**MCP Tool Result**: The complete result of one upstream MCP tool call, retaining its ordered MCP Content Blocks
separately from its tool-typed structured content and metadata. An explicit `isError: true` is not a Declared Success,
so the Program receives the result through a Tool Error. _Avoid_: Flattened tool result, preferred structured
content

**HTTP Tool Result**: The Tool Result of one HTTP Declared Success, which carries its status beside its data.
A Tool whose schema Coral cannot type declares one untyped result in full.

**MCP Resource Reference (`McpResourceRef`)**: An opaque, Provider-bound reference created from an upstream MCP resource
link. Reading it is a separate Provider Tool; Coral never auto-fetches it. _Avoid_: Raw resource URI, Binary Value

**Saved Function Path**: The Principal-owned dotted JavaScript path relative to `coral.functions` that names one Saved
Function, such as `internal.finance.calculateTax`. Each segment is a valid JavaScript path segment, and Coral rejects
reserved internal segments. Parent paths are virtual namespaces rather than folders or separate resources. The complete
path identifies one current definition for that Principal in one Workspace. _Avoid_: Filesystem path, public package
name, Saved Function version

**Saved Function Save**: The atomic creation or replacement of one current Saved Function at an exact Saved Function
Path. `save_function` supplies the path, explicit description, and source; it creates an absent path and replaces an
existing path. Only the owning Principal can save or replace it. The latest accepted save wins. It replaces the source,
description, declaration, and diagnostics together, while failure leaves the current function unchanged. MCP exposes
non-executing `save_function` and `delete_function` authoring tools with required Task and intent attribution. Reef uses
a private first-party service for the same behavior. These surfaces do not execute code, and MCP `exec` remains the sole
public execution entry point. A Workspace Owner can list, inspect, and delete an Agent-owned Saved Function but cannot
save or replace one as the Agent. V1 exposes no public versions, edit token, or rename operation, and updating or
deleting a function cannot change an already accepted Run. _Avoid_: Draft save, partial publication, Saved Function
generation

**Saved Function Declaration**: The exact, self-contained TypeScript declaration emitted from one accepted Saved
Function source by Coral's pinned TypeScript compiler. It is the callable interface used by Universal Search, exact
description, Reef authoring, and Program admission. A Saved Function is an accepted callable rather than an editor
draft. Saving fails on syntax, declaration emission, forbidden capabilities, invalid exports, unresolved static Saved
Function paths, or authoring limits. Ordinary TypeScript semantic diagnostics remain advisory and are stored with the
accepted source. The declaration can use TypeScript interfaces that have no faithful JSON Schema projection. Coral
stores it atomically with the source that produced it and does not make JSON Schema the Saved Function's interface
authority. _Avoid_: Saved Function schema, hand-maintained declaration, runtime type

**Saved Function**: A reusable named TypeScript callable that a User or Agent saves at a Saved Function Path within one
Workspace under that Principal's `coral.functions` namespace. A User's current Saved Functions are private to that User.
A Workspace Owner can read the source, description, declaration, and diagnostics of an Agent-owned Saved Function and
can delete it, but cannot author code as the Agent. Disabling a Principal retains its current Saved Functions. Deleting
an Agent or removing a User from the Workspace removes that Principal's current Saved Functions without transferring
them; Administrative Lifecycle Events and retained Run source snapshots remain under their own retention rules. Its
source is a no-import TypeScript module with exactly one default-exported callable. Coral owns the external name and
call path; the module can contain local types, classes, constants, helper functions, and top-level initialization, but
it cannot use packages or Node APIs. Top-level initialization can use the normal Program Runtime Surface, including
Tools, Saved Functions, `coral.emit`, and `console`. It runs once for each accepted Run package before the
submitted Program body. Every resulting call, output, and failure uses the same attribution, gates, limits, deadline,
durability, and failure rules as the rest of the Run. Coral applies no special static effect analysis to this code, and
a linked module can invoke a Tool even when the submitted Program never calls its exported function. Its authored
TypeScript is its behavior authority, and its Saved Function Declaration is its interface authority. It is not a
Provider Tool, can call several Tools or Saved Functions, and can expose TypeScript interfaces that are richer
than JSON-compatible data contracts. At Program admission Coral statically identifies literal Saved Function references,
rejects computed function paths, and links the exact current source closure into the Program compilation. V1 does not
preserve reverse-reference integrity when a Saved Function is later deleted; a missing source in the closure blocks a
later Run at admission. Coral does not publish the Saved Function as JSON Schema or a separately retained JavaScript
executable. Within an accepted Run it behaves as an ordinary linked synchronous or asynchronous JavaScript function: its
thrown errors are catchable, its Tool calls, emissions, logs, time, and resource use belong to the enclosing Run,
and it creates no separate durable Run or Invocation record. Its type information guides callers but does not create
runtime TypeScript semantics. _Avoid_: UDF, Provider Tool, Operation, tool, Saved Function JSON Schema, dynamic function lookup

**Coral Program**: Principal-authored TypeScript lowered to JavaScript and executed by a Coral-owned, secret-free
runtime against schema-typed Provider Tools under their Call Roots (`coral.providers` and `coral.workspace`) and Saved
Functions under `coral.functions`.
JavaScript remains valid source, and type information guides authoring without gating admission. It receives Tool
Results, catchable Tool Errors, `coral.emit`, and a bounded global `console`; Run cancellation and deadlines remain
host-owned and have no Program-visible signal. It receives no Workspace, Principal, Provider Account, policy, approval,
credential, connection URL, or connection-request data. A Tool Error can identify a safe condition such as a
required account, required approval, or disabled Tool, but it does not reveal authority configuration. A
Coral Program can reach Upstream Services only through host-mediated Tools. _Avoid_: Trusted local script,
arbitrary Node process, ambient authority

**Program Compiler**: The private host component that applies the Coral TypeScript Profile, resolves Saved Function
source, emits JavaScript and source maps, and produces safe Program Diagnostics. It keeps an official `typescript.js`
compiler warm in one or more compiler-only V8 isolates that receive only packaged compiler assets and virtual source
files. Compiler isolates expose no Program host binding or ambient authority and are recycled after memory growth or
replaced after a failure. Program execution never shares a compiler isolate; every accepted Run uses a fresh execution
isolate containing only emitted JavaScript and the Program Runtime Surface. _Avoid_: Runtime compiler, Node compiler
process, shared compiler and Program isolate

**Program Runtime Surface**: The secret-free ECMAScript environment in which V8 executes an accepted Program package. It
provides ordinary objects, arrays, classes, closures, generators, Promises, `async` and `await`, maps, sets, regular
expressions, dates, JSON, `Intl`, BigInt, Symbol, the bounded Coral `console`, and the `coral` host namespace. BigInt
and other non-Program Values can be used internally but cannot be emitted or returned as Program Values. It provides no
timers, DOM, browser, Node, Deno, npm, import, `require`, worker, subprocess, shared-memory, Atomics, filesystem,
environment, direct-network, credential, or WebAssembly surface. V8 disables code generation from strings, including
`eval`, `new Function`, and indirect access through `function.constructor`. _Avoid_: Browser sandbox, Node sandbox,
ambient standard library, dynamic code

**Program Admission**: The host step that accepts a submitted TypeScript Program only after Coral has resolved its
statically named Saved Functions, built the exact source closure, enforced capability and size rules, and emitted
executable JavaScript and source maps. Syntax, missing source, forbidden capability, emission, and limit failures return
safe diagnostics without creating a Program Run. V1 does not compute ordinary TypeScript semantic diagnostics during
one-time Program admission. After admission succeeds, later JavaScript or Tool failures belong to the durable
Program Run. _Avoid_: Runtime compilation, partial Run, semantic type gate

**Program Run**: The higher-level Workspace record for one accepted execution of a Coral Program; MCP work associates
it with a Task, while standalone Reef execution does not require one. It stores the exact accepted Program source, the
identities and exact source snapshots of linked Saved Functions, and enough private compiler output and provenance to
retain the code that V8 executed. Every new Run compiles its submitted source and linked Saved Function closure with Coral's currently pinned TypeScript profile, even when a function was saved under an
older profile. Coral stores the selected profile and emitted output with the Run and does not retain or select old
compilers per Saved Function. The Run identity identifies this stored source and executable package; v1 adds no separate
source or executable digest. A later profile update can affect future Runs but cannot change an accepted Run.
Immediately before Program execution starts, Coral rechecks the current Organization Membership, Workspace, Principal,
Agent, and Agent Registration authority. Authority loss after acceptance but before execution starts fails the Run
without executing the Program or creating a Tool Invocation. After execution starts, authority loss blocks later
upstream dispatch but does not stop secret-free computation. The accepted Run fixes its linked Saved Function code but
does not fix one Provider generation for all later Tool calls; each Tool Invocation keeps selecting the active
generation when that call starts. Child Tool Invocations reference this stored Program context. Authorized human
Run detail can show the exact submitted Program source, linked Saved Function sources, and logical Invocation arguments,
but not credentials, generated authentication fields, or internal host values. The Run is attributed to its Invoking
Principal but is not owned by that Principal. Sharing a Workspace shares Providers and policy, not Run
history. An authenticated Agent can observe and cancel only its own accepted Run by Run identifier and cursor. It cannot
list Run history or access another Principal's Run. A User can list, inspect, and cancel Runs invoked by that User. A
Workspace Owner can inspect and cancel Runs invoked by the Owner's User and by Workspace-owned Agents. The
Owner receives no access to another User's Runs. Other Members cannot inspect another Principal's Runs. An Organization
Owner without a Workspace Membership sees no Runs there. Credential provisioning alone grants no Run access.
Loss of Workspace access stops future reads but does not delete history. The Run is identified independently of the
request used to start or observe it. Caller disconnection does not end the Run; completion, explicit cancellation, or
Coral policy does. A handled Tool Error does not fail the Run, while an unhandled one does, including one that
carries an Upstream Outcome. _Avoid_: Program request, connection-scoped execution, Principal-owned Run

**Run identifier**: The name Coral gives a Program Run when it accepts it: four words from Coral's word list, lowercase
and joined by hyphens, such as `height-word-database-library`. Coral draws the words at random, inserts the Run with
them as its primary key, and draws again when the name is already in use, so no two Runs in one deployment share one. A
Run accepted before Coral issued words keeps its UUID text. The identifier names the Run in MCP, in gRPC resource
names, in the Run Status Page URL and `run_status_url`, and in logs. It grants no authority: a Principal who guesses it
sees nothing more than for a Run that does not exist. _Avoid_: run id, run key, run handle, run alias, short
identifier, agent identifier

**Program Run Event**: An immutable, sequenced observation recorded during a Coral Program Run for incremental
consumption and audit. It describes Run execution, including cancellation and recovery effects, but does not duplicate
control-plane, authoring, or approval decisions recorded as Administrative Lifecycle Events. Run Events are not replay
checkpoints. _Avoid_: Administrative Lifecycle Event, stream chunk, workflow checkpoint

**Coral Instance**: One running Coral server process, identified by a random id it generates each time it starts. The
Coral Instance that holds a Run's claim owns that Run. While the claim's lease is live, no other Coral Instance takes
the Run over. Any Coral Instance may receive an outside Run Request for the Run. _Avoid_: replica, node, worker, pod,
process

**Run Owner**: The single writer of one Coral Program Run while the Run is not terminal. It exists from Run acceptance
until the Run ends, receives every Run Request, Invocation outcome, deadline and wake-up for that Run in one order, and
alone appends Program Run Events and the Run-scoped records they describe. It decides the Run's lifecycle and keeps its
record durable; it does not call upstream or handle credential material, which the Tool Invocations it starts do
outside it. A requester outside the Run acts on an outcome only once its Run Request is Recorded; the Program may
continue on an Applied one. A repeat of the same Run Request receives the recorded answer. Outside Run Requests reach
it through the Run Inbox. Workspace- and Provider-scoped changes reach it only as wake-ups. When a Run's worker lease
lapses and no Run Owner renews it, Coral recreates its Run Owner. The recreated owner records any deadline that
passed, applies the requests in the Run's Run Inbox, records the interruption or the termination already decided, and
retires. Coral never resumes a Run. _Avoid_: Run actor, claim holder, lock holder, sweeper, external writer

**Run Ledger**: The durable record of one Coral Program Run that its Run Owner appends to in chunks: the Program Run
Events, the Run-scoped records they describe, and the Run's head. Every chunk advances the head by one and commits only
if it extends the head it was built on, so the Ledger has one writer and one order. The Run Owner commits a chunk
whenever none is in flight and something is pending, so a chunk holds what arrived during the previous commit, up to a
fixed size. An uncertain commit is resolved by reading the head again. _Avoid_: event log, stream, journal, checkpoint

**Run Request**: A message that asks the Run Owner of one Coral Program Run to decide something about that Run: start an
Tool Invocation, publish a Program Emission or Program Log, finish the Run, decide an Approval Request, or cancel.
A Run Request passes through three stages: **Applied**, when the Run Owner has decided it in memory; **Recorded**, when
that decision is durable; and **Settled**, when what it started has ended. The Program shares the Run Owner's fate and
continues on an Applied answer; every later effect of the Run is recorded only after that answer is. A requester outside
the Run acts on an answer only once it is Recorded. A Run Request that decides something is identified by the thing it
decides (the Approval Request, the Run, the Invocation), so the first request wins and a repeat receives the recorded
answer. An Emission or Log has no identity and no repeat: the Program shares the Run Owner's fate, so it never sends one
twice. When the output the Run Owner has applied but not yet recorded exceeds the **Applied cap**, a Program request
answers at Recorded instead, so the output a Run can lose or hold in memory stays bounded. Deadlines and changes outside
the Run reach the Run Owner as wake-ups, not Run Requests. _Avoid_: command, signal, mailbox message, request token,
idempotency key

**Run Inbox**: The durable place where an outside Run Request (a decision on an Approval Request, or a cancellation)
waits for the Run Owner of its Run. Any Coral Instance may add a request to a Run's Run Inbox. While the Run is not
terminal, only its Run Owner removes it, in the commit that records it. A request is identified by the thing it
decides, so the first one wins. A Run Owner recovered after its lease lapsed applies the requests in the Run's Run
Inbox before it ends the Run. _Avoid_: mailbox, work queue, outbox, message queue

**Run Trace**: The Coral-owned trace that every span of one Coral Program Run belongs to. It starts at Run acceptance
and is separate from the request that started the Run, which it links to. _Avoid_: Run context, worker trace

**Run Output Budget**: The limit on the bytes of Program Emissions, Program Logs, Tool Invocation results, and the
Final Value that one Coral Program Run may produce. The Run Owner decides every request against the exact total when it
applies the request, so a Run never exceeds its budget by a byte. A refused Emission or Log is not Run output and the
Program observes the refusal; a refused Invocation result ends that Invocation with a Tool Error; a Final Value
that does not fit fails the Run. Tool arguments do not count. _Avoid_: approximate budget, soft limit, output
quota, bounded overshoot

**Program Emission**: A bounded, structured value that a Coral Program explicitly publishes during its Run. An Emission
is durable caller-facing output, does not suspend execution, and remains observable if the Run later fails. _Avoid_:
Yield, console output

**Program Log**: A diagnostic message produced through a Coral Program's console during its Run. A Program Log aids
observation but is not caller-facing Program output. _Avoid_: Emission, result

**Binary Value (`BinaryValue`)**: A bounded, transport-neutral inline representation of finite binary data in an
HTTP-backed Tool or Program Value. It carries validated base64 data and safe descriptive metadata but has no
durable identity, storage lifetime, or stable cross-Run reference; MCP-backed Tools keep provider-native content
blocks. _Avoid_: Artifact, file path, byte stream, stable cross-Run reference

**Program Value**: The bounded, interoperable JSON published when a Coral Program crosses a durable output boundary. A
Program can use any supported JavaScript value internally; native ECMAScript JSON property access, `toJSON`, omission,
ordering, and coercion semantics apply when `coral.emit` or the successful return value publishes output. A root
`undefined` return means there is no Final Value, while an Emission must produce interoperable JSON; serialization or
shared JSON-model failure produces no Program Value. Bounded Binary Values and provider-native MCP Content Blocks are
explicit tagged extensions that retain their binary forms. A Tool Error follows ordinary native serialization, so
a Program must project its safe fields to a plain value when it wants to publish them. _Avoid_: Treating an internal
JavaScript value as already durable, assuming every runtime value has a portable JSON form

**Final Value**: The optional Program Value produced when a Coral Program Run succeeds. It is distinct from the Run's
Emissions and Program Logs. _Avoid_: Transcript, accumulated output

**Run Update**: An observation of a Coral Program Run containing its current state, a current Principal-filtered
`required_actions` list, a bounded ordered batch of authored Program Emissions and Program Logs after an opaque cursor,
and the next cursor. Automatic execution Events remain in authorized human history, not Run Updates. A terminal Run's
outcome appears on the final output page; `has_more` means authored records or terminal details remain to be delivered.
The required actions are a current snapshot, not a Run state or Program Run Events. Several can exist for
concurrent Invocations. Each visible Request appears once, even if several Invocations wait on it. Required actions
identify an account connection or Invocation approval, its required actor, deadline, opaque non-authorizing request
identifier, and stable non-secret Run Status Page URL. An Account Connection Request appears only to its invoking User.
An Agent can see that its own Run requires a Workspace Owner action, but cannot inspect or resolve the request. A
terminal Run has no required actions. The cursor identifies authored-record delivery progress and the last visible
required-action revision; internal execution bookkeeping alone is not a meaningful update. Adapters may project a Run
Update into an MCP tool result or HTTP response without changing its semantics. _Avoid_: Full execution history,
stream chunk, transport result, treating a required action as Run state or a Program Run Event
