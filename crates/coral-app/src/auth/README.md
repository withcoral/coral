# Public Principal authentication

The Workspace MCP endpoint is exactly `/workspaces/{workspace_id}/mcp`.
Its URL fixes the Workspace, not the Principal or authority. The Organization
MCP Endpoint is exactly `/mcp`; there the credential fixes the Workspace (see
[below](#organization-mcp-endpoint)).

- Auth-disabled serving is loopback-only. Missing credentials map to the implicit
  local Organization Owner through normal domain authorization. A presented
  credential is never ignored: it must authenticate an Agent Registration.
- Remote User clients use Coral's OAuth authorization server, backed by the
  configured OIDC provider. Coral-issued User tokens are bound to the exact
  Workspace resource URL. Protected-resource metadata and the authentication
  challenge advertise that URL and the configured authorization server.
- Agents present their Registration bearer credential at the same endpoint.
  Agent-kind JWT claims are not Registration authentication.

`coral-auth::PrincipalAuthentication` owns credential verification into canonical
Principal evidence, including private JWT checks and current authority lookup.
`WorkspaceAuthentication` is the thin MCP Adapter: it supplies the exact resource
URL and projects safe errors and OAuth metadata. Bootstrap supplies resolved
signing configuration and authority Modules. Login HTTP orchestration remains here.

Every HTTP request verifies its credential again. MCP sessions are partitioned
by Workspace and canonical Principal; session IDs and `clientInfo` cannot select
another actor. Domain Modules read current Coral-owned Organization Membership
or Agent/Registration state rather than trusting a session's retained role.

## Organization MCP Endpoint

`/mcp` serves the Workspace its credential names, for clients that can store
only one fixed URL ([ADR 0025](../../../../docs/adr/0025-fixed-mcp-endpoint-takes-its-workspace-from-the-credential.md)).

- A User token for `/mcp` has `aud = {listeners.mcp.public_url}/mcp` and a
  signed `workspace_id`. It does not work at a Workspace MCP endpoint, and a
  Workspace MCP token does not work at `/mcp`. `auth.allowed_audiences` cannot
  name `/mcp`, and `coral_audience` cannot be redeemed for it.
- The sign-in goes to the identity provider first. After the callback, the
  approval page also lists the User's active Workspace Memberships, and Coral
  issues the code only when they allow the client with one picked. So the
  approval comes after sign-in at `/mcp`, and a trusted client sees it too. The
  pending approval is sealed into the form and bound to the approval cookie, so
  nothing is stored before the pick. The submission rechecks the choice. With no Workspace, Coral shows a page that links to the web
  interface and issues no code. Nothing is preselected; the browser requires a
  pick before Allow, and the server refuses a submission without one.
- An Agent Registration credential works at `/mcp`. Coral finds the
  Registration by its verifier alone and serves its Workspace.
- Each request rechecks current authority. A lost Membership or an archived
  Workspace answers 401 with `error="invalid_token"` and the `/mcp` metadata
  challenge, so an OAuth client signs in again and the User can pick another.
  `serverInfo.title` names the Workspace.
- Protected-resource metadata is at `/.well-known/oauth-protected-resource/mcp`.
- Auth-disabled local mode serves `/mcp` to Agent credentials only. A request
  without a credential gets a 401 that points to the Workspace MCP URL.

## OAuth client registration: DCR and CIMD

An OAuth-capable User client can start with only a Workspace MCP URL. Protected-resource
metadata identifies Coral's authorization server. Authenticated deployments advertise
both `client_id_metadata_document_supported: true` and an absolute
`registration_endpoint` at `{listeners.auth.public_url}/oauth/register`.
Auth-disabled local mode serves neither User OAuth nor registration.

Two client identity paths share the same authorization flow:

- **CIMD:** the client supplies its canonical metadata-document URL as `client_id`.
  Coral retains hardened fetching, the operator's outbound destination policy,
  the five-second timeout, and the 5 KiB response bound. A document is published
  for every authorization server, so Coral drops redirect URIs it cannot use and
  fails only when none remain.
- **DCR (RFC 7591):** the client sends a JSON object to `POST /oauth/register` without an
  existing token, client ID, or initial access token. Coral validates and commits
  the registration before returning HTTP 201, an opaque `dcr_` client ID, and the
  actual accepted metadata. Responses use JSON and `Cache-Control: no-store`.
  Registration never fetches URLs. Only URL client IDs enter the CIMD fetcher;
  unknown or malformed opaque IDs fail closed.

OAuth client registrations describe deployment-level client software or instances.
They are not Users, Agents, Agent Registrations, Workspace Memberships, or access
credentials. They do not grant Workspace access. Store the returned client ID for
reuse; do not register again for each connection. Registrations survive Coral restart
on SQLite and Postgres and do not use the five-minute OAuth handshake lifetime.

### Accepted registration metadata

Coral supports only public clients and authorization-code grants with `code`
responses. Explicit confidential-client authentication methods fail. If the method
is omitted, Coral explicitly overrides RFC 7591's confidential default by returning
`token_endpoint_auth_method: "none"`. Omitted grants and response types use the RFC
`authorization_code` and `code` defaults. Requests containing those supported values
are accepted; the response contains only that supported subset. In particular,
Pi's `["authorization_code", "refresh_token"]` request is accepted without claiming
refresh-token support. A request with no usable supported flow fails.

- Supply 1–8 redirect URIs, each at most 2,048 UTF-8 bytes. Coral accepts HTTPS,
  explicit loopback HTTP, and native-app reverse-domain schemes such as
  `com.example.app:/oauth` (RFC 8252 §7.1). Other schemes, wildcards, credentials,
  fragments, control characters, and OAuth response-parameter collisions fail.
- Matching remains strict except for the existing HTTP loopback **port** variation.
  The actual requested redirect URI, including its port, binds the authorization code.
- An optional client name must be nonempty, trimmed, at most 255 UTF-8 bytes, and
  contain no control or bidirectional formatting characters. An absent name becomes
  `Unnamed OAuth client`. The consent page escapes names and identifies them as
  self-asserted. No supplied name, software identifier, metadata URL, or redirect
  origin acquires Reef's first-party identity or a trusted-client exception.
- Grant and response lists contain at most eight entries of at most 64 bytes each.
  Unrecognized metadata is ignored rather than stored. Coral does not fetch or render
  client logos, homepages, or other optional URLs. Requested scopes grant nothing;
  the accepted response explicitly returns an empty `scope`.
- No client secret, registration access token, access token, or refresh token is
  issued by registration. Malformed/unsupported requests return
  `invalid_client_metadata`; invalid redirect lists return `invalid_redirect_uri`.

Every new DCR client requires ordinary Coral consent, upstream OIDC sign-in, and
normal User/Workspace authorization. S256 PKCE remains mandatory. Code redemption
checks the exact client ID, actual redirect, verifier, and resource. A successful
exchange consumes the code once. Registration changes no access-token lifetime,
private first-party audience rule, or Agent Registration behavior.

### Anonymous admission and recovery

The registration body is limited to **5 KiB before parsing**, independently of
Content-Length. The deployment accepts at most **60 registrations per 60-second
fixed window** and retains at most **1,024 registrations**. One database admission
row serializes checks and inserts across concurrent requests and independent pools
on both supported databases. The rate clock and registry are durable; restarting
Coral or changing client identities cannot reset them. A backwards host clock does
not reopen the current window. There is no IP/header-derived exemption.

Throttling returns HTTP 429 with `temporarily_unavailable` and `Retry-After: 60`.
Wait before trying again. Full storage returns HTTP 503 with
`temporarily_unavailable`; it never silently evicts working registrations. Existing
IDs continue to resolve at capacity. Invalid requests and failed transactions leave
no partial registration.

Coral keeps no server state for a sign-in until the person signs in at the
identity provider, so anonymous authorization requests cost no storage and have
no shared budget to exhaust. The pending consent travels in the consent form and the sign-in
in a browser cookie. Both are sealed with AES-256-GCM under keys derived from the
session signing key. Authorization codes are stored in the database for five
minutes. Every Coral process that shares the signing key and the database can
therefore finish a sign-in that another process started and redeem its code, once.

At capacity, the operator must review registrations before reclaiming space. V1 has
no registration-management endpoint or UI. Stop every Coral process using the database,
back up the database, and remove only reviewed obsolete rows from `oauth_clients` by
exact `client_id` using the database's administration tools. Leave the singleton
`oauth_registration_admission` row intact. Capacity is counted from surviving rows;
restart does not require a counter repair. Removed clients must register and obtain
consent again. Do not delete User, Workspace, or Agent Registration records to recover
OAuth client capacity. Restoring the database also restores its registrations.

Keep registration bodies, callback queries, names, and credentials out of operational
logs. DCR emits no raw payload telemetry. Configure an ingress proxy's independent
connection limits for anonymous traffic without trusting arbitrary forwarding headers.

## Listener configuration

Normal runtime startup serves distinct MCP, auth-support, and private native
gRPC listeners. Reef runs as the separate Coral web HTTP process, not a route on
MCP or gRPC. Omitting the following sections selects ephemeral loopback binds;
it does not disable any listener. Startup failure tears down the already
started listeners rather than leaving a partial runtime. Local auth-support has
its own configurable loopback listener, with no User OAuth routes.

```toml
[listeners.grpc]
bind = "127.0.0.1:14555"

[listeners.mcp]
bind = "127.0.0.1:14556"

[listeners.auth]
bind = "127.0.0.1:9080"
```

With `[auth]`, both `listeners.mcp.public_url` and `listeners.auth.public_url`
are required. They name the public MCP origin and Coral OAuth issuer,
independently of the bind addresses. Without `[auth]`, auth-support must bind
loopback and cannot advertise a public OAuth URL. Authentication policy remains
under `[auth]`, `[auth.session]`, and `[auth.provider]`; manual client-consent
exceptions are `auth.trusted_clients`.

## Hosted web interface

Declare the operator-controlled web interface once in Coral's configuration:

```toml
[listeners.web]
public_url = "https://coral.example.com"
```

With `[auth]` configured, Coral registers this origin as a web resource and
trusts its exact `/.well-known/oauth-client` client ID when it returns to the
derived `/auth/callback` login redirect. Only that pairing skips Coral's client
consent screen, and it must use the web origin as its resource. The same client
ID returning to any other redirect its metadata document names is an ordinary
client request: Coral checks the redirect against the document and the resource
against the registered resources, and the user confirms it. Reef makes such a
request with its Provider Setup callback when a workspace adds another
workspace's MCP server on the same deployment. Listing the web client ID in
`auth.trusted_clients` would skip consent on every callback, so do not repeat
it there. Metadata validation, PKCE, upstream sign-in, Organization admission,
and normal domain authorization still run on both paths. Other clients,
including clients on the same host, still require consent unless independently
listed in `auth.trusted_clients`. Do not repeat the web origin in
`auth.allowed_audiences` either; both settings remain available for other
explicit registrations.

For hosted web-client registration, `public_url` must be an HTTPS origin with
no path, query, credentials, or fragment. It may use a private-network hostname.
The web process must configure `CORAL_WEB_PUBLIC_URL` to the same origin. This
setting does not launch a web process or change listener binds.

The same origin's `/.well-known/oauth-client` is also the Client ID Metadata
Document Coral presents to upstream Provider authorization servers. Whether a
new automatic MCP Provider sign-in uses that document or Dynamic Client
Registration is governed by `outbound.mcp_oauth_client_bootstrap`; by default Coral
prefers the document only when the hostname resolves to public addresses. See
[Provider OAuth client identity](../../../../docs/development.md#provider-oauth-client-identity)
for the values, the split-DNS limitation and troubleshooting. Web and MCP may
share a public origin behind a reverse proxy while retaining separate internal
listeners. MCP uses this explicit web destination for absolute human-review links,
never the incoming Host header or the MCP origin.

Every served deployment must declare a human-interface destination. Explicit
loopback HTTP origins are accepted for navigation, but do not register an OAuth
audience or grant web-client trust. Loopback OAuth development must register its
audience/client separately.

Local Desktop omits `[listeners.web]` and `[auth]`. Its launcher passes the actual
loopback navigation-bridge origin through `CORAL_LOCAL_WEB_URL` before starting
Coral. Local web launchers can supply their actual Reef origin the same way.
This bootstrap-only variable is rejected with `[auth]` or `[listeners.web]`;
missing or invalid destinations fail startup instead of producing relative links.
Desktop uses the implicit local User and needs no client consent. Desktop must
be running on the browser's machine for its loopback Run links to work.

### Private web origins

Each HTTPS origin in `auth.allowed_audiences` (including the derived web
resource) authorizes fetching that origin's exact `/.well-known/oauth-client` metadata document over private
networks, including a tailnet. This is an operator-selected destination, not
an origin-wide or network-wide SSRF exception. Other client IDs retain public
URL and DNS restrictions; Workspace MCP origins do not grant this permission.
The fetch still verifies TLS, rejects redirects, and enforces the five-second
request timeout and 5 KiB response limit. `trusted_clients` remains a separate
choice to skip the login approval page for other clients. An additional audience
alone does not imply client trust, and client trust grants no Principal authority.

## Organization admission and initial Owner

An eligible first OIDC login atomically provisions a stable Coral User and an
Organization Member. Set `auth.provider.bootstrap_owner_subject` to the exact
upstream `sub` **before that identity's first Coral login** to provision the
initial Organization Owner. The configured provider issuer must also match.
This is an operator selection, not first-login-wins or an email/client claim.

Subsequent logins do not overwrite roles or re-enable disabled Users, even if
the bootstrap setting names them. Role changes and disablement belong to
`coral_workspace::OrganizationModule`, including last-enabled-Owner protection
and atomic Administrative Lifecycle Events. Reef controls are follow-up work.

For Google Workspace, an example provider section is:

```toml
[auth.provider]
issuer = "https://accounts.google.com"
client_id = "your-google-client-id"
client_secret_env = "GOOGLE_CLIENT_SECRET"
bootstrap_owner_subject = "the-initial-owners-exact-google-sub"

[auth.provider.required_claims]
hd = "example.com"
```

### Trusted profiles and Workspace email lookup

`auth.provider.name_claim`, `email_claim`, and `email_verified_claim` default to
`name`, `email`, and `email_verified`. They are read separately from the verified
ID token. The older `display_name_claim` remains private authentication metadata
and is not assumed to be a person's name. Profile refresh preserves the stable
Coral User, roles, and disabled state; out-of-order logins cannot restore an older
profile. Names and emails are mutable and non-unique, never identity or linking keys.

Coral adds no email-verification ceremony. Google Workspace admission uses the
signed `hd` claim above, and email lookup uses the signed `email_verified` evidence.
Other providers' missing/false verification evidence prevents lookup, not otherwise
eligible login or existing access. Explicit operator-required admission claims
remain independent of this lookup policy.

Workspace Owners can see names and emails in People and retained Agent
creator attribution. Members can see themselves and the designated Owners only.
Exact email lookup is Owner-only in an active Workspace, trims surrounding whitespace,
and ignores ASCII case without alias rewriting. Multiple eligible matches return no
candidate list or grant. Confirmation rechecks the selected stable User and reviewed
profile evidence, ambiguity, eligibility, and current authority in the grant transaction.
A changed selection must be looked up and confirmed again; a committed request replay
never grants to a different User. Profiles can remain stale until the next login;
Coral does not synchronize an external directory or resolve account collisions.

### WorkOS and upstream access-token admission

WorkOS Connect returns identity claims in the ID token, but its selected
Organization is the `org_id` claim of the access token. Configure a confidential
Connect OAuth application (not the AuthKit environment's client ID) and opt in
to access-token admission to restrict the installation to one WorkOS Organization:

```toml
[auth.provider]
issuer = "https://your-environment.authkit.app"
client_id = "client_your_connect_application"
client_secret_env = "WORKOS_CONNECT_CLIENT_SECRET"
bootstrap_owner_subject = "user_the_initial_owner"

[auth.provider.access_token]
# With no resource indicator requested, WorkOS Connect uses the environment's
# client ID as its access-token audience, not the Connect application's ID.
audience = "client_your_authkit_environment"

[auth.provider.access_token.required_claims]
org_id = "org_your_organization"
```

This optional, provider-neutral policy requires an RS256 JWT access token from
that same code exchange. Coral verifies it with the configured provider's
JWKS, checks issuer, exact single audience, expiration/issuance/not-before times,
and the same subject as the independently verified ID token. Required
access-token claims use exact JSON equality. A missing, opaque, invalid, or
nonmatching access token fails login before User provisioning or code issuance.
The upstream access token is discarded, never persisted or forwarded to Reef or
MCP. Its roles do not become Coral roles. Without this policy, ordinary OIDC
sign-in still requires only a valid ID token; Google and other providers need
not issue JWT access tokens.

This is login admission, not external membership synchronization: removing a
WorkOS membership does not itself revoke an existing Coral session. Use Coral
Organization disablement to block current Coral authority.

Register `{listeners.auth.public_url}/auth/oidc/callback` as the callback
URL at the identity provider. Coral derives that URL; it is not a separate
`redirect_uri` setting. OIDC identity always uses the verified issuer and `sub`,
not a configurable Principal claim. Discovery metadata is cached for five minutes.

The browser that starts a sign-in receives a five-minute `coral_oauth_signin_*`
cookie, with the `__Host-` prefix on HTTPS. The cookie carries the sealed sign-in,
so only that browser can finish it. If the identity provider's sign-in link is
opened in another browser, the callback fails with `invalid_request` before Coral
contacts the identity provider. A proxy in front of Coral must pass this cookie through to the
callback.

- A client's `client_id`, `redirect_uri`, and `state` together may be at most
  1,536 bytes, so that the sign-in fits in one cookie. A longer request, or one
  whose sign-in does not fit for another reason such as a long resource, fails
  with `invalid_request` before the consent page.
- A new sign-in ends the browser's oldest sign-ins beyond three. For resources of
  at most 350 bytes, this keeps the sign-in cookies inside the 8 KB header limit of
  common proxies, unless the browser holds other large cookies for this host. Sign-ins started at
  the same moment can briefly exceed three until they finish or expire.
- Each sign-in issues at most one authorization code. A replayed callback gets an
  error and no code.
- Rotating the session signing key ends the sign-ins in progress. Start them again.

The remaining authorization-server, signing-key, and MCP listener settings are
still required. Google domain admission validates the signed `hd` claim; an
email suffix or login-screen hint is not sufficient. With Okta, configure its
OIDC issuer and application assignments, plus required claims when appropriate.

SAML and external membership synchronization are not implemented here. Removing
access at the identity provider does not immediately invalidate existing Coral
access. Coral-side disablement blocks subsequent authenticated requests and
later Run dispatches without deleting retained history or disabling Agents
merely because that User created or provisioned them. It does not retroactively
undo a fenced dispatch or introduce a new Run-termination policy.

## Listener health and private audiences

HTTP listeners expose unauthenticated `/healthz` (process liveness) and `/readyz`
(listener readiness). Private gRPC uses `grpc.health.v1.Health/Check`: the empty
service name is liveness and `coral.readiness` is private-listener readiness.
Readiness probes are bounded, cached, and coalesced before they reach the database.
Auth-support also requires composed login dependencies in authenticated mode.
Reef bounds its private gRPC readiness check and returns a safe HTTP 503 on failure;
its liveness stays healthy. Neither Workspace catalog state, upstream Provider
health, nor telemetry export decides listener readiness. Operators terminate public
TLS and isolate private gRPC on loopback or an internal network (optional operator
mTLS). No auth-disabled listener may bind off loopback.

A first-party User client redeems its normal OAuth authorization code and PKCE
verifier at `/oauth/token`, with the original `resource` and an optional
`coral_audience=mcp|reef` form parameter. This is a private first-party extension,
not token exchange or client-credentials authentication:

- Omit it for the unchanged public URL-audience token.
- `mcp` requires an exact resource in the configured Workspace MCP URL family.
  The signed token carries `aud=mcp` and that exact `workspace_id`.
- `reef` requires an explicitly registered resource in `auth.allowed_audiences`,
  not a Workspace MCP resource. The signed token carries `aud=reef`; each method
  still authorizes its requested Workspace. Configure only Reef resources in this
  explicit registry; MCP resources already belong to the Workspace URL family.
- An unrecognized or incompatible selection fails with `invalid_target`. The
  resource approved at login cannot be replaced at redemption. The code fixes
  the User and client identifier; a request cannot supply a Principal or role.

Private RPCs require `authorization: Bearer …` and `coral-audience: mcp|reef`.
The metadata selects the expected audience; it cannot override the signed one.
Public URL tokens do not authenticate private gRPC. MCP User tokens must contain
matching signed Workspace scope. Agents use their existing exact Workspace-bound
Registration credential, admitted only as `mcp`; Coral issues no second Agent or
workload credential. Local missing credentials select the implicit User, still
subject to method reachability and normal domain authorization. Malformed,
duplicated, or invalid credentials never fall back to local authority.
