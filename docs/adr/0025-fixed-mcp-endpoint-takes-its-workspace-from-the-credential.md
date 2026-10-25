---
status: accepted
---

# A fixed MCP endpoint takes its Workspace from the credential

Each deployment serves `https://<host>/mcp`, the Organization MCP Endpoint, beside `/workspaces/{workspace_id}/mcp`.
The fixed URL names no Workspace. A User picks one when they sign in, and Coral signs it into the token as
`workspace_id`. An Agent's Registration already fixes its Workspace. Before this, the endpoint always fixed the
Workspace; now the endpoint or the credential does, and nothing later in a session changes it.

## Why

ChatGPT and Claude list a plugin with one fixed MCP URL. A template URL that names a Workspace needs trusted-partner
status, which Coral does not have. A listing can point at the hosted deployment's `/mcp` and nothing else. Each
deployment holds one Organization, so the listing reaches only that deployment's people. Every deployment serves
`/mcp`, but choosing a deployment from a listing is out of scope.

## How the Workspace is chosen

- Coral's own sign-in pages show the picker after the identity provider's callback and before Coral issues the code.
  The choice is sealed to the browser like the consent page, so nothing is stored on the server before sign-in.
- The picker lists the User's active Workspace Memberships. It shows the page even for one Workspace, so the client is
  never bound somewhere the User did not see. With none, Coral issues no code and the page links to Reef to create a
  Workspace. A browser cookie preselects the last choice.
- Coral issues no refresh tokens, so each expiry runs sign-in and the picker again. To switch Workspace, the User signs
  in again. No MCP tool or argument switches it.
- `/mcp` names the chosen Workspace in its MCP `instructions`, for display only.

## Consequences

- A `/mcp` token carries `aud = https://<host>/mcp` and a signed `workspace_id`. It is refused at a Workspace MCP
  endpoint, and a Workspace MCP token is refused at `/mcp`. The private `coral_audience=mcp` redemption still needs an
  exact Workspace MCP resource.
- `/mcp` advertises protected-resource metadata at `/.well-known/oauth-protected-resource/mcp`, with
  `resource = https://<host>/mcp`. The authorization server accepts that resource only with a Workspace chosen at
  sign-in.
- Every request rechecks the Membership. When the Membership is gone or the Workspace is archived, `/mcp` answers 401
  `invalid_token` with the metadata challenge, so the client signs in again and the User can pick another Workspace.
  A 403 stays for a valid token denied one operation.
- An Agent credential at `/mcp` is found by its verifier alone, the one lookup not scoped by Workspace. The credential is
  a secret, not an ID, so the scoping rule's threat (a guessed or leaked ID reaching another Workspace) does not apply,
  and the Workspace it returns scopes every later read. The verifier column is already unique.
- In auth-disabled local mode, `/mcp` refuses a request without a credential and points to the Workspace MCP URL. The
  implicit local User has no sign-in to pick with. An Agent credential still works there.
- The Workspace MCP endpoint stays, with no deprecation. Clients key a connection by URL, so two Workspaces at once need
  two Workspace URLs. A Provider that reaches another Workspace names it by its URL. Local mode and existing configs
  use it too.
- Reef's server picks the URL Reef shows, so Reef keeps one build. With sign-in configured, the connect actions copy
  `/mcp`; in local mode they copy the Workspace MCP URL. The sidebar's Workspace menu always copies that Workspace's
  URL, labelled "Copy workspace MCP URL".

## Considered options

- **Wait for template URLs as a trusted partner.** Rejected: it blocks the listing on a status outside Coral's control.
- **Switch Workspace inside a session** with a tool or a `start_task` argument. Rejected: a session-selected Workspace
  would let a Program or client change the confidentiality boundary after authentication.
- **One token for both endpoint kinds.** Rejected: a token would then work at a URL its User never chose.
- **A default Workspace in local mode, or a loopback authorization server with the picker.** Not taken now. A default
  is a second way to select a Workspace. A local authorization server adds a signing key on disk and loopback attack
  surface, and a listing's hosted URL never reaches a Desktop anyway. Revisit if a local client needs `/mcp`.
