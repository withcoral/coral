# OAuth approval page

Edit `approval.html`, `workspace_choice.html`, `no_workspace.html` and
`approval.css` directly. The three pages share the stylesheet and its CSP hash.
`confirmation.rs` embeds these hand-authored files in Coral's authorization
listener. No frontend build, Reef server, JavaScript, bundled fonts, or external
assets are required. The page uses system fonts and the browser's light/dark preference.
The Coral mark is embedded inline from `apps/reef/public/coral-light.svg`.
The client and provider icons are intentionally generic. Client-provided artwork
is neither fetched nor treated as verified branding. The approval page shows no
private Workspace or User data before sign-in.

A sign-in at the Organization MCP Endpoint (`/mcp`) ends at
`workspace_choice.html` after the identity provider answers: it lists the User's
active Workspaces and posts the choice to `/oauth/workspace`
(`../workspace_choice.rs`). With no Workspace, `no_workspace.html` shows instead
and no code is issued. The approval page then asks for "one of your workspaces".

Rust computes the CSP hash from the embedded CSS once. Keep `<style>@@styles@@</style>`
without extra whitespace inside the element so the hash matches the served bytes.
The approval template uses only `styles`, `client_name`, `client_identity_label`,
`client_identity`, `redirect_label`, `redirect_destination`, `access_request`,
`providers_label`, and `ticket` slots. The picker uses `styles`, `client_name`,
`ticket`, and `options`, which Rust builds from escaped Workspace names. The
empty page uses `styles` and `coral_link`.
Rust escapes dynamic values and substitutes slots in one pass; substituted
client text is never a template.
Keep Cancel first in each form so implicit submission does not approve access.
The Allow button retains the `continue` form value; this presentation change does
not change the authorization protocol or move consent after sign-in.

Rust authorization tests cover escaping, the CSP, browser binding, and decisions.
An optional Chromium check uses Reef's existing developer dependencies, but
neither the page nor its build depends on them:

```sh
cd apps/reef
node --test scripts/oauth-approval.test.ts
```
