# OAuth approval page

Edit `approval.html`, `workspace_choices.html`, `no_workspace.html` and
`approval.css` directly. The pages share the stylesheet and its CSP hash.
`confirmation.rs` embeds these hand-authored files in Coral's authorization
listener. No frontend build, Reef server, JavaScript, bundled fonts, or external
assets are required. The page uses system fonts and the browser's light/dark preference.
The Coral mark is embedded inline from `apps/reef/public/coral-light.svg`; it is
the page's only artwork. Client-provided artwork is neither fetched nor treated
as verified branding. The left pane says who asks and where the code goes; the
right pane holds the decision. Below 760px they stack, with the details last. Before sign-in, the approval
page shows no private Workspace or User data.

At the Organization MCP Endpoint (`/mcp`) the approval page shows after the
identity provider answers instead of before. Its `choices` slot then holds
`workspace_choices.html`, the User's active Workspaces, and the form posts to
`/oauth/workspace` (`../workspace_choice.rs`). For a Workspace URL, `choices` is
empty and the form posts to `/oauth/authorize`. With no Workspace,
`no_workspace.html` shows instead and no code is issued.

Rust computes the CSP hash from the embedded CSS once. Keep `<style>@@styles@@</style>`
without extra whitespace inside the element so the hash matches the served bytes.
The approval template uses only `styles`, `client_name`, `client_identity_label`,
`client_identity`, `redirect_label`, `redirect_destination`, `access_scope`,
`decision_title`, `decision_detail`, `form_action`, `choices`, and `ticket` slots. The Workspace list uses only
`options`, which Rust builds from escaped Workspace names, and `signed_in_as`.
The empty page uses `styles` and `coral_link`.
Rust escapes dynamic values and substitutes slots in one pass; substituted
client text is never a template.
Keep Cancel first in each form so implicit submission does not approve access.
The Allow button retains the `continue` form value. The Workspace radios are
`required` and none is checked, so Cancel carries `formnovalidate`.

Rust authorization tests cover escaping, the CSP, browser binding, and decisions.
An optional Chromium check uses Reef's existing developer dependencies, but
neither the page nor its build depends on them:

```sh
cd apps/reef
node --test scripts/oauth-approval.test.ts
```
