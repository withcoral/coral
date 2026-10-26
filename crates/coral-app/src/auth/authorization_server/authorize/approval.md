# OAuth approval page

Edit `approval.html` and `approval.css` directly. Every sign-in page shares
the stylesheet and its CSP hash. `confirmation.rs` embeds these
hand-authored files in Coral's authorization listener. No frontend build, Reef
server, JavaScript, bundled fonts, or external assets are required. The page
uses system fonts and the browser's light/dark preference.
The Coral mark is embedded inline from `apps/reef/public/coral-light.svg`. The
client's icon is a generic one: client-provided artwork is neither fetched nor
treated as verified branding. The left pane says what the client asks for and
where the code goes; the right pane holds the decision. Below 800px they stack,
with the decision right after the heading.
Before sign-in, the approval page shows no private Workspace or User data.

At the Organization MCP Endpoint (`/mcp`) the approval page shows after the
identity provider answers instead of before. `account` then names the signed-in
account, `choices` holds a card per active Workspace with its Providers, and
the form posts to `/oauth/workspace` (`../workspace_choice.rs`). For a
Workspace URL both are empty and the form posts to `/oauth/authorize`. With no
Workspace, `choices` says how to get one and links to Coral when its URL is
known. `cancel_label` then reads "Back to …" and `connect_hidden` hides Connect
and its notice, so the only way on is Coral or back to the client, which gets
`access_denied`. No code is issued.

Rust computes the CSP hash from the embedded CSS once. Keep `<style>@@styles@@</style>`
without extra whitespace inside the element so the hash matches the served bytes.
The approval template uses only `styles`, `client_name`, `client_identity`,
`redirect_label`, `redirect_destination`, `access_scope`, `decision_title`,
`form_action`, `account`, `choices`, `cancel_label`, `connect_hidden`, and
`ticket` slots.
Rust escapes dynamic values and substitutes slots in one pass; substituted
client text is never a template.
Keep Cancel the first submit button in the form so implicit submission does not
approve access; `order` places it beside Connect. Every Connect button keeps
the `continue` form value. Each Workspace card is followed by its own
"Connect to …" button, and the stylesheet shows only the picked card's, so the
button names the Workspace without a script. The radios are `required` and none
is checked, so Cancel carries `formnovalidate`.

Rust authorization tests cover escaping, the CSP, browser binding, and decisions.
An optional Chromium check uses Reef's existing developer dependencies, but
neither the page nor its build depends on them:

```sh
cd apps/reef
node --test scripts/oauth-approval.test.ts
```
