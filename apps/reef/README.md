# Reef

Reef is Coral's shared human interface, served on the web or packaged inside
Coral Desktop. Its npm package is `reef`; public deployment environment variables
use the `CORAL_WEB_*` prefix. Default cookies use the `coral_web_` namespace;
existing development sessions are not carried forward under the old cookie names.

## Local Development

For the complete authenticated Coral + Reef stack over your tailnet, follow the
[development setup guide](../../docs/development.md) and run `./dev setup` from
the repository root. It creates isolated, Git-ignored state and supports ordinary
terminals or a two-pane Herdr layout.

For UI-only work without hosted authentication, install dependencies:

```bash
npm install
```

Start the React Router development server:

```bash
npm run dev
```

Open `http://localhost:5173` unless the dev server prints a different URL.

Authentication is disabled by default for local development and for Coral
Desktop. No auth environment variables are needed for those modes.

Hosted Reef must choose its auth behavior explicitly. Set
`CORAL_WEB_AUTH_MODE=required` together with the server-only Coral authorization
issuer, Reef public URL, and session values documented in `.env.example`. Reef
derives its OAuth resource, client metadata URL, callback URL, and cookie
security from `CORAL_WEB_PUBLIC_URL`. A non-desktop production server without an
explicit mode fails closed rather than accidentally publishing an
unauthenticated app.

### Hosted authentication

`CORAL_WEB_PUBLIC_URL` is Reef's single public identity. It must be an origin without
a path, query, or fragment. Declare that HTTPS origin as Coral's first-party web
interface:

```toml
# Coral config.toml (alongside the normal [auth] configuration)
[listeners.auth]
bind = "127.0.0.1:9080"
public_url = "https://auth.coral.example.com"

[listeners.web]
public_url = "https://coral-web.example.com"
```

```bash
CORAL_WEB_AUTH_MODE=required
CORAL_WEB_PUBLIC_URL=https://coral-web.example.com
CORAL_WEB_AUTH_ISSUER=https://auth.coral.example.com
CORAL_WEB_SESSION_SECRET=replace-with-at-least-32-random-characters
CORAL_ENDPOINT=https://coral.internal.example.com
```

The `CORAL_WEB_PUBLIC_URL` value and Coral's `listeners.web.public_url` must canonicalize
to the same URL. Coral derives the accepted resource and trusted client from
that setting, so the first-party UI skips Coral's client-consent screen. No
separate `auth.allowed_audiences` or `trusted_clients` entry is needed for it.
The origin can be private (for example, a Tailscale Serve hostname): Coral
permits fetching only its exact `/.well-known/oauth-client` document over that
private network, with normal TLS verification, redirect rejection, and response
bounds. Reef derives the OAuth resource
`https://coral-web.example.com`, client ID
`https://coral-web.example.com/.well-known/oauth-client`, and callback
`https://coral-web.example.com/auth/callback` from it. Do not configure separate
client, resource, callback, or scope values. Coral pins that client's callback
and resource to those derived values; sign-in and domain permissions still apply.
Local Desktop needs no `[listeners.web]` setting: its UI and backend are application-managed
and local authentication uses the implicit User.

`CORAL_ENDPOINT` is only the Reef-to-Coral data-plane address and is required
whenever authentication is enabled. Reef never derives an authenticated Coral
destination from the browser request URL, `Host`, or forwarded-host headers.
Authenticated Reef uses native gRPC over HTTP/2 and attaches the server-held
bearer token. It does not use Coral's MCP HTTP endpoint or protected-resource
metadata for login or data access.

Choose one transport topology:

1. Use `CORAL_ENDPOINT=https://coral.internal.example.com` when TLS terminates
   at Coral or at a trusted proxy directly in front of it.
2. Use explicit-loopback HTTP, such as `http://127.0.0.1:14555` or bare
   `http://localhost:14555`, when Reef and Coral share a host. Acceptance of
   `localhost` trusts that host's name resolution; use a loopback IP when that
   trust is undesirable. Names such as `coral.localhost` are not treated as
   explicit loopback.
3. On a trusted private network without Reef-to-Coral TLS, set an `http://`
   endpoint and `CORAL_WEB_ALLOW_INSECURE_CORAL_ENDPOINT=true`. This sends bearer
   tokens in cleartext on that network and emits a process warning once per
   Coral origin.

The insecure endpoint flag relaxes only the Reef-to-Coral hop. It does not
relax `CORAL_WEB_PUBLIC_URL`, the OAuth issuer, browser-to-Reef transport, audience,
cookie, or callback validation.

`CORAL_WEB_SESSION_MAX_AGE_SECONDS` defaults to 3600 and caps how long Reef keeps a
login, even if Coral issues a longer-lived access token. A shorter token expiry
still wins. When the encrypted Reef session expires, or Coral rejects its token
with `UNAUTHENTICATED`, Reef clears the session and sends the browser through a
new login while preserving the requested path. There is no token refresh,
silent renewal, token revocation, or upstream IdP logout in this release;
signing out clears only the Reef session.

Over HTTPS, Reef names its session and CSRF cookies with the `__Host-` prefix:
`__Host-coral_web_session` and `__Host-coral_web_session_csrf` by default. The
browser stores a `__Host-` cookie only when it carries no `Domain` attribute, so
it drops a domain-scoped cookie of the same name that another host under the
same registrable domain tries to set. A sibling site cannot replace a User's
session with its own.
`CORAL_WEB_SESSION_COOKIE_NAME` sets the part after the prefix. Over
explicit-loopback HTTP the names carry no prefix, because browsers discard
prefixed cookies there. Upgrading a hosted deployment to these names ends every
existing session, and each User signs in again.

Live Activity updates reach the browser over server-sent event streams that
Reef heartbeats every 5 seconds. A proxy that terminates the browser's HTTP/2
connection, such as an AWS Application Load Balancer, can keep its own request
to Reef open after the browser has dropped a stream, and the heartbeats keep
that request past the proxy's idle timeout. Reef therefore ends every stream
after two minutes and the browser reconnects, so a stranded stream costs at
most that long. Give the proxy an idle timeout above 5 seconds and let it pass
`text/event-stream` responses through unbuffered.

#### Local end-to-end authentication

A public HTTPS tunnel is unnecessary when Reef, Coral's authorization server,
and the gRPC endpoint are all local. Use explicit loopback URLs for the complete
topology:

```toml
# Coral config.toml
[listeners.grpc]
bind = "127.0.0.1:14555"

[listeners.mcp]
bind = "127.0.0.1:14556"
public_url = "http://localhost:14556"

[listeners.auth]
bind = "127.0.0.1:9080"
public_url = "http://localhost:9080"

[auth]
allowed_audiences = ["http://localhost:5173"]
```

```bash
CORAL_WEB_AUTH_MODE=required
CORAL_WEB_PUBLIC_URL=http://localhost:5173
CORAL_WEB_AUTH_ISSUER=http://localhost:9080
CORAL_WEB_SESSION_SECRET=replace-with-at-least-32-random-characters
CORAL_ENDPOINT=http://127.0.0.1:14555
```

The remaining Coral `[auth.session]` and `[auth.provider]` settings are still
required. `listeners.auth.public_url` must equal `CORAL_WEB_AUTH_ISSUER`, and the provider callback must use that issuer's
origin. Plain HTTP is rejected if only one side is local, for hostname aliases,
or for a non-loopback address.

#### Troubleshooting login

Coral intentionally returns the same opaque `invalid_request` response for
client-metadata and redirect failures. Check Coral's server logs for the
sanitized resolution cause, then inspect Reef directly:

```bash
curl -i "$CORAL_WEB_PUBLIC_URL/.well-known/oauth-client"
```

The response must be a direct HTTP 200 JSON response no larger than 5 KiB. Its
`client_id` must exactly equal
`$CORAL_WEB_PUBLIC_URL/.well-known/oauth-client`; its callback must exactly equal
`$CORAL_WEB_PUBLIC_URL/auth/callback`; and it must advertise authorization code,
PKCE-compatible public-client metadata. Common failures are an unreachable or
TLS-invalid Reef URL, a redirect, a non-canonical client ID, an unexpected HTTP
status, oversized or invalid JSON, unsupported metadata, or a callback mismatch.
For loopback HTTP, Coral also requires the exact derived client ID to come from
the loopback `auth.allowed_audiences` entry and requires its own authorization
issuer to use explicit-loopback HTTP.

The approval form POST must carry one `Origin` header exactly equal to Coral's
configured `listeners.auth.public_url`. If approval fails or expires,
verify that the browser opens the configured public issuer and that a reverse
proxy preserves that origin instead of exposing an internal hostname.

Reef form actions (including sign out) allow the public host from
`CORAL_WEB_PUBLIC_URL` through React Router's origin check when authentication is
required. This supports TLS-terminating proxies whose backend URL is plain HTTP.
Development reads the value from `.env`; production sets it at server startup,
not image build time. Forwarded headers do not grant additional allowed origins,
and logout still validates its session-bound CSRF token. Restart the server after
changing the public URL.

Run checks:

```bash
npm run format:check
npm run lint -- --deny-warnings
npm run typecheck
npm test
npm run build
```

## Tracing

The production server traces each request when an OTLP endpoint is configured.
See [Trace Reef requests](../docs/reference/configuration.mdx) for the variables.
`npm run dev` opens no spans, because Vite's server does not run `server.js`.

## Recorded browser scenarios

See [Reel](e2e/README.md) for the disposable Coral + Reef stack,
Vitest/Playwright scenarios, film mode, video/trace review, and harness self-check.
Run it separately with `npm run test:e2e`; ordinary `npm test` remains fast.

## Storybook

Run the Wax component library locally:

```bash
npm run storybook
```

`npm test` also renders every story in headless Chromium and runs its `play`
function (`npm run test:stories` runs only the stories). Install the browser once with `npx playwright install chromium`. To
keep a story out of the test run, add `tags: ['!test']` with a comment that says
why.
