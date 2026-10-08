# Coral UI

Coral UI is the Coral frontend shell.

## Local Development

Install dependencies:

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

Hosted Coral UI must choose its auth behavior explicitly. Set
`CORAL_UI_AUTH_MODE=required` together with the server-only Coral authorization
issuer, Coral UI public URL, and session values documented in `.env.example`. Coral UI
derives its OAuth resource, client metadata URL, callback URL, and cookie
security from `CORAL_UI_PUBLIC_URL`. A non-desktop production server without an
explicit mode fails closed rather than accidentally publishing an
unauthenticated app.

### Hosted authentication

`CORAL_UI_PUBLIC_URL` is Coral UI's single public identity. It must be an origin without
a path, query, or fragment. Configure that exact origin as an accepted Coral
audience:

```toml
# Coral config.toml
[auth]
allowed_audiences = ["https://coral-ui.example.com"]
```

```bash
CORAL_UI_AUTH_MODE=required
CORAL_UI_PUBLIC_URL=https://coral-ui.example.com
CORAL_UI_AUTH_ISSUER=https://auth.coral.example.com
CORAL_UI_SESSION_SECRET=replace-with-at-least-32-random-characters
CORAL_ENDPOINT=https://coral.internal.example.com
```

The `CORAL_UI_PUBLIC_URL` value and Coral's `auth.allowed_audiences` entry must
canonicalize to the same URL. Coral UI derives the OAuth resource
`https://coral-ui.example.com`, client ID
`https://coral-ui.example.com/.well-known/oauth-client`, and callback
`https://coral-ui.example.com/auth/callback` from it. Do not configure separate
client, resource, callback, or scope values.

`CORAL_ENDPOINT` is only the Coral UI-to-Coral data-plane address and is required
whenever authentication is enabled. Coral UI never derives an authenticated Coral
destination from the browser request URL, `Host`, or forwarded-host headers.
Authenticated Coral UI uses native gRPC over HTTP/2 and attaches the server-held
bearer token. It does not use Coral's MCP HTTP endpoint or protected-resource
metadata for login or data access.

Choose one transport topology:

1. Use `CORAL_ENDPOINT=https://coral.internal.example.com` when TLS terminates
   at Coral or at a trusted proxy directly in front of it.
2. Use explicit-loopback HTTP, such as `http://127.0.0.1:14555` or bare
   `http://localhost:14555`, when Coral UI and Coral share a host. Acceptance of
   `localhost` trusts that host's name resolution; use a loopback IP when that
   trust is undesirable. Names such as `coral.localhost` are not treated as
   explicit loopback.
3. On a trusted private network without Coral UI-to-Coral TLS, set an `http://`
   endpoint and `CORAL_UI_ALLOW_INSECURE_CORAL_ENDPOINT=true`. This sends bearer
   tokens in cleartext on that network and emits a process warning once per
   Coral origin.

The insecure endpoint flag relaxes only the Coral UI-to-Coral hop. It does not
relax `CORAL_UI_PUBLIC_URL`, the OAuth issuer, browser-to-Coral UI transport, audience,
cookie, or callback validation.

`CORAL_UI_SESSION_MAX_AGE_SECONDS` defaults to 3600 and caps how long Coral UI keeps a
login, even if Coral issues a longer-lived access token. A shorter token expiry
still wins. When the encrypted Coral UI session expires, or Coral rejects its token
with `UNAUTHENTICATED`, Coral UI clears the session and sends the browser through a
new login while preserving the requested path. There is no token refresh,
silent renewal, token revocation, or upstream IdP logout in this release;
signing out clears only the Coral UI session.

#### Local end-to-end authentication

A public HTTPS tunnel is unnecessary when Coral UI, Coral's authorization server,
and the gRPC endpoint are all local. Use explicit loopback URLs for the complete
topology:

```toml
# Coral config.toml
[server]
bind_addr = "127.0.0.1:14555"

[auth]
http_bind_addr = "127.0.0.1:9080"
allowed_audiences = ["http://localhost:5173"]
```

```bash
CORAL_UI_AUTH_MODE=required
CORAL_UI_PUBLIC_URL=http://localhost:5173
CORAL_UI_AUTH_ISSUER=http://localhost:9080
CORAL_UI_SESSION_SECRET=replace-with-at-least-32-random-characters
CORAL_ENDPOINT=http://127.0.0.1:14555
```

The remaining Coral `[auth.session]`, `[auth.authorization_server]`, and
`[auth.provider]` settings are still required. The authorization-server issuer
must equal `CORAL_UI_AUTH_ISSUER`, and the provider callback must use that issuer's
origin. Plain HTTP is rejected if only one side is local, for hostname aliases,
or for a non-loopback address.

#### Troubleshooting login

Coral intentionally returns the same opaque `invalid_request` response for
client-metadata and redirect failures. Check Coral's server logs for the
sanitized resolution cause, then inspect Coral UI directly:

```bash
curl -i "$CORAL_UI_PUBLIC_URL/.well-known/oauth-client"
```

The response must be a direct HTTP 200 JSON response no larger than 5 KiB. Its
`client_id` must exactly equal
`$CORAL_UI_PUBLIC_URL/.well-known/oauth-client`; its callback must exactly equal
`$CORAL_UI_PUBLIC_URL/auth/callback`; and it must advertise authorization code,
PKCE-compatible public-client metadata. Common failures are an unreachable or
TLS-invalid Coral UI URL, a redirect, a non-canonical client ID, an unexpected HTTP
status, oversized or invalid JSON, unsupported metadata, or a callback mismatch.
For loopback HTTP, Coral also requires the exact derived client ID to come from
the loopback `auth.allowed_audiences` entry and requires its own authorization
issuer to use explicit-loopback HTTP.

The approval form POST must carry one `Origin` header exactly equal to Coral's
configured `auth.authorization_server.issuer`. If approval fails or expires,
verify that the browser opens the configured public issuer and that a reverse
proxy preserves that origin instead of exposing an internal hostname.

Run checks:

```bash
npm run format:check
npm run lint -- --deny-warnings
npm run typecheck
npm test
npm run build
```

## Storybook

Run the Wax component library locally:

```bash
npm run storybook
```
