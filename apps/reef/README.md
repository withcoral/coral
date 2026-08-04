# Reef

Reef is the Coral frontend shell.

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

Hosted Reef must choose its auth behavior explicitly. Set
`REEF_AUTH_MODE=required` together with the server-only Coral authorization
issuer, Reef public URL, and session values documented in `.env.example`. Reef
derives its OAuth resource, client metadata URL, callback URL, and cookie
security from `REEF_PUBLIC_URL`. A non-desktop production server without an
explicit mode fails closed rather than accidentally publishing an
unauthenticated app.

### Hosted authentication

`REEF_PUBLIC_URL` is Reef's single public identity. It must be an origin without
a path, query, or fragment. Configure that exact origin as an accepted Coral
audience:

```toml
# Coral config.toml
[auth]
allowed_audiences = ["https://reef.example.com"]
```

```bash
REEF_AUTH_MODE=required
REEF_PUBLIC_URL=https://reef.example.com
REEF_AUTH_ISSUER=https://auth.coral.example.com
REEF_SESSION_SECRET=replace-with-at-least-32-random-characters
CORAL_ENDPOINT=https://coral.internal.example.com
```

The `REEF_PUBLIC_URL` value and Coral's `auth.allowed_audiences` entry must
canonicalize to the same URL. Reef derives the OAuth resource
`https://reef.example.com`, client ID
`https://reef.example.com/.well-known/oauth-client`, and callback
`https://reef.example.com/auth/callback` from it. Do not configure separate
client, resource, callback, or scope values.

`CORAL_ENDPOINT` is only the data-plane address. Authenticated Reef calls it
with native gRPC over HTTP/2 and attaches the server-held bearer token. The
endpoint must use HTTPS, except for the explicit-loopback local topology below.
It is not an OAuth resource and does not need to match `REEF_PUBLIC_URL`.
Reef does not use Coral's MCP HTTP endpoint or protected-resource metadata for
login or data access.

`REEF_SESSION_MAX_AGE_SECONDS` defaults to 3600 and caps how long Reef keeps a
login, even if Coral issues a longer-lived access token. A shorter token expiry
still wins. When the encrypted Reef session expires, or Coral rejects its token
with `UNAUTHENTICATED`, Reef clears the session and sends the browser through a
new login while preserving the requested path. There is no token refresh,
silent renewal, token revocation, or upstream IdP logout in this release;
signing out clears only the Reef session.

#### Local end-to-end authentication

A public HTTPS tunnel is unnecessary when Reef, Coral's authorization server,
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
REEF_AUTH_MODE=required
REEF_PUBLIC_URL=http://localhost:5173
REEF_AUTH_ISSUER=http://localhost:9080
REEF_SESSION_SECRET=replace-with-at-least-32-random-characters
CORAL_ENDPOINT=http://127.0.0.1:14555
```

The remaining Coral `[auth.session]`, `[auth.authorization_server]`, and
`[auth.provider]` settings are still required. The authorization-server issuer
must equal `REEF_AUTH_ISSUER`, and the provider callback must use that issuer's
origin. Plain HTTP is rejected if only one side is local, for hostname aliases,
or for a non-loopback address.

#### Troubleshooting login

Coral intentionally returns the same opaque `invalid_request` response for
client-metadata and redirect failures. Check Coral's server logs for the
sanitized resolution cause, then inspect Reef directly:

```bash
curl -i "$REEF_PUBLIC_URL/.well-known/oauth-client"
```

The response must be a direct HTTP 200 JSON response no larger than 5 KiB. Its
`client_id` must exactly equal
`$REEF_PUBLIC_URL/.well-known/oauth-client`; its callback must exactly equal
`$REEF_PUBLIC_URL/auth/callback`; and it must advertise authorization code,
PKCE-compatible public-client metadata. Common failures are an unreachable or
TLS-invalid Reef URL, a redirect, a non-canonical client ID, an unexpected HTTP
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
