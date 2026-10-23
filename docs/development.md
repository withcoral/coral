# Developing Coral and Reef

This is the authenticated development setup. It runs the Coral backend
and Reef from your checkout, using WorkOS for sign-in and Tailscale Serve for
private HTTPS by default. It does not install a service or enable Funnel, but
preserves matching Funnel mappings you have explicitly enabled.

## Prerequisites

On the machine that runs the checkout:

- Linux or macOS (use WSL for Windows).
- Node from `apps/reef/.node-version`, or a newer patch/minor of that major, and npm.
- Git and Rust via rustup; Cargo uses `rust-toolchain.toml`. Install the native
  build tools required by your platform. Linux uses clang and mold as specified
  in `.cargo/config.toml`.
- Tailscale installed, running, and signed into your tailnet. Your browser's
  machine must also have access to this tailnet device unless you explicitly enable
  Funnel for the surfaces it needs.
- Access to the team's WorkOS staging environment, and an enabled User membership
  in the Organization you will configure. You need permission to create a
  Connect application, or a teammate must create it for you.

Herdr and Rex are optional. Docker and a locally managed Postgres are not
required; this setup uses a separate SQLite database.

## First setup

```bash
./dev setup
```

The wizard installs Reef's pinned npm dependencies, builds Coral, and then walks
you through:

1. Confirming your machine's Tailscale hostname and the WorkOS environment.
   Enter its AuthKit issuer, environment client ID, and Organization ID. The
   repository holds no defaults for them; a team keeps its own in a password manager.
2. Creating a **separate confidential Connect OAuth application** for your
   development machine: choose **Managed by you**, keep client-secret
   authentication, and register the exact callback the wizard prints.
3. Entering its client secret (hidden input) and selecting your own WorkOS
   `user_…` ID as this checkout's initial Coral Organization Owner.
4. Generating fresh signing/session keys and configuring Tailscale Serve. Choose
   sudo only if your Tailscale installation requires it. If Tailscale prints an
   HTTPS-enablement link, follow it and retry `./dev proxy` or `./dev proxy --sudo`.

The environment client ID from **Developer → API Keys** and the OAuth client ID
from **Connect** are different values. Both are needed. Keep the client secret
in your password manager; never send it through chat or commit it.

The same private ports are used on every developer's own machine:

| Surface | Loopback address | Tailnet HTTPS |
| --- | --- | --- |
| Reef | `127.0.0.1:5173` | `https://<machine>.<tailnet>.ts.net` |
| Coral auth | `127.0.0.1:9080` | same hostname, port `8443` |
| Coral MCP | `127.0.0.1:14556` | same hostname, port `10000` |
| Private gRPC | `127.0.0.1:14555` | not exposed |

There can be one running stack on these ports per machine. The runner refuses
occupied ports rather than killing processes. Setup reuses matching Serve or
Funnel mappings without changing their visibility, but refuses to replace another
service, path handler, or incomplete configuration. Missing mappings are always
created with private Serve. Run/proxy/doctor warn when existing mappings use Funnel.
Tailnet access controls should normally restrict the development server to trusted
contributors: Vite serves development source code, not just authenticated pages.

### Optional public access with Funnel

Funnel removes the tailnet access boundary: anyone on the internet can reach the
exposed port. Coral's authentication still applies to protected application routes,
but it does not protect Vite's development source. Only enable public access when
you accept that exposure; expose only the surfaces external clients need.

To explicitly make all three surfaces public:

```bash
tailscale funnel --bg --https=443 http://127.0.0.1:5173
tailscale funnel --bg --https=8443 http://127.0.0.1:9080
tailscale funnel --bg --https=10000 http://127.0.0.1:14556
```

Follow any Tailscale enablement prompts. These public ports are supported by Funnel;
the backend loopback ports do not change. `./dev proxy` never enables or disables
Funnel itself. Check visibility with `tailscale funnel status` and test public
reachability from a device disconnected from Tailscale. Never expose private gRPC.

If upgrading an older setup that used MCP HTTPS port `8444`, run `./dev setup` to
regenerate the advertised URL and create the `10000` mapping, then restart Coral
and update MCP clients to the new URL. After verifying it, remove only your old
`8444` mapping using `tailscale serve --bg --https=8444 off` (or `tailscale funnel`
with the same flags if it used Funnel). Do not reset all Tailscale mappings.

## Run

```bash
./dev run
```

Coral rebuilds through Cargo; Reef uses Vite hot reload. Output from both
processes appears in the terminal. The runner waits up to five minutes for
readiness and prints the web URL. Ctrl-C stops both process groups, including
npm/Vite and Cargo children. If either server exits, the other is stopped too.
For Rust changes, stop and rerun the command. Alternatively, use Bacon to rebuild
and restart just Coral automatically, with Reef in a separate terminal:

```bash
bacon coral
./dev reef  # In another terminal
```

`bacon coral` wraps `./dev coral`, using the same setup, credentials, readiness
checks, and process cleanup. It watches backend sources and dev scripts, not
runtime state under `.dev/`.

For left/right Herdr panes, start in **one ordinary shell pane in a fresh
workspace**, not a coding-agent pane:

```bash
/path/to/checkout/dev run --herdr
```

This keeps Coral in the left pane and starts Reef on the right without changing
focus. Each pane is independent: stop each with Ctrl-C. Restart with `./dev coral`
or `./dev reef`. The same two commands work in ordinary separate terminals.

For the same layout in Rex, start in **the only pane of a fresh tab**, an
ordinary shell rather than a coding-agent pane:

```bash
/path/to/checkout/dev run --rex
```

```bash
./dev doctor  # Check an already-running stack, including its HTTPS routes
./dev test    # Test the tools; requires Reef's npm dependencies, but no live IdP or tailnet
```

## Configuration and isolation

Everything private is under this checkout's Git-ignored `.dev/`:

- `setup.env`: saved wizard answers, including the Connect client secret.
- `state/config.toml` and `state/coral.db`: Coral configuration and isolated state.
- `secrets/`: per-checkout signing key and web session secret.
- `web.env`: server-only Reef configuration (`CORAL_WEB_*`).
- `settings.json`: the last applied non-secret setup identity.

`apps/reef/.env` is a relative symlink to `.dev/web.env`. Existing unrelated `.env`
files, development dotenv overrides, and unrecognized `.dev/` directories are
rejected rather than overwritten. If you already use a machine-local setup,
leave it alone or deliberately move its Reef env link aside before opting in.
These tools never read, migrate, or delete `~/.config/coral`.

Setup is rerunnable: Enter retains saved answers, and signing keys, session
secrets, and the database are preserved. Once the database exists, changing its
IdP, Organization, or initial Owner is refused. Use a fresh checkout for a fresh
installation; there is no automatic reset or migration. Edit settings through
`./dev setup`, then restart the servers.

`.dev/` is also excluded from Docker build contexts and explicitly denied by
Vite's filesystem server, including direct `@fs` requests. File permissions are
private to your OS user. Do not copy another developer's `.dev/` directory.

### Outbound provider access

Edit your deployment's `config.toml`, then restart Coral. Each field independently
accepts `deny`, `https`, or `http-and-https`:

```toml
[outbound]
loopback = "deny"
private_networks = "deny"
```

These are the uniform base defaults. The implicit-user CLI composition and Desktop
launcher supply `http-and-https` defaults for both categories. Explicit configuration
always wins, including `deny`. Omitted fields keep their composition defaults without
being saved as explicit selections. Ordinary server builders and authenticated
`./dev` deployments supply no permissive preset. Authentication never overwrites an
explicit outbound choice. Inbound auth-disabled listeners still require loopback.

```toml
# Authenticated HTTP localhost sidecar; private networks remain denied.
[outbound]
loopback = "http-and-https"
```

```toml
# Verified HTTPS internal providers; loopback remains denied.
[outbound]
private_networks = "https"
```

For an HTTP Docker-network sidecar such as `http://sidecar:5000`, select
`private_networks = "http-and-https"`. Private means RFC1918 IPv4 or unique-local IPv6,
including all DNS answers. Public addresses remain HTTPS-only. Metadata, link-local,
CGNAT (`100.64.0.0/10`) and other blocked special ranges stay blocked. Only explicit
localhost names and loopback literals can use loopback permission.

HTTP permission never disables HTTPS certificate verification. Enabling a category
trusts Provider administrators and imported Provider documents with that network
reachability, including unauthenticated internal services. Private HTTP can be
intercepted. This address policy is not network isolation or complete lateral-movement
prevention. The same resolved policy covers server-side setup, publication checks, HTTP/MCP
dispatch, redirects and OAuth fetches. There is no per-provider override or old-key alias.

Provider consent navigation is separate. Coral screens each initial HTTP consent URL
when it issues it, including reuse. Private HTTP hostnames must resolve exclusively to
private addresses in Coral's DNS lookup. Public HTTP destinations and blocked-special
literals are rejected; blocked-special literals are rejected under HTTPS too. Browser
DNS and later redirects can differ, so this is not connection-time enforcement.
`[outbound]` does not restrict browser consent transport: HTTP consent can coexist
with HTTPS-only discovery and token exchange. See
[ADR 0009](adr/0009-provider-consent-navigation.md) for the exact contract.

`./dev setup` regenerates `.dev/state/config.toml`. Reapply operator selections after
running it. See [ADR 0008](adr/0008-outbound-network-permissions.md) for the decision.

### Provider OAuth client identity

When a Provider's authorization server needs an OAuth client and nobody enters a
client ID, Coral establishes one automatically in one of two ways. This applies to
MCP providers only: MCP discovery is what reports document support and a registration
endpoint, so an OpenAPI provider needs a client ID you enter or its recipe pins. With
a Client ID Metadata Document, Coral's client ID is the URL `/.well-known/oauth-client` under
`listeners.web.public_url`, which the authorization server fetches. With Dynamic
Client Registration, Coral posts its details to the server's registration endpoint
and uses the client ID the server returns. Only an HTTPS `listeners.web.public_url`
on a deployment with `[auth]` configured publishes a document. Desktop and other local deployments publish none,
so they register dynamically when the server offers a registration endpoint and
otherwise need a client ID.

A server that advertises document support cannot necessarily fetch a document hosted
on a tailnet or an internal network. The consent then fails on the server's own page
after sign-in, where Coral cannot observe it. By default, when a new automatic
sign-in starts, Coral resolves the document's hostname once through the deployment's
normal resolver and prefers the document only when every returned address is public.
A private, loopback, CGNAT (`100.64.0.0/10`), link-local or other special-use answer
selects registration when the server offers it; so do an empty answer, a resolver
failure and a timeout, which Coral reports as unresolved rather than as private.
When the server offers only the document, the sign-in stops before the browser opens
and says why. The person can enter a client ID from their own registration, or you
can publish the document on a public address or select a policy below. An IP literal
or `localhost` origin is judged without DNS. Nothing is stored and nothing polls DNS.

Split DNS is the known limitation. A hostname can resolve to tailnet space on Coral's
host while public DNS, and the authorization server, see a public address, for
example a Tailscale Funnel origin that also resolves inside the tailnet. A non-public
answer therefore does not prove the server cannot fetch the document, and a public
answer does not prove it can, or that the document is valid. Edit your deployment's
`config.toml`, then restart Coral. The policy lives under `[outbound]` because it
shapes how Coral identifies itself to an upstream MCP authorization server. It does
not change what Coral may reach; `loopback` and `private_networks` govern that:

```toml
[outbound]
mcp_oauth_client_bootstrap = "prefer-client-id-metadata-document"
```

| Value                                           | New automatic clients                                                                                                                                                                                                                                       |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `automatic` (default)                           | The document when the server supports it and its hostname resolves to public addresses only; otherwise registration. Without registration, the sign-in stops with `CLIENT_METADATA_NOT_PUBLIC` or `CLIENT_METADATA_UNRESOLVED`. With neither a supported document nor a registration endpoint, the sign-in needs a client ID (`CLIENT_BOOTSTRAP_UNAVAILABLE`). |
| `prefer-client-id-metadata-document`            | The document whenever the server supports it, whatever the local resolver answers. For split DNS, and for servers with approved access to your internal network. Registration remains the choice for servers without document support.                   |
| `dynamic-client-registration-only`              | Registration or nothing. A server without a registration endpoint needs a client ID (`CLIENT_BOOTSTRAP_UNAVAILABLE`), and a failed registration is reported (`AUTHORIZATION_DISCOVERY_CLIENT_BOOTSTRAP_FAILED`) rather than replaced by the document. |

The policy governs only new automatic clients. A client ID someone enters, a client a
recipe or an OpenAPI document pins, and a registration Coral already holds for the
same callback are used as they are, and a working client is never rotated because a
DNS answer or a default changed. The policy does not relax URL validation, issuer
binding, callback checks, PKCE, consent, or the `[outbound]` destination policy.

Troubleshooting:

- The server shows an error such as "Unknown OAuth client" after consent and never
  returns to Coral: the server could not fetch the document. Confirm the sign-in used
  the document: the consent page's address carried `client_id=` with the document URL.
  Then publish the document on an address the server can reach, enter a client ID from
  a manual registration with the server, or rely on the default policy, which selects
  registration when the hostname resolves to non-public addresses.
- The sign-in stops with `CLIENT_METADATA_NOT_PUBLIC`: the hostname resolves to
  non-public addresses on Coral's host, or the origin is a non-public IP literal or
  `localhost`, which Coral judges without DNS, and the server offers no registration.
  If the server can reach the document anyway, select
  `prefer-client-id-metadata-document`.
- The sign-in stops with `CLIENT_METADATA_UNRESOLVED`: Coral's resolver could not
  resolve its own `public_url` hostname within a few seconds. Check the host's DNS
  configuration; `prefer-client-id-metadata-document` skips the lookup.

### Credential encryption key

Coral encrypts stored Provider Account credentials with one AES-256 key. By default
the runtime creates that key as `credentials.key` in its config directory on first
start and reads it from there afterwards. A deployment whose secrets live in a
secret manager names the environment variable that carries the key instead, so
the runtime never writes a key file. Edit your deployment's `config.toml`, then
restart Coral:

```toml
[credentials]
key_env = "CORAL_CREDENTIALS_KEY"
```

The variable holds the standard base64 encoding of 32 random bytes, the same
bytes a key file holds. Generate a new key with `openssl rand -base64 32`, or
move an existing deployment's key into the secret with `base64 < credentials.key`
and then remove the file. Coral refuses to start while a key file sits beside
`key_env`, because sealing new credentials under a different key from the one
the stored credentials need would only surface later as unreadable credentials.
A missing variable or one that does not decode to 32 bytes also refuses startup.
Losing the key makes every stored credential unreadable; back it up like the
database.

## Google sign-in through WorkOS

Google OAuth is configured **once per WorkOS environment**, not once per developer.
If Google displays `redirect_uri_mismatch`:

1. Open WorkOS **Authentication → OAuth providers → Google → Manage** and copy
   its Google **Redirect URI**.
2. Add that exact URI to the authorized redirects on the Google OAuth client
   configured in WorkOS.

That is **Google → WorkOS**, not the developer-specific **WorkOS → Coral** callback
ending in `/auth/oidc/callback`. Do not replace one with the other. See the
[WorkOS Google OAuth guide](https://workos.com/docs/integrations/google-oauth).

Successful sign-in still requires the configured WorkOS Organization and verified
identity. WorkOS roles do not become Coral roles. Coral's configured first-party
web client skips Coral's client-consent page when it signs a user in; its
Provider Setup flow and other MCP clients retain consent.
