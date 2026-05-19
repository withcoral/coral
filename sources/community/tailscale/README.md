# Tailscale Community Source

Query Tailscale devices, users, keys, routes, and DNS settings through Coral SQL
using the [Tailscale API](https://tailscale.com/api).

## Setup

### 1. Create a Tailscale API token

Create an API access token from the Tailscale admin console under
**Settings > Keys**.

For longer-lived automation, use a Tailscale trust credential to mint scoped
access tokens. Grant only the scopes needed for the tables you plan to query:

- `devices:core:read` for `devices`
- `users:read` for `users`
- `auth_keys:read`, `api_access_tokens:read`, `oauth_keys:read`, or
  `federated_keys:read` for `keys`
- `devices:routes:read` for `device_routes`
- `dns:read` for `dns_configuration`

### 2. Add the source

```bash
export TAILSCALE_API_TOKEN="<your-token>"
export TAILSCALE_TAILNET="-" # or your tailnet name/ID
coral source add --file sources/community/tailscale/manifest.yaml
```

### 3. Verify

```bash
coral source test tailscale
```

The default test query reads `tailscale.devices`, so the token must be able to
list devices in the configured tailnet.

## Tables

### `tailscale.devices`

Lists devices in the configured tailnet.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Legacy device ID |
| `node_id` | Utf8 | Preferred device ID for API calls |
| `user` | Utf8 | User who registered the device |
| `name` | Utf8 | MagicDNS name |
| `hostname` | Utf8 | Machine name |
| `client_version` | Utf8 | Tailscale client version |
| `os` | Utf8 | Device operating system |
| `addresses` | Json | Tailscale IPv4 and IPv6 addresses |
| `tags` | Json | Device tags |
| `created_at` | Timestamp | Time the device joined the tailnet |
| `connected_to_control` | Boolean | Recent control-server connection status |
| `last_seen_at` | Timestamp | Last control-server connection time |
| `expires_at` | Timestamp | Device key expiration time |
| `authorized` | Boolean | Whether the device is authorized |
| `key_expiry_disabled` | Boolean | Whether key expiry is disabled |
| `is_external` | Boolean | Whether the device is shared in |
| `is_ephemeral` | Boolean | Whether the device is ephemeral |
| `update_available` | Boolean | Whether a client update is available |
| `blocks_incoming_connections` | Boolean | Whether incoming Tailscale connections are blocked |
| `enabled_routes` | Json | Approved subnet routes |
| `advertised_routes` | Json | Advertised subnet routes |
| `client_connectivity` | Json | Physical network connectivity details |
| `machine_key` | Utf8 | Machine key |
| `node_key` | Utf8 | Node key |

**Optional filters:** `hostname`, `name`, `user`, `os`, `tags`, `authorized`,
`is_external`, `is_ephemeral`, `key_expiry_disabled`

### `tailscale.users`

Lists users in the configured tailnet.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | User ID |
| `display_name` | Utf8 | Display name |
| `login_name` | Utf8 | Login name |
| `profile_pic_url` | Utf8 | Profile picture URL |
| `tailnet_id` | Utf8 | Owning tailnet |
| `created_at` | Timestamp | Join time |
| `type` | Utf8 | User relation, such as `member` or `shared` |
| `role` | Utf8 | User role |
| `status` | Utf8 | User status |
| `device_count` | Int64 | Number of devices owned by the user |
| `last_seen_at` | Timestamp | Last authentication or owned-device connection time |
| `currently_connected` | Boolean | Whether any owned device is connected |

**Optional filters:** `type`, `role`

### `tailscale.keys`

Lists active auth keys, API access tokens, OAuth clients, and federated
identities visible to the token.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Key ID |
| `key_type` | Utf8 | `auth`, `client`, `api`, or `federated` |
| `expiry_seconds` | Int64 | Duration until expiry |
| `created_at` | Timestamp | Creation time |
| `updated_at` | Timestamp | Update time |
| `expires_at` | Timestamp | Expiration time |
| `revoked_at` | Timestamp | Revocation time |
| `scopes` | Json | Granted scopes |
| `tags` | Json | Trust credential tags |
| `capabilities` | Json | Auth key capabilities |
| `description` | Utf8 | Key description |
| `invalid` | Boolean | Whether the key is revoked or expired |
| `user_id` | Utf8 | User who created the key |
| `audience` | Utf8 | Federated identity audience |
| `issuer` | Utf8 | Federated identity issuer |

**Optional filter:** `all`

### `tailscale.device_routes`

Lists advertised and enabled subnet routes for a single device.

| Column | Type | Description |
|---|---|---|
| `device_id` | Utf8 | Device ID used for the request |
| `advertised_routes` | Json | Subnets this device requests to expose |
| `enabled_routes` | Json | Approved subnet routes |

**Required filter:** `device_id`

### `tailscale.dns_configuration`

Returns the full DNS configuration for the configured tailnet.

| Column | Type | Description |
|---|---|---|
| `nameservers` | Json | Global DNS resolvers |
| `split_dns` | Json | Split DNS suffix mappings |
| `search_paths` | Json | DNS search domains |
| `preferences__override_local_dns` | Boolean | Whether resolvers override local OS DNS |
| `preferences__magic_dns` | Boolean | Whether MagicDNS is enabled |

## Example queries

```sql
-- Find Linux devices that have not checked in recently
SELECT hostname, user, last_seen_at, client_version
FROM tailscale.devices
WHERE os = 'linux'
ORDER BY last_seen_at ASC
LIMIT 20;

-- Inventory tagged production devices
SELECT hostname, user, addresses, tags
FROM tailscale.devices
WHERE tags = 'tag:prod';

-- Review suspended or idle users
SELECT display_name, login_name, status, device_count, last_seen_at
FROM tailscale.users
WHERE role = 'member'
ORDER BY last_seen_at ASC;

-- Inspect route approvals for a device
SELECT advertised_routes, enabled_routes
FROM tailscale.device_routes
WHERE device_id = 'n292kg92CNTRL';

-- Check MagicDNS status
SELECT preferences__magic_dns, preferences__override_local_dns, nameservers
FROM tailscale.dns_configuration;
```

## Validation

```bash
export TAILSCALE_API_TOKEN="<your-token>"
export TAILSCALE_TAILNET="-"
coral source lint sources/community/tailscale/manifest.yaml
coral source add --file sources/community/tailscale/manifest.yaml
coral source test tailscale
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'tailscale'"
coral sql "SELECT * FROM coral.columns WHERE schema_name = 'tailscale'"
coral sql "SELECT hostname, os FROM tailscale.devices LIMIT 5"
```

## Limitations

- **Read-only.** This source does not authorize, rename, delete, tag, invite, or
  otherwise mutate Tailscale resources.
- **No pagination.** The Tailscale API returns all results for these endpoints
  at once.
- **Token visibility.** The rows returned by `tailscale.keys` depend on the
  access token type and scopes used for the request.
- **Dynamic device filters.** Tailscale supports server-side filtering by
  top-level device fields. This source exposes common filters in the manifest;
  less common API fields may require extending the source.

## Out of scope for v1

- Policy file and ACL inspection
- Configuration and network logs
- Webhook management
- User and device invite management
- Tailscale Services
- Any write operation
