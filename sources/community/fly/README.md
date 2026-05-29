# Fly.io Community Source

Query Fly.io apps, machines, and volumes through Coral SQL using the
[Fly.io Machines API](https://fly.io/docs/machines/api/).

## Setup

### 1. Create a Fly.io API token

Create a read-only org token (recommended):

```bash
fly tokens create readonly -o <org>
```

Or an app-scoped deploy token (sufficient for `machines` and `volumes`,
but cannot list apps):

```bash
fly tokens create deploy -a <app-name>
```

See [Fly.io API authentication](https://fly.io/docs/machines/api/working-with-machines-api/)
for more details.

### 2. Add the source

```bash
export FLY_API_TOKEN="<your-token>"
coral source add --file sources/community/fly/manifest.yaml
```

### 3. Verify

```bash
coral source test fly
```

> **Note:** The built-in test query uses `fly.apps`, which requires an org or
> read-only token. If you are using an app-scoped deploy token, skip this step
> and verify directly with a machines or volumes query against your app.

## Tables

### `fly.apps`

Lists Fly Apps in an organization.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | App ID |
| `name` | Utf8 | App name (use as `app_name` filter for other tables) |
| `status` | Utf8 | App lifecycle status (e.g. deployed, suspended, pending) |
| `machine_count` | Int64 | Number of machines |
| `volume_count` | Int64 | Number of volumes |
| `network` | Utf8 | Network name |
| `org_slug` | Utf8 | Echoed filter value |

**Required filter:** `org_slug`

### `fly.machines`

Lists Fly Machines for a specific app.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Machine ID |
| `name` | Utf8 | Machine name |
| `state` | Utf8 | Current state (started, stopped, suspended, destroyed) |
| `host_status` | Utf8 | Host-level health (ok, unknown, unreachable) |
| `region` | Utf8 | Region code |
| `instance_id` | Utf8 | Current instance version |
| `private_ip` | Utf8 | 6PN IPv6 address |
| `image_ref__registry` | Utf8 | Docker image registry |
| `image_ref__repository` | Utf8 | Docker image repository |
| `image_ref__tag` | Utf8 | Docker image tag |
| `image_ref__digest` | Utf8 | Docker image digest |
| `config` | Json | Full machine configuration |
| `checks` | Json | Health check status |
| `events` | Json | Event log |
| `created_at` | Timestamp | Creation time |
| `updated_at` | Timestamp | Last update time |
| `app_name` | Utf8 | Echoed filter value |

**Required filter:** `app_name`
**Optional filters:** `state`, `region`, `include_deleted`

### `fly.volumes`

Lists Fly Volumes (persistent storage) for a specific app.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Volume ID |
| `name` | Utf8 | Volume name |
| `state` | Utf8 | Volume state |
| `host_status` | Utf8 | Host-level health (ok, unknown, unreachable) |
| `type` | Utf8 | Volume type (local or cache) |
| `size_gb` | Int64 | Size in GB |
| `bytes_total` | Int64 | Total size in bytes |
| `bytes_used` | Int64 | Used space in bytes |
| `region` | Utf8 | Region code |
| `zone` | Utf8 | Availability zone |
| `encrypted` | Boolean | Encryption status |
| `attached_machine_id` | Utf8 | Attached machine ID |
| `block_size` | Int64 | Block size in bytes |
| `blocks` | Int64 | Total blocks |
| `blocks_free` | Int64 | Free blocks |
| `blocks_avail` | Int64 | Available blocks |
| `fstype` | Utf8 | Filesystem type |
| `snapshot_retention` | Int64 | Snapshot retention count |
| `auto_backup_enabled` | Boolean | Auto backup status |
| `created_at` | Timestamp | Creation time |
| `app_name` | Utf8 | Echoed filter value |

**Required filter:** `app_name`

## Example queries

```sql
-- List all apps in your personal org
SELECT name, machine_count, volume_count
FROM fly.apps
WHERE org_slug = 'personal';

-- Check machine states for an app
SELECT name, state, region, image_ref__repository
FROM fly.machines
WHERE app_name = 'my-web-app';

-- Find stopped machines
SELECT name, region, updated_at
FROM fly.machines
WHERE app_name = 'my-api' AND state = 'stopped';

-- Find unattached volumes
SELECT name, size_gb, region
FROM fly.volumes
WHERE app_name = 'my-app' AND attached_machine_id IS NULL;

-- Inspect machine config as JSON
SELECT name, json_get_str(config, 'image') AS image
FROM fly.machines
WHERE app_name = 'my-app';
```

## Validation

```bash
export FLY_API_TOKEN="<your-token>"
coral source lint sources/community/fly/manifest.yaml
coral source add --file sources/community/fly/manifest.yaml
coral source test fly
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'fly'"
coral sql "SELECT * FROM coral.columns WHERE schema_name = 'fly'"
coral sql "SELECT name, machine_count FROM fly.apps WHERE org_slug = 'personal' LIMIT 5"
```

> **Note:** `coral source test fly` and the `fly.apps` query above require an
> org or read-only token. With a deploy token, verify using a machines or
> volumes query instead (e.g.
> `coral sql "SELECT id, state FROM fly.machines WHERE app_name = 'my-app' LIMIT 1"`).


## Limitations

- **Read-only.** This source does not create, update, start, stop, or delete
  any Fly.io resources.
- **No pagination.** The Fly.io Machines API returns all items in a single
  response. This works well for typical accounts (tens to low hundreds of
  apps/machines), but very large organizations may see larger response payloads.
- **Token creation.** Tokens are typically created and managed via the `flyctl`
  CLI (`fly tokens create readonly`, `fly tokens create deploy`). Org-scoped
  tokens can also be created from the Fly.io dashboard.
- **Machines and volumes are per-app.** You must provide an `app_name` filter
  obtained from the `apps` table.

## Out of scope for v1

- Certificates management
- Machine start/stop/suspend operations
- Volume snapshots
- Volume extension
- Network policies
- Secrets management
- Write operations of any kind
