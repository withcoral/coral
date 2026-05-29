# DigitalOcean

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 11
**Base URL:** `https://api.digitalocean.com`

Query Droplets, domains, volumes, databases, apps, Kubernetes clusters,
projects, SSH keys, regions, sizes, and account data from DigitalOcean.

## Authentication

Requires a `DIGITALOCEAN_TOKEN`. Generate one at
**[Control Panel → API → Tokens](https://cloud.digitalocean.com/account/api/tokens)**.

- Use **read-only** scope for safe querying
- Tokens start with `dop_v1_`

```bash
DIGITALOCEAN_TOKEN=dop_v1_... coral source add --file sources/community/digitalocean/manifest.yaml
```

Run from the repo root.

## Tables

| Table | Description | Pagination |
|---|---|---|
| `account` | Authenticated account info | None (singleton) |
| `droplets` | Virtual machine instances | Page |
| `domains` | DNS domains | Page |
| `volumes` | Block storage volumes | Page |
| `databases` | Managed database clusters | Page |
| `apps` | App Platform applications | Page |
| `kubernetes_clusters` | Managed Kubernetes clusters | Page |
| `projects` | Project groupings | Page |
| `ssh_keys` | SSH keys on the account | Page |
| `regions` | Available datacenter regions | Page |
| `sizes` | Available Droplet size plans | Page |

## Quick start

```bash
# Confirm connectivity — see your account info
coral sql "SELECT uuid, email, status, droplet_limit FROM digitalocean.account"

# List all Droplets with region and status
coral sql "
  SELECT id, name, status, memory, vcpus, size_slug, region__slug
  FROM digitalocean.droplets
  ORDER BY name
"

# Find unattached volumes
coral sql "
  SELECT id, name, size_gigabytes, region__slug, droplet_ids
  FROM digitalocean.volumes
  WHERE droplet_ids = '[]'
"

# Database clusters and their engines
coral sql "
  SELECT id, name, engine, version, status, size, region, num_nodes
  FROM digitalocean.databases
  ORDER BY engine, name
"

# App Platform deployments
coral sql "
  SELECT id, spec__name, region__slug, region__label, live_url,
         active_deployment__phase
  FROM digitalocean.apps
  ORDER BY updated_at DESC
"

# Kubernetes clusters
coral sql "
  SELECT id, name, region, version, status__state, vpc_uuid
  FROM digitalocean.kubernetes_clusters
"

# List all projects
coral sql "
  SELECT id, name, purpose, environment, is_default
  FROM digitalocean.projects
  ORDER BY name
"

# SSH keys
coral sql "SELECT id, name, fingerprint FROM digitalocean.ssh_keys"

# Available regions
coral sql "SELECT slug, name, available FROM digitalocean.regions ORDER BY slug"

# Cheapest Droplet sizes
coral sql "
  SELECT slug, memory, vcpus, disk, price_monthly, price_hourly
  FROM digitalocean.sizes
  WHERE available = true
  ORDER BY price_monthly
  LIMIT 10
"

# Cross-table: Droplets with pricing info
coral sql "
  SELECT d.name, d.status, d.region__slug, d.size_slug,
         s.price_monthly, s.memory, s.vcpus
  FROM digitalocean.droplets d
  JOIN digitalocean.sizes s ON d.size_slug = s.slug
  ORDER BY s.price_monthly DESC
"
```

## Discovery order

```text
account
  → droplet_limit, floating_ip_limit (quotas)

droplets
  → size_slug → sizes.slug (pricing)
  → region__slug → regions.slug (region details)

volumes
  → droplet_ids → droplets.id (attachments)
  → region__slug → regions.slug

databases
  → region → regions.slug

apps
  → region__slug (App Platform slug, e.g. nyc — differs from regions.slug)
  → spec__region (desired region from app spec)

kubernetes_clusters
  → region → regions.slug

projects
  → (group droplets, volumes, databases by project)

ssh_keys
  → id, fingerprint (used when creating Droplets)

regions
  → slug (join target for all regional resources)
  → sizes (available size slugs)

sizes
  → slug (join target for droplets)
  → regions (where each size is available)
```
