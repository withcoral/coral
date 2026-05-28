# Railway source

Query your [Railway](https://railway.com) infrastructure through Coral using
Railway's [public GraphQL API](https://docs.railway.com/integrations/api).

## Tables

| Table              | Description                       | Required filters                             | Optional filters                         |
| ------------------ | --------------------------------- | -------------------------------------------- | ---------------------------------------- |
| `projects`         | Projects accessible by the token  | —                                            | `workspace_id`                           |
| `services`         | Services within a project         | `project_id`                                 | —                                        |
| `environments`     | Environments within a project     | `project_id`                                 | —                                        |
| `deployments`      | Deployments scoped to a project   | `project_id`                                 | `environment_id`, `service_id`           |
| `variables`        | Environment variables             | `project_id`, `environment_id`               | `service_id` (omit for shared variables) |
| `domains`          | Railway-generated service domains | `project_id`, `service_id`, `environment_id` | —                                        |
| `custom_domains`   | Custom domains for a service      | `project_id`, `service_id`, `environment_id` | —                                        |
| `volumes`          | Persistent volumes in a project   | `project_id`                                 | —                                        |
| `volume_instances` | Per-environment volume mounts     | `volume_instance_id`                         | —                                        |

## Authentication

This source uses API tokens authenticated via `Authorization: Bearer <token>`.
Both **account tokens** and **workspace tokens** are supported.

- **Workspace tokens** (recommended) — scoped to a single workspace.
  Sufficient for most use cases and the safer default.
- **Account tokens** — grant access to all projects across all workspaces.
  Use only when you need cross-workspace visibility.

Create either token type at
[railway.com/account/tokens](https://railway.com/account/tokens).

> **Note:** Project-scoped tokens use a different header
> (`Project-Access-Token`) and are **not supported** by this source.
> Use a workspace or account token instead.

## Setup

```bash
RAILWAY_API_TOKEN=<token> coral source add --file sources/community/railway/manifest.yaml
```

Or interactively:

```bash
coral source add --file sources/community/railway/manifest.yaml --interactive
```

When using `--interactive`, you will be prompted for `RAILWAY_API_TOKEN`.
Otherwise, set it as an environment variable before running the command.

If you use a workspace token, include `workspace_id` when you first query
`railway.projects` so Railway can scope project discovery to that workspace.

## Example queries

### List all projects

Account-token users can list all visible projects:

```sql
SELECT * FROM railway.projects;
```

Workspace-token users should scope the discovery query with their workspace ID:

```sql
SELECT *
  FROM railway.projects
 WHERE workspace_id = 'your-workspace-id';
```

### Services in a project

```sql
SELECT *
  FROM railway.services
 WHERE project_id = 'your-project-id';
```

### Recent deployments

```sql
SELECT id, status, service_id, created_at
  FROM railway.deployments
 WHERE project_id = 'your-project-id'
 ORDER BY created_at DESC
 LIMIT 10;
```

### Deployments for a specific service

```sql
SELECT id, status, environment_id, created_at
  FROM railway.deployments
 WHERE project_id = 'your-project-id'
   AND service_id = 'your-service-id'
 ORDER BY created_at DESC
 LIMIT 10;
```

### Environments for a project

```sql
SELECT id, name, is_ephemeral
  FROM railway.environments
 WHERE project_id = 'your-project-id';
```

### Service-scoped variables

> **Warning:** Querying `railway.variables.value` can expose secret environment variables
> or credentials in plain text. Railway API tokens can access all variables in their
> scope (workspace or account). Only select `value` if you intentionally need to read secrets.

```sql
SELECT name
  FROM railway.variables
 WHERE project_id     = 'your-project-id'
   AND environment_id = 'your-env-id'
   AND service_id     = 'your-service-id';
```

### Shared environment variables

Omit `service_id` to retrieve variables shared across all services in an
environment:

```sql
SELECT name
  FROM railway.variables
 WHERE project_id     = 'your-project-id'
   AND environment_id = 'your-env-id';
```

### Railway-generated domains

```sql
SELECT domain, suffix, target_port, created_at
  FROM railway.domains
 WHERE project_id     = 'your-project-id'
   AND service_id     = 'your-service-id'
   AND environment_id = 'your-env-id';
```

### Custom domains

```sql
SELECT domain, target_port, certificate_status, verification_token, dns_records
  FROM railway.custom_domains
 WHERE project_id     = 'your-project-id'
   AND service_id     = 'your-service-id'
   AND environment_id = 'your-env-id';
```

### Volumes in a project

```sql
SELECT id, name, volume_instances, created_at
  FROM railway.volumes
 WHERE project_id = 'your-project-id';
```

### Volume instance details

Use the `volume_instances` JSON column from `railway.volumes` to discover
volume instance IDs, then query the mount details:

```sql
SELECT volume_id, environment_id, service_id, mount_path, state,
       size_mb, current_size_mb, region
  FROM railway.volume_instances
 WHERE volume_instance_id = 'your-volume-instance-id';
```

## Pagination

The Railway API uses **Relay-style cursor pagination**. The source spec
implements `cursor_body` mode with `endCursor` / `after` semantics,
matching the pattern used by the Linear core source. Pages default to
50 items with a maximum of 100.

Paginated tables: `projects`, `services`, `environments`, `deployments`,
`volumes`.

The `variables`, `domains`, `custom_domains`, and `volume_instances` tables do
not use pagination because the API returns all items in a single response or a
single resource by ID.

## Rate limits

Railway enforces API rate limits (see [Railway API Rate Limits](https://docs.railway.com/integrations/api#rate-limits)). Since this source uses pagination to fetch all items for a query, querying high-cardinality tables (like `deployments`) without filters can consume significant quota. The `deployments` table sets `fetch_limit_default: 100` to cap the default fetch. Use `service_id`, `environment_id`, or `LIMIT` to narrow your queries and reduce the number of requests.

## Known limitations

- **Project-scoped tokens only unsupported** — project-scoped tokens use
  a different auth header (`Project-Access-Token`) and are not supported.
  Use a workspace or account token instead.
- **Variables are returned as a flat JSON object** — the API returns
  `{ KEY: "value" }` rather than a connection. The source uses
  `row_strategy: dict_entries` to normalize this into rows.
- **Domain listing** requires `service_id` and `environment_id` in
  addition to `project_id` because the `domains` query requires all
  three identifiers.
- **Railway-generated and custom domains** are split into separate tables
  (`domains` and `custom_domains`) because the GraphQL response nests
  them under different keys (`serviceDomains` vs `customDomains`).
- **Volume instance discovery** — detailed mount fields live on
  `VolumeInstance`. Use the `volumes.volume_instances` JSON column to discover
  instance IDs, then query `railway.volume_instances` for mount path, state,
  size, region, service, and environment fields.
