# Railway source

Query your [Railway](https://railway.com) infrastructure through Coral using
Railway's [public GraphQL API](https://docs.railway.com/integrations/api).

## Tables

| Table            | Description                        | Required filters                                      |
| ---------------- | ---------------------------------- | ----------------------------------------------------- |
| `projects`       | Projects accessible by the token   | —                                                     |
| `services`       | Services within a project          | `project_id`                                          |
| `environments`   | Environments within a project      | `project_id`                                          |
| `deployments`    | Deployments scoped to a project    | `project_id`                                          |
| `variables`      | Environment variables for a service| `project_id`, `environment_id`, `service_id`          |
| `domains`        | Railway-generated service domains  | `project_id`, `service_id`, `environment_id`          |
| `custom_domains` | Custom domains for a service       | `project_id`, `service_id`, `environment_id`          |
| `volumes`        | Persistent volumes in a project    | `project_id`                                          |

## Authentication

This source uses **account-level API tokens** authenticated via
`Authorization: Bearer <token>`.

Create an account token at
[railway.com/account/tokens](https://railway.com/account/tokens).
Account tokens provide access to all projects across all workspaces.

> **Note:** Project-scoped tokens use a different header
> (`Project-Access-Token`) and are **not supported** by this source.
> Use an account token instead.

Set the token as `RAILWAY_API_TOKEN` when adding this source.

## Setup

```bash
coral source add --file sources/community/railway/manifest.yaml
```

Then configure your token when prompted, or set the `RAILWAY_API_TOKEN`
environment variable.

## Example queries

### List all projects

```sql
SELECT * FROM railway.projects;
```

### Services in a project

```sql
SELECT *
  FROM railway.services
 WHERE project_id = 'your-project-id';
```

### Recent deployments

```sql
SELECT id, status, created_at
  FROM railway.deployments
 WHERE project_id = 'your-project-id'
 ORDER BY created_at DESC
 LIMIT 10;
```

### Environments for a project

```sql
SELECT id, name, is_ephemeral
  FROM railway.environments
 WHERE project_id = 'your-project-id';
```

### Variables for a service

```sql
SELECT name, value
  FROM railway.variables
 WHERE project_id     = 'your-project-id'
   AND environment_id = 'your-env-id'
   AND service_id     = 'your-service-id';
```

### Railway-generated domains

```sql
SELECT domain, created_at
  FROM railway.domains
 WHERE project_id     = 'your-project-id'
   AND service_id     = 'your-service-id'
   AND environment_id = 'your-env-id';
```

### Custom domains

```sql
SELECT domain, created_at
  FROM railway.custom_domains
 WHERE project_id     = 'your-project-id'
   AND service_id     = 'your-service-id'
   AND environment_id = 'your-env-id';
```

### Volumes in a project

```sql
SELECT id, name, created_at
  FROM railway.volumes
 WHERE project_id = 'your-project-id';
```

## Pagination

The Railway API uses **Relay-style cursor pagination**. The source spec
implements `cursor_body` mode with `endCursor` / `after` semantics,
matching the pattern used by the Linear core source. Pages default to
50 items with a maximum of 100.

The `variables`, `domains`, `custom_domains`, and `volumes` tables do
not use pagination. `volumes` is a Relay connection but the manifest
currently reads it without `first`/`after` to keep the query simple;
most projects have only a handful of volumes.

## Known limitations

- **Account tokens only** — project-scoped tokens use a different auth
  header (`Project-Access-Token`) and are not supported by this manifest.
- **Variables are returned as a flat JSON object** — the API returns
  `{ KEY: "value" }` rather than a connection. The source uses
  `row_strategy: dict_entries` to normalize this into rows.
- **Domain listing** requires `service_id` and `environment_id` in
  addition to `project_id` because the `domains` query requires all
  three identifiers.
- **Railway-generated and custom domains** are split into separate tables
  (`domains` and `custom_domains`) because the GraphQL response nests
  them under different keys (`serviceDomains` vs `customDomains`).
- **Volume fields** — the Volume type only exposes `id`, `name`, and
  `createdAt`. Mount path and state live on `VolumeInstance` (a
  per-environment resource) and are not included in this source.
