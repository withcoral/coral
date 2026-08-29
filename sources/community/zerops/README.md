# Zerops community source

Query Zerops regions, supported service stack types, and personal access tokens through Coral SQL.
This source adds the Zerops public REST API to the community catalog so users
and agents can inspect available regions, browse the runtimes and managed
services Zerops supports, and audit their own access tokens through SQL.

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 4
**Base URL (default):** `https://api.app-prg1.zerops.io/api/rest/public`

## Why this source

Zerops is a developer-first PaaS that runs on bare metal with fine-grained
horizontal and vertical autoscaling and a configurable build and deploy
pipeline. Coral did not have a Zerops source yet, so this community spec gives
the reef a focused read/query surface for:

- Discovering which Zerops regions a CLI, SDK, or agent can target.
- Looking up the runtimes, databases, and storage types Zerops supports, and
  whether each one is managed, backed up, or user-configurable.
- Listing the personal access tokens associated with the current Zerops user
  for audit and rotation workflows.
- Looking up one personal access token by ID for detailed review.

The v1 surface is intentionally narrow and read-oriented. It proves Coral can
authenticate against Zerops with a personal access token, hit the
authentication-free `/region` and `/settings` endpoints, and map the
`/user-token` endpoints into verifiable tables. Project and service-level
endpoints require a client ID and are out of scope for this first version.

## Installation

Community sources are not bundled with the Coral binary. Clone the Coral
repository and add the manifest from this directory:

```bash
coral source add --file sources/community/zerops/manifest.yaml
```

You can also copy `manifest.yaml` into another workspace and pass that path to
`coral source add --file`.

## Authentication

Create a personal access token from the Zerops GUI:

1. Log in to the Zerops dashboard at <https://app-prg1.zerops.io/>.
2. Open the user menu and go to **Access Token management**.
3. Create a new personal access token and copy it.

See https://docs.zerops.io/references/api for the full REST API reference.

Set the token as `ZEROPS_API_KEY` before adding or testing the source. Coral
sends it as a Bearer token to the Zerops REST API.

```bash
export ZEROPS_API_KEY="your_zerops_personal_access_token"
coral source add --file sources/community/zerops/manifest.yaml
```

Interactive install also works:

```bash
coral source add --interactive --file sources/community/zerops/manifest.yaml
```

The default HTTP host is `api.app-prg1.zerops.io`. The full base URL is
constructed by the manifest as `https://{ZEROPS_HOST}/api/rest/public`. The
public region API currently reports a single region (prg1), so that host is
the only one verified to work:

- `api.app-prg1.zerops.io` (default)

Do not assume other API hosts exist; the Zerops docs base URL references only
`api.app-prg1.zerops.io`. Query `zerops.regions` to discover the hosts the
region API actually exposes instead.

Do not include the scheme or the `/api/rest/public` path; the manifest
prepends `https://` and appends the path automatically.

## Provider docs

- Zerops REST API reference: https://docs.zerops.io/references/api
- Zerops introduction: https://docs.zerops.io
- Zerops CLI (`zCLI`): https://docs.zerops.io/references/cli
- Zerops YAML spec: https://docs.zerops.io/zerops-yaml/specification

## Tables

| Table | Description | Required filters |
| --- | --- | --- |
| `zerops.regions` | Zerops datacenters/regions exposed by the region API. | None |
| `zerops.service_stack_types` | Supported Zerops service stack types (runtimes, databases, storage). | None |
| `zerops.user_tokens` | Personal access tokens issued for the current Zerops user. | None |
| `zerops.user_token` | A single Zerops personal access token by ID. | `id` |

### `zerops.regions`

Lists the regions Zerops exposes through `GET /region`. The `address` column
is the bare API host for that region — pass it as `ZEROPS_HOST` when targeting
a non-default region.

```sql
SELECT name, address, is_default FROM zerops.regions;
```

### `zerops.service_stack_types`

Lists the supported service stack types from `GET /settings`. Each row includes
runtime/managed/backup capability flags and a nested `service_stack_type_version_list`
JSON column with the available versions for that stack type.

```sql
SELECT id, name, category, is_runtime, is_managed, has_backup
FROM zerops.service_stack_types
WHERE is_runtime = true
LIMIT 10;
```

Filter by managed-service capability:

```sql
SELECT id, name, is_managed, has_backup
FROM zerops.service_stack_types
WHERE is_managed = true AND has_backup = true
ORDER BY name;
```

### `zerops.user_tokens`

Lists the personal access tokens for the current Zerops user via
`GET /user-token/list`. Use the `id` to look up one token.

```sql
SELECT id, name, created FROM zerops.user_tokens;
```

### `zerops.user_token`

Looks up one personal access token by ID via `GET /user-token/{id}`.

```sql
SELECT id, name, created FROM zerops.user_token
WHERE id = 'your_token_id';
```

## Validation

Run the source-level checks with a valid `ZEROPS_API_KEY` before opening or
updating a PR. The API key is required for `source add`, `source test`, and
live SQL queries, but it should never be printed or committed.

```bash
coral source lint sources/community/zerops/manifest.yaml

export ZEROPS_API_KEY="your_zerops_personal_access_token"
coral source add --file sources/community/zerops/manifest.yaml
coral source test zerops
```

The declared test queries cover region discovery, runtime stack filtering,
a single service stack type lookup, and a user-token list:

```sql
SELECT name, address, is_default FROM zerops.regions;

SELECT id, name, category, is_runtime, is_managed
FROM zerops.service_stack_types
WHERE is_runtime = true
LIMIT 10;

SELECT id, name FROM zerops.service_stack_types
WHERE id = 'alpine'
LIMIT 1;

SELECT id, name, created FROM zerops.user_tokens LIMIT 10;
```

### Live validation output

The following output was captured from a live validation run using a real
Zerops personal access token.

#### Manifest lint

Command:

```bash
coral source lint sources/community/zerops/manifest.yaml
```

Output:

```text
Manifest is valid
```

#### Add source and run declared tests

Command:

```bash
coral source add --file sources/community/zerops/manifest.yaml
```

Output:

```text
Added source zerops (secrets: keychain)
Validating source...

  ✓ zerops connected successfully
  Secrets: keychain

    zerops (4 tables)
    ├─ regions
    ├─ service_stack_types
    ├─ user_token
    └─ user_tokens
    Query tests
    4 declared · 4 passed · 0 failed

    ✓ SELECT name, address, is_default FROM zerops.regions
      1 row

    ✓ SELECT id, name, category, is_runtime, is_managed FROM zerops.service_stack_types WHERE is_runtime = true LIMIT 10
      10 rows

    ✓ SELECT id, name FROM zerops.service_stack_types WHERE id = 'alpine' LIMIT 1
      1 row

    ✓ SELECT id, name, created FROM zerops.user_tokens LIMIT 10
      1 row
```

#### Re-run source tests

Command:

```bash
coral source test zerops
```

Output:

```text
  ✓ zerops connected successfully
  Secrets: keychain

    zerops (4 tables)
    ├─ regions
    ├─ service_stack_types
    ├─ user_token
    └─ user_tokens
    Query tests
    4 declared · 4 passed · 0 failed

    ✓ SELECT name, address, is_default FROM zerops.regions
      1 row

    ✓ SELECT id, name, category, is_runtime, is_managed FROM zerops.service_stack_types WHERE is_runtime = true LIMIT 10
      10 rows

    ✓ SELECT id, name FROM zerops.service_stack_types WHERE id = 'alpine' LIMIT 1
      1 row

    ✓ SELECT id, name, created FROM zerops.user_tokens LIMIT 10
      1 row
```

#### Confirm table discovery

Command:

```bash
coral sql "SELECT table_name FROM coral.tables WHERE schema_name = 'zerops' ORDER BY table_name"
```

Output:

```text
+---------------------+
| table_name          |
+---------------------+
| regions             |
| service_stack_types |
| user_token          |
| user_tokens         |
+---------------------+
```

#### Confirm column discovery

Command:

```bash
coral sql "SELECT table_name, column_name, data_type, is_nullable FROM coral.columns WHERE schema_name = 'zerops' ORDER BY table_name, ordinal_position"
```

Output:

```text
+---------------------+---------------------------------+-----------+-------------+
| table_name          | column_name                     | data_type | is_nullable |
+---------------------+---------------------------------+-----------+-------------+
| regions             | name                            | Utf8      | false       |
| regions             | address                         | Utf8      | false       |
| regions             | is_default                      | Boolean   | false       |
| service_stack_types | id                              | Utf8      | false       |
| service_stack_types | name                            | Utf8      | false       |
| service_stack_types | description                     | Utf8      | true        |
| service_stack_types | category                        | Utf8      | true        |
| service_stack_types | subcategory                     | Utf8      | true        |
| service_stack_types | docs_url                        | Utf8      | true        |
| service_stack_types | is_build                        | Boolean   | true        |
| service_stack_types | is_runtime                      | Boolean   | true        |
| service_stack_types | is_managed                      | Boolean   | true        |
| service_stack_types | has_backup                      | Boolean   | true        |
| service_stack_types | has_access_details              | Boolean   | true        |
| service_stack_types | has_configuration               | Boolean   | true        |
| service_stack_types | os_list                         | Json      | true        |
| service_stack_types | mode_list                       | Json      | true        |
| service_stack_types | service_stack_type_version_list | Json      | true        |
| service_stack_types | created                         | Timestamp | true        |
| service_stack_types | last_update                     | Timestamp | true        |
| user_token          | id                              | Utf8      | false       |
| user_token          | name                            | Utf8      | true        |
| user_token          | created                         | Timestamp | true        |
| user_tokens         | id                              | Utf8      | false       |
| user_tokens         | name                            | Utf8      | true        |
| user_tokens         | created                         | Timestamp | true        |
+---------------------+---------------------------------+-----------+-------------+
```

#### Confirm input discovery

Command:

```bash
coral sql "SELECT key, kind, required FROM coral.inputs WHERE schema_name = 'zerops' ORDER BY key"
```

Output:

```text
+----------------+----------+----------+
| key            | kind     | required |
+----------------+----------+----------+
| ZEROPS_API_KEY | secret   | true     |
| ZEROPS_HOST    | variable | false    |
+----------------+----------+----------+
```

#### Run a live regions query

Command:

```bash
coral sql "SELECT name, address, is_default FROM zerops.regions"
```

Output:

```text
+------+------------------------+------------+
| name | address                | is_default |
+------+------------------------+------------+
| prg1 | api.app-prg1.zerops.io | true       |
+------+------------------------+------------+
```

#### Run a live runtime stack types query

Command:

```bash
coral sql "SELECT id, name, category, is_runtime, is_managed, has_backup FROM zerops.service_stack_types WHERE is_runtime = true ORDER BY id LIMIT 10"
```

Output:

```text
+--------+--------------+----------+------------+------------+------------+
| id     | name         | category | is_runtime | is_managed | has_backup |
+--------+--------------+----------+------------+------------+------------+
| alpine | Alpine       | USER     | true       | false      | false      |
| bun    | Bun          | USER     | true       | false      | false      |
| deno   | Deno         | USER     | true       | false      | false      |
| docker | Docker       | USER     | true       | false      | false      |
| dotnet | .NET         | USER     | true       | false      | false      |
| elixir | Elixir       | USER     | true       | false      | false      |
| gleam  | Gleam        | USER     | true       | false      | false      |
| golang | Golang       | USER     | true       | false      | false      |
| java   | Java         | USER     | true       | false      | false      |
| nginx  | Nginx static | USER     | true       | false      | false      |
+--------+--------------+----------+------------+------------+------------+
```

#### Run a live single service stack type query

Command:

```bash
coral sql "SELECT id, name, category, is_runtime FROM zerops.service_stack_types WHERE id = 'alpine' LIMIT 1"
```

Output:

```text
+--------+--------+----------+------------+
| id     | name   | category | is_runtime |
+--------+--------+----------+------------+
| alpine | Alpine | USER     | true       |
+--------+--------+----------+------------+
```

#### Run a live managed services query

Command:

```bash
coral sql "SELECT id, name, is_managed, has_backup FROM zerops.service_stack_types WHERE is_managed = true AND has_backup = true ORDER BY name"
```

Output:

```text
+----------------+----------------+------------+------------+
| id             | name           | is_managed | has_backup |
+----------------+----------------+------------+------------+
| clickhouse     | ClickHouse     | true       | true       |
| elasticsearch  | Elasticsearch  | true       | true       |
| mariadb        | MariaDB        | true       | true       |
| meilisearch    | Meilisearch    | true       | true       |
| nats           | NATS           | true       | true       |
| postgresql     | PostgreSQL     | true       | true       |
| qdrant         | Qdrant         | true       | true       |
| shared_storage | Shared Storage | true       | true       |
| valkey         | Valkey         | true       | true       |
+----------------+----------------+------------+------------+
```

#### Run a live user tokens query

Command:

```bash
coral sql "SELECT id, name, created FROM zerops.user_tokens"
```

Output:

```text
+----------------+----------------+----------------------+
| id             | name           | created              |
+----------------+----------------+----------------------+
| your_token_id  | your_token_name | your_token_created   |
+----------------+----------------+----------------------+
```

#### Run a live single user token query

Command:

```bash
coral sql "SELECT id, name, created FROM zerops.user_token WHERE id = 'your_token_id'"
```

Output:

```text
+----------------+----------------+----------------------+
| id             | name           | created              |
+----------------+----------------+----------------------+
| your_token_id  | your_token_name | your_token_created   |
+----------------+----------------+----------------------+
```

## Limitations

- This source is read/query oriented and does not manage Zerops projects or
  services.
- Project and service-stack endpoints require a client ID, which is not
  exposed by the current endpoints accessible to a personal access token.
  Those endpoints are intentionally out of scope for this first version.
- The `service_stack_type_versions` are exposed as a nested JSON column on
  `service_stack_types`; a dedicated flattened versions table can be added in
  a follow-up version once the consumption pattern stabilizes.
- The Zerops `service_stack_types.description` field is sometimes a literal
  `"todo"` placeholder from the provider. This is a provider data quality
  issue, not a source bug.
- Several `service_stack_types` fields carry provider-side quirks that are
  surfaced verbatim and cannot be normalized by this source: KeyDB, MongoDB,
  and RabbitMQ report `is_managed = false` under the `STANDARD` category, six
  stack types (KeyDB, MongoDB, RabbitMQ, and the three build/prepare runtimes)
  report an empty `subcategory`, and `nodejs` reports an empty `mode_list`.
- The public region API currently reports only the `prg1` region, so
  `zerops.regions` returns a single row. Additional regions may exist upstream
  but are not discoverable through the current API surface.
- Available regions, service stack types, token metadata, and error responses
  depend on the Zerops account, the personal access token permissions, and the
  current provider API.

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md), keep the manifest focused,
and include the validation commands plus proof output in the PR description.
