# OpenBao Coral Source

## What This Source Exposes

This source exposes [OpenBao](https://openbao.org/) security and secrets
management metadata as SQL tables in Coral. It is designed for platform
engineering, security auditing, and compliance inventory workflows.

It is read-only. It does not call OpenBao `/data/` endpoints and does not expose
plaintext secret values.

Metadata can still be operationally sensitive. Policy rules can reveal secret
path structure and granted capabilities, audit device options can reveal audit
configuration, and KV custom metadata is user-controlled. Treat query results
from this source as security metadata even though plaintext secret values are
not returned.

## Authentication

Create or use an OpenBao token with read access to the metadata you want to
inspect. Coral sends the token with the `X-Vault-Token` header.

Required inputs:

| Input | Kind | Default | Description |
|---|---|---|---|
| `OPENBAO_BASE_URL` | variable | `http://127.0.0.1:8200` | Base URL for your OpenBao deployment, without a trailing slash. |
| `OPENBAO_TOKEN` | secret | - | OpenBao token with access to the metadata endpoints you want Coral to inspect. |

## Local Setup

Run OpenBao in development mode:

```bash
docker run --cap-add=IPC_LOCK -p 8200:8200 \
  -e BAO_DEV_ROOT_TOKEN_ID=dev-token \
  openbao/openbao:latest
```

Add the source:

```bash
OPENBAO_BASE_URL=http://127.0.0.1:8200 \
OPENBAO_TOKEN=dev-token \
coral source add --file sources/community/openbao/manifest.yaml
```

Or run interactively:

```bash
coral source add --interactive --file sources/community/openbao/manifest.yaml
```

## Minimum Policy

For production use, prefer a scoped token instead of a root token. The exact
policy depends on which tables you want to query.

Example policy for the system metadata tables:

```hcl
path "sys/mounts" {
  capabilities = ["read"]
}

path "sys/auth" {
  capabilities = ["read"]
}

path "sys/policy" {
  capabilities = ["read"]
}

path "sys/policy/*" {
  capabilities = ["read"]
}

path "sys/audit" {
  capabilities = ["read", "sudo"]
}
```

Add KV v2 metadata access for the mount and paths you want to inspect:

```hcl
path "secret/metadata/*" {
  capabilities = ["read"]
}
```

This policy does not grant access to `secret/data/*`, so it does not allow
reading plaintext secret values.

## Tables

| Table | Purpose |
|---|---|
| `openbao.mounts` | Enabled secrets engines and mount configuration. |
| `openbao.auth_methods` | Enabled authentication methods. |
| `openbao.policies` | ACL policy names. |
| `openbao.policy_details` | Policy rules and metadata for one policy name. |
| `openbao.audit_devices` | Configured audit devices. This does not read audit log records. |
| `openbao.kv_secret_metadata` | Metadata for one KV v2 secret path. Requires `mount` and `path`. |
| `openbao.kv_secret_versions` | Per-version metadata for one KV v2 secret path. Requires `mount` and `path`. |

## Example Queries

Enabled secrets engines:

```sql
SELECT path, type, description
FROM openbao.mounts
ORDER BY path;
```

KV v2 mounts:

```sql
SELECT path, options
FROM openbao.mounts
WHERE type = 'kv';
```

Enabled auth methods:

```sql
SELECT path, type, description
FROM openbao.auth_methods
ORDER BY path;
```

Policy names:

```sql
SELECT name
FROM openbao.policies
ORDER BY name;
```

Policy details:

```sql
SELECT name, rules, modified
FROM openbao.policy_details
WHERE name = 'default';
```

Configured audit devices:

```sql
SELECT path, type, description
FROM openbao.audit_devices
ORDER BY path;
```

KV v2 metadata for a known path:

```sql
SELECT mount, path, current_version, updated_time, custom_metadata
FROM openbao.kv_secret_metadata
WHERE mount = 'secret' AND path = 'app/config';
```

KV v2 version history for a known path:

```sql
SELECT mount, path, version, created_time, deletion_time, destroyed
FROM openbao.kv_secret_versions
WHERE mount = 'secret' AND path = 'app/config'
ORDER BY version DESC;
```

## Limitations

- This source is metadata-only and does not expose plaintext secret values.
- The source does not call OpenBao `/data/` endpoints.
- `openbao.audit_devices` lists configured audit devices, not audit log records.
- `openbao.kv_secret_metadata` and `openbao.kv_secret_versions` require both
  `mount` and `path` filters.
- Recursive KV path listing is intentionally not included in v1. OpenBao
  supports listing metadata paths, including through `GET` with `?list=true`,
  but this first version requires known `mount` and `path` filters to avoid
  broad secret-path enumeration.
- OpenBao deployments can differ by enabled engines and policy permissions; a
  token that can read system metadata may not be able to read every KV metadata
  path.

## Validation

Local validation for this source:

```text
$ coral source lint sources/community/openbao/manifest.yaml
Manifest is valid
```

Selected output from live validation against `openbao/openbao:2.5.4`:

```text
$ coral source add --file sources/community/openbao/manifest.yaml
Added source openbao

  ✓ openbao connected successfully

    openbao (7 tables)
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT path, type FROM openbao.mounts LIMIT 5
      4 rows

    ✓ SELECT path, type FROM openbao.auth_methods LIMIT 5
      1 row

$ coral source test openbao

  ✓ openbao connected successfully

    openbao (7 tables)
    Query tests
    2 declared · 2 passed · 0 failed
```

Representative queries:

```text
$ coral sql "SELECT path, type FROM openbao.mounts ORDER BY path LIMIT 5"
+------------+-----------+
| path       | type      |
+------------+-----------+
| cubbyhole/ | cubbyhole |
| identity/  | identity  |
| secret/    | kv        |
| sys/       | system    |
+------------+-----------+

$ coral sql "SELECT path, type FROM openbao.audit_devices LIMIT 5"
+------+------+
| path | type |
+------+------+
+------+------+
```

The empty `openbao.audit_devices` result is expected for an OpenBao dev server
without configured audit devices. It verifies that the table reads from the
response `data` envelope instead of treating response envelope keys as devices.

Before submitting changes, also run:

```text
make rust-checks
```

Live API testing should use a local OpenBao dev server or a scoped non-production
token. Do not commit OpenBao tokens or secret payloads.
