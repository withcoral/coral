# Milvus

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 8
**Base URL:** your Milvus REST API URL (set via `MILVUS_URL`)

Query databases, collections, partitions, users, roles, and aliases
from Milvus (self-hosted or Zilliz Cloud).

## Setup

### Self-hosted (standalone)

Start a standalone Milvus instance using the official script:

```powershell
# Windows (PowerShell)
iwr https://raw.githubusercontent.com/milvus-io/milvus/master/scripts/standalone_embed.bat -OutFile standalone.bat
.\standalone.bat start
```

The REST API is available at `http://localhost:19530` by default.

For a UI, install Attu:

```bash
docker run -d --name attu -p 8000:3000 zilliz/attu:latest
```

Open `http://localhost:8000`, set Address to `host.docker.internal:19530`, and click Connect.

### Authentication

For a default local instance with no authentication configured, set
`MILVUS_TOKEN` to any non-empty placeholder value (e.g. `none`).

For instances with username/password auth enabled, use the format
`username:password` as the token value.

For Zilliz Cloud, use your API key directly.

See the [Milvus authentication docs](https://milvus.io/docs/authenticate.md).

```bash
MILVUS_URL=http://localhost:19530 \
MILVUS_TOKEN=none \
  coral source add --file sources/community/milvus/manifest.yaml
```

Run from the repo root. Or interactively:

```bash
MILVUS_URL=http://localhost:19530 \
MILVUS_TOKEN=none \
  coral source add --file sources/community/milvus/manifest.yaml --interactive
```

> Note:
> The `users` table requires admin privileges on the Milvus instance.
> Non-admin users may receive permission errors when querying it.

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `databases` | Databases in the Milvus instance | -- | -- |
| `collections` | Collections in a database | -- | `db_name` |
| `collection_details` | Schema and config for a specific collection | `collection_name` | `db_name` |
| `collection_stats` | Row count for a specific collection | `collection_name` | `db_name` |
| `partitions` | Partitions within a collection | `collection_name` | `db_name` |
| `users` | Users in the internal auth database (requires admin privileges) | -- | -- |
| `roles` | Roles defined in the instance | -- | -- |
| `aliases` | Collection aliases | -- | `db_name` |

## Quick start

```bash
# List all databases
coral sql "SELECT database_name FROM milvus.databases"

# List all collections in the default database
coral sql "SELECT collection_name FROM milvus.collections"

# List collections in a specific database
coral sql "
  SELECT collection_name
  FROM milvus.collections
  WHERE db_name = 'my_database'
"

# Inspect a collection schema
coral sql "
  SELECT collection_name, description, load, shards_num,
         consistency_level, enable_dynamic_field, fields, indexes
  FROM milvus.collection_details
  WHERE collection_name = 'my_collection'
"

# Get the row count for a collection
coral sql "
  SELECT row_count
  FROM milvus.collection_stats
  WHERE collection_name = 'my_collection'
"

# List partitions in a collection
coral sql "
  SELECT partition_name
  FROM milvus.partitions
  WHERE collection_name = 'my_collection'
"

# List users (requires admin privileges)
coral sql "SELECT username FROM milvus.users"

# List roles
coral sql "SELECT role_name FROM milvus.roles"

# List aliases
coral sql "SELECT alias_name FROM milvus.aliases"
```

## Discovery order

```text
databases
  -> database_name
    -> collections (WHERE db_name = '...')
      -> collection_name
        -> collection_details (WHERE collection_name = '...')
        -> collection_stats   (WHERE collection_name = '...')
        -> partitions         (WHERE collection_name = '...')

users
  -> username

roles
  -> role_name

aliases
  -> alias_name
```
