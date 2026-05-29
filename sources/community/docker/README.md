# Docker

**Version:** 0.2.0
**Backend:** HTTP
**Tables:** 2
**Base URL:** `http://localhost:2375` (configurable)

Query Docker container metadata as SQL via the [Docker Engine HTTP API](https://docs.docker.com/reference/api/engine/). No authentication required for local use.

```bash
coral source add --file sources/community/docker/manifest.yaml
```

## Tables

| Table | Description |
|---|---|
| `containers` | All containers including stopped, exited, paused, and dead ones |
| `running` | Only currently running containers |

---

### `containers`

All containers on the Docker host. Equivalent to `docker ps --all`.

#### Columns

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Full 64-character container ID |
| `image` | Utf8 | Image name and tag (e.g. `nginx:latest`) |
| `status` | Utf8 | Human-readable status string (e.g. `Exited (255) 2 days ago`) |
| `state` | Utf8 | Machine-readable state: `running`, `exited`, `dead`, `paused`, `restarting` |
| `created_epoch` | Int64 | Container creation time as Unix epoch seconds |
| `command` | Utf8 | Entrypoint command string |

---

### `running`

Only currently running containers. Equivalent to `docker ps`.

#### Columns

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Full 64-character container ID |
| `image` | Utf8 | Image name and tag |
| `status` | Utf8 | Human-readable status string |
| `state` | Utf8 | Machine-readable state |
| `created_epoch` | Int64 | Container creation time as Unix epoch seconds |

---

## Setup

### Option 1 — Local via socat (recommended)

Proxy the Unix socket over TCP without exposing a network port:

```bash
socat TCP-LISTEN:2375,reuseaddr,fork UNIX-CLIENT:/var/run/docker.sock
```

Set `DOCKER_API_BASE=http://localhost:2375`.

### Option 2 — Docker Desktop (Windows / macOS)

Docker Desktop → Settings → General → **"Expose daemon on tcp://localhost:2375 without TLS"**

> ⚠️ Only enable on trusted local machines. Never expose port 2375 on a network interface.

### Option 3 — Remote daemon with TLS

Follow Docker's [remote access guide](https://docs.docker.com/engine/daemon/remote-access/) to configure TLS certificates, then set:

```
DOCKER_API_BASE=https://your-docker-host:2376
```

---

## Quick start

```bash
# Confirm connectivity
coral sql "SELECT id, image, status, state FROM docker.containers LIMIT 1"

# List all containers with their state
coral sql "SELECT id, image, status, state FROM docker.containers LIMIT 20"

# Find only exited containers
coral sql "SELECT id, image, status FROM docker.containers WHERE state = 'exited' LIMIT 10"

# List running containers
coral sql "SELECT id, image, status FROM docker.running LIMIT 20"

# Cross-source JOIN with GitHub to correlate container crashes with recent deploys
coral sql "
  SELECT
    dc.id,
    dc.image,
    dc.status,
    gp.title        AS last_pr,
    gp.user__login  AS deployed_by,
    gp.merged_at    AS deploy_time
  FROM docker.containers dc
  LEFT JOIN github.pulls gp
    ON gp.owner = 'your-owner'
    AND gp.repo  = 'your-repo'
    AND gp.state = 'closed'
  WHERE dc.state != 'running'
  LIMIT 10
"
```

---

## Live evidence

```
$ coral source add --file sources/community/docker/manifest.yaml
Added source docker
  ✓ docker connected successfully
    docker (2 tables)
    ├─ containers
    └─ running
    Query tests
    1 declared · 1 passed · 0 failed
    ✓ SELECT id, image, status, state FROM docker.containers LIMIT 1
      1 row

$ coral sql "SELECT id, image, status, state FROM docker.containers LIMIT 3"
+------------------------------------------------------------------+---------------------------------+-------------------------+--------+
| id                                                               | image                           | status                  | state  |
+------------------------------------------------------------------+---------------------------------+-------------------------+--------+
| 6d0f1aa827b5b4efbebf3047e65a509a1ded2ef6ad4d4ba3d0ca20d999a23188 | jaegertracing/all-in-one:latest | Exited (255) 2 days ago | exited |
+------------------------------------------------------------------+---------------------------------+-------------------------+--------+
```

---

## Notes

- The Docker Engine API does not support server-side filtering by arbitrary fields. `WHERE` clauses on `state`, `image`, etc. are evaluated locally by Coral after fetching the full container list.
- `created_epoch` is a Unix timestamp (seconds). Use your environment's date functions to convert if needed.
- Container logs and per-container stats require separate API calls (`/containers/{id}/logs`, `/containers/{id}/stats`) and are not included in these tables.
- The Docker Engine API version targeted is v1.43+ (Docker 24.0+). Older versions may differ in field availability.