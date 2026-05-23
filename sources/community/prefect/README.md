# Prefect source

Query Prefect flows, flow runs, deployments, work pools, and task runs to
monitor pipeline health and orchestration state. Works with both
**Prefect Cloud** and **self-hosted Prefect Server**.

## Authentication

| Mode | `PREFECT_API_URL` | `PREFECT_API_KEY` |
|---|---|---|
| Prefect Cloud | `https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id>` | Required — API key from account settings |
| Self-hosted (with auth) | `http://<host>:4200/api` | Required — configured in server |
| Self-hosted (no auth) | `http://<host>:4200/api` | Any non-empty placeholder |

---

## Option A — Prefect Cloud

### 1. Find your API URL

Log in to [app.prefect.cloud](https://app.prefect.cloud). Your workspace URL
contains both IDs you need:

```
https://app.prefect.cloud/account/<ACCOUNT-ID>/workspace/<WORKSPACE-ID>/dashboard
```

Your API URL is:

```
https://api.prefect.cloud/api/accounts/<ACCOUNT-ID>/workspaces/<WORKSPACE-ID>
```

### 2. Generate an API key

1. Click the account icon in the bottom-left corner of the UI
2. Select **API Keys**
3. Click **+**, give it a name and expiry, then copy the key immediately —
   it cannot be shown again after you close the dialog

### 3. Install the source

```bash
PREFECT_API_URL=https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id> \
PREFECT_API_KEY=pnu_xxxxxxxxxxxxxxxxxxxxxxxxxxxx \
coral source add --file manifest.yaml
```

---

## Option B — Self-hosted Prefect Server (pip)

### 1. Install and start the server

```bash
pip install prefect
prefect server start
```

The server starts at `http://localhost:4200`. The UI is at
`http://localhost:4200/api/docs`. Keep this terminal running.

### 2. Install the source

Self-hosted Prefect Server has no authentication by default. Use any
non-empty placeholder for the API key:

```bash
PREFECT_API_URL=http://localhost:4200/api \
PREFECT_API_KEY=placeholder \
coral source add --file manifest.yaml
```

---

## Option C — Self-hosted Prefect Server (Docker)

### 1. Start the server

```bash
docker run -d --name prefect-server \
  -p 4200:4200 \
  prefecthq/prefect:3-latest \
  prefect server start --host 0.0.0.0
```

Wait a few seconds, then verify it is up:

```bash
curl http://localhost:4200/api/health
# → true
```

### 2. Install the source

```bash
PREFECT_API_URL=http://localhost:4200/api \
PREFECT_API_KEY=placeholder \
coral source add --file manifest.yaml
```

### Docker Compose (optional)

For a persistent setup with a PostgreSQL backend:

```yaml
version: "3"
services:
  prefect-db:
    image: postgres:15
    environment:
      POSTGRES_USER: prefect
      POSTGRES_PASSWORD: prefect
      POSTGRES_DB: prefect
    volumes:
      - prefect-db:/var/lib/postgresql/data

  prefect-server:
    image: prefecthq/prefect:3-latest
    command: prefect server start --host 0.0.0.0
    ports:
      - "4200:4200"
    environment:
      PREFECT_API_DATABASE_CONNECTION_URL: postgresql+asyncpg://prefect:prefect@prefect-db:5432/prefect
    depends_on:
      - prefect-db

volumes:
  prefect-db:
```

```bash
docker compose up -d
```

---

## Tables

| Table | Description |
|---|---|
| `flows` | All registered flows with name, tags, labels, and creation time |
| `flow_runs` | Run history with state, timing, deployment, and work pool |
| `deployments` | Deployment definitions with schedule, status, entrypoint, and last poll time |
| `work_pools` | Work pool inventory with type, pause status, and concurrency limits |
| `task_runs` | Task-level run history with state and parent flow run link |

## Example queries

```sql
-- Which flows have run recently and what was their outcome?
SELECT f.name, fr.name AS run_name, fr.state_type, fr.total_run_time, fr.start_time
FROM prefect.flows f
JOIN prefect.flow_runs fr ON f.id = fr.flow_id
ORDER BY fr.start_time DESC
LIMIT 20;

-- All failed flow runs
SELECT f.name, fr.id, fr.start_time, fr.end_time, fr.total_run_time
FROM prefect.flow_runs fr
JOIN prefect.flows f ON fr.flow_id = f.id
WHERE fr.state_type = 'FAILED'
ORDER BY fr.start_time DESC;

-- Deployment health — which are ready and which have no workers polling?
SELECT name, status, paused, work_queue_name, last_polled, entrypoint
FROM prefect.deployments
ORDER BY status, name;

-- Work pool overview
SELECT name, type, status, is_paused, active_slots, concurrency_limit
FROM prefect.work_pools;

-- Task-level breakdown for a specific flow run
SELECT name, task_key, state_type, total_run_time, start_time
FROM prefect.task_runs
WHERE flow_run_id = '<flow-run-id>'
ORDER BY start_time;

-- Slowest flows by average run time
SELECT f.name, COUNT(fr.id) AS runs, AVG(fr.total_run_time) AS avg_seconds
FROM prefect.flows f
JOIN prefect.flow_runs fr ON f.id = fr.flow_id
WHERE fr.state_type = 'COMPLETED'
GROUP BY f.name
ORDER BY avg_seconds DESC;
```

## Notes

- **`total_run_time`** is in seconds (Float64). It excludes scheduling wait
  time — only the actual execution window.
- **`labels`** is a JSON key-value dict. Prefect automatically populates
  `prefect.flow.id` on flow runs; you can add custom labels (e.g.
  `env: production`, `team: data-eng`) via deployment configuration.
- **`task_runs`** has very high cardinality — one row per task per flow run.
  Always use `LIMIT` or filter by `flow_run_id` for targeted queries.
- **`flow_runs`** and **`task_runs`** default to the 200 most recent entries.
  Use SQL `LIMIT` or `WHERE state_type = '...'` to narrow results.
- **`work_pools`** status `NOT_READY` means no worker process is currently
  polling. This is normal for on-demand infrastructure; `READY` means a
  worker is actively listening.
- **`deployments`** `last_polled = null` means no worker has checked in for
  this deployment. `status = READY` requires at least one active worker.
