# Grafana (Community)

**Version:** 0.1.0
**Backend:** HTTP (Grafana REST API)
**Tables:** 3
**Base URL:** `{{input.GRAFANA_BASE_URL}}/api`

Query Grafana dashboards, data sources, and organization folders directly through Coral SQL using the [Grafana HTTP REST API](https://grafana.com/docs/grafana/latest/developers/http_api/).

Use this source for:
- dashboard inventory auditing
- data source visibility inspection
- operational observability reviews
- Grafana organization structure auditing
- monitoring configuration validation

Coral exposes read-only `GET` tables. Write operations (creating dashboards, modifying credentials, deleting folders, managing alert contact points) are out of scope for v1.

---

# Install

Community sources are not bundled with the Coral binary.

```bash
coral source add --file sources/community/grafana/manifest.yaml
```

You may also copy `manifest.yaml` locally and reference it directly.

---

# Inputs

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `GRAFANA_BASE_URL` | variable | yes | Root Grafana URL without trailing slash and without `/api` |
| `GRAFANA_SERVICE_ACCOUNT_TOKEN` | secret | yes | Service Account Token generated from Grafana Administration → Service Accounts |

---

# Authentication

Grafana authentication commonly uses:
- Service Account Tokens
- Administrator API tokens

Example:

```bash
export GRAFANA_BASE_URL=https://grafana.example.com
export GRAFANA_SERVICE_ACCOUNT_TOKEN=<token>
```

Coral authenticates using:

```text
Authorization: Bearer <token>
```

---

# Tables Overview

| Table | API Endpoint | Notes |
| --- | --- | --- |
| `datasources` | `GET /api/datasources` | Configured Grafana data sources |
| `dashboards` | `GET /api/search?type=dash-db` | Dashboard inventory search |
| `folders` | `GET /api/folders` | Dashboard folders |

---

# Table Reference

## `grafana.datasources`

Configured data sources connected to Grafana.

| Column | Type | Description |
| --- | --- | --- |
| `id` | Int64 | Internal data source identifier |
| `uid` | Utf8 | Unique data source identifier |
| `org_id` | Int64 | Organization identifier |
| `name` | Utf8 | Data source name |
| `type` | Utf8 | Data source plugin type |
| `url` | Utf8 | Data source target URL |
| `access` | Utf8 | Access mode (`proxy` or `direct`) |
| `basic_auth` | Boolean | Whether Basic Authentication is enabled |
| `is_default` | Boolean | Whether the data source is the organization default |
| `read_only` | Boolean | Whether the data source is read-only |

---

## `grafana.dashboards`

Dashboards visible within the active Grafana organization.

| Column | Type | Description |
| --- | --- | --- |
| `id` | Int64 | Internal dashboard identifier |
| `uid` | Utf8 | Unique dashboard identifier |
| `title` | Utf8 | Dashboard title |
| `uri` | Utf8 | Dashboard URI path |
| `url` | Utf8 | Dashboard URL path |
| `type` | Utf8 | Dashboard search result type |
| `is_starred` | Boolean | Whether the dashboard is starred |
| `folder_id` | Int64 | Parent folder identifier |
| `folder_uid` | Utf8 | Parent folder UID |
| `folder_title` | Utf8 | Parent folder title |

---

## `grafana.folders`

Dashboard folders within the active Grafana organization.

| Column | Type | Description |
| --- | --- | --- |
| `id` | Int64 | Internal folder identifier |
| `uid` | Utf8 | Unique folder identifier |
| `title` | Utf8 | Folder title |
| `url` | Utf8 | Folder URL path |

---

# Example Queries

## Discover starred dashboards

```sql
SELECT
  title,
  folder_title,
  url
FROM grafana.dashboards
WHERE is_starred = true;
```

---

## Audit Prometheus data sources

```sql
SELECT
  name,
  type,
  url,
  is_default
FROM grafana.datasources
WHERE type = 'prometheus';
```

---

## Folder inventory

```sql
SELECT
  title,
  url
FROM grafana.folders
ORDER BY title;
```

---

# Validation

Run formatting and schema mapping evaluations locally before generating your pull request:

```bash
# YAML and style verification
make lint-sources

# Validate schema structure types against Coral DSL engine rules
coral source lint sources/community/grafana/manifest.yaml
```

Execute a live target connection test locally:

```bash
export GRAFANA_BASE_URL=https://grafana.example.com
export GRAFANA_SERVICE_ACCOUNT_TOKEN=<token>

coral source add --file sources/community/grafana/manifest.yaml

coral source test grafana
```

---

# Representative Live Output Evidence

```text
$ coral source test grafana

  ✓ grafana connected successfully

    grafana (3 tables)
    ├─ datasources
    ├─ dashboards
    └─ folders

    Query tests
    1 declared · 1 passed · 0 failed

  ✓ SELECT id, name FROM grafana.datasources LIMIT 1

    +----+-----------------------+
    | id | name                  |
    +----+-----------------------+
    |  4 | k8s-prometheus-engine |
    +----+-----------------------+

    1 row
```

---

# Representative Query Output

```text
$ coral sql "SELECT title, is_starred FROM grafana.dashboards LIMIT 5"

+--------------------------------+-------------+
| title                          | is_starred |
+--------------------------------+-------------+
| Kubernetes Cluster Overview    | true       |
| Production API Metrics         | false      |
+--------------------------------+-------------+

$ coral sql "SELECT name, type FROM grafana.datasources LIMIT 5"

+--------------------------+-------------+
| name                     | type        |
+--------------------------+-------------+
| prometheus-main          | prometheus  |
| loki-logs                | loki        |
+--------------------------+-------------+
```

---

# Limitations

- Read-only source
- No dashboard creation or modification support
- No alert rule or notification management
- Pagination parameters are not modeled in v1
- Data source visibility depends on Grafana token permissions
- Dashboard and folder visibility depend on organization role bindings and ACLs
- Grafana OSS, Enterprise, and Cloud editions may expose different API behavior
- Only REST API-visible metadata is modeled
