# Checkly

Query synthetic monitoring checks, check results, alert channels, and
dashboards from [Checkly](https://checklyhq.com/) — the synthetic monitoring
platform for API and browser checks.

## Authentication

Checkly requires **two credentials** for every API request:

1. **API Key** — authenticates the request
2. **Account ID** — scopes the request to your account

### Step 1 — Get your API key

- **Personal key**: Checkly dashboard → User Settings → [API Keys](https://app.checklyhq.com/settings/user/api-keys) → Create API key
- **Service key** (recommended for CI/CD): Checkly dashboard → Account Settings → API Keys → Create API key

This source only performs read-only GET requests. Any Checkly API key is sufficient — Checkly does not provide scoped or read-only key variants.

### Step 2 — Get your Account ID

Checkly dashboard → Account Settings → General → **Account ID** (shown at the top of the page).

### Step 3 — Add the source

```sh
export CHECKLY_API_KEY="cu_..."
export CHECKLY_ACCOUNT_ID="12345678"
coral source add --file sources/community/checkly/manifest.yaml
```

See the [Checkly authentication docs](https://developers.checklyhq.com/docs/authentication) for details.

## Tables

| Table | Description | Required filters | Optional pushdown filters |
|---|---|---|---|
| `checkly.checks` | All API and browser checks in the account — inventory, type, frequency, locations | — | `tag`, `check_type`, `search`, `status` |
| `checkly.check_results` | Recent run results for a specific check — pass/fail, response time, run location | `check_id` (required) | `result_type`, `has_failures`, `from`, `to`, `location`, `check_type` |
| `checkly.alert_channels` | Notification channels — Slack, Email, PagerDuty, Webhook, etc. — and their routing config | — | — |
| `checkly.dashboards` | Public and private status page dashboards with domain and tag configuration | — | — |

### `checkly.checks`

Lists all synthetic monitoring checks in the account, newest first. No filter is required, but you can filter results upstream using `tag`, `check_type`, `search`, or `status` pushdown filters.

Key columns:

| Column | Type | Description |
|---|---|---|
| `id` | `Utf8` | Check UUID — use this as `check_id` in `check_results` |
| `name` | `Utf8` | Check display name |
| `check_type` | `Utf8` | `API` · `BROWSER` · `HEARTBEAT` · `DNS` · `TCP` · `MULTI_STEP` · `PLAYWRIGHT` · `AGENTIC` · `ICMP` · `URL` |
| `frequency` | `Int64` | Run interval in minutes |
| `activated` | `Boolean` | `false` = check is paused |
| `muted` | `Boolean` | `true` = alerts suppressed |
| `locations` | `Json` | Array of AWS region strings |
| `tags` | `Json` | Array of tag strings |
| `degraded_response_time` | `Int64` | Degraded threshold in ms |
| `max_response_time` | `Int64` | Failure threshold in ms |
| `created_at` | `Timestamp` | Check creation time |
| `tag` | `Utf8` (Virtual) | Filter checks by a specific tag (pushdown) |
| `search` | `Utf8` (Virtual) | Search checks by name or tag (pushdown) |
| `status` | `Utf8` (Virtual) | Filter checks by their current status (pushdown) |

### `checkly.check_results`

Recent run results for one specific check. **`check_id` is required.** Get check IDs from `checkly.checks`. Raw results are retained for **30 days**.

| Filter | Required | Description |
|---|---|---|
| `check_id` | ✅ Yes | UUID of the check to fetch results for |
| `result_type` | No | `FINAL` (completed runs) or `ATTEMPT` (retry attempts) |
| `has_failures` | No | `true` to return only failing runs |
| `from` | No | UNIX timestamp in **seconds** — lower time bound. Use quoted SQL: `WHERE "from" = '1716768000'` |
| `to` | No | UNIX timestamp in **seconds** — upper time bound. Use quoted SQL: `WHERE "to" = '1716854400'` |
| `location` | No | Filter check results by execution location (e.g. `us-east-1`) |
| `check_type` | No | Filter check results by check type (e.g. `API` or `BROWSER`) |

Key columns:

| Column | Type | Description |
|---|---|---|
| `id` | `Utf8` | Result ID |
| `check_run_id` | `Int64` | Monotonic run sequence number |
| `result_type` | `Utf8` | `FINAL` or `ATTEMPT` |
| `has_failures` | `Boolean` | `true` if assertions failed or timeout occurred |
| `has_errors` | `Boolean` | `true` if a Checkly platform error occurred |
| `run_location` | `Utf8` | AWS region where the check ran |
| `started_at` | `Timestamp` | Run start time |
| `stopped_at` | `Timestamp` | Run completion time |
| `response_time` | `Int64` | Execution time in ms |
| `location` | `Utf8` (Virtual) | Filter check results by execution location — pushed upstream as `location` query param |
| `check_type` | `Utf8` (Virtual) | Filter check results by check type — pushed upstream as `checkType` query param |
| `from` | `Utf8` (Virtual) | Lower time bound (UNIX seconds) — pushed upstream. Use quoted SQL: `WHERE "from" = '...'` |
| `to` | `Utf8` (Virtual) | Upper time bound (UNIX seconds) — pushed upstream. Use quoted SQL: `WHERE "to" = '...'` |

### `checkly.alert_channels`

All notification channels in the account. Each row has a `type` and the
corresponding notification routing flags (`send_failure`, `send_recovery`,
`send_degraded`, `ssl_expiry`).

Known `type` values: `EMAIL`, `SLACK`, `SLACK_APP`, `WEBHOOK`, `SMS`,
`PAGERDUTY`, `OPSGENIE`, `CALL`.

> **Note**: The `config` field (channel-specific JSON configuration) is not
> exposed by this source. It can contain credential material such as webhook
> authorization headers, PagerDuty service keys, and OpsGenie API keys.
> Use the Checkly dashboard to inspect individual channel configuration.

### `checkly.dashboards`

Status page dashboards for the account. `tags` controls which checks appear on each dashboard — only checks with matching tags are displayed.

## Example queries

### Inventory all active checks

```sql
SELECT
  id,
  name,
  check_type,
  frequency,
  locations,
  tags
FROM checkly.checks
WHERE activated = true
ORDER BY name;
```

### Find paused or muted checks

```sql
SELECT
  id,
  name,
  check_type,
  activated,
  muted
FROM checkly.checks
WHERE activated = false OR muted = true;
```

### Get recent failing results for a specific check

```sql
SELECT
  id,
  has_failures,
  run_location,
  response_time,
  started_at,
  stopped_at
FROM checkly.check_results
WHERE check_id = '<your-check-id>'
  AND has_failures = true
  AND result_type = 'FINAL'
LIMIT 50;
```

### Search checks by tag and type (upstream pushdown)

```sql
SELECT
  id,
  name,
  check_type,
  frequency
FROM checkly.checks
WHERE tag = 'production'
  AND check_type = 'BROWSER';
```

### Filter check results by location and type (upstream pushdown)

```sql
SELECT
  id,
  has_failures,
  run_location,
  response_time,
  started_at
FROM checkly.check_results
WHERE check_id = '<your-check-id>'
  AND location = 'us-east-1'
  AND check_type = 'API'
  AND result_type = 'FINAL'
LIMIT 50;
```

### Audit alert channel routing

```sql
SELECT
  id,
  type,
  send_failure,
  send_recovery,
  send_degraded,
  ssl_expiry
FROM checkly.alert_channels
ORDER BY type;
```

### List public status page dashboards

```sql
SELECT
  id,
  custom_url,
  custom_domain,
  is_private,
  tags
FROM checkly.dashboards
WHERE is_private = false;
```

## Rate Limits and Limitations

### Rate Limits
Checkly enforces rate limits to ensure API stability:
* **General API**: 10 requests per second (RPS) or 600 requests per minute (RPM) per account.
* **Check Results Endpoint**: Stricter limit of 5 requests per 10 seconds.

If these limits are exceeded, Checkly returns an HTTP `429 Too Many Requests` status code. Coral will propagate this error back to your client. 

### Large-Query Guidance
To avoid triggering the strict check-results rate limits:
* Always filter check results by a specific time window using the `from` and `to` filters (Unix timestamps in seconds).
* Avoid executing queries that scan all check results without time constraints.

## Auth

This source uses two headers on every request:

| Header | Value | Input kind |
|---|---|---|
| `Authorization` | `Bearer <CHECKLY_API_KEY>` | `secret` |
| `X-Checkly-Account` | `<CHECKLY_ACCOUNT_ID>` | `secret` |

Both credentials are required. Requests without the `X-Checkly-Account` header will be rejected by the API regardless of the API key's validity.

See the [Checkly API overview and authentication](https://www.checklyhq.com/docs/api-reference/overview/) for full documentation.

### Per-endpoint API references

| Table | Endpoint reference |
|---|---|
| `checkly.checks` | [List all checks](https://www.checklyhq.com/docs/api-reference/checks/list-all-checks/) |
| `checkly.check_results` | [List all check results](https://www.checklyhq.com/docs/api-reference/check-results/lists-all-check-results-1/) |
| `checkly.alert_channels` | [List all alert channels](https://www.checklyhq.com/docs/api-reference/alert-channels/list-all-alert-channels/) |
| `checkly.dashboards` | [List all dashboards](https://www.checklyhq.com/docs/api-reference/dashboards/list-all-dashboards/) |
