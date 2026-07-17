# Langfuse

**Version:** 0.2.0
**Backend:** HTTP (Langfuse Public API)
**Tables:** 5
**Base URL:** `https://cloud.langfuse.com` (override with `LANGFUSE_BASE_URL`)

Query traces, observations, scores, sessions, and projects from Langfuse
(Cloud or self-hosted). Join with Linear, GitHub, and Sentry for cross-source
AI cost attribution — link every dollar of LLM spend to the feature it shipped.

## Tables

| Table | Description | Optional filters |
|---|---|---|
| `langfuse.projects` | Projects accessible with the API keys | — |
| `langfuse.traces` | LLM application traces with cost, latency, and metadata | `name`, `user_id`, `session_id`, `tags`, `environment`, `from_timestamp`, `to_timestamp` |
| `langfuse.observations` | Spans, generations, and events within traces | `trace_id`, `type`, `name`, `user_id`, `environment`, `from_start_time`, `to_start_time` |
| `langfuse.scores` | Evaluation scores on traces or observations | `trace_id`, `observation_id`, `name`, `data_type`, `environment`, `from_timestamp`, `to_timestamp` |
| `langfuse.sessions` | Multi-turn conversation sessions | `environment`, `from_timestamp`, `to_timestamp` |

## Authentication

Langfuse uses HTTP Basic Auth. The public key is the username and the secret
key is the password. Both are available in your project settings under
**Settings -> API Keys**.

```bash
LANGFUSE_BASE_URL=https://cloud.langfuse.com \
LANGFUSE_PUBLIC_KEY=pk-lf-... \
LANGFUSE_SECRET_KEY=sk-lf-... \
coral source add --file sources/community/langfuse/manifest.yaml
```

### Cloud regions

| Region | Base URL |
|---|---|
| EU Cloud (default) | `https://cloud.langfuse.com` |
| US Cloud | `https://us.cloud.langfuse.com` |
| Japan Cloud | `https://jp.cloud.langfuse.com` |
| Self-hosted | Your instance URL |

### Self-hosted

Set `LANGFUSE_BASE_URL` to your instance URL:

```bash
LANGFUSE_BASE_URL=http://localhost:3000 \
LANGFUSE_PUBLIC_KEY=pk-lf-... \
LANGFUSE_SECRET_KEY=sk-lf-... \
coral source add --file sources/community/langfuse/manifest.yaml
```

## Validation

```bash
# Add and run test queries (output sanitized)
coral source add --file sources/community/langfuse/manifest.yaml
# coral source test langfuse produces the same output
```

```text
  ✓ langfuse connected successfully
  Secrets: keyring

    langfuse (5 tables)
    ├─ observations
    ├─ projects
    ├─ scores
    ├─ sessions
    └─ traces

    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT id, name FROM langfuse.projects LIMIT 1
      1 row

    ✓ SELECT id, name, total_cost FROM langfuse.traces ORDER BY timestamp DESC LIMIT 1
      1 row
```

```bash
# Cost breakdown by model (output sanitized)
coral sql "
  SELECT model, COUNT(*) AS calls,
    SUM(input_tokens) AS total_input,
    SUM(output_tokens) AS total_output,
    ROUND(SUM(total_cost), 4) AS total_cost_usd
  FROM langfuse.observations
  WHERE type = 'GENERATION'
  GROUP BY model
  ORDER BY total_cost_usd DESC
  LIMIT 5
"
```

```text
+--------------------+-------+-------------+--------------+----------------+
| model              | calls | total_input | total_output | total_cost_usd |
+--------------------+-------+-------------+--------------+----------------+
| claude-opus-4      |   312 |     4200000 |       890000 |        29.0400 |
| claude-sonnet-4    |   847 |     1100000 |       340000 |         5.2100 |
| claude-haiku-4     |   890 |      420000 |       180000 |         0.2100 |
| gpt-4o             |   124 |      280000 |        92000 |         3.8600 |
+--------------------+-------+-------------+--------------+----------------+
4 rows
```

## Example queries

### Confirm connectivity

```sql
SELECT id, name FROM langfuse.projects;
```

### Recent traces with cost and latency

```sql
SELECT id, name, user_id, latency, total_cost, timestamp
FROM langfuse.traces
ORDER BY timestamp DESC
LIMIT 20;
```

### Filter by time window

```sql
SELECT id, name, total_cost, latency, timestamp
FROM langfuse.traces
WHERE from_timestamp = '2026-05-01T00:00:00Z'
  AND to_timestamp   = '2026-05-31T23:59:59Z'
ORDER BY total_cost DESC
LIMIT 50;
```

### All LLM generations with token usage and cost

```sql
SELECT id, trace_id, name, model, latency,
       input_tokens, output_tokens, total_cost
FROM langfuse.observations
WHERE type = 'GENERATION'
ORDER BY created_at DESC
LIMIT 20;
```

### Filter generations by model and time window

The time-window predicate is pushed to Langfuse to bound the fetched rows.
The `model` predicate is then evaluated locally by Coral.

```sql
SELECT name, model, input_tokens, output_tokens, total_cost, start_time
FROM langfuse.observations
WHERE type = 'GENERATION'
  AND model = 'claude-opus-4'
  AND from_start_time = '2026-05-01T00:00:00Z'
ORDER BY total_cost DESC
LIMIT 20;
```

### Observations for a specific trace

```sql
SELECT id, type, name, model, latency, total_cost, level
FROM langfuse.observations
WHERE trace_id = 'your-trace-id'
ORDER BY start_time;
```

### Cost breakdown by model

```sql
SELECT model,
       COUNT(*)          AS calls,
       SUM(input_tokens) AS total_input,
       SUM(output_tokens) AS total_output,
       ROUND(SUM(total_cost), 4) AS total_cost_usd
FROM langfuse.observations
WHERE type = 'GENERATION'
GROUP BY model
ORDER BY total_cost_usd DESC;
```

### Cost breakdown by trace name (AI operation)

```sql
SELECT name,
       COUNT(*)          AS trace_count,
       ROUND(SUM(total_cost), 4) AS total_cost_usd,
       ROUND(AVG(latency), 2)    AS avg_latency_s
FROM langfuse.traces
GROUP BY name
ORDER BY total_cost_usd DESC
LIMIT 20;
```

### Evaluation scores for a trace

```sql
SELECT name, value, string_value, data_type, source, comment
FROM langfuse.scores
WHERE trace_id = 'your-trace-id';
```

### Average score by name across all traces

```sql
SELECT name,
       ROUND(AVG(value), 3) AS avg_score,
       COUNT(*)              AS count
FROM langfuse.scores
WHERE data_type = 'NUMERIC'
GROUP BY name
ORDER BY avg_score DESC;
```

Use `from_timestamp` and `to_timestamp` to bound score history in large
projects:

```sql
SELECT name,
       ROUND(AVG(value), 3) AS avg_score,
       COUNT(*)              AS count
FROM langfuse.scores
WHERE data_type = 'NUMERIC'
  AND from_timestamp = '2026-05-01T00:00:00Z'
  AND to_timestamp   = '2026-05-31T23:59:59Z'
GROUP BY name
ORDER BY avg_score DESC;
```

### Sessions ordered by creation time

```sql
SELECT id, created_at, project_id
FROM langfuse.sessions
WHERE from_timestamp = '2026-05-01T00:00:00Z'
  AND to_timestamp   = '2026-05-31T23:59:59Z'
ORDER BY created_at DESC
LIMIT 10;
```

### Traces within a session

```sql
SELECT id, name, total_cost, latency, timestamp
FROM langfuse.traces
WHERE session_id = 'your-session-id'
ORDER BY timestamp;
```

### Loop waste — traces with high observation counts

```sql
SELECT t.name        AS operation,
       t.session_id,
       COUNT(o.id)   AS loop_iterations,
       ROUND(SUM(o.total_cost), 4) AS wasted_cost_usd,
       MIN(t.timestamp) AS session_start
FROM langfuse.traces t
JOIN langfuse.observations o
  ON o.trace_id = t.id
  AND o.type = 'GENERATION'
GROUP BY t.name, t.session_id
HAVING COUNT(o.id) > 10
ORDER BY wasted_cost_usd DESC
LIMIT 20;
```

### Model mismatch — expensive models on small tasks

```sql
SELECT name            AS operation,
       model,
       ROUND(AVG(CAST(total_tokens AS DOUBLE)), 0) AS avg_tokens,
       ROUND(AVG(total_cost), 6)                   AS avg_cost_per_call,
       COUNT(*)                                     AS calls_7d,
       ROUND(SUM(total_cost), 2)                   AS total_cost_usd
FROM langfuse.observations
WHERE type   = 'GENERATION'
  AND model IN ('claude-opus-4', 'claude-opus-4-5', 'gpt-4o', 'gpt-4-turbo')
  AND total_tokens < 3000
GROUP BY name, model
HAVING COUNT(*) > 5
ORDER BY total_cost_usd DESC
LIMIT 20;
```

### Extract custom metadata fields

Instrument your application to attach metadata to traces:

```python
# Python SDK example
langfuse.trace(
    name="billing-agent",
    metadata={
        "linearId":   "LIN-4821",
        "featureTag": "billing-agent",
        "team":       "growth",
    }
)
```

Then query with `json_get_str`:

```sql
SELECT name,
       json_get_str(metadata, 'linearId')   AS linear_ticket,
       json_get_str(metadata, 'featureTag') AS feature_tag,
       json_get_str(metadata, 'team')       AS team,
       ROUND(SUM(total_cost), 4)            AS cost_usd,
       COUNT(*)                             AS trace_count
FROM langfuse.traces
GROUP BY name,
         json_get_str(metadata, 'linearId'),
         json_get_str(metadata, 'featureTag'),
         json_get_str(metadata, 'team')
ORDER BY cost_usd DESC
LIMIT 20;
```

## Cross-source examples

### AI cost attributed to Linear tickets (requires `linear` source)

Links every dollar of LLM spend to the Linear ticket it was built for, using
the custom `linearId` field your app sets in trace metadata:

```sql
SELECT t.name                                AS ai_operation,
       json_get_str(t.metadata, 'linearId') AS linear_id,
       li.title                              AS feature_title,
       li.state_name                         AS ticket_state,
       li.assignee_name                      AS owner,
       ROUND(SUM(t.total_cost), 4)          AS cost_usd,
       COUNT(t.id)                           AS trace_count
FROM langfuse.traces t
LEFT JOIN linear.issues li
  ON li.identifier = json_get_str(t.metadata, 'linearId')
GROUP BY t.name,
         json_get_str(t.metadata, 'linearId'),
         li.title, li.state_name, li.assignee_name
ORDER BY cost_usd DESC
LIMIT 20;
```

### Model cost spike correlated with GitHub merges (requires `github` source)

Detects whether a code change introduced a more expensive model or a runaway
agent loop. `github.pulls` requires `owner` and `repo` WHERE predicates:

```sql
SELECT g.number     AS pr_number,
       g.title      AS pr_title,
       g.merged_at,
       g.user__login AS author,
       COUNT(o.id)  AS generations_after_merge,
       ROUND(SUM(o.total_cost), 4) AS cost_after_merge_usd
FROM github.pulls g
JOIN langfuse.observations o
  ON o.type = 'GENERATION'
  AND CAST(o.start_time AS TIMESTAMP) >= CAST(g.merged_at AS TIMESTAMP)
  AND CAST(o.start_time AS TIMESTAMP) <= CAST(g.merged_at AS TIMESTAMP) + INTERVAL '24 hours'
WHERE g.owner = 'your-org'
  AND g.repo  = 'your-repo'
  AND g.state = 'closed'
  AND CAST(g.merged_at AS TIMESTAMP) >= CAST(CURRENT_DATE AS TIMESTAMP) - INTERVAL '14 days'
GROUP BY g.number, g.title, g.merged_at, g.user__login
ORDER BY cost_after_merge_usd DESC
LIMIT 20;
```

### Sentry errors correlated with LLM operations (requires `sentry` source)

Detects production errors that appeared within one hour of a high-cost LLM
trace — useful for identifying regressions introduced by AI-generated code:

```sql
SELECT t.name            AS ai_operation,
       ROUND(t.total_cost, 4) AS trace_cost_usd,
       t.timestamp        AS trace_time,
       s.title            AS sentry_error,
       s.level,
       s.count            AS error_occurrences,
       s.first_seen
FROM langfuse.traces t
JOIN sentry.issues s
  ON CAST(s.first_seen AS TIMESTAMP) >= CAST(t.timestamp AS TIMESTAMP)
  AND CAST(s.first_seen AS TIMESTAMP) <= CAST(t.timestamp AS TIMESTAMP) + INTERVAL '1 hour'
WHERE s.query = 'is:unresolved'
  AND t.total_cost > 0.10
ORDER BY t.total_cost DESC
LIMIT 20;
```

## Discovery order

```text
projects
  id

traces
  id (trace_id)  -> observations (WHERE trace_id = '...')
  id (trace_id)  -> scores       (WHERE trace_id = '...')
  session_id     -> sessions     (WHERE id = '...')
  metadata       -> linear.issues (WHERE identifier = json_get_str(metadata, 'linearId'))

observations
  trace_id       -> traces (WHERE id = '...')
  id             -> scores (WHERE observation_id = '...')
```

## Notes

- `traces.total_cost` is the sum of all observations in the trace. `observations.total_cost` is the cost for a single LLM call.
- `traces.latency` and `observations.latency` are in **seconds** (not milliseconds).
- `traces.metadata` is a JSON object your application attaches at trace creation. Use `json_get_str(metadata, 'key')` to extract fields. This is the primary mechanism for linking traces to Linear tickets, GitHub PRs, or other business context.
- `traces.input` and `traces.output` contain the raw LLM input/output payload. These can be large; avoid selecting them in high-volume aggregation queries.
- `fetch_limit_default` is 500 for traces, 1000 for observations, 500 for scores, and 200 for sessions. Use `from_timestamp` / `to_timestamp` for traces, scores, and sessions, or `from_start_time` / `to_start_time` for observations, to work within a specific time window for large projects.
- All timestamp columns are ISO 8601 strings. Cast with `CAST(timestamp AS TIMESTAMP)` for date arithmetic.
- Rate limits vary by Langfuse plan. The connector handles `429` responses automatically via `Retry-After`.

## API reference

- [Traces API](https://api.reference.langfuse.com/#tag--Trace)
- [Observations API](https://api.reference.langfuse.com/#tag--Observations)
- [Scores API](https://api.reference.langfuse.com/#tag--Score)
- [Sessions API](https://api.reference.langfuse.com/#tag--Sessions)
- [Projects API](https://api.reference.langfuse.com/#tag--Projects)
- [Authentication](https://langfuse.com/docs/api)
