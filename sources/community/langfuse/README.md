# Langfuse

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 5
**Base URL:** `https://cloud.langfuse.com` (override with `LANGFUSE_BASE_URL`)

Query traces, observations, scores, sessions, and projects from Langfuse
(Cloud or self-hosted).

## Authentication

Langfuse uses HTTP Basic Auth. The public key is the username and the secret
key is the password. Both are available in your project settings under
**Settings → API Keys**.

```bash
LANGFUSE_BASE_URL=https://cloud.langfuse.com \
LANGFUSE_PUBLIC_KEY=pk-lf-... \
LANGFUSE_SECRET_KEY=sk-lf-... \
  coral source add --file sources/community/langfuse/manifest.yaml
```

Or interactively:

```bash
LANGFUSE_BASE_URL=https://cloud.langfuse.com \
LANGFUSE_PUBLIC_KEY=pk-lf-... \
LANGFUSE_SECRET_KEY=sk-lf-... \
  coral source add --file sources/community/langfuse/manifest.yaml --interactive
```

### Self-hosted

Set `LANGFUSE_BASE_URL` to your instance URL:

```bash
LANGFUSE_BASE_URL=http://localhost:3000 \
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

## Tables

| Table | Description | Optional filters |
|---|---|---|
| `projects` | Projects accessible with the API keys | — |
| `traces` | LLM application traces | `name`, `user_id`, `session_id`, `tags`, `environment` |
| `observations` | Spans, generations, and events within traces | `trace_id`, `type`, `name`, `user_id`, `environment` |
| `scores` | Evaluation scores on traces or observations | `trace_id`, `observation_id`, `name`, `data_type`, `environment` |
| `sessions` | Multi-turn conversation sessions | `environment` |

## Quick start

```bash
# Confirm connectivity
coral sql "SELECT id, name FROM langfuse.projects"

# Recent traces with cost and latency
coral sql "
  SELECT id, name, user_id, latency, total_cost, timestamp
  FROM langfuse.traces
  ORDER BY timestamp DESC
  LIMIT 20
"

# Filter traces by name (use case)
coral sql "
  SELECT id, user_id, latency, total_cost, timestamp
  FROM langfuse.traces
  WHERE name = 'my-pipeline'
  ORDER BY timestamp DESC
  LIMIT 20
"

# All LLM generations with token usage and cost
coral sql "
  SELECT id, trace_id, name, model, latency, input_tokens, output_tokens, total_cost
  FROM langfuse.observations
  WHERE type = 'GENERATION'
  ORDER BY created_at DESC
  LIMIT 20
"

# Observations for a specific trace
coral sql "
  SELECT id, type, name, model, latency, total_cost, level
  FROM langfuse.observations
  WHERE trace_id = '<your-trace-id>'
  ORDER BY start_time
"

# Evaluation scores for a trace
coral sql "
  SELECT name, value, string_value, data_type, source, comment
  FROM langfuse.scores
  WHERE trace_id = '<your-trace-id>'
"

# Average score by name
coral sql "
  SELECT name, AVG(value) as avg_score, COUNT(*) as count
  FROM langfuse.scores
  WHERE data_type = 'NUMERIC'
  GROUP BY name
  ORDER BY avg_score DESC
"

# Cost breakdown by model
coral sql "
  SELECT model, COUNT(*) as calls,
    SUM(input_tokens) as total_input,
    SUM(output_tokens) as total_output,
    SUM(total_cost) as total_cost_usd
  FROM langfuse.observations
  WHERE type = 'GENERATION'
  GROUP BY model
  ORDER BY total_cost_usd DESC
"

# Sessions ordered by creation time
coral sql "SELECT id, created_at, project_id FROM langfuse.sessions ORDER BY created_at DESC LIMIT 10"
```

## Discovery order

```text
projects
  → id (project context)

traces
  → id (trace_id)
    → observations (WHERE trace_id = '...')
    → scores (WHERE trace_id = '...')

sessions
  → id
    → traces (WHERE session_id = '...')
```
