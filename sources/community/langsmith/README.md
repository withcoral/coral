# LangSmith Community Source

Query LangSmith projects, runs, datasets, examples, and feedback through Coral
SQL using the [LangSmith API](https://api.smith.langchain.com/redoc).

## Setup

### 1. Create a LangSmith API key

Create an API key from LangSmith settings. Use a key with read access to the
workspace resources you plan to inspect.

### 2. Add the source

```bash
export LANGSMITH_API_KEY="<your-key>"
coral source add --file sources/community/langsmith/manifest.yaml
```

For self-hosted LangSmith, set `LANGSMITH_API_BASE`:

```bash
export LANGSMITH_API_BASE="https://langsmith.example.com"
```

### 3. Verify

```bash
coral source test langsmith
```

The default test query reads `langsmith.info`, which validates the server API
shape. Query authenticated tables such as `projects` or `datasets` to validate
workspace access.

## Tables

### `langsmith.info`

Server and deployment metadata.

| Column | Type | Description |
|---|---|---|
| `version` | Utf8 | LangSmith server version |
| `git_sha` | Utf8 | Server build Git SHA |
| `license_expiration_time` | Timestamp | License expiration time |
| `instance_flags` | Json | Deployment feature flags |
| `batch_ingest_config` | Json | Batch ingest configuration |
| `customer_info` | Json | Customer information |

### `langsmith.projects`

Tracing projects, also called tracer sessions.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Project ID |
| `name` | Utf8 | Project name |
| `description` | Utf8 | Project description |
| `tenant_id` | Utf8 | Tenant ID |
| `reference_dataset_id` | Utf8 | Linked reference dataset |
| `default_dataset_id` | Utf8 | Linked default dataset |
| `start_time` | Timestamp | Project start time |
| `end_time` | Timestamp | Project end time |
| `last_run_start_time` | Timestamp | Last run start time |
| `run_count` | Int64 | Number of runs |
| `total_tokens` | Int64 | Total token count |
| `prompt_tokens` | Int64 | Prompt token count |
| `completion_tokens` | Int64 | Completion token count |
| `total_cost` | Float64 | Total traced cost |
| `prompt_cost` | Float64 | Prompt cost |
| `completion_cost` | Float64 | Completion cost |
| `latency_p50` | Float64 | p50 latency |
| `latency_p99` | Float64 | p99 latency |
| `error_rate` | Float64 | Error rate |
| `feedback_stats` | Json | Aggregated feedback stats |
| `extra` | Json | Extra project metadata |

**Optional filters:** `name`, `name_contains`, `reference_free`,
`reference_dataset_id`, `metadata`, `filter`, `include_stats`

### `langsmith.runs`

Runs queried from LangSmith tracing projects.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Run ID |
| `name` | Utf8 | Run name |
| `run_type` | Utf8 | Run type |
| `status` | Utf8 | Run status |
| `error` | Utf8 | Error text |
| `start_time` | Timestamp | Start time |
| `end_time` | Timestamp | End time |
| `first_token_time` | Timestamp | First token time |
| `session_id` | Utf8 | Project/session ID |
| `trace_id` | Utf8 | Trace ID |
| `parent_run_id` | Utf8 | Parent run ID |
| `reference_example_id` | Utf8 | Reference example ID |
| `reference_dataset_id` | Utf8 | Reference dataset ID |
| `total_tokens` | Int64 | Total tokens |
| `prompt_tokens` | Int64 | Prompt tokens |
| `completion_tokens` | Int64 | Completion tokens |
| `total_cost` | Float64 | Total cost |
| `prompt_cost` | Float64 | Prompt cost |
| `completion_cost` | Float64 | Completion cost |
| `inputs` | Json | Run inputs |
| `outputs` | Json | Run outputs |
| `events` | Json | Run events |
| `tags` | Json | Run tags |
| `feedback_stats` | Json | Feedback stats |
| `extra` | Json | Extra run metadata |

**Optional filters:** `project_id`, `run_type`, `trace_id`, `parent_run_id`,
`reference_example_id`, `start_time`, `end_time`, `error`, `query`, `filter`,
`trace_filter`, `tree_filter`, `is_root`, `order`

### `langsmith.datasets`

Datasets used for evaluation and testing.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Dataset ID |
| `name` | Utf8 | Dataset name |
| `description` | Utf8 | Dataset description |
| `data_type` | Utf8 | Dataset data type |
| `tenant_id` | Utf8 | Tenant ID |
| `example_count` | Int64 | Number of examples |
| `session_count` | Int64 | Number of sessions |
| `created_at` | Timestamp | Creation time |
| `modified_at` | Timestamp | Modification time |
| `last_session_start_time` | Timestamp | Last linked session start time |
| `metadata` | Json | Dataset metadata |
| `inputs_schema_definition` | Json | Input schema |
| `outputs_schema_definition` | Json | Output schema |

**Optional filters:** `name`, `name_contains`, `data_type`, `metadata`

### `langsmith.examples`

Examples in LangSmith datasets.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Example ID |
| `dataset_id` | Utf8 | Dataset ID |
| `source_run_id` | Utf8 | Source run ID |
| `name` | Utf8 | Example name |
| `created_at` | Timestamp | Creation time |
| `modified_at` | Timestamp | Modification time |
| `inputs` | Json | Example inputs |
| `outputs` | Json | Example outputs |
| `metadata` | Json | Example metadata |
| `attachment_urls` | Json | Attachment URLs |

**Optional filters:** `dataset_id`, `metadata`, `full_text_contains`, `splits`,
`as_of`, `order`, `filter`

### `langsmith.feedback`

Feedback attached to runs, traces, sessions, or comparative experiments.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Feedback ID |
| `run_id` | Utf8 | Run ID |
| `trace_id` | Utf8 | Trace ID |
| `session_id` | Utf8 | Session/project ID |
| `key` | Utf8 | Feedback key |
| `score` | Float64 | Numeric score |
| `value` | Utf8 | String value |
| `comment` | Utf8 | Feedback comment |
| `correction` | Json | Correction payload |
| `feedback_source` | Json | Feedback source metadata |
| `feedback_group_id` | Utf8 | Feedback group ID |
| `feedback_thread_id` | Utf8 | Feedback thread ID |
| `comparative_experiment_id` | Utf8 | Comparative experiment ID |
| `is_root` | Boolean | Whether this is root feedback |
| `created_at` | Timestamp | Creation time |
| `modified_at` | Timestamp | Modification time |
| `start_time` | Timestamp | Feedback start time |
| `extra` | Json | Extra metadata |

**Optional filters:** `run_id`, `trace_id`, `session_id`, `key`, `source`,
`has_comment`, `has_score`, `level`, `min_created_at`, `max_created_at`

## Example Queries

```sql
-- Inspect projects by recent activity
SELECT name, run_count, total_tokens, total_cost, last_run_start_time
FROM langsmith.projects
WHERE include_stats = true
ORDER BY last_run_start_time DESC
LIMIT 20;

-- Find recent errored LLM runs
SELECT name, run_type, status, error, start_time, total_tokens, total_cost
FROM langsmith.runs
WHERE run_type = 'llm' AND error = true
ORDER BY start_time DESC
LIMIT 20;

-- Review datasets and example counts
SELECT name, data_type, example_count, session_count, modified_at
FROM langsmith.datasets
ORDER BY modified_at DESC
LIMIT 20;

-- Pull examples for a dataset
SELECT name, inputs, outputs
FROM langsmith.examples
WHERE dataset_id = '00000000-0000-0000-0000-000000000000'
LIMIT 10;

-- Inspect low-scoring feedback
SELECT key, score, comment, run_id, created_at
FROM langsmith.feedback
WHERE has_score = true
ORDER BY created_at DESC
LIMIT 20;
```

## Validation

```bash
export LANGSMITH_API_KEY="<your-key>"
coral source lint sources/community/langsmith/manifest.yaml
coral source add --file sources/community/langsmith/manifest.yaml
coral source test langsmith
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'langsmith'"
coral sql "SELECT * FROM coral.columns WHERE schema_name = 'langsmith'"
coral sql "SELECT name, run_count FROM langsmith.projects LIMIT 5"
```

## Limitations

- **Read-only.** This source does not create, update, delete, share, or run
  LangSmith resources.
- **Workspace permissions.** Results depend on the API key's workspace access.
- **Large JSON fields.** Inputs, outputs, events, feedback stats, schemas, and
  metadata are exposed as JSON for flexible inspection.

## Out of scope for v1

- Annotation queues
- Prompt hub resources
- Automations and rules
- Comparative experiment details
- Dataset export formats
- Write operations
