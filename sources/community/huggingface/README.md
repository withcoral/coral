# Hugging Face source

Query [Hugging Face Hub](https://huggingface.co) data through Coral using
the [Hub REST API](https://huggingface.co/docs/hub/en/api).

## Tables

| Table          | Description                                    | Required filters | Key columns |
| -------------- | ---------------------------------------------- | ---------------- | ----------- |
| `models`       | Models published on the Hub                    | —                | `id`, `author`, `pipeline_tag`, `library_name`, `likes`, `downloads`, `trending_score`, `tags`, `siblings` |
| `datasets`     | Datasets published on the Hub                  | —                | `id`, `author`, `likes`, `downloads`, `trending_score`, `description`, `tags`, `card_data`, `siblings` |
| `spaces`       | Spaces (apps and demos) on the Hub             | —                | `id`, `author`, `sdk`, `subdomain`, `likes`, `trending_score`, `tags`, `card_data`, `siblings` |
| `daily_papers` | Community-curated daily research papers        | —                | `paper_id`, `title`, `summary`, `ai_summary`, `ai_keywords`, `authors`, `github_repo`, `upvotes` |
| `trending`     | Currently trending repos across all types      | —                | `repo_id`, `repo_type`, `author__name`, `author__fullname`, `likes`, `downloads`, `pipeline_tag` |

## Authentication

This source uses **Hugging Face User Access Tokens** authenticated via
`Authorization: Bearer <token>`.

Create a token at
[huggingface.co/settings/tokens](https://huggingface.co/settings/tokens)
with at least `read` scope. A read-only token is sufficient for all
tables in this source.

Set the token as `HF_TOKEN` when adding this source.

## Setup

```bash
coral source add --file sources/community/huggingface/manifest.yaml
```

Then configure your token when prompted, or set the `HF_TOKEN`
environment variable.

## Example queries

### List trending models

```sql
SELECT id, pipeline_tag, likes, downloads, trending_score
  FROM huggingface.models
 LIMIT 10;
```

### Search for text-generation models

```sql
SELECT id, author, likes, downloads
  FROM huggingface.models
 WHERE pipeline_tag = 'text-generation'
 LIMIT 10;
```

### Find models by organization

```sql
SELECT id, pipeline_tag, likes, downloads, created_at
  FROM huggingface.models
 WHERE author = 'meta-llama';
```

### Search models by keyword

```sql
SELECT id, likes, downloads, pipeline_tag, library_name
  FROM huggingface.models
 WHERE search = 'llama';
```

### Find transformer models

```sql
SELECT id, author, downloads, likes
  FROM huggingface.models
 WHERE filter = 'transformers'
 LIMIT 20;
```

### List popular datasets

```sql
SELECT id, author, likes, downloads, description
  FROM huggingface.datasets
 LIMIT 10;
```

### Search datasets by keyword

```sql
SELECT id, likes, downloads, description
  FROM huggingface.datasets
 WHERE search = 'code';
```

### Inspect dataset card metadata

```sql
SELECT id, card_data
  FROM huggingface.datasets
 WHERE author = 'huggingface'
 LIMIT 5;
```

### List Gradio Spaces

```sql
SELECT id, sdk, likes, subdomain
  FROM huggingface.spaces
 WHERE filter = 'gradio'
 LIMIT 10;
```

### Find MCP server Spaces

```sql
SELECT id, author, likes, card_data
  FROM huggingface.spaces
 WHERE filter = 'mcp-server'
 LIMIT 10;
```

### Browse today's papers with AI summaries

```sql
SELECT paper_id, title, ai_summary, upvotes, num_comments
  FROM huggingface.daily_papers
 LIMIT 10;
```

### Find papers with GitHub repos

```sql
SELECT paper_id, title, github_repo, upvotes
  FROM huggingface.daily_papers
 WHERE github_repo IS NOT NULL
 LIMIT 20;
```

### Papers by organization

```sql
SELECT paper_id, title, organization__fullname, upvotes
  FROM huggingface.daily_papers
 LIMIT 20;
```

### See what's trending

```sql
SELECT repo_id, repo_type, author__fullname, likes, downloads
  FROM huggingface.trending;
```

### Trending models only

```sql
SELECT repo_id, likes, downloads, pipeline_tag
  FROM huggingface.trending
 WHERE type = 'model';
```

## Filters

### models

| Filter         | Required | Description |
| -------------- | -------- | ----------- |
| `search`       | No       | Keyword search across model names and metadata |
| `author`       | No       | Filter by author or organization namespace |
| `pipeline_tag` | No       | Filter by task (e.g. text-generation, image-classification) |
| `filter`       | No       | Tag-based filter (e.g. transformers, license:apache-2.0) |
| `sort`         | No       | Sort field (trendingScore, likes, downloads, lastModified) |
| `direction`    | No       | Sort direction (-1 for descending; ascending is not supported for all fields) |

### datasets

| Filter      | Required | Description |
| ----------- | -------- | ----------- |
| `search`    | No       | Keyword search across dataset names and metadata |
| `author`    | No       | Filter by author or organization namespace |
| `filter`    | No       | Tag-based filter (e.g. task_categories:question-answering) |
| `sort`      | No       | Sort field |
| `direction` | No       | Sort direction (-1 for descending; ascending not supported for all fields) |

### spaces

| Filter      | Required | Description |
| ----------- | -------- | ----------- |
| `search`    | No       | Keyword search across Space names and metadata |
| `author`    | No       | Filter by author or organization namespace |
| `filter`    | No       | Tag-based filter (e.g. gradio, mcp-server) |
| `sort`      | No       | Sort field |
| `direction` | No       | Sort direction (-1 for descending; ascending not supported for all fields) |

### trending

| Filter | Required | Description |
| ------ | -------- | ----------- |
| `type` | No       | Filter by repo type: model, dataset, or space |

## Pagination

The Hugging Face API uses **Link header** pagination (`rel="next"`).
The `models`, `datasets`, and `spaces` tables use `link_header` mode
with pages of 100 items.

The `daily_papers` table uses `mode: none` and returns up to 100
recent papers in a single request. This avoids unbounded pagination
through the full paper archive.

The `trending` table also uses `mode: none`. The API returns up to
60 trending items in a single response (20 models + 20 datasets +
20 spaces). When filtered by `type`, it returns up to 20 items.

## JSON columns

Several columns return structured JSON data for deeper inspection:

- **`siblings`** (models, datasets, spaces) — repository file listing as
  a JSON array. Each entry has an `rfilename` field.
- **`card_data`** (datasets, spaces) — full card YAML metadata as JSON,
  including license, language, task categories, and configuration.
- **`authors`** (daily_papers) — JSON array of paper author objects with
  `name` and `_id` fields.

## Known limitations

- **Read-only** — no create, update, or delete operations.
- **Rate limits** — the Hub API has rate limits that depend on account
  tier. See [rate limits docs](https://huggingface.co/docs/hub/en/rate-limits).
- **Tags as strings** — the `tags` column is a comma-separated string
  because the API returns tags as a JSON array. Use SQL `LIKE` to filter.
- **No repo-level detail** — this source covers Hub-level listing and
  discovery. Repo-specific endpoints (files, commits, discussions) are
  not included in v1.
- **Trending scope** — the `trending` table returns the Hub's curated
  trending list, not all repos sorted by score.
- **Gated field type** — the `gated` field is typed as `Utf8` because
  the API returns either `false` (boolean) or a string like `"auto"` /
  `"manual"`. Both are represented as strings.
- **Daily papers scope** — the `daily_papers` table returns up to 100
  recent papers per query without pagination. Client-side filters
  (e.g. `IS NOT NULL`) work within that batch.
