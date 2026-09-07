# MiniMax community source

Query MiniMax OpenAI-compatible model metadata and run bounded chat-completion
checks through Coral SQL. This source adds MiniMax to the community catalog so
users and agents can inspect available models, verify model configuration, and
smoke test prompts without leaving the Coral workflow.

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 3
**Base URL:** `https://api.minimax.io/v1` (global) or `https://api.minimaxi.com/v1` (China)

## Why this source

MiniMax is a common LLM provider for agent prototypes, chat workloads, and
multimodal experiments. Coral did not have a MiniMax source yet, so this
community spec gives the reef a focused read/query surface for:

- Discovering available MiniMax models from SQL.
- Looking up metadata for one model before using it in an agent or workflow.
- Running a bounded chat-completion prompt as an integration smoke test.
- Joining model metadata with other Coral sources in local analysis workflows.

The v1 surface is intentionally narrow and read-oriented. It proves Coral can
authenticate against MiniMax, call MiniMax's OpenAI-compatible endpoints, map
JSON responses into tables, and validate the source with declared test queries.

## Installation

Community sources are not bundled with the Coral binary. Clone the Coral
repository and add the manifest from this directory:

```bash
coral source add --file sources/community/minimax/manifest.yaml
```

You can also copy `manifest.yaml` into another workspace and pass that path to
`coral source add --file`.

## Authentication

Create or copy an API key from the MiniMax platform. See the platform
documentation for key management:

- Global: https://platform.minimax.io/docs
- China: https://platform.minimaxi.com/docs

Set the key as `MINIMAX_API_KEY` before adding or testing the source. Coral
sends it as a bearer token to MiniMax's API.

```bash
export MINIMAX_API_KEY="your_minimax_api_key"
coral source add --file sources/community/minimax/manifest.yaml
```

Interactive install also works:

```bash
coral source add --interactive --file sources/community/minimax/manifest.yaml
```

### Base URL

The source defaults to the global endpoint `https://api.minimax.io/v1`. To use
the China endpoint, set `MINIMAX_BASE_URL` to `https://api.minimaxi.com/v1`
when adding the source:

```bash
MINIMAX_BASE_URL="https://api.minimaxi.com/v1" coral source add --file sources/community/minimax/manifest.yaml
```

## Provider docs

- MiniMax API overview: https://platform.minimax.io/docs
- MiniMax API overview (China): https://platform.minimaxi.com/docs

## Model metadata

The source targets the following MiniMax chat models:

| Model | Context window | Input price ($/M) | Output price ($/M) | Cache read ($/M) | Cache write ($/M) | Input modalities | Thinking |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `MiniMax-M3` | 1,000,000 | 0.60 | 2.40 | 0.12 | — | text, image, video | adaptive, disabled |
| `MiniMax-M2.7` | 204,800 | 0.30 | 1.20 | 0.06 | 0.375 | text | always_on |

Model metadata that the Models API returns (such as `context_window`) is
exposed as nullable columns on `minimax.models` and `minimax.model`.

## Tables

| Table | Description | Required filters |
| --- | --- | --- |
| `minimax.models` | MiniMax models returned by `GET /models`. | None |
| `minimax.model` | Metadata for one MiniMax model ID. | `model_id` |
| `minimax.chat_completions` | Run one bounded chat-completion request using SQL filters. | `model`, `prompt`, `max_tokens` |

### `minimax.models`

Lists models available to the API key.

```sql
SELECT id, object, owned_by, context_window
FROM minimax.models
LIMIT 20;
```

### `minimax.model`

Fetches metadata for one model from `GET /models/{model_id}`.

```sql
SELECT id, object, owned_by, context_window
FROM minimax.model
WHERE model_id = 'MiniMax-M3';
```

### `minimax.chat_completions`

Runs one bounded, single-turn, non-streaming chat completion through
`POST /chat/completions`. `model`, `prompt`, and `max_tokens` are required so
validation and examples stay bounded.

```sql
SELECT content, finish_reason, reasoning_content, max_tokens, returned_model, total_tokens
FROM minimax.chat_completions
WHERE model = 'MiniMax-M3'
  AND prompt = 'Coral MiniMax works'
  AND max_tokens = 20
LIMIT 1;
```

This table keeps top-level response metadata, including the response ID,
returned model, raw `choices` array, and token usage, and also exposes the
first choice's `content`, `reasoning_content` (thinking output when returned),
`finish_reason`, and `message_role`.

## Validation

Run the source-level checks with a valid `MINIMAX_API_KEY` before opening or
updating a PR. The API key is required for `source add`, `source test`, and live
SQL queries, but it should never be printed or committed.

```bash
coral source lint sources/community/minimax/manifest.yaml

export MINIMAX_API_KEY="your_minimax_api_key"
coral source add --file sources/community/minimax/manifest.yaml
coral source test minimax
```

The declared test queries cover model discovery and a bounded chat-completion
smoke test:

```sql
SELECT id, object, owned_by FROM minimax.models LIMIT 5;

SELECT content
FROM minimax.chat_completions
WHERE model = 'MiniMax-M3'
  AND prompt = 'Coral MiniMax works'
  AND max_tokens = 20
LIMIT 1;
```

## Implementation notes

- Uses Coral source-spec DSL v3 with the HTTP backend.
- Uses `HeaderAuth` with `Authorization: Bearer {{input.MINIMAX_API_KEY}}`.
- Defaults `base_url` to the global endpoint and documents the China endpoint
  through the `MINIMAX_BASE_URL` variable input.
- Maps the `data` array from `GET /models` into `minimax.models`.
- Maps `GET /models/{model_id}` into `minimax.model`.
- Maps `POST /chat/completions` into `minimax.chat_completions`, preserving
  top-level response metadata, the first choice's content and reasoning text,
  and token usage.
- Echoes required SQL filters such as `model`, `prompt`, `max_tokens`, and
  `model_id` back as virtual columns so query results keep their request
  context.
- Does not require runtime, CLI, MCP, or UI changes.

## Limitations

- This source is read/query oriented and does not manage MiniMax account
  settings.
- `chat_completions` performs a live API call for each query.
- The chat table supports one user message per query. It is intended for
  validation and lightweight SQL workflows, not as a full chat client.
- Chat is single-turn and non-streaming in this first version.
- Responses, available models, pricing, thinking modes, input capabilities,
  rate limits, and errors depend on the MiniMax account, API key permissions,
  selected model, and current provider limits.
- Model metadata such as pricing and thinking modes is documented above; only
  the metadata the Models API returns (for example `context_window`) is
  surfaced as live columns.

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md), keep the manifest focused,
and include the validation commands plus proof output in the PR description.
