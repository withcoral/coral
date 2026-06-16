# Lynx

[Lynx](https://github.com/lorenzo-cambiaghi/LynxMCP) is a **100% local** semantic
code-search engine (an MCP server) for codebases, library docs, and PDFs. This
source exposes its local API as SQL, so you can query your code by *behavior*
and **join the results with live data** from other Coral sources — without a
byte leaving your machine.

- `lynx.sources` — the sources you've indexed (codebases / docs sites / PDFs).
- `lynx.search(q => '...')` — ranked hybrid (dense + BM25) search. Each row
  carries `file`, `file_path`, `symbol`, `kind`, `language`,
  `start_line`/`end_line`, `score`, and the code `content` itself.

## Setup

1. Install and run Lynx locally (it serves the API on `127.0.0.1`):

   ```bash
   pipx install lynx-mcp        # or: uv tool install lynx-mcp
   lynx manager init            # point it at your codebase
   lynx build                   # index it
   lynx manager ui --port 8765 --no-browser
   ```

2. Register this source. The only input is `LYNX_PORT` (default `8765`):

   ```bash
   coral source add --file sources/community/lynx/manifest.yaml
   coral source test lynx
   ```

## Inputs

| Input | Required | Default | Description |
|-------|----------|---------|-------------|
| `LYNX_PORT` | no | `8765` | Port of the local Lynx API (`lynx manager ui --port 8765`). |

## Examples

Find code by behavior, not keywords:

```sql
SELECT file, symbol, score
FROM lynx.search(q => 'where the camera zoom is clamped')
WHERE language = 'c_sharp'
ORDER BY score DESC
LIMIT 5;
```

Line up a code search against the repo's open PRs:

```sql
SELECT s.file, s.symbol, s.score, p.html_url
FROM lynx.search(q => 'retry logic for payment webhooks') s
CROSS JOIN github.pulls p
WHERE p.owner = 'your-org' AND p.repo = 'your-repo' AND p.state = 'open'
ORDER BY s.score DESC;
```

List what you've indexed:

```sql
SELECT name, type, chunk_count FROM lynx.sources;
```

## Notes

- **Local-first.** The codebase and embeddings never leave your machine; only
  the live-data side of a join touches an API.
- The search string is a **literal** you pass — Coral resolves table-function
  arguments at plan time, so `lynx.search` is a joinable source, not a per-row
  enrichment of another table. For one search per row of another table, Lynx
  ships a batch endpoint (`POST /api/v1/search`) and a small Python helper.
- Requires the Lynx API to be running (`lynx manager ui`). See the
  [Lynx Coral guide](https://github.com/lorenzo-cambiaghi/LynxMCP/blob/main/docs/CORAL.md).
