# Lynx

[Lynx](https://github.com/lorenzo-cambiaghi/LynxMCP) is a **100% local** semantic
code-search engine (an MCP server) for codebases, library docs, and PDFs. This
source exposes its local API as SQL, so you can query your code by *behavior*
and **join the results with live data** from other Coral sources — without a
byte leaving your machine.

- `lynx.sources` — the sources you've indexed (codebases / docs sites / PDFs).
- `lynx.search(q => '...', source => '...')` — ranked hybrid (dense + BM25)
  search. `q` is required; `source` is optional (omit it to search every
  indexed source at once). Control how many results Lynx returns with SQL
  `LIMIT` — Coral maps it to Lynx's `top_k` (clamped to 50). Each row carries
  `source`, `file`, `file_path`, `symbol`, `kind`, `language`,
  `start_line`/`end_line`, `score`, and the code `content` itself.

## Setup

1. Install Lynx and index at least one source (it serves the API on
   `127.0.0.1`):

   ```bash
   pipx install lynx-mcp                     # or: uv tool install lynx-mcp
   lynx manager init                         # downloads the embedding model (writes an EMPTY config)
   lynx manager ui --port 8765 --no-browser  # serves the local API on 127.0.0.1:8765
   ```

   `lynx manager init` creates an **empty** config — it does not index
   anything on its own. Open <http://127.0.0.1:8765> and use
   **+ Add your first source** to add a codebase, docs site, or PDF folder;
   the UI indexes it for you. Leave this process running — it's the API
   Coral talks to. (Prefer config-based setup? Edit `config.json` to add a
   source, then `lynx build`.)

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
FROM lynx.search(q => 'clamp a value between a min and max')
WHERE language = 'c_sharp'
LIMIT 5;
```

Broad context: pair the top code hits with the repo's open PRs. This is a
`CROSS JOIN` — every hit against every open PR — so keep the `LIMIT`s small
and treat it as a context dump, not a precise match:

```sql
SELECT s.file, s.symbol, s.score, p.html_url
FROM lynx.search(q => 'retry logic for payment webhooks') s
CROSS JOIN github.pulls p
WHERE p.owner = 'your-org' AND p.repo = 'your-repo' AND p.state = 'open'
ORDER BY s.score DESC
LIMIT 20;
```

List what you've indexed:

```sql
SELECT name, type, chunk_count FROM lynx.sources;
```

## Live validation

Captured with coral 0.4.2 against a running Lynx API (`lynx manager ui --port
8765`) and one indexed codebase source. Everything runs on `127.0.0.1`; local
paths redacted.

```console
$ LYNX_PORT=8765 coral source add --file sources/community/lynx/manifest.yaml
Added source lynx (secrets: none)

  ✓ lynx connected successfully
  Secrets: none

    lynx (1 table)
    └─ sources
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT name, type, chunk_count FROM lynx.sources
      1 row

    ✓ SELECT file, symbol, score FROM lynx.search(q => 'where passwords are hashed') LIMIT 5
      5 rows

$ coral source test lynx

  ✓ lynx connected successfully
  Secrets: none

    lynx (1 table)
    └─ sources
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT name, type, chunk_count FROM lynx.sources
      1 row

    ✓ SELECT file, symbol, score FROM lynx.search(q => 'where passwords are hashed') LIMIT 5
      5 rows

$ coral sql "SELECT name, type, chunk_count FROM lynx.sources"
+-----------+----------+-------------+
| name      | type     | chunk_count |
+-----------+----------+-------------+
| framework | codebase | 12656       |
+-----------+----------+-------------+

$ coral sql "SELECT file, symbol, score FROM lynx.search(q => 'clamp a value between a min and max') WHERE language = 'c_sharp' LIMIT 5"
+---------------------------+-----------------------------------------------------------------------------+----------------------+
| file                      | symbol                                                                      | score                |
+---------------------------+-----------------------------------------------------------------------------+----------------------+
| Math.cs                   | Framework.Utility.Math.Clamp                                                | 0.01639344262295082  |
| SplineSamplingConfig.cs   | Framework.SplineUtility.Sampler.SplineSamplingConfig.ClampToValidRanges     | 0.016129032258064516 |
| WheelGripConfiguration.cs | Framework.Vehicle.WheelGripConfiguration.ClampGrip                          | 0.015873015873015872 |
| MinMaxValueAttributes.cs  | Framework.Utility.MinMaxCacheSegmentsAttribute.MinMaxCacheSegmentsAttribute | 0.015384615384615384 |
| InspectorValidation.cs    | Framework.UIEditorKit.InspectorValidation.Range                             | 0.014925373134328358 |
+---------------------------+-----------------------------------------------------------------------------+----------------------+
```

`LIMIT` sets how many candidates Lynx returns (its `top_k`); any `WHERE` then
filters that fetched set. With no `WHERE`, `LIMIT n` returns up to `n` rows,
capped at 50:

| Query | Rows returned |
|-------|---------------|
| `SELECT file FROM lynx.search(...) LIMIT 3`   | 3 |
| `SELECT file FROM lynx.search(...) LIMIT 12`  | 12 — exceeds Lynx's default 8, so `LIMIT` is driving `top_k` |
| `SELECT file FROM lynx.search(...) LIMIT 100` | 50 — Lynx clamps `top_k` to its `[1, 50]` ceiling |

## Notes

- **Local-first.** The codebase and embeddings never leave your machine; only
  the live-data side of a join touches an API.
- The search string is a **literal** you pass — Coral resolves table-function
  arguments at plan time, so `lynx.search` is a joinable source, not a per-row
  enrichment of another table. For one search per row of another table, Lynx
  ships a batch endpoint (`POST /api/v1/search`) and a small Python helper.
- Requires the Lynx API to be running (`lynx manager ui`). See the
  [Lynx Coral guide](https://github.com/lorenzo-cambiaghi/LynxMCP/blob/main/docs/CORAL.md).
