# Voyage AI Source

Query Voyage AI embedding vectors and token usage through SQL.

## Summary

This source lets Coral call Voyage AI's hosted text embedding endpoint and
expose the embedding vector, the returned model, the API object type, and
token usage fields through SQL. The single live table
`voyage_ai.embeddings` sends one Voyage request per SQL row and preserves
the full top-level response shape so users can preview the vector, count
its dimension, or aggregate token usage without losing the response
metadata.

## Provider docs

- Embeddings API reference:
  https://docs.voyageai.com/reference/embeddings-api
- Embeddings guide and supported parameters:
  https://docs.voyageai.com/docs/embeddings
- Models overview:
  https://docs.voyageai.com/docs/models
- Error codes:
  https://docs.voyageai.com/docs/error-codes
- Pricing:
  https://docs.voyageai.com/docs/pricing
- API keys:
  https://dashboard.voyageai.com/organization/api-keys

## Authentication

Create or copy a Voyage AI API key in the Voyage dashboard, then add the
community source:

```bash
coral source add --interactive --file sources/community/voyage_ai/manifest.yaml
```

For scripted setup, provide the key as an environment variable:

```bash
VOYAGE_API_KEY=... coral source add --file sources/community/voyage_ai/manifest.yaml
```

The key is stored locally by Coral and sent in the `Authorization: Bearer
<key>` header. Voyage keys are account-scoped, so a single key works for
every Voyage model the account has access to and charges usage to the same
billing account.

## Live request costs

Selecting the `voyage_ai.embeddings` table performs one live
`POST /v1/embeddings` call per SQL row returned. Voyage AI charges per
token consumed, so queries with long inputs, large `output_dimension`
filters, or many SQL rows can consume tokens quickly. Refer to
<https://docs.voyageai.com/docs/pricing> for current per-model rates and
shorten inputs or add `LIMIT 1` while validating.

## Source shape

- `voyage_ai.embeddings` generates one text embedding through
  `POST /v1/embeddings` with required `model` and `input` filters and
  optional `input_type`, `truncation`, `output_dimension`, and
  `output_dtype` filters.

## Source scope

- Targets Voyage's hosted API at `https://api.voyageai.com/v1`.
- Requires `VOYAGE_API_KEY` authentication as a Bearer token.
- The embeddings table requires `model` and `input` filters and an
  optional `input_type` (`query` / `document` / null), `truncation`
  (`true` / `false`), `output_dimension` (integer), and `output_dtype`
  (`float` / `int8` / `uint8` / `binary` / `ubinary`) filter set.
- `truncation` is a `Boolean` SQL filter. Pass `true` or `false` in the
  `WHERE` clause (for example `WHERE truncation = true`).
- `output_dimension` is an `Int64` SQL filter. Pass an integer literal
  in the `WHERE` clause (for example `WHERE output_dimension = 256`).
- The `embedding` column is the raw JSON array Voyage returns. Use
  `substr(CAST(embedding AS VARCHAR), 1, 80) AS embedding_preview` for
  compact output, or `json_length(embedding)` to compute the dimension
  in SQL.

## Limitations

- The first version supports `POST /v1/embeddings` only. Other Voyage
  endpoints are intentionally out of scope:
  - `POST /v1/rerank` (reranker models) - would require a `documents`
    JSON-array body field, which the current Coral source-spec DSL does
    not model cleanly. String filters become JSON string values, not
    arrays. A future revision could model rerank as a `kind: search`
    table function with a JSON-typed argument.
  - `POST /v1/multimodalembeddings` - multimodal (text + image) inputs
    require base64 image payloads and are out of scope.
  - `POST /v1/contextualizedembeddings` - the contextualized chunk
    embedding endpoint takes a list of lists of strings as `inputs` and
    is intentionally out of scope.
- Only single-text `input` is supported. To embed many short strings,
  run one row per string; Voyage supports up to 1,000 inputs per call
  but the current source sends one at a time.
- Streaming, batch embeddings, `encoding_format`, and other first-class
  Voyage features are intentionally not modeled.
- `output_dtype` accepts the values documented at
  <https://docs.voyageai.com/docs/embeddings> (`float` / `int8` /
  `uint8` / `binary` / `ubinary`). The current source does not validate
  the value against the chosen model; Voyage will return an error if
  the combination is unsupported.
- Voyage does not publish a public `GET /v1/models` listing endpoint, so
  the first version does not include a `models` table. The available
  model IDs are documented at
  <https://docs.voyageai.com/docs/models> and at least the following are
  currently valid for `model` requests:
  `voyage-4-large`, `voyage-4`, `voyage-4-lite`, `voyage-3-large`,
  `voyage-3.5`, `voyage-3.5-lite`, `voyage-code-3`, `voyage-finance-2`,
  `voyage-law-2`.

## Tables

### `voyage_ai.embeddings`

Generates one Voyage AI embedding vector and exposes the response
metadata alongside the input filters.

```sql
SELECT returned_model,
       object,
       total_tokens,
       substr(CAST(embedding AS VARCHAR), 1, 80) AS embedding_preview
FROM voyage_ai.embeddings
WHERE model = 'voyage-3.5-lite'
  AND input = 'Coral source validation'
LIMIT 1;
```

Use `input_type = 'query'` for search queries and `input_type = 'document'`
for index entries. Voyage prepends a retrieval prompt to the input in
those modes; embeddings with and without `input_type` are compatible.

```sql
SELECT returned_model,
       total_tokens,
       json_length(embedding) AS embedding_dim
FROM voyage_ai.embeddings
WHERE model = 'voyage-3.5-lite'
  AND input = 'Coral source validation'
  AND input_type = 'query'
  AND output_dimension = 256
LIMIT 1;
```

Disable truncation to make Voyage return an error for over-length
inputs instead of silently cutting them off:

```sql
SELECT returned_model, total_tokens
FROM voyage_ai.embeddings
WHERE model = 'voyage-3.5-lite'
  AND input = 'Coral source validation'
  AND truncation = false
LIMIT 1;
```

Request a quantized embedding with `output_dtype = 'int8'` to reduce
storage and bandwidth at the cost of precision. Voyage still returns a
JSON array, but the values are signed 8-bit integers:

```sql
SELECT returned_model,
       total_tokens,
       json_length(embedding) AS embedding_dim
FROM voyage_ai.embeddings
WHERE model = 'voyage-3.5-lite'
  AND input = 'Coral source validation'
  AND output_dtype = 'int8'
LIMIT 1;
```

Useful columns:

| Column | Notes |
|---|---|
| `model` | Model name supplied in the SQL filter. |
| `input` | Text supplied in the SQL filter. |
| `input_type` | Retrieval hint supplied in the SQL filter, when set. |
| `output_dimension` | Output dimension supplied in the SQL filter, when set. |
| `truncation` | Truncation behaviour supplied in the SQL filter, when set. |
| `output_dtype` | Output dtype supplied in the SQL filter, when set. |
| `object` | Voyage response object type, typically `list`. |
| `returned_model` | Model identifier returned by Voyage. |
| `usage` | Raw `usage` object returned by Voyage. |
| `total_tokens` | Total tokens consumed by the request. |
| `data` | Raw `data` array returned by Voyage. |
| `index` | Embedding index inside the `data` array, normally `0` for a single-text request. |
| `embedding` | Embedding vector as a JSON array. |

## Live validation output

Validated against a live Voyage AI account with a valid `VOYAGE_API_KEY`.

```bash
$ coral source lint sources/community/voyage_ai/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/voyage_ai/manifest.yaml
Added source voyage_ai

  ✓ voyage_ai connected successfully

    voyage_ai (1 table)
    └─ embeddings
    Query tests
    2 declared · 2 passed · 0 failed
```

**Tables introspection:**

```sql
SELECT schema_name, table_name, description, required_filters
FROM coral.tables
WHERE schema_name = 'voyage_ai'
ORDER BY table_name;
```

```text
+-------------+------------+-------------------------------------------------------------------------------------------------------------------------------------+------------------+
| schema_name | table_name | description                                                                                                                         | required_filters |
+-------------+------------+-------------------------------------------------------------------------------------------------------------------------------------+------------------+
| voyage_ai   | embeddings | Generate one Voyage AI embedding vector through `POST /v1/embeddings`. One SQL row per call; preserves top-level response metadata. | model,input      |
+-------------+------------+-------------------------------------------------------------------------------------------------------------------------------------+------------------+
```

**Columns introspection:**

```sql
SELECT table_name, column_name, data_type, is_required_filter
FROM coral.columns
WHERE schema_name = 'voyage_ai'
ORDER BY table_name, ordinal_position;
```

```text
+------------+------------------+-----------+--------------------+
| table_name | column_name      | data_type | is_required_filter |
+------------+------------------+-----------+--------------------+
| embeddings | model            | Utf8      | true               |
| embeddings | input            | Utf8      | true               |
| embeddings | input_type       | Utf8      | false              |
| embeddings | output_dimension | Int64     | false              |
| embeddings | truncation       | Boolean   | false              |
| embeddings | output_dtype     | Utf8      | false              |
| embeddings | object           | Utf8      | false              |
| embeddings | returned_model   | Utf8      | false              |
| embeddings | usage            | Json      | false              |
| embeddings | total_tokens     | Int64     | false              |
| embeddings | data             | Json      | false              |
| embeddings | index            | Int64     | false              |
| embeddings | embedding        | Json      | false              |
+------------+------------------+-----------+--------------------+
```

**Inputs introspection:**

```sql
SELECT key, kind, required, is_set
FROM coral.inputs
WHERE schema_name = 'voyage_ai'
ORDER BY key;
```

```text
+----------------+--------+----------+--------+
| key            | kind   | required | is_set |
+----------------+--------+----------+--------+
| VOYAGE_API_KEY | secret | true     | true   |
+----------------+--------+----------+--------+
```

```bash
$ coral source test voyage_ai
  ✓ voyage_ai connected successfully

    voyage_ai (1 table)
    └─ embeddings
    Query tests
    2 declared · 2 passed · 0 failed
```

**Live bounded embedding proof:**

```sql
SELECT returned_model, object, total_tokens,
       substr(CAST(embedding AS VARCHAR), 1, 80) AS embedding_preview
FROM voyage_ai.embeddings
WHERE model = 'voyage-3.5-lite'
  AND input = 'Coral source validation'
LIMIT 1;
```

```text
+-----------------+--------+--------------+----------------------------------------------------------------------------------+
| returned_model  | object | total_tokens | embedding_preview                                                                |
+-----------------+--------+--------------+----------------------------------------------------------------------------------+
| voyage-3.5-lite | list   | 4            | [-0.044563316,0.032666322,-0.020567684,0.019256998,-0.010485485,0.038513996,0.00 |
+-----------------+--------+--------------+----------------------------------------------------------------------------------+
```

**Live bounded embedding proof with non-default output dimension:**

```sql
SELECT returned_model, total_tokens,
       json_length(embedding) AS embedding_dim
FROM voyage_ai.embeddings
WHERE model = 'voyage-3.5-lite'
  AND input = 'Coral source validation'
  AND output_dimension = 256
LIMIT 1;
```

```text
+-----------------+--------------+---------------+
| returned_model  | total_tokens | embedding_dim |
+-----------------+--------------+---------------+
| voyage-3.5-lite | 4            | 256           |
+-----------------+--------------+---------------+
```

## Screenshots

![Voyage proof 1 - lint](https://raw.githubusercontent.com/FiscalMindset/coral/voyage-ai-proof-assets/output_proof/voyage_ai/1_lint.png)
![Voyage proof 2 - source add](https://raw.githubusercontent.com/FiscalMindset/coral/voyage-ai-proof-assets/output_proof/voyage_ai/2_add.png)
![Voyage proof 3 - tables](https://raw.githubusercontent.com/FiscalMindset/coral/voyage-ai-proof-assets/output_proof/voyage_ai/3_tables.png)
![Voyage proof 4 - columns](https://raw.githubusercontent.com/FiscalMindset/coral/voyage-ai-proof-assets/output_proof/voyage_ai/4_columns.png)
![Voyage proof 5 - inputs](https://raw.githubusercontent.com/FiscalMindset/coral/voyage-ai-proof-assets/output_proof/voyage_ai/5_inputs.png)
![Voyage proof 6 - source test](https://raw.githubusercontent.com/FiscalMindset/coral/voyage-ai-proof-assets/output_proof/voyage_ai/6_source_test.png)
![Voyage proof 7 - bounded live embedding](https://raw.githubusercontent.com/FiscalMindset/coral/voyage-ai-proof-assets/output_proof/voyage_ai/7_embedding_main.png)
![Voyage proof 8 - bounded live embedding with output_dimension=256](https://raw.githubusercontent.com/FiscalMindset/coral/voyage-ai-proof-assets/output_proof/voyage_ai/8_embedding_dim_256.png)
![Voyage proof 9 - bounded live embedding with input_type=query and output_dimension=256](https://raw.githubusercontent.com/FiscalMindset/coral/voyage-ai-proof-assets/output_proof/voyage_ai/9_input_type_dim_256.png)
![Voyage proof 10 - bounded live embedding with truncation=false](https://raw.githubusercontent.com/FiscalMindset/coral/voyage-ai-proof-assets/output_proof/voyage_ai/10_truncation_false.png)
![Voyage proof 11 - bounded live embedding with output_dtype=int8](https://raw.githubusercontent.com/FiscalMindset/coral/voyage-ai-proof-assets/output_proof/voyage_ai/11_output_dtype_int8.png)
