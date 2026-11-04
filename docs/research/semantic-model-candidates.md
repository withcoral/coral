# Candidate embedding and late-interaction models for Universal Search

Ticket: withcoral/coral#2859. Map: #2856. Written 2026-11-04 from primary sources
(Hugging Face model cards, repository metadata and files, runtime source code,
the authors' posts and papers). Every fact below carries a source number from
the list at the end. "Hub" means the Hugging Face repository metadata API.

## Answer in one screen

**Pass the license and runtime filter** (permissive license, and an ONNX file a
Rust ONNX runtime can load, from the author or a known converter):

| Model | Why it passes | Watch out |
|---|---|---|
| potion-base-8M | MIT; 30 MB; static model; `model2vec-rs` crate runs it without ONNX; ONNX file also shipped | No int8 file shipped; quantize at build time |
| all-MiniLM-L6-v2 | Apache-2.0; fp32 and int8 ONNX in the author repo; in fastembed Python and Rust | Default max length 256 tokens |
| bge-small-en-v1.5 | MIT; fp32 ONNX in author repo; int8 in Xenova and Qdrant mirrors; in fastembed Python and Rust | Query prefix optional |
| bge-base-en-v1.5 | MIT; same as small | 436 MB fp32 |
| snowflake-arctic-embed-xs | Apache-2.0 per card body and GitHub; fp32 and int8 ONNX in author repo; in fastembed Python and Rust | No license tag in Hub metadata |
| snowflake-arctic-embed-s | Apache-2.0; fp32 and int8 ONNX in author repo; in fastembed Python and Rust | None |
| nomic-embed-text-v1.5 | Apache-2.0; fp32 and int8 ONNX in author repo; Matryoshka; 8192 tokens; in fastembed Python and Rust | Prefixes are mandatory; 547 MB fp32 |
| Qwen3-Embedding-0.6B | Apache-2.0; ONNX from onnx-community (fp32 2.4 GB, int8 614 MB); Matryoshka 32 to 1024 | 596M params; no ONNX from the author; fastembed-rs runs it on candle, not ONNX; not in fastembed Python |
| jina-embeddings-v2-base-code | Apache-2.0; fp32, fp16 and int8 ONNX in author repo; 8192 tokens; in fastembed Python and Rust | 161M params; custom ALiBi BERT, so only the ONNX path avoids remote code |
| answerai-colbert-small-v1 | Apache-2.0; 33M; fp32 and int8 ONNX in author repo; in fastembed Python; LightOn ONNX re-export | No late-interaction type in fastembed-rs |
| colbert-ir/colbertv2.0 | MIT; 110M; fp32 ONNX in author repo; in fastembed Python | No int8 file; 2021 model |
| LateOn (lightonai) | Apache-2.0; 149M; fp32 and int8 ONNX in author repo | Hub repo created 2026-03-13; not in fastembed |
| GTE-ModernColBERT-v1 | Apache-2.0; 149M; fp32 and int8 ONNX in author repo | Not in fastembed |
| mxbai-edge-colbert-v0-17m / -32m | Apache-2.0; 17M / 32M; fp32 and int8 ONNX in author repo; the only candidates with a published CPU timing | Not in fastembed |
| LateOn-Code-edge (lightonai) | Apache-2.0; 17M; fp32 and int8 ONNX; code-search model on the same backbone as mxbai 17m | Code-tuned; not on the ticket's list |

**Fail the filter:**

- jina-colbert-v2: CC-BY-NC-4.0, and 559M parameters.
- jina-colbert-v1-en: Apache-2.0 but no ONNX file anywhere on the Hub.
- Reason-ModernColBERT (lightonai): CC-BY-NC-4.0.
- lightonai/lateon-fix-s: no license declared.
- Every other small PyLate ColBERT found in the sweep (NeuML, VAGOsolutions, DataScience-UIBK, ColBERT-Zero, ModernColBERT-embed-base, Agent-ModernColBERT, LateOn-regularized): permissive, but no ONNX file. LightOn's `pylate-onnx-export` tool can produce one; that is work, not a download.
- EmbeddingGemma: out by decision (Gemma terms), not evaluated.

**Surprises:**

1. No candidate repository ships a LICENSE file. Every license is a one-line
   Hub metadata tag or a sentence in the card body. arctic-embed-xs has no tag
   at all; the card body and the Snowflake GitHub repository say Apache-2.0.
2. LightOn's ONNX re-exports of answerai-colbert-small-v1 and
   mxbai-edge-colbert-v0-32m carry an MIT tag while the source models are
   Apache-2.0. The source license governs; the re-export cannot change it
   (inference, moderate confidence).
3. fastembed-rs 7.5.0 has no late-interaction model type. Its only multi-vector
   output is the BGE-M3 joint model. LightOn's `next-plaid-onnx` 1.8.5 crate
   (Apache-2.0) encodes ColBERT models through ONNX Runtime, and `next-plaid`
   1.8.5 does CPU MaxSim search. `next-plaid-onnx` pins `ort =2.0.0-rc.11`;
   fastembed-rs pins `ort =2.0.0-rc.15`. One binary cannot hold both pins
   (moderate confidence; the runtime ticket should confirm).
4. Published CPU throughput is almost absent. Only the mxbai-edge-colbert tech
   report gives a CPU timing, on an unnamed CPU. Model2Vec claims "up to 500
   times faster on CPU than the original model" with no absolute number.
5. Qwen3-Embedding-0.6B is 4 times the parameters of the next-largest candidate
   and its author ships no ONNX. Its ONNX comes from onnx-community, which
   carries no license tag.
6. LateOn's own card calls it "the first ColBERT (and sub 150M parameter)
   model" above 57 nDCG@10 on BEIR. GTE-ModernColBERT-v1 and LateOn both sit at
   149,015,808 parameters, under the 150M bound by less than one million.

## Table

Sizes are file sizes from Hub metadata in MB (10^6 bytes). "Author repo" means
the model's own Hugging Face repository. "fe-py" is fastembed Python 0.9.1;
"fe-rs" is fastembed-rs 7.5.0 (both read from source on 2026-11-04). Max length
is tokens. For ColBERT models "dim" is the per-token vector size, and the
query/document lengths are the PyLate or ColBERT defaults shipped in the repo.

| Model | License (permissive?) | Params | Disk float / int8 | ONNX and runtime | Max length | Dim, Matryoshka | Query prefix / document prefix | Published CPU throughput | Release |
|---|---|---|---|---|---|---|---|---|---|
| minishlab/potion-base-8M | MIT (yes) [1] | 7.56M [1] | 30.2 MB safetensors fp32; 30.2 MB ONNX fp32; no int8 file [1] | Author repo `onnx/model.onnx` [1]; `model2vec` exports via `torch.onnx.export` [21]; `model2vec-rs` 0.4.0 (MIT) loads safetensors, no ONNX runtime, int8 in memory [22][23]; not in fe-py or fe-rs [24][25] | No positional limit; static bag of token vectors [1][20] | 256; no MRL [1] | none / none [1][20] | "up to 500 times faster on CPU than the original model", no absolute figure [20] | Hub 2024-10-29 [1] |
| sentence-transformers/all-MiniLM-L6-v2 | Apache-2.0 (yes) [2] | 22.7M [2] | 90.4 MB ONNX fp32; 23.0 MB `model_qint8_arm64.onnx` [2] | Author repo fp32, O1-O4, qint8 arm64/avx512, quint8 avx2 [2]; fe-py via `Qdrant/all-MiniLM-L6-v2-onnx` [24]; fe-rs via same plus `Xenova` quantized [25] | 256 default (`sentence_bert_config`), 512 positions [2] | 384; no MRL [2] | none / none [2] | none on the card [2] | Trained in the HF JAX/Flax community week, 7 to 14 July 2021 [2][26]; Hub createdAt 2022-03-02 is not a release date |
| BAAI/bge-small-en-v1.5 | MIT (yes) [3] | 33.4M [3] | 133.1 MB ONNX fp32 (author); int8 33.8 MB in `Xenova/bge-small-en-v1.5`; 66.5 MB `Qdrant/bge-small-en-v1.5-onnx-Q` [3][27] | Author repo `onnx/model.onnx` [3]; fe-py via Qdrant Q [24]; fe-rs via Xenova and Qdrant Q [25] | 512 [3] | 384; no MRL [3] | `Represent this sentence for searching relevant passages: ` for short queries, optional for v1.5 / none [3] | none [3] | Card changelog 2023-09-12 [3] |
| BAAI/bge-base-en-v1.5 | MIT (yes) [4] | 109.5M [4] | 435.8 MB ONNX fp32 (author); int8 109.6 MB in `Xenova/bge-base-en-v1.5`; 217.8 MB Qdrant Q [4][27] | As small [4][24][25] | 512 [4] | 768; no MRL [4] | as small [4] | none [4] | Card changelog 2023-09-12 [4] |
| Snowflake/snowflake-arctic-embed-xs | Apache-2.0 in card body and GitHub; **no license tag in Hub metadata** (yes, with flag) [5][28] | 22.6M [5] | 90.4 MB ONNX fp32; 23.0 MB `onnx/model_int8.onnx` [5] | Author repo fp32, fp16, int8, uint8, q4, bnb4 [5]; fe-py [24]; fe-rs plus Q [25] | 512 [5] | 384; no MRL [5] | `Represent this sentence for searching relevant passages: ` / none; CLS pooling [5] | none [5] | Card changelog 2024-04-16 [5] |
| Snowflake/snowflake-arctic-embed-s | Apache-2.0 (yes) [6] | 33.2M [6] | 133.1 MB ONNX fp32; 34.0 MB int8 [6] | As xs [6][24][25] | 512 [6] | 384; no MRL [6] | as xs [6] | none [6] | Card changelog 2024-04-16 [6] |
| nomic-ai/nomic-embed-text-v1.5 | Apache-2.0 (yes) [7] | 136.7M [7] | 547.3 MB ONNX fp32; 137.3 MB int8 [7] | Author repo fp32, fp16, int8, uint8, q4, q4f16, bnb4 [7]; fe-py plus `-Q` [24]; fe-rs plus Q [25] | 8192 (`sentence_bert_config`); config has 2048 positions plus rotary scaling [7] | 768; MRL, card table shows 768/512/256/128 [7] | `search_query: ` / `search_document: `; the card says the prefix is mandatory [7] | none [7] | Hub 2024-02-10 [7]; the blog URL in the card returns 404 (2026-11-04) |
| Qwen/Qwen3-Embedding-0.6B | Apache-2.0 (yes) [8] | 595.8M [8] | 1,191.6 MB safetensors bf16 [8]; ONNX only in `onnx-community/Qwen3-Embedding-0.6B-ONNX`: fp32 2,400.6 MB (307.2 + 2,093.4 external), int8 613.5 MB, q4f16 567.5 MB [29] | No ONNX from the author [8]; onnx-community export, no license tag [29]; not in fe-py [24]; fe-rs runs it on candle behind the `qwen3` feature, not ONNX [25] | 32k per card [8] | 1024; MRL 32 to 1024 [8] | `Instruct: {task}\nQuery:{query}` on queries / none [8] | none [8][30] | Hub 2025-06-03 [8]; Qwen blog 2025-06-05 [30] |
| jinaai/jina-embeddings-v2-base-code | Apache-2.0 (yes) [9] | 160.9M [9] | 321.8 MB safetensors fp16; ONNX fp32 641.5 MB, fp16 321.1 MB, int8 (`model_quantized`) 161.9 MB [9] | Author repo [9]; fe-py [24]; fe-rs [25]; custom JinaBert with ALiBi, remote code outside ONNX [9] | 8192 [9] | 768; no MRL [9] | none / none [9][31] | none [9] | Hub 2023-11-17 [9]; Jina post 2024-02-05 [31] |
| answerdotai/answerai-colbert-small-v1 | Apache-2.0 (yes) [10] | 33.4M [10] | 133.2 MB ONNX fp32 (`onnx/model.onnx`, also `model.onnx`, `vespa_colbert.onnx`); 34.0 MB `onnx/model_int8.onnx` [10] | Author repo fp32 and int8 [10]; fe-py uses `vespa_colbert.onnx` [24]; no late-interaction type in fe-rs [25]; LightOn re-export `lightonai/answerai-colbert-small-v1-onnx` (tagged MIT) for `next-plaid-onnx` [32][33] | 512 positions; query 32, document 300 (`artifact.metadata`) [10] | 96 per token; no MRL [10] | `[Q]` = `[unused0]` (id 1) / `[D]` = `[unused1]` (id 2); ColBERT pads queries to 32 with `[MASK]`, fastembed does this [10][24] | "search through hundreds of thousands of documents in milliseconds", no figure [34] | Hub 2024-08-12 [10]; Answer.AI post 2024-08-13 [34] |
| jinaai/jina-colbert-v2 | **CC-BY-NC-4.0 (no)** [11] | 559.5M [11] | 1,119.0 MB safetensors bf16; ONNX fp32 2,237.8 MB (4.0 + 2,233.8 external); no int8 [11] | Author repo [11]; fe-py [24] | 8192 [11] | 128 per token; 96 and 64 as separate repos [11] | `[QueryMarker]` (250002) / `[DocumentMarker]` (250003) [11][24] | none [11] | Hub 2024-08-16 [11] |
| colbert-ir/colbertv2.0 | MIT (yes) [12] | 109.6M [12] | 436.2 MB `model.onnx` fp32; no int8 file [12] | Author repo [12]; fe-py [24]; not fe-rs [25] | 512 positions; query 32, document 180 (`artifact.metadata`) [12] | 128 per token; no MRL [12] | `[unused0]` / `[unused1]` marker tokens; queries padded to 32 with `[MASK]` [12][24] | none [12] | Hub 2023-06-27 [12]; ColBERTv2 paper 2021-12-02 [35] |
| lightonai/LateOn | Apache-2.0 (yes) [13] | 149.0M [13] | 615.8 MB `model.onnx` fp32; 154.7 MB `model_int8.onnx` [13] | Author repo fp32 and int8 [13]; `next-plaid-onnx` [33]; not in fe-py or fe-rs [24][25] | 8192 positions; PyLate query 32, document 300 [13] | 128 per token; no MRL [13] | `[Q] ` / `[D] `; no other prompt; no query expansion [13][36] | none [13][36] | Hub 2026-03-13 [13]; LightOn post 2026-04-21 [36] |
| lightonai/GTE-ModernColBERT-v1 | Apache-2.0 (yes) [14] | 149.0M [14] | 596.9 MB `model.onnx` fp32; 150.0 MB `model_int8.onnx` [14] | Author repo [14]; `next-plaid-onnx` example model [33]; not in fastembed [24][25] | 8192 positions; PyLate query 48, document 300 [14] | 128 per token; no MRL [14] | `[Q] ` / `[D] ` [14] | none [14] | Hub 2025-04-30 [14] |
| mixedbread-ai/mxbai-edge-colbert-v0-17m | Apache-2.0 (yes) [15] | 16.8M [15] | 68.0 MB `model.onnx` fp32; 17.2 MB `model_int8.onnx` [15] | Author repo [15]; not in fastembed [24][25] | 7999 positions; PyLate query 48, document 512; trained on 220-token documents [15][37] | 48 per token; no MRL [15][37] | `[Q] ` / `[D] ` [15] | Tech report table 13: CPU 487 s to encode about 67,000 documents plus 650 queries (NanoBEIR), CPU model not named; about 139 docs/s by division [37] | Hub 2025-10-13 [15]; arXiv v1 2025-10-16 [37] |
| mixedbread-ai/mxbai-edge-colbert-v0-32m | Apache-2.0 (yes) [16] | 31.9M [16] | 131.5 MB `model.onnx` fp32; 33.2 MB `model_int8.onnx` [16] | Author repo [16]; LightOn re-export `lightonai/mxbai-edge-colbert-v0-32m-onnx` (tagged MIT) [32]; not in fastembed [24][25] | 7999 positions; PyLate query 48, document 512 [16] | 64 per token; no MRL [16] | `[Q] ` / `[D] ` [16] | Same table: CPU 589 s, about 115 docs/s by division [37] | Hub 2025-10-13 [16]; arXiv v1 2025-10-16 [37] |
| jinaai/jina-colbert-v1-en | Apache-2.0 (yes) [17] | 137.5M [17] | 549.9 MB safetensors fp32; **no ONNX file** [17] | None found on the Hub [17]; not in fastembed [24][25] | 8192 [17] | 128 per token [17] | ColBERT markers (not verified in detail) | none [17] | Hub 2024-01-23 [17] |
| lightonai/LateOn-Code-edge (extra) | Apache-2.0 (yes) [18] | 16.8M [18] | 68.0 MB fp32; 17.2 MB int8 [18] | Author repo [18]; not in fastembed | 7999 positions; PyLate query 256, document 2048 [18] | 48 per token [18] | `[Q] ` / `[D] ` [18] | none [18] | Hub 2026-02-05 [18] |

## Models that fail the filter

- **jina-colbert-v2**: CC-BY-NC-4.0 forbids commercial use [11]. Also 559.5M
  parameters and a 2.2 GB fp32 ONNX [11].
- **jina-colbert-v1-en**: Apache-2.0, but no ONNX file in the author repo and
  none found on the Hub [17]. Passes only if Coral exports it.
- **lightonai/Reason-ModernColBERT**: CC-BY-NC-4.0 [19].
- **lightonai/lateon-fix-s**: no license tag, no card [19].
- **Small PyLate ColBERTs without ONNX** (all Apache-2.0, all created before
  2026-10-21, none in fastembed): NeuML/colbert-muvera-small (33.4M,
  2025-09-17), NeuML/colbert-muvera-micro and NeuML/pylate-bert-tiny (4.4M,
  2025-08/09), NeuML/colbert-muvera-pico (0.45M, 2025-10-08),
  VAGOsolutions/SauerkrautLM-Multi-ColBERT-15m and -33m and
  SauerkrautLM-Reason-Multi-ColBERT-15m (15.3M / 32.8M, 2025-07, multilingual),
  DataScience-UIBK/Reason-mxbai-colbert-v0.1-32m (31.9M, 2026-04-24, fine-tune of
  mxbai-edge 32m; the "SmallReason-ColBERT" paper arXiv 2609.29652 is likely
  this lineage, not confirmed), lightonai/ColBERT-Zero and
  lightonai/ModernColBERT-embed-base (149.0M, 2026-02-19, prompts
  `[Q] search_query: ` / `[D] search_document: `), lightonai/LateOn-regularized
  and LateOn-hpool-regularized (149.0M, 2026-05/07), lightonai/Agent-ModernColBERT
  (149.0M, 2026-05-12; trained on AgentIR reasoning-trace queries with an
  instruct prefix; its card says it builds on Reason-ModernColBERT, which is
  CC-BY-NC, while its metadata says GTE-ModernColBERT-v1; lineage unverified),
  DataScience-UIBK/uibk-embedding-v1 and Agent-uibk-v1 (149.0M, 2026-10-09/16),
  raphaelsty/neural-cherche-colbert (109.5M, MIT, 2024-02-26). Each fails the
  runtime filter as-is. LightOn's `pylate-onnx-export` can convert them [33].
  [19][38]
- **Over 150M parameters, so outside the ColBERT bound**: lightonai/mLateOn
  (306.9M, multilingual, Apache-2.0, ONNX), LiquidAI/LFM2.5-ColBERT-350M,
  perplexity-ai/pplx-embed-v1-late-0.6b [19][38].
- **Language-specific ColBERTs** (German, Korean, Turkish, French, Arabic) were
  seen in the sweep and skipped as out of scope [38].
- **EmbeddingGemma**: out by decision (Gemma terms). fastembed-rs lists it as
  `EmbeddingGemma300M` from `onnx-community/embeddinggemma-300m-ONNX` [25].

## Runtime facts the pick ticket needs

- **fastembed Python 0.9.1** (`fastembed/text/onnx_embedding.py`,
  `late_interaction/colbert.py`, `jina_colbert.py`, read from `main`; PyPI
  latest is 0.9.1): dense entries for all-MiniLM-L6-v2, bge-small/base-en-v1.5,
  arctic-embed-xs/s, nomic-embed-text-v1.5 (and `-Q`), jina-embeddings-v2-base-code;
  late-interaction entries for colbertv2.0, answerai-colbert-small-v1,
  jina-colbert-v2. No potion, Qwen3, LateOn, GTE-ModernColBERT or mxbai. [24]
- **fastembed-rs 7.5.0** (crates.io 2026-10-23; `src/models/text_embedding.rs`):
  dense entries for all-MiniLM-L6-v2 (+Q), bge-small/base-en-v1.5 (+Q),
  arctic-embed-xs/s (+Q), nomic-embed-text-v1.5 (+Q),
  jina-embeddings-v2-base-code, EmbeddingGemma. Qwen3-Embedding-0.6B runs on
  candle behind the `qwen3` feature. No late-interaction model type; `src/models`
  has no ColBERT file; only the BGE-M3 joint model returns per-token vectors.
  Pins `ort = "=2.0.0-rc.15"`. [25]
- **next-plaid** (github.com/lightonai/next-plaid, Apache-2.0, Rust, created
  2026-01-06): `next-plaid-onnx` 1.8.5 encodes ColBERT models with ONNX Runtime
  (CPU, CUDA, CoreML, TensorRT, DirectML; encode only, `encode_documents`,
  `encode_queries`, `pool_document_embeddings`); `next-plaid` 1.8.5 is a CPU
  PLAID index with MaxSim search. Pins `ort = "=2.0.0-rc.11"`. The Python
  `pylate-onnx-export` tool in the same repo writes fp32 and int8 ONNX plus a
  `config_sentence_transformers.json` carrying `query_prefix`,
  `document_prefix`, `query_length`, `document_length`. Whether the crate
  applies the prefixes itself was not found in its docs. [32][33]
- **model2vec-rs 0.4.0** (MIT, crates.io 2026-10-30): loads potion safetensors
  directly, f32/f16/i8, `quantize_int8()` in memory ("cosine similarity above
  0.9998" to f32 per its README). No ONNX Runtime dependency. [23]
- **onnx-community** exports exist for Qwen3-Embedding-0.6B (2025-06-05, no
  license tag) and a separate INT8 repo (2026-09-28, Apache-2.0 tag). [29]
- **ort** crate: crates.io max version 2.0.0-rc.15 on 2026-10-19; no stable
  2.0 release. [25]

## Method

- Hub metadata: `https://huggingface.co/api/models/<repo>?blobs=true` for
  createdAt, license tag, safetensors parameter counts and file sizes.
  Parameter counts are the safetensors totals, which include position-id
  buffers for BERT models (512 int64 values), so they run a few hundred above
  the weight count.
- Raw files: `README.md`, `config.json`, `sentence_bert_config.json`,
  `config_sentence_transformers.json`, `artifact.metadata`, `*_Dense/config.json`.
- ColBERT sweep: Hub API `search=colbert` (200 rows) and `filter=PyLate`
  (100 rows), then per-repo parameter counts, kept under 150,000,000 and
  created before 2026-10-21. The sweep is not exhaustive; the Hub caps each
  listing.
- Runtime lists read from source on GitHub `main`, versions cross-checked on
  PyPI and crates.io.
- Nothing was run. No throughput here is Coral's own measurement.

## Not verified or unreachable

- The nomic blog post linked from the card (`blog.nomic.ai/posts/nomic-embed-matryoshka`)
  returned 404 on 2026-11-04 after two tries, with and without the redirect. The
  release date is the Hub date only.
- Whether `next-plaid-onnx` applies `[Q] ` / `[D] ` and query padding itself.
- The lineage of lightonai/Agent-ModernColBERT (card vs metadata disagree).
- The CPU model behind the mxbai-edge-colbert timings.
- jina-colbert-v1-en marker tokens (not needed, it fails the filter).
- The SmallReason-ColBERT paper's Hub repository.

## Sources (all fetched 2026-11-04)

1. https://huggingface.co/minishlab/potion-base-8M (card, `config.json`, Hub API)
2. https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2 (card, `sentence_bert_config.json`, Hub API)
3. https://huggingface.co/BAAI/bge-small-en-v1.5 (card changelog and model list, Hub API)
4. https://huggingface.co/BAAI/bge-base-en-v1.5
5. https://huggingface.co/Snowflake/snowflake-arctic-embed-xs (card body "Arctic is licensed under the Apache-2"; no `license:` front-matter key)
6. https://huggingface.co/Snowflake/snowflake-arctic-embed-s
7. https://huggingface.co/nomic-ai/nomic-embed-text-v1.5
8. https://huggingface.co/Qwen/Qwen3-Embedding-0.6B
9. https://huggingface.co/jinaai/jina-embeddings-v2-base-code
10. https://huggingface.co/answerdotai/answerai-colbert-small-v1 (card, `artifact.metadata`, Hub API)
11. https://huggingface.co/jinaai/jina-colbert-v2
12. https://huggingface.co/colbert-ir/colbertv2.0 (card, `artifact.metadata`)
13. https://huggingface.co/lightonai/LateOn (card, `config_sentence_transformers.json`, `3_Dense/config.json`)
14. https://huggingface.co/lightonai/GTE-ModernColBERT-v1
15. https://huggingface.co/mixedbread-ai/mxbai-edge-colbert-v0-17m (`2_Dense/config.json` out_features 48)
16. https://huggingface.co/mixedbread-ai/mxbai-edge-colbert-v0-32m (`3_Dense/config.json` out_features 64)
17. https://huggingface.co/jinaai/jina-colbert-v1-en
18. https://huggingface.co/lightonai/LateOn-Code-edge
19. Hub API `https://huggingface.co/api/models?author=lightonai` and per-repo records for Reason-ModernColBERT, lateon-fix-s, ColBERT-Zero, ModernColBERT-embed-base, Agent-ModernColBERT, LateOn-regularized, LateOn-hpool-regularized, mLateOn, LateOn-Code, LateOn-Code-pretrain
20. https://github.com/MinishLab/model2vec README ("up to 500 times faster on CPU than the original model"; "~30 MB on disk"; MIT)
21. https://github.com/MinishLab/model2vec `model2vec/onnx.py` and `model2vec/model.py` (`quantize_to`)
22. https://github.com/MinishLab/model2vec-rs (GitHub API: MIT, created 2025-05-09)
23. https://crates.io/crates/model2vec-rs (0.4.0, 2026-10-30) and its README (`quantize_int8`, f32/f16/i8 safetensors)
24. https://github.com/qdrant/fastembed `fastembed/text/onnx_embedding.py`, `pooled_embedding.py`, `pooled_normalized_embedding.py`, `late_interaction/colbert.py`, `late_interaction/jina_colbert.py`, `pyproject.toml` (0.9.1); PyPI 0.9.1
25. https://github.com/Anush008/fastembed-rs `src/models/text_embedding.rs`, `src/models/` listing, `README.md`, `Cargo.toml` (7.5.0, `ort = "=2.0.0-rc.15"`); crates.io fastembed 7.5.0 (2026-10-23), ort 2.0.0-rc.15 (2026-10-19)
26. https://discuss.huggingface.co/t/open-to-the-community-community-week-using-jax-flax-for-nlp-cv/7104 (posted 2021-06-23; week 7 to 14 July 2021)
27. Hub API for Xenova/bge-small-en-v1.5, Xenova/bge-base-en-v1.5, Qdrant/bge-small-en-v1.5-onnx-Q, Qdrant/bge-base-en-v1.5-onnx-Q, Qdrant/all-MiniLM-L6-v2-onnx, Xenova/all-MiniLM-L6-v2
28. https://github.com/Snowflake-Labs/arctic-embed (GitHub API license Apache-2.0)
29. Hub API for onnx-community/Qwen3-Embedding-0.6B-ONNX (2025-06-05) and onnx-community/Qwen3-Embedding-0.6B-INT8-ONNX (2026-09-28)
30. https://qwenlm.github.io/blog/qwen3-embedding/ (2025-06-05; Apache 2.0; 32K; 1024; MRL yes)
31. https://jina.ai/news/elevate-your-code-search-with-new-jina-code-embeddings (2024-02-05; "161 million parameters (307MB without quantization)"; Apache 2.0)
32. Hub API and cards for lightonai/answerai-colbert-small-v1-onnx and lightonai/mxbai-edge-colbert-v0-32m-onnx (library `colbert-onnx`, MIT tag, export tool link)
33. https://github.com/lightonai/next-plaid (GitHub API: Apache-2.0, Rust, created 2026-01-06); `next-plaid-onnx/Cargo.toml` (`ort = "=2.0.0-rc.11"`); https://crates.io/crates/next-plaid-onnx and https://crates.io/crates/next-plaid (1.8.5, 2026-10-10, Apache-2.0); https://docs.rs/next-plaid-onnx
34. https://www.answer.ai/posts/2024-08-13-small-but-mighty-colbert.html (2024-08-13; "just 33 million parameters")
35. https://arxiv.org/abs/2112.01488 (ColBERTv2, v1 2021-12-02)
36. https://huggingface.co/blog/lightonai/denseon-lateon (2026-04-21; "ModernBERT backbone at 149M parameters"; "released under Apache 2.0"; results "without the use of the prompt"; no throughput)
37. https://arxiv.org/abs/2510.14880 and https://arxiv.org/html/2510.14880 (v1 2025-10-16; table 13 runtimes; "projection dimension set to just 48"; "documents truncated to 220 tokens"; Ettin encoders)
38. Hub API sweeps `?search=colbert&limit=200` and `?filter=PyLate&limit=100` plus per-repo records (401 repos checked; 132 under 150M parameters)
