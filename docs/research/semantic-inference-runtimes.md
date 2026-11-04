# Rust CPU inference runtimes for the semantic retriever

Research ticket: #2858. Map: #2856. Date: 2026-11-04.

Question: which Rust runtime can embed text inside the Coral process on every
Desktop platform and in the server image, and what would a late-interaction
(MaxSim) scorer need on top of it?

Every claim below names its source. "Measured" means this research ran the
command on 2026-11-04 on an Apple Silicon Mac with Rust 1.98. "Inferred" means
the source states the premise and the conclusion is ours; each such claim
carries a confidence level.

## Answer

**`ort` (the pykeio crate, direct, statically linked) fits every target Coral
ships today and the server image.** The other three candidates each fail one
requirement.

- Coral ships three Desktop targets and one server target: macOS arm64, Windows
  x64, Linux x64 (`apps/desktop/electron-builder.config.ts`) and `linux/amd64`
  (`.github/workflows/release.yml`, `docker buildx build --platform
  linux/amd64`). `docker/Dockerfile` also accepts `arm64`. Coral ships no macOS
  x64, no Windows arm64 and no Linux arm64 Desktop.
- `ort` 2.0.0-rc.15 pins ONNX Runtime 1.31.0 and downloads a static
  `libonnxruntime.a` at build time for exactly those four targets plus Linux
  arm64 and Windows arm64. There is no prebuilt binary for macOS x64. The build
  needs HTTPS to `cdn.pyke.io` once per target per machine; the SHA-256 of each
  archive is pinned inside the `ort-sys` crate, so `Cargo.lock` pins the
  binaries transitively.
- Measured cost on macOS arm64: a release binary with `ort` and a session is
  19.3 MB, against 0.34 MB without it, so `ort` adds about 19 MB to the `coral`
  executable. The static archive is 83.5 MB (macOS arm64) and 111.7 MB (Linux
  x64) but only the linked part lands in the binary.
- Int8: ONNX Runtime's CPU provider runs U8U8, U8S8 and S8S8 quantized graphs,
  and the quantization guide recommends dynamic quantization for transformers.
  The int8 ONNX files of every candidate model (arctic-embed-xs, MiniLM,
  bge-small, nomic-1.5, LateOn) exist on Hugging Face today.
- One core: `SessionBuilder::with_intra_threads(1)` makes ONNX Runtime create no
  thread pool, so every operator runs on the thread that calls `run`. Coral
  sets that thread's priority with the OS API; ONNX Runtime has no priority
  setting of its own. For pools larger than one, `ort` exposes a
  `ThreadManager` trait so Coral spawns the pool threads itself.
- Late interaction: `ort` returns whatever tensor the graph outputs. LightOn's
  official `LateOn/model_int8.onnx` (154.7 MB, dynamic int8, opset 14) outputs
  one `FLOAT [batch, sequence, 128]` tensor, which is the per-token input a
  MaxSim scorer needs. Nothing more is needed from the runtime; the scorer
  (query tokens x document tokens dot products, max per query token, sum) is
  plain Rust over `f32` slices.
- Storage for per-token vectors on Set B (19,830 rows, measured with the LateOn
  tokenizer, documents cut at the model's 300-token document length): 880,573
  tokens for path + title + description, 2,003,939 tokens with the signature
  line. At 128 x f32 per token that is 451 MB or 1,026 MB, against 30.5 MB for
  one 384 x f32 vector per row: a 14.8x or 33.7x multiplier. Int8 per-token
  storage divides those by four.
- Two hard constraints of the prebuilt binaries: all x86-64 builds assume
  x86-64-v3 (Intel Haswell 2013 or later, any AMD Ryzen), and Windows builds
  link DirectML, DXCore, DXGI and D3D12 import libraries, so the executable
  imports `DirectML.dll` (a Windows 10 1903 and later component). Both go away
  with a self-built ONNX Runtime pointed at by `ORT_LIB_LOCATION`, or with
  `load-dynamic` and Microsoft's own shared library (17 MB on Windows x64,
  29 MB on Linux x64, 45 MB on macOS arm64).

Why not the others:

- `fastembed-rs` 7.5.0 is a thin layer over `ort`. It pins `ort =2.0.0-rc.15`
  exactly, downloads models from Hugging Face by default, pools to one vector
  (`Cls` or `Mean`; no `None`), and tests only on Ubuntu. Its lower-level
  `transform` plus `EmbeddingOutput::into_raw` does expose the raw tensors, and
  `try_new_from_user_defined` takes model bytes from any source. It saves the
  tokenizer and input-tensor glue, nothing else. Using `ort` directly keeps the
  ONNX Runtime version in Coral's hands. It also sidesteps a verified pin
  conflict: LightOn's `next-plaid-onnx` 1.8.5, the one Rust crate with a
  ColBERT encoder, pins `ort =2.0.0-rc.11`, and Cargo refuses to resolve it
  beside `fastembed-rs` (section "Crate pins" below).
- `candle` 0.11.1 is pure Rust and so compiles for every target, but its
  `candle-transformers` has no quantized BERT-class model (it has f32 `bert`,
  `modernbert`, `jina_bert`, `nomic_bert`, `distilbert`, `xlm_roberta`), it
  reads its thread count from environment variables and the global rayon pool,
  and its CI covers Linux x64 and arm64, Windows x64 and macOS arm64 only. It
  is the fallback for a target without a prebuilt ONNX Runtime (macOS x64),
  at f32 cost.
- `model2vec-rs` 0.4.0 runs static models only (a token table and a mean).
  Pure Rust, MIT, f32, f16 or int8 tables. It cannot run bge-small, LateOn or
  any contextual model. On the bench, potion-base-8M scored 0.718 vector-only
  against 0.826 for bge-small, and static MaxSim scored 0.477 (the harness
  README in `coral-benchmarks`).

## Coral's build today

| Fact | Source |
|---|---|
| Desktop targets: macOS `arm64` dmg + zip, Windows `x64` nsis, Linux `x64` AppImage | `apps/desktop/electron-builder.config.ts`, `mac.target`, `win.target`, `linux.target` |
| macOS entitlements: `com.apple.security.cs.allow-jit` only; hardened runtime on | `apps/desktop/resources/entitlements.mac.plist`, `electron-builder.config.ts` `hardenedRuntime: true` |
| Release image: `debian:trixie-slim` final stage, `rust:1.98-trixie` build stage, `cargo build --locked --release -p coral --bin coral`; `TARGETARCH` `amd64` or `arm64` | `docker/Dockerfile` |
| Release workflow builds the image for `linux/amd64` only, from a clean state with no Rust cache | `.github/workflows/release.yml`, `docker buildx build --platform linux/amd64`; comment "No Rust or npm caches" |
| CI runners: Ubuntu 24.04 x64 (Blacksmith), Windows 2025 x64, macOS 26 arm64 | `.github/workflows/ci.yml`, `desktop.yml`, `release.yml` `runs-on` |
| Toolchain 1.98 | `rust-toolchain.toml` |
| No ML crate in the lock file (`ort`, `fastembed`, `candle-*`, `model2vec`, `tokenizers`, `safetensors`, `hf-hub` absent) | `grep` over `Cargo.lock` |
| `trixie-slim` (Debian 13.7) ships `libstdc++.so.6.0.33` and `libgcc_s.so.1`, glibc 2.41 | Measured: `docker run --rm debian:trixie-slim ls /usr/lib/*/libstdc++*` |

## Facts per runtime

### `ort` (pykeio)

| Fact | Value | Source |
|---|---|---|
| Version | 2.0.0-rc.15, published 2026-10-19; no stable 2.0 yet | crates.io API, `max_stable_version: null` |
| ONNX Runtime version | 1.31.0 (ONNX opset 24) | `ort.pyke.io/misc/version-mapping`; `ort-sys/build/download/dist.tsv` URLs carry `ms@1.31.0` |
| MSRV | 1.92 | `Cargo.toml` `rust-version` |
| Default features | `std`, `ndarray`, `tracing`, `download-binaries`, `tls-native`, `copy-dylibs`, `api-31` | `Cargo.toml` `[features] default` |
| License | MIT OR Apache-2.0 | `Cargo.toml` `license`; both license files in the repo |
| Prebuilt targets for CPU use | `aarch64-apple-darwin` (CoreML build), `x86_64-pc-windows-msvc` (DirectML build), `aarch64-pc-windows-msvc` (DirectML build), `x86_64-unknown-linux-gnu` (no EP), `aarch64-unknown-linux-gnu` (no EP). No `x86_64-apple-darwin`, no musl | `ort-sys/build/download/dist.tsv` |
| EP choice when no EP feature is on | The resolver sorts candidates by shared features and takes the first with an exact match, else the first candidate. With an empty feature set the first row wins: on macOS the CoreML build, on Windows the DirectML build | `ort-sys/build/download/resolve.rs` `resolve_dist`; measured: the macOS binary links `CoreML.framework` |
| Static or dynamic | Static by default (`cargo:rustc-link-lib=static=onnxruntime`). `load-dynamic` feature loads a shared library at runtime from `ORT_DYLIB_PATH` or `ort::init_from(path)`. `ORT_PREFER_DYNAMIC_LINK=1` links a shared library at compile time | `ort-sys/build/main.rs`; `ort.pyke.io/setup/linking` |
| What `download-binaries` does | `build.rs` reads `TARGET`, picks a row from `dist.tsv`, downloads the `.tar.lzma2` over HTTPS (native TLS by default), extracts to the OS cache dir (`~/Library/Caches/ort.pyke.io/dfbin/<target>/<sha256>` on macOS), verifies the SHA-256 against `dist.tsv`, then emits the link line. A cached directory skips the download | `ort-sys/build/main.rs`; measured cache path |
| Skip and override switches | `CARGO_NET_OFFLINE`, `ORT_SKIP_DOWNLOAD` or `ORT_OFFLINE` = `1` skips the download and defers a link error. `ORT_LIB_PATH` or `ORT_LIB_LOCATION` points at a self-built ONNX Runtime; `ORT_LIB_PROFILE` picks the build profile; `ORT_CXX_STDLIB` overrides the C++ runtime link | `ort-sys/build/vars.rs`, `main.rs` |
| What CI must hold | Egress to `https://cdn.pyke.io` during `cargo build` on each runner and inside the Docker build stage. `--locked` does not cover build-script downloads. Integrity is pinned (hash in the crate source), availability is not. A release build has no cache, so it downloads every time (10.7 MB for Linux x64). Mitigation: vendor the archive and set `ORT_LIB_LOCATION`, or build ONNX Runtime from source | `dist.tsv`; `release.yml`; measured archive size |
| System libraries linked | Linux: `stdc++`; macOS: `c++`, `Foundation.framework`, `CoreML.framework`; Windows: `shell32`, and for pyke builds `dxguid`, `DXCORE`, `DXGI`, `D3D12`, `DirectML` | `ort-sys/build/static_link/mod.rs` `static_link_prerequisites` |
| C++ runtime of the Linux archive | The docs say "compiled with Clang, not GCC, and depend on libc++". The archive itself holds 5,891 libstdc++-style `std::__cxx11` symbols and zero libc++ `std::__1` symbols, consistent with the `stdc++` link line. Debian trixie-slim has libstdc++ (above) | `ort.pyke.io/misc/prebuilt-binaries`; measured scan of `x86_64-unknown-linux-gnu.tar.lzma2` |
| CPU baseline of x86-64 builds | x86-64-v3: "Intel Haswell (Cores/Xeons after 2013), Intel Gracemont (Atoms after Nov 2021), AMD Excavator or later, or any AMD Ryzen processor" | `ort.pyke.io/misc/prebuilt-binaries` |
| Windows runtime dependency | pyke Windows builds "always ship compiled with DirectML", and the build script links the DirectML import library. DirectML "was introduced in Windows 10, version 1903". Inferred, moderate confidence: the `coral.exe` then imports `DirectML.dll` at load and fails to start on a Windows without it | `ort-sys/build/static_link/mod.rs`; `learn.microsoft.com/windows/ai/directml/dml` |
| Binary size added | Measured on macOS arm64, Rust 1.98, release, `strip = true`: 342,608 B without `ort`, 19,285,824 B with `ort` and one session (CoreML build). Static archives: 83,546,072 B (macOS arm64), 111,701,892 B (Linux x64). Microsoft's shared libraries, 1.31.1: Linux x64 29,345,384 B, Linux arm64 25,266,408 B, macOS arm64 44,797,256 B, Windows x64 17,253,728 B, Windows arm64 17,507,640 B | Measured; `github.com/microsoft/onnxruntime/releases/tag/v1.31.1` archives |
| Int8 | CPU EP runs U8U8, U8S8, S8S8; dynamic quantization recommended for transformers; x86-64 with VNNI and Arm with dot-product instructions are faster, older hardware can be slower than f32; `MatMulNBits` 4-bit exists | `onnxruntime.ai/docs/performance/model-optimizations/quantization.html` |
| Thread count | `with_intra_threads(n)`, `with_inter_threads(n)`, `with_parallel_execution(bool)`, `with_intra_op_spinning(bool)`, `with_inter_op_spinning(bool)`, `with_independent_thread_pool()`. Default intra-op size is the physical core count | docs.rs `SessionBuilder` (rc.15); `onnxruntime.ai/docs/performance/tune-performance/threading.html` |
| One thread, no pool | `CreateThreadPoolHelper`: `if (options.thread_pool_size <= 1) { return nullptr; }`. Inferred, high confidence: with `with_intra_threads(1)` every operator runs on the calling thread | `onnxruntime/core/util/thread_utils.cc` on `main` |
| Thread priority | No setting in ONNX Runtime or `ort`. Two routes: set the calling thread's priority with the OS API when the pool size is 1 (`libc` and `windows-sys` are already in `Cargo.lock`); or `with_thread_manager(T: ThreadManager)` on the session or the environment, where Coral's `create(&self, work)` spawns each pool thread and sets its priority | `src/environment.rs` `pub trait ThreadManager`; `src/session/builder/impl_options.rs` `with_thread_manager`; `tests/thread_manager.rs` |
| Token-level outputs | `session.run` returns every graph output as a tensor; the shape is the model's. No pooling in the crate | docs.rs `Session::run`; measured: `withort` probe |
| CI of the crate | Ubuntu x64, Ubuntu 24.04 arm64, Windows x64, macOS 15 arm64 | `.github/workflows/test.yml` matrix |

### `fastembed-rs`

| Fact | Value | Source |
|---|---|---|
| Version | 7.5.0; MSRV 1.88; edition 2021 | `Cargo.toml` on `main` |
| License | Apache-2.0 | `Cargo.toml`; GitHub license API |
| Runtime | `ort = "=2.0.0-rc.15"`, `default-features = false`, features `ndarray`, `std`, `api-24`; default features add `ort/download-binaries` + `ort/tls-native` and `hf-hub` | `Cargo.toml` `[dependencies]`, `[features]` |
| Targets | Whatever `ort` links; its own CI runs on `ubuntu-latest` only | `.github/workflows/test.yml` |
| Static or dynamic | Same as `ort`; `ort-load-dynamic` feature passes `load-dynamic` through | `Cargo.toml` `[features]` |
| Model source | `TextEmbedding::try_new` downloads from Hugging Face (`hf-hub`); `try_new_from_user_defined(UserDefinedEmbeddingModel::new(onnx_bytes, tokenizer_files))` takes bytes from any source, with `with_quantization`, `with_pooling` and external initializer files | `src/text_embedding/init.rs`, `src/text_embedding/impl.rs` |
| Binary size added | Not measured. Inferred, moderate confidence: `ort` plus `tokenizers` 0.23 (`onig` regex engine), `ndarray`, `safetensors`, `image` (default `image-models`), a few MB over `ort` alone | `Cargo.toml` |
| Int8 | Model list has 17 quantized entries; `QuantizationMode` on user-defined models; "Dynamic quantization cannot be used with batching", so such models run one text per `run` | `src/models/text_embedding.rs`; `src/text_embedding/impl.rs` |
| Thread count | `with_intra_threads(n)`; `None` uses `std::thread::available_parallelism`; `with_session_config(key, value)` passes raw ONNX Runtime keys (spinning off, affinities) | `src/text_embedding/init.rs` `InitOptionsUserDefined` |
| Thread priority | None | same |
| Token-level outputs | `embed` pools (`Pooling::Cls` or `Mean`, no `None`; a TODO in `select_and_pool_output` says so). `transform(texts, batch)` returns `EmbeddingOutput`; `into_raw()` returns the raw per-batch session outputs with the attention mask; output precedence is `OnlyOne`, `text_embeds`, `last_hidden_state`, `sentence_embedding`; `token_embeddings` is commented out | `src/output/embedding_output.rs`; `src/text_embedding/output.rs` |
| BGE-M3 ColBERT | A separate `bgem3_embedding` module returns dense, sparse and ColBERT vectors for that one model | `src/bgem3_embedding/`; README |
| Note on model files | `BGESmallENV15Q` points at `Qdrant/bge-small-en-v1.5-onnx-Q/model_optimized.onnx`, 66.5 MB. Xenova's `bge-small-en-v1.5/onnx/model_int8.onnx` is 33.8 MB against 133.1 MB f32. Inferred, moderate confidence: the Qdrant file is not a full int8 graph | Hugging Face API file sizes |

### `candle`

| Fact | Value | Source |
|---|---|---|
| Version | 0.11.1; edition 2021; no `rust-version` in the workspace manifest | workspace `Cargo.toml` |
| License | MIT OR Apache-2.0 | workspace `Cargo.toml`; `LICENSE-MIT`, `LICENSE-APACHE` |
| Dependencies of `candle-core` | `gemm` 0.19, `rayon`, `half`, `safetensors` 0.8, `memmap2`, `num_cpus`, `tokenizers` 0.23; MKL, Accelerate, CUDA, Metal are optional features | `candle-core/Cargo.toml`; workspace `Cargo.toml` |
| Targets | Pure Rust without the optional features. CI: `ubuntu-latest`, `ubuntu-24.04`, `windows-latest`, `macOS-latest`, `ubuntu-24.04-arm`. Windows arm64 and macOS x64 are not in CI. Inferred, moderate confidence: they compile, since no C library is linked | `.github/workflows/rust-ci.yml` |
| Static or dynamic | Compiled into the binary; no shared library | same |
| Build-time downloads | None | same |
| Binary size added | Measured on macOS arm64: 5,243,920 B for a binary with `BertModel` forward plus `tokenizers`, against 342,608 B empty, so about 4.9 MB | Measured, see the last section |
| Int8 | "Quantization support using the llama.cpp quantized types" (GGUF). `candle-transformers/src/models/` has `quantized_*` for Llama, Mistral, Phi, Qwen2, Qwen3, T5, Gemma3 and others, and no quantized BERT, ModernBERT, Jina or Nomic BERT. `bert.rs` is `DType::F32` with no quantized note | README; directory listing on `main`; `bert.rs` |
| Models for this map | `bert.rs`, `modernbert.rs`, `jina_bert.rs`, `nomic_bert.rs`, `distilbert.rs`, `xlm_roberta.rs`, `qwen3.rs`, `stella_en_v5.rs`. No `arctic`, `gte`, `mpnet` or ColBERT head; LateOn's three `Dense` layers would be written in Rust on top of `modernbert.rs` (small; unverified) | directory listing |
| Thread count | `get_num_threads()` reads `RAYON_NUM_THREADS`, `CANDLE_NUM_THREADS`, on Linux `CANDLE_CPU_MASK`, else physical cores; these are process-wide environment variables, not a per-session API. Work runs on rayon's global pool (inferred from the `rayon` dependency, moderate confidence) | `candle-core/src/utils.rs` |
| Thread priority | None | same |
| Token-level outputs | `BertModel::forward(input_ids, token_type_ids, attention_mask) -> Tensor` returns the encoder's full sequence output; there is no pooler in the file | `candle-transformers/src/models/bert.rs` |

### `model2vec-rs`

| Fact | Value | Source |
|---|---|---|
| Version | 0.4.0; MSRV 1.88; edition 2024 | `Cargo.toml` |
| License | MIT | README; GitHub license API |
| Dependencies | `tokenizers` 0.21 (`default-features = false`), `safetensors` 0.5, `half`, `hf-hub` 0.4 optional; no `ndarray`, no direct `rayon` | `Cargo.toml` |
| Features | `default = ["onig", "hf-hub"]`; `local-only` removes the Hub client; `wasm` | `Cargo.toml` |
| Targets | Pure Rust. CI: `ubuntu-latest` plus a `wasm32-unknown-unknown` check. Inferred, moderate confidence: compiles on all six targets | `.github/workflows/ci.yaml` |
| Model source | `StaticModel::from_pretrained(path_or_repo)`; `from_bytes(tokenizer, model, config)`; `from_owned_i8` | `src/model.rs` |
| Binary size added | Measured on macOS arm64: 4,170,304 B for a binary with `StaticModel::encode` plus `tokenizers`, against 342,608 B empty, so about 3.8 MB | Measured, see the last section |
| Int8 | Embedding tensor dtypes `F32`, `F16` (decoded to f32), `I8` per-row symmetric; `quantize_int8()`; README reports cosine above 0.9998 against f32 | `src/model.rs`; README |
| How it computes | Char-level truncation, token id lookup into the table, mean over rows, skip `unk`, L2 normalize; `max_length` default 512 | `src/model.rs` |
| Thread count | `maybe_par_iter` from `tokenizers` (rayon's global pool); the README benchmarks set `RAYON_NUM_THREADS=1`. No API | `src/model.rs`; README |
| Thread priority | None | same |
| Token-level outputs | Static per-token rows exist, but they carry no context. P's harness measured static MaxSim with potion-base-8M at hit@10 0.477 vector-only on Set A, against 0.718 pooled | `coral-benchmarks/tools/universal-search/prototype-semantic/README.md` |
| Throughput | 575K texts/s single thread on an M3 Pro with potion-base-8M (README benchmark) | README |

### ONNX Runtime itself (what `ort` links)

| Fact | Value | Source |
|---|---|---|
| Latest release | 1.31.1 on 2026-10-26 (patch of 1.31.0, 2026-10-09) | GitHub releases |
| License | MIT | GitHub license API |
| Official prebuilt packages, 1.31.1 | `linux-x64`, `linux-aarch64`, `osx-arm64`, `win-x64`, `win-arm64`, `win-arm64x`, plus CUDA variants. No `osx-x86_64` and no `osx-universal2` since 1.25.0 (2026-04-20); 1.23.0 (2025-09-26) still had them | release asset lists |
| glibc floor of the official Linux build | Highest versioned symbol `GLIBC_2.28`, `GLIBCXX_3.4.22`; links `libstdc++.so.6`, `libgcc_s.so.1` | Measured scan of `libonnxruntime.so.1.31.1` x64 and aarch64 |
| Windows prerequisite | "Windows builds require Visual C++ 2019 runtime" | `onnxruntime.ai/docs/install/` |
| 1.31.0 notes relevant here | Windows ARM64 GitHub Actions coverage added; Arm KleidiAI SVE2.1 FP16 GEMM; CPU weight prepacking parallelized; ONNX dependency stays at 1.22.0 | release notes v1.31.0 |
| Custom and minimal builds | A minimal build drops unused kernels and runs ORT-format models only; the docs give no size figure | `onnxruntime.ai/docs/build/custom.html` |

## What a late-interaction scorer needs

- A per-token output tensor. LightOn's `lightonai/LateOn` (ModernBERT, 149M
  parameters, Apache 2.0, 128-dimensional token vectors, query length 32,
  document length 300, PyLate, MaxSim) hosts `model.onnx` (615.8 MB) and
  `model_int8.onnx` (154.7 MB) beside the safetensors. The model card does not
  document them. The int8 graph, loaded with the `onnx` package: inputs
  `input_ids` and `attention_mask` `INT64 [batch_size, sequence_length]`,
  output `output FLOAT [batch_size, sequence_length, 128]`, ir_version 7,
  opset 14, producer `onnx.quantize`, 93 `MatMulInteger` and 91
  `DynamicQuantizeLinear` nodes (dynamic int8). `ort` runs it as is.
- A tokenizer. LateOn's `tokenizer.json` is a ModernBERT BPE; `tokenizers`
  0.23 (the version `candle` and `fastembed` use) loads it. LateOn is
  case-sensitive.
- The scorer: for each query token vector, the maximum dot product over the
  document's token vectors, summed over query tokens. With L2-normalized
  vectors this is `f32` arithmetic over slices; no runtime feature is involved.
- Query-time cost, inferred: a 32-token query against every Set B token at 128
  dimensions is 32 x 2.0M x 128 = 8.2 GFLOP per search over the signature
  index text. A re-rank of the top N rows from the lexical and dense lists
  bounds that cost; a full scan does not. Unmeasured.
- Masking: the scorer must skip padding and, for ColBERT-style models, the
  query-side expansion tokens; `attention_mask` from the tokenizer gives the
  document mask.

## Storage multiplier of per-token vectors on Set B

Measured on `coral-benchmarks/tools/universal-search/prototype-semantic/cache/corpus-graph-pg.jsonl`
(19,830 rows, every row has a signature line in the 2026-10-20 Graph run's
`inventory.jsonl`), with `tokenizers` 0.22.2, special tokens added, no
truncation. Index texts follow the harness: `path+title+desc` is provider
display name + Call Path words, title, description; `+sig` appends the
signature line.

| Tokenizer | Index text | p50 | mean | p90 | p99 | max | tokens, all | tokens, cut at 300 | tokens, cut at 512 | rows over 300 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| bge-small (WordPiece) | path+title+desc | 34 | 46.6 | 74 | 231 | 4,181 | 924,552 | 904,148 | 915,776 | 101 |
| bge-small (WordPiece) | path+title+desc+sig | 103 | 112.8 | 159 | 319 | 4,217 | 2,237,566 | 2,206,145 | 2,226,650 | 236 |
| LateOn (ModernBERT BPE) | path+title+desc | 33 | 45.3 | 71 | 223 | 3,967 | 897,484 | 880,573 | 889,985 | 83 |
| LateOn (ModernBERT BPE) | path+title+desc+sig | 93 | 102.2 | 142 | 294 | 3,997 | 2,027,559 | 2,003,939 | 2,018,785 | 182 |

Vector bytes, raw payload only (no row ids, tuple headers or index pages):

| Layout | Bytes per unit | Set B total | Multiplier vs one 384 x f32 per row |
|---|---:|---:|---:|
| One 384 x f32 vector per row (bge-small class) | 1,536 B per row | 30.5 MB | 1.0x |
| One 384 x int8 vector per row | 384 B per row | 7.6 MB | 0.25x |
| 128 x f32 per token, path+title+desc, LateOn, cut at 300 | 512 B per token | 450.9 MB | 14.8x |
| 128 x f32 per token, path+title+desc+sig, LateOn, cut at 300 | 512 B per token | 1,026.0 MB | 33.7x |
| 128 x int8 per token, path+title+desc, cut at 300 | 128 B per token | 112.7 MB | 3.7x |
| 128 x int8 per token, path+title+desc+sig, cut at 300 | 128 B per token | 256.5 MB | 8.4x |

The multiplier is mean tokens per row divided by 3 (384 / 128). pgvector
stores a `vector(128)` as 4 + 128 x 4 bytes plus the tuple header and the row
id, so the table is larger than the payload; the overhead is unmeasured.
Using bge-small's own 384-dimensional hidden states for MaxSim, without a
projection, triples the per-token figures.

## Crate pins: `fastembed-rs` and `next-plaid-onnx` cannot share one binary

Facts from #2859, verified here against the crates.io manifests on 2026-11-04:

| Crate | `ort` requirement | Other pins | Source |
|---|---|---|---|
| `fastembed` 7.5.0 | `=2.0.0-rc.15`, features `ndarray`, `std`, `api-24`, optional | `tokenizers ^0.23.2` | crates.io `/api/v1/crates/fastembed/7.5.0/dependencies`; `Cargo.toml` on `main` |
| `next-plaid-onnx` 1.8.5 (LightOn, Apache-2.0, published 2026-10-10, "Fast ColBERT multi-vector encoding using ONNX Runtime") | `=2.0.0-rc.11`, default features on, plus `ndarray`, `load-dynamic` | `tokenizers ^0.21.1`, `ndarray ^0.16` | crates.io `/api/v1/crates/next-plaid-onnx/1.8.5/dependencies`; repo `lightonai/next-plaid` |

Measured: a scratch crate that depends on `fastembed = "=7.5.0"` and
`next-plaid-onnx = "=1.8.5"` fails at `cargo generate-lockfile` with "failed
to select a version for `ort`" (previously selected `ort v2.0.0-rc.15` for
`fastembed`, `=2.0.0-rc.11` required by `next-plaid-onnx`). Cargo treats both
as one `2.x` slot, and `ort-sys` carries `links = "onnxruntime"`, so two copies
could not link even if Cargo allowed them. `fastembed-rs` has no
late-interaction model type (the `bgem3_embedding` module is the one ColBERT
output it knows). `next-plaid-onnx` also uses `load-dynamic`, so it expects
a shared ONNX Runtime library at run time, not the static link this file
recommends.

**A Rust MaxSim scorer over `ort` directly avoids the conflict.** Coral then
depends on one `ort` version of its own choice, runs the single-vector model
and the late-interaction model through the same `Session::run`, and writes
MaxSim over the `[batch, sequence, 128]` output itself. Neither `fastembed-rs`
nor `next-plaid-onnx` is in the dependency graph, so neither pin applies.
The two helper crates save only the tokenizer and input-tensor glue, which
`tokenizers` 0.23 and `ort` provide. If Coral did take `fastembed-rs` for the
single-vector model, `next-plaid-onnx` would still have to stay out; the
ColBERT encode is the same `session.run` call and gains nothing from it.

## Which runtime loads which passing model

The passing list is in #2859 (comment of 2026-11-04). Per runtime:

| Model | `ort` (ONNX file exists per #2859) | `candle` | `model2vec-rs` |
|---|---|---|---|
| potion-base-8M | No ONNX | No | Yes, its own format |
| all-MiniLM-L6-v2, bge-small-en-v1.5, bge-base-en-v1.5, arctic-embed-xs, arctic-embed-s | Yes | `bert.rs`, f32 only | No |
| nomic-embed-text-v1.5 | Yes | `nomic_bert.rs`, f32 only | No |
| jina-embeddings-v2-base-code | Yes | `jina_bert.rs`, f32 only | No |
| Qwen3-Embedding-0.6B | Yes (onnx-community export) | `qwen3.rs` f32, `quantized_qwen3.rs` GGUF | No |
| colbertv2.0, answerai-colbert-small-v1 | Yes | BERT backbone in `bert.rs`; the ColBERT linear head is not in candle and would be written in Rust. Unverified | No |
| LateOn, GTE-ModernColBERT-v1, LateOn-Code-edge | Yes (LateOn's int8 graph verified above) | `modernbert.rs` backbone, f32; the projection head is not in candle. Backbone of LateOn-Code-edge unverified | No |
| mxbai-edge-colbert-v0-17m, -32m | Yes | Backbone not checked. Unverified | No |

`candle` rows name the architecture file in `candle-transformers/src/models/`
and say nothing about weight-name mapping, which is unverified for every
model. Only `ort` runs the int8 files.

## Open questions

1. Whether the Windows executable with the pyke static build starts on a
   Windows without `DirectML.dll` and `DXCore.dll`, or whether those imports
   are delay-loaded. Unverified; a Windows CI run with the probe binary
   answers it.
2. The x86-64-v3 baseline of the pyke binaries against Coral's Desktop user
   base on Windows and Linux x64. A self-built ONNX Runtime or Microsoft's
   shared library removes the constraint; the cost is a build pipeline or a
   17 to 29 MB shared library in the package.
3. A vendored ONNX Runtime archive in CI, so a release build does not depend
   on `cdn.pyke.io` being up. `ORT_LIB_LOCATION` is the switch; nothing is
   set up.
4. Binary size on Linux x64 and Windows x64 with the pyke static build. Only
   macOS arm64 was measured (about 19 MB added).
5. Latency of LateOn int8 on one core at document length 300, and of bge-small
   int8, on an M4 Pro and on a server-class box. This map's measurement
   tickets own it; the runtime choice does not change it.
6. Whether Coral keeps `ort` at `2.0.0-rc.15` or waits for 2.0.0 stable. The
   release candidates have shipped since 2024; rc.14 and rc.15 arrived in
   October 2026.
7. Whether `ort`'s macOS build should be a CPU-only self-build instead of the
   CoreML build the resolver picks. The CoreML provider is compiled in but not
   registered unless the `coreml` feature is on; the cost is binary size only.
8. The `onnx` opset of the chosen single-vector model's int8 export against
   ONNX Runtime 1.31 (opset 24 ceiling); the candidates listed above are at or
   below opset 14 where checked (LateOn). Unverified for the others.

## How the numbers were measured

Scratch projects outside the repo, Rust 1.98, `cargo build --release` with
`strip = true`, host `aarch64-apple-darwin`, 2026-11-04.

- `ort` probe: `ort = "=2.0.0-rc.15"` default features; one binary builds an
  `Environment`, a `Session` with `with_intra_threads(1)` and
  `GraphOptimizationLevel::Level3`, runs one `input_ids` tensor. Sizes:
  `empty` 342,608 B, `withort` 19,285,824 B. `otool -L` lists
  `libc++.1.dylib`, `Foundation`, `CoreML`, `libSystem`, `CoreFoundation`,
  `libobjc`. The build downloaded
  `aarch64-apple-darwin+coreml.tar.lzma2` and cached
  `libonnxruntime.a` (83,546,072 B) under `~/Library/Caches/ort.pyke.io/dfbin/`.
- pyke Linux x64 archive: `x86_64-unknown-linux-gnu.tar.lzma2`, 10,744,417 B,
  SHA-256 `02aac0d3...7678` matches `dist.tsv`; raw LZMA2 with a 64 MiB
  dictionary (as `ort-sys/build/download/extract.rs` reads it); contains
  `libonnxruntime.a` 111,701,892 B.
- Microsoft 1.31.1 archives: listed with `tar tzvf` and `unzip -l`.
- Token counts: a Python script over the corpus and inventory with
  `tokenizers` 0.22.2, `BAAI/bge-small-en-v1.5` `tokenizer.json` and
  `lightonai/LateOn` `tokenizer.json`.
- LateOn ONNX graph: `onnx` 1.19.1 `onnx.load(..., load_external_data=False)`
  on `model_int8.onnx` from `huggingface.co/lightonai/LateOn`.
- Pin conflict: a scratch crate with `fastembed = { version = "=7.5.0",
  default-features = false, features = ["onnx"] }` and
  `next-plaid-onnx = "=1.8.5"`; `cargo +1.98 generate-lockfile` fails as
  quoted in the "Crate pins" section.
- `candle` and `model2vec-rs` probes: `candle-core`, `candle-nn`,
  `candle-transformers` `=0.11.1` default features with a `BertModel` forward,
  and `model2vec-rs =0.4.0` with features `onig`, `local-only` and
  `StaticModel::encode`. Sizes: `withcandle` 5,243,920 B, `withmodel2vec`
  4,170,304 B, against the same 342,608 B empty binary. Both include
  `tokenizers` with the `onig` regex engine, which is not measured on its own.

## Sources

Read on 2026-11-04 unless a date is given.

- pykeio/ort, branch `main`: `Cargo.toml`; `ort-sys/build/main.rs`,
  `vars.rs`, `download/dist.tsv`, `download/resolve.rs`, `download/mod.rs`,
  `download/extract.rs`, `static_link/mod.rs`; `src/environment.rs`;
  `src/session/builder/impl_options.rs`; `tests/thread_manager.rs`;
  `.github/workflows/test.yml`. Releases: v2.0.0-rc.15 (2026-10-19),
  rc.14 (2026-10-12), rc.13 (2026-07-28), rc.12 (2026-03-05).
- ort.pyke.io: `/setup/linking` (updated 2026-08-23), `/setup/cargo-features`
  (2026-07-28), `/misc/version-mapping` (2026-10-09), `/misc/prebuilt-binaries`
  (2026-07-28). `/setup/platforms` renders its table in JavaScript and gave no
  static text; `dist.tsv` stands in for it. The site answered 403 to the
  fetch tool and 200 to `curl`.
- docs.rs/ort/2.0.0-rc.15: `session::builder::SessionBuilder`.
- crates.io API: `/api/v1/crates/ort`.
- microsoft/onnxruntime: releases v1.20.0 to v1.31.1 (asset lists, v1.31.0 and
  v1.31.1 notes); `onnxruntime/core/util/thread_utils.cc` on `main`;
  `LICENSE`. onnxruntime.ai docs: `performance/tune-performance/threading`,
  `performance/model-optimizations/quantization`, `reference/compatibility`,
  `install/`, `build/custom`.
- Anush008/fastembed-rs, `main`: `Cargo.toml`, `README.md`,
  `src/text_embedding/impl.rs`, `init.rs`, `output.rs`,
  `src/output/embedding_output.rs`, `src/pooling.rs`,
  `src/models/text_embedding.rs`, `.github/workflows/test.yml`.
- huggingface/candle, `main`: `Cargo.toml`, `candle-core/Cargo.toml`,
  `candle-core/src/utils.rs`, `candle-transformers/src/models/` listing and
  `bert.rs`, `README.md`, `LICENSE-MIT`, `.github/workflows/rust-ci.yml`.
- MinishLab/model2vec-rs, `main`: `Cargo.toml`, `README.md`, `src/model.rs`,
  `.github/workflows/ci.yaml`.
- Hugging Face: model cards and file lists (API `?blobs=true`) of
  `lightonai/LateOn`, `Qdrant/bge-small-en-v1.5-onnx-Q`,
  `Xenova/bge-small-en-v1.5`, `snowflake/snowflake-arctic-embed-xs`,
  `Xenova/all-MiniLM-L6-v2`, `nomic-ai/nomic-embed-text-v1.5`,
  `intextus/lateon-onnx`.
- crates.io API: `/api/v1/crates/next-plaid-onnx` and
  `/api/v1/crates/next-plaid-onnx/1.8.5/dependencies`;
  `/api/v1/crates/fastembed/7.5.0/dependencies`. `ort-sys` 2.0.0-rc.15
  `Cargo.toml` (`links = "onnxruntime"`) from the local registry copy.
- withcoral/coral #2859, resolution comment of 2026-11-04 (passing model list).
- learn.microsoft.com: `windows/ai/directml/dml` (page dated 2025-02-10).
- Coral: `apps/desktop/electron-builder.config.ts`,
  `apps/desktop/resources/entitlements.mac.plist`, `docker/Dockerfile`,
  `rust-toolchain.toml`, `.github/workflows/{ci,desktop,release}.yml`,
  `Cargo.lock` at `c82b1700e`.
- coral-benchmarks (local checkout):
  `tools/universal-search/prototype-semantic/README.md`,
  `semantic_prototype.py`, `cache/corpus-graph-pg.jsonl`;
  `.worktrees/us-v1-hits/tools/universal-search/runs/2026-10-20-graph-pg/inventory.jsonl`.
