# sqlite-vec inside the Coral process

Research for issue #2860, part of the semantic search map #2856.
Written 2026-11-04 against Coral commit `c82b1700e` and sqlite-vec crate 0.1.9.
Line numbers below refer to that Coral commit and to the `sqlite-vec.c` inside the
published 0.1.9 crate. Nothing was built or run; the `cargo tree` and archive
listings below are reads.

## Answer

**sqlite-vec loads inside the Coral process on the bundled SQLite. Yes.**
Confidence: high.

**It can carry Universal Search's eligibility filter, as the same SQL the lexical
retriever already runs. Yes.** Confidence: moderate to high. The mechanism is
`rowid IN (<eligibility subquery>)` on the `vec0` KNN query. The code path exists
in 0.1.9 and filters before any distance is computed. A prototype must confirm
the planner hands a subquery to it (open question 1).

Deciding facts:

1. Coral links exactly one SQLite. `Cargo.lock` holds one `libsqlite3-sys`
   (0.37.0), used by both `rusqlite` 0.39.0 and `sqlx-sqlite` 0.9.0. The `bundled`
   feature is on from both sides (`cargo tree -e features -i libsqlite3-sys`).
   The bundled source is SQLite 3.51.3, compiled with `SQLITE_CORE`,
   `SQLITE_ENABLE_LOAD_EXTENSION=1` and `SQLITE_THREADSAFE=1` [S1] [S2].
2. The `sqlite-vec` crate has no runtime dependency. Its `build.rs` compiles
   `sqlite-vec.c` with `SQLITE_CORE` into a static library and `src/lib.rs`
   exports the C entry point `sqlite3_vec_init` [S3]. The entry point calls
   SQLite's API symbols directly, so it binds to the bundled 3.51.3 at link time.
3. `sqlite3_auto_extension` is process global. SQLite runs every registered entry
   point inside each `sqlite3_open*` call, whichever library made the call. The
   list lives under SQLite's main static mutex [S4] [S5]. sqlx's pools open
   through `sqlite3_open_v2` and get the extension like any other connection [S6].
4. Registration must run before the first pool. In Coral every SQLite pool opens
   in `crates/coral-db/src/database.rs` (`connect_sqlite`, line 172;
   `connect_read_only_sqlite`, line 213). A `std::sync::Once` before the writer
   pool at line 192 covers every production and test path. Section 3 has the
   detail.
5. `vec0` KNN accepts one `rowid IN (...)` constraint, reads the whole set
   through `sqlite3_vtab_in_first` and `sqlite3_vtab_in_next` before the scan,
   ANDs it into the candidate bitmap, and only then computes distances. The flat
   path returns the exact top k over the filtered set [S7]. SQLite offers every
   `IN` term to a virtual table all at once except row-value slices, so a subquery
   on the right side qualifies [S8].
6. The costs are small. The 0.1.9 static library is 159 KB on macOS arm64 and
   224 KB on Linux x86_64 [S9]. The project's own brute-force numbers put a
   20,000 row scan at 384 or 768 dimensions in single-digit milliseconds on an
   M1 class core, under 15 ms as a bound (section 5).

What argues against it, in short: the project is pre-v1 with one maintainer, had a
16 month gap between stable releases, and its own author calls the docs stale.
The Rust crate has no upstream CI, so a Windows MSVC build of the crate is
unverified. None of these is a "no". Section 7 lists them as open questions.

## 1. Crate version, pre-v1 status, release cadence, license

| Fact | Value | Source |
| --- | --- | --- |
| Crate | `sqlite-vec`, "FFI bindings to the sqlite-vec SQLite extension" | [S10] |
| Latest stable | 0.1.9, published 2026-03-31 | [S10] |
| Newest | 0.1.10-alpha.4, published 2026-05-18 | [S10] |
| License | `MIT/Apache-2.0` (dual, user picks) | [S10] [S11] |
| Crate size | 227,786 bytes packed | [S10] |
| Runtime deps | none | [S12] |
| Build deps | `cc ^1.0` | [S12] |
| Dev deps | `rusqlite 0.31.0` | [S12] |
| Vendored SQLite header | 3.45.3 (`sqlite3.h`, 641,889 bytes) | [S13] |
| Extension version string | `v0.1.9`, built 2026-03-31 | [S13] |

**Pre-v1.** The README says: "`sqlite-vec` is a pre-v1, so expect breaking
changes!" [S11]. The v0.1.0 announcement (2024-08-01) hoped for "a stable `v1`
release in the next year or so" [S14]. Twenty-seven months later the line is still
0.1.x. The 0.1.7 notes (2026-03-17) say "the documentation site is stale, docs are
out-of-date" and "There's still much to do" [S15]. The 0.1.10 alpha line adds
approximate indexes (rescore, DiskANN, an IVF marked "experimental, not enabled")
[S16]. The flat `vec0` path this note relies on predates them.

**Release cadence (stable tags).** From the crates.io version list [S10]:

| Version | Date |
| --- | --- |
| 0.1.0 | 2024-08-01 |
| 0.1.1 | 2024-08-06 |
| 0.1.2 | 2024-09-13 |
| 0.1.3 | 2024-09-26 |
| 0.1.4, 0.1.5 | 2024-11-15 |
| 0.1.6 | 2024-11-20 |
| 0.1.7 | 2026-03-17 |
| 0.1.8 | 2026-03-30 |
| 0.1.9 | 2026-03-31 |

Gap between 0.1.6 and 0.1.7: 16 months. Last activity of any kind: the
0.1.10-alpha.4 tag on 2026-05-18, five and a half months before this note.
Development is sponsored through Mozilla Builders and company sponsors [S11].
The upstream CI builds and tests the loadable extension on Linux x86_64 and
arm64, macOS Intel and arm64, and Windows with `cl.exe`. It has no Rust step and
never builds `bindings/rust` [S17].

**Notable changes per release** [S15] [S16] [S18] [S19] [S20]:

- 0.1.6: metadata columns, partition keys and auxiliary columns.
- 0.1.7: fuzz testing; "Proper `DELETE` support: Deleting vectors will now clear
  up space (if you DELETE enough to clear out a chunk, ~1024 vectors)";
  range constraints on the distance column.
- 0.1.9: fixes `DELETE` on tables with text metadata longer than 12 characters,
  which returned an error before.
- 0.1.10-alpha.2: `ALTER TABLE RENAME` support. So 0.1.9 cannot rename a `vec0`
  table.
- 0.1.10-alpha.3: `INSERT OR REPLACE INTO` support. So 0.1.9 needs `DELETE`
  then `INSERT`, or `UPDATE`, for an upsert.

## 2. Coral's SQLite: one bundled library, shared by rusqlite and sqlx

`Cargo.toml` pins `rusqlite = { version = "0.39", features = ["bundled"] }`
(line 75) and `sqlx = { version = "0.9", ..., features = [..., "sqlite", ...] }`
(line 95) [S1]. The lock resolves to `rusqlite 0.39.0`, `sqlx 0.9.0`,
`sqlx-sqlite 0.9.0` and a single `libsqlite3-sys 0.37.0` [S1].

`cargo tree -e features -i libsqlite3-sys` shows the `bundled` feature reached
from `sqlx` feature `sqlite` through `sqlite-bundled` and `sqlx-sqlite/bundled`,
and from `rusqlite` [S1]. `sqlx-sqlite` declares
`libsqlite3-sys = ">=0.30.1, <0.38.0"` with default features off [S21], so both
crates agree on 0.37.0 and share its symbols.

`libsqlite3-sys` 0.37.0 bundles SQLite 3.51.3 and compiles it with, among others,
`-DSQLITE_CORE`, `-DSQLITE_ENABLE_FTS5`, `-DSQLITE_ENABLE_JSON1`,
`-DSQLITE_ENABLE_LOAD_EXTENSION=1`, `-DSQLITE_ENABLE_RTREE` and
`-DSQLITE_THREADSAFE=1` [S2]. `sqlite3_auto_extension` is defined outside the
`SQLITE_OMIT_LOAD_EXTENSION` guard in the amalgamation, so it would exist even
without that flag [S5].

Only `crates/coral-app` uses `rusqlite` directly, for the read-only format probe
in `src/state/database_format.rs` (line 36) [S22]. Every other SQLite access goes
through sqlx pools that `crates/coral-db` opens.

## 3. Registration through `sqlite3_auto_extension`, and where

### What the crate gives

`src/lib.rs` of the crate is one extern block [S3]:

```rust
#[link(name = "sqlite_vec0")]
extern "C" {
    pub fn sqlite3_vec_init();
}
```

`build.rs` is one line [S3]:

```rust
cc::Build::new().file("sqlite-vec.c").define("SQLITE_CORE", None).compile("sqlite_vec0");
```

The real C signature is
`int sqlite3_vec_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi)`
[S13]. Under `SQLITE_CORE` the function skips `SQLITE_EXTENSION_INIT2`, so the
`pApi` pointer is unused and the extension calls SQLite's exported functions
directly [S23]. The crate's Rust declaration drops the arguments on purpose to
match SQLite's `void (*)(void)` prototype. The project's Rust page registers it
before any connection opens [S24]:

```rust
use rusqlite::ffi::sqlite3_auto_extension;
use sqlite_vec::sqlite3_vec_init;

unsafe {
    sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_vec_init as *const ())));
}
```

`rusqlite::auto_extension::register_auto_extension(ax: RawAutoExtension)` is a
typed wrapper. `RawAutoExtension` is
`unsafe extern "C" fn(db: *mut sqlite3, pz_err_msg: *mut *mut c_char, _: *const sqlite3_api_routines) -> c_int`,
which matches `sqlite3_vec_init`'s real signature. The module is available when
rusqlite is not built as a loadable extension, which is Coral's case [S25].
Coral-db today depends on sqlx only, so it would add `libsqlite3-sys` (already in
the lock) or `rusqlite` to reach `sqlite3_auto_extension`.

### What SQLite guarantees

From the bundled `sqlite3.h` [S4]:

> This interface causes the xEntryPoint() function to be invoked for each new
> database connection that is created. The idea here is that xEntryPoint() is the
> entry point for a statically linked SQLite extension that is to be
> automatically loaded into all new database connections.

> Calling sqlite3_auto_extension(X) with an entry point X that is already on the
> list of automatic extensions is a harmless no-op. No entry point will be called
> more than once for each database connection that is opened.

If the entry point fails, the `sqlite3_open*` call that triggered it fails [S4].
In the amalgamation the registration takes `SQLITE_MUTEX_STATIC_MAIN` and appends
to a process-wide list; `openDatabase` calls `sqlite3AutoLoadExtensions(db)` on
every open [S5]. Connections opened before the registration do not get it.

sqlx's own `SqliteConnectOptions::extension()` is a different thing. It loads a
shared library by path through `sqlite3_load_extension`, needs sqlx's
`load-extension` feature, and enables `SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION` only
while it loads [S6] [S26]. Coral's sqlx feature list does not include
`load-extension` [S1]. The auto-extension route needs none of it.

### Where it has to happen in Coral

Every SQLite pool opens in `crates/coral-db/src/database.rs` [S27]:

- `connect_sqlite` (line 172) builds `SqliteConnectOptions` (line 181), opens the
  one-connection writer pool with `SqlitePoolOptions::new().connect_with`
  (line 192), then the reader pool with `query_only` on (line 197).
- `connect_read_only_sqlite` (line 213) opens one read-only pool (line 221).

Process bootstrap (`crates/coral-app/src/state/initialization.rs`, lines 66 to
101) runs in this order: `validate_database_state` (the rusqlite probe),
`Database::connect` (line 92), `coral_search::prepare_storage` (line 94),
`migrate_current` (line 95) [S28].

The registration belongs in coral-db, in a `std::sync::Once` right before the
writer pool at line 192, and the same call at line 221. Reasons:

- coral-db owns pools by its own module doc ("owns backend selection, connection
  pools...") [S29], and `coral-search` depends on it, so the call cannot live in
  coral-search without a hook.
- Every test path reaches those lines too: `coral-test-support` calls
  `Database::connect` (lib.rs lines 120, 134, 149, 190, 219) before it opens its
  own raw sqlx connections for fixtures (line 169) and for
  `connect_virtual_tables` (line 247) [S30]. That helper runs
  `SELECT 1 FROM "<table>" LIMIT 0` on every `CREATE VIRTUAL TABLE` object in
  the file, so a `vec0` table in the schema makes the extension mandatory in
  test processes as well.
- The rusqlite format probe at `database_format.rs` line 36 opens earlier and
  without the extension. That is fine: it reads `sqlite_schema` and
  `_sqlx_migrations` only [S22].

Boot check: `coral_search::prepare_storage` already probes FTS5 and the trigram
tokenizer by creating temporary virtual tables and reports
`StorageError::MissingFeature` with a remedy [S31]. A `vec0` probe
(`CREATE VIRTUAL TABLE temp.coral_search_vec_probe USING vec0(v float[2])`) and
`SELECT vec_version()` fit that pattern.

Side effect to accept: the extension loads into every SQLite connection of the
process, including the format probe and anything else that opens SQLite in the
`coral` binary. Each open registers two modules (`vec0`, `vec_each`) and about
eighteen functions [S23]. The Desktop app runs the Rust runtime as its own
process (`apps/desktop/scripts/runtime.mjs` runs `cargo build` for it) [S32], so
Electron's SQLite, if any, is untouched.

## 4. `vec0` capabilities

All limits below come from the 0.1.9 source in the published crate [S33] unless
marked otherwise. The documentation site lags the code; where they differ the
code is quoted.

### Table shape

```sql
CREATE VIRTUAL TABLE search_vectors USING vec0(
    workspace_id TEXT PARTITION KEY,
    embedding FLOAT[384] distance_metric=cosine,
    +doc_id TEXT
);
```

| Item | Rule | Source |
| --- | --- | --- |
| Element types | `float` (float32), `int8`, `bit` | [S33] lines 2371 to 2377, [S11] |
| Dimensions | 1 to 8192 per vector column (`SQLITE_VEC_VEC0_MAX_DIMENSIONS`) | [S33] line 3488 |
| Vector columns | up to 16 | [S33] |
| Distance metrics | `l2` (default), `l1`, `cosine`, per column | [S33] lines 2442 to 2446 |
| Primary key | integer, or text backed by an internal int64 rowid | [S33] lines 3445 to 3447 |
| Partition keys | up to 4; types `text`, `int`, `integer` | [S33] line 3484 and parser |
| Metadata columns | up to 16; `TEXT`, `INTEGER`, `FLOAT`, `BOOLEAN` | [S33] line 3486, [S34] |
| Auxiliary columns | up to 16, `+` prefix, not usable in a KNN `WHERE` | [S33] line 3485, [S34] |
| `chunk_size` | default 1024, max 4096, divisible by 8 | [S33] lines 4921 to 4943 |

Shadow tables per `vec0` table: `_info`, `_chunks`, `_rowids`,
`_vector_chunks00`, `_metadatachunks00`, `_metadatatext00`, `_auxiliary`
(numbered per column) [S35]. They are ordinary tables in the main schema, like
FTS5's five. `DROP TABLE` on the virtual table removes them.

### KNN query

```sql
SELECT rowid, distance
FROM search_vectors
WHERE embedding MATCH ?1 AND k = ?2
  AND workspace_id = ?3
  AND rowid IN (SELECT ...)
ORDER BY distance;
```

| Rule | Detail | Source |
| --- | --- | --- |
| `MATCH` | exactly one, on a vector column | [S33] `vec0BestIndex` |
| `k` | `k = ?` required, or `LIMIT` on SQLite 3.41+ (Coral has 3.51.3); not both | [S33], [S36] |
| `k` range | 0 to 4096 (`SQLITE_VEC_VEC0_K_MAX`); 0 returns no rows | [S33] line 7111 |
| `ORDER BY` | `distance` ascending only | [S33] |
| `rowid IN (...)` | one allowed; needs SQLite 3.38+; read all at once through `sqlite3_vtab_in` | [S33] lines 5484 to 5612 |
| Partition key ops | `=`, `!=`, `<`, `<=`, `>`, `>=` | [S33] lines 5636 to 5656 |
| Metadata ops | `=`, `!=`, `<`, `<=`, `>`, `>=`; boolean only `=` and `!=` | [S34] |
| Metadata `IN` | code accepts `IN` on `INTEGER` and `TEXT` metadata columns; the docs do not list it | [S33] lines 5694 to 5712 |
| Forbidden | `IS NULL`, `LIKE`, `GLOB`, `REGEXP` on metadata: "error or incorrect results" | [S34] |
| Distance range | one of `<`, `<=`, `>`, `>=` on `distance`, applied after distances | [S33], [S15] |

Filter order in the flat path, from `vec0Filter_knn` and
`vec0Filter_knn_chunks_iter` [S7]:

1. Partition constraints go into the `WHERE` of the chunk `SELECT`, so other
   partitions' chunks are never read.
2. The `rowid IN` values are collected and sorted; each chunk's validity bitmap is
   ANDed with the rowid set.
3. Metadata constraints fill a bitmap per constraint and AND into the same bitmap.
4. Distances are computed only for rows still set.
5. Per chunk, `min_idx` keeps the best `min(k, chunk_size)`; `merge_sorted_lists`
   merges into the running top `k`.

So partition, rowid and metadata filters are pre-filters and the result is exact
over the filtered set. Only the distance-range constraint is a post-filter.

### Carrying Coral's eligibility filter

The lexical retriever's SQLite statement joins `search_entities_fts` to
`search_entities d` and applies `Scope::sql`, which for Tools is [S37]:

```sql
d.workspace_id = ?1 AND d.kind = 'provider_tool'
AND d.provider_id IN (SELECT value FROM json_each(?2))
AND d.provider_generation_id IN (SELECT value FROM json_each(?3))
AND NOT EXISTS (SELECT 1 FROM json_each(?4) AS x
                WHERE json_extract(x.value, '$[0]') = d.provider_id
                AND json_extract(x.value, '$[1]') = d.tool_key)
```

For Saved Functions it is `workspace_id`, `kind`, `principal_id` and
`principal_kind` equalities. `vec0` cannot take `NOT EXISTS` or a `json_each`
join as a metadata constraint. Three ways to carry the filter, best first:

1. **`rowid IN (subquery)`.** Give each search row an integer vector rowid in a
   keys table, as `search_entities_fts_keys` already does for FTS5
   (`crates/coral-db/migrations/sqlite/0039_search_text_index_keys.sql`,
   lines 12 to 17) [S38]. Then the KNN query carries the unchanged `Scope::sql`
   string inside
   `rowid IN (SELECT k.vec_rowid FROM search_entities d JOIN search_vectors_keys k ON ... WHERE <conditions>)`.
   SQLite marks every `IN` term for all-at-once delivery except row-value slices
   (`allocateIndexInfo` in the amalgamation: `if( op==WO_IN ){ if( (pTerm->wtFlags & TERM_SLICE)==0 ){ pHidden->mIn |= ...` ) [S8],
   and `vec0BestIndex` claims it when `sqlite3_vtab_in(pIdxInfo, i, -1)` is true
   [S33] line 5489. This is the exact filter, computed once, before any distance.
   Confidence: moderate to high. Open question 1 asks the prototype to confirm the
   plan with `EXPLAIN QUERY PLAN`.
2. **Partition key plus metadata `IN`.** `workspace_id TEXT PARTITION KEY`,
   `provider_generation_id TEXT` metadata with `IN (...)`, and the excluded
   pairs removed after the scan with `k` raised by their count. Exact as well,
   but it stores a second copy of two text keys per row, and text metadata over
   12 characters is "slightly inefficient" per the docs [S34]. The docs do not
   list `IN` for metadata; only the code does.
3. **Partition per Generation.** `(workspace_id, provider_generation_id)` as
   partition keys, one KNN per active Generation, merged in Rust. The docs warn
   against over-sharding and ask for "~100s of vectors" per key value [S34].
   Small Providers would break that rule.

The rows of a superseded Generation stay in `search_entities` until their delete
lands; the lexical condition on `provider_generation_id` hides them. Option 1
inherits that behaviour unchanged.

### Writes, rebuilds and model changes

- Inserts take a vector as a BLOB of little-endian float32, or JSON text; the
  Rust page recommends `zerocopy` to pass `Vec<f32>` without copying [S24].
- `DELETE` frees space only when a whole chunk of about 1024 vectors empties
  (0.1.7) [S15]. `UPDATE` exists [S18]. `INSERT OR REPLACE` arrives in
  0.1.10-alpha.3 [S19], so on 0.1.9 an upsert is `DELETE` plus `INSERT`.
  The FTS5 rule in Coral's migration 0030 ("never `INSERT OR REPLACE`") carries
  over [S39].
- The dimension is part of the DDL (`float[384]`). A model with another
  dimension needs a new table: `DROP TABLE` plus `CREATE VIRTUAL TABLE`, and a
  full re-embed. `ALTER TABLE RENAME` does not exist on 0.1.9 [S20], so a
  side-by-side table cannot be swapped by name; it can be swapped by writing the
  table name into the query.
- A new embedder revision at the same dimension can `DELETE` every row of the
  Workspace and refill in the background. The map's background embedder, keyed by
  row fingerprint and embedder revision, decides which rows to write; a row
  without a vector ranks lexically (#2856).
- Coral's migration plan "must fail on unexpected objects" [S29]. The `vec0`
  table and its seven shadow tables must be created by a migration step, like
  the FTS5 table in 0030, not at runtime by the embedder. A dimension change is
  then a migration.
- The read pool sets `PRAGMA query_only = ON` [S27]. The flat KNN path only reads
  shadow tables and blobs [S7]. Open question 2 asks the prototype to run a KNN
  on that pool.

## 5. Expected brute-force latency, 20,000 rows, 384 and 768 dimensions

The project publishes brute-force numbers from a Mac M1 mini with 8 GB RAM,
queries run one after another, average reported, datasets in memory [S14]:

| Dataset | Rows | Dims | `vec0` query time |
| --- | --- | --- | --- |
| sift1m, k=20, chunk 2048 | 1,000,000 | 128 | 33 ms |
| internal float test | 100,000 | 384, 768, 1024 | "all under 75 ms" |
| internal float test | 100,000 | 1536 | 105 ms |
| internal float test | 100,000 | 3072 | 214 ms |
| internal bit test | 100,000 | 3072 | 11 ms |

The author's own caveats: "Benchmarks rarely ever reflect real-world
performance" and the results "may be skewed toward `sqlite-vec`'s strengths"
[S14].

Cost per row and dimension from those rows: sift1m gives 0.26 ns; the 1536 and
3072 rows give 0.68 and 0.70 ns. Scaling to 20,000 rows:

| Dims | Row times dims | At 0.26 ns | At 0.70 ns | Bound from "under 75 ms at 100k" |
| --- | --- | --- | --- | --- |
| 384 | 7.7 M | 2 ms | 5.4 ms | under 15 ms |
| 768 | 15.4 M | 4 ms | 10.8 ms | under 15 ms |

Third-party measurements agree in order of magnitude (low confidence, different
hardware and settings): 10,000 rows at 768 dims on an M1 Pro, p50 2.1 ms,
p99 4.3 ms [S40]; 10,000 rows at 384 dims float32, k=10, 3.55 ms on
sqlite-vec 0.1.6 [S41]; 100,000 rows at 384 dims on an M1 Pro, 67.8 ms [S42].

Expectation for Coral: single-digit milliseconds per KNN over 20,000 rows at
either dimension on an M-series core, under 15 ms as a bound, before the
eligibility subquery and the join back to `search_entities`. The lexical query
already pays for that subquery. The map's guide of about 100 ms p50 for the
semantic side leaves the KNN scan a small share; the query embedding itself will
dominate. Confidence: moderate. Two things lower it:

- The crate's `build.rs` passes no SIMD define. The project's Makefile enables
  `SQLITE_VEC_ENABLE_AVX` with `-mavx -mavx2` and `SQLITE_VEC_ENABLE_NEON` with
  `-mcpu=apple-m1` only through `CFLAGS` [S43]. The blog does not say which
  flags its builds used. A Rust build runs the scalar loops, plus whatever the C
  compiler vectorises on its own.
- Nothing above was measured on Coral's build or data. The pick ticket measures.

Storage for 20,000 rows: 30.7 MB at 384 float32, 61.4 MB at 768 float32; int8
is a quarter of that, `bit` is a thirty-second [S14].

## 6. Binary size and build requirements

Measured by listing the 0.1.9 release archives, nothing extracted [S9]:

| Artifact | Size |
| --- | --- |
| `libsqlite_vec0.a`, macOS arm64 | 159,464 bytes |
| `libsqlite_vec0.a`, Linux x86_64 | 224,276 bytes |
| `vec0.dylib`, macOS arm64 | 161,896 bytes |
| `vec0.so`, Linux x86_64 | 159,816 bytes |
| `vec0.dll`, Windows x86_64 | 289,280 bytes |

Expect 150 to 300 KB added to the `coral` binary per platform. Confidence:
moderate. The static archives above were built with `-O3` by the project's
Makefile [S43]; a Cargo build uses the profile's own optimisation level.

Build requirements, all three platforms:

- A C compiler reachable by the `cc` crate. `libsqlite3-sys` with `bundled`
  already compiles `sqlite3.c` through `cc` [S2], so Coral's toolchains on Ubuntu
  24.04, macOS 26 and Windows 2025 runners meet this today [S44]. The release
  workflow packages macOS (with the `aarch64-apple-darwin` target), Windows and
  Linux through `xtask release-desktop-package` [S44].
- One more C translation unit of 10,199 lines, plus the crate's vendored
  `sqlite3.h` 3.45.3 for prototypes only [S13] [S33]. Link time resolves the
  SQLite symbols against the bundled 3.51.3. The `sqlite3_module` struct in both
  headers ends with the same `xIntegrity` slot (SQLite 3.44+), and `vec0Module`
  declares `iVersion` 3 [S33] lines 9366 to 9391. SQLite only appends to these
  structs, so the skew is safe. Confidence: high for linking, moderate for the
  rest; open question 3.
- Windows: the source handles `_MSC_VER`, maps `__builtin_popcountl` to
  `__popcnt64` through `<intrin.h>` on x86_64, and carries a loop fallback for
  MSVC on ARM [S33] line 522. Upstream CI compiles and tests the loadable build
  with `cl.exe` on `windows-2022` [S17]. `SQLITE_VEC_API` expands to
  `__declspec(dllexport)` on `_WIN32` unless `SQLITE_VEC_STATIC` is set, and the
  crate's `build.rs` does not set it [S13] [S3]. An exported symbol in a static
  library linked into an executable is harmless. No one upstream builds the Rust
  crate on any platform [S17]; open question 4.
- No Rust feature flags, no `pkg-config`, no system library.

## 7. Open questions

1. **Does the planner deliver `rowid IN (SELECT ...)` to `vec0` all at once?**
   The amalgamation marks every non-slice `IN` term as eligible [S8] and `vec0`
   claims it [S33]. Not observed. The prototype should run
   `EXPLAIN QUERY PLAN` on the option 1 query and compare its result to a plain
   `ORDER BY vec_distance_cosine(...)` scan over the same rows.
2. **KNN on the `query_only` read pool.** The flat path reads only [S7]. Not
   observed on a `query_only` connection.
3. **Header skew.** The crate compiles against SQLite 3.45.3 headers and links
   3.51.3. Safe by SQLite's append-only policy; not observed.
4. **Rust crate on Windows MSVC and on `aarch64-apple-darwin`.** Upstream has no
   Rust CI. The desktop and release workflows will show it on the first build.
5. **Which crate owns the dependency.** coral-db owns the pools and the `Once`;
   coral-search owns the probe and the schema. One of them adds `sqlite-vec`
   and `libsqlite3-sys` (or `rusqlite`) to its `Cargo.toml`. An ADR decision.
6. **Version pin and upgrade policy.** Pin 0.1.9 exactly. The 0.1.10 alphas add
   on-disk structures for approximate indexes and change `DELETE` behaviour on
   those indexes [S16]; the flat path is untouched, but a pre-v1 crate warrants
   an exact pin and a changelog read per bump.
7. **`sqlx` on its own worker thread.** sqlx opens each connection on a
   dedicated thread. The auto-extension list is global and mutex-guarded [S5],
   so the thread does not matter. Not observed.

## Sources

Coral, commit `c82b1700e`:

- [S1] `Cargo.toml` lines 75 and 95; `Cargo.lock` entries `libsqlite3-sys 0.37.0`,
  `rusqlite 0.39.0`, `sqlx 0.9.0`, `sqlx-sqlite 0.9.0`;
  `cargo tree --offline -e features -i libsqlite3-sys` run in the worktree.
- [S22] `crates/coral-app/src/state/database_format.rs` lines 28 to 75 and 186.
- [S27] `crates/coral-db/src/database.rs` lines 172 to 234.
- [S28] `crates/coral-app/src/state/initialization.rs` lines 66 to 101.
- [S29] `crates/coral-db/src/lib.rs` lines 1 to 48.
- [S30] `crates/coral-test-support/src/lib.rs` lines 120 to 262.
- [S31] `crates/coral-search/src/storage.rs` lines 1 to 155;
  `crates/coral-search/src/storage/repository.rs` lines 56 to 85.
- [S37] `crates/coral-search/src/retriever/repository.rs` lines 50 to 130
  (`Scope::sql`) and 479 to 507 (the SQLite FTS5 statement).
- [S38] `crates/coral-db/migrations/sqlite/0039_search_text_index_keys.sql`
  lines 12 to 17; `0045_tool_schema_names.sql` lines 452 to 472
  (`search_entities` columns and primary key).
- [S39] `crates/coral-db/migrations/sqlite/0030_search_text_search.sql`.
- [S32] `apps/desktop/scripts/runtime.mjs` lines 8 to 32.
- [S44] `.github/workflows/ci.yml`, `desktop.yml`, `release.yml` (runner labels
  `blacksmith-*-ubuntu-2404`, `blacksmith-*-macos-26`,
  `blacksmith-*-windows-2025`; `rustup target add aarch64-apple-darwin`;
  `xtask release-desktop-package --platform mac|win|linux`).

`libsqlite3-sys` 0.37.0, from the local Cargo registry:

- [S2] `build.rs` lines 120 to 143 (compile flags); `sqlite3/sqlite3.h`
  `#define SQLITE_VERSION "3.51.3"`.
- [S4] `sqlite3/sqlite3.h` lines 7491 to 7523, "Automatically Load Statically
  Linked Extensions".
- [S5] `sqlite3/sqlite3.c`: `sqlite3_auto_extension` at line 141788 (after the
  `SQLITE_OMIT_LOAD_EXTENSION` block ends at 141753), takes
  `SQLITE_MUTEX_STATIC_MAIN`; `sqlite3AutoLoadExtensions(db)` called from
  `openDatabase` at line 188514.
- [S8] `sqlite3/sqlite3.c` lines 168241 to 168250 (`allocateIndexInfo`, `mIn`);
  `sqlite3/sqlite3.h` lines 10330 to 10425 (`sqlite3_vtab_in`,
  `sqlite3_vtab_in_first`).

sqlite-vec, upstream:

- [S3] crate 0.1.9 `src/lib.rs` and `build.rs`,
  https://docs.rs/crate/sqlite-vec/0.1.9/source/
- [S7] `sqlite-vec.c` on `main`, functions `vec0Filter_knn` and
  `vec0Filter_knn_chunks_iter`,
  https://raw.githubusercontent.com/asg017/sqlite-vec/main/sqlite-vec.c
- [S9] release archives for v0.1.9, listed with `tar tzvf`,
  https://github.com/asg017/sqlite-vec/releases/tag/v0.1.9
- [S10] https://crates.io/api/v1/crates/sqlite-vec (versions, dates, license, size)
- [S11] https://raw.githubusercontent.com/asg017/sqlite-vec/main/README.md
- [S12] crate 0.1.9 `Cargo.toml`,
  https://docs.rs/crate/sqlite-vec/0.1.9/source/Cargo.toml
- [S13] crate 0.1.9 `sqlite-vec.h` and `sqlite3.h`, read from
  https://static.crates.io/crates/sqlite-vec/sqlite-vec-0.1.9.crate
- [S14] "Introducing sqlite-vec v0.1.0", 2024-08-01,
  https://alexgarcia.xyz/blog/2024/sqlite-vec-stable-release/index.html
- [S15] release notes v0.1.7,
  https://github.com/asg017/sqlite-vec/releases/tag/v0.1.7
- [S16] release notes v0.1.10-alpha.1 and alpha.4,
  https://github.com/asg017/sqlite-vec/releases
- [S17] `.github/workflows/test.yaml` on `main`,
  https://raw.githubusercontent.com/asg017/sqlite-vec/main/.github/workflows/test.yaml
- [S18] release notes v0.1.2 and v0.1.6,
  https://github.com/asg017/sqlite-vec/releases/tag/v0.1.2 and `/v0.1.6`
- [S19] release notes v0.1.10-alpha.3 (crates.io list and GitHub releases page)
- [S20] release notes v0.1.10-alpha.2,
  https://github.com/asg017/sqlite-vec/releases/tag/v0.1.10-alpha.2
- [S23] `sqlite-vec.c` on `main`, `sqlite3_vec_init` (module and function
  registration, `#ifndef SQLITE_CORE` around `SQLITE_EXTENSION_INIT2`).
- [S24] https://alexgarcia.xyz/sqlite-vec/rust.html
- [S33] `sqlite-vec.c` inside crate 0.1.9 (10,199 lines); line numbers in the
  text: 90 to 92 (`COMPILER_SUPPORTS_VTAB_IN`), 522 (`_MSC_VER`), 2371 to 2377
  (element types), 2442 to 2446 (metrics), 3445 to 3447 (text primary key),
  3484 to 3488 (column and dimension limits), 4921 to 4943 (`chunk_size`),
  5484 to 5712 (`vec0BestIndex`: rowid `IN`, partition and metadata operators),
  7111 (`SQLITE_VEC_VEC0_K_MAX`), 9366 to 9391 (`vec0Module`).
- [S34] https://alexgarcia.xyz/sqlite-vec/features/vec0.html (metadata,
  partition key and auxiliary column sections).
- [S35] https://raw.githubusercontent.com/asg017/sqlite-vec/main/ARCHITECTURE.md
  and the shadow-table name formats in `sqlite-vec.c`.
- [S36] https://alexgarcia.xyz/sqlite-vec/features/knn.html
- [S43] `Makefile` on `main`,
  https://raw.githubusercontent.com/asg017/sqlite-vec/main/Makefile

rusqlite and sqlx:

- [S6] sqlx 0.9.0 `sqlx-sqlite/src/connection/establish.rs`,
  https://raw.githubusercontent.com/launchbadge/sqlx/v0.9.0/sqlx-sqlite/src/connection/establish.rs
- [S21] sqlx 0.9.0 `sqlx-sqlite/Cargo.toml`,
  https://raw.githubusercontent.com/launchbadge/sqlx/v0.9.0/sqlx-sqlite/Cargo.toml
- [S25] https://docs.rs/rusqlite/0.39.0/rusqlite/auto_extension/index.html and
  `fn.register_auto_extension.html`, `type.RawAutoExtension.html`
- [S26] https://docs.rs/sqlx/0.9.0/sqlx/sqlite/struct.SqliteConnectOptions.html

Third-party latency reports (low confidence, not primary):

- [S40] https://dev.to/hypernexus/sqlite-vector-search-the-dependency-free-ai-memory-stack-that-outperforms-pinecone-5d27
- [S41] https://coderdayton.github.io/SimpleVecDB/benchmarks
- [S42] https://marcobambini.substack.com/p/the-state-of-vector-search-in-sqlite

Unreachable after two tries: `sqlite-vec.h` on `main` (it is generated from
`sqlite-vec.h.tmpl` at release time; the crate copy was used instead) and
`benchmarks/README.md` (HTTP 501 from the raw host; the blog post carries the
project's numbers).
