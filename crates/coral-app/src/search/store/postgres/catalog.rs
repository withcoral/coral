//! Catalog projection and retrieval on the Postgres store.
//!
//! Retrieval is BM25 in plain SQL, the `SQLite` side's `bm25()` rebuilt on
//! vanilla Postgres (`ts_rank_cd` has no corpus statistics and degenerates
//! to a term-frequency count under coral's disjunctive queries):
//! corpus IDF from the per-Workspace `catalog_terms` statistics, FTS5's `k1`
//! and negative-IDF clamp, a `b` tuned for short structured documents, and
//! the same 8/6/2/1 field weights the `SQLite` side passes to `bm25()`.
//! Every query word scores with its true IDF; only *candidate selection* —
//! a prefix `tsquery` OR-ed with trigram `ILIKE` patterns (exact substring
//! semantics; the executor's recheck makes false positives impossible) — is
//! restricted to words below the document-frequency cap, which is a latency
//! lever. The whole-query phrase boost leads the order so a whole-phrase
//! match outranks a co-occurrence match; `doc_id` closes the deterministic
//! total order.

use std::collections::BTreeSet;

use sqlx::{Postgres, Row as _, Transaction};

use super::{PostgresSearchError, PostgresSearchStore};
use crate::search::catalog::index::{
    CatalogClearResult, CatalogDocumentClass, CatalogIndexDocument, CatalogIndexSnapshot,
    CatalogRebuildResult, CatalogRefreshResult, CatalogSearchHit, CatalogSearchHits,
    NormalizedSearchTerm, indexed_searchable_text, is_known_field_role, is_known_surface_kind,
    normalized_search_term_variants, probe_limit, truncate_probe_hits,
};
use crate::search::store::{CatalogStore, SearchStoreError};

const CATALOG_SNAPSHOT_FINGERPRINT_META_KEY: &str = "catalog_snapshot_fingerprint";
/// Rows per `UNNEST` insert. Bounds the parameter arrays a 21k-document
/// snapshot sends without turning the rebuild into thousands of round trips.
const INSERT_CHUNK_ROWS: usize = 2_000;
/// Terms shorter than this cannot match a trigram index; the `SQLite` side
/// applies the same floor.
const MIN_TERM_CHARS: usize = 3;
/// Field weights, identical to what the `SQLite` side passes to `bm25()`:
/// qualified name, title, description, searchable text. They weight both the
/// whole-query phrase boost and each lexeme occurrence in the BM25 score.
const FIELD_WEIGHT_QUALIFIED_NAME: u8 = 8;
const FIELD_WEIGHT_TITLE: u8 = 6;
const FIELD_WEIGHT_DESCRIPTION: u8 = 2;
const FIELD_WEIGHT_SEARCHABLE_TEXT: u8 = 1;
/// FTS5's term-frequency saturation constant (`fts5_aux.c`).
const BM25_K1: f64 = 1.2;
/// Length normalization. FTS5 hard-codes 0.75 for prose; catalog documents
/// are short and structured, and 0.75 makes a column-rich canonical table pay
/// in its name field for its column count. 0.4 measured best on the replay
/// of recorded agent queries against the used-table labels.
const BM25_B: f64 = 0.4;
/// FTS5 clamps a non-positive IDF to this instead of letting a term that
/// appears in more than half the corpus push results around.
const IDF_FLOOR: f64 = 1e-6;
/// Words in more of the corpus than this share are dropped from *candidate
/// selection* only — a latency lever, not a relevance rule (measured:
/// uncapped candidates cost 5× for the same relevance). Every word still
/// scores with its true IDF: at 10% frequency a word carries real signal
/// (IDF ≈ 2.2) that FTS5's clamp — which only bites above half the corpus —
/// would keep.
const DOCUMENT_FREQUENCY_CAP: f64 = 0.05;
/// When every word is above the cap, keep this many rarest words as
/// candidate keys instead of searching for nothing.
const RARE_WORD_KEEP: usize = 3;

// Recorded alternatives to the document-frequency cap (2026-09-10; neither
// is implemented, both were sized against the replay harness):
//
// A. A materialized postings table, `catalog_postings(term, doc_kind, doc_id,
//    tf)`, written by `refresh_ranking_stats` from `unnest(tsv)` with the
//    8/6/2/1 weights folded into `tf` (~500k rows at 21k documents). Scoring
//    becomes a join of the query words against it (`term LIKE word || '%'`
//    ranges over a `text_pattern_ops` btree) grouped by `doc_id`: cost is the
//    sum of the words' posting lengths, as in an inverted index, instead of
//    candidates × lexemes × words as here. That removes the per-candidate
//    lateral and, with it, the query-length latency scaling, and the cap is
//    no longer needed on the tsquery path. GIN itself cannot do this: it
//    holds lexeme → TID only; positions and weights live in the heap row.
//    The substring path (`ILIKE`) has no posting and stays as a fallback for
//    typed words that prefix-match no lexeme at all.
//
// B. Keep this shape and choose candidate keys by score bound instead of by
//    frequency (MaxScore): store `max_tf` per term, bound a word by
//    `idf × sat(max_tf)` at the shortest document, key the highest bounds,
//    read the k-th score θ from the first pass, and run a second pass with
//    more keys only if the excluded words' bounds sum to θ or more. Exact for
//    the top k with no threshold constant. Costs more than the cap unless k
//    is the request limit rather than the lane window (the 51st score is
//    usually weak, so `issues`-class words would become keys), and the
//    prefix join needs the bound summed over the prefix range (`github` alone
//    prefixes ~20k lexemes through compact variants). Does not change the
//    per-candidate cost, so it is not a latency fix on its own.
//
// Independent of both: the `ILIKE` disjunct is the expensive half of
// candidate selection (measured 264 ms vs 31 ms for tsquery alone) and only
// earns its place for words that prefix-match no lexeme; restricting it to
// those is one `EXISTS` per word in the statistics lookup.

impl CatalogStore for PostgresSearchStore {
    fn projection_is_current(&self, fingerprint: &str) -> Result<bool, SearchStoreError> {
        Ok(self.block_on(async {
            let mut tx = self.begin().await?;
            let current = current_fingerprint(&mut tx).await?;
            tx.commit().await?;
            Ok::<_, PostgresSearchError>(current.as_deref() == Some(fingerprint))
        })?)
    }

    fn refresh_projection(
        &self,
        snapshot: &CatalogIndexSnapshot,
    ) -> Result<CatalogRefreshResult, SearchStoreError> {
        let document_count = u32::try_from(snapshot.documents.len()).unwrap_or(u32::MAX);
        Ok(self.block_on(async {
            let mut tx = self.begin_write().await?;
            if current_fingerprint(&mut tx).await?.as_deref() == Some(snapshot.fingerprint.as_str())
            {
                tx.commit().await?;
                return Ok::<_, PostgresSearchError>(CatalogRefreshResult {
                    refreshed: false,
                    document_count,
                });
            }
            replace_documents(&mut tx, snapshot).await?;
            tx.commit().await?;
            Ok(CatalogRefreshResult {
                refreshed: true,
                document_count,
            })
        })?)
    }

    fn rebuild_projection(
        &self,
        snapshot: &CatalogIndexSnapshot,
        force: bool,
    ) -> Result<CatalogRebuildResult, SearchStoreError> {
        Ok(self.block_on(async {
            let mut tx = self.begin_write().await?;
            let current = current_fingerprint(&mut tx).await?;
            let old_document_count = document_count(&mut tx).await?;
            let projection_changed = current.as_deref() != Some(snapshot.fingerprint.as_str());
            let rebuild_performed = force || projection_changed;
            if rebuild_performed {
                replace_documents(&mut tx, snapshot).await?;
            }
            let new_document_count = if rebuild_performed {
                document_count(&mut tx).await?
            } else {
                old_document_count
            };
            tx.commit().await?;
            Ok::<_, PostgresSearchError>(CatalogRebuildResult {
                old_document_count,
                new_document_count,
                projection_changed,
                rebuild_performed,
            })
        })?)
    }

    fn document_count(&self) -> Result<u32, SearchStoreError> {
        Ok(self.block_on(async {
            let mut tx = self.begin().await?;
            let count = document_count(&mut tx).await?;
            tx.commit().await?;
            Ok::<_, PostgresSearchError>(count)
        })?)
    }

    fn search(
        &self,
        terms: &[String],
        limit: usize,
        class: CatalogDocumentClass,
    ) -> Result<CatalogSearchHits, SearchStoreError> {
        let terms = normalized_search_term_variants(terms);
        let Some(plan) = (if limit == 0 {
            None
        } else {
            CatalogQueryPlan::build(&terms)
        }) else {
            return Ok(CatalogSearchHits {
                hits: Vec::new(),
                retrieval_limited: false,
            });
        };
        let mut hits = self.block_on(async {
            let mut tx = self.begin().await?;
            let hits = plan.fetch(&mut tx, class, probe_limit(limit)).await?;
            tx.commit().await?;
            Ok::<_, PostgresSearchError>(hits)
        })?;
        let retrieval_limited = truncate_probe_hits(&mut hits, limit);
        Ok(CatalogSearchHits {
            hits,
            retrieval_limited,
        })
    }

    fn clear_source(&self, source_name: &str) -> Result<CatalogClearResult, SearchStoreError> {
        Ok(self.block_on(async {
            let mut tx = self.begin_write().await?;
            let deleted = sqlx::query("DELETE FROM catalog_documents WHERE source_name = $1")
                .bind(source_name)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            // The ranking statistics stay: rows remain, and the cleared
            // fingerprint makes the next search rebuild the projection, which
            // rewrites them. Stale-but-present beats absent for a search that
            // races the rebuild.
            clear_fingerprint(&mut tx).await?;
            tx.commit().await?;
            Ok::<_, PostgresSearchError>(CatalogClearResult {
                deleted_document_count: u32::try_from(deleted).unwrap_or(u32::MAX),
            })
        })?)
    }

    fn clear_workspace(&self) -> Result<CatalogClearResult, SearchStoreError> {
        Ok(self.block_on(async {
            let mut tx = self.begin_write().await?;
            let deleted = sqlx::query("DELETE FROM catalog_documents")
                .execute(&mut *tx)
                .await?
                .rows_affected();
            sqlx::query("DELETE FROM catalog_terms")
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM catalog_stats")
                .execute(&mut *tx)
                .await?;
            clear_fingerprint(&mut tx).await?;
            tx.commit().await?;
            Ok::<_, PostgresSearchError>(CatalogClearResult {
                deleted_document_count: u32::try_from(deleted).unwrap_or(u32::MAX),
            })
        })?)
    }
}

/// The stored fingerprint. Replacement is one transaction, so the rows always
/// match it; there is no per-row copy to reconcile.
async fn current_fingerprint(
    tx: &mut Transaction<'static, Postgres>,
) -> Result<Option<String>, PostgresSearchError> {
    Ok(
        sqlx::query_scalar("SELECT value FROM search_meta WHERE key = $1")
            .bind(CATALOG_SNAPSHOT_FINGERPRINT_META_KEY)
            .fetch_optional(&mut **tx)
            .await?,
    )
}

async fn document_count(
    tx: &mut Transaction<'static, Postgres>,
) -> Result<u32, PostgresSearchError> {
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog_documents")
        .fetch_one(&mut **tx)
        .await?;
    Ok(u32::try_from(count).unwrap_or(u32::MAX))
}

async fn replace_documents(
    tx: &mut Transaction<'static, Postgres>,
    snapshot: &CatalogIndexSnapshot,
) -> Result<(), PostgresSearchError> {
    sqlx::query("DELETE FROM catalog_documents")
        .execute(&mut **tx)
        .await?;
    for chunk in snapshot.documents.chunks(INSERT_CHUNK_ROWS) {
        insert_documents(tx, chunk).await?;
    }
    refresh_ranking_stats(tx).await?;
    sqlx::query(
        "INSERT INTO search_meta (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
    )
    .bind(CATALOG_SNAPSHOT_FINGERPRINT_META_KEY)
    .bind(&snapshot.fingerprint)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_documents(
    tx: &mut Transaction<'static, Postgres>,
    documents: &[CatalogIndexDocument],
) -> Result<(), PostgresSearchError> {
    let column = |select: fn(&CatalogIndexDocument) -> &str| {
        documents.iter().map(select).collect::<Vec<_>>()
    };
    sqlx::query(
        "INSERT INTO catalog_documents (
            doc_id, doc_kind, source_name, catalog_name, surface_kind, surface_name,
            field_name, field_role, qualified_name, title, description, searchable_text
        )
        SELECT * FROM UNNEST(
            $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
            $7::text[], $8::text[], $9::text[], $10::text[], $11::text[], $12::text[]
        )",
    )
    .bind(column(|document| document.doc_id.as_str()))
    .bind(
        documents
            .iter()
            .map(|document| document.doc_kind.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(column(|document| document.source_name.as_str()))
    .bind(
        documents
            .iter()
            .map(|document| document.catalog_name.as_deref())
            .collect::<Vec<_>>(),
    )
    .bind(column(|document| document.surface_kind.as_str()))
    .bind(column(|document| document.surface_name.as_str()))
    .bind(column(|document| document.field_name.as_str()))
    .bind(column(|document| document.field_role.as_str()))
    .bind(column(|document| document.qualified_name.as_str()))
    .bind(column(|document| document.title.as_str()))
    .bind(column(|document| document.description.as_str()))
    .bind(
        documents
            .iter()
            .map(indexed_searchable_text)
            .collect::<Vec<_>>(),
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Rewrites the BM25 corpus statistics from the rows just inserted, inside the
/// same transaction, so statistics and projection can never disagree.
async fn refresh_ranking_stats(
    tx: &mut Transaction<'static, Postgres>,
) -> Result<(), PostgresSearchError> {
    sqlx::query(
        "UPDATE catalog_documents AS d SET doc_len = coalesce((
             SELECT sum(coalesce(array_length(u.positions, 1), 1))
             FROM unnest(d.tsv) AS u(lexeme, positions, weights)
         ), 0)",
    )
    .execute(&mut **tx)
    .await?;
    sqlx::query("DELETE FROM catalog_terms")
        .execute(&mut **tx)
        .await?;
    sqlx::query(
        "INSERT INTO catalog_terms (term, ndoc)
         SELECT word, ndoc FROM ts_stat('SELECT tsv FROM catalog_documents')",
    )
    .execute(&mut **tx)
    .await?;
    sqlx::query("DELETE FROM catalog_stats")
        .execute(&mut **tx)
        .await?;
    sqlx::query(
        "INSERT INTO catalog_stats (total_docs, avgdl)
         SELECT count(*), coalesce(avg(doc_len), 1.0) FROM catalog_documents",
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn clear_fingerprint(
    tx: &mut Transaction<'static, Postgres>,
) -> Result<(), PostgresSearchError> {
    sqlx::query("DELETE FROM search_meta WHERE key = $1")
        .bind(CATALOG_SNAPSHOT_FINGERPRINT_META_KEY)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// One retrieval, fully parameterized: every value travels as a bind, and the
/// only interpolated fragments are fixed SQL (`doc_kind_predicate`, the BM25
/// constants) and placeholder numbers.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogQueryPlan {
    /// The whole normalized query as a `%phrase%` pattern for the boost. It is
    /// the longest term: query construction appends the whole query as a term,
    /// and it contains every other token.
    phrase_pattern: String,
    /// Lexeme words of every term (runs of alphanumerics and `_`), the floor
    /// applied, sorted and deduplicated. They key the statistics lookup and,
    /// after the document-frequency cap, drive candidates and scoring.
    words: Vec<String>,
    /// The subset of `words` that only a compact identifier variant produced
    /// (`issuelabels` from `issue labels`), never a term the caller typed.
    /// Documents hold such a variant only as a whole lexeme, so one the
    /// statistics show absent is dropped before it can key candidates,
    /// score, or count as a survivor of the document-frequency cap.
    variant_words: Vec<String>,
    /// `%term%` per term that yields no word above the floor
    /// (punctuation-heavy identifiers), so its substring semantics survive
    /// the word derivation.
    wordless_patterns: Vec<String>,
}

/// One query word's document frequency, read from `catalog_terms`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct WordStat {
    word: String,
    ndoc: i64,
}

/// What the statistics lookup resolved the plan's words into.
#[derive(Debug, Clone, PartialEq)]
struct ScoringPlan {
    /// Every query word with its IDF — all of them score.
    scored: Vec<ScoredWord>,
    /// Words under the document-frequency cap (or the rarest few when nothing
    /// is): the candidate-selection keys.
    candidate_words: Vec<String>,
    avgdl: f64,
}

#[derive(Debug, Clone, PartialEq)]
struct ScoredWord {
    word: String,
    idf: f64,
}

impl CatalogQueryPlan {
    fn build(terms: &[NormalizedSearchTerm]) -> Option<Self> {
        let terms = terms
            .iter()
            .filter(|term| term.text.chars().count() >= MIN_TERM_CHARS)
            .collect::<Vec<_>>();
        let phrase = terms
            .iter()
            .max_by_key(|term| term.text.chars().count())?
            .text
            .clone();
        let mut given_words = BTreeSet::new();
        let mut variant_words = BTreeSet::new();
        let mut wordless = BTreeSet::new();
        for term in &terms {
            let term_words = lexeme_words(&term.text)
                .into_iter()
                .filter(|word| word.chars().count() >= MIN_TERM_CHARS)
                .collect::<Vec<_>>();
            if term_words.is_empty() {
                wordless.insert(contains_pattern(&term.text));
            } else if term.compact_variant {
                variant_words.extend(term_words);
            } else {
                given_words.extend(term_words);
            }
        }
        let words = given_words.union(&variant_words).cloned().collect();
        let variant_words = variant_words.difference(&given_words).cloned().collect();
        Some(Self {
            phrase_pattern: contains_pattern(&phrase),
            words,
            variant_words,
            wordless_patterns: wordless.into_iter().collect(),
        })
    }

    async fn fetch(
        &self,
        tx: &mut Transaction<'static, Postgres>,
        class: CatalogDocumentClass,
        limit: usize,
    ) -> Result<Vec<CatalogSearchHit>, PostgresSearchError> {
        let scoring = if self.words.is_empty() {
            ScoringPlan {
                scored: Vec::new(),
                candidate_words: Vec::new(),
                avgdl: 1.0,
            }
        } else {
            let (stats, total_docs, avgdl) = fetch_word_stats(tx, &self.words).await?;
            scoring_plan(&stats, total_docs, avgdl, &self.variant_words)
        };
        if scoring.scored.is_empty() && self.wordless_patterns.is_empty() {
            // Every word was a compact variant the corpus lacks: nothing is
            // left to select candidates with.
            return Ok(Vec::new());
        }
        // Audited: the only dynamic fragments are `doc_kind_predicate`, fixed
        // literals, and placeholder numbers; every value is a bind.
        let mut query = sqlx::query(sqlx::AssertSqlSafe(self.sql(&scoring, class)));
        if !scoring.scored.is_empty() {
            query = query.bind(tsquery(&scoring.candidate_words));
            for word in &scoring.scored {
                query = query.bind(prefix_pattern(&word.word));
                query = query.bind(word.idf);
            }
            for word in &scoring.candidate_words {
                query = query.bind(contains_pattern(word));
            }
        }
        for pattern in &self.wordless_patterns {
            query = query.bind(pattern);
        }
        if !scoring.scored.is_empty() {
            query = query.bind(scoring.avgdl);
        }
        let rows = query
            .bind(&self.phrase_pattern)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .fetch_all(&mut **tx)
            .await?;
        rows.iter().map(hit_from_row).collect()
    }

    fn sql(&self, scoring: &ScoringPlan, class: CatalogDocumentClass) -> String {
        let scored_count = scoring.scored.len();
        let candidate_word_count = scoring.candidate_words.len();
        let mut next = 1_usize;
        let mut take = |count: usize| {
            let start = next;
            next += count;
            start
        };
        let tsquery = (scored_count > 0).then(|| take(1));
        let values_start = take(scored_count * 2);
        let word_pattern_start = take(candidate_word_count);
        let wordless_start = take(self.wordless_patterns.len());
        let avgdl = (scored_count > 0).then(|| take(1));
        let phrase = take(1);
        let limit = take(1);

        let mut candidates = Vec::new();
        if let Some(tsquery) = tsquery {
            candidates.push(format!("d.tsv @@ to_tsquery('simple', ${tsquery})"));
        }
        for index in 0..candidate_word_count {
            candidates.push(format!("d.all_text ILIKE ${}", word_pattern_start + index));
        }
        for index in 0..self.wordless_patterns.len() {
            candidates.push(format!("d.all_text ILIKE ${}", wordless_start + index));
        }
        let candidates = candidates.join(" OR ");

        let score = match avgdl {
            Some(avgdl) => {
                let values = (0..scored_count)
                    .map(|index| {
                        let pattern = values_start + 2 * index;
                        let idf = pattern + 1;
                        format!("(${pattern}::text, ${idf}::float8)")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "CROSS JOIN LATERAL (
                         SELECT coalesce(sum(
                             q.idf * (tf.f * ({BM25_K1} + 1))
                             / (tf.f + {BM25_K1} * (1 - {BM25_B} + {BM25_B} * coalesce(c.doc_len::float8, ${avgdl}) / ${avgdl}))
                         ), 0)::float8 AS score
                         FROM (VALUES {values}) AS q(pattern, idf)
                         JOIN unnest(c.tsv) AS u(lexeme, positions, weights)
                           ON u.lexeme LIKE q.pattern
                         CROSS JOIN LATERAL (
                             SELECT sum(CASE w
                                 WHEN 'A' THEN {FIELD_WEIGHT_QUALIFIED_NAME}::float8
                                 WHEN 'B' THEN {FIELD_WEIGHT_TITLE}
                                 WHEN 'C' THEN {FIELD_WEIGHT_DESCRIPTION}
                                 ELSE {FIELD_WEIGHT_SEARCHABLE_TEXT} END) AS f
                             FROM unnest(u.weights) AS w
                         ) AS tf
                     ) AS s"
                )
            }
            None => "CROSS JOIN LATERAL (SELECT 0::float8 AS score) AS s".to_string(),
        };
        // `MATERIALIZED` walls candidate selection off from the scoring join
        // and the sort: planned together, the planner abandons the trigram/tsv
        // bitmap scans for a sequential scan (measured 2x the median). The
        // score joins through a LATERAL with `coalesce` so a candidate no
        // scored word reaches — a mid-word substring match — keeps rank
        // instead of vanishing, which an inner-join GROUP BY shape gets wrong.
        format!(
            "WITH candidates AS MATERIALIZED (
                 SELECT d.doc_id, d.source_name, d.surface_kind, d.surface_name, d.field_name,
                        d.field_role, d.catalog_name, d.qualified_name, d.title, d.description,
                        d.searchable_text, d.tsv, d.doc_len
                 FROM catalog_documents AS d
                 WHERE {predicate} AND ({candidates})
             )
             SELECT c.doc_id, c.source_name, c.surface_kind, c.surface_name, c.field_name,
                    c.field_role, c.catalog_name
             FROM candidates AS c
             {score}
             ORDER BY
                 (c.qualified_name ILIKE ${phrase})::int * {FIELD_WEIGHT_QUALIFIED_NAME}
                 + (c.title ILIKE ${phrase})::int * {FIELD_WEIGHT_TITLE}
                 + (c.description ILIKE ${phrase})::int * {FIELD_WEIGHT_DESCRIPTION}
                 + (c.searchable_text ILIKE ${phrase})::int * {FIELD_WEIGHT_SEARCHABLE_TEXT} DESC,
                 s.score DESC,
                 c.doc_id ASC
             LIMIT ${limit}",
            predicate = doc_kind_predicate(class),
        )
    }
}

/// Document frequency per word plus the corpus totals, in one statement. A
/// schema whose statistics were never built reads as an empty corpus and
/// degrades to unweighted scoring instead of failing.
async fn fetch_word_stats(
    tx: &mut Transaction<'static, Postgres>,
    words: &[String],
) -> Result<(Vec<WordStat>, i64, f64), PostgresSearchError> {
    let rows = sqlx::query(
        "SELECT w.term, coalesce(t.ndoc, 0) AS ndoc,
                coalesce(s.total_docs, 0) AS total_docs,
                coalesce(s.avgdl, 0) AS avgdl
         FROM unnest($1::text[]) AS w(term)
         LEFT JOIN catalog_terms AS t ON t.term = w.term
         LEFT JOIN catalog_stats AS s ON true",
    )
    .bind(words)
    .fetch_all(&mut **tx)
    .await?;
    let mut stats = Vec::with_capacity(rows.len());
    let mut total_docs = 0_i64;
    let mut avgdl = 0.0_f64;
    for row in &rows {
        let ndoc: i32 = row.try_get("ndoc")?;
        stats.push(WordStat {
            word: row.try_get("term")?,
            ndoc: i64::from(ndoc),
        });
        total_docs = row.try_get("total_docs")?;
        avgdl = row.try_get("avgdl")?;
    }
    Ok((stats, total_docs, avgdl))
}

/// Splits words into candidate keys (document-frequency cap applied) and the
/// scoring set (every word, with FTS5's IDF formula and clamp).
///
/// A compact variant the corpus does not hold as a lexeme has nothing to
/// meet, so it leaves first: it must not key candidates, score, or count as
/// a survivor of the cap — an absent variant of the whole query would
/// otherwise keep every real word from reaching the rarest-few fallback and
/// select no candidates at all. A word the caller typed keeps its substring
/// and prefix reach whatever its statistics say.
fn scoring_plan(
    stats: &[WordStat],
    total_docs: i64,
    avgdl: f64,
    variant_words: &[String],
) -> ScoringPlan {
    let stats = stats
        .iter()
        .filter(|stat| stat.ndoc > 0 || !variant_words.contains(&stat.word))
        .collect::<Vec<_>>();
    let total = total_docs.max(0);
    let cap = DOCUMENT_FREQUENCY_CAP * precise_f64(total.max(1));
    let mut kept = stats
        .iter()
        .copied()
        .filter(|stat| precise_f64(stat.ndoc.max(0)) <= cap)
        .collect::<Vec<_>>();
    if kept.is_empty() {
        let mut by_rarity = stats.clone();
        by_rarity.sort_by_key(|stat| (stat.ndoc, stat.word.clone()));
        by_rarity.truncate(RARE_WORD_KEEP);
        kept = by_rarity;
    }
    ScoringPlan {
        scored: stats
            .iter()
            .map(|stat| ScoredWord {
                word: stat.word.clone(),
                idf: inverse_document_frequency(total, stat.ndoc),
            })
            .collect(),
        candidate_words: kept.into_iter().map(|stat| stat.word.clone()).collect(),
        avgdl: if avgdl > 0.0 { avgdl } else { 1.0 },
    }
}

/// FTS5's IDF (`fts5_aux.c`): `log((N - n + 0.5) / (n + 0.5))`, clamped to a
/// small positive floor so a word in more than half the corpus scores nothing
/// instead of scoring negative.
fn inverse_document_frequency(total_docs: i64, ndoc: i64) -> f64 {
    let total = precise_f64(total_docs.max(0));
    let matched = precise_f64(ndoc.clamp(0, total_docs.max(0)));
    ((total - matched + 0.5) / (matched + 0.5))
        .ln()
        .max(IDF_FLOOR)
}

/// Catalog corpora stay far below `f64`'s exact-integer range; spelled out so
/// the cast reads as a decision rather than an accident.
#[expect(clippy::cast_precision_loss, reason = "document counts fit in f64")]
fn precise_f64(value: i64) -> f64 {
    value as f64
}

/// `word:* | word:*` over the scored words for candidate selection. Words are
/// runs of alphanumerics and `_`, so the tsquery parser accepts them without
/// quoting; a word with `_` parses as a prefix phrase, which is stricter, not
/// broken.
fn tsquery(words: &[String]) -> String {
    words
        .iter()
        .map(|word| format!("{word}:*"))
        .collect::<Vec<_>>()
        .join(" | ")
}

/// Fixed SQL over the `d` alias; the kind names are the vocabulary's own
/// literals, never caller input.
fn doc_kind_predicate(class: CatalogDocumentClass) -> String {
    let kinds = class
        .document_kinds()
        .iter()
        .map(|kind| format!("'{}'", kind.as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    format!("d.doc_kind IN ({kinds})")
}

fn hit_from_row(row: &sqlx::postgres::PgRow) -> Result<CatalogSearchHit, PostgresSearchError> {
    let surface_kind: String = row.try_get("surface_kind")?;
    if !is_known_surface_kind(&surface_kind) {
        return Err(PostgresSearchError::InvalidStorageValue {
            field: "surface_kind",
            value: surface_kind,
        });
    }
    let field_role: String = row.try_get("field_role")?;
    if !is_known_field_role(&field_role) {
        return Err(PostgresSearchError::InvalidStorageValue {
            field: "field_role",
            value: field_role,
        });
    }
    Ok(CatalogSearchHit {
        doc_id: row.try_get("doc_id")?,
        source_name: row.try_get("source_name")?,
        catalog_name: row.try_get("catalog_name")?,
        surface_kind,
        surface_name: row.try_get("surface_name")?,
        field_name: row.try_get("field_name")?,
        field_role,
    })
}

/// `\`, `%`, and `_` escaped, so identifiers such as `deploy_url` match
/// literally under `LIKE`.
fn escape_like(term: &str) -> String {
    let mut escaped = String::with_capacity(term.len());
    for ch in term.chars() {
        if matches!(ch, '\\' | '%' | '_') {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

/// `%term%` for substring candidates.
fn contains_pattern(term: &str) -> String {
    format!("%{}%", escape_like(term))
}

/// `term%` for the lexeme-prefix join in the score, mirroring the candidate
/// tsquery's `:*`.
fn prefix_pattern(term: &str) -> String {
    format!("{}%", escape_like(term))
}

/// Lexeme candidates: runs of alphanumerics (any script, terms are already
/// lowercased) and `_`, which the `simple` parser accepts without operators
/// or quoting.
fn lexeme_words(term: &str) -> Vec<String> {
    term.split(|ch: char| !(ch.is_alphanumeric() || ch == '_'))
        .filter(|word| !word.is_empty())
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
pub(super) mod plan_tests {
    use super::{
        CatalogQueryPlan, ScoredWord, ScoringPlan, WordStat, contains_pattern,
        inverse_document_frequency, scoring_plan,
    };
    use crate::search::catalog::index::{CatalogDocumentClass, NormalizedSearchTerm};

    fn given(terms: &[&str]) -> Vec<NormalizedSearchTerm> {
        terms
            .iter()
            .map(|term| NormalizedSearchTerm {
                text: (*term).to_string(),
                compact_variant: false,
            })
            .collect()
    }

    fn variant(term: &str) -> NormalizedSearchTerm {
        NormalizedSearchTerm {
            text: term.to_string(),
            compact_variant: true,
        }
    }

    fn stat(word: &str, ndoc: i64) -> WordStat {
        WordStat {
            word: word.to_string(),
            ndoc,
        }
    }

    fn scored(words: &[&str]) -> ScoringPlan {
        ScoringPlan {
            scored: words
                .iter()
                .map(|word| ScoredWord {
                    word: (*word).to_string(),
                    idf: 1.0,
                })
                .collect(),
            candidate_words: words.iter().map(|word| (*word).to_string()).collect(),
            avgdl: 10.0,
        }
    }

    #[test]
    fn patterns_escape_like_metacharacters_and_drop_short_terms() {
        let plan = CatalogQueryPlan::build(&given(&["ab", "deploy_url", "100%"])).expect("plan");

        assert_eq!(plan.words, vec!["100", "deploy_url"]);
        assert_eq!(plan.phrase_pattern, "%deploy\\_url%");
        assert!(plan.wordless_patterns.is_empty());
        assert_eq!(contains_pattern("a\\b"), "%a\\\\b%");
    }

    #[test]
    fn the_phrase_is_the_longest_term_and_terms_split_into_words() {
        let plan =
            CatalogQueryPlan::build(&given(&["issue", "issue labels", "labels"])).expect("plan");

        assert_eq!(plan.phrase_pattern, "%issue labels%");
        assert_eq!(plan.words, vec!["issue", "labels"]);
    }

    #[test]
    fn words_keep_non_ascii_letters() {
        let plan = CatalogQueryPlan::build(&given(&["über-café"])).expect("plan");

        assert_eq!(plan.words, vec!["café", "über"]);
    }

    #[test]
    fn terms_without_words_keep_substring_patterns_and_skip_scoring() {
        let plan = CatalogQueryPlan::build(&given(&["---", "a-b"])).expect("plan");

        assert!(plan.words.is_empty());
        assert_eq!(plan.wordless_patterns, vec!["%---%", "%a-b%"]);
        let sql = plan.sql(&scored(&[]), CatalogDocumentClass::Entries);
        assert!(sql.contains("0::float8 AS score"));
        assert!(!sql.contains("to_tsquery"));
        assert!(sql.contains("d.all_text ILIKE $1 OR d.all_text ILIKE $2"));
    }

    #[test]
    fn only_short_terms_yield_no_plan() {
        assert_eq!(CatalogQueryPlan::build(&given(&["ab"])), None);
        assert_eq!(CatalogQueryPlan::build(&[]), None);
    }

    #[test]
    fn placeholders_are_numbered_in_bind_order() {
        let plan = CatalogQueryPlan::build(&given(&["alpha", "beta"])).expect("plan");

        let sql = plan.sql(&scored(&["alpha", "beta"]), CatalogDocumentClass::Fields);

        assert!(sql.contains("to_tsquery('simple', $1)"));
        assert!(sql.contains("($2::text, $3::float8), ($4::text, $5::float8)"));
        assert!(sql.contains("d.all_text ILIKE $6 OR d.all_text ILIKE $7"));
        assert!(sql.contains("coalesce(c.doc_len::float8, $8) / $8"));
        assert!(sql.contains("c.qualified_name ILIKE $9"));
        assert!(sql.contains("LIMIT $10"));
        assert!(sql.contains("d.doc_kind IN ('column_hint')"));
        assert!(sql.contains("WITH candidates AS MATERIALIZED"));
    }

    #[test]
    fn the_document_frequency_cap_restricts_candidates_but_not_scoring() {
        let stats = [
            WordStat {
                word: "issues".to_string(),
                ndoc: 100,
            },
            WordStat {
                word: "slack".to_string(),
                ndoc: 40,
            },
        ];

        let plan = scoring_plan(&stats, 1_000, 23.0, &[]);

        assert_eq!(plan.candidate_words, vec!["slack"]);
        // The common word still scores, with its true IDF, not a clamp.
        let issues = plan
            .scored
            .iter()
            .find(|word| word.word == "issues")
            .expect("capped word scores");
        let expected = ((1_000.0_f64 - 100.0 + 0.5) / 100.5).ln();
        assert!((issues.idf - expected).abs() < 1e-12);
        assert!((plan.avgdl - 23.0).abs() < f64::EPSILON);
    }

    #[test]
    fn all_common_words_fall_back_to_the_rarest_few() {
        let stats = ["delta", "alpha", "bravo", "charlie"]
            .iter()
            .zip([400_i64, 100, 200, 300])
            .map(|(word, ndoc)| WordStat {
                word: (*word).to_string(),
                ndoc,
            })
            .collect::<Vec<_>>();

        let plan = scoring_plan(&stats, 1_000, 23.0, &[]);

        assert_eq!(
            plan.candidate_words,
            vec!["alpha", "bravo", "charlie"],
            "the rarest words survive as candidate keys, rarest first"
        );
        assert_eq!(plan.scored.len(), 4, "every word still scores");
    }

    #[test]
    fn idf_follows_fts5_including_the_negative_clamp() {
        // A word in most of the corpus is clamped off, not negative.
        assert!((inverse_document_frequency(1_000, 977) - 1e-6).abs() < f64::EPSILON);
        // A rare word scores by the FTS5 formula.
        let expected = ((1_000.0_f64 - 40.0 + 0.5) / 40.5).ln();
        assert!((inverse_document_frequency(1_000, 40) - expected).abs() < 1e-12);
        // Degenerate inputs stay finite.
        assert!((inverse_document_frequency(0, 0) - 1e-6).abs() < f64::EPSILON);
        assert!(inverse_document_frequency(10, 20).is_finite());
    }

    #[test]
    fn a_statistics_free_schema_degrades_to_unweighted_scoring() {
        let stats = [WordStat {
            word: "github".to_string(),
            ndoc: 0,
        }];

        let plan = scoring_plan(&stats, 0, 0.0, &[]);

        let word = plan.scored.first().expect("the only word survives");
        assert!((word.idf - 1e-6).abs() < f64::EPSILON);
        assert_eq!(plan.candidate_words, vec!["github"]);
        assert!((plan.avgdl - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn compact_variants_are_tracked_apart_from_typed_words() {
        let mut terms = given(&["issue", "issue labels", "labels"]);
        terms.push(variant("issuelabels"));

        let plan = CatalogQueryPlan::build(&terms).expect("plan");

        assert_eq!(plan.words, vec!["issue", "issuelabels", "labels"]);
        assert_eq!(plan.variant_words, vec!["issuelabels"]);
    }

    #[test]
    fn a_word_both_typed_and_derived_counts_as_typed() {
        let mut terms = given(&["deploy_url", "deployurl"]);
        terms.push(variant("deployurl"));

        let plan = CatalogQueryPlan::build(&terms).expect("plan");

        assert_eq!(plan.words, vec!["deploy_url", "deployurl"]);
        assert!(plan.variant_words.is_empty());
    }

    #[test]
    fn an_absent_compact_variant_neither_keys_candidates_nor_scores() {
        // `github events` on a corpus where both words are common and no
        // document is named `githubevents`.
        let stats = [
            stat("events", 920),
            stat("github", 970),
            stat("githubevents", 0),
        ];

        let plan = scoring_plan(&stats, 1_000, 23.0, &["githubevents".to_string()]);

        assert_eq!(
            plan.candidate_words,
            vec!["events", "github"],
            "with the variant gone, the rarest-few fallback runs on the real words"
        );
        assert_eq!(
            plan.scored
                .iter()
                .map(|word| word.word.as_str())
                .collect::<Vec<_>>(),
            vec!["events", "github"]
        );
    }

    #[test]
    fn a_present_compact_variant_keys_candidates_and_scores() {
        // `pull requests` where `github.pull_requests` indexes `pullrequests`.
        let stats = [
            stat("pull", 300),
            stat("pullrequests", 6),
            stat("requests", 200),
        ];

        let plan = scoring_plan(&stats, 1_000, 23.0, &["pullrequests".to_string()]);

        assert_eq!(plan.candidate_words, vec!["pullrequests"]);
        assert_eq!(plan.scored.len(), 3);
    }

    #[test]
    fn a_typed_word_the_corpus_lacks_keeps_its_substring_reach() {
        // `enchmark` is no lexeme, but `%enchmark%` must still select
        // `benchmark_runs`; only variants are subject to the absence rule.
        let stats = [stat("enchmark", 0), stat("github", 970)];

        let plan = scoring_plan(&stats, 1_000, 23.0, &[]);

        assert_eq!(plan.candidate_words, vec!["enchmark"]);
    }
}
