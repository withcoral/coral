//! Catalog projection and retrieval on the Postgres store.
//!
//! Retrieval is BM25 in plain SQL (research memo 11): corpus IDF from the
//! per-Workspace `catalog_terms` statistics, FTS5's constants and negative-IDF
//! clamp, and the same 8/6/2/1 field weights the `SQLite` side passes to
//! `bm25()`. Candidates come from a prefix `tsquery` over the query's words
//! OR-ed with trigram `ILIKE` patterns (exact substring semantics; the
//! executor's recheck makes false positives impossible), both restricted to
//! words below the document-frequency cap — FTS5's own `idf <= 0` clamp made
//! explicit, applied where it also buys latency. The whole-query phrase boost
//! leads the order so a whole-phrase match outranks a co-occurrence match;
//! `doc_id` closes the deterministic total order.

use std::collections::BTreeSet;

use sqlx::{Postgres, Row as _, Transaction};

use super::{PostgresSearchError, PostgresSearchStore};
use crate::search::catalog::index::{
    CatalogClearResult, CatalogDocumentClass, CatalogIndexDocument, CatalogIndexSnapshot,
    CatalogRebuildResult, CatalogRefreshResult, CatalogSearchHit, CatalogSearchHits,
    indexed_searchable_text, is_known_field_role, is_known_surface_kind, normalized_search_terms,
    probe_limit, truncate_probe_hits,
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
/// FTS5's hard-coded BM25 constants (`fts5_aux.c`), kept so both backends
/// saturate term frequency and normalize length the same way.
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;
/// FTS5 clamps a non-positive IDF to this instead of letting a term that
/// appears in more than half the corpus push results around.
const IDF_FLOOR: f64 = 1e-6;
/// Words in more of the corpus than this share are dropped from candidates
/// and scoring. Measured (memo 11, design D7): the cap is what keeps the
/// candidate set selective; without it the same relevance costs 5× the time.
const DOCUMENT_FREQUENCY_CAP: f64 = 0.05;
/// When every word is above the cap, keep this many rarest words instead of
/// searching for nothing.
const RARE_WORD_KEEP: usize = 3;

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
        let terms = normalized_search_terms(terms);
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
    /// Words under the document-frequency cap (or the rarest few when nothing
    /// is), with their IDF.
    words: Vec<ScoredWord>,
    avgdl: f64,
}

#[derive(Debug, Clone, PartialEq)]
struct ScoredWord {
    word: String,
    idf: f64,
}

impl CatalogQueryPlan {
    fn build(terms: &[String]) -> Option<Self> {
        let terms = terms
            .iter()
            .filter(|term| term.chars().count() >= MIN_TERM_CHARS)
            .cloned()
            .collect::<Vec<_>>();
        let phrase = terms
            .iter()
            .max_by_key(|term| term.chars().count())?
            .clone();
        let mut words = BTreeSet::new();
        let mut wordless = BTreeSet::new();
        for term in &terms {
            let term_words = lexeme_words(term)
                .into_iter()
                .filter(|word| word.chars().count() >= MIN_TERM_CHARS)
                .collect::<Vec<_>>();
            if term_words.is_empty() {
                wordless.insert(contains_pattern(term));
            } else {
                words.extend(term_words);
            }
        }
        Some(Self {
            phrase_pattern: contains_pattern(&phrase),
            words: words.into_iter().collect(),
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
                words: Vec::new(),
                avgdl: 1.0,
            }
        } else {
            let (stats, total_docs, avgdl) = fetch_word_stats(tx, &self.words).await?;
            scoring_plan(&stats, total_docs, avgdl)
        };
        // Audited: the only dynamic fragments are `doc_kind_predicate`, fixed
        // literals, and placeholder numbers; every value is a bind.
        let mut query = sqlx::query(sqlx::AssertSqlSafe(self.sql(&scoring, class)));
        if !scoring.words.is_empty() {
            query = query.bind(tsquery(&scoring.words));
            for word in &scoring.words {
                query = query.bind(prefix_pattern(&word.word));
                query = query.bind(word.idf);
            }
            for word in &scoring.words {
                query = query.bind(contains_pattern(&word.word));
            }
        }
        for pattern in &self.wordless_patterns {
            query = query.bind(pattern);
        }
        if !scoring.words.is_empty() {
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
        let word_count = scoring.words.len();
        let mut next = 1_usize;
        let mut take = |count: usize| {
            let start = next;
            next += count;
            start
        };
        let tsquery = (word_count > 0).then(|| take(1));
        let values_start = take(word_count * 2);
        let word_pattern_start = take(word_count);
        let wordless_start = take(self.wordless_patterns.len());
        let avgdl = (word_count > 0).then(|| take(1));
        let phrase = take(1);
        let limit = take(1);

        let mut candidates = Vec::new();
        if let Some(tsquery) = tsquery {
            candidates.push(format!("d.tsv @@ to_tsquery('simple', ${tsquery})"));
        }
        for index in 0..word_count {
            candidates.push(format!("d.all_text ILIKE ${}", word_pattern_start + index));
        }
        for index in 0..self.wordless_patterns.len() {
            candidates.push(format!("d.all_text ILIKE ${}", wordless_start + index));
        }
        let candidates = candidates.join(" OR ");

        let score = match avgdl {
            Some(avgdl) => {
                let values = (0..word_count)
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

/// Applies the document-frequency cap and computes each surviving word's IDF
/// with FTS5's formula and clamp.
fn scoring_plan(stats: &[WordStat], total_docs: i64, avgdl: f64) -> ScoringPlan {
    let total = total_docs.max(0);
    let cap = DOCUMENT_FREQUENCY_CAP * precise_f64(total.max(1));
    let mut kept = stats
        .iter()
        .filter(|stat| precise_f64(stat.ndoc.max(0)) <= cap)
        .collect::<Vec<_>>();
    if kept.is_empty() {
        let mut by_rarity = stats.iter().collect::<Vec<_>>();
        by_rarity.sort_by_key(|stat| (stat.ndoc, stat.word.clone()));
        by_rarity.truncate(RARE_WORD_KEEP);
        kept = by_rarity;
    }
    ScoringPlan {
        words: kept
            .into_iter()
            .map(|stat| ScoredWord {
                word: stat.word.clone(),
                idf: inverse_document_frequency(total, stat.ndoc),
            })
            .collect(),
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
fn tsquery(words: &[ScoredWord]) -> String {
    words
        .iter()
        .map(|word| format!("{}:*", word.word))
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
    use crate::search::catalog::index::CatalogDocumentClass;

    fn scored(words: &[&str]) -> ScoringPlan {
        ScoringPlan {
            words: words
                .iter()
                .map(|word| ScoredWord {
                    word: (*word).to_string(),
                    idf: 1.0,
                })
                .collect(),
            avgdl: 10.0,
        }
    }

    #[test]
    fn patterns_escape_like_metacharacters_and_drop_short_terms() {
        let plan = CatalogQueryPlan::build(&[
            "ab".to_string(),
            "deploy_url".to_string(),
            "100%".to_string(),
        ])
        .expect("plan");

        assert_eq!(plan.words, vec!["100", "deploy_url"]);
        assert_eq!(plan.phrase_pattern, "%deploy\\_url%");
        assert!(plan.wordless_patterns.is_empty());
        assert_eq!(contains_pattern("a\\b"), "%a\\\\b%");
    }

    #[test]
    fn the_phrase_is_the_longest_term_and_terms_split_into_words() {
        let plan = CatalogQueryPlan::build(&[
            "issue".to_string(),
            "issue labels".to_string(),
            "labels".to_string(),
        ])
        .expect("plan");

        assert_eq!(plan.phrase_pattern, "%issue labels%");
        assert_eq!(plan.words, vec!["issue", "labels"]);
    }

    #[test]
    fn words_keep_non_ascii_letters() {
        let plan = CatalogQueryPlan::build(&["über-café".to_string()]).expect("plan");

        assert_eq!(plan.words, vec!["café", "über"]);
    }

    #[test]
    fn terms_without_words_keep_substring_patterns_and_skip_scoring() {
        let plan = CatalogQueryPlan::build(&["---".to_string(), "a-b".to_string()]).expect("plan");

        assert!(plan.words.is_empty());
        assert_eq!(plan.wordless_patterns, vec!["%---%", "%a-b%"]);
        let sql = plan.sql(&scored(&[]), CatalogDocumentClass::Entries);
        assert!(sql.contains("0::float8 AS score"));
        assert!(!sql.contains("to_tsquery"));
        assert!(sql.contains("d.all_text ILIKE $1 OR d.all_text ILIKE $2"));
    }

    #[test]
    fn only_short_terms_yield_no_plan() {
        assert_eq!(CatalogQueryPlan::build(&["ab".to_string()]), None);
        assert_eq!(CatalogQueryPlan::build(&[]), None);
    }

    #[test]
    fn placeholders_are_numbered_in_bind_order() {
        let plan =
            CatalogQueryPlan::build(&["alpha".to_string(), "beta".to_string()]).expect("plan");

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
    fn the_document_frequency_cap_drops_common_words() {
        let stats = [
            WordStat {
                word: "github".to_string(),
                ndoc: 950,
            },
            WordStat {
                word: "slack".to_string(),
                ndoc: 40,
            },
        ];

        let plan = scoring_plan(&stats, 1_000, 23.0);

        assert_eq!(
            plan.words
                .iter()
                .map(|word| word.word.as_str())
                .collect::<Vec<_>>(),
            vec!["slack"]
        );
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

        let plan = scoring_plan(&stats, 1_000, 23.0);

        assert_eq!(
            plan.words
                .iter()
                .map(|word| word.word.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "bravo", "charlie"],
            "the rarest words survive, rarest first"
        );
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

        let plan = scoring_plan(&stats, 0, 0.0);

        let word = plan.words.first().expect("the only word survives");
        assert!((word.idf - 1e-6).abs() < f64::EPSILON);
        assert!((plan.avgdl - 1.0).abs() < f64::EPSILON);
    }
}
