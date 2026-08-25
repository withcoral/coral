//! Catalog projection and retrieval on the Postgres store.
//!
//! Retrieval is the engine the benchmark locked: trigram-accelerated `ILIKE`
//! per term for candidates (exact substring semantics; the executor's recheck
//! makes false positives impossible), then a deterministic total order —
//! field-weighted whole-phrase boost, weighted `ts_rank_cd`, trigram
//! similarity, primary key.

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
const PHRASE_WEIGHT_QUALIFIED_NAME: u8 = 8;
const PHRASE_WEIGHT_TITLE: u8 = 6;
const PHRASE_WEIGHT_DESCRIPTION: u8 = 2;
const PHRASE_WEIGHT_SEARCHABLE_TEXT: u8 = 1;

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
            clear_fingerprint(&mut tx).await?;
            tx.commit().await?;
            Ok::<_, PostgresSearchError>(CatalogClearResult {
                deleted_document_count: u32::try_from(deleted).unwrap_or(u32::MAX),
            })
        })?)
    }
}

/// The stored fingerprint, or `None` when any row disagrees with it — a
/// half-written projection is treated as absent, as on the `SQLite` side.
async fn current_fingerprint(
    tx: &mut Transaction<'static, Postgres>,
) -> Result<Option<String>, PostgresSearchError> {
    let fingerprint: Option<String> =
        sqlx::query_scalar("SELECT value FROM search_meta WHERE key = $1")
            .bind(CATALOG_SNAPSHOT_FINGERPRINT_META_KEY)
            .fetch_optional(&mut **tx)
            .await?;
    let Some(fingerprint) = fingerprint else {
        return Ok(None);
    };
    let projection_is_stale: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM catalog_documents WHERE snapshot_fingerprint <> $1)",
    )
    .bind(&fingerprint)
    .fetch_one(&mut **tx)
    .await?;
    Ok((!projection_is_stale).then_some(fingerprint))
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
        insert_documents(tx, chunk, &snapshot.fingerprint).await?;
    }
    sqlx::query(
        "INSERT INTO search_meta (key, value, updated_at) VALUES ($1, $2, now())
         ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
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
    fingerprint: &str,
) -> Result<(), PostgresSearchError> {
    let column = |select: fn(&CatalogIndexDocument) -> &str| {
        documents.iter().map(select).collect::<Vec<_>>()
    };
    sqlx::query(
        "INSERT INTO catalog_documents (
            doc_id, doc_kind, source_name, catalog_name, surface_kind, surface_name,
            field_name, field_role, qualified_name, title, description, searchable_text,
            snapshot_fingerprint
        )
        SELECT
            u.doc_id, u.doc_kind, u.source_name, u.catalog_name, u.surface_kind, u.surface_name,
            u.field_name, u.field_role, u.qualified_name, u.title, u.description,
            u.searchable_text, $13
        FROM UNNEST(
            $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
            $7::text[], $8::text[], $9::text[], $10::text[], $11::text[], $12::text[]
        ) AS u(
            doc_id, doc_kind, source_name, catalog_name, surface_kind, surface_name,
            field_name, field_role, qualified_name, title, description, searchable_text
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
    .bind(fingerprint)
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
/// only interpolated fragments are fixed SQL (`doc_kind_predicate`) and
/// placeholder numbers.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogQueryPlan {
    /// `%term%` per term, `LIKE` metacharacters escaped, for candidates.
    patterns: Vec<String>,
    /// The whole normalized query as a `%phrase%` pattern for the boost. It is
    /// the longest term: query construction appends the whole query as a term,
    /// and it contains every other token.
    phrase_pattern: String,
    /// `word | word` for `to_tsquery('simple', …)`; `None` when no term yields
    /// a lexeme, which drops the rank tier instead of erroring.
    words_query: Option<String>,
    /// All terms joined for `similarity()`.
    joined_terms: String,
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
        let words = terms
            .iter()
            .flat_map(|term| lexeme_words(term))
            .collect::<BTreeSet<_>>();
        Some(Self {
            patterns: terms.iter().map(|term| contains_pattern(term)).collect(),
            phrase_pattern: contains_pattern(&phrase),
            words_query: (!words.is_empty())
                .then(|| words.into_iter().collect::<Vec<_>>().join(" | ")),
            joined_terms: terms.join(" "),
        })
    }

    fn sql(&self, class: CatalogDocumentClass) -> String {
        let candidates = (1..=self.patterns.len())
            .map(|index| format!("d.all_text ILIKE ${index}"))
            .collect::<Vec<_>>()
            .join(" OR ");
        let phrase = self.patterns.len() + 1;
        let mut next = phrase + 1;
        let rank = if self.words_query.is_some() {
            let words = next;
            next += 1;
            format!("ts_rank_cd(d.tsv, to_tsquery('simple', ${words}))")
        } else {
            "0".to_string()
        };
        let joined = next;
        let limit = next + 1;
        format!(
            "SELECT d.doc_id, d.source_name, d.surface_kind, d.surface_name, d.field_name,
                    d.field_role, d.catalog_name
             FROM catalog_documents AS d
             WHERE {predicate} AND ({candidates})
             ORDER BY
                 (d.qualified_name ILIKE ${phrase})::int * {PHRASE_WEIGHT_QUALIFIED_NAME}
                 + (d.title ILIKE ${phrase})::int * {PHRASE_WEIGHT_TITLE}
                 + (d.description ILIKE ${phrase})::int * {PHRASE_WEIGHT_DESCRIPTION}
                 + (d.searchable_text ILIKE ${phrase})::int * {PHRASE_WEIGHT_SEARCHABLE_TEXT} DESC,
                 {rank} DESC,
                 similarity(d.all_text, ${joined}) DESC,
                 d.doc_id ASC
             LIMIT ${limit}",
            predicate = class.doc_kind_predicate(),
        )
    }

    async fn fetch(
        &self,
        tx: &mut Transaction<'static, Postgres>,
        class: CatalogDocumentClass,
        limit: usize,
    ) -> Result<Vec<CatalogSearchHit>, PostgresSearchError> {
        // Audited: the only dynamic fragments are `doc_kind_predicate`, a
        // fixed literal, and placeholder numbers; every value is a bind.
        let mut query = sqlx::query(sqlx::AssertSqlSafe(self.sql(class)));
        for pattern in &self.patterns {
            query = query.bind(pattern);
        }
        query = query.bind(&self.phrase_pattern);
        if let Some(words_query) = &self.words_query {
            query = query.bind(words_query);
        }
        let rows = query
            .bind(&self.joined_terms)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .fetch_all(&mut **tx)
            .await?;
        rows.iter().map(hit_from_row).collect()
    }
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

/// `%term%` with `\`, `%`, and `_` escaped, so identifiers such as
/// `deploy_url` match literally.
fn contains_pattern(term: &str) -> String {
    let mut escaped = String::with_capacity(term.len() + 2);
    escaped.push('%');
    for ch in term.chars() {
        if matches!(ch, '\\' | '%' | '_') {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped.push('%');
    escaped
}

/// Lexeme candidates for the rank tier: runs of alphanumerics (any script,
/// terms are already lowercased) and `_`, which the `simple` parser accepts
/// without operators or quoting.
fn lexeme_words(term: &str) -> Vec<String> {
    term.split(|ch: char| !(ch.is_alphanumeric() || ch == '_'))
        .filter(|word| !word.is_empty())
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
pub(super) mod plan_tests {
    use super::{CatalogQueryPlan, contains_pattern};
    use crate::search::catalog::index::CatalogDocumentClass;

    #[test]
    fn patterns_escape_like_metacharacters_and_drop_short_terms() {
        let plan = CatalogQueryPlan::build(&[
            "ab".to_string(),
            "deploy_url".to_string(),
            "100%".to_string(),
        ])
        .expect("plan");

        assert_eq!(plan.patterns, vec!["%deploy\\_url%", "%100\\%%"]);
        assert_eq!(contains_pattern("a\\b"), "%a\\\\b%");
    }

    #[test]
    fn the_phrase_is_the_longest_term_and_words_feed_the_rank_tier() {
        let plan = CatalogQueryPlan::build(&[
            "issue".to_string(),
            "issue labels".to_string(),
            "labels".to_string(),
        ])
        .expect("plan");

        assert_eq!(plan.phrase_pattern, "%issue labels%");
        assert_eq!(plan.words_query.as_deref(), Some("issue | labels"));
        assert_eq!(plan.joined_terms, "issue issue labels labels");
    }

    #[test]
    fn lexemes_keep_non_ascii_letters() {
        let plan = CatalogQueryPlan::build(&["über-café".to_string()]).expect("plan");

        assert_eq!(plan.words_query.as_deref(), Some("café | über"));
    }

    #[test]
    fn terms_without_lexemes_drop_the_rank_tier_instead_of_erroring() {
        let plan = CatalogQueryPlan::build(&["---".to_string()]).expect("plan");

        assert_eq!(plan.words_query, None);
        assert!(plan.sql(CatalogDocumentClass::Entries).contains("0 DESC"));
        assert!(
            !plan
                .sql(CatalogDocumentClass::Entries)
                .contains("to_tsquery")
        );
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

        let sql = plan.sql(CatalogDocumentClass::Fields);

        assert!(sql.contains("d.all_text ILIKE $1 OR d.all_text ILIKE $2"));
        assert!(sql.contains("d.qualified_name ILIKE $3"));
        assert!(sql.contains("to_tsquery('simple', $4)"));
        assert!(sql.contains("similarity(d.all_text, $5)"));
        assert!(sql.contains("LIMIT $6"));
        assert!(sql.contains("d.doc_kind = 'column_hint'"));
    }
}
