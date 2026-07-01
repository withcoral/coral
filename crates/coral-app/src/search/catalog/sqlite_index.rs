//! `SQLite` catalog metadata projection and retrieval primitives.

#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "SQLite catalog index is wired by follow-up catalog provider PR"
    )
)]

use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension as _, Transaction, TransactionBehavior, params};

use crate::search::sqlite_store::SqliteSearchError;
use crate::workspaces::WorkspaceName;

const CATALOG_SNAPSHOT_FINGERPRINT_META_KEY_PREFIX: &str = "catalog_snapshot_fingerprint";

#[derive(Clone, Default)]
pub(crate) struct SqliteCatalogIndex;

impl SqliteCatalogIndex {
    pub(crate) fn new() -> Self {
        Self
    }

    #[expect(
        clippy::unused_self,
        reason = "kept as an instance method so catalog provider can own index capability consistently"
    )]
    pub(crate) fn refresh(
        &self,
        connection: &mut Connection,
        workspace_name: &WorkspaceName,
        snapshot: &CatalogIndexSnapshot,
    ) -> Result<CatalogRefreshResult, SqliteSearchError> {
        let current_fingerprint = catalog_fingerprint(connection, workspace_name)?;
        if current_fingerprint.as_deref() == Some(snapshot.fingerprint.as_str()) {
            return Ok(CatalogRefreshResult {
                refreshed: false,
                document_count: u32::try_from(snapshot.documents.len()).unwrap_or(u32::MAX),
            });
        }

        refresh_catalog_documents_after_stale_check(connection, workspace_name, snapshot)
    }

    #[expect(
        clippy::unused_self,
        reason = "kept as an instance method so catalog provider can own index capability consistently"
    )]
    pub(crate) fn search(
        &self,
        connection: &Connection,
        workspace_name: &WorkspaceName,
        terms: &[String],
        limit: usize,
    ) -> Result<CatalogSearchHits, SqliteSearchError> {
        let document_count = catalog_document_count(connection, workspace_name)?;
        let terms = normalized_search_terms(terms);
        if terms.is_empty() || limit == 0 {
            return Ok(CatalogSearchHits {
                hits: Vec::new(),
                document_count,
                retrieval_limited: false,
            });
        }

        let mut hits = BTreeMap::<String, CatalogSearchHit>::new();
        let mut retrieval_limited = false;

        if let Some(match_query) = fts_match_query(&terms) {
            let mut fts_hits = fts_search(connection, workspace_name, &match_query, &terms, limit)?;
            retrieval_limited |= truncate_probe_hits(&mut fts_hits, limit);
            merge_hits(&mut hits, fts_hits);
        }

        let exact_hits = exact_prefix_search(
            connection,
            workspace_name,
            &terms,
            limit,
            &mut retrieval_limited,
        )?;
        merge_hits(&mut hits, exact_hits);

        let mut hits = hits.into_values().collect::<Vec<_>>();
        // SQLite returns a bounded candidate window; provider ranking owns final relevance ordering.
        sort_catalog_hits_for_storage(&mut hits);

        Ok(CatalogSearchHits {
            hits,
            document_count,
            retrieval_limited,
        })
    }
}

fn refresh_catalog_documents_after_stale_check(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    snapshot: &CatalogIndexSnapshot,
) -> Result<CatalogRefreshResult, SqliteSearchError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current_fingerprint = catalog_fingerprint(&transaction, workspace_name)?;
    if current_fingerprint.as_deref() == Some(snapshot.fingerprint.as_str()) {
        transaction.commit()?;
        return Ok(CatalogRefreshResult {
            refreshed: false,
            document_count: u32::try_from(snapshot.documents.len()).unwrap_or(u32::MAX),
        });
    }

    replace_catalog_documents(&transaction, workspace_name, snapshot)?;
    transaction.commit()?;
    Ok(CatalogRefreshResult {
        refreshed: true,
        document_count: u32::try_from(snapshot.documents.len()).unwrap_or(u32::MAX),
    })
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogIndexSnapshot {
    pub(crate) documents: Vec<CatalogIndexDocument>,
    pub(crate) fingerprint: String,
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogIndexDocument {
    pub(crate) doc_id: String,
    pub(crate) doc_kind: CatalogIndexDocumentKind,
    pub(crate) source_name: String,
    pub(crate) surface_kind: String,
    pub(crate) surface_name: String,
    pub(crate) field_name: String,
    pub(crate) field_role: String,
    pub(crate) qualified_name: String,
    pub(crate) title: String,
    pub(crate) description: String,
    pub(crate) searchable_text: String,
    pub(crate) payload_json: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogIndexDocumentKind {
    CatalogTable,
    CatalogTableFunction,
    ColumnHint,
}

impl CatalogIndexDocumentKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::CatalogTable => "catalog_table",
            Self::CatalogTableFunction => "catalog_table_function",
            Self::ColumnHint => "column_hint",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "catalog_table" => Some(Self::CatalogTable),
            "catalog_table_function" => Some(Self::CatalogTableFunction),
            "column_hint" => Some(Self::ColumnHint),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogRefreshResult {
    pub(crate) refreshed: bool,
    pub(crate) document_count: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogSearchHits {
    pub(crate) hits: Vec<CatalogSearchHit>,
    pub(crate) document_count: u32,
    pub(crate) retrieval_limited: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogSearchHit {
    pub(crate) doc_id: String,
    pub(crate) doc_kind: CatalogIndexDocumentKind,
    pub(crate) source_name: String,
    pub(crate) surface_kind: String,
    pub(crate) surface_name: String,
    pub(crate) field_name: String,
    pub(crate) field_role: String,
    pub(crate) description: String,
    pub(crate) matched_fields: Vec<String>,
    pub(crate) retrieval_score: u32,
}

fn replace_catalog_documents(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    snapshot: &CatalogIndexSnapshot,
) -> Result<(), SqliteSearchError> {
    transaction.execute(
        "DELETE FROM catalog_documents_fts WHERE workspace = ?1",
        params![workspace_name.as_str()],
    )?;
    transaction.execute(
        "DELETE FROM catalog_documents WHERE workspace = ?1",
        params![workspace_name.as_str()],
    )?;

    {
        let mut document_insert = transaction.prepare(
            "
            INSERT INTO catalog_documents (
                workspace,
                doc_id,
                doc_kind,
                source_name,
                surface_kind,
                surface_name,
                field_name,
                field_role,
                qualified_name,
                title,
                description,
                payload_json,
                snapshot_fingerprint,
                updated_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            ",
        )?;
        let mut fts_insert = transaction.prepare(
            "
            INSERT INTO catalog_documents_fts (
                workspace,
                doc_id,
                title,
                qualified_name,
                description,
                searchable_text
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ",
        )?;

        for document in &snapshot.documents {
            document_insert.execute(params![
                workspace_name.as_str(),
                &document.doc_id,
                document.doc_kind.as_str(),
                &document.source_name,
                &document.surface_kind,
                &document.surface_name,
                &document.field_name,
                &document.field_role,
                &document.qualified_name,
                &document.title,
                &document.description,
                &document.payload_json,
                &snapshot.fingerprint,
            ])?;
            fts_insert.execute(params![
                workspace_name.as_str(),
                &document.doc_id,
                &document.title,
                &document.qualified_name,
                &document.description,
                &document.searchable_text,
            ])?;
        }
    }

    transaction.execute(
        "
        INSERT INTO search_meta (key, value, updated_at)
        VALUES (?1, ?2, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        ",
        params![
            catalog_fingerprint_key(workspace_name),
            snapshot.fingerprint.as_str()
        ],
    )?;
    Ok(())
}

fn catalog_fingerprint(
    connection: &Connection,
    workspace_name: &WorkspaceName,
) -> Result<Option<String>, SqliteSearchError> {
    Ok(connection
        .query_row(
            "SELECT value FROM search_meta WHERE key = ?1",
            params![catalog_fingerprint_key(workspace_name)],
            |row| row.get::<_, String>(0),
        )
        .optional()?)
}

fn catalog_fingerprint_key(workspace_name: &WorkspaceName) -> String {
    format!(
        "{CATALOG_SNAPSHOT_FINGERPRINT_META_KEY_PREFIX}:{}",
        workspace_name.as_str()
    )
}

fn catalog_document_count(
    connection: &Connection,
    workspace_name: &WorkspaceName,
) -> Result<u32, SqliteSearchError> {
    let count: i64 = connection.query_row(
        "SELECT count(*) FROM catalog_documents WHERE workspace = ?1",
        params![workspace_name.as_str()],
        |row| row.get(0),
    )?;
    Ok(u32::try_from(count).unwrap_or(u32::MAX))
}

fn fts_search(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    match_query: &str,
    terms: &[String],
    limit: usize,
) -> Result<Vec<CatalogSearchHit>, SqliteSearchError> {
    let mut statement = connection.prepare(
        "
        SELECT
            d.doc_id,
            d.doc_kind,
            d.source_name,
            d.surface_kind,
            d.surface_name,
            d.field_name,
            d.field_role,
            d.qualified_name,
            d.title,
            d.description,
            f.title,
            f.qualified_name,
            f.description,
            f.searchable_text
        FROM catalog_documents_fts f
        JOIN catalog_documents d
            ON d.workspace = f.workspace AND d.doc_id = f.doc_id
        WHERE f.workspace = ?1 AND catalog_documents_fts MATCH ?2
        ORDER BY bm25(catalog_documents_fts, 1.0, 1.0, 6.0, 8.0, 2.0, 1.0) ASC,
            d.doc_kind ASC,
            d.doc_id ASC
        LIMIT ?3
        ",
    )?;
    let rows = statement.query_map(
        params![
            workspace_name.as_str(),
            match_query,
            i64::try_from(probe_limit(limit)).unwrap_or(i64::MAX),
        ],
        |row| hit_from_row(row, terms, 2_000),
    )?;

    collect_hits(rows)
}

fn exact_prefix_search(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    terms: &[String],
    limit: usize,
    retrieval_limited: &mut bool,
) -> Result<Vec<CatalogSearchHit>, SqliteSearchError> {
    let mut hits = Vec::new();
    let per_term_limit = probe_limit(limit);
    let mut statement = connection.prepare(
        "
        SELECT
            d.doc_id,
            d.doc_kind,
            d.source_name,
            d.surface_kind,
            d.surface_name,
            d.field_name,
            d.field_role,
            d.qualified_name,
            d.title,
            d.description,
            d.title,
            d.qualified_name,
            d.description,
            ''
        FROM catalog_documents d
        WHERE d.workspace = ?1
            AND (
                lower(d.title) = ?2
                OR lower(d.qualified_name) = ?2
                OR lower(d.surface_name) = ?2
                OR lower(d.field_name) = ?2
                OR lower(d.title) LIKE ?3 ESCAPE '\\'
                OR lower(d.qualified_name) LIKE ?3 ESCAPE '\\'
                OR lower(d.surface_name) LIKE ?3 ESCAPE '\\'
                OR lower(d.field_name) LIKE ?3 ESCAPE '\\'
            )
        ORDER BY
            CASE
                WHEN lower(d.qualified_name) = ?2 THEN 0
                WHEN lower(d.title) = ?2
                    OR lower(d.surface_name) = ?2
                    OR lower(d.field_name) = ?2
                THEN 1
                WHEN lower(d.qualified_name) LIKE ?3 ESCAPE '\\' THEN 2
                WHEN lower(d.title) LIKE ?3 ESCAPE '\\'
                    OR lower(d.surface_name) LIKE ?3 ESCAPE '\\'
                    OR lower(d.field_name) LIKE ?3 ESCAPE '\\'
                THEN 3
                ELSE 4
            END,
            d.doc_kind ASC,
            d.doc_id ASC
        LIMIT ?4
        ",
    )?;
    for term in terms {
        let prefix = like_prefix_pattern(term);
        let rows = statement.query_map(
            params![
                workspace_name.as_str(),
                term,
                prefix,
                i64::try_from(per_term_limit).unwrap_or(i64::MAX),
            ],
            |row| hit_from_row(row, terms, 5_000),
        )?;
        let mut term_hits = collect_hits(rows)?;
        *retrieval_limited |= truncate_probe_hits(&mut term_hits, limit);
        hits.extend(term_hits);
    }
    Ok(hits)
}

fn probe_limit(limit: usize) -> usize {
    limit.saturating_add(1).max(1)
}

fn truncate_probe_hits(hits: &mut Vec<CatalogSearchHit>, limit: usize) -> bool {
    if hits.len() > limit {
        hits.truncate(limit);
        true
    } else {
        false
    }
}

fn like_prefix_pattern(term: &str) -> String {
    let mut pattern = String::with_capacity(term.len() + 1);
    for ch in term.chars() {
        if matches!(ch, '\\' | '%' | '_') {
            pattern.push('\\');
        }
        pattern.push(ch);
    }
    pattern.push('%');
    pattern
}

fn hit_from_row(
    row: &rusqlite::Row<'_>,
    terms: &[String],
    base_score: u32,
) -> rusqlite::Result<CatalogSearchHit> {
    let doc_kind_raw: String = row.get(1)?;
    let doc_kind = CatalogIndexDocumentKind::from_str(&doc_kind_raw)
        .unwrap_or(CatalogIndexDocumentKind::ColumnHint);
    let title_field: String = row.get(10)?;
    let qualified_name_field: String = row.get(11)?;
    let description_field: String = row.get(12)?;
    let searchable_text: String = row.get(13)?;
    let field_name: String = row.get(5)?;
    let surface_name: String = row.get(4)?;
    let matched_fields = matched_fields(
        terms,
        &[
            ("title", title_field.as_str()),
            ("qualified_name", qualified_name_field.as_str()),
            ("surface_name", surface_name.as_str()),
            ("field_name", field_name.as_str()),
            ("description", description_field.as_str()),
            ("searchable_text", searchable_text.as_str()),
        ],
    );

    Ok(CatalogSearchHit {
        doc_id: row.get(0)?,
        doc_kind,
        source_name: row.get(2)?,
        surface_kind: row.get(3)?,
        surface_name,
        field_name,
        field_role: row.get(6)?,
        description: row.get(9)?,
        matched_fields,
        retrieval_score: base_score,
    })
}

fn collect_hits(
    rows: rusqlite::MappedRows<
        '_,
        impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<CatalogSearchHit>,
    >,
) -> Result<Vec<CatalogSearchHit>, SqliteSearchError> {
    let mut hits = Vec::new();
    for row in rows {
        hits.push(row?);
    }
    Ok(hits)
}

fn merge_hits(hits: &mut BTreeMap<String, CatalogSearchHit>, incoming: Vec<CatalogSearchHit>) {
    for hit in incoming {
        hits.entry(hit.doc_id.clone())
            .and_modify(|existing| {
                existing.matched_fields.extend(hit.matched_fields.clone());
                existing.matched_fields.sort();
                existing.matched_fields.dedup();
                if hit.retrieval_score > existing.retrieval_score {
                    existing.retrieval_score = hit.retrieval_score;
                }
            })
            .or_insert(hit);
    }
}

fn fts_match_query(terms: &[String]) -> Option<String> {
    let phrases = terms
        .iter()
        .filter(|term| term.chars().count() >= 3)
        .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
        .collect::<Vec<_>>();
    if phrases.is_empty() {
        None
    } else {
        Some(phrases.join(" OR "))
    }
}

fn normalized_search_terms(terms: &[String]) -> Vec<String> {
    terms
        .iter()
        .map(|term| term.trim().to_lowercase())
        .filter(|term| !term.is_empty())
        .collect::<Vec<_>>()
}

fn matched_fields(terms: &[String], fields: &[(&'static str, &str)]) -> Vec<String> {
    let mut matched = fields
        .iter()
        .filter_map(|(field, value)| {
            let normalized = value.to_lowercase();
            terms
                .iter()
                .any(|term| normalized.contains(term.as_str()))
                .then_some((*field).to_string())
        })
        .collect::<Vec<_>>();
    matched.sort();
    matched.dedup();
    matched
}

fn sort_catalog_hits_for_storage(hits: &mut [CatalogSearchHit]) {
    hits.sort_by(|left, right| {
        (
            std::cmp::Reverse(left.retrieval_score),
            doc_kind_order(left.doc_kind),
            left.doc_id.as_str(),
        )
            .cmp(&(
                std::cmp::Reverse(right.retrieval_score),
                doc_kind_order(right.doc_kind),
                right.doc_id.as_str(),
            ))
    });
}

fn doc_kind_order(kind: CatalogIndexDocumentKind) -> u8 {
    match kind {
        CatalogIndexDocumentKind::CatalogTable | CatalogIndexDocumentKind::CatalogTableFunction => {
            0
        }
        CatalogIndexDocumentKind::ColumnHint => 1,
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{
        CatalogIndexDocument, CatalogIndexDocumentKind, CatalogIndexSnapshot, SqliteCatalogIndex,
        like_prefix_pattern, refresh_catalog_documents_after_stale_check,
    };
    use crate::search::sqlite_store::SqliteSearchStore;
    use crate::workspaces::WorkspaceName;

    #[test]
    fn refresh_and_search_catalog_metadata() {
        let temp = tempdir().expect("tempdir");
        let store = SqliteSearchStore::open(temp.path().join("search.sqlite3")).expect("store");
        let mut connection = store.connect().expect("connect");
        let workspace = WorkspaceName::default();
        let index = SqliteCatalogIndex::new();
        let snapshot = catalog_index_snapshot();

        let refresh = index
            .refresh(&mut connection, &workspace, &snapshot)
            .expect("refresh catalog");
        assert!(refresh.refreshed);
        assert!(refresh.document_count > 0);

        let second_refresh = index
            .refresh(&mut connection, &workspace, &snapshot)
            .expect("second refresh");
        assert!(!second_refresh.refreshed);

        let hits = index
            .search(
                &connection,
                &workspace,
                &[
                    "github".to_string(),
                    "deployments".to_string(),
                    "sha".to_string(),
                ],
                10,
            )
            .expect("search catalog");

        assert!(hits.hits.iter().any(|hit| hit.doc_kind
            == CatalogIndexDocumentKind::CatalogTableFunction
            && hit.surface_name == "search_deployments"));
        assert_eq!(hits.document_count, 3);
        let sha_hit = hits
            .hits
            .iter()
            .find(|hit| {
                hit.doc_kind == CatalogIndexDocumentKind::ColumnHint && hit.field_name == "sha"
            })
            .expect("sha column hit");
        assert_eq!(sha_hit.surface_kind, "table_function");
        assert_eq!(sha_hit.field_role, "table_function_result_column");
        assert_eq!(sha_hit.description, "Deployment commit SHA");
    }

    #[test]
    fn refresh_rechecks_fingerprint_after_writer_lock() {
        let temp = tempdir().expect("tempdir");
        let store = SqliteSearchStore::open(temp.path().join("search.sqlite3")).expect("store");
        let mut connection = store.connect().expect("connect");
        let workspace = WorkspaceName::default();
        let index = SqliteCatalogIndex::new();
        let snapshot = catalog_index_snapshot();

        let refresh = index
            .refresh(&mut connection, &workspace, &snapshot)
            .expect("refresh catalog");
        assert!(refresh.refreshed);

        let refresh_after_stale_check =
            refresh_catalog_documents_after_stale_check(&mut connection, &workspace, &snapshot)
                .expect("refresh after stale check");

        assert!(!refresh_after_stale_check.refreshed);
    }

    #[test]
    fn short_identifiers_match_without_trigram_fts() {
        let temp = tempdir().expect("tempdir");
        let store = SqliteSearchStore::open(temp.path().join("search.sqlite3")).expect("store");
        let mut connection = store.connect().expect("connect");
        let workspace = WorkspaceName::default();
        let index = SqliteCatalogIndex::new();
        let snapshot = catalog_index_snapshot();
        index
            .refresh(&mut connection, &workspace, &snapshot)
            .expect("refresh");

        let hits = index
            .search(&connection, &workspace, &["q".to_string()], 10)
            .expect("short search");

        assert!(hits.hits.iter().any(|hit| hit.field_name == "q"));
    }

    #[test]
    fn search_terms_are_normalized_before_retrieval() {
        let temp = tempdir().expect("tempdir");
        let store = SqliteSearchStore::open(temp.path().join("search.sqlite3")).expect("store");
        let mut connection = store.connect().expect("connect");
        let workspace = WorkspaceName::default();
        let index = SqliteCatalogIndex::new();
        let snapshot = catalog_index_snapshot();
        index
            .refresh(&mut connection, &workspace, &snapshot)
            .expect("refresh");

        let hits = index
            .search(&connection, &workspace, &[" GitHub ".to_string()], 10)
            .expect("search");

        assert!(hits.hits.iter().any(|hit| hit.source_name == "github"));
        assert!(hits.hits.iter().any(|hit| !hit.matched_fields.is_empty()));
    }

    #[test]
    fn empty_terms_are_ignored() {
        let temp = tempdir().expect("tempdir");
        let store = SqliteSearchStore::open(temp.path().join("search.sqlite3")).expect("store");
        let mut connection = store.connect().expect("connect");
        let workspace = WorkspaceName::default();
        let index = SqliteCatalogIndex::new();
        let snapshot = catalog_index_snapshot();
        index
            .refresh(&mut connection, &workspace, &snapshot)
            .expect("refresh");

        let hits = index
            .search(&connection, &workspace, &["  ".to_string()], 10)
            .expect("search");

        assert!(hits.hits.is_empty());
        assert!(!hits.retrieval_limited);
    }

    #[test]
    fn exact_field_name_matches_report_matched_field() {
        let temp = tempdir().expect("tempdir");
        let store = SqliteSearchStore::open(temp.path().join("search.sqlite3")).expect("store");
        let mut connection = store.connect().expect("connect");
        let workspace = WorkspaceName::default();
        let index = SqliteCatalogIndex::new();
        let snapshot = field_name_match_snapshot();
        index
            .refresh(&mut connection, &workspace, &snapshot)
            .expect("refresh");

        let hits = index
            .search(&connection, &workspace, &["id".to_string()], 10)
            .expect("search");
        let hit = hits
            .hits
            .iter()
            .find(|hit| hit.field_name == "id")
            .expect("id field hit");

        assert!(hit.matched_fields.iter().any(|field| field == "field_name"));
    }

    #[test]
    fn like_prefix_pattern_escapes_sql_wildcards() {
        assert_eq!(like_prefix_pattern("user_id"), "user\\_id%");
        assert_eq!(like_prefix_pattern("rate%"), "rate\\%%");
        assert_eq!(like_prefix_pattern("path\\name"), "path\\\\name%");
    }

    #[test]
    fn prefix_fallback_treats_underscores_as_literals() {
        let temp = tempdir().expect("tempdir");
        let store = SqliteSearchStore::open(temp.path().join("search.sqlite3")).expect("store");
        let mut connection = store.connect().expect("connect");
        let workspace = WorkspaceName::default();
        let index = SqliteCatalogIndex::new();
        let snapshot = underscore_column_snapshot();
        index
            .refresh(&mut connection, &workspace, &snapshot)
            .expect("refresh");

        let hits = index
            .search(&connection, &workspace, &["user_id".to_string()], 10)
            .expect("search");

        assert!(hits.hits.iter().any(|hit| hit.field_name == "user_id"));
        assert!(
            !hits.hits.iter().any(|hit| hit.field_name == "user0id"),
            "SQL LIKE prefix fallback must not treat '_' as a wildcard"
        );
    }

    #[test]
    fn fts_ranking_weights_qualified_name_before_title_inside_limit() {
        let temp = tempdir().expect("tempdir");
        let store = SqliteSearchStore::open(temp.path().join("search.sqlite3")).expect("store");
        let mut connection = store.connect().expect("connect");
        let workspace = WorkspaceName::default();
        let index = SqliteCatalogIndex::new();
        let snapshot = fts_weight_snapshot();
        index
            .refresh(&mut connection, &workspace, &snapshot)
            .expect("refresh");

        let hits = index
            .search(&connection, &workspace, &["needle".to_string()], 1)
            .expect("search");

        assert_eq!(hits.hits.len(), 1);
        assert_eq!(
            hits.hits.first().expect("top search hit").doc_id,
            "column:qualified-name-match"
        );
        assert!(hits.retrieval_limited);
    }

    #[test]
    fn exact_identifier_is_retained_before_prefix_limit() {
        let temp = tempdir().expect("tempdir");
        let store = SqliteSearchStore::open(temp.path().join("search.sqlite3")).expect("store");
        let mut connection = store.connect().expect("connect");
        let workspace = WorkspaceName::default();
        let index = SqliteCatalogIndex::new();
        let snapshot = identifier_priority_snapshot();
        index
            .refresh(&mut connection, &workspace, &snapshot)
            .expect("refresh");

        let hits = index
            .search(&connection, &workspace, &["id".to_string()], 1)
            .expect("search");

        assert_eq!(hits.hits.len(), 1);
        assert_eq!(hits.hits.first().expect("top search hit").field_name, "id");
        assert!(hits.retrieval_limited);
    }

    #[test]
    fn merged_candidate_windows_are_not_globally_truncated() {
        let temp = tempdir().expect("tempdir");
        let store = SqliteSearchStore::open(temp.path().join("search.sqlite3")).expect("store");
        let mut connection = store.connect().expect("connect");
        let workspace = WorkspaceName::default();
        let index = SqliteCatalogIndex::new();
        let snapshot = identifier_priority_snapshot();
        index
            .refresh(&mut connection, &workspace, &snapshot)
            .expect("refresh");

        let hits = index
            .search(
                &connection,
                &workspace,
                &["id".to_string(), "identity".to_string()],
                1,
            )
            .expect("search");

        assert!(hits.hits.iter().any(|hit| hit.field_name == "id"));
        assert!(hits.hits.iter().any(|hit| hit.field_name == "identity"));
        assert!(hits.retrieval_limited);
    }

    #[test]
    fn exact_fit_does_not_report_retrieval_limited() {
        let temp = tempdir().expect("tempdir");
        let store = SqliteSearchStore::open(temp.path().join("search.sqlite3")).expect("store");
        let mut connection = store.connect().expect("connect");
        let workspace = WorkspaceName::default();
        let index = SqliteCatalogIndex::new();
        let snapshot = identifier_priority_snapshot();
        index
            .refresh(&mut connection, &workspace, &snapshot)
            .expect("refresh");

        let hits = index
            .search(&connection, &workspace, &["id".to_string()], 2)
            .expect("search");

        assert_eq!(hits.hits.len(), 2);
        assert!(!hits.retrieval_limited);
    }

    #[test]
    fn probe_past_limit_reports_retrieval_limited() {
        let temp = tempdir().expect("tempdir");
        let store = SqliteSearchStore::open(temp.path().join("search.sqlite3")).expect("store");
        let mut connection = store.connect().expect("connect");
        let workspace = WorkspaceName::default();
        let index = SqliteCatalogIndex::new();
        let snapshot = identifier_probe_snapshot();
        index
            .refresh(&mut connection, &workspace, &snapshot)
            .expect("refresh");

        let hits = index
            .search(&connection, &workspace, &["id".to_string()], 2)
            .expect("search");

        assert_eq!(hits.hits.len(), 2);
        assert!(hits.retrieval_limited);
    }

    fn catalog_index_snapshot() -> CatalogIndexSnapshot {
        CatalogIndexSnapshot {
            fingerprint: "catalog-fixture-v1".to_string(),
            documents: vec![
                document(DocumentInput {
                    doc_id: "catalog:function:github.search_deployments",
                    doc_kind: CatalogIndexDocumentKind::CatalogTableFunction,
                    source_name: "github",
                    surface_kind: "table_function",
                    surface_name: "search_deployments",
                    field_name: "",
                    field_role: "",
                    qualified_name: "github.search_deployments",
                    title: "search_deployments",
                    description: "Search GitHub deployments",
                    searchable_text: "github search_deployments github.search_deployments deployments search",
                }),
                document(DocumentInput {
                    doc_id: "argument:function:github.search_deployments:q",
                    doc_kind: CatalogIndexDocumentKind::ColumnHint,
                    source_name: "github",
                    surface_kind: "table_function",
                    surface_name: "search_deployments",
                    field_name: "q",
                    field_role: "table_function_argument",
                    qualified_name: "github.search_deployments.q",
                    title: "q",
                    description: "Table function argument",
                    searchable_text: "github search_deployments q table function argument",
                }),
                document(DocumentInput {
                    doc_id: "result_column:function:github.search_deployments:sha",
                    doc_kind: CatalogIndexDocumentKind::ColumnHint,
                    source_name: "github",
                    surface_kind: "table_function",
                    surface_name: "search_deployments",
                    field_name: "sha",
                    field_role: "table_function_result_column",
                    qualified_name: "github.search_deployments.sha",
                    title: "sha",
                    description: "Deployment commit SHA",
                    searchable_text: "github search_deployments sha deployment commit",
                }),
            ],
        }
    }

    fn fts_weight_snapshot() -> CatalogIndexSnapshot {
        CatalogIndexSnapshot {
            fingerprint: "fts-weight-fixture-v1".to_string(),
            documents: vec![
                document(DocumentInput {
                    doc_id: "column:title-match",
                    doc_kind: CatalogIndexDocumentKind::ColumnHint,
                    source_name: "fixture",
                    surface_kind: "table",
                    surface_name: "users",
                    field_name: "title_match",
                    field_role: "table_column",
                    qualified_name: "fixture.users.title_match",
                    title: "title needle",
                    description: "",
                    searchable_text: "",
                }),
                document(DocumentInput {
                    doc_id: "column:qualified-name-match",
                    doc_kind: CatalogIndexDocumentKind::ColumnHint,
                    source_name: "fixture",
                    surface_kind: "table",
                    surface_name: "users",
                    field_name: "qualified_name_match",
                    field_role: "table_column",
                    qualified_name: "fixture.needle",
                    title: "qualified row",
                    description: "",
                    searchable_text: "",
                }),
            ],
        }
    }

    fn identifier_priority_snapshot() -> CatalogIndexSnapshot {
        CatalogIndexSnapshot {
            fingerprint: "identifier-priority-fixture-v1".to_string(),
            documents: vec![
                document(DocumentInput {
                    doc_id: "a-prefix-identifier",
                    doc_kind: CatalogIndexDocumentKind::ColumnHint,
                    source_name: "fixture",
                    surface_kind: "table",
                    surface_name: "users",
                    field_name: "identity",
                    field_role: "table_column",
                    qualified_name: "fixture.users.identity",
                    title: "identity",
                    description: "",
                    searchable_text: "fixture users identity",
                }),
                document(DocumentInput {
                    doc_id: "z-exact-id",
                    doc_kind: CatalogIndexDocumentKind::ColumnHint,
                    source_name: "fixture",
                    surface_kind: "table",
                    surface_name: "users",
                    field_name: "id",
                    field_role: "table_column",
                    qualified_name: "fixture.users.id",
                    title: "id",
                    description: "",
                    searchable_text: "fixture users id",
                }),
            ],
        }
    }

    fn identifier_probe_snapshot() -> CatalogIndexSnapshot {
        let mut snapshot = identifier_priority_snapshot();
        snapshot.fingerprint = "identifier-probe-fixture-v1".to_string();
        snapshot.documents.push(document(DocumentInput {
            doc_id: "b-prefix-id-token",
            doc_kind: CatalogIndexDocumentKind::ColumnHint,
            source_name: "fixture",
            surface_kind: "table",
            surface_name: "users",
            field_name: "id_token",
            field_role: "table_column",
            qualified_name: "fixture.users.id_token",
            title: "id_token",
            description: "",
            searchable_text: "fixture users id_token",
        }));
        snapshot
    }

    fn underscore_column_snapshot() -> CatalogIndexSnapshot {
        CatalogIndexSnapshot {
            fingerprint: "underscore-fixture-v1".to_string(),
            documents: vec![
                document(DocumentInput {
                    doc_id: "column:table:github.users:user0id",
                    doc_kind: CatalogIndexDocumentKind::ColumnHint,
                    source_name: "github",
                    surface_kind: "table",
                    surface_name: "users",
                    field_name: "user0id",
                    field_role: "table_column",
                    qualified_name: "github.users.user0id",
                    title: "user0id",
                    description: "",
                    searchable_text: "github users user0id",
                }),
                document(DocumentInput {
                    doc_id: "column:table:github.users:user_id",
                    doc_kind: CatalogIndexDocumentKind::ColumnHint,
                    source_name: "github",
                    surface_kind: "table",
                    surface_name: "users",
                    field_name: "user_id",
                    field_role: "table_column",
                    qualified_name: "github.users.user_id",
                    title: "user_id",
                    description: "",
                    searchable_text: "github users user_id",
                }),
            ],
        }
    }

    fn field_name_match_snapshot() -> CatalogIndexSnapshot {
        CatalogIndexSnapshot {
            fingerprint: "field-name-match-fixture-v1".to_string(),
            documents: vec![document(DocumentInput {
                doc_id: "column:table:fixture.users:id",
                doc_kind: CatalogIndexDocumentKind::ColumnHint,
                source_name: "fixture",
                surface_kind: "table",
                surface_name: "users",
                field_name: "id",
                field_role: "table_column",
                qualified_name: "fixture.users.id",
                title: "primary key",
                description: "",
                searchable_text: "",
            })],
        }
    }

    #[derive(Clone, Copy)]
    struct DocumentInput<'a> {
        doc_id: &'a str,
        doc_kind: CatalogIndexDocumentKind,
        source_name: &'a str,
        surface_kind: &'a str,
        surface_name: &'a str,
        field_name: &'a str,
        field_role: &'a str,
        qualified_name: &'a str,
        title: &'a str,
        description: &'a str,
        searchable_text: &'a str,
    }

    fn document(input: DocumentInput<'_>) -> CatalogIndexDocument {
        CatalogIndexDocument {
            doc_id: input.doc_id.to_string(),
            doc_kind: input.doc_kind,
            source_name: input.source_name.to_string(),
            surface_kind: input.surface_kind.to_string(),
            surface_name: input.surface_name.to_string(),
            field_name: input.field_name.to_string(),
            field_role: input.field_role.to_string(),
            qualified_name: input.qualified_name.to_string(),
            title: input.title.to_string(),
            description: input.description.to_string(),
            searchable_text: input.searchable_text.to_string(),
            payload_json: "{}".to_string(),
        }
    }
}
