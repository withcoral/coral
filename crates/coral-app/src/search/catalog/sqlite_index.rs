//! `SQLite` catalog metadata projection and retrieval primitives.

use std::collections::BTreeMap;

use rusqlite::{
    Connection, OptionalExtension as _, Transaction, TransactionBehavior, params, types::Type,
};
use serde::Deserialize;

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
    pub(crate) fn rebuild(
        &self,
        connection: &mut Connection,
        workspace_name: &WorkspaceName,
        snapshot: &CatalogIndexSnapshot,
        force: bool,
    ) -> Result<CatalogRebuildResult, SqliteSearchError> {
        rebuild_catalog_documents(connection, workspace_name, snapshot, force)
    }

    #[expect(
        clippy::unused_self,
        reason = "kept as an instance method so catalog provider can own index capability consistently"
    )]
    pub(crate) fn projection_is_current(
        &self,
        connection: &Connection,
        workspace_name: &WorkspaceName,
        fingerprint: &str,
    ) -> Result<bool, SqliteSearchError> {
        Ok(catalog_fingerprint(connection, workspace_name)?.as_deref() == Some(fingerprint))
    }

    #[expect(
        clippy::unused_self,
        reason = "kept as an instance method so catalog provider can own index capability consistently"
    )]
    pub(crate) fn document_count(
        &self,
        connection: &Connection,
        workspace_name: &WorkspaceName,
    ) -> Result<u32, SqliteSearchError> {
        catalog_document_count(connection, workspace_name)
    }

    #[expect(
        clippy::unused_self,
        reason = "kept as an instance method so catalog provider can own index capability consistently"
    )]
    pub(crate) fn clear_workspace(
        &self,
        connection: &mut Connection,
        workspace_name: &WorkspaceName,
    ) -> Result<CatalogClearResult, SqliteSearchError> {
        clear_catalog_workspace_documents(connection, workspace_name)
    }

    #[expect(
        clippy::unused_self,
        reason = "kept as an instance method so catalog provider can own index capability consistently"
    )]
    pub(crate) fn clear_source(
        &self,
        connection: &mut Connection,
        workspace_name: &WorkspaceName,
        source_name: &str,
    ) -> Result<CatalogClearResult, SqliteSearchError> {
        clear_catalog_source_documents(connection, workspace_name, source_name)
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

fn rebuild_catalog_documents(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    snapshot: &CatalogIndexSnapshot,
    force: bool,
) -> Result<CatalogRebuildResult, SqliteSearchError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current_fingerprint = catalog_fingerprint(&transaction, workspace_name)?;
    let old_document_count = catalog_document_count(&transaction, workspace_name)?;
    let projection_changed = current_fingerprint.as_deref() != Some(snapshot.fingerprint.as_str());
    let rebuild_performed = force || projection_changed;

    if rebuild_performed {
        replace_catalog_documents(&transaction, workspace_name, snapshot)?;
    }

    let new_document_count = if rebuild_performed {
        catalog_document_count(&transaction, workspace_name)?
    } else {
        old_document_count
    };
    transaction.commit()?;
    Ok(CatalogRebuildResult {
        old_document_count,
        new_document_count,
        projection_changed,
        rebuild_performed,
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
    pub(crate) owner_source_name: String,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogRebuildResult {
    pub(crate) old_document_count: u32,
    pub(crate) new_document_count: u32,
    pub(crate) projection_changed: bool,
    pub(crate) rebuild_performed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogClearResult {
    pub(crate) deleted_document_count: u32,
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
    pub(crate) catalog_name: Option<String>,
    pub(crate) schema_name: String,
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
    delete_catalog_projection_rows(transaction, workspace_name)?;
    insert_catalog_source_owners(transaction, workspace_name, snapshot)?;
    insert_catalog_snapshot_documents(transaction, workspace_name, snapshot)?;
    set_catalog_fingerprint(transaction, workspace_name, &snapshot.fingerprint)?;
    Ok(())
}

fn delete_catalog_projection_rows(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
) -> Result<(), SqliteSearchError> {
    transaction.execute(
        "DELETE FROM catalog_documents_fts WHERE workspace = ?1",
        params![workspace_name.as_str()],
    )?;
    transaction.execute(
        "DELETE FROM catalog_documents WHERE workspace = ?1",
        params![workspace_name.as_str()],
    )?;
    transaction.execute(
        "DELETE FROM catalog_source_owners WHERE workspace = ?1",
        params![workspace_name.as_str()],
    )?;
    Ok(())
}

fn insert_catalog_source_owners(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    snapshot: &CatalogIndexSnapshot,
) -> Result<(), SqliteSearchError> {
    let mut insert = transaction.prepare(
        "
        INSERT INTO catalog_source_owners (
            workspace,
            source_name,
            owner_source_name,
            snapshot_fingerprint,
            updated_at
        )
        VALUES (?1, ?2, ?3, ?4, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        ON CONFLICT(workspace, source_name) DO UPDATE SET
            owner_source_name = excluded.owner_source_name,
            snapshot_fingerprint = excluded.snapshot_fingerprint,
            updated_at = excluded.updated_at
        ",
    )?;
    let mut source_owners = BTreeMap::new();
    for document in &snapshot.documents {
        source_owners.insert(
            document.source_name.as_str(),
            document.owner_source_name.as_str(),
        );
    }
    for (source_name, owner_source_name) in source_owners {
        insert.execute(params![
            workspace_name.as_str(),
            source_name,
            owner_source_name,
            &snapshot.fingerprint,
        ])?;
    }
    Ok(())
}

fn insert_catalog_snapshot_documents(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    snapshot: &CatalogIndexSnapshot,
) -> Result<(), SqliteSearchError> {
    let mut document_insert = transaction.prepare(
        "
        INSERT INTO catalog_documents (
            workspace, doc_id, doc_kind, source_name, surface_kind, surface_name,
            field_name, field_role, qualified_name, title, description, payload_json,
            snapshot_fingerprint, updated_at
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
            strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        ",
    )?;
    let mut fts_insert = transaction.prepare(
        "
        INSERT INTO catalog_documents_fts (
            workspace, doc_id, title, qualified_name, description, searchable_text
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ",
    )?;

    for document in &snapshot.documents {
        let searchable_text = fts_searchable_text(document);
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
            &searchable_text,
        ])?;
    }
    Ok(())
}

fn set_catalog_fingerprint(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    fingerprint: &str,
) -> Result<(), SqliteSearchError> {
    transaction.execute(
        "
        INSERT INTO search_meta (key, value, updated_at)
        VALUES (?1, ?2, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        ",
        params![catalog_fingerprint_key(workspace_name), fingerprint],
    )?;
    Ok(())
}

fn clear_catalog_workspace_documents(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
) -> Result<CatalogClearResult, SqliteSearchError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let result = clear_catalog_workspace_documents_in_transaction(&transaction, workspace_name)?;
    transaction.commit()?;
    Ok(result)
}

pub(crate) fn clear_catalog_workspace_documents_in_transaction(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
) -> Result<CatalogClearResult, SqliteSearchError> {
    transaction.execute(
        "DELETE FROM catalog_documents_fts WHERE workspace = ?1",
        params![workspace_name.as_str()],
    )?;
    let deleted_document_count = transaction.execute(
        "DELETE FROM catalog_documents WHERE workspace = ?1",
        params![workspace_name.as_str()],
    )?;
    transaction.execute(
        "DELETE FROM catalog_source_owners WHERE workspace = ?1",
        params![workspace_name.as_str()],
    )?;
    clear_catalog_fingerprint(transaction, workspace_name)?;
    Ok(CatalogClearResult {
        deleted_document_count: u32::try_from(deleted_document_count).unwrap_or(u32::MAX),
    })
}

fn clear_catalog_source_documents(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    source_name: &str,
) -> Result<CatalogClearResult, SqliteSearchError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let result =
        clear_catalog_source_documents_in_transaction(&transaction, workspace_name, source_name)?;
    transaction.commit()?;
    Ok(result)
}

pub(crate) fn clear_catalog_source_documents_in_transaction(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    owner_source_name: &str,
) -> Result<CatalogClearResult, SqliteSearchError> {
    transaction.execute(
        "
        DELETE FROM catalog_documents_fts
        WHERE workspace = ?1
          AND doc_id IN (
              SELECT documents.doc_id
              FROM catalog_documents AS documents
              INNER JOIN catalog_source_owners AS owners
                  ON owners.workspace = documents.workspace
                 AND owners.source_name = documents.source_name
              WHERE documents.workspace = ?1
                AND owners.owner_source_name = ?2
          )
        ",
        params![workspace_name.as_str(), owner_source_name],
    )?;
    let deleted_document_count = transaction.execute(
        "
        DELETE FROM catalog_documents
        WHERE workspace = ?1
          AND source_name IN (
              SELECT source_name
              FROM catalog_source_owners
              WHERE workspace = ?1 AND owner_source_name = ?2
          )
        ",
        params![workspace_name.as_str(), owner_source_name],
    )?;
    transaction.execute(
        "
        DELETE FROM catalog_source_owners
        WHERE workspace = ?1 AND owner_source_name = ?2
        ",
        params![workspace_name.as_str(), owner_source_name],
    )?;
    clear_catalog_fingerprint(transaction, workspace_name)?;
    Ok(CatalogClearResult {
        deleted_document_count: u32::try_from(deleted_document_count).unwrap_or(u32::MAX),
    })
}

fn clear_catalog_fingerprint(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
) -> Result<(), SqliteSearchError> {
    transaction.execute(
        "DELETE FROM search_meta WHERE key = ?1",
        params![catalog_fingerprint_key(workspace_name)],
    )?;
    Ok(())
}

fn catalog_fingerprint(
    connection: &Connection,
    workspace_name: &WorkspaceName,
) -> Result<Option<String>, SqliteSearchError> {
    let fingerprint = connection
        .query_row(
            "SELECT value FROM search_meta WHERE key = ?1",
            params![catalog_fingerprint_key(workspace_name)],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let Some(fingerprint) = fingerprint else {
        return Ok(None);
    };
    let ownership_is_incomplete: bool = connection.query_row(
        "
        SELECT
            EXISTS (
                SELECT 1
                FROM catalog_documents AS documents
                LEFT JOIN catalog_source_owners AS owners
                    ON owners.workspace = documents.workspace
                   AND owners.source_name = documents.source_name
                WHERE documents.workspace = ?1
                  AND (
                      owners.source_name IS NULL
                      OR documents.snapshot_fingerprint <> ?2
                      OR owners.snapshot_fingerprint <> ?2
                  )
            )
            OR EXISTS (
                SELECT 1
                FROM catalog_source_owners
                WHERE workspace = ?1 AND snapshot_fingerprint <> ?2
            )
        ",
        params![workspace_name.as_str(), &fingerprint],
        |row| row.get(0),
    )?;
    if ownership_is_incomplete {
        Ok(None)
    } else {
        Ok(Some(fingerprint))
    }
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
            f.searchable_text,
            d.payload_json
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
            '',
            d.payload_json
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
    let doc_kind = doc_kind_from_storage(&doc_kind_raw)?;
    let surface_kind_raw: String = row.get(3)?;
    let surface_kind = surface_kind_from_storage(&surface_kind_raw)?;
    let title_field: String = row.get(10)?;
    let qualified_name_field: String = row.get(11)?;
    let description_field: String = row.get(12)?;
    let searchable_text: String = row.get(13)?;
    let field_name: String = row.get(5)?;
    let surface_name: String = row.get(4)?;
    let field_role_raw: String = row.get(6)?;
    let field_role = field_role_from_storage(&field_role_raw)?;
    let source_name: String = row.get(2)?;
    let payload_json: String = row.get(14)?;
    let identity =
        serde_json::from_str::<CatalogDocumentIdentity>(&payload_json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(14, Type::Text, Box::new(error))
        })?;
    let schema_name = if identity.schema_name.is_empty() {
        source_name.clone()
    } else {
        identity.schema_name
    };
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
        source_name,
        catalog_name: identity.catalog_name,
        schema_name,
        surface_kind,
        surface_name,
        field_name,
        field_role,
        description: row.get(9)?,
        matched_fields,
        retrieval_score: base_score,
    })
}

#[derive(Debug, Default, Deserialize)]
struct CatalogDocumentIdentity {
    #[serde(default)]
    catalog_name: Option<String>,
    #[serde(default)]
    schema_name: String,
}

fn surface_kind_from_storage(value: &str) -> rusqlite::Result<String> {
    match value {
        "" | "table" | "table_function" => Ok(value.to_string()),
        _ => invalid_catalog_storage_value(3, "surface_kind", value),
    }
}

fn field_role_from_storage(value: &str) -> rusqlite::Result<String> {
    match value {
        ""
        | "table_column"
        | "table_filter"
        | "table_function_argument"
        | "table_function_result_column" => Ok(value.to_string()),
        _ => invalid_catalog_storage_value(6, "field_role", value),
    }
}

fn doc_kind_from_storage(value: &str) -> rusqlite::Result<CatalogIndexDocumentKind> {
    CatalogIndexDocumentKind::from_str(value)
        .ok_or_else(|| invalid_catalog_storage_error(1, "doc_kind", value))
}

fn invalid_catalog_storage_value<T>(
    column: usize,
    field: &'static str,
    value: &str,
) -> rusqlite::Result<T> {
    Err(invalid_catalog_storage_error(column, field, value))
}

fn invalid_catalog_storage_error(
    column: usize,
    field: &'static str,
    value: &str,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column,
        Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unknown catalog search {field} '{value}'"),
        )),
    )
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
    let mut normalized = Vec::new();
    for term in terms {
        let term = term.trim().to_lowercase();
        if term.is_empty() {
            continue;
        }
        push_search_term(&mut normalized, term.clone());
        if let Some(compact) = compact_identifier_variant(&term) {
            push_search_term(&mut normalized, compact);
        }
    }
    normalized
}

fn push_search_term(terms: &mut Vec<String>, term: String) {
    if !terms.iter().any(|existing| existing == &term) {
        terms.push(term);
    }
}

fn fts_searchable_text(document: &CatalogIndexDocument) -> String {
    let mut parts = vec![document.searchable_text.clone()];
    for value in [
        document.source_name.as_str(),
        document.surface_name.as_str(),
        document.field_name.as_str(),
        document.qualified_name.as_str(),
        document.title.as_str(),
    ] {
        if let Some(compact) = compact_identifier_variant(value) {
            parts.push(compact);
        }
    }
    parts
        .into_iter()
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn compact_identifier_variant(value: &str) -> Option<String> {
    let normalized = value.trim().to_lowercase();
    let compact = normalized
        .chars()
        .filter(|ch| ch.is_alphanumeric())
        .collect::<String>();
    (!compact.is_empty() && compact != normalized).then_some(compact)
}

fn matched_fields(terms: &[String], fields: &[(&'static str, &str)]) -> Vec<String> {
    let mut matched = fields
        .iter()
        .filter_map(|(field, value)| {
            let normalized = value.to_lowercase();
            terms
                .iter()
                .any(|term| field_matches_term(&normalized, term))
                .then_some((*field).to_string())
        })
        .collect::<Vec<_>>();
    matched.sort();
    matched.dedup();
    matched
}

fn field_matches_term(normalized: &str, term: &str) -> bool {
    normalized.contains(term)
        || compact_identifier_variant(normalized)
            .as_deref()
            .is_some_and(|compact| compact.contains(term))
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
mod tests;
