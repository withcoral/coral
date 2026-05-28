//! Workspace-scoped `SQLite` storage for Universal Search retrieval.

use std::path::PathBuf;
use std::time::Duration;

use coral_engine::{
    CatalogInfo, ColumnInfo, TableFunctionArgumentInfo, TableFunctionInfo,
    TableFunctionResultColumnInfo, TableInfo,
};
use rusqlite::{Connection, params};

use crate::sources::SourceName;
use crate::state::AppStateLayout;
use crate::storage::fs::ensure_dir;
use crate::workspaces::WorkspaceName;

pub(crate) const SEARCH_INDEX_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub(crate) struct SearchIndexStore {
    path: PathBuf,
    capabilities: SqliteSearchCapabilities,
}

impl SearchIndexStore {
    pub(crate) fn open_workspace(
        layout: &AppStateLayout,
        workspace_name: &WorkspaceName,
    ) -> Result<Self, SearchIndexError> {
        Self::open(layout.search_index_file(workspace_name))
    }

    pub(crate) fn open_existing_workspace(
        layout: &AppStateLayout,
        workspace_name: &WorkspaceName,
    ) -> Result<Option<Self>, SearchIndexError> {
        let path = layout.search_index_file(workspace_name);
        if !path.exists() {
            return Ok(None);
        }
        Self::open(path).map(Some)
    }

    pub(crate) fn open(path: impl Into<PathBuf>) -> Result<Self, SearchIndexError> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            ensure_dir(parent)?;
        }

        let mut connection = Connection::open(&path)?;
        configure_connection(&connection)?;
        let capabilities = detect_capabilities(&connection)?;
        ensure_supported(&capabilities)?;
        migrate(&mut connection)?;

        Ok(Self { path, capabilities })
    }

    pub(crate) fn connect(&self) -> Result<Connection, SearchIndexError> {
        let connection = Connection::open(&self.path)?;
        configure_connection(&connection)?;
        Ok(connection)
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub(crate) fn capabilities(&self) -> &SqliteSearchCapabilities {
        &self.capabilities
    }

    pub(crate) fn replace_catalog(
        &self,
        workspace_name: &WorkspaceName,
        catalog: &CatalogInfo,
    ) -> Result<(), SearchIndexError> {
        let records = catalog_entity_records(catalog);
        let mut connection = self.connect()?;
        let transaction = connection.transaction()?;

        transaction.execute(
            "DELETE FROM catalog_entities_fts WHERE workspace = ?1",
            params![workspace_name.as_str()],
        )?;
        transaction.execute(
            "DELETE FROM catalog_entities WHERE workspace = ?1",
            params![workspace_name.as_str()],
        )?;

        {
            let mut entity_insert = transaction.prepare(
                "
                INSERT INTO catalog_entities (
                    workspace,
                    entity_key,
                    result_type,
                    surface_kind,
                    schema_name,
                    surface_name,
                    name,
                    data_type,
                    is_required,
                    description,
                    updated_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                ",
            )?;
            let mut fts_insert = transaction.prepare(
                "
                INSERT INTO catalog_entities_fts (
                    workspace,
                    entity_key,
                    name,
                    qualified_name,
                    description,
                    searchable_text
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ",
            )?;

            for record in records {
                entity_insert.execute(params![
                    workspace_name.as_str(),
                    &record.entity_key,
                    record.result_type.as_str(),
                    record.surface_kind.as_str(),
                    &record.schema_name,
                    &record.surface_name,
                    &record.name,
                    &record.data_type,
                    i64::from(record.required),
                    &record.description,
                ])?;
                fts_insert.execute(params![
                    workspace_name.as_str(),
                    &record.entity_key,
                    &record.name,
                    &record.qualified_name,
                    &record.description,
                    &record.searchable_text,
                ])?;
            }
        }

        transaction.execute(
            "
            INSERT INTO search_index_meta (key, value, updated_at)
            VALUES (?1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            ",
            params![format!("catalog_refreshed_at:{}", workspace_name.as_str())],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn search_catalog(
        &self,
        workspace_name: &WorkspaceName,
        terms: &[String],
        limit: usize,
    ) -> Result<Vec<CatalogSearchHit>, SearchIndexError> {
        let Some(match_query) = fts_match_query(terms) else {
            return Ok(Vec::new());
        };
        let connection = self.connect()?;
        let mut statement = connection.prepare(
            "
            SELECT
                e.entity_key,
                e.result_type,
                e.surface_kind,
                e.schema_name,
                e.surface_name,
                e.name,
                e.data_type,
                e.is_required,
                e.description,
                f.name,
                f.qualified_name,
                f.description,
                f.searchable_text,
                bm25(catalog_entities_fts, 4.0, 5.0, 2.0, 1.0) AS rank
            FROM catalog_entities_fts f
            JOIN catalog_entities e
                ON e.workspace = f.workspace AND e.entity_key = f.entity_key
            WHERE f.workspace = ?1 AND catalog_entities_fts MATCH ?2
            ORDER BY rank ASC, e.result_type ASC, e.entity_key ASC
            LIMIT ?3
            ",
        )?;
        let rows = statement.query_map(
            params![
                workspace_name.as_str(),
                match_query,
                i64::try_from(limit).unwrap_or(i64::MAX),
            ],
            |row| {
                let result_type_raw: String = row.get(1)?;
                let surface_kind_raw: String = row.get(2)?;
                let name_field: String = row.get(9)?;
                let qualified_name: String = row.get(10)?;
                let description_field: String = row.get(11)?;
                let searchable_text: String = row.get(12)?;
                Ok(CatalogSearchHit {
                    entity_key: row.get(0)?,
                    result_type: CatalogSearchResultType::from_str(&result_type_raw),
                    surface_kind: CatalogSearchSurfaceKind::from_str(&surface_kind_raw),
                    schema_name: row.get(3)?,
                    surface_name: row.get(4)?,
                    name: row.get(5)?,
                    data_type: row.get(6)?,
                    required: row.get::<_, i64>(7)? != 0,
                    description: row.get(8)?,
                    matched_fields: matched_fields(
                        terms,
                        [
                            ("name", name_field.as_str()),
                            ("qualified_name", qualified_name.as_str()),
                            ("description", description_field.as_str()),
                            ("searchable_text", searchable_text.as_str()),
                        ],
                    ),
                    score: 0,
                })
            },
        )?;

        let mut hits = Vec::new();
        for row in rows {
            hits.push(row?);
        }
        let hit_count = u32::try_from(hits.len()).unwrap_or(u32::MAX);
        for (position, hit) in hits.iter_mut().enumerate() {
            let position = u32::try_from(position).unwrap_or(u32::MAX);
            hit.score = hit_count.saturating_sub(position);
        }
        Ok(hits)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Observed-value SQLite upsert keeps paired table/FTS writes in one transaction."
    )]
    pub(crate) fn upsert_observed_values(
        &self,
        workspace_name: &WorkspaceName,
        records: Vec<ObservedValueRecord>,
    ) -> Result<(), SearchIndexError> {
        if records.is_empty() {
            return Ok(());
        }

        let mut connection = self.connect()?;
        let transaction = connection.transaction()?;

        {
            let mut value_upsert = transaction.prepare(
                "
                INSERT INTO observed_values (
                    workspace,
                    source_name,
                    surface_kind,
                    surface_name,
                    column_name,
                    normalized_value_key,
                    display_value,
                    sensitivity_tier,
                    suggested_operator,
                    first_observed_at,
                    last_observed_at,
                    observed_count,
                    updated_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9,
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    ?10,
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                ON CONFLICT (
                    workspace,
                    source_name,
                    surface_kind,
                    surface_name,
                    column_name,
                    normalized_value_key
                )
                DO UPDATE SET
                    display_value = excluded.display_value,
                    sensitivity_tier = excluded.sensitivity_tier,
                    suggested_operator = excluded.suggested_operator,
                    last_observed_at = excluded.last_observed_at,
                    observed_count = observed_values.observed_count + excluded.observed_count,
                    updated_at = excluded.updated_at
                ",
            )?;
            let mut fts_delete = transaction.prepare(
                "
                DELETE FROM observed_values_fts
                WHERE workspace = ?1
                    AND source_name = ?2
                    AND surface_kind = ?3
                    AND surface_name = ?4
                    AND column_name = ?5
                    AND normalized_value_key = ?6
                ",
            )?;
            let mut fts_insert = transaction.prepare(
                "
                INSERT INTO observed_values_fts (
                    workspace,
                    source_name,
                    surface_kind,
                    surface_name,
                    column_name,
                    normalized_value_key,
                    display_value,
                    searchable_text
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                ",
            )?;

            for record in records {
                value_upsert.execute(params![
                    workspace_name.as_str(),
                    &record.source_name,
                    record.surface_kind.as_str(),
                    &record.surface_name,
                    &record.column_name,
                    &record.normalized_value_key,
                    &record.display_value,
                    record.sensitivity_tier.as_str(),
                    record.suggested_operator.as_str(),
                    i64::try_from(record.observed_count).unwrap_or(i64::MAX),
                ])?;
                fts_delete.execute(params![
                    workspace_name.as_str(),
                    &record.source_name,
                    record.surface_kind.as_str(),
                    &record.surface_name,
                    &record.column_name,
                    &record.normalized_value_key,
                ])?;
                fts_insert.execute(params![
                    workspace_name.as_str(),
                    &record.source_name,
                    record.surface_kind.as_str(),
                    &record.surface_name,
                    &record.column_name,
                    &record.normalized_value_key,
                    &record.display_value,
                    &record.searchable_text,
                ])?;
            }
        }

        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn search_observed_values(
        &self,
        workspace_name: &WorkspaceName,
        terms: &[String],
        limit: usize,
    ) -> Result<Vec<ObservedValueSearchHit>, SearchIndexError> {
        let Some(match_query) = fts_match_query(terms) else {
            return Ok(Vec::new());
        };
        let connection = self.connect()?;
        let mut statement = connection.prepare(
            "
            SELECT
                o.source_name,
                o.surface_name,
                o.column_name,
                o.normalized_value_key,
                o.display_value,
                o.last_observed_at,
                f.display_value,
                f.searchable_text,
                bm25(observed_values_fts, 4.0, 2.0) AS rank
            FROM observed_values_fts f
            JOIN observed_values o
                ON o.workspace = f.workspace
                AND o.source_name = f.source_name
                AND o.surface_kind = f.surface_kind
                AND o.surface_name = f.surface_name
                AND o.column_name = f.column_name
                AND o.normalized_value_key = f.normalized_value_key
            WHERE f.workspace = ?1 AND observed_values_fts MATCH ?2
            ORDER BY rank ASC, o.last_observed_at DESC, o.source_name ASC,
                o.surface_name ASC, o.column_name ASC
            LIMIT ?3
            ",
        )?;
        let rows = statement.query_map(
            params![
                workspace_name.as_str(),
                match_query,
                i64::try_from(limit).unwrap_or(i64::MAX),
            ],
            |row| {
                let display_field: String = row.get(6)?;
                let searchable_text: String = row.get(7)?;
                Ok(ObservedValueSearchHit {
                    source_name: row.get(0)?,
                    surface_name: row.get(1)?,
                    column_name: row.get(2)?,
                    normalized_value_key: row.get(3)?,
                    display_value: row.get(4)?,
                    last_observed_at: row.get(5)?,
                    matched_fields: matched_fields(
                        terms,
                        [
                            ("value", display_field.as_str()),
                            ("source_name", ""),
                            ("surface_name", ""),
                            ("column_name", ""),
                            ("searchable_text", searchable_text.as_str()),
                        ],
                    ),
                    score: 0,
                })
            },
        )?;

        let mut hits = Vec::new();
        for row in rows {
            let mut hit = row?;
            hit.matched_fields.extend(matched_fields(
                terms,
                [
                    ("source_name", hit.source_name.as_str()),
                    ("surface_name", hit.surface_name.as_str()),
                    ("column_name", hit.column_name.as_str()),
                ],
            ));
            hit.matched_fields.sort();
            hit.matched_fields.dedup();
            hits.push(hit);
        }
        let hit_count = u32::try_from(hits.len()).unwrap_or(u32::MAX);
        for (position, hit) in hits.iter_mut().enumerate() {
            let position = u32::try_from(position).unwrap_or(u32::MAX);
            hit.score = hit_count.saturating_sub(position);
        }
        Ok(hits)
    }

    pub(crate) fn delete_observed_values_for_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<(), SearchIndexError> {
        let mut connection = self.connect()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "
            DELETE FROM observed_values_fts
            WHERE workspace = ?1 AND source_name = ?2
            ",
            params![workspace_name.as_str(), source_name.as_str()],
        )?;
        transaction.execute(
            "
            DELETE FROM observed_values
            WHERE workspace = ?1 AND source_name = ?2
            ",
            params![workspace_name.as_str(), source_name.as_str()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn purge_observed_values_before(
        &self,
        workspace_name: &WorkspaceName,
        cutoff: &str,
    ) -> Result<(), SearchIndexError> {
        let mut connection = self.connect()?;
        let rows_to_purge = {
            let mut statement = connection.prepare(
                "
                SELECT source_name, surface_kind, surface_name, column_name, normalized_value_key
                FROM observed_values
                WHERE workspace = ?1 AND last_observed_at < ?2
                ",
            )?;
            let rows = statement.query_map(params![workspace_name.as_str(), cutoff], |row| {
                Ok(ObservedValueKey {
                    source_name: row.get(0)?,
                    surface_kind: row.get(1)?,
                    surface_name: row.get(2)?,
                    column_name: row.get(3)?,
                    normalized_value_key: row.get(4)?,
                })
            })?;
            let mut rows_to_purge = Vec::new();
            for row in rows {
                rows_to_purge.push(row?);
            }
            rows_to_purge
        };
        if rows_to_purge.is_empty() {
            return Ok(());
        }

        let transaction = connection.transaction()?;
        {
            let mut fts_delete = transaction.prepare(
                "
                DELETE FROM observed_values_fts
                WHERE workspace = ?1
                    AND source_name = ?2
                    AND surface_kind = ?3
                    AND surface_name = ?4
                    AND column_name = ?5
                    AND normalized_value_key = ?6
                ",
            )?;
            for row in &rows_to_purge {
                fts_delete.execute(params![
                    workspace_name.as_str(),
                    &row.source_name,
                    &row.surface_kind,
                    &row.surface_name,
                    &row.column_name,
                    &row.normalized_value_key,
                ])?;
            }
        }
        transaction.execute(
            "
            DELETE FROM observed_values
            WHERE workspace = ?1 AND last_observed_at < ?2
            ",
            params![workspace_name.as_str(), cutoff],
        )?;
        transaction.commit()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqliteSearchCapabilities {
    pub(crate) sqlite_version: String,
    pub(crate) fts5: bool,
    pub(crate) trigram: bool,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SearchIndexError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("SQLite {sqlite_version} does not support required search feature: {feature}")]
    UnsupportedCapability {
        feature: &'static str,
        sqlite_version: String,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogSearchHit {
    pub(crate) entity_key: String,
    pub(crate) result_type: Option<CatalogSearchResultType>,
    pub(crate) surface_kind: Option<CatalogSearchSurfaceKind>,
    pub(crate) schema_name: String,
    pub(crate) surface_name: String,
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) required: bool,
    pub(crate) description: String,
    pub(crate) matched_fields: Vec<String>,
    pub(crate) score: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogSearchResultType {
    CatalogTable,
    CatalogTableFunction,
    ColumnHint,
    NativeSearchPath,
}

impl CatalogSearchResultType {
    fn as_str(self) -> &'static str {
        match self {
            Self::CatalogTable => "catalog_table",
            Self::CatalogTableFunction => "catalog_table_function",
            Self::ColumnHint => "column_hint",
            Self::NativeSearchPath => "native_search_path",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "catalog_table" => Some(Self::CatalogTable),
            "catalog_table_function" => Some(Self::CatalogTableFunction),
            "column_hint" => Some(Self::ColumnHint),
            "native_search_path" => Some(Self::NativeSearchPath),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CatalogSearchSurfaceKind {
    Table,
    TableFunction,
}

#[derive(Debug, Clone)]
pub(crate) struct ObservedValueRecord {
    pub(crate) source_name: String,
    pub(crate) surface_kind: ObservedValueSurfaceKind,
    pub(crate) surface_name: String,
    pub(crate) column_name: String,
    pub(crate) normalized_value_key: String,
    pub(crate) display_value: String,
    pub(crate) searchable_text: String,
    pub(crate) sensitivity_tier: ObservedValueSensitivityTier,
    pub(crate) suggested_operator: ObservedValueSuggestedOperator,
    pub(crate) observed_count: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ObservedValueSearchHit {
    pub(crate) source_name: String,
    pub(crate) surface_name: String,
    pub(crate) column_name: String,
    pub(crate) normalized_value_key: String,
    pub(crate) display_value: String,
    pub(crate) last_observed_at: String,
    pub(crate) matched_fields: Vec<String>,
    pub(crate) score: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ObservedValueSurfaceKind {
    Table,
    TableFunction,
}

impl ObservedValueSurfaceKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::TableFunction => "table_function",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ObservedValueSensitivityTier {
    LowRisk,
}

impl ObservedValueSensitivityTier {
    const fn as_str(self) -> &'static str {
        match self {
            Self::LowRisk => "low_risk",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ObservedValueSuggestedOperator {
    Exact,
}

impl ObservedValueSuggestedOperator {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
        }
    }
}

#[derive(Debug)]
struct ObservedValueKey {
    source_name: String,
    surface_kind: String,
    surface_name: String,
    column_name: String,
    normalized_value_key: String,
}

impl CatalogSearchSurfaceKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::TableFunction => "table_function",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "table" => Some(Self::Table),
            "table_function" => Some(Self::TableFunction),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct CatalogEntityRecord {
    entity_key: String,
    result_type: CatalogSearchResultType,
    surface_kind: CatalogSearchSurfaceKind,
    schema_name: String,
    surface_name: String,
    name: String,
    qualified_name: String,
    data_type: String,
    required: bool,
    description: String,
    searchable_text: String,
}

fn configure_connection(connection: &Connection) -> Result<(), SearchIndexError> {
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    Ok(())
}

fn detect_capabilities(
    connection: &Connection,
) -> Result<SqliteSearchCapabilities, SearchIndexError> {
    let sqlite_version =
        connection.query_row("SELECT sqlite_version()", [], |row| row.get::<_, String>(0))?;
    let fts5 = connection
        .execute_batch(
            "
            CREATE VIRTUAL TABLE temp.coral_search_fts5_check USING fts5(value);
            DROP TABLE temp.coral_search_fts5_check;
            ",
        )
        .is_ok();
    let trigram = connection
        .execute_batch(
            "
            CREATE VIRTUAL TABLE temp.coral_search_trigram_check
            USING fts5(value, tokenize = 'trigram');
            DROP TABLE temp.coral_search_trigram_check;
            ",
        )
        .is_ok();

    Ok(SqliteSearchCapabilities {
        sqlite_version,
        fts5,
        trigram,
    })
}

fn ensure_supported(capabilities: &SqliteSearchCapabilities) -> Result<(), SearchIndexError> {
    if !capabilities.fts5 {
        return Err(SearchIndexError::UnsupportedCapability {
            feature: "FTS5",
            sqlite_version: capabilities.sqlite_version.clone(),
        });
    }
    if !capabilities.trigram {
        return Err(SearchIndexError::UnsupportedCapability {
            feature: "FTS5 trigram tokenizer",
            sqlite_version: capabilities.sqlite_version.clone(),
        });
    }
    Ok(())
}

fn migrate(connection: &mut Connection) -> Result<(), SearchIndexError> {
    let transaction = connection.transaction()?;
    transaction.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS search_index_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        );

        CREATE TABLE IF NOT EXISTS catalog_entities (
            workspace TEXT NOT NULL,
            entity_key TEXT NOT NULL,
            result_type TEXT NOT NULL,
            surface_kind TEXT NOT NULL,
            schema_name TEXT NOT NULL,
            surface_name TEXT NOT NULL,
            name TEXT NOT NULL,
            data_type TEXT NOT NULL DEFAULT '',
            is_required INTEGER NOT NULL DEFAULT 0,
            description TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            PRIMARY KEY (workspace, entity_key)
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS catalog_entities_fts USING fts5(
            workspace UNINDEXED,
            entity_key UNINDEXED,
            name,
            qualified_name,
            description,
            searchable_text,
            tokenize = 'trigram'
        );

        CREATE TABLE IF NOT EXISTS observed_values (
            workspace TEXT NOT NULL,
            source_name TEXT NOT NULL,
            surface_kind TEXT NOT NULL,
            surface_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            normalized_value_key TEXT NOT NULL,
            display_value TEXT NOT NULL DEFAULT '',
            sensitivity_tier TEXT NOT NULL DEFAULT 'low_risk',
            suggested_operator TEXT NOT NULL DEFAULT 'exact',
            first_observed_at TEXT NOT NULL,
            last_observed_at TEXT NOT NULL,
            observed_count INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            PRIMARY KEY (
                workspace,
                source_name,
                surface_kind,
                surface_name,
                column_name,
                normalized_value_key
            )
        );

        CREATE INDEX IF NOT EXISTS observed_values_last_observed_idx
            ON observed_values (workspace, last_observed_at);

        CREATE VIRTUAL TABLE IF NOT EXISTS observed_values_fts USING fts5(
            workspace UNINDEXED,
            source_name UNINDEXED,
            surface_kind UNINDEXED,
            surface_name UNINDEXED,
            column_name UNINDEXED,
            normalized_value_key UNINDEXED,
            display_value,
            searchable_text,
            tokenize = 'trigram'
        );

        ",
    )?;
    transaction.execute(
        "
        INSERT INTO search_index_meta (key, value, updated_at)
        SELECT 'schema_version', ?1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE NOT EXISTS (
            SELECT 1 FROM search_index_meta
            WHERE key = 'schema_version' AND value = ?1
        )
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        ",
        params![SEARCH_INDEX_SCHEMA_VERSION.to_string()],
    )?;
    let user_version: u32 = transaction.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if user_version != SEARCH_INDEX_SCHEMA_VERSION {
        transaction.pragma_update(None, "user_version", SEARCH_INDEX_SCHEMA_VERSION)?;
    }
    transaction.commit()?;
    Ok(())
}

fn catalog_entity_records(catalog: &CatalogInfo) -> Vec<CatalogEntityRecord> {
    let mut records = Vec::new();
    for table in &catalog.tables {
        table_entity_records(table, &mut records);
    }
    for function in &catalog.table_functions {
        table_function_entity_records(function, &mut records);
    }
    records
}

fn table_entity_records(table: &TableInfo, records: &mut Vec<CatalogEntityRecord>) {
    let qualified_name = qualified_name(&table.schema_name, &table.table_name);
    records.push(CatalogEntityRecord {
        entity_key: format!("catalog:table:{qualified_name}"),
        result_type: CatalogSearchResultType::CatalogTable,
        surface_kind: CatalogSearchSurfaceKind::Table,
        schema_name: table.schema_name.clone(),
        surface_name: table.table_name.clone(),
        name: table.table_name.clone(),
        qualified_name: qualified_name.clone(),
        data_type: String::new(),
        required: false,
        description: table.description.clone(),
        searchable_text: join_search_text([
            table.schema_name.as_str(),
            table.table_name.as_str(),
            qualified_name.as_str(),
            table.description.as_str(),
            table.guide.as_str(),
            table.required_filters.join(" ").as_str(),
        ]),
    });

    for column in &table.columns {
        table_column_record(table, column, records);
    }
    for filter in &table.required_filters {
        table_required_filter_record(table, filter, records);
    }
}

fn table_column_record(
    table: &TableInfo,
    column: &ColumnInfo,
    records: &mut Vec<CatalogEntityRecord>,
) {
    let surface_name = qualified_name(&table.schema_name, &table.table_name);
    records.push(CatalogEntityRecord {
        entity_key: format!("column:table:{surface_name}:{}", column.name),
        result_type: CatalogSearchResultType::ColumnHint,
        surface_kind: CatalogSearchSurfaceKind::Table,
        schema_name: table.schema_name.clone(),
        surface_name: table.table_name.clone(),
        name: column.name.clone(),
        qualified_name: format!("{surface_name}.{}", column.name),
        data_type: column.data_type.clone(),
        required: column.is_required_filter,
        description: column.description.clone(),
        searchable_text: join_search_text([
            table.schema_name.as_str(),
            table.table_name.as_str(),
            column.name.as_str(),
            column.data_type.as_str(),
            column.description.as_str(),
        ]),
    });
}

fn table_required_filter_record(
    table: &TableInfo,
    filter: &str,
    records: &mut Vec<CatalogEntityRecord>,
) {
    let surface_name = qualified_name(&table.schema_name, &table.table_name);
    records.push(CatalogEntityRecord {
        entity_key: format!("filter:table:{surface_name}:{filter}"),
        result_type: CatalogSearchResultType::ColumnHint,
        surface_kind: CatalogSearchSurfaceKind::Table,
        schema_name: table.schema_name.clone(),
        surface_name: table.table_name.clone(),
        name: filter.to_string(),
        qualified_name: format!("{surface_name}.{filter}"),
        data_type: String::new(),
        required: true,
        description: "Required table filter".to_string(),
        searchable_text: join_search_text([
            table.schema_name.as_str(),
            table.table_name.as_str(),
            filter,
            "required table filter",
        ]),
    });
}

fn table_function_entity_records(
    function: &TableFunctionInfo,
    records: &mut Vec<CatalogEntityRecord>,
) {
    let qualified_name = qualified_name(&function.schema_name, &function.function_name);
    let arguments = function
        .arguments
        .iter()
        .map(|argument| argument.name.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    let result_columns = function
        .result_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    records.push(CatalogEntityRecord {
        entity_key: format!("catalog:function:{qualified_name}"),
        result_type: CatalogSearchResultType::CatalogTableFunction,
        surface_kind: CatalogSearchSurfaceKind::TableFunction,
        schema_name: function.schema_name.clone(),
        surface_name: function.function_name.clone(),
        name: function.function_name.clone(),
        qualified_name: qualified_name.clone(),
        data_type: String::new(),
        required: false,
        description: function.description.clone(),
        searchable_text: join_search_text([
            function.schema_name.as_str(),
            function.function_name.as_str(),
            qualified_name.as_str(),
            function.description.as_str(),
            function.kind.as_str(),
            arguments.as_str(),
            result_columns.as_str(),
        ]),
    });

    if function.kind == "search" {
        records.push(CatalogEntityRecord {
            entity_key: format!("native_search:{qualified_name}"),
            result_type: CatalogSearchResultType::NativeSearchPath,
            surface_kind: CatalogSearchSurfaceKind::TableFunction,
            schema_name: function.schema_name.clone(),
            surface_name: function.function_name.clone(),
            name: function.function_name.clone(),
            qualified_name: qualified_name.clone(),
            data_type: String::new(),
            required: false,
            description: function.description.clone(),
            searchable_text: join_search_text([
                function.schema_name.as_str(),
                function.function_name.as_str(),
                qualified_name.as_str(),
                function.description.as_str(),
                "native search path source scoped table function",
                arguments.as_str(),
                result_columns.as_str(),
            ]),
        });
    }

    for argument in &function.arguments {
        table_function_argument_record(function, argument, records);
    }
    for column in &function.result_columns {
        table_function_result_column_record(function, column, records);
    }
}

fn table_function_argument_record(
    function: &TableFunctionInfo,
    argument: &TableFunctionArgumentInfo,
    records: &mut Vec<CatalogEntityRecord>,
) {
    let surface_name = qualified_name(&function.schema_name, &function.function_name);
    let values = argument.values.join(" ");
    records.push(CatalogEntityRecord {
        entity_key: format!("argument:function:{surface_name}:{}", argument.name),
        result_type: CatalogSearchResultType::ColumnHint,
        surface_kind: CatalogSearchSurfaceKind::TableFunction,
        schema_name: function.schema_name.clone(),
        surface_name: function.function_name.clone(),
        name: argument.name.clone(),
        qualified_name: format!("{surface_name}.{}", argument.name),
        data_type: String::new(),
        required: argument.required,
        description: "Table function argument".to_string(),
        searchable_text: join_search_text([
            function.schema_name.as_str(),
            function.function_name.as_str(),
            argument.name.as_str(),
            values.as_str(),
            "table function argument",
        ]),
    });
}

fn table_function_result_column_record(
    function: &TableFunctionInfo,
    column: &TableFunctionResultColumnInfo,
    records: &mut Vec<CatalogEntityRecord>,
) {
    let surface_name = qualified_name(&function.schema_name, &function.function_name);
    records.push(CatalogEntityRecord {
        entity_key: format!("result_column:function:{surface_name}:{}", column.name),
        result_type: CatalogSearchResultType::ColumnHint,
        surface_kind: CatalogSearchSurfaceKind::TableFunction,
        schema_name: function.schema_name.clone(),
        surface_name: function.function_name.clone(),
        name: column.name.clone(),
        qualified_name: format!("{surface_name}.{}", column.name),
        data_type: column.data_type.clone(),
        required: false,
        description: column.description.clone(),
        searchable_text: join_search_text([
            function.schema_name.as_str(),
            function.function_name.as_str(),
            column.name.as_str(),
            column.data_type.as_str(),
            column.description.as_str(),
            "table function result column",
        ]),
    });
}

fn qualified_name(schema_name: &str, surface_name: &str) -> String {
    format!("{schema_name}.{surface_name}")
}

fn join_search_text<const N: usize>(parts: [&str; N]) -> String {
    parts
        .into_iter()
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ")
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

fn matched_fields<const N: usize>(
    terms: &[String],
    fields: [(&'static str, &str); N],
) -> Vec<String> {
    let mut matched = fields
        .into_iter()
        .filter_map(|(field, value)| {
            let normalized = value.to_ascii_lowercase();
            terms
                .iter()
                .any(|term| normalized.contains(term.as_str()))
                .then_some(field.to_string())
        })
        .collect::<Vec<_>>();
    matched.sort();
    matched.dedup();
    matched
}

#[cfg(test)]
mod tests {
    use coral_engine::{
        CatalogInfo, TableFunctionArgumentInfo, TableFunctionInfo, TableFunctionResultColumnInfo,
    };
    use rusqlite::OptionalExtension as _;
    use tempfile::tempdir;

    use super::{
        CatalogSearchResultType, ObservedValueRecord, ObservedValueSensitivityTier,
        ObservedValueSuggestedOperator, ObservedValueSurfaceKind, SEARCH_INDEX_SCHEMA_VERSION,
        SearchIndexStore, fts_match_query,
    };
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;

    #[test]
    fn open_workspace_creates_search_index_schema() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");

        assert_eq!(store.path(), layout.search_index_file(&workspace));
        assert!(store.capabilities().fts5);
        assert!(store.capabilities().trigram);

        let connection = store.connect().expect("connect");
        let user_version: u32 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("user_version");
        assert_eq!(user_version, SEARCH_INDEX_SCHEMA_VERSION);

        let schema_version = connection
            .query_row(
                "SELECT value FROM search_index_meta WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .expect("schema_version query")
            .expect("schema_version");
        assert_eq!(schema_version, SEARCH_INDEX_SCHEMA_VERSION.to_string());
    }

    #[test]
    fn catalog_fts_supports_bm25_trigram_matches() {
        let temp = tempdir().expect("tempdir");
        let store = SearchIndexStore::open(temp.path().join("search.sqlite")).expect("store");
        let connection = store.connect().expect("connect");
        connection
            .execute(
                "
                INSERT INTO catalog_entities_fts (
                    workspace,
                    entity_key,
                    name,
                    qualified_name,
                    description,
                    searchable_text
                )
                VALUES ('default', 'function:github.search_commits', 'search_commits',
                    'github.search_commits', 'Search commits', 'github commit sha')
                ",
                [],
            )
            .expect("insert fts row");

        let row_count: u32 = connection
            .query_row(
                "
                SELECT count(*)
                FROM catalog_entities_fts
                WHERE catalog_entities_fts MATCH '\"commit\"'
                ORDER BY bm25(catalog_entities_fts)
                ",
                [],
                |row| row.get(0),
            )
            .expect("fts query");
        assert_eq!(row_count, 1);
    }

    #[test]
    fn replace_catalog_indexes_function_metadata() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::open(temp.path().join("search.sqlite")).expect("store");
        store
            .replace_catalog(&workspace, &catalog_with_search_function())
            .expect("replace catalog");

        let hits = store
            .search_catalog(
                &workspace,
                &[
                    "github".to_string(),
                    "deployments".to_string(),
                    "sha".to_string(),
                ],
                10,
            )
            .expect("search catalog");

        assert!(hits.iter().any(|hit| hit.result_type
            == Some(CatalogSearchResultType::NativeSearchPath)
            && hit.surface_name == "search_deployments"));
        assert!(hits.iter().any(|hit| hit.result_type
            == Some(CatalogSearchResultType::ColumnHint)
            && hit.name == "sha"));

        store
            .replace_catalog(
                &workspace,
                &CatalogInfo {
                    tables: Vec::new(),
                    table_functions: Vec::new(),
                },
            )
            .expect("replace empty catalog");
        let hits = store
            .search_catalog(&workspace, &["github".to_string()], 10)
            .expect("search empty catalog");
        assert!(hits.is_empty());
    }

    #[test]
    fn fts_match_query_quotes_technical_terms() {
        assert_eq!(
            fts_match_query(&["github.search_commits".to_string(), "id".to_string()])
                .expect("query"),
            "\"github.search_commits\""
        );
    }

    #[test]
    fn observed_values_upsert_count_and_search() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::open(temp.path().join("search.sqlite")).expect("store");

        store
            .upsert_observed_values(
                &workspace,
                vec![
                    observed_record("payments-api", 2),
                    observed_record("payments-api", 1),
                ],
            )
            .expect("upsert observed");

        let hits = store
            .search_observed_values(&workspace, &["payments-api".to_string()], 10)
            .expect("search observed");
        assert_eq!(hits.len(), 1);
        let hit = hits.first().expect("observed hit");
        assert_eq!(hit.column_name, "service");
        let observed_count: i64 = store
            .connect()
            .expect("connect")
            .query_row(
                "
                SELECT observed_count
                FROM observed_values
                WHERE workspace = 'default' AND display_value = 'payments-api'
                ",
                [],
                |row| row.get(0),
            )
            .expect("observed count");
        assert_eq!(observed_count, 3);
    }

    #[test]
    fn observed_values_purge_by_source_and_last_observed_at() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = crate::sources::SourceName::parse("notion").expect("source");
        let store = SearchIndexStore::open(temp.path().join("search.sqlite")).expect("store");

        store
            .upsert_observed_values(
                &workspace,
                vec![
                    observed_record("stale-value", 1),
                    ObservedValueRecord {
                        source_name: "notion".to_string(),
                        display_value: "notion-value".to_string(),
                        searchable_text: "notion page notion-value".to_string(),
                        ..observed_record("notion-value", 1)
                    },
                ],
            )
            .expect("upsert observed");
        store
            .connect()
            .expect("connect")
            .execute(
                "
                UPDATE observed_values
                SET last_observed_at = '2000-01-01T00:00:00.000Z'
                WHERE source_name = 'github'
                ",
                [],
            )
            .expect("age observed value");

        store
            .purge_observed_values_before(&workspace, "2001-01-01T00:00:00.000Z")
            .expect("purge stale");
        assert!(
            store
                .search_observed_values(&workspace, &["stale-value".to_string()], 10)
                .expect("search stale")
                .is_empty()
        );
        assert!(
            !store
                .search_observed_values(&workspace, &["notion-value".to_string()], 10)
                .expect("search fresh")
                .is_empty()
        );

        store
            .delete_observed_values_for_source(&workspace, &source)
            .expect("delete source values");
        assert!(
            store
                .search_observed_values(&workspace, &["notion-value".to_string()], 10)
                .expect("search deleted")
                .is_empty()
        );
    }

    fn catalog_with_search_function() -> CatalogInfo {
        CatalogInfo {
            tables: Vec::new(),
            table_functions: vec![TableFunctionInfo {
                schema_name: "github".to_string(),
                function_name: "search_deployments".to_string(),
                description: "Search GitHub deployments".to_string(),
                arguments: vec![TableFunctionArgumentInfo {
                    name: "q".to_string(),
                    required: true,
                    values: Vec::new(),
                }],
                result_columns: vec![TableFunctionResultColumnInfo {
                    name: "sha".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: false,
                    description: "Deployment commit SHA".to_string(),
                }],
                kind: "search".to_string(),
                search_limits_json: None,
            }],
        }
    }

    fn observed_record(value: &str, observed_count: u64) -> ObservedValueRecord {
        ObservedValueRecord {
            source_name: "github".to_string(),
            surface_kind: ObservedValueSurfaceKind::Table,
            surface_name: "deployments".to_string(),
            column_name: "service".to_string(),
            normalized_value_key: value.to_ascii_lowercase(),
            display_value: value.to_string(),
            searchable_text: format!("github deployments service {value}"),
            sensitivity_tier: ObservedValueSensitivityTier::LowRisk,
            suggested_operator: ObservedValueSuggestedOperator::Exact,
            observed_count,
        }
    }
}
