//! Workspace-scoped `SQLite` storage for Universal Search.

#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "SQLite search substrate is wired by follow-up catalog provider PR"
    )
)]

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{Connection, ErrorCode, TransactionBehavior};

use crate::search::catalog::sqlite_index::{
    CatalogClearResult, CatalogIndexSnapshot, CatalogRebuildResult, CatalogRefreshResult,
    CatalogSearchHits, SqliteCatalogIndex, clear_catalog_source_documents_in_transaction,
};
use crate::search::observed::{ObservedValuesClearResult, clear_observed_source_in_transaction};
use crate::state::AppStateLayout;
use crate::storage::fs::create_new_file_private;
use crate::workspaces::WorkspaceName;

pub(crate) const SEARCH_SQLITE_SCHEMA_VERSION: u32 = 2;

struct SearchSqliteMigration {
    version: u32,
    sql: &'static str,
}

// These migrations are intentionally local to the SQLite search sidecar. The
// schema uses SQLite FTS5/trigram features that are not portable to the shared
// app database migration stream.
const SEARCH_SQLITE_MIGRATIONS: &[SearchSqliteMigration] = &[
    SearchSqliteMigration {
        version: 1,
        sql: include_str!("migrations/0001_catalog_search.sql"),
    },
    SearchSqliteMigration {
        version: 2,
        sql: include_str!("migrations/0002_observed_values.sql"),
    },
];

#[derive(Debug, Clone)]
pub(crate) struct SqliteSearchStore {
    path: PathBuf,
    workspace_name: WorkspaceName,
    capabilities: SqliteSearchCapabilities,
}

impl SqliteSearchStore {
    pub(crate) fn open_workspace(
        layout: &AppStateLayout,
        workspace_name: &WorkspaceName,
    ) -> Result<Self, SqliteSearchError> {
        Self::open_at(
            layout.search_sqlite_file(workspace_name),
            workspace_name.clone(),
        )
    }

    #[cfg(test)]
    pub(crate) fn open(
        path: impl Into<PathBuf>,
        workspace_name: WorkspaceName,
    ) -> Result<Self, SqliteSearchError> {
        Self::open_at(path, workspace_name)
    }

    fn open_at(
        path: impl Into<PathBuf>,
        workspace_name: WorkspaceName,
    ) -> Result<Self, SqliteSearchError> {
        let path = path.into();
        ensure_sqlite_file(&path)?;

        let mut connection = Connection::open(&path)?;
        configure_connection(&connection)?;
        let capabilities = detect_capabilities(&connection)?;
        ensure_supported(&capabilities)?;
        migrate_if_needed(&mut connection)?;

        Ok(Self {
            path,
            workspace_name,
            capabilities,
        })
    }

    pub(super) fn connect(&self) -> Result<Connection, SqliteSearchError> {
        let connection = Connection::open(&self.path)?;
        configure_connection(&connection)?;
        Ok(connection)
    }

    #[cfg(test)]
    pub(crate) fn connect_for_test(&self) -> Result<Connection, SqliteSearchError> {
        self.connect()
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub(crate) fn capabilities(&self) -> &SqliteSearchCapabilities {
        &self.capabilities
    }

    pub(crate) fn catalog_projection_is_current(
        &self,
        fingerprint: &str,
    ) -> Result<bool, SqliteSearchError> {
        let connection = self.connect()?;
        SqliteCatalogIndex::new().projection_is_current(
            &connection,
            &self.workspace_name,
            fingerprint,
        )
    }

    pub(crate) fn refresh_catalog_projection(
        &self,
        snapshot: &CatalogIndexSnapshot,
    ) -> Result<CatalogRefreshResult, SqliteSearchError> {
        let mut connection = self.connect()?;
        SqliteCatalogIndex::new().refresh(&mut connection, &self.workspace_name, snapshot)
    }

    pub(crate) fn rebuild_catalog_projection(
        &self,
        snapshot: &CatalogIndexSnapshot,
        force: bool,
    ) -> Result<CatalogRebuildResult, SqliteSearchError> {
        let mut connection = self.connect()?;
        SqliteCatalogIndex::new().rebuild(&mut connection, &self.workspace_name, snapshot, force)
    }

    pub(crate) fn catalog_document_count(&self) -> Result<u32, SqliteSearchError> {
        let connection = self.connect()?;
        SqliteCatalogIndex::new().document_count(&connection, &self.workspace_name)
    }

    pub(crate) fn clear_catalog_workspace(&self) -> Result<CatalogClearResult, SqliteSearchError> {
        let mut connection = self.connect()?;
        SqliteCatalogIndex::new().clear_workspace(&mut connection, &self.workspace_name)
    }

    pub(crate) fn clear_catalog_source(
        &self,
        source_name: &str,
    ) -> Result<CatalogClearResult, SqliteSearchError> {
        let mut connection = self.connect()?;
        SqliteCatalogIndex::new().clear_source(&mut connection, &self.workspace_name, source_name)
    }

    pub(crate) fn clear_source_all(
        &self,
        source_name: &str,
    ) -> Result<(CatalogClearResult, ObservedValuesClearResult), SqliteSearchError> {
        let mut connection = self.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let catalog = clear_catalog_source_documents_in_transaction(
            &transaction,
            &self.workspace_name,
            source_name,
        )?;
        let observed =
            clear_observed_source_in_transaction(&transaction, &self.workspace_name, source_name)?;
        transaction.commit()?;
        Ok((catalog, observed))
    }

    pub(crate) fn compact_after_clear(&self) -> SqliteSearchCompactionResult {
        let mut notes = Vec::new();
        let wal_checkpoint_truncate_completed = match self.wal_checkpoint_truncate() {
            Ok(WalCheckpointOutcome::Completed) => true,
            Ok(WalCheckpointOutcome::Busy {
                log_frame_count,
                checkpointed_frame_count,
            }) => {
                notes.push(format!(
                        "WAL checkpoint/truncate did not complete because a reader is active (log frames: {log_frame_count}, checkpointed frames: {checkpointed_frame_count})"
                    ));
                false
            }
            Err(error) => {
                notes.push(format!("WAL checkpoint/truncate failed: {error}"));
                false
            }
        };
        let vacuum_completed = match self.execute_maintenance_batch("VACUUM;") {
            Ok(()) => true,
            Err(error) => {
                notes.push(format!("VACUUM failed: {error}"));
                false
            }
        };

        let note = if notes.is_empty() {
            "WAL checkpoint/truncate and VACUUM completed".to_string()
        } else {
            notes.join("; ")
        };
        SqliteSearchCompactionResult {
            wal_checkpoint_truncate_completed,
            vacuum_completed,
            note,
        }
    }

    fn wal_checkpoint_truncate(&self) -> Result<WalCheckpointOutcome, SqliteSearchError> {
        let connection = self.connect()?;
        wal_checkpoint_truncate(&connection)
    }

    fn execute_maintenance_batch(&self, sql: &str) -> Result<(), SqliteSearchError> {
        let connection = self.connect()?;
        connection.execute_batch(sql)?;
        Ok(())
    }

    pub(crate) fn search_catalog(
        &self,
        terms: &[String],
        limit: usize,
    ) -> Result<CatalogSearchHits, SqliteSearchError> {
        let connection = self.connect()?;
        SqliteCatalogIndex::new().search(&connection, &self.workspace_name, terms, limit)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqliteSearchCapabilities {
    pub(crate) sqlite_version: String,
    pub(crate) fts5: bool,
    pub(crate) trigram: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqliteSearchCompactionResult {
    pub(crate) wal_checkpoint_truncate_completed: bool,
    pub(crate) vacuum_completed: bool,
    pub(crate) note: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalCheckpointOutcome {
    Completed,
    Busy {
        log_frame_count: i64,
        checkpointed_frame_count: i64,
    },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SqliteSearchError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("SQLite {sqlite_version} does not support required search feature: {feature}")]
    UnsupportedCapability {
        feature: &'static str,
        sqlite_version: String,
    },
    #[error(
        "SQLite search schema version {database_version} is newer than this binary supports ({supported_version})"
    )]
    UnsupportedSchemaVersion {
        database_version: u32,
        supported_version: u32,
    },
    #[error("SQLite search schema is incomplete after rebuilding version {schema_version}")]
    IncompleteSchemaAfterRebuild { schema_version: u32 },
}

impl SqliteSearchError {
    pub(crate) fn is_lock_contention(&self) -> bool {
        matches!(
            self,
            Self::Sqlite(error)
                if matches!(
                    error.sqlite_error_code(),
                    Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
                )
        )
    }

    pub(crate) fn is_storage_exhaustion(&self) -> bool {
        match self {
            Self::Io(error) => matches!(
                error.kind(),
                io::ErrorKind::StorageFull
                    | io::ErrorKind::QuotaExceeded
                    | io::ErrorKind::OutOfMemory
                    | io::ErrorKind::FileTooLarge
            ),
            Self::Sqlite(error) => matches!(
                error.sqlite_error_code(),
                Some(ErrorCode::DiskFull | ErrorCode::OutOfMemory | ErrorCode::TooBig)
            ),
            Self::UnsupportedCapability { .. }
            | Self::UnsupportedSchemaVersion { .. }
            | Self::IncompleteSchemaAfterRebuild { .. } => false,
        }
    }
}

fn wal_checkpoint_truncate(
    connection: &Connection,
) -> Result<WalCheckpointOutcome, SqliteSearchError> {
    let (busy, log_frame_count, checkpointed_frame_count) =
        connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
    if busy == 0 {
        Ok(WalCheckpointOutcome::Completed)
    } else {
        Ok(WalCheckpointOutcome::Busy {
            log_frame_count,
            checkpointed_frame_count,
        })
    }
}

fn ensure_sqlite_file(path: &Path) -> Result<(), SqliteSearchError> {
    sqlite_file_creation_result(create_new_file_private(path))
}

fn sqlite_file_creation_result(result: io::Result<std::fs::File>) -> Result<(), SqliteSearchError> {
    match result {
        Ok(file) => {
            drop(file);
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn configure_connection(connection: &Connection) -> Result<(), SqliteSearchError> {
    connection.busy_timeout(Duration::from_secs(5))?;
    if connection.pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))? != "wal"
    {
        connection.pragma_update(None, "journal_mode", "WAL")?;
    }
    connection.pragma_update(None, "synchronous", "NORMAL")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.pragma_update(None, "temp_store", "MEMORY")?;
    Ok(())
}

fn detect_capabilities(
    connection: &Connection,
) -> Result<SqliteSearchCapabilities, SqliteSearchError> {
    let sqlite_version =
        connection.query_row("SELECT sqlite_version()", [], |row| row.get::<_, String>(0))?;
    let fts5 = probe_capability(
        connection,
        "
        CREATE VIRTUAL TABLE temp.coral_search_fts5_check USING fts5(value);
        DROP TABLE temp.coral_search_fts5_check;
        ",
    )?;
    let trigram = if fts5 {
        probe_capability(
            connection,
            "
            CREATE VIRTUAL TABLE temp.coral_search_trigram_check
            USING fts5(value, tokenize = 'trigram');
            DROP TABLE temp.coral_search_trigram_check;
            ",
        )?
    } else {
        false
    };

    Ok(SqliteSearchCapabilities {
        sqlite_version,
        fts5,
        trigram,
    })
}

fn probe_capability(connection: &Connection, sql: &str) -> Result<bool, SqliteSearchError> {
    classify_capability_probe(connection.execute_batch(sql))
}

fn classify_capability_probe(result: rusqlite::Result<()>) -> Result<bool, SqliteSearchError> {
    match result {
        Ok(()) => Ok(true),
        Err(rusqlite::Error::SqliteFailure(error, _)) if error.code == ErrorCode::Unknown => {
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

fn ensure_supported(capabilities: &SqliteSearchCapabilities) -> Result<(), SqliteSearchError> {
    if !capabilities.fts5 {
        return Err(SqliteSearchError::UnsupportedCapability {
            feature: "FTS5",
            sqlite_version: capabilities.sqlite_version.clone(),
        });
    }
    if !capabilities.trigram {
        return Err(SqliteSearchError::UnsupportedCapability {
            feature: "FTS5 trigram tokenizer",
            sqlite_version: capabilities.sqlite_version.clone(),
        });
    }
    Ok(())
}

fn migrate_if_needed(connection: &mut Connection) -> Result<(), SqliteSearchError> {
    if schema_is_current(connection)? {
        return Ok(());
    }

    let user_version: u32 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if user_version > SEARCH_SQLITE_SCHEMA_VERSION {
        return Err(SqliteSearchError::UnsupportedSchemaVersion {
            database_version: user_version,
            supported_version: SEARCH_SQLITE_SCHEMA_VERSION,
        });
    }

    // Replay the full idempotent history before upgrading so a damaged older
    // schema cannot carry missing objects into the current version.
    match apply_all_migrations(connection) {
        Ok(()) if schema_is_current(connection)? => return Ok(()),
        Ok(()) => {
            tracing::warn!("SQLite search schema remained incomplete after replaying migrations");
        }
        Err(error) if schema_rebuild_can_recover(&error) => {
            tracing::warn!(
                error = %error,
                "SQLite search schema repair failed while replaying migrations"
            );
        }
        Err(error) => return Err(error),
    }

    if catalog_schema_is_current(connection)? {
        tracing::warn!(
            "resetting incompatible observed-values schema while preserving the catalog index"
        );
        discard_observed_values_schema(connection)?;
        apply_all_migrations(connection)?;
        if schema_is_current(connection)? {
            return Ok(());
        }
        return Err(SqliteSearchError::IncompleteSchemaAfterRebuild {
            schema_version: SEARCH_SQLITE_SCHEMA_VERSION,
        });
    }

    tracing::warn!("rebuilding the disposable search index after catalog schema repair failed");
    discard_search_index_schema(connection)?;
    apply_all_migrations(connection)?;
    if !schema_is_current(connection)? {
        return Err(SqliteSearchError::IncompleteSchemaAfterRebuild {
            schema_version: SEARCH_SQLITE_SCHEMA_VERSION,
        });
    }
    Ok(())
}

fn schema_rebuild_can_recover(error: &SqliteSearchError) -> bool {
    matches!(
        error,
        SqliteSearchError::Sqlite(error)
            if matches!(
                sqlite_error_code(error),
                Some(
                    ErrorCode::Unknown
                        | ErrorCode::SchemaChanged
                        | ErrorCode::ConstraintViolation
                )
            )
    )
}

fn sqlite_error_code(error: &rusqlite::Error) -> Option<ErrorCode> {
    match error {
        rusqlite::Error::SqliteFailure(error, _) | rusqlite::Error::SqlInputError { error, .. } => {
            Some(error.code)
        }
        _ => None,
    }
}

fn apply_all_migrations(connection: &mut Connection) -> Result<(), SqliteSearchError> {
    for migration in SEARCH_SQLITE_MIGRATIONS {
        apply_migration(connection, migration)?;
    }
    Ok(())
}

fn discard_observed_values_schema(connection: &mut Connection) -> Result<(), SqliteSearchError> {
    let objects = {
        let mut statement = connection.prepare(
            "
            SELECT type, name
            FROM sqlite_schema
            WHERE type IN ('trigger', 'view', 'index', 'table')
              AND (
                  name GLOB 'observed_*'
                  OR name GLOB 'idx_observed_*'
              )
            ORDER BY CASE
                WHEN type = 'trigger' THEN 0
                WHEN type = 'view' THEN 1
                WHEN type = 'index' THEN 2
                WHEN sql LIKE 'CREATE VIRTUAL TABLE%' THEN 3
                ELSE 4
            END,
            name
            ",
        )?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };

    let transaction = connection.transaction()?;
    for (object_type, name) in objects {
        let drop_kind = match object_type.as_str() {
            "trigger" => "TRIGGER",
            "view" => "VIEW",
            "index" => "INDEX",
            "table" => "TABLE",
            _ => continue,
        };
        let quoted_name = format!("\"{}\"", name.replace('"', "\"\""));
        transaction.execute_batch(&format!("DROP {drop_kind} IF EXISTS {quoted_name}"))?;
    }
    transaction.commit()?;
    Ok(())
}

fn discard_search_index_schema(connection: &mut Connection) -> Result<(), SqliteSearchError> {
    let objects = {
        let mut statement = connection.prepare(
            "
            SELECT type, name
            FROM sqlite_schema
            WHERE type IN ('trigger', 'view', 'index', 'table')
              AND name NOT GLOB 'sqlite_*'
            ORDER BY CASE
                WHEN type = 'trigger' THEN 0
                WHEN type = 'view' THEN 1
                WHEN type = 'index' THEN 2
                WHEN sql LIKE 'CREATE VIRTUAL TABLE%' THEN 3
                ELSE 4
            END,
            name
            ",
        )?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };

    let transaction = connection.transaction()?;
    for (object_type, name) in objects {
        let drop_kind = match object_type.as_str() {
            "trigger" => "TRIGGER",
            "view" => "VIEW",
            "index" => "INDEX",
            "table" => "TABLE",
            _ => continue,
        };
        let quoted_name = format!("\"{}\"", name.replace('"', "\"\""));
        transaction.execute_batch(&format!("DROP {drop_kind} IF EXISTS {quoted_name}"))?;
    }
    transaction.pragma_update(None, "user_version", 0)?;
    transaction.commit()?;
    Ok(())
}

fn apply_migration(
    connection: &mut Connection,
    migration: &SearchSqliteMigration,
) -> Result<(), SqliteSearchError> {
    let transaction = connection.transaction()?;
    transaction.execute_batch(migration.sql)?;
    transaction.execute(
        "
        INSERT INTO search_meta (key, value, updated_at)
        VALUES ('schema_version', ?1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        WHERE search_meta.value <> excluded.value
        ",
        [migration.version.to_string()],
    )?;
    transaction.pragma_update(None, "user_version", migration.version)?;
    transaction.commit()?;
    Ok(())
}

fn schema_is_current(connection: &Connection) -> Result<bool, SqliteSearchError> {
    let user_version: u32 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if user_version != SEARCH_SQLITE_SCHEMA_VERSION {
        return Ok(false);
    }

    Ok(catalog_schema_is_current(connection)? && observed_values_schema_is_current(connection)?)
}

fn catalog_schema_is_current(connection: &Connection) -> Result<bool, SqliteSearchError> {
    if !tables_exist(
        connection,
        &["search_meta", "catalog_documents", "catalog_documents_fts"],
    )? {
        return Ok(false);
    }
    let search_meta_is_valid = schema_query_is_valid(
        connection,
        "SELECT key, value, updated_at FROM search_meta LIMIT 0",
    )?;
    let catalog_table_is_valid = schema_query_is_valid(
        connection,
        "
        SELECT
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
        FROM catalog_documents
        LIMIT 0
        ",
    )?;
    let catalog_fts_is_valid = schema_query_is_valid(
        connection,
        "
        SELECT
            workspace,
            doc_id,
            title,
            qualified_name,
            description,
            searchable_text
        FROM catalog_documents_fts
        LIMIT 0
        ",
    )?;
    Ok(search_meta_is_valid && catalog_table_is_valid && catalog_fts_is_valid)
}

fn observed_values_schema_is_current(connection: &Connection) -> Result<bool, SqliteSearchError> {
    if !tables_exist(
        connection,
        &[
            "observed_workspace_generations",
            "observed_source_generations",
            "observed_values",
            "observed_values_fts",
            "observed_queue_jobs",
        ],
    )? {
        return Ok(false);
    }

    for query in [
        "
        SELECT workspace, generation, updated_at
        FROM observed_workspace_generations
        LIMIT 0
        ",
        "
        SELECT workspace, source_name, generation, updated_at
        FROM observed_source_generations
        LIMIT 0
        ",
        "
        SELECT
            workspace,
            owner_source_name,
            source_name,
            source_scope_id,
            surface_kind,
            surface_name,
            column_name,
            value_key,
            display_value,
            search_text,
            first_observed_at,
            last_observed_at,
            observation_count,
            source_generation,
            workspace_generation
        FROM observed_values
        LIMIT 0
        ",
        "
        SELECT
            workspace,
            owner_source_name,
            source_name,
            source_scope_id,
            surface_kind,
            surface_name,
            column_name,
            value_key,
            display_value,
            search_text
        FROM observed_values_fts
        LIMIT 0
        ",
        "
        SELECT
            id,
            workspace,
            owner_source_name,
            source_name,
            source_scope_id,
            surface_kind,
            surface_name,
            workspace_generation,
            source_generation,
            payload_json,
            attempts,
            last_error,
            created_at,
            updated_at
        FROM observed_queue_jobs
        LIMIT 0
        ",
    ] {
        if !schema_query_is_valid(connection, query)? {
            return Ok(false);
        }
    }

    indexes_exist(
        connection,
        &[
            "idx_observed_queue_jobs_workspace_id",
            "idx_observed_queue_jobs_source",
            "idx_observed_queue_jobs_pending_scope",
            "idx_observed_values_source",
        ],
    )
}

fn tables_exist(connection: &Connection, table_names: &[&str]) -> Result<bool, SqliteSearchError> {
    for table_name in table_names {
        let exists: bool = connection.query_row(
            "
            SELECT EXISTS (
                SELECT 1
                FROM sqlite_master
                WHERE type IN ('table', 'virtual table') AND name = ?1
            )
            ",
            [*table_name],
            |row| row.get(0),
        )?;
        if !exists {
            return Ok(false);
        }
    }

    Ok(true)
}

fn indexes_exist(connection: &Connection, index_names: &[&str]) -> Result<bool, SqliteSearchError> {
    for index_name in index_names {
        let exists: bool = connection.query_row(
            "
            SELECT EXISTS (
                SELECT 1
                FROM sqlite_master
                WHERE type = 'index' AND name = ?1
            )
            ",
            [*index_name],
            |row| row.get(0),
        )?;
        if !exists {
            return Ok(false);
        }
    }

    Ok(true)
}

fn schema_query_is_valid(connection: &Connection, query: &str) -> Result<bool, SqliteSearchError> {
    match connection.prepare(query) {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                sqlite_error_code(&error),
                Some(ErrorCode::Unknown | ErrorCode::SchemaChanged)
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::{io, time::Duration};

    use rusqlite::{Connection, OptionalExtension as _};
    use tempfile::tempdir;

    use super::{
        SEARCH_SQLITE_MIGRATIONS, SEARCH_SQLITE_SCHEMA_VERSION, SqliteSearchError,
        SqliteSearchStore, WalCheckpointOutcome, classify_capability_probe, configure_connection,
        sqlite_file_creation_result, wal_checkpoint_truncate,
    };
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;

    #[test]
    fn schema_version_tracks_latest_migration() {
        let mut previous_version = 0;
        for migration in SEARCH_SQLITE_MIGRATIONS {
            assert!(migration.version > previous_version);
            previous_version = migration.version;
        }

        assert_eq!(previous_version, SEARCH_SQLITE_SCHEMA_VERSION);
    }

    #[test]
    fn search_sqlite_migrations_are_rerunnable() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let mut connection = rusqlite::Connection::open(&path).expect("raw connection");

        for migration in SEARCH_SQLITE_MIGRATIONS {
            super::apply_migration(&mut connection, migration).expect("first apply");
            super::apply_migration(&mut connection, migration).expect("second apply");
        }

        let user_version: u32 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("user_version");
        assert_eq!(user_version, SEARCH_SQLITE_SCHEMA_VERSION);
    }

    #[test]
    fn opening_v1_repairs_missing_search_meta_before_upgrade() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = v1_search_connection(&path);
        seed_catalog_document(&connection);
        connection
            .execute_batch("DROP TABLE search_meta")
            .expect("remove v1 metadata table");
        drop(connection);

        let connection = open_current_search_connection(&path);
        assert_eq!(catalog_document_count(&connection), 1);
    }

    #[test]
    fn opening_v1_repairs_missing_catalog_table_before_upgrade() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = v1_search_connection(&path);
        connection
            .execute_batch("DROP TABLE catalog_documents")
            .expect("remove v1 catalog table");
        drop(connection);

        let connection = open_current_search_connection(&path);
        assert_eq!(catalog_document_count(&connection), 0);
    }

    #[test]
    fn opening_v1_repairs_missing_catalog_fts_before_upgrade() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = v1_search_connection(&path);
        seed_catalog_document(&connection);
        connection
            .execute_batch("DROP TABLE catalog_documents_fts")
            .expect("remove v1 catalog FTS table");
        drop(connection);

        let connection = open_current_search_connection(&path);
        assert_eq!(catalog_document_count(&connection), 1);
    }

    #[test]
    fn opening_v1_rebuilds_disposable_index_when_repair_stays_invalid() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = v1_search_connection(&path);
        connection
            .execute(
                "INSERT INTO search_meta (key, value) VALUES ('sentinel', 'discarded')",
                [],
            )
            .expect("seed disposable search metadata");
        connection
            .execute_batch(
                "
                DROP TABLE catalog_documents;
                CREATE VIEW catalog_documents AS SELECT 1 AS malformed;
                ",
            )
            .expect("replace catalog table with malformed v1 object");
        drop(connection);

        let connection = open_current_search_connection(&path);
        let object_type = connection
            .query_row(
                "SELECT type FROM sqlite_schema WHERE name = 'catalog_documents'",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("catalog object type");
        let sentinel = connection
            .query_row(
                "SELECT value FROM search_meta WHERE key = 'sentinel'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .expect("sentinel metadata query");

        assert_eq!(object_type, "table");
        assert_eq!(sentinel, None);
    }

    #[test]
    fn open_workspace_creates_search_sqlite_schema() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace_name = WorkspaceName::parse("default").expect("workspace");
        let store = SqliteSearchStore::open_workspace(&layout, &workspace_name).expect("store");

        assert_eq!(store.path(), layout.search_sqlite_file(&workspace_name));
        assert!(store.capabilities().fts5);
        assert!(store.capabilities().trigram);

        let connection = store.connect_for_test().expect("connect");
        let user_version: u32 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("user_version");
        assert_eq!(user_version, SEARCH_SQLITE_SCHEMA_VERSION);

        let schema_version = connection
            .query_row(
                "SELECT value FROM search_meta WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .expect("schema_version query")
            .expect("schema_version");
        assert_eq!(schema_version, SEARCH_SQLITE_SCHEMA_VERSION.to_string());
    }

    #[test]
    fn opening_future_schema_version_fails() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = rusqlite::Connection::open(&path).expect("raw connection");
        connection
            .pragma_update(None, "user_version", SEARCH_SQLITE_SCHEMA_VERSION + 1)
            .expect("stamp future schema version");
        drop(connection);

        let error = SqliteSearchStore::open(&path, WorkspaceName::default())
            .expect_err("future schema must fail");
        assert!(matches!(
            error,
            SqliteSearchError::UnsupportedSchemaVersion {
                database_version,
                supported_version: SEARCH_SQLITE_SCHEMA_VERSION,
            } if database_version == SEARCH_SQLITE_SCHEMA_VERSION + 1
        ));
    }

    #[test]
    fn non_sqlite_lock_error_is_not_lock_contention() {
        let error = SqliteSearchError::UnsupportedCapability {
            feature: "FTS5",
            sqlite_version: "fixture".to_string(),
        };

        assert!(!error.is_lock_contention());
    }

    #[test]
    fn disk_full_error_is_storage_exhaustion() {
        let error = SqliteSearchError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL),
            None,
        ));

        assert!(error.is_storage_exhaustion());
    }

    #[test]
    fn storage_full_file_creation_error_preserves_exhaustion_category() {
        let error = sqlite_file_creation_result(Err(io::Error::new(
            io::ErrorKind::StorageFull,
            "fixture disk full",
        )))
        .expect_err("storage-full file creation must fail");

        assert!(matches!(
            &error,
            SqliteSearchError::Io(error) if error.kind() == io::ErrorKind::StorageFull
        ));
        assert!(error.is_storage_exhaustion());
    }

    #[test]
    fn capability_probe_only_treats_plain_sqlite_error_as_unsupported() {
        assert!(classify_capability_probe(Ok(())).expect("successful probe"));
        assert!(
            !classify_capability_probe(Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                Some("fixture unsupported capability".to_string()),
            )))
            .expect("plain SQLite error should mean unsupported capability")
        );
    }

    #[test]
    fn capability_probe_preserves_operational_sqlite_errors() {
        for code in [
            rusqlite::ffi::SQLITE_FULL,
            rusqlite::ffi::SQLITE_NOMEM,
            rusqlite::ffi::SQLITE_IOERR,
            rusqlite::ffi::SQLITE_CORRUPT,
        ] {
            let expected = rusqlite::ffi::Error::new(code).code;
            let error = classify_capability_probe(Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(code),
                None,
            )))
            .expect_err("operational probe failure must be preserved");

            match error {
                SqliteSearchError::Sqlite(error) => {
                    assert_eq!(error.sqlite_error_code(), Some(expected));
                }
                other => panic!("expected SQLite error for code {code}, got {other:?}"),
            }
        }
    }

    #[test]
    fn schema_rebuild_only_follows_repairable_schema_errors() {
        let schema_error = SqliteSearchError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
            None,
        ));
        let locked = SqliteSearchError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            None,
        ));
        let constraint = SqliteSearchError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT),
            None,
        ));
        let disk_full = SqliteSearchError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL),
            None,
        ));

        assert!(super::schema_rebuild_can_recover(&schema_error));
        assert!(super::schema_rebuild_can_recover(&constraint));
        assert!(!super::schema_rebuild_can_recover(&locked));
        assert!(!super::schema_rebuild_can_recover(&disk_full));
    }

    #[test]
    fn wal_checkpoint_reports_reader_contention() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("checkpoint.sqlite3");
        let writer = Connection::open(&path).expect("writer");
        configure_connection(&writer).expect("configure writer");
        writer
            .pragma_update(None, "wal_autocheckpoint", 0)
            .expect("disable automatic checkpoints");
        writer
            .execute_batch(
                "
                CREATE TABLE records (value TEXT NOT NULL);
                INSERT INTO records (value) VALUES ('initial');
                PRAGMA wal_checkpoint(TRUNCATE);
                ",
            )
            .expect("seed database");

        let reader = Connection::open(&path).expect("reader");
        configure_connection(&reader).expect("configure reader");
        reader.execute_batch("BEGIN").expect("begin reader");
        let count: i64 = reader
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .expect("establish reader snapshot");
        assert_eq!(count, 1);

        writer
            .execute("INSERT INTO records (value) VALUES ('new')", [])
            .expect("write after reader snapshot");

        let checkpoint = Connection::open(&path).expect("checkpoint connection");
        configure_connection(&checkpoint).expect("configure checkpoint connection");
        checkpoint
            .busy_timeout(Duration::ZERO)
            .expect("disable checkpoint wait");
        let outcome = wal_checkpoint_truncate(&checkpoint).expect("checkpoint result");

        assert!(matches!(outcome, WalCheckpointOutcome::Busy { .. }));
        reader.execute_batch("ROLLBACK").expect("end reader");
    }

    #[test]
    fn source_all_clear_rolls_back_catalog_when_observed_clear_fails() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let connection = store.connect_for_test().expect("connect");
        connection
            .execute(
                "INSERT INTO catalog_documents (workspace, doc_id, doc_kind, source_name, title, payload_json, snapshot_fingerprint) VALUES ('default', 'doc', 'catalog_table', 'github', 'Issues', '{}', 'fingerprint')",
                [],
            )
            .expect("seed catalog document");
        connection
            .execute(
                "INSERT INTO observed_values (workspace, source_name, source_scope_id, surface_kind, surface_name, column_name, value_key, display_value, search_text, source_generation, workspace_generation) VALUES ('default', 'github', 'scope', 'table', 'issues', 'title', 'value', 'Payment issue', 'payment issue', 0, 0)",
                [],
            )
            .expect("seed observed value");
        connection
            .execute_batch(
                "CREATE TRIGGER fail_observed_delete BEFORE DELETE ON observed_values BEGIN SELECT RAISE(ABORT, 'forced observed clear failure'); END;",
            )
            .expect("install failure trigger");
        drop(connection);

        store
            .clear_source_all("github")
            .expect_err("combined clear should fail");

        let connection = store.connect_for_test().expect("reconnect");
        let catalog_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM catalog_documents WHERE workspace = 'default' AND source_name = 'github'",
                [],
                |row| row.get(0),
            )
            .expect("catalog count");
        let observed_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM observed_values WHERE workspace = 'default' AND source_name = 'github'",
                [],
                |row| row.get(0),
            )
            .expect("observed count");
        let generation_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM observed_source_generations WHERE workspace = 'default' AND source_name = 'github'",
                [],
                |row| row.get(0),
            )
            .expect("generation count");
        assert_eq!(catalog_count, 1);
        assert_eq!(observed_count, 1);
        assert_eq!(generation_count, 0);
    }

    #[test]
    fn opening_current_schema_does_not_rewrite_metadata() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let store = SqliteSearchStore::open(&path, WorkspaceName::default()).expect("store");
        let connection = store.connect_for_test().expect("connect");
        connection
            .execute(
                "UPDATE search_meta SET updated_at = 'sentinel' WHERE key = 'schema_version'",
                [],
            )
            .expect("seed sentinel timestamp");
        drop(connection);
        drop(store);

        SqliteSearchStore::open(&path, WorkspaceName::default()).expect("reopen current schema");
        let connection = rusqlite::Connection::open(&path).expect("raw connection");
        let updated_at = connection
            .query_row(
                "SELECT updated_at FROM search_meta WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .expect("schema version timestamp query")
            .expect("schema version timestamp");

        assert_eq!(updated_at, "sentinel");
    }

    #[test]
    fn opening_current_version_repairs_missing_schema_objects() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = rusqlite::Connection::open(&path).expect("raw connection");
        connection
            .pragma_update(None, "user_version", SEARCH_SQLITE_SCHEMA_VERSION)
            .expect("stamp current schema version");
        drop(connection);

        let store =
            SqliteSearchStore::open(&path, WorkspaceName::default()).expect("repair schema");
        let connection = store.connect_for_test().expect("connect");
        for table_name in ["search_meta", "catalog_documents", "catalog_documents_fts"] {
            let exists: bool = connection
                .query_row(
                    "
                    SELECT EXISTS (
                        SELECT 1
                        FROM sqlite_master
                        WHERE type IN ('table', 'virtual table') AND name = ?1
                    )
                    ",
                    [table_name],
                    |row| row.get(0),
                )
                .expect("schema object lookup");
            assert!(exists, "{table_name} should exist after repair");
        }
    }

    #[test]
    fn opening_current_version_repairs_observed_schema_without_dropping_data() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = open_current_search_connection(&path);
        seed_observed_queue_job(&connection);
        connection
            .execute_batch("DROP INDEX idx_observed_queue_jobs_workspace_id")
            .expect("remove observed queue index");
        drop(connection);

        let connection = open_current_search_connection(&path);

        assert_eq!(observed_queue_job_count(&connection), 1);
    }

    #[test]
    fn opening_current_version_resets_only_incompatible_observed_schema() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = open_current_search_connection(&path);
        seed_catalog_document(&connection);
        seed_observed_queue_job(&connection);
        connection
            .execute(
                "INSERT INTO search_meta (key, value) VALUES ('sentinel', 'preserved')",
                [],
            )
            .expect("seed preserved search metadata");
        connection
            .execute_batch(
                "
                DROP TABLE observed_queue_jobs;
                CREATE TABLE observed_queue_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workspace TEXT NOT NULL,
                    owner_source_name TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    surface_kind TEXT NOT NULL,
                    surface_name TEXT NOT NULL,
                    workspace_generation INTEGER NOT NULL,
                    source_generation INTEGER NOT NULL
                );
                ",
            )
            .expect("replace queue table with incompatible shape");
        drop(connection);

        let connection = open_current_search_connection(&path);
        let sentinel = connection
            .query_row(
                "SELECT value FROM search_meta WHERE key = 'sentinel'",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("preserved search metadata");

        assert_eq!(catalog_document_count(&connection), 1);
        assert_eq!(observed_queue_job_count(&connection), 0);
        assert_eq!(sentinel, "preserved");
    }

    #[test]
    fn opening_current_version_resets_duplicate_observed_rows_after_repair_fails() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = open_current_search_connection(&path);
        seed_catalog_document(&connection);
        connection
            .execute(
                "INSERT INTO search_meta (key, value) VALUES ('sentinel', 'preserved')",
                [],
            )
            .expect("seed preserved search metadata");
        connection
            .execute_batch("DROP INDEX idx_observed_queue_jobs_pending_scope")
            .expect("remove observed queue uniqueness guard");
        seed_observed_queue_job(&connection);
        seed_observed_queue_job(&connection);
        assert_eq!(observed_queue_job_count(&connection), 2);
        drop(connection);

        let connection = open_current_search_connection(&path);
        let sentinel = connection
            .query_row(
                "SELECT value FROM search_meta WHERE key = 'sentinel'",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("preserved search metadata");

        assert_eq!(catalog_document_count(&connection), 1);
        assert_eq!(observed_queue_job_count(&connection), 0);
        assert_eq!(sentinel, "preserved");
    }

    fn v1_search_connection(path: &std::path::Path) -> Connection {
        let mut connection = Connection::open(path).expect("raw v1 connection");
        super::apply_migration(
            &mut connection,
            SEARCH_SQLITE_MIGRATIONS.first().expect("v1 migration"),
        )
        .expect("apply v1 migration");
        connection
    }

    fn open_current_search_connection(path: &std::path::Path) -> Connection {
        let store = SqliteSearchStore::open(path, WorkspaceName::default()).expect("open search");
        let connection = store.connect_for_test().expect("connect");
        assert!(super::schema_is_current(&connection).expect("validate current schema"));
        connection
    }

    fn seed_catalog_document(connection: &Connection) {
        connection
            .execute(
                "
                INSERT INTO catalog_documents (
                    workspace,
                    doc_id,
                    doc_kind,
                    title,
                    payload_json,
                    snapshot_fingerprint
                ) VALUES ('default', 'fixture', 'catalog_table', 'Fixture', '{}', 'fixture')
                ",
                [],
            )
            .expect("seed catalog document");
    }

    fn seed_observed_queue_job(connection: &Connection) {
        connection
            .execute(
                "
                INSERT INTO observed_queue_jobs (
                    workspace,
                    owner_source_name,
                    source_name,
                    source_scope_id,
                    surface_kind,
                    surface_name,
                    workspace_generation,
                    source_generation,
                    payload_json
                ) VALUES (
                    'default',
                    'github',
                    'github',
                    'fixture-scope',
                    'table',
                    'issues',
                    0,
                    0,
                    '{}'
                )
                ",
                [],
            )
            .expect("seed observed queue job");
    }

    fn catalog_document_count(connection: &Connection) -> i64 {
        connection
            .query_row("SELECT COUNT(*) FROM catalog_documents", [], |row| {
                row.get(0)
            })
            .expect("catalog document count")
    }

    fn observed_queue_job_count(connection: &Connection) -> i64 {
        connection
            .query_row("SELECT COUNT(*) FROM observed_queue_jobs", [], |row| {
                row.get(0)
            })
            .expect("observed queue job count")
    }
}
