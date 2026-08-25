//! Workspace-scoped `SQLite` storage for Universal Search.

use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rusqlite::{Connection, ErrorCode, TransactionBehavior};

use crate::search::catalog::index::{
    CatalogClearResult, CatalogDocumentClass, CatalogIndexSnapshot, CatalogRebuildResult,
    CatalogRefreshResult, CatalogSearchHits,
};
use crate::search::catalog::sqlite_index::{
    SqliteCatalogIndex, clear_catalog_source_documents_in_transaction,
    clear_catalog_workspace_documents_in_transaction,
};
use crate::search::observed::{
    ObservedValuesClearResult, clear_observed_source_in_transaction,
    clear_observed_workspace_in_transaction,
};
use crate::state::AppStateLayout;
use crate::storage::fs::create_new_file_private;
use crate::workspaces::WorkspaceName;

mod observed_identity_migration;

use observed_identity_migration::{
    LegacyObservedIdentity, apply_singular_observed_migrations, observed_schema_has_singular_tables,
};

pub(crate) const SEARCH_SQLITE_SCHEMA_VERSION: u32 = 6;

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
    SearchSqliteMigration {
        version: 3,
        sql: include_str!("migrations/0003_observed_values_governance.sql"),
    },
    SearchSqliteMigration {
        version: 4,
        sql: include_str!("migrations/0004_catalog_source_ownership.sql"),
    },
    SearchSqliteMigration {
        version: 5,
        sql: include_str!("migrations/0005_observed_source_identity.sql"),
    },
    SearchSqliteMigration {
        version: 6,
        sql: include_str!("migrations/0006_catalog_source_identity.sql"),
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
        migrate_if_needed(&mut connection, &workspace_name)?;

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

    pub(crate) fn clear_workspace_all(
        &self,
    ) -> Result<(CatalogClearResult, ObservedValuesClearResult), SqliteSearchError> {
        let mut connection = self.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let catalog =
            clear_catalog_workspace_documents_in_transaction(&transaction, &self.workspace_name)?;
        let observed = clear_observed_workspace_in_transaction(&transaction, &self.workspace_name)?;
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
        class: CatalogDocumentClass,
    ) -> Result<CatalogSearchHits, SqliteSearchError> {
        let connection = self.connect()?;
        SqliteCatalogIndex::new().search_ranked(
            &connection,
            &self.workspace_name,
            terms,
            limit,
            class,
        )
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

fn migrate_if_needed(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
) -> Result<(), SqliteSearchError> {
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

    // A singular observed schema must never reach the owner-era replay below.
    // Migration 0002 names `owner_source_name` in three index statements, and
    // creating one of those indexes when it is *missing* fails against singular
    // tables -- which would route intact observed values and queue jobs into the
    // discarding reset further down. Restating the current-era objects first
    // repairs the gap and also makes the later replay inert, because
    // `CREATE INDEX IF NOT EXISTS` short-circuits on the index name before it
    // resolves the column list.
    if observed_schema_has_singular_tables(connection)? {
        match apply_singular_observed_migrations(connection, workspace_name) {
            Ok(()) if schema_is_current(connection)? => return Ok(()),
            Ok(()) => {}
            Err(error) if schema_rebuild_can_recover(&error) => {
                tracing::warn!(
                    error = %error,
                    "singular observed-values schema repair failed before replaying migrations"
                );
            }
            Err(error) => return Err(error),
        }
    }

    // Replay the full idempotent history before upgrading so a damaged older
    // schema cannot carry missing objects into the current version.
    match apply_all_migrations(connection, workspace_name) {
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
        apply_all_migrations(connection, workspace_name)?;
        if schema_is_current(connection)? {
            return Ok(());
        }
        return Err(SqliteSearchError::IncompleteSchemaAfterRebuild {
            schema_version: SEARCH_SQLITE_SCHEMA_VERSION,
        });
    }

    tracing::warn!("rebuilding the disposable search index after catalog schema repair failed");
    discard_search_index_schema(connection)?;
    apply_all_migrations(connection, workspace_name)?;
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

fn apply_all_migrations(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
) -> Result<(), SqliteSearchError> {
    for migration in SEARCH_SQLITE_MIGRATIONS {
        apply_migration(connection, migration, workspace_name)?;
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
    workspace_name: &WorkspaceName,
) -> Result<(), SqliteSearchError> {
    let started_at = Instant::now();
    let transaction = connection.transaction()?;
    let legacy_observed = if migration.version == 5 {
        LegacyObservedIdentity::detect(&transaction)?
    } else {
        LegacyObservedIdentity::NONE
    };
    // The renames must precede `execute_batch`: 0005 creates its tables with
    // `IF NOT EXISTS`, so it would no-op against legacy tables still occupying
    // the canonical names.
    legacy_observed.move_legacy_objects_aside(&transaction)?;
    transaction.execute_batch(migration.sql)?;
    let preserved = legacy_observed.copy_preserved_rows(&transaction)?;
    if legacy_observed.is_legacy() {
        tracing::info!(
            workspace = %workspace_name.as_str(),
            preserved_values = preserved.values,
            discarded_divergent_values = preserved.discarded_values,
            preserved_queue_jobs = preserved.queue_jobs,
            discarded_divergent_queue_jobs = preserved.discarded_queue_jobs,
            duration_ms = started_at.elapsed().as_millis(),
            "migrated observed-values storage to singular source identity"
        );
    }
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
    // 0006 removed the ownership projection for good, so its presence means the
    // schema predates v6 no matter what version it claims to be.
    if tables_exist(connection, &["catalog_source_owners"])? {
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
            catalog_name,
            surface_kind,
            surface_name,
            field_name,
            field_role,
            qualified_name,
            title,
            description,
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

    // Presence probes cannot see a *surplus* column, and a surviving legacy
    // queue table (`owner_source_name NOT NULL`, no default) would pass the
    // probes above and then reject every rewritten enqueue with no self-heal.
    for query in [
        "SELECT owner_source_name FROM observed_values LIMIT 0",
        "SELECT owner_source_name FROM observed_values_fts LIMIT 0",
        "SELECT owner_source_name FROM observed_queue_jobs LIMIT 0",
    ] {
        if schema_query_is_valid(connection, query)? {
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
            "idx_observed_values_workspace_last_observed",
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
mod tests;
