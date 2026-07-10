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
            Self::UnsupportedCapability { .. } | Self::UnsupportedSchemaVersion { .. } => false,
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

    // Repair mode deliberately reruns every current migration when the version
    // stamp says "current" but required schema objects are missing. Keep every
    // migration idempotent: use IF NOT EXISTS and conflict-safe metadata writes.
    let repair_current_version = user_version == SEARCH_SQLITE_SCHEMA_VERSION;
    for migration in SEARCH_SQLITE_MIGRATIONS {
        let should_apply = migration.version > user_version || repair_current_version;
        if !should_apply {
            continue;
        }
        apply_migration(connection, migration)?;
    }
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

    for table_name in [
        "search_meta",
        "catalog_documents",
        "catalog_documents_fts",
        "observed_workspace_generations",
        "observed_source_generations",
        "observed_values",
        "observed_values_fts",
        "observed_queue_jobs",
    ] {
        let exists: bool = connection.query_row(
            "
            SELECT EXISTS (
                SELECT 1
                FROM sqlite_master
                WHERE type IN ('table', 'virtual table') AND name = ?1
            )
            ",
            [table_name],
            |row| row.get(0),
        )?;
        if !exists {
            return Ok(false);
        }
    }

    for index_name in [
        "idx_observed_queue_jobs_workspace_id",
        "idx_observed_queue_jobs_source",
        "idx_observed_queue_jobs_pending_scope",
        "idx_observed_values_source",
    ] {
        let exists: bool = connection.query_row(
            "
            SELECT EXISTS (
                SELECT 1
                FROM sqlite_master
                WHERE type = 'index' AND name = ?1
            )
            ",
            [index_name],
            |row| row.get(0),
        )?;
        if !exists {
            return Ok(false);
        }
    }

    Ok(true)
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
}
