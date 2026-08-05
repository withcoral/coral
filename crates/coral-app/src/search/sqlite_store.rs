//! Workspace-scoped `SQLite` storage for Universal Search.

use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rusqlite::{Connection, ErrorCode, Transaction, TransactionBehavior};

use crate::search::catalog::sqlite_index::{
    CatalogClearResult, CatalogDocumentClass, CatalogIndexSnapshot, CatalogRebuildResult,
    CatalogRefreshResult, CatalogSearchHits, SqliteCatalogIndex,
    clear_catalog_source_documents_in_transaction,
    clear_catalog_workspace_documents_in_transaction,
};
use crate::search::observed::{
    ObservedValuesClearResult, clear_observed_source_in_transaction,
    clear_observed_workspace_in_transaction,
};
use crate::state::AppStateLayout;
use crate::storage::fs::create_new_file_private;
use crate::workspaces::WorkspaceName;

pub(crate) const SEARCH_SQLITE_SCHEMA_VERSION: u32 = 5;

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

/// Legacy observed tables renamed aside by the version-5 hook before the
/// singular schema is created, then copied from and dropped in the same
/// transaction.
const LEGACY_OBSERVED_VALUES_TABLE: &str = "observed_values_legacy_v4";
const LEGACY_OBSERVED_QUEUE_JOBS_TABLE: &str = "observed_queue_jobs_legacy_v4";

/// Columns carried across the version-5 copy, one constant per table.
///
/// Each is interpolated into both halves of an `INSERT ... SELECT`, so the two
/// lists cannot drift. Written out twice they could, and a mismatch would not
/// fail -- it would shuffle values between columns as the migration copied them.
const OBSERVED_VALUES_COPY_COLUMNS: &str = "
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
";

/// `id` is carried explicitly so queue ordering survives; `SQLite` advances the
/// `AUTOINCREMENT` sequence to match the highest inserted id.
const OBSERVED_QUEUE_JOBS_COPY_COLUMNS: &str = "
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
";

/// `rowid` is carried explicitly: the projection keys every FTS row to its
/// canonical rowid and refreshes by inserting at that exact rowid. Letting fts5
/// assign fresh sequential rowids would silently misalign the two whenever
/// canonical rowids are gapped -- as they are after eviction or a stale purge --
/// and the next refresh of a gapped row would collide with whichever value now
/// occupies its rowid, failing the queue job until it dead-letters.
const OBSERVED_VALUES_FTS_COPY_COLUMNS: &str = "
    rowid,
    workspace,
    source_name,
    source_scope_id,
    surface_kind,
    surface_name,
    column_name,
    value_key,
    display_value,
    search_text
";

/// First migration whose observed schema is singular. A database that already
/// reached it must be repaired from here, never by replaying the owner-era DDL
/// below it.
const SINGULAR_OBSERVED_SCHEMA_VERSION: u32 = 5;

/// Observed tables that carried the pre-#1791 owner/component identity pair.
const OWNER_ERA_OBSERVED_TABLES: &[&str] = &[
    "observed_values",
    "observed_values_fts",
    "observed_queue_jobs",
];

/// Restates the current-era observed schema without replaying owner-era DDL.
///
/// Every statement is `IF NOT EXISTS`, so this fills in missing objects and
/// leaves existing rows alone.
fn apply_singular_observed_migrations(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
) -> Result<(), SqliteSearchError> {
    for migration in SEARCH_SQLITE_MIGRATIONS
        .iter()
        .filter(|migration| migration.version >= SINGULAR_OBSERVED_SCHEMA_VERSION)
    {
        apply_migration(connection, migration, workspace_name)?;
    }
    Ok(())
}

/// Whether any observed table has already dropped the owner column.
///
/// Probed per table because a partially repaired sidecar can be singular in one
/// table while another still carries the pair; the owner-era DDL is unsafe as
/// soon as *one* table has moved on.
fn observed_schema_has_singular_tables(connection: &Connection) -> Result<bool, SqliteSearchError> {
    for table_name in OWNER_ERA_OBSERVED_TABLES {
        if tables_exist(connection, &[table_name])?
            && !schema_query_is_valid(
                connection,
                &format!("SELECT owner_source_name FROM {table_name} LIMIT 0"),
            )?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Which observed tables still carry the pre-#1791 owner/component identity
/// pair, decided per table so a partially repaired sidecar is healed by the
/// same preserving path rather than falling through to the discarding reset.
#[derive(Debug, Clone, Copy)]
struct LegacyObservedIdentity {
    values: bool,
    queue_jobs: bool,
    fts: bool,
}

/// Row counts carried across the version-5 transform, for the migration log.
#[derive(Debug, Clone, Copy, Default)]
struct PreservedObservedRows {
    values: usize,
    discarded_values: i64,
    queue_jobs: usize,
    discarded_queue_jobs: i64,
}

impl LegacyObservedIdentity {
    const NONE: Self = Self {
        values: false,
        queue_jobs: false,
        fts: false,
    };

    /// A missing table reads as not-legacy, which is what fresh and
    /// already-singular databases need.
    fn detect(transaction: &Transaction<'_>) -> Result<Self, SqliteSearchError> {
        Ok(Self {
            values: schema_query_is_valid(
                transaction,
                "SELECT owner_source_name FROM observed_values LIMIT 0",
            )?,
            queue_jobs: schema_query_is_valid(
                transaction,
                "SELECT owner_source_name FROM observed_queue_jobs LIMIT 0",
            )?,
            fts: schema_query_is_valid(
                transaction,
                "SELECT owner_source_name FROM observed_values_fts LIMIT 0",
            )?,
        })
    }

    const fn is_legacy(self) -> bool {
        self.values || self.queue_jobs || self.fts
    }

    /// The FTS index is a projection of `observed_values`, so it is rebuilt
    /// whenever either side is legacy.
    const fn rebuilds_fts(self) -> bool {
        self.values || self.fts
    }

    fn move_legacy_objects_aside(
        self,
        transaction: &Transaction<'_>,
    ) -> Result<(), SqliteSearchError> {
        if self.values {
            // SQLite keeps index *names* attached to a renamed table, so 0005's
            // `CREATE INDEX` statements would silently no-op and leave the new
            // table unindexed. Drop them by name before the rename lands.
            transaction.execute_batch(&format!(
                "
                DROP TABLE IF EXISTS {LEGACY_OBSERVED_VALUES_TABLE};
                DROP INDEX IF EXISTS idx_observed_values_source;
                DROP INDEX IF EXISTS idx_observed_values_workspace_last_observed;
                ALTER TABLE observed_values RENAME TO {LEGACY_OBSERVED_VALUES_TABLE};
                "
            ))?;
        }
        if self.queue_jobs {
            transaction.execute_batch(&format!(
                "
                DROP TABLE IF EXISTS {LEGACY_OBSERVED_QUEUE_JOBS_TABLE};
                DROP INDEX IF EXISTS idx_observed_queue_jobs_workspace_id;
                DROP INDEX IF EXISTS idx_observed_queue_jobs_source;
                DROP INDEX IF EXISTS idx_observed_queue_jobs_pending_scope;
                ALTER TABLE observed_queue_jobs RENAME TO {LEGACY_OBSERVED_QUEUE_JOBS_TABLE};
                "
            ))?;
        }
        if self.rebuilds_fts() {
            // Dropping the fts5 table takes its shadow tables with it.
            transaction.execute_batch("DROP TABLE IF EXISTS observed_values_fts")?;
        }
        Ok(())
    }

    fn copy_preserved_rows(
        self,
        transaction: &Transaction<'_>,
    ) -> Result<PreservedObservedRows, SqliteSearchError> {
        let mut preserved = PreservedObservedRows::default();
        if self.values {
            (preserved.values, preserved.discarded_values) = copy_preserved_rows(
                transaction,
                LEGACY_OBSERVED_VALUES_TABLE,
                "observed_values",
                OBSERVED_VALUES_COPY_COLUMNS,
            )?;
        }
        if self.queue_jobs {
            (preserved.queue_jobs, preserved.discarded_queue_jobs) = copy_preserved_rows(
                transaction,
                LEGACY_OBSERVED_QUEUE_JOBS_TABLE,
                "observed_queue_jobs",
                OBSERVED_QUEUE_JOBS_COPY_COLUMNS,
            )?;
        }
        if self.rebuilds_fts() {
            repopulate_observed_fts_from_canonical(transaction)?;
        }
        Ok(preserved)
    }
}

/// Copies rows whose two identities agreed from a renamed legacy table into its
/// singular replacement, drops the legacy table, and reports
/// `(preserved, discarded)`.
///
/// Divergence is unreachable on production paths, so a nonzero discard count in
/// the wild would falsify that premise -- which is why it is logged.
///
/// `legacy_table`, `target_table`, and `columns` are interpolated into SQL, so
/// every caller passes a constant defined in this file. None of them is reachable
/// from a request, a manifest, or a stored row.
fn copy_preserved_rows(
    transaction: &Transaction<'_>,
    legacy_table: &str,
    target_table: &str,
    columns: &str,
) -> Result<(usize, i64), SqliteSearchError> {
    let discarded = transaction.query_row(
        &format!(
            "
            SELECT COUNT(*)
            FROM {legacy_table}
            WHERE owner_source_name <> source_name
            "
        ),
        [],
        |row| row.get(0),
    )?;
    let preserved = transaction.execute(
        &format!(
            "
            INSERT INTO {target_table} ({columns})
            SELECT {columns}
            FROM {legacy_table}
            WHERE owner_source_name = source_name
            "
        ),
        [],
    )?;
    transaction.execute_batch(&format!("DROP TABLE {legacy_table}"))?;
    Ok((preserved, discarded))
}

/// `observed_values` already holds exactly the preserved rows and carries both
/// indexed columns, so the values are searchable the moment this transaction
/// commits -- no reconcile gap.
fn repopulate_observed_fts_from_canonical(
    transaction: &Transaction<'_>,
) -> Result<(), SqliteSearchError> {
    transaction.execute(
        &format!(
            "
            INSERT INTO observed_values_fts ({OBSERVED_VALUES_FTS_COPY_COLUMNS})
            SELECT {OBSERVED_VALUES_FTS_COPY_COLUMNS}
            FROM observed_values
            "
        ),
        [],
    )?;
    Ok(())
}

fn apply_migration(
    connection: &mut Connection,
    migration: &SearchSqliteMigration,
    workspace_name: &WorkspaceName,
) -> Result<(), SqliteSearchError> {
    let started_at = Instant::now();
    let transaction = connection.transaction()?;
    let initializes_catalog_source_ownership =
        migration.version == 4 && !tables_exist(&transaction, &["catalog_source_owners"])?;
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
    if initializes_catalog_source_ownership {
        // Existing catalog rows predate durable installed-owner identity. They
        // are disposable and cannot be safely backfilled for multi-component
        // sources without loading source artifacts during migration.
        transaction.execute("DELETE FROM catalog_documents_fts", [])?;
        transaction.execute("DELETE FROM catalog_documents", [])?;
        transaction.execute(
            "DELETE FROM search_meta WHERE key GLOB 'catalog_snapshot_fingerprint:*'",
            [],
        )?;
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
        &[
            "search_meta",
            "catalog_documents",
            "catalog_documents_fts",
            "catalog_source_owners",
        ],
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
    let catalog_source_owners_is_valid = schema_query_is_valid(
        connection,
        "
        SELECT
            workspace,
            source_name,
            owner_source_name,
            snapshot_fingerprint,
            updated_at
        FROM catalog_source_owners
        LIMIT 0
        ",
    )?;
    let catalog_source_owner_index_is_valid =
        indexes_exist(connection, &["idx_catalog_source_owners_workspace_owner"])?;
    Ok(search_meta_is_valid
        && catalog_table_is_valid
        && catalog_fts_is_valid
        && catalog_source_owners_is_valid
        && catalog_source_owner_index_is_valid)
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
mod tests {
    use std::{io, time::Duration};

    use rusqlite::{Connection, OptionalExtension as _};
    use tempfile::tempdir;

    use super::{
        SEARCH_SQLITE_MIGRATIONS, SEARCH_SQLITE_SCHEMA_VERSION, SqliteSearchError,
        SqliteSearchStore, WalCheckpointOutcome, classify_capability_probe, configure_connection,
        sqlite_file_creation_result, wal_checkpoint_truncate,
    };
    use crate::search::catalog::sqlite_index::{
        CatalogIndexDocument, CatalogIndexDocumentKind, CatalogIndexSnapshot,
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
            super::apply_migration(&mut connection, migration, &WorkspaceName::default())
                .expect("first apply");
            super::apply_migration(&mut connection, migration, &WorkspaceName::default())
                .expect("second apply");
        }

        let user_version: u32 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("user_version");
        assert_eq!(user_version, SEARCH_SQLITE_SCHEMA_VERSION);
    }

    #[test]
    fn opening_v2_adds_observed_retention_index_without_dropping_queue_data() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let mut connection = Connection::open(&path).expect("raw v2 connection");
        for migration in SEARCH_SQLITE_MIGRATIONS.iter().take(2) {
            super::apply_migration(&mut connection, migration, &WorkspaceName::default())
                .expect("apply v2 history");
        }
        seed_legacy_v4_observed_queue_job(&connection);
        assert!(!index_exists(
            &connection,
            "idx_observed_values_workspace_last_observed"
        ));
        drop(connection);

        let connection = open_current_search_connection(&path);

        assert_eq!(observed_queue_job_count(&connection), 1);
        assert!(index_exists(
            &connection,
            "idx_observed_values_workspace_last_observed"
        ));
    }

    #[test]
    fn opening_v3_invalidates_catalog_rows_without_durable_source_ownership() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let mut connection = Connection::open(&path).expect("raw v3 connection");
        for migration in SEARCH_SQLITE_MIGRATIONS.iter().take(3) {
            super::apply_migration(&mut connection, migration, &WorkspaceName::default())
                .expect("apply v3 history");
        }
        seed_catalog_document(&connection);
        seed_legacy_v4_observed_queue_job(&connection);
        connection
            .execute(
                "INSERT INTO search_meta (key, value) VALUES ('catalog_snapshot_fingerprint:default', 'legacy')",
                [],
            )
            .expect("seed legacy fingerprint");
        drop(connection);

        let connection = open_current_search_connection(&path);
        let catalog_fingerprint = connection
            .query_row(
                "SELECT value FROM search_meta WHERE key = 'catalog_snapshot_fingerprint:default'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .expect("catalog fingerprint query");

        assert_eq!(catalog_document_count(&connection), 0);
        assert_eq!(observed_queue_job_count(&connection), 1);
        assert_eq!(catalog_fingerprint, None);
        assert!(index_exists(
            &connection,
            "idx_catalog_source_owners_workspace_owner"
        ));
    }

    #[test]
    fn opening_v1_repairs_missing_search_meta_and_discards_unowned_catalog() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = v1_search_connection(&path);
        seed_catalog_document(&connection);
        connection
            .execute_batch("DROP TABLE search_meta")
            .expect("remove v1 metadata table");
        drop(connection);

        let connection = open_current_search_connection(&path);
        assert_eq!(catalog_document_count(&connection), 0);
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
    fn opening_v1_repairs_missing_catalog_fts_and_discards_unowned_catalog() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = v1_search_connection(&path);
        seed_catalog_document(&connection);
        connection
            .execute_batch("DROP TABLE catalog_documents_fts")
            .expect("remove v1 catalog FTS table");
        drop(connection);

        let connection = open_current_search_connection(&path);
        assert_eq!(catalog_document_count(&connection), 0);
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
                "INSERT INTO catalog_documents (workspace, doc_id, doc_kind, source_name, title, snapshot_fingerprint) VALUES ('default', 'doc', 'catalog_table', 'github', 'Issues', 'fingerprint')",
                [],
            )
            .expect("seed catalog document");
        connection
            .execute(
                "INSERT INTO catalog_source_owners (workspace, source_name, owner_source_name, snapshot_fingerprint) VALUES ('default', 'github', 'github', 'fingerprint')",
                [],
            )
            .expect("seed catalog source owner");
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
        let catalog_owner_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM catalog_source_owners WHERE workspace = 'default' AND owner_source_name = 'github'",
                [],
                |row| row.get(0),
            )
            .expect("catalog owner count");
        let generation_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM observed_source_generations WHERE workspace = 'default' AND source_name = 'github'",
                [],
                |row| row.get(0),
            )
            .expect("generation count");
        assert_eq!(catalog_count, 1);
        assert_eq!(observed_count, 1);
        assert_eq!(catalog_owner_count, 1);
        assert_eq!(generation_count, 0);
    }

    #[test]
    fn workspace_all_clear_rolls_back_every_data_class_when_epoch_advance_fails() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let connection = store.connect_for_test().expect("connect");
        seed_workspace_all_clear_rollback_fixture(&connection);
        let before = workspace_all_clear_rollback_snapshot(&connection);
        assert_eq!(before, expected_workspace_all_clear_rollback_snapshot());
        drop(connection);

        store
            .clear_workspace_all()
            .expect_err("combined clear should fail");

        let connection = store.connect_for_test().expect("reconnect");
        let after = workspace_all_clear_rollback_snapshot(&connection);
        assert_eq!(
            after, before,
            "failed clear must roll back every data class"
        );
    }

    #[test]
    fn source_all_clear_uses_persisted_catalog_ownership_after_restart() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SqliteSearchStore::open(&path, workspace.clone()).expect("store");
        store
            .refresh_catalog_projection(&CatalogIndexSnapshot {
                fingerprint: "multi-component-v1".to_string(),
                documents: vec![
                    catalog_document("github_v4", "github_v4_rest", "issues"),
                    catalog_document("github_v4", "github_v4_mcp", "search_issues"),
                    catalog_document("slack_v4", "slack_v4_rest", "messages"),
                ],
            })
            .expect("refresh catalog projection");
        let connection = store.connect_for_test().expect("connect");
        for (source_name, display_value) in [
            ("github_v4", "Payment issue"),
            ("slack_v4", "Payment message"),
        ] {
            connection
                .execute(
                    "INSERT INTO observed_values (workspace, source_name, source_scope_id, surface_kind, surface_name, column_name, value_key, display_value, search_text, source_generation, workspace_generation) VALUES ('default', ?1, 'scope', 'table', 'items', 'title', ?2, ?2, ?2, 0, 0)",
                    (source_name, display_value),
                )
                .expect("seed observed value");
        }
        drop(connection);
        drop(store);

        let reopened = SqliteSearchStore::open(&path, workspace).expect("reopen store");
        let (catalog, observed) = reopened
            .clear_source_all("github_v4")
            .expect("clear installed source");

        assert_eq!(catalog.deleted_document_count, 2);
        assert_eq!(observed.values, 1);
        let connection = reopened.connect_for_test().expect("reconnect");
        let remaining_catalog_sources = connection
            .prepare(
                "SELECT source_name FROM catalog_documents WHERE workspace = 'default' ORDER BY source_name",
            )
            .expect("prepare catalog sources")
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query catalog sources")
            .collect::<Result<Vec<_>, _>>()
            .expect("catalog sources");
        let remaining_observed_sources = connection
            .prepare(
                "SELECT source_name FROM observed_values WHERE workspace = 'default' ORDER BY source_name",
            )
            .expect("prepare observed sources")
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query observed sources")
            .collect::<Result<Vec<_>, _>>()
            .expect("observed sources");
        assert_eq!(remaining_catalog_sources, ["slack_v4_rest"]);
        assert_eq!(remaining_observed_sources, ["slack_v4"]);
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
        for table_name in [
            "search_meta",
            "catalog_documents",
            "catalog_documents_fts",
            "catalog_source_owners",
        ] {
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
    fn opening_current_version_repairs_missing_catalog_ownership_and_invalidates_rows() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = open_current_search_connection(&path);
        seed_catalog_document(&connection);
        connection
            .execute(
                "INSERT INTO search_meta (key, value) VALUES ('catalog_snapshot_fingerprint:default', 'current')",
                [],
            )
            .expect("seed catalog fingerprint");
        connection
            .execute_batch("DROP TABLE catalog_source_owners")
            .expect("remove ownership table");
        drop(connection);

        let connection = open_current_search_connection(&path);
        let catalog_fingerprint = connection
            .query_row(
                "SELECT value FROM search_meta WHERE key = 'catalog_snapshot_fingerprint:default'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .expect("catalog fingerprint query");

        assert_eq!(catalog_document_count(&connection), 0);
        assert_eq!(catalog_fingerprint, None);
        assert!(index_exists(
            &connection,
            "idx_catalog_source_owners_workspace_owner"
        ));
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
            &WorkspaceName::default(),
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
                    snapshot_fingerprint
                ) VALUES ('default', 'fixture', 'catalog_table', 'Fixture', 'fixture')
                ",
                [],
            )
            .expect("seed catalog document");
    }

    fn catalog_document(
        owner_source_name: &str,
        source_name: &str,
        surface_name: &str,
    ) -> CatalogIndexDocument {
        let qualified_name = format!("{source_name}.{surface_name}");
        CatalogIndexDocument {
            doc_id: format!("catalog:table:{qualified_name}"),
            doc_kind: CatalogIndexDocumentKind::CatalogTable,
            owner_source_name: owner_source_name.to_string(),
            source_name: source_name.to_string(),
            surface_kind: "table".to_string(),
            surface_name: surface_name.to_string(),
            field_name: String::new(),
            field_role: String::new(),
            qualified_name,
            title: surface_name.to_string(),
            description: String::new(),
            searchable_text: surface_name.to_string(),
            catalog_name: None,
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct WorkspaceAllClearRollbackSnapshot {
        catalog_documents: i64,
        catalog_fts_documents: i64,
        catalog_source_owners: i64,
        catalog_fingerprints: i64,
        observed_values: i64,
        observed_fts_values: i64,
        observed_queue_jobs: i64,
        observed_workspace_generations: i64,
    }

    fn seed_workspace_all_clear_rollback_fixture(connection: &Connection) {
        connection
            .execute_batch(
                "
                INSERT INTO catalog_documents (workspace, doc_id, doc_kind, source_name, title, snapshot_fingerprint)
                VALUES ('default', 'doc', 'catalog_table', 'github', 'Issues', 'fingerprint');
                INSERT INTO catalog_documents_fts (workspace, doc_id, title, qualified_name, description, searchable_text)
                VALUES ('default', 'doc', 'Issues', 'github.issues', 'GitHub issues', 'github issues');
                INSERT INTO catalog_source_owners (workspace, source_name, owner_source_name, snapshot_fingerprint)
                VALUES ('default', 'github', 'github', 'fingerprint');
                INSERT INTO search_meta (key, value)
                VALUES ('catalog_snapshot_fingerprint:default', 'fingerprint');
                INSERT INTO observed_values (workspace, source_name, source_scope_id, surface_kind, surface_name, column_name, value_key, display_value, search_text, source_generation, workspace_generation)
                VALUES ('default', 'github', 'scope', 'table', 'issues', 'title', 'value', 'Payment issue', 'payment issue', 0, 0);
                INSERT INTO observed_values_fts (workspace, source_name, source_scope_id, surface_kind, surface_name, column_name, value_key, display_value, search_text)
                VALUES ('default', 'github', 'scope', 'table', 'issues', 'title', 'value', 'Payment issue', 'payment issue');
                CREATE TRIGGER fail_workspace_epoch_insert
                BEFORE INSERT ON observed_workspace_generations
                BEGIN
                    SELECT RAISE(ABORT, 'forced workspace epoch failure');
                END;
                ",
            )
            .expect("seed workspace clear rollback fixture");
        seed_observed_queue_job(connection);
    }

    fn workspace_all_clear_rollback_snapshot(
        connection: &Connection,
    ) -> WorkspaceAllClearRollbackSnapshot {
        WorkspaceAllClearRollbackSnapshot {
            catalog_documents: matching_row_count(
                connection,
                "SELECT COUNT(*) FROM catalog_documents WHERE workspace = 'default'",
                "catalog document count",
            ),
            catalog_fts_documents: matching_row_count(
                connection,
                "SELECT COUNT(*) FROM catalog_documents_fts WHERE workspace = 'default' AND doc_id = 'doc' AND title = 'Issues' AND qualified_name = 'github.issues' AND description = 'GitHub issues' AND searchable_text = 'github issues'",
                "catalog FTS document count",
            ),
            catalog_source_owners: matching_row_count(
                connection,
                "SELECT COUNT(*) FROM catalog_source_owners WHERE workspace = 'default'",
                "catalog source owner count",
            ),
            catalog_fingerprints: matching_row_count(
                connection,
                "SELECT COUNT(*) FROM search_meta WHERE key = 'catalog_snapshot_fingerprint:default' AND value = 'fingerprint'",
                "catalog fingerprint count",
            ),
            observed_values: matching_row_count(
                connection,
                "SELECT COUNT(*) FROM observed_values WHERE workspace = 'default'",
                "observed value count",
            ),
            observed_fts_values: matching_row_count(
                connection,
                "SELECT COUNT(*) FROM observed_values_fts WHERE workspace = 'default' AND source_name = 'github' AND source_scope_id = 'scope' AND surface_kind = 'table' AND surface_name = 'issues' AND column_name = 'title' AND value_key = 'value' AND display_value = 'Payment issue' AND search_text = 'payment issue'",
                "observed FTS value count",
            ),
            observed_queue_jobs: matching_row_count(
                connection,
                "SELECT COUNT(*) FROM observed_queue_jobs WHERE workspace = 'default' AND source_name = 'github' AND source_scope_id = 'fixture-scope' AND surface_kind = 'table' AND surface_name = 'issues' AND workspace_generation = 0 AND source_generation = 0 AND payload_json = '{}'",
                "observed queue job count",
            ),
            observed_workspace_generations: matching_row_count(
                connection,
                "SELECT COUNT(*) FROM observed_workspace_generations WHERE workspace = 'default'",
                "observed workspace generation count",
            ),
        }
    }

    fn expected_workspace_all_clear_rollback_snapshot() -> WorkspaceAllClearRollbackSnapshot {
        WorkspaceAllClearRollbackSnapshot {
            catalog_documents: 1,
            catalog_fts_documents: 1,
            catalog_source_owners: 1,
            catalog_fingerprints: 1,
            observed_values: 1,
            observed_fts_values: 1,
            observed_queue_jobs: 1,
            observed_workspace_generations: 0,
        }
    }

    // ---- version-5 observed-identity migration -------------------------------

    #[test]
    fn seeded_v4_upgrade_preserves_equal_identity_observed_rows() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = seed_v4_observed_fixture(&path);
        drop(connection);

        let connection = open_current_search_connection(&path);

        assert_eq!(
            observed_source_names(&connection, "observed_values"),
            ["github_mcp_v4", "github_v4"],
            "rows whose two identities were equal must survive"
        );
        // Preserved values are searchable the moment the migration commits --
        // the FTS index is rebuilt from the canonical table in the same
        // transaction, so there is no reconcile gap.
        assert_eq!(
            matching_row_count(
                &connection,
                "SELECT COUNT(*) FROM observed_values_fts WHERE observed_values_fts MATCH 'payment'",
                "searchable preserved values",
            ),
            2
        );
        assert_eq!(
            observed_source_names(&connection, "observed_values_fts"),
            ["github_mcp_v4", "github_v4"]
        );
        assert_eq!(
            observed_source_names(&connection, "observed_queue_jobs"),
            ["github_v4"],
            "queue jobs follow the same preserve-equal-rows rule"
        );
        assert_eq!(
            matching_row_count(
                &connection,
                "SELECT id FROM observed_queue_jobs",
                "preserved queue job id",
            ),
            7,
            "explicit ids are carried across so queue ordering is stable"
        );
        assert_eq!(
            matching_row_count(
                &connection,
                "SELECT generation FROM observed_workspace_generations WHERE workspace = 'default'",
                "workspace generation",
            ),
            3
        );
        assert_eq!(
            matching_row_count(
                &connection,
                "SELECT generation FROM observed_source_generations WHERE source_name = 'github_v4'",
                "source generation",
            ),
            5
        );
        assert!(
            !observed_tables_carry_owner_column(&connection),
            "no observed table may keep the legacy owner column"
        );
    }

    #[test]
    fn already_singular_schema_replay_preserves_observed_rows() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = open_current_search_connection(&path);
        seed_singular_observed_value(&connection, "github_v4", "payment outage");
        seed_observed_queue_job(&connection);
        drop(connection);

        // Reopening replays the whole idempotent history; the version-5 hook
        // must be a complete no-op against a schema that is already singular.
        let connection = open_current_search_connection(&path);

        assert_eq!(observed_value_count(&connection), 1);
        assert_eq!(observed_queue_job_count(&connection), 1);
    }

    #[test]
    fn mis_stamped_v5_is_repaired_with_data_preserved() {
        for legacy_table in [
            "observed_values",
            "observed_values_fts",
            "observed_queue_jobs",
        ] {
            let temp = tempdir().expect("tempdir");
            let path = temp.path().join("search.sqlite3");
            let connection = seed_v4_observed_fixture(&path);
            if legacy_table != "observed_values" {
                // Leave the owner column on only one table, so the repair path
                // cannot lean on detecting it everywhere.
                make_observed_values_singular(&connection);
            }
            if legacy_table != "observed_queue_jobs" {
                make_observed_queue_jobs_singular(&connection);
            }
            if legacy_table == "observed_queue_jobs" {
                rebuild_observed_fts_as_singular(&connection);
            }
            connection
                .pragma_update(None, "user_version", SEARCH_SQLITE_SCHEMA_VERSION)
                .expect("mis-stamp the schema version");
            assert!(
                !super::schema_is_current(&connection).expect("validate mis-stamped schema"),
                "a surplus owner column on {legacy_table} must fail validation"
            );
            drop(connection);

            let connection = open_current_search_connection(&path);

            assert!(
                observed_value_count(&connection) > 0,
                "repairing a mis-stamped {legacy_table} must preserve observed data"
            );
            assert!(!observed_tables_carry_owner_column(&connection));
        }
    }

    #[test]
    fn missing_singular_index_is_repaired_without_discarding_observed_data() {
        // Migration 0002 builds three of the five observed indexes over
        // `owner_source_name`. On a singular database a *missing* one of those
        // cannot be replayed, and before the singular-era repair that failure
        // routed intact data into the discarding reset.
        for index_name in [
            "idx_observed_values_source",
            "idx_observed_queue_jobs_source",
            "idx_observed_queue_jobs_pending_scope",
        ] {
            let temp = tempdir().expect("tempdir");
            let path = temp.path().join("search.sqlite3");
            let connection = open_current_search_connection(&path);
            seed_singular_observed_value(&connection, "github_v4", "payment outage");
            seed_observed_queue_job(&connection);
            connection
                .execute_batch(&format!("DROP INDEX {index_name}"))
                .expect("drop a singular index");
            assert!(
                !super::schema_is_current(&connection).expect("validate damaged schema"),
                "a missing {index_name} must fail validation"
            );
            drop(connection);

            let connection = open_current_search_connection(&path);

            assert_eq!(
                observed_value_count(&connection),
                1,
                "repairing a missing {index_name} must preserve observed values"
            );
            assert_eq!(
                observed_queue_job_count(&connection),
                1,
                "repairing a missing {index_name} must preserve queued jobs"
            );
            assert!(index_exists(&connection, index_name));
        }
    }

    #[test]
    fn fts_repair_keeps_fts_rowids_aligned_with_gapped_canonical_rowids() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = open_current_search_connection(&path);
        for source_name in ["source_a", "source_b", "source_c"] {
            seed_singular_observed_value(&connection, source_name, "payment outage");
        }
        // Eviction and stale purges leave gaps, so canonical rowids are not a
        // dense 1..N sequence the FTS index can reproduce by counting.
        connection
            .execute("DELETE FROM observed_values WHERE rowid = 1", [])
            .expect("evict the first canonical row");
        rebuild_observed_fts_as_owner_era(&connection);
        drop(connection);

        let connection = open_current_search_connection(&path);

        let misaligned: i64 = connection
            .query_row(
                "
                SELECT COUNT(*)
                FROM observed_values v
                LEFT JOIN observed_values_fts f
                    ON f.rowid = v.rowid
                   AND f.value_key = v.value_key
                WHERE f.rowid IS NULL
                ",
                [],
                |row| row.get(0),
            )
            .expect("rowid alignment query");
        assert_eq!(
            misaligned, 0,
            "every preserved row must keep its canonical rowid in the FTS index"
        );
        assert_eq!(observed_value_count(&connection), 2);
    }

    /// Replaces the FTS index with the pre-#1791 shape, leaving the canonical
    /// table singular -- the partial-repair state the version-5 hook heals.
    fn rebuild_observed_fts_as_owner_era(connection: &Connection) {
        connection
            .execute_batch(
                "
                DROP TABLE observed_values_fts;
                CREATE VIRTUAL TABLE observed_values_fts USING fts5(
                    workspace UNINDEXED,
                    owner_source_name UNINDEXED,
                    source_name UNINDEXED,
                    source_scope_id UNINDEXED,
                    surface_kind UNINDEXED,
                    surface_name UNINDEXED,
                    column_name UNINDEXED,
                    value_key UNINDEXED,
                    display_value,
                    search_text,
                    tokenize = 'trigram'
                );
                ",
            )
            .expect("install an owner-era FTS index");
    }

    fn seed_v4_observed_fixture(path: &std::path::Path) -> Connection {
        let mut connection = Connection::open(path).expect("raw v4 connection");
        for migration in SEARCH_SQLITE_MIGRATIONS.iter().take(4) {
            super::apply_migration(&mut connection, migration, &WorkspaceName::default())
                .expect("apply v4 history");
        }
        connection
            .execute_batch(
                "
                INSERT INTO observed_values (
                    workspace, owner_source_name, source_name, source_scope_id, surface_kind,
                    surface_name, column_name, value_key, display_value, search_text,
                    source_generation, workspace_generation
                ) VALUES
                    ('default', 'github_v4', 'github_v4', 'rest-scope', 'table', 'issues',
                     'title', 'rest', 'REST payment outage', 'rest payment outage', 0, 0),
                    ('default', 'github_mcp_v4', 'github_mcp_v4', 'mcp-scope', 'table', 'issues',
                     'title', 'mcp', 'MCP payment outage', 'mcp payment outage', 0, 0),
                    ('default', 'github_v4', 'github_v4_rest', 'rest-scope', 'table', 'issues',
                     'title', 'divergent', 'Divergent value', 'divergent value', 0, 0);

                INSERT INTO observed_values_fts (
                    workspace, owner_source_name, source_name, source_scope_id, surface_kind,
                    surface_name, column_name, value_key, display_value, search_text
                )
                SELECT workspace, owner_source_name, source_name, source_scope_id, surface_kind,
                       surface_name, column_name, value_key, display_value, search_text
                FROM observed_values;

                INSERT INTO observed_queue_jobs (
                    id, workspace, owner_source_name, source_name, source_scope_id, surface_kind,
                    surface_name, workspace_generation, source_generation, payload_json
                ) VALUES
                    (7, 'default', 'github_v4', 'github_v4', 'rest-scope', 'table', 'issues',
                     0, 0, '{}'),
                    (8, 'default', 'github_v4', 'github_v4_rest', 'rest-scope', 'table', 'pulls',
                     0, 0, '{}');

                INSERT INTO observed_workspace_generations (workspace, generation)
                VALUES ('default', 3);
                INSERT INTO observed_source_generations (workspace, source_name, generation)
                VALUES ('default', 'github_v4', 5);
                ",
            )
            .expect("seed v4 observed fixture");
        connection
    }

    fn seed_singular_observed_value(connection: &Connection, source_name: &str, search_text: &str) {
        connection
            .execute(
                "
                INSERT INTO observed_values (
                    workspace, source_name, source_scope_id, surface_kind, surface_name,
                    column_name, value_key, display_value, search_text,
                    source_generation, workspace_generation
                ) VALUES ('default', ?1, 'scope', 'table', 'issues', 'title', 'key', ?2, ?2, 0, 0)
                ",
                (source_name, search_text),
            )
            .expect("seed singular observed value");
    }

    fn make_observed_values_singular(connection: &Connection) {
        connection
            .execute_batch(
                "
                DROP INDEX IF EXISTS idx_observed_values_source;
                DROP INDEX IF EXISTS idx_observed_values_workspace_last_observed;
                ALTER TABLE observed_values RENAME TO observed_values_owner_era;
                ",
            )
            .expect("move the owner-era canonical table aside");
        connection
            .execute_batch(include_str!("migrations/0005_observed_source_identity.sql"))
            .expect("create the singular canonical table");
        connection
            .execute_batch(
                "
                INSERT INTO observed_values (
                    workspace, source_name, source_scope_id, surface_kind, surface_name,
                    column_name, value_key, display_value, search_text, first_observed_at,
                    last_observed_at, observation_count, source_generation, workspace_generation
                )
                SELECT workspace, source_name, source_scope_id, surface_kind, surface_name,
                       column_name, value_key, display_value, search_text, first_observed_at,
                       last_observed_at, observation_count, source_generation, workspace_generation
                FROM observed_values_owner_era
                WHERE owner_source_name = source_name;
                DROP TABLE observed_values_owner_era;
                ",
            )
            .expect("copy rows into the singular canonical table");
    }

    fn make_observed_queue_jobs_singular(connection: &Connection) {
        connection
            .execute_batch(
                "
                DROP INDEX IF EXISTS idx_observed_queue_jobs_workspace_id;
                DROP INDEX IF EXISTS idx_observed_queue_jobs_source;
                DROP INDEX IF EXISTS idx_observed_queue_jobs_pending_scope;
                DROP TABLE observed_queue_jobs;
                ",
            )
            .expect("drop the owner-era queue table");
        connection
            .execute_batch(include_str!("migrations/0005_observed_source_identity.sql"))
            .expect("create the singular queue table");
    }

    fn rebuild_observed_fts_as_singular(connection: &Connection) {
        connection
            .execute_batch("DROP TABLE observed_values_fts")
            .expect("drop the owner-era FTS table");
        connection
            .execute_batch(include_str!("migrations/0005_observed_source_identity.sql"))
            .expect("create the singular FTS table");
    }

    fn observed_source_names(connection: &Connection, table_name: &str) -> Vec<String> {
        let sql = format!("SELECT source_name FROM {table_name} ORDER BY source_name");
        let mut statement = connection.prepare(&sql).expect("source-name query");
        statement
            .query_map([], |row| row.get(0))
            .expect("query source names")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect source names")
    }

    fn observed_tables_carry_owner_column(connection: &Connection) -> bool {
        [
            "observed_values",
            "observed_values_fts",
            "observed_queue_jobs",
        ]
        .into_iter()
        .any(|table_name| {
            super::schema_query_is_valid(
                connection,
                &format!("SELECT owner_source_name FROM {table_name} LIMIT 0"),
            )
            .expect("owner-column probe")
        })
    }

    fn observed_value_count(connection: &Connection) -> i64 {
        connection
            .query_row("SELECT COUNT(*) FROM observed_values", [], |row| row.get(0))
            .expect("observed value count")
    }

    fn matching_row_count(connection: &Connection, query: &str, context: &str) -> i64 {
        connection
            .query_row(query, [], |row| row.get(0))
            .expect(context)
    }

    /// Seeds the pre-#1791 owner/component shape, for tests that build a v2/v3
    /// era database and then upgrade through the version-5 copy path.
    fn seed_legacy_v4_observed_queue_job(connection: &Connection) {
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
            .expect("seed legacy observed queue job");
    }

    fn seed_observed_queue_job(connection: &Connection) {
        connection
            .execute(
                "
                INSERT INTO observed_queue_jobs (
                    workspace,
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

    fn index_exists(connection: &Connection, index_name: &str) -> bool {
        connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'index' AND name = ?1)",
                [index_name],
                |row| row.get(0),
            )
            .expect("index lookup")
    }
}
