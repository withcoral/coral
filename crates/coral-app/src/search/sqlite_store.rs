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

use rusqlite::{Connection, ErrorCode};

use crate::search::catalog::sqlite_index::{
    CatalogClearResult, CatalogIndexSnapshot, CatalogRefreshResult, CatalogSearchHits,
    SqliteCatalogIndex,
};
use crate::state::AppStateLayout;
use crate::storage::fs::create_new_file_private;
use crate::workspaces::WorkspaceName;

pub(crate) const SEARCH_SQLITE_SCHEMA_VERSION: u32 = 1;

struct SearchSqliteMigration {
    version: u32,
    sql: &'static str,
}

// These migrations are intentionally local to the SQLite search sidecar. The
// schema uses SQLite FTS5/trigram features that are not portable to the shared
// app database migration stream.
const SEARCH_SQLITE_MIGRATIONS: &[SearchSqliteMigration] = &[SearchSqliteMigration {
    version: 1,
    sql: include_str!("migrations/0001_catalog_search.sql"),
}];

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

    fn connect(&self) -> Result<Connection, SqliteSearchError> {
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

    pub(crate) fn catalog_document_count(&self) -> Result<u32, SqliteSearchError> {
        let connection = self.connect()?;
        SqliteCatalogIndex::new().document_count(&connection, &self.workspace_name)
    }

    pub(crate) fn clear_catalog_source(
        &self,
        source_name: &str,
    ) -> Result<CatalogClearResult, SqliteSearchError> {
        let mut connection = self.connect()?;
        SqliteCatalogIndex::new().clear_source(&mut connection, &self.workspace_name, source_name)
    }

    pub(crate) fn clear_catalog_workspace(&self) -> Result<CatalogClearResult, SqliteSearchError> {
        let mut connection = self.connect()?;
        SqliteCatalogIndex::new().clear_workspace(&mut connection, &self.workspace_name)
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
}

fn ensure_sqlite_file(path: &Path) -> Result<(), SqliteSearchError> {
    match create_new_file_private(path) {
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

    let repair_current_version = user_version == SEARCH_SQLITE_SCHEMA_VERSION;
    for migration in SEARCH_SQLITE_MIGRATIONS {
        let should_apply = migration.version > user_version
            || (repair_current_version && migration.version == user_version);
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

    for table_name in ["search_meta", "catalog_documents", "catalog_documents_fts"] {
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

    Ok(true)
}

#[cfg(test)]
mod tests {
    use rusqlite::OptionalExtension as _;
    use tempfile::tempdir;

    use super::{
        SEARCH_SQLITE_MIGRATIONS, SEARCH_SQLITE_SCHEMA_VERSION, SqliteSearchError,
        SqliteSearchStore,
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
