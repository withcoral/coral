use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, params};

use super::{
    AppStorageError, AppStoreBackend, AppStoreWriteTransaction, OpenEpisodeResult, StoredEpisode,
    StoredFeedbackReport,
};
use crate::storage::fs as storage_fs;

const SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS app_schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS app_data_migrations (
    name TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS episodes (
    workspace TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    intent TEXT NOT NULL,
    parent_episode_id TEXT,
    created_at_unix_nanos INTEGER NOT NULL,
    record_bytes INTEGER NOT NULL CHECK (record_bytes >= 0),
    PRIMARY KEY (workspace, episode_id)
);

CREATE TABLE IF NOT EXISTS feedback_reports (
    workspace TEXT NOT NULL,
    report_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    trying_to_do TEXT NOT NULL,
    tried TEXT NOT NULL,
    stuck TEXT NOT NULL,
    PRIMARY KEY (workspace, report_id)
);

CREATE INDEX IF NOT EXISTS feedback_reports_workspace_created_idx
    ON feedback_reports (workspace, created_at, report_id);

INSERT OR IGNORE INTO app_schema_migrations (version) VALUES (1);
";

pub(super) struct SqliteAppStore {
    path: PathBuf,
    connection: Mutex<Connection>,
}

impl SqliteAppStore {
    pub(super) fn open(path: PathBuf) -> Result<Self, AppStorageError> {
        storage_fs::ensure_file_private(&path)?;
        let connection = Connection::open(&path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            r"
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
",
        )?;
        connection.execute_batch(SCHEMA)?;
        set_sqlite_file_permissions(&path)?;
        Ok(Self {
            path,
            connection: Mutex::new(connection),
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, Connection>, AppStorageError> {
        self.connection
            .lock()
            .map_err(|_error| AppStorageError::Poisoned)
    }
}

impl AppStoreBackend for SqliteAppStore {
    fn begin_write(&self) -> Result<Box<dyn AppStoreWriteTransaction + '_>, AppStorageError> {
        let connection = self.lock()?;
        connection.execute_batch("BEGIN IMMEDIATE")?;
        Ok(Box::new(SqliteWriteTransaction {
            path: self.path.clone(),
            connection,
            completed: false,
        }))
    }

    fn migration_applied(&self, name: &str) -> Result<bool, AppStorageError> {
        let connection = self.lock()?;
        migration_applied(&connection, name)
    }

    #[cfg(test)]
    fn test_read_episode(
        &self,
        workspace: &str,
        episode_id: &str,
    ) -> Result<Option<StoredEpisode>, AppStorageError> {
        let connection = self.lock()?;
        select_episode(&connection, workspace, episode_id)
    }

    #[cfg(test)]
    fn test_count_episodes(&self, workspace: &str) -> Result<usize, AppStorageError> {
        let connection = self.lock()?;
        let count = connection.query_row(
            "SELECT COUNT(*) FROM episodes WHERE workspace = ?1",
            params![workspace],
            |row| row.get::<_, i64>(0),
        )?;
        usize::try_from(count).map_err(|_error| AppStorageError::ValueOutOfRange {
            field: "episode_count",
            value: count.to_string(),
        })
    }

    #[cfg(test)]
    fn test_episode_bytes(&self, workspace: &str) -> Result<u64, AppStorageError> {
        let connection = self.lock()?;
        episode_bytes(&connection, workspace)
    }

    #[cfg(test)]
    fn test_read_feedback_reports(
        &self,
        workspace: &str,
    ) -> Result<Vec<StoredFeedbackReport>, AppStorageError> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(
            r"
SELECT report_id, workspace, created_at, trying_to_do, tried, stuck
FROM feedback_reports
WHERE workspace = ?1
ORDER BY rowid ASC
",
        )?;
        let rows = statement.query_map(params![workspace], |row| {
            Ok(StoredFeedbackReport {
                id: row.get(0)?,
                workspace: row.get(1)?,
                created_at_rfc3339: row.get(2)?,
                trying_to_do: row.get(3)?,
                tried: row.get(4)?,
                stuck: row.get(5)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }
}

struct SqliteWriteTransaction<'a> {
    path: PathBuf,
    connection: MutexGuard<'a, Connection>,
    completed: bool,
}

impl AppStoreWriteTransaction for SqliteWriteTransaction<'_> {
    fn open_episode(
        &mut self,
        episode: &StoredEpisode,
        max_bytes: u64,
    ) -> Result<OpenEpisodeResult, AppStorageError> {
        if let Some(existing) = select_episode(&self.connection, &episode.workspace, &episode.id)? {
            return if existing.intent == episode.intent
                && existing.parent_episode_id == episode.parent_episode_id
            {
                Ok(OpenEpisodeResult::AlreadyOpen)
            } else {
                Ok(OpenEpisodeResult::Conflict)
            };
        }

        self.connection.execute(
            r"
INSERT INTO episodes (
    workspace,
    episode_id,
    intent,
    parent_episode_id,
    created_at_unix_nanos,
    record_bytes
) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
",
            params![
                episode.workspace.as_str(),
                episode.id.as_str(),
                episode.intent.as_str(),
                episode.parent_episode_id.as_deref(),
                episode.created_at_unix_nanos,
                sqlite_i64("record_bytes", episode.record_bytes)?
            ],
        )?;
        prune_episodes(&self.connection, &episode.workspace, max_bytes)?;
        Ok(OpenEpisodeResult::Opened)
    }

    fn import_episode(&mut self, episode: &StoredEpisode) -> Result<(), AppStorageError> {
        self.connection.execute(
            r"
INSERT OR IGNORE INTO episodes (
    workspace,
    episode_id,
    intent,
    parent_episode_id,
    created_at_unix_nanos,
    record_bytes
) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
",
            params![
                episode.workspace.as_str(),
                episode.id.as_str(),
                episode.intent.as_str(),
                episode.parent_episode_id.as_deref(),
                episode.created_at_unix_nanos,
                sqlite_i64("record_bytes", episode.record_bytes)?
            ],
        )?;
        Ok(())
    }

    fn append_feedback_report(
        &mut self,
        report: &StoredFeedbackReport,
    ) -> Result<(), AppStorageError> {
        self.connection.execute(
            r"
INSERT INTO feedback_reports (
    workspace,
    report_id,
    created_at,
    trying_to_do,
    tried,
    stuck
) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
",
            params![
                report.workspace.as_str(),
                report.id.as_str(),
                report.created_at_rfc3339.as_str(),
                report.trying_to_do.as_str(),
                report.tried.as_str(),
                report.stuck.as_str()
            ],
        )?;
        Ok(())
    }

    fn import_feedback_report(
        &mut self,
        report: &StoredFeedbackReport,
    ) -> Result<(), AppStorageError> {
        self.connection.execute(
            r"
INSERT OR IGNORE INTO feedback_reports (
    workspace,
    report_id,
    created_at,
    trying_to_do,
    tried,
    stuck
) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
",
            params![
                report.workspace.as_str(),
                report.id.as_str(),
                report.created_at_rfc3339.as_str(),
                report.trying_to_do.as_str(),
                report.tried.as_str(),
                report.stuck.as_str()
            ],
        )?;
        Ok(())
    }

    fn mark_migration_applied(&mut self, name: &str) -> Result<(), AppStorageError> {
        self.connection.execute(
            "INSERT OR IGNORE INTO app_data_migrations (name) VALUES (?1)",
            params![name],
        )?;
        Ok(())
    }

    fn commit(mut self: Box<Self>) -> Result<(), AppStorageError> {
        self.connection.execute_batch("COMMIT")?;
        set_sqlite_file_permissions(&self.path)?;
        self.completed = true;
        Ok(())
    }
}

impl Drop for SqliteWriteTransaction<'_> {
    fn drop(&mut self) {
        if !self.completed {
            drop(self.connection.execute_batch("ROLLBACK"));
        }
    }
}

struct SqlEpisodeRow {
    workspace: String,
    id: String,
    intent: String,
    parent_episode_id: Option<String>,
    created_at_unix_nanos: i64,
    record_bytes: i64,
}

fn select_episode(
    connection: &Connection,
    workspace: &str,
    episode_id: &str,
) -> Result<Option<StoredEpisode>, AppStorageError> {
    let row = connection
        .query_row(
            r"
SELECT workspace, episode_id, intent, parent_episode_id, created_at_unix_nanos, record_bytes
FROM episodes
WHERE workspace = ?1 AND episode_id = ?2
",
            params![workspace, episode_id],
            |row| {
                Ok(SqlEpisodeRow {
                    workspace: row.get(0)?,
                    id: row.get(1)?,
                    intent: row.get(2)?,
                    parent_episode_id: row.get(3)?,
                    created_at_unix_nanos: row.get(4)?,
                    record_bytes: row.get(5)?,
                })
            },
        )
        .optional()?;
    row.map(stored_episode).transpose()
}

fn migration_applied(connection: &Connection, name: &str) -> Result<bool, AppStorageError> {
    let present = connection
        .query_row(
            "SELECT 1 FROM app_data_migrations WHERE name = ?1",
            params![name],
            |_row| Ok(()),
        )
        .optional()?
        .is_some();
    Ok(present)
}

fn stored_episode(row: SqlEpisodeRow) -> Result<StoredEpisode, AppStorageError> {
    let record_bytes = u64::try_from(row.record_bytes).map_err(|_error| {
        AppStorageError::Corrupt(format!(
            "episode '{}' in workspace '{}' has negative record_bytes",
            row.id, row.workspace
        ))
    })?;
    Ok(StoredEpisode {
        workspace: row.workspace,
        id: row.id,
        intent: row.intent,
        parent_episode_id: row.parent_episode_id,
        created_at_unix_nanos: row.created_at_unix_nanos,
        record_bytes,
    })
}

fn prune_episodes(
    connection: &Connection,
    workspace: &str,
    max_bytes: u64,
) -> Result<(), AppStorageError> {
    let mut total = sqlite_i64("episode_bytes", episode_bytes(connection, workspace)?)?;
    let max_bytes = sqlite_i64("max_bytes", max_bytes)?;
    if total <= max_bytes {
        return Ok(());
    }

    let mut statement = connection.prepare(
        r"
SELECT rowid, record_bytes
FROM episodes
WHERE workspace = ?1
ORDER BY rowid ASC
",
    )?;
    let rows = statement
        .query_map(params![workspace], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let row_count = rows.len();
    for (index, (rowid, record_bytes)) in rows.into_iter().enumerate() {
        if total <= max_bytes || index + 1 >= row_count {
            break;
        }
        connection.execute("DELETE FROM episodes WHERE rowid = ?1", params![rowid])?;
        total = total.saturating_sub(record_bytes);
    }
    Ok(())
}

fn episode_bytes(connection: &Connection, workspace: &str) -> Result<u64, AppStorageError> {
    let bytes = connection.query_row(
        "SELECT COALESCE(SUM(record_bytes), 0) FROM episodes WHERE workspace = ?1",
        params![workspace],
        |row| row.get::<_, i64>(0),
    )?;
    u64::try_from(bytes).map_err(|_error| {
        AppStorageError::Corrupt(format!(
            "workspace '{workspace}' has negative episode byte total"
        ))
    })
}

fn sqlite_i64(field: &'static str, value: u64) -> Result<i64, AppStorageError> {
    i64::try_from(value).map_err(|_error| AppStorageError::ValueOutOfRange {
        field,
        value: value.to_string(),
    })
}

fn set_sqlite_file_permissions(path: &Path) -> Result<(), AppStorageError> {
    storage_fs::set_file_permissions_private_if_exists(path)?;
    for sidecar in sqlite_sidecar_paths(path) {
        storage_fs::set_file_permissions_private_if_exists(&sidecar)?;
    }
    Ok(())
}

fn sqlite_sidecar_paths(path: &Path) -> [PathBuf; 2] {
    [
        sqlite_sidecar_path(path, "-wal"),
        sqlite_sidecar_path(path, "-shm"),
    ]
}

fn sqlite_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut raw = path.as_os_str().to_os_string();
    raw.push(suffix);
    PathBuf::from(raw)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;

    #[cfg(unix)]
    #[test]
    fn creates_database_and_wal_sidecars_with_private_permissions() {
        let temp = TempDir::new().expect("temp dir");
        let database = temp.path().join("nested").join("state.sqlite3");
        let store = SqliteAppStore::open(database.clone()).expect("sqlite app store");
        append_feedback_report(&store, "feedback-1");

        assert_eq!(mode(database.parent().expect("database parent")), 0o700);
        assert_eq!(mode(&database), 0o600);
        for sidecar in sqlite_sidecar_paths(&database) {
            assert!(
                sidecar.exists(),
                "sidecar should exist: {}",
                sidecar.display()
            );
            assert_eq!(mode(&sidecar), 0o600, "sidecar: {}", sidecar.display());
        }
    }

    #[cfg(unix)]
    #[test]
    fn tightens_existing_database_and_wal_sidecar_permissions() {
        let temp = TempDir::new().expect("temp dir");
        let database = temp.path().join("state.sqlite3");
        std::fs::write(&database, b"").expect("precreate database");
        set_mode(&database, 0o666);

        let store = SqliteAppStore::open(database.clone()).expect("sqlite app store");
        assert_eq!(mode(&database), 0o600);
        append_feedback_report(&store, "feedback-1");

        set_mode(&database, 0o666);
        for sidecar in sqlite_sidecar_paths(&database) {
            assert!(
                sidecar.exists(),
                "sidecar should exist before loosening: {}",
                sidecar.display()
            );
            set_mode(&sidecar, 0o666);
        }
        append_feedback_report(&store, "feedback-2");

        assert_eq!(mode(&database), 0o600);
        for sidecar in sqlite_sidecar_paths(&database) {
            assert_eq!(mode(&sidecar), 0o600, "sidecar: {}", sidecar.display());
        }
    }

    #[cfg(unix)]
    fn append_feedback_report(store: &SqliteAppStore, id: &str) {
        let mut transaction = store.begin_write().expect("begin write");
        transaction
            .append_feedback_report(&StoredFeedbackReport {
                id: id.to_string(),
                workspace: "default".to_string(),
                created_at_rfc3339: "2026-06-26T00:00:00Z".to_string(),
                trying_to_do: "trying".to_string(),
                tried: "tried".to_string(),
                stuck: "stuck".to_string(),
            })
            .expect("append feedback");
        transaction.commit().expect("commit");
    }

    #[cfg(unix)]
    fn set_mode(path: &Path, mode: u32) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
            .expect("set permissions");
    }

    #[cfg(unix)]
    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777
    }
}
