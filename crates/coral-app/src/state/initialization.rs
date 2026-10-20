//! Owns state-format validation and fresh-state initialization.
//!
//! The [`initialize`] Interface is the only production path from a discovered
//! state layout to a migration-ready database. It first performs every
//! filesystem and database inspection without mutation. Only after both stores
//! are accepted does it publish the filesystem marker, create the layout, run
//! the embedded database migration, and prepare Universal Search's
//! backend-specific storage on top of it. Rejected state is therefore unchanged.
//!
//! Bootstrap supplies one captured configuration document. Database
//! configuration resolution is injected because process bootstrap owns
//! environment access. The resolver is called only after the filesystem
//! fence accepts the state root. Configuration validation is injected too and
//! runs after both inspections and before the first write, so retired state is
//! reported first and invalid configuration leaves a fresh root unchanged.
//!
//! The Module also makes one runtime the state root's only user. Once the
//! filesystem root is accepted, it locks the root's existing lock file, so a
//! second runtime is refused before it inspects a database another runtime
//! is using. A root without a lock file gets one only after every inspection
//! accepts it, and the database is inspected again under that lock. The
//! returned [`InitializedState`] carries the lock to whoever uses the state.
//! Callers do not coordinate any part of the state-format sequence themselves.

use std::sync::Arc;

use super::database_format::validate_database_state;
use super::format::{initialize_state_root, validate_state_root};
use super::{AppStateLayout, StateLock};
use crate::bootstrap::AppError;
use coral_db::{Database, DatabaseOptions};

/// State that has passed the format fence and completed fresh initialization.
pub(crate) struct InitializedState {
    database: Arc<Database>,
    credential_key_path: std::path::PathBuf,
    lock: StateLock,
}

impl InitializedState {
    /// Returns the database and the state root's lock. Keep the lock for as
    /// long as anything uses the state.
    pub(crate) fn into_parts(self) -> (Arc<Database>, StateLock) {
        (self.database, self.lock)
    }

    /// The key file the host credential subsystem uses unless the deployment
    /// names a secret-held key instead; bootstrap makes that choice.
    pub(crate) fn credential_key_path(&self) -> &std::path::Path {
        &self.credential_key_path
    }
}

/// Accepts current or provably fresh state and returns its initialized stores.
///
/// # Errors
///
/// Returns [`AppError::FailedPrecondition`] without mutation when either the
/// filesystem root or configured database contains unmarked or malformed
/// state. Configuration failures are also returned without mutation. Returns
/// [`AppError::StateLocked`] without mutation when another runtime holds the
/// state root. I/O,
/// connection, and migration failures are returned after the latest stage that
/// completed successfully.
pub(crate) async fn initialize<F, V>(
    layout: AppStateLayout,
    config: &str,
    resolve_database: F,
    validate_config: V,
) -> Result<InitializedState, AppError>
where
    F: FnOnce(&AppStateLayout, &str) -> Result<DatabaseOptions, AppError>,
    V: FnOnce() -> Result<(), AppError>,
{
    validate_state_root(&layout, config)?;
    let held = StateLock::acquire_existing(&layout)?;
    let database_config = resolve_database(&layout, config)?;
    validate_database_state(&database_config).await?;
    validate_config()?;

    let lock = if let Some(lock) = held {
        lock
    } else {
        let lock = StateLock::acquire(&layout)?;
        // Another runtime may have used the stores before this lock.
        validate_database_state(&database_config).await?;
        lock
    };
    // This re-inspects the filesystem root under the lock.
    initialize_state_root(&layout, config)?;
    layout.ensure()?;
    let database = Database::connect(database_config).await?;
    // The chain's search steps need the features this checks.
    coral_search::prepare_storage(&database).await?;
    super::migrations::migrate_current(&database).await?;

    Ok(InitializedState {
        credential_key_path: layout.config_dir().join("credentials.key"),
        database: Arc::new(database),
        lock,
    })
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use tempfile::TempDir;

    use crate::bootstrap::AppError;
    use crate::state::{AppStateLayout, LOCK_FILE_NAME, StateLock};

    const CONFIG: &str = "[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n";

    /// Every path under `root` with its file bytes, so a test can tell that
    /// nothing changed.
    fn snapshot(root: &Path) -> Vec<(PathBuf, Option<Vec<u8>>)> {
        let mut entries = Vec::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(dir) = pending.pop() {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path.clone());
                    entries.push((path, None));
                } else if path.file_name() == Some(LOCK_FILE_NAME.as_ref()) {
                    // A held lock file cannot be read on Windows. It holds no bytes.
                    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
                    entries.push((path, Some(Vec::new())));
                } else {
                    let bytes = std::fs::read(&path).unwrap();
                    entries.push((path, Some(bytes)));
                }
            }
        }
        entries.sort();
        entries
    }

    #[tokio::test]
    async fn a_held_state_root_refuses_another_runtime_without_change() {
        for initialized in [false, true] {
            let temp = TempDir::new().unwrap();
            std::fs::write(temp.path().join("config.toml"), CONFIG).unwrap();
            // A running server, or one that has only taken the lock on a fresh root.
            let held: Box<dyn std::any::Any> = if initialized {
                Box::new(
                    crate::ServerBuilder::new()
                        .with_config_dir(temp.path())
                        .prepare()
                        .await
                        .unwrap(),
                )
            } else {
                let layout = AppStateLayout::discover(Some(temp.path().to_path_buf())).unwrap();
                Box::new(StateLock::acquire(&layout).unwrap())
            };
            let before = snapshot(temp.path());

            let refused = crate::ServerBuilder::new()
                .with_config_dir(temp.path())
                .prepare()
                .await;

            let Err(AppError::StateLocked { path }) = refused else {
                panic!("initialized {initialized}: a held root must refuse a second runtime");
            };
            assert_eq!(path, temp.path(), "initialized {initialized}");
            assert_eq!(snapshot(temp.path()), before, "initialized {initialized}");
            drop(held);
            crate::ServerBuilder::new()
                .with_config_dir(temp.path())
                .prepare()
                .await
                .unwrap();
        }
    }
}
