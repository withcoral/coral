//! Owns state-format validation and fresh-state initialization.
//!
//! The [`initialize`] Interface is the only production path from a discovered
//! state layout to a migration-ready database. It first performs every
//! filesystem and database inspection without mutation. Only after both stores
//! are accepted does it publish the filesystem marker, create the layout, check
//! Universal Search's database features, run the embedded database migration,
//! and prepare Universal Search's vector table on top of it. Rejected state is
//! therefore unchanged.
//!
//! Bootstrap supplies one captured configuration document. Database
//! configuration resolution is injected because process bootstrap owns
//! environment access. The resolver is called only after the filesystem
//! fence accepts the state root. Configuration validation is injected too and
//! runs after both inspections and before the first write, so retired state is
//! reported first and invalid configuration leaves a fresh root unchanged. It
//! returns the one setting storage preparation reads: Universal Search's
//! vector side.
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
    V: FnOnce() -> Result<coral_search::VectorStorage, AppError>,
{
    validate_state_root(&layout, config)?;
    let held = StateLock::acquire_existing(&layout)?;
    let database_config = resolve_database(&layout, config)?;
    validate_database_state(&database_config).await?;
    let vectors = validate_config()?;

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
    coral_search::prepare_storage(&database, vectors).await?;
    super::migrations::migrate_current(&database).await?;
    // After the chain: a table in a schema without the state marker reads
    // as pre-v1 state at the next boot.
    coral_search::prepare_vector_table(&database, vectors).await?;

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
    async fn a_fresh_postgres_boots_with_a_vector_table_only_while_the_setting_is_on() {
        coral_test_support::database_only!(Postgres);
        let url = crate::bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read Postgres test URL")
            .expect("the Postgres test selector requires CORAL_TEST_POSTGRES_URL");
        // A fixture puts the extensions the boot checks need in `public`, as
        // in a default deployment. Created by this boot, they would land in
        // its own schema, off every other test's search path.
        let _extensions = coral_test_support::TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("install the shared extensions");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("open Postgres");

        for (vectors, expected) in [
            (
                coral_search::VectorStorage::On { dimension: 3 },
                Some("halfvec(3)"),
            ),
            (coral_search::VectorStorage::Off, None),
        ] {
            let schema = format!("coral_boot_{}", uuid::Uuid::new_v4().simple());
            sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
                .execute(&pool)
                .await
                .expect("create the boot schema");
            let mut scoped = url::Url::parse(&url).expect("parse Postgres test URL");
            scoped
                .query_pairs_mut()
                .append_pair("options[search_path]", &format!("{schema},public"));
            let options = coral_db::DatabaseOptions::Postgres { url: scoped.into() };
            let temp = TempDir::new().unwrap();
            let layout = AppStateLayout::discover(Some(temp.path().to_path_buf())).unwrap();

            let state = super::initialize(layout, "", |_, _| Ok(options), || Ok(vectors))
                .await
                .unwrap_or_else(|error| panic!("{vectors:?}: boot: {error}"));

            let vector_type: Option<String> = sqlx::query_scalar(
                "SELECT format_type(a.atttypid, a.atttypmod) \
                 FROM pg_catalog.pg_attribute a \
                 JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 \
                 AND c.relname = 'search_entity_embeddings' \
                 AND a.attname = 'embedding'",
            )
            .bind(&schema)
            .fetch_optional(&pool)
            .await
            .expect("read the vector column");
            let (database, _lock) = state.into_parts();
            database.close().await;
            assert_eq!(vector_type.as_deref(), expected, "{vectors:?}");
        }
        pool.close().await;
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
