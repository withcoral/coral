//! Test-only composition for Coral's real database Adapters.
//!
//! [`TestDatabase`] is the process-bootstrap seam for backend-neutral database
//! tests. It selects `SQLite` by default and Postgres only when explicitly
//! requested, gives each fixture isolated storage, applies the caller-owned
//! migration plan, and passes an ordinary [`coral_db::Database`] to Modules.
//! Backend-specific tests use [`database_only!`] to return before arranging a
//! fixture when the other Adapter is selected.

mod context;
mod keys;
mod migration;
mod organization;
mod provider;
pub use context::{change, trace};
pub use keys::{RSA_TEST_PRIVATE_KEY_DER, RSA_TEST_PUBLIC_MODULUS, p256_pkcs8};
pub use migration::{edited_migration_plan, migration_plan_before, migration_plan_with};
pub use organization::provision_user;
pub use provider::fixture_request_id;

use std::sync::Arc;

pub use coral_credentials::CredentialValues as FixtureCredentialValues;
pub use coral_credentials::Secret as FixtureSecret;
pub use coral_db::DatabaseSystem;
use coral_db::{Database, DatabaseOptions, MigrationPlan};
use tempfile::TempDir;
use thiserror::Error;
use uuid::Uuid;

const TEST_DATABASE_ENV: &str = "CORAL_TEST_DATABASE";
const POSTGRES_URL_ENV: &str = "CORAL_TEST_POSTGRES_URL";

/// A failure to compose an isolated test database.
#[derive(Debug, Error)]
pub enum TestDatabaseError {
    /// Test bootstrap configuration is missing or invalid.
    #[error("{0}")]
    Config(String),
    /// The selected Coral database Adapter failed.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
    /// Postgres fixture administration failed.
    #[error("could not administer the Postgres test fixture: {0}")]
    PostgresAdministration(#[from] sqlx::Error),
    /// The configured Postgres URL could not be scoped to an isolated schema.
    #[error("invalid Postgres test URL: {0}")]
    PostgresUrl(#[from] url::ParseError),
    /// Temporary `SQLite` storage could not be created.
    #[error("could not create temporary SQLite storage: {0}")]
    SqliteStorage(#[from] std::io::Error),
}

/// One isolated real database fixture selected at test-process bootstrap.
///
/// The fixture retains temporary `SQLite` storage for the lifetime of its
/// database handle. Postgres fixtures use a unique schema inside the disposable
/// database supplied by `make postgres-tests`.
pub struct TestDatabase {
    database: Arc<Database>,
    options: DatabaseOptions,
    _sqlite_directory: Option<TempDir>,
}

impl TestDatabase {
    /// Creates an isolated database and applies its half of `plan`.
    ///
    /// With no `CORAL_TEST_DATABASE`, this creates a temporary `SQLite` file.
    /// `CORAL_TEST_DATABASE=postgres` requires `CORAL_TEST_POSTGRES_URL` and
    /// creates a unique schema in that disposable database.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bootstrap configuration, fixture creation,
    /// connection, or migration failure.
    pub async fn new(plan: &MigrationPlan) -> Result<Self, TestDatabaseError> {
        let selection = optional_environment(TEST_DATABASE_ENV)?;
        let postgres_url = optional_environment(POSTGRES_URL_ENV)?;
        match select_database(selection.as_deref(), postgres_url.as_deref())
            .map_err(TestDatabaseError::Config)?
        {
            DatabaseSystem::Sqlite => Self::sqlite(plan).await,
            DatabaseSystem::Postgres => {
                Self::postgres(
                    plan,
                    postgres_url.as_deref().ok_or_else(|| {
                        TestDatabaseError::Config(format!(
                            "{POSTGRES_URL_ENV} must be set when {TEST_DATABASE_ENV}=postgres"
                        ))
                    })?,
                )
                .await
            }
        }
    }

    /// Returns the database handle passed to domain Modules.
    #[must_use]
    pub fn database(&self) -> Arc<Database> {
        self.database.clone()
    }

    /// Returns the connection options for this fixture's isolated storage.
    ///
    /// Bootstrap checks that open their own connections use this to inspect
    /// the fixture.
    #[must_use]
    pub fn options(&self) -> &DatabaseOptions {
        &self.options
    }

    /// Opens another independent handle to the same isolated storage.
    ///
    /// This is intended for explicit multi-worker persistence contracts.
    ///
    /// # Errors
    ///
    /// Returns an error when the selected Adapter cannot open another handle.
    pub async fn connect_peer(&self) -> Result<Database, TestDatabaseError> {
        Database::connect(self.options.clone())
            .await
            .map_err(Into::into)
    }

    /// Opens a peer handle whose Postgres notification liveness check runs every `keepalive`.
    ///
    /// # Errors
    ///
    /// Returns an error when the selected Adapter cannot open another handle.
    pub async fn connect_peer_with_notification_keepalive(
        &self,
        keepalive: std::time::Duration,
    ) -> Result<Database, TestDatabaseError> {
        Database::connect_with_notification_keepalive(self.options.clone(), keepalive)
            .await
            .map_err(Into::into)
    }

    /// Opens a peer handle that waits at most `writer_wait` for the `SQLite`
    /// writer before a write fails as busy.
    ///
    /// # Errors
    ///
    /// Returns an error when the selected Adapter cannot open another handle.
    pub async fn connect_peer_with_writer_wait(
        &self,
        writer_wait: std::time::Duration,
    ) -> Result<Database, TestDatabaseError> {
        Database::connect_with_writer_wait(self.options.clone(), writer_wait)
            .await
            .map_err(Into::into)
    }

    /// Runs raw SQL against this fixture's isolated storage on a separate connection.
    ///
    /// Fault-injection tests use this to install or remove a trigger after the
    /// fixture is arranged. Tests must not use it to arrange or read domain state.
    /// Returns the rows the statements changed, so a rewrite can prove it matched.
    ///
    /// # Errors
    ///
    /// Returns an error when the connection or any statement fails.
    pub async fn execute_fixture_sql(&self, sql: &str) -> Result<u64, TestDatabaseError> {
        use sqlx::Connection as _;
        let sql = sqlx::AssertSqlSafe(sql.to_owned());
        match &self.options {
            DatabaseOptions::Sqlite { path } => {
                let mut connection = sqlx::SqliteConnection::connect_with(
                    &sqlx::sqlite::SqliteConnectOptions::new().filename(path),
                )
                .await?;
                let rows = sqlx::raw_sql(sql).execute(&mut connection).await?;
                connection.close().await?;
                Ok(rows.rows_affected())
            }
            DatabaseOptions::Postgres { url } => {
                let mut connection = sqlx::PgConnection::connect(url).await?;
                let rows = sqlx::raw_sql(sql).execute(&mut connection).await?;
                connection.close().await?;
                Ok(rows.rows_affected())
            }
        }
    }

    async fn sqlite(plan: &MigrationPlan) -> Result<Self, TestDatabaseError> {
        let directory = tempfile::tempdir()?;
        let options = DatabaseOptions::Sqlite {
            path: directory.path().join("coral.sqlite"),
        };
        let database = Arc::new(Database::connect(options.clone()).await?);
        database.migrate(plan).await?;
        connect_virtual_tables(&database, &options).await?;
        Ok(Self {
            database,
            options,
            _sqlite_directory: Some(directory),
        })
    }

    async fn postgres(plan: &MigrationPlan, base_url: &str) -> Result<Self, TestDatabaseError> {
        coral_db::validate_postgres_url(base_url)?;
        let administration_pool = sqlx::PgPool::connect(base_url).await?;
        let schema = format!("coral_test_{}", Uuid::new_v4().simple());
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&administration_pool)
            .await?;
        install_shared_extensions(&administration_pool).await?;
        administration_pool.close().await;

        // Like a default deployment's `"$user", public`, the fixture schema
        // comes first and `public` holds the database's extensions.
        let mut scoped_url = url::Url::parse(base_url)?;
        scoped_url
            .query_pairs_mut()
            .append_pair("options[search_path]", &format!("{schema},public"));
        let options = DatabaseOptions::Postgres {
            url: scoped_url.into(),
        };
        let database = Arc::new(Database::connect(options.clone()).await?);
        database.migrate(plan).await?;
        Ok(Self {
            database,
            options,
            _sqlite_directory: None,
        })
    }
}

/// Connects every virtual table on the `SQLite` writer, as a production writer
/// does on its first write.
///
/// Fault-injection tests change the schema from a separate connection after
/// the writer loaded it. `SQLite` then fails the writer's first statement that
/// fires a trigger into a virtual table it has not connected: it reports the
/// statement's own table as missing when that table has a foreign key.
/// Production changes the schema only while it migrates, before its writer
/// loads the schema.
async fn connect_virtual_tables(
    database: &Database,
    options: &DatabaseOptions,
) -> Result<(), TestDatabaseError> {
    use sqlx::Connection as _;
    let DatabaseOptions::Sqlite { path } = options else {
        return Ok(());
    };
    let mut connection = sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(path),
    )
    .await?;
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND sql LIKE 'CREATE VIRTUAL TABLE%'",
    )
    .fetch_all(&mut connection)
    .await?;
    connection.close().await?;
    let mut transaction = database.begin().await?;
    for table in tables {
        transaction
            .execute_script(&format!("SELECT 1 FROM \"{table}\" LIMIT 0"))
            .await?;
    }
    transaction.rollback().await?;
    Ok(())
}

/// Installs the extensions Coral's boot checks require once per disposable
/// database, in `public`.
///
/// An extension lives in one schema per database. Created from a fixture, it
/// would land in that fixture's schema, off every other fixture's search
/// path. Concurrent fixtures can race to create it; the loser's duplicate
/// error means it exists.
async fn install_shared_extensions(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    const UNIQUE_VIOLATION: &str = "23505";
    const DUPLICATE_OBJECT: &str = "42710";
    match sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public")
        .execute(pool)
        .await
    {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(error))
            if matches!(
                error.code().as_deref(),
                Some(UNIQUE_VIOLATION | DUPLICATE_OBJECT)
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// Returns the database system selected for backend-neutral tests.
///
/// # Errors
///
/// Returns an error when test bootstrap configuration is missing or invalid.
pub fn selected_database() -> Result<DatabaseSystem, TestDatabaseError> {
    let selection = optional_environment(TEST_DATABASE_ENV)?;
    let postgres_url = optional_environment(POSTGRES_URL_ENV)?;
    select_database(selection.as_deref(), postgres_url.as_deref())
        .map_err(TestDatabaseError::Config)
}

/// Marks a unit-returning test as meaningful only on one database Adapter.
///
/// On the other selected Adapter, the test returns before arranging its
/// backend-specific fixture. Rust's standard test harness reports that early
/// return as a passing test rather than a distinct skipped state.
#[macro_export]
macro_rules! database_only {
    ($system:ident) => {
        if $crate::selected_database().expect("resolve selected test database")
            != $crate::DatabaseSystem::$system
        {
            return;
        }
    };
}

fn select_database(
    selection: Option<&str>,
    postgres_url: Option<&str>,
) -> Result<DatabaseSystem, String> {
    match selection {
        None | Some("sqlite") => Ok(DatabaseSystem::Sqlite),
        Some("postgres") if postgres_url.is_some_and(|url| !url.is_empty()) => {
            Ok(DatabaseSystem::Postgres)
        }
        Some("postgres") => Err(format!(
            "{POSTGRES_URL_ENV} must be set when {TEST_DATABASE_ENV}=postgres"
        )),
        Some("") => Err(format!(
            "{TEST_DATABASE_ENV} must be sqlite or postgres, got an empty value"
        )),
        Some(selection) => Err(format!(
            "{TEST_DATABASE_ENV} must be sqlite or postgres, got {selection}"
        )),
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "this test-only crate is the database-test process bootstrap"
)]
fn optional_environment(name: &str) -> Result<Option<String>, TestDatabaseError> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(TestDatabaseError::Config(format!(
            "{name} must contain valid Unicode"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static MIGRATOR: std::sync::LazyLock<coral_db::MigrationPlan> =
        std::sync::LazyLock::new(|| {
            coral_db::MigrationPlan::shared(&sqlx::migrate!("./tests/migrations").migrations)
        });

    #[tokio::test]
    async fn selected_database_is_live_and_shareable() {
        let fixture = TestDatabase::new(&MIGRATOR)
            .await
            .expect("create selected test database");
        fixture.database().ping().await.expect("ping database");
        let second = fixture.connect_peer().await.expect("connect peer");
        second.ping().await.expect("ping peer");
    }

    #[test]
    fn sqlite_is_the_explicit_and_implicit_default() {
        assert_eq!(
            select_database(None, None).expect("implicit SQLite selection"),
            DatabaseSystem::Sqlite
        );
        assert_eq!(
            select_database(Some("sqlite"), None).expect("explicit SQLite selection"),
            DatabaseSystem::Sqlite
        );
    }

    #[test]
    fn postgres_requires_a_nonempty_url() {
        for url in [None, Some("")] {
            assert_eq!(
                select_database(Some("postgres"), url).expect_err("missing URL must fail"),
                format!("{POSTGRES_URL_ENV} must be set when {TEST_DATABASE_ENV}=postgres")
            );
        }
        assert_eq!(
            select_database(Some("postgres"), Some("postgres://localhost/coral_test"))
                .expect("Postgres selection"),
            DatabaseSystem::Postgres
        );
    }

    #[test]
    fn unknown_database_selection_is_rejected() {
        assert_eq!(
            select_database(Some("mysql"), None).expect_err("unknown selection must fail"),
            "CORAL_TEST_DATABASE must be sqlite or postgres, got mysql"
        );
    }

    #[test]
    fn empty_database_selection_is_rejected() {
        assert_eq!(
            select_database(Some(""), None).expect_err("empty selection must fail"),
            "CORAL_TEST_DATABASE must be sqlite or postgres, got an empty value"
        );
    }
}
