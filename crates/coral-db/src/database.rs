use std::fs::{self, OpenOptions};
use std::path::Path;
use std::str::FromStr as _;
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};

use crate::backend::DatabaseBackend;
use crate::{DatabaseOptions, DatabaseSystem, Error};

const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Marks a database handle that can start write transactions and migrations.
/// Only these handles commit changes, so only they carry change notification.
pub struct ReadWrite {
    pub(crate) changes: crate::changes::Changes,
    /// Keeps the pools in `coral.persistence.pool.in_use` once a transaction
    /// begins under the metrics layer.
    pub(crate) pool_metric: std::sync::OnceLock<coral_telemetry::metric::PoolObservation>,
}

/// Marks a database handle that can only inspect existing state.
pub struct ReadOnly;

/// A database handle that hides its concrete connection pool.
///
/// Domain Modules own schemas and repository behavior. This Module owns only
/// backend selection, pools, short transactions, migrations, and liveness.
pub struct Database<Access = ReadWrite> {
    pub(crate) backend: DatabaseBackend,
    pub(crate) access: Access,
}

/// A database handle whose backend enforces read-only access.
pub type ReadOnlyDatabase = Database<ReadOnly>;

impl Database {
    /// Opens the selected backend and prepares its connection policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the options are invalid, the `SQLite` file cannot
    /// be prepared privately, or the selected database cannot be reached.
    pub async fn connect(options: impl Into<DatabaseOptions>) -> Result<Self, Error> {
        Self::connect_with_keepalive(options.into(), crate::changes::DEFAULT_KEEPALIVE).await
    }

    /// Opens the selected backend with a shorter Postgres notification liveness
    /// period, so tests can observe keepalives without waiting seconds.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::connect`].
    #[cfg(feature = "test-support")]
    pub async fn connect_with_notification_keepalive(
        options: impl Into<DatabaseOptions>,
        keepalive: Duration,
    ) -> Result<Self, Error> {
        Self::connect_with_keepalive(options.into(), keepalive).await
    }

    /// Opens the selected backend with a shorter wait for the `SQLite`
    /// writer, so tests can observe [`Error::WriterBusy`] without waiting
    /// seconds. Postgres has no writer wait and ignores it.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::connect`].
    #[cfg(feature = "test-support")]
    pub async fn connect_with_writer_wait(
        options: impl Into<DatabaseOptions>,
        writer_wait: Duration,
    ) -> Result<Self, Error> {
        match options.into() {
            DatabaseOptions::Sqlite { path } => connect_sqlite(&path, writer_wait).await,
            DatabaseOptions::Postgres { url } => {
                connect_postgres(&url, crate::changes::DEFAULT_KEEPALIVE).await
            }
        }
    }

    async fn connect_with_keepalive(
        options: DatabaseOptions,
        keepalive: Duration,
    ) -> Result<Self, Error> {
        match options {
            DatabaseOptions::Sqlite { path } => connect_sqlite(&path, SQLITE_BUSY_TIMEOUT).await,
            DatabaseOptions::Postgres { url } => connect_postgres(&url, keepalive).await,
        }
    }

    /// Closes change notification and all pooled connections.
    pub async fn close(&self) {
        self.access.changes.close();
        self.close_pool().await;
    }
}

impl ReadOnlyDatabase {
    /// Opens an existing backend with database-enforced read-only access.
    ///
    /// # Errors
    ///
    /// Returns an error when the options are invalid or the selected database
    /// cannot be inspected.
    pub async fn connect_read_only(options: impl Into<DatabaseOptions>) -> Result<Self, Error> {
        match options.into() {
            DatabaseOptions::Sqlite { path } => connect_read_only_sqlite(&path).await,
            DatabaseOptions::Postgres { url } => connect_read_only_postgres(&url).await,
        }
    }

    /// Closes all pooled connections.
    pub async fn close(&self) {
        self.close_pool().await;
    }
}

impl<Access> Database<Access> {
    /// Identifies the selected Adapter without exposing its pool.
    #[must_use]
    pub const fn system(&self) -> DatabaseSystem {
        match self.backend {
            DatabaseBackend::Sqlite { .. } => DatabaseSystem::Sqlite,
            DatabaseBackend::Postgres(_) => DatabaseSystem::Postgres,
        }
    }

    /// Verifies that the database accepts a minimal query.
    ///
    /// # Errors
    ///
    /// Returns the backend connection or query error.
    pub async fn ping(&self) -> Result<(), Error> {
        match &self.backend {
            DatabaseBackend::Sqlite { reads, .. } => {
                sqlx::query("SELECT 1").execute(reads).await?;
            }
            DatabaseBackend::Postgres(pool) => {
                sqlx::query("SELECT 1").execute(pool).await?;
            }
        }
        Ok(())
    }

    async fn close_pool(&self) {
        match &self.backend {
            DatabaseBackend::Sqlite { reads, writes } => {
                writes.close().await;
                reads.close().await;
            }
            DatabaseBackend::Postgres(pool) => pool.close().await,
        }
    }
}

/// `writer_wait` bounds how long a write waits for the one writer: in this
/// process for the pool's connection, and in others for the file lock.
async fn connect_sqlite(path: &Path, writer_wait: Duration) -> Result<Database, Error> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::MissingDatabaseParent(path.to_path_buf()))?;
    if !parent.as_os_str().is_empty() {
        ensure_private_directory(parent)?;
    }
    ensure_private_file(path)?;

    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(writer_wait)
        // `REGEXP` lets a statement use the pattern it gives Postgres's `~`.
        .with_regexp();
    // Connect the writer first: it may switch a new file to WAL, which a
    // query-only reader cannot do.
    let writes = SqlitePoolOptions::new()
        .max_connections(1)
        .acquire_timeout(writer_wait)
        .connect_with(options.clone())
        .await?;
    let reads = SqlitePoolOptions::new()
        .connect_with(options.pragma("query_only", "ON"))
        .await?;
    Ok(Database {
        backend: DatabaseBackend::Sqlite { reads, writes },
        access: ReadWrite {
            changes: crate::changes::Changes::local(),
            pool_metric: std::sync::OnceLock::new(),
        },
    })
}

async fn connect_read_only_sqlite(path: &Path) -> Result<ReadOnlyDatabase, Error> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .create_if_missing(false)
        .foreign_keys(true)
        .busy_timeout(SQLITE_BUSY_TIMEOUT)
        .with_regexp();
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    Ok(ReadOnlyDatabase {
        backend: DatabaseBackend::Sqlite {
            reads: pool.clone(),
            writes: pool,
        },
        access: ReadOnly,
    })
}

async fn connect_postgres(url: &str, keepalive: Duration) -> Result<Database, Error> {
    let options = postgres_connect_options(url)?;
    let pool = PgPoolOptions::new().connect_with(options).await?;
    Ok(Database {
        access: ReadWrite {
            changes: crate::changes::Changes::postgres(url, &pool, keepalive).await?,
            pool_metric: std::sync::OnceLock::new(),
        },
        backend: DatabaseBackend::Postgres(pool),
    })
}

async fn connect_read_only_postgres(url: &str) -> Result<ReadOnlyDatabase, Error> {
    let options = postgres_connect_options(url)?.options([("default_transaction_read_only", "on")]);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    Ok(ReadOnlyDatabase {
        backend: DatabaseBackend::Postgres(pool),
        access: ReadOnly,
    })
}

/// Validates a Postgres URL under the shared transport security policy.
///
/// # Errors
///
/// Returns [`Error::Config`] for an invalid URL or unsafe remote TLS mode.
pub fn validate_postgres_url(url: &str) -> Result<(), Error> {
    postgres_connect_options(url).map(drop)
}

pub(crate) fn postgres_connect_options(url: &str) -> Result<PgConnectOptions, Error> {
    let parsed_url = url::Url::parse(url)
        .map_err(|error| Error::Config(format!("invalid Postgres database URL: {error}")))?;
    let explicit_ssl_mode = postgres_url_ssl_mode(&parsed_url)?;
    let options = PgConnectOptions::from_str(parsed_url.as_str())
        .map_err(|error| Error::Config(format!("invalid Postgres database URL: {error}")))?;

    if postgres_requires_tls(&options)
        && !explicit_ssl_mode.is_some_and(postgres_ssl_mode_authenticates_server)
    {
        return Err(Error::Config(
            "remote Postgres database URLs must set sslmode=verify-full".to_string(),
        ));
    }

    Ok(options)
}

fn postgres_url_ssl_mode(url: &url::Url) -> Result<Option<PgSslMode>, Error> {
    let mut ssl_mode = None;
    for (key, value) in url.query_pairs() {
        if key == "sslmode" || key == "ssl-mode" {
            ssl_mode = Some(value.parse().map_err(|error| {
                Error::Config(format!("invalid Postgres database URL sslmode: {error}"))
            })?);
        }
    }
    Ok(ssl_mode)
}

fn postgres_requires_tls(options: &PgConnectOptions) -> bool {
    !postgres_uses_local_socket(options) && !postgres_host_is_loopback(options.get_host())
}

fn postgres_uses_local_socket(options: &PgConnectOptions) -> bool {
    options.get_socket().is_some() || options.get_host().starts_with('/')
}

fn postgres_host_is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

const fn postgres_ssl_mode_authenticates_server(ssl_mode: PgSslMode) -> bool {
    matches!(ssl_mode, PgSslMode::VerifyFull)
}

fn ensure_private_directory(path: &Path) -> Result<(), std::io::Error> {
    fs::create_dir_all(path)?;
    set_directory_permissions_private(path)
}

fn ensure_private_file(path: &Path) -> Result<(), std::io::Error> {
    match open_new_private_file(path) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if !fs::symlink_metadata(path)?.file_type().is_file() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("path exists and is not a regular file: {}", path.display()),
                ));
            }
            set_file_permissions_private(path)
        }
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn open_new_private_file(path: &Path) -> Result<std::fs::File, std::io::Error> {
    use std::os::unix::fs::OpenOptionsExt as _;

    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn open_new_private_file(path: &Path) -> Result<std::fs::File, std::io::Error> {
    OpenOptions::new().write(true).create_new(true).open(path)
}

#[cfg(unix)]
fn set_directory_permissions_private(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_directory_permissions_private(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(unix)]
fn set_file_permissions_private(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_file_permissions_private(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Database, DatabaseBackend, DatabaseOptions, postgres_connect_options};

    mod sqlite_only {
        use super::*;

        #[tokio::test]
        async fn connections_have_bounded_busy_handling() {
            coral_test_support::database_only!(Sqlite);
            let temp = tempfile::tempdir().expect("temp dir");
            let database = Database::connect(DatabaseOptions::Sqlite {
                path: temp.path().join("coral.db"),
            })
            .await
            .expect("connect SQLite");
            let DatabaseBackend::Sqlite { reads, writes } = &database.backend else {
                panic!("selected backend must be SQLite");
            };

            for pool in [reads, writes] {
                let busy_timeout_ms: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
                    .fetch_one(pool)
                    .await
                    .expect("read SQLite busy timeout");
                assert_eq!(busy_timeout_ms, 5_000);
            }
        }
    }

    #[test]
    fn remote_postgres_requires_hostname_authenticated_tls() {
        for url in [
            "postgres://coral:secret@db.example.com:5432/coral",
            "postgres://coral:secret@db.example.com:5432/coral?sslmode=require",
            "postgres://coral:secret@db.example.com:5432/coral?sslmode=verify-ca",
        ] {
            let error = postgres_connect_options(url).expect_err("unsafe URL must be rejected");
            assert!(error.to_string().contains("sslmode=verify-full"));
        }

        postgres_connect_options(
            "postgres://coral:secret@db.example.com:5432/coral?sslmode=verify-full",
        )
        .expect("authenticated remote TLS");
        postgres_connect_options("postgres://coral:secret@127.0.0.1:5432/coral")
            .expect("loopback development URL");
    }
}
