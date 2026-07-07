use std::path::Path;
use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use super::backend::{CoralDbBackend, PostgresCoralDb, SqliteCoralDb};
use super::{CoralTx, DbError, ResolvedDatabaseConfig};
use crate::storage::fs as storage_fs;

#[derive(Debug)]
pub(crate) struct CoralDb {
    pub(super) backend: CoralDbBackend,
}

impl CoralDb {
    pub(crate) async fn open(config: ResolvedDatabaseConfig) -> Result<Self, DbError> {
        match config {
            ResolvedDatabaseConfig::Sqlite { path } => open_sqlite(&path).await,
            ResolvedDatabaseConfig::Postgres { url } => open_postgres(&url).await,
        }
    }

    pub(crate) async fn begin(&self) -> Result<CoralTx<'_>, DbError> {
        CoralTx::begin(&self.backend).await
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Phase 1 keeps an explicit database health probe for the server diagnostics wired in a later stack PR."
        )
    )]
    pub(crate) async fn ping(&self) -> Result<(), DbError> {
        match &self.backend {
            CoralDbBackend::Sqlite(db) => {
                sqlx::query("SELECT 1").execute(&db.pool).await?;
            }
            CoralDbBackend::Postgres(db) => {
                sqlx::query("SELECT 1").execute(&db.pool).await?;
            }
        }
        Ok(())
    }
}

async fn open_sqlite(path: &Path) -> Result<CoralDb, DbError> {
    path.parent()
        .ok_or_else(|| DbError::MissingDatabaseParent(path.to_path_buf()))?;
    storage_fs::ensure_file_private(path)?;

    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false);
    let pool = SqlitePoolOptions::new().connect_with(options).await?;
    Ok(CoralDb {
        backend: CoralDbBackend::Sqlite(SqliteCoralDb { pool }),
    })
}

async fn open_postgres(url: &str) -> Result<CoralDb, DbError> {
    let options = postgres_connect_options(url)?;
    let pool = PgPoolOptions::new().connect_with(options).await?;
    Ok(CoralDb {
        backend: CoralDbBackend::Postgres(PostgresCoralDb { pool }),
    })
}

fn postgres_connect_options(url: &str) -> Result<PgConnectOptions, DbError> {
    let parsed_url = url::Url::parse(url)
        .map_err(|error| DbError::Config(format!("invalid Postgres database URL: {error}")))?;
    let explicit_ssl_mode = postgres_url_ssl_mode(&parsed_url)?;
    let options = PgConnectOptions::from_str(parsed_url.as_str())
        .map_err(|error| DbError::Config(format!("invalid Postgres database URL: {error}")))?;

    if postgres_requires_tls(&options)
        && !explicit_ssl_mode.is_some_and(postgres_ssl_mode_authenticates_server)
    {
        return Err(DbError::Config(
            "remote Postgres database URLs must set sslmode=verify-full".to_string(),
        ));
    }

    Ok(options)
}

fn postgres_url_ssl_mode(url: &url::Url) -> Result<Option<PgSslMode>, DbError> {
    let mut ssl_mode = None;
    for (key, value) in url.query_pairs() {
        if key == "sslmode" || key == "ssl-mode" {
            ssl_mode = Some(value.parse().map_err(|error| {
                DbError::Config(format!("invalid Postgres database URL sslmode: {error}"))
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
            .is_ok_and(|addr| addr.is_loopback())
}

fn postgres_ssl_mode_authenticates_server(ssl_mode: PgSslMode) -> bool {
    matches!(ssl_mode, PgSslMode::VerifyFull)
}

#[cfg(test)]
mod tests {
    use super::postgres_connect_options;

    #[cfg(unix)]
    #[tokio::test]
    async fn sqlite_open_creates_and_tightens_private_database_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("temp dir");
        let database_file = temp.path().join("state").join("coral.db");
        for mode in [None, Some(0o644)] {
            if let Some(mode) = mode {
                std::fs::set_permissions(&database_file, std::fs::Permissions::from_mode(mode))
                    .expect("loosen database permissions");
            }
            super::open_sqlite(&database_file)
                .await
                .expect("open sqlite");
            let actual_mode = std::fs::metadata(&database_file)
                .expect("database metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(actual_mode, 0o600);
        }
    }

    #[test]
    fn postgres_options_reject_remote_tcp_without_required_tls() {
        let error = postgres_connect_options("postgres://coral:secret@db.example.com:5432/coral")
            .expect_err("remote TCP without sslmode must be rejected");

        assert!(
            error
                .to_string()
                .contains("remote Postgres database URLs must set sslmode=verify-full"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn postgres_options_reject_remote_tcp_with_encryption_without_server_identity() {
        for sslmode in ["require", "verify-ca"] {
            let error = postgres_connect_options(&format!(
                "postgres://coral:secret@db.example.com:5432/coral?sslmode={sslmode}"
            ))
            .expect_err("remote TCP without hostname-authenticated TLS must be rejected");

            assert!(
                error
                    .to_string()
                    .contains("remote Postgres database URLs must set sslmode=verify-full"),
                "unexpected error for sslmode={sslmode}: {error}"
            );
        }
    }

    #[test]
    fn postgres_options_allow_remote_tcp_with_hostname_authenticated_tls() {
        postgres_connect_options(
            "postgres://coral:secret@db.example.com:5432/coral?sslmode=verify-full",
        )
        .expect("remote TCP with sslmode=verify-full should be accepted");
    }

    #[test]
    fn postgres_options_accept_ssl_mode_alias() {
        postgres_connect_options(
            "postgres://coral:secret@db.example.com:5432/coral?ssl-mode=verify-full",
        )
        .expect("remote TCP with ssl-mode=verify-full should be accepted");
    }

    #[test]
    fn postgres_options_allow_loopback_without_tls() {
        postgres_connect_options("postgres://coral:secret@127.0.0.1:5432/coral")
            .expect("loopback Postgres is allowed without TLS for local development and CI");
    }
}
