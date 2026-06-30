use std::path::Path;

use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use super::backend::{CoralDbBackend, PostgresCoralDb, SqliteCoralDb};
use super::{CoralTx, DbError, ResolvedDatabaseConfig};

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
    let parent = path
        .parent()
        .ok_or_else(|| DbError::MissingDatabaseParent(path.to_path_buf()))?;
    std::fs::create_dir_all(parent)?;

    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new().connect_with(options).await?;
    Ok(CoralDb {
        backend: CoralDbBackend::Sqlite(SqliteCoralDb { pool }),
    })
}

async fn open_postgres(url: &str) -> Result<CoralDb, DbError> {
    let pool = PgPoolOptions::new().connect(url).await?;
    Ok(CoralDb {
        backend: CoralDbBackend::Postgres(PostgresCoralDb { pool }),
    })
}
