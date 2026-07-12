use sea_query::SelectStatement;
use sea_query_sqlx::SqlxBinder;
use sqlx::postgres::PgRow;
use sqlx::sqlite::SqliteRow;
use sqlx::{FromRow, Postgres, Sqlite};

use super::DbError;
use super::backend::CoralDbBackend;

pub(crate) struct CoralTx<'a> {
    backend: CoralTxBackend<'a>,
}

enum CoralTxBackend<'a> {
    Sqlite(sqlx::Transaction<'a, Sqlite>),
    Postgres(sqlx::Transaction<'a, Postgres>),
}

impl<'a> CoralTx<'a> {
    pub(super) async fn begin(backend: &'a CoralDbBackend) -> Result<Self, DbError> {
        let backend = match backend {
            CoralDbBackend::Sqlite(db) => CoralTxBackend::Sqlite(db.pool.begin().await?),
            CoralDbBackend::Postgres(db) => CoralTxBackend::Postgres(db.pool.begin().await?),
        };
        Ok(Self { backend })
    }

    pub(super) async fn begin_read_snapshot(backend: &'a CoralDbBackend) -> Result<Self, DbError> {
        let mut tx = Self::begin(backend).await?;
        if let CoralTxBackend::Postgres(postgres) = &mut tx.backend {
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                .execute(&mut **postgres)
                .await?;
        }
        Ok(tx)
    }

    pub(super) async fn begin_serializable(backend: &'a CoralDbBackend) -> Result<Self, DbError> {
        let mut tx = Self::begin(backend).await?;
        if let CoralTxBackend::Postgres(postgres) = &mut tx.backend {
            sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                .execute(&mut **postgres)
                .await?;
        }
        Ok(tx)
    }

    #[cfg(test)]
    pub(crate) async fn disable_sqlite_busy_wait(&mut self) -> Result<bool, DbError> {
        let CoralTxBackend::Sqlite(sqlite) = &mut self.backend else {
            return Ok(false);
        };
        sqlx::query("PRAGMA busy_timeout = 0")
            .execute(&mut **sqlite)
            .await?;
        Ok(true)
    }

    pub(crate) async fn commit(self) -> Result<(), DbError> {
        match self.backend {
            CoralTxBackend::Sqlite(tx) => tx.commit().await?,
            CoralTxBackend::Postgres(tx) => tx.commit().await?,
        }
        Ok(())
    }

    pub(crate) async fn rollback(self) -> Result<(), DbError> {
        match self.backend {
            CoralTxBackend::Sqlite(tx) => tx.rollback().await?,
            CoralTxBackend::Postgres(tx) => tx.rollback().await?,
        }
        Ok(())
    }

    pub(super) async fn execute<S>(&mut self, statement: S) -> Result<(), DbError>
    where
        S: SqlxBinder,
    {
        self.execute_affected(statement).await?;
        Ok(())
    }

    pub(super) async fn execute_affected<S>(&mut self, statement: S) -> Result<u64, DbError>
    where
        S: SqlxBinder,
    {
        let rows_affected = match &mut self.backend {
            CoralTxBackend::Sqlite(tx) => {
                let (sql, values) = statement.build_sqlx(sea_query::SqliteQueryBuilder);
                sqlx::query_with::<Sqlite, _>(sqlx::AssertSqlSafe(sql), values)
                    .execute(&mut **tx)
                    .await?
                    .rows_affected()
            }
            CoralTxBackend::Postgres(tx) => {
                let (sql, values) = statement.build_sqlx(sea_query::PostgresQueryBuilder);
                sqlx::query_with::<Postgres, _>(sqlx::AssertSqlSafe(sql), values)
                    .execute(&mut **tx)
                    .await?
                    .rows_affected()
            }
        };
        Ok(rows_affected)
    }

    pub(super) async fn fetch_optional<T>(
        &mut self,
        statement: SelectStatement,
    ) -> Result<Option<T>, DbError>
    where
        T: Send + Unpin,
        for<'r> T: FromRow<'r, SqliteRow>,
        for<'r> T: FromRow<'r, PgRow>,
    {
        match &mut self.backend {
            CoralTxBackend::Sqlite(tx) => {
                let (sql, values) = statement.build_sqlx(sea_query::SqliteQueryBuilder);
                sqlx::query_as_with::<Sqlite, T, _>(sqlx::AssertSqlSafe(sql), values)
                    .fetch_optional(&mut **tx)
                    .await
                    .map_err(Into::into)
            }
            CoralTxBackend::Postgres(tx) => {
                let (sql, values) = statement.build_sqlx(sea_query::PostgresQueryBuilder);
                sqlx::query_as_with::<Postgres, T, _>(sqlx::AssertSqlSafe(sql), values)
                    .fetch_optional(&mut **tx)
                    .await
                    .map_err(Into::into)
            }
        }
    }

    pub(super) async fn fetch_all<T>(
        &mut self,
        statement: SelectStatement,
    ) -> Result<Vec<T>, DbError>
    where
        T: Send + Unpin,
        for<'r> T: FromRow<'r, SqliteRow>,
        for<'r> T: FromRow<'r, PgRow>,
    {
        match &mut self.backend {
            CoralTxBackend::Sqlite(tx) => {
                let (sql, values) = statement.build_sqlx(sea_query::SqliteQueryBuilder);
                sqlx::query_as_with::<Sqlite, T, _>(sqlx::AssertSqlSafe(sql), values)
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(Into::into)
            }
            CoralTxBackend::Postgres(tx) => {
                let (sql, values) = statement.build_sqlx(sea_query::PostgresQueryBuilder);
                sqlx::query_as_with::<Postgres, T, _>(sqlx::AssertSqlSafe(sql), values)
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(Into::into)
            }
        }
    }
}
