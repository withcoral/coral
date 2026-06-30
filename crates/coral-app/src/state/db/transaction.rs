use sea_query::{DeleteStatement, InsertStatement, SelectStatement, UpdateStatement};
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

    pub(super) async fn execute(&mut self, statement: InsertStatement) -> Result<(), DbError> {
        match &mut self.backend {
            CoralTxBackend::Sqlite(tx) => {
                let (sql, values) = statement.build_sqlx(sea_query::SqliteQueryBuilder);
                sqlx::query_with::<Sqlite, _>(sqlx::AssertSqlSafe(sql), values)
                    .execute(&mut **tx)
                    .await?;
            }
            CoralTxBackend::Postgres(tx) => {
                let (sql, values) = statement.build_sqlx(sea_query::PostgresQueryBuilder);
                sqlx::query_with::<Postgres, _>(sqlx::AssertSqlSafe(sql), values)
                    .execute(&mut **tx)
                    .await?;
            }
        }
        Ok(())
    }

    pub(super) async fn execute_delete(
        &mut self,
        statement: DeleteStatement,
    ) -> Result<(), DbError> {
        match &mut self.backend {
            CoralTxBackend::Sqlite(tx) => {
                let (sql, values) = statement.build_sqlx(sea_query::SqliteQueryBuilder);
                sqlx::query_with::<Sqlite, _>(sqlx::AssertSqlSafe(sql), values)
                    .execute(&mut **tx)
                    .await?;
            }
            CoralTxBackend::Postgres(tx) => {
                let (sql, values) = statement.build_sqlx(sea_query::PostgresQueryBuilder);
                sqlx::query_with::<Postgres, _>(sqlx::AssertSqlSafe(sql), values)
                    .execute(&mut **tx)
                    .await?;
            }
        }
        Ok(())
    }

    pub(super) async fn execute_update(
        &mut self,
        statement: UpdateStatement,
    ) -> Result<u64, DbError> {
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
