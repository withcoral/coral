use sea_query::SelectStatement;
use sea_query_sqlx::SqlxBinder;
use sqlx::postgres::PgRow;
use sqlx::sqlite::SqliteRow;
use sqlx::{FromRow, Postgres, Sqlite};

use crate::backend::DatabaseBackend;
use crate::{Database, Error};

/// A column of an aliased table, for repository queries that join or nest tables.
#[must_use]
pub fn column(table: &str, field: &str) -> sea_query::ColumnRef {
    use sea_query::IntoColumnRef as _;
    (sea_query::Alias::new(table), sea_query::Alias::new(field)).into_column_ref()
}

/// [`column()`] as an expression.
#[must_use]
pub fn col(table: &str, field: &str) -> sea_query::SimpleExpr {
    sea_query::Expr::col(column(table, field))
}

impl Database {
    /// Executes one domain-owned statement outside a larger transaction.
    ///
    /// Domain Modules must use [`Database::begin`] when an invariant spans
    /// several statements or repository facets.
    ///
    /// # Errors
    ///
    /// Returns a backend query or constraint error.
    pub async fn execute<S>(&self, statement: S) -> Result<u64, Error>
    where
        S: SqlxBinder,
    {
        match &self.backend {
            DatabaseBackend::Sqlite { writes, .. } => {
                let (sql, values) = statement.build_sqlx(sea_query::SqliteQueryBuilder);
                let result = sqlx::query_with::<Sqlite, _>(sqlx::AssertSqlSafe(sql), values)
                    .execute(writes)
                    .await
                    .map_err(crate::error::writer_busy)?;
                Ok(result.rows_affected())
            }
            DatabaseBackend::Postgres(pool) => {
                let (sql, values) = statement.build_sqlx(sea_query::PostgresQueryBuilder);
                let result = sqlx::query_with::<Postgres, _>(sqlx::AssertSqlSafe(sql), values)
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected())
            }
        }
    }
}

impl<Access> Database<Access> {
    /// Fetches the first matching row with a domain-owned select statement.
    ///
    /// Returns `None` when no row matches. This does not verify that the
    /// statement matches at most one row.
    ///
    /// # Errors
    ///
    /// Returns a backend query or row-decoding error.
    pub async fn fetch_optional<T>(&self, statement: SelectStatement) -> Result<Option<T>, Error>
    where
        T: Send + Unpin,
        for<'row> T: FromRow<'row, SqliteRow>,
        for<'row> T: FromRow<'row, PgRow>,
    {
        match &self.backend {
            DatabaseBackend::Sqlite { reads, .. } => {
                let (sql, values) = statement.build_sqlx(sea_query::SqliteQueryBuilder);
                sqlx::query_as_with::<Sqlite, T, _>(sqlx::AssertSqlSafe(sql), values)
                    .fetch_optional(reads)
                    .await
                    .map_err(Into::into)
            }
            DatabaseBackend::Postgres(pool) => {
                let (sql, values) = statement.build_sqlx(sea_query::PostgresQueryBuilder);
                sqlx::query_as_with::<Postgres, T, _>(sqlx::AssertSqlSafe(sql), values)
                    .fetch_optional(pool)
                    .await
                    .map_err(Into::into)
            }
        }
    }

    /// Fetches and fully materializes all rows matching a domain-owned select.
    ///
    /// # Errors
    ///
    /// Returns a backend query or row-decoding error.
    pub async fn fetch_all<T>(&self, statement: SelectStatement) -> Result<Vec<T>, Error>
    where
        T: Send + Unpin,
        for<'row> T: FromRow<'row, SqliteRow>,
        for<'row> T: FromRow<'row, PgRow>,
    {
        match &self.backend {
            DatabaseBackend::Sqlite { reads, .. } => {
                let (sql, values) = statement.build_sqlx(sea_query::SqliteQueryBuilder);
                sqlx::query_as_with::<Sqlite, T, _>(sqlx::AssertSqlSafe(sql), values)
                    .fetch_all(reads)
                    .await
                    .map_err(Into::into)
            }
            DatabaseBackend::Postgres(pool) => {
                let (sql, values) = statement.build_sqlx(sea_query::PostgresQueryBuilder);
                sqlx::query_as_with::<Postgres, T, _>(sqlx::AssertSqlSafe(sql), values)
                    .fetch_all(pool)
                    .await
                    .map_err(Into::into)
            }
        }
    }
}
