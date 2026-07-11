use sea_query::SelectStatement;
use sea_query_sqlx::SqlxBinder;
use sqlx::postgres::PgRow;
use sqlx::sqlite::SqliteRow;
use sqlx::{FromRow, Postgres, Sqlite};

use super::backend::CoralDbBackend;
use super::{CoralDb, CoralTx, DbError};
use crate::state::db::repositories::identity_specs::{
    IdentitySpecDocumentsRepo, IdentitySpecsRepo,
};
use crate::state::db::repositories::state_migrations::StateMigrationsRepo;
use crate::state::db::repositories::workspaces::WorkspacesRepo;

pub(crate) trait DbSession {
    async fn execute<S>(&mut self, statement: S) -> Result<(), DbError>
    where
        S: SqlxBinder;

    async fn fetch_optional<T>(&mut self, statement: SelectStatement) -> Result<Option<T>, DbError>
    where
        T: Send + Unpin,
        for<'r> T: FromRow<'r, SqliteRow>,
        for<'r> T: FromRow<'r, PgRow>;

    async fn fetch_all<T>(&mut self, statement: SelectStatement) -> Result<Vec<T>, DbError>
    where
        T: Send + Unpin,
        for<'r> T: FromRow<'r, SqliteRow>,
        for<'r> T: FromRow<'r, PgRow>;
}

pub(crate) trait DbRepos: DbSession + Sized {
    fn state_migrations(&mut self) -> StateMigrationsRepo<'_, Self> {
        StateMigrationsRepo::new(self)
    }

    fn workspaces(&mut self) -> WorkspacesRepo<'_, Self> {
        WorkspacesRepo::new(self)
    }

    fn identity_specs(&mut self) -> IdentitySpecsRepo<'_, Self> {
        IdentitySpecsRepo::new(self)
    }

    fn identity_spec_documents(&mut self) -> IdentitySpecDocumentsRepo<'_, Self> {
        IdentitySpecDocumentsRepo::new(self)
    }
}

impl<T> DbRepos for T where T: DbSession + Sized {}

impl DbSession for &CoralDb {
    async fn execute<S>(&mut self, statement: S) -> Result<(), DbError>
    where
        S: SqlxBinder,
    {
        execute_statement(&self.backend, statement).await
    }

    async fn fetch_optional<T>(&mut self, statement: SelectStatement) -> Result<Option<T>, DbError>
    where
        T: Send + Unpin,
        for<'r> T: FromRow<'r, SqliteRow>,
        for<'r> T: FromRow<'r, PgRow>,
    {
        fetch_optional_statement(&self.backend, statement).await
    }

    async fn fetch_all<T>(&mut self, statement: SelectStatement) -> Result<Vec<T>, DbError>
    where
        T: Send + Unpin,
        for<'r> T: FromRow<'r, SqliteRow>,
        for<'r> T: FromRow<'r, PgRow>,
    {
        fetch_all_statement(&self.backend, statement).await
    }
}

impl DbSession for CoralTx<'_> {
    async fn execute<S>(&mut self, statement: S) -> Result<(), DbError>
    where
        S: SqlxBinder,
    {
        self.execute(statement).await
    }

    async fn fetch_optional<T>(&mut self, statement: SelectStatement) -> Result<Option<T>, DbError>
    where
        T: Send + Unpin,
        for<'r> T: FromRow<'r, SqliteRow>,
        for<'r> T: FromRow<'r, PgRow>,
    {
        self.fetch_optional(statement).await
    }

    async fn fetch_all<T>(&mut self, statement: SelectStatement) -> Result<Vec<T>, DbError>
    where
        T: Send + Unpin,
        for<'r> T: FromRow<'r, SqliteRow>,
        for<'r> T: FromRow<'r, PgRow>,
    {
        self.fetch_all(statement).await
    }
}

pub(super) async fn execute_statement<S>(
    backend: &CoralDbBackend,
    statement: S,
) -> Result<(), DbError>
where
    S: SqlxBinder,
{
    match backend {
        CoralDbBackend::Sqlite(db) => {
            let (sql, values) = statement.build_sqlx(sea_query::SqliteQueryBuilder);
            sqlx::query_with::<Sqlite, _>(sqlx::AssertSqlSafe(sql), values)
                .execute(&db.pool)
                .await?;
        }
        CoralDbBackend::Postgres(db) => {
            let (sql, values) = statement.build_sqlx(sea_query::PostgresQueryBuilder);
            sqlx::query_with::<Postgres, _>(sqlx::AssertSqlSafe(sql), values)
                .execute(&db.pool)
                .await?;
        }
    }
    Ok(())
}

pub(super) async fn fetch_optional_statement<T>(
    backend: &CoralDbBackend,
    statement: SelectStatement,
) -> Result<Option<T>, DbError>
where
    T: Send + Unpin,
    for<'r> T: FromRow<'r, SqliteRow>,
    for<'r> T: FromRow<'r, PgRow>,
{
    match backend {
        CoralDbBackend::Sqlite(db) => {
            let (sql, values) = statement.build_sqlx(sea_query::SqliteQueryBuilder);
            sqlx::query_as_with::<Sqlite, T, _>(sqlx::AssertSqlSafe(sql), values)
                .fetch_optional(&db.pool)
                .await
                .map_err(Into::into)
        }
        CoralDbBackend::Postgres(db) => {
            let (sql, values) = statement.build_sqlx(sea_query::PostgresQueryBuilder);
            sqlx::query_as_with::<Postgres, T, _>(sqlx::AssertSqlSafe(sql), values)
                .fetch_optional(&db.pool)
                .await
                .map_err(Into::into)
        }
    }
}

pub(super) async fn fetch_all_statement<T>(
    backend: &CoralDbBackend,
    statement: SelectStatement,
) -> Result<Vec<T>, DbError>
where
    T: Send + Unpin,
    for<'r> T: FromRow<'r, SqliteRow>,
    for<'r> T: FromRow<'r, PgRow>,
{
    match backend {
        CoralDbBackend::Sqlite(db) => {
            let (sql, values) = statement.build_sqlx(sea_query::SqliteQueryBuilder);
            sqlx::query_as_with::<Sqlite, T, _>(sqlx::AssertSqlSafe(sql), values)
                .fetch_all(&db.pool)
                .await
                .map_err(Into::into)
        }
        CoralDbBackend::Postgres(db) => {
            let (sql, values) = statement.build_sqlx(sea_query::PostgresQueryBuilder);
            sqlx::query_as_with::<Postgres, T, _>(sqlx::AssertSqlSafe(sql), values)
                .fetch_all(&db.pool)
                .await
                .map_err(Into::into)
        }
    }
}
