use sea_query::{DeleteStatement, InsertStatement, SelectStatement, UpdateStatement};
use sea_query_sqlx::SqlxBinder;
use sqlx::postgres::PgRow;
use sqlx::sqlite::SqliteRow;
use sqlx::{FromRow, Postgres, Sqlite};

use super::backend::CoralDbBackend;
use super::{CoralDb, CoralTx, DbError};
use crate::state::db::repositories::app_state_markers::AppStateMarkersRepo;
use crate::state::db::repositories::feedback_reports::FeedbackReportsRepo;
use crate::state::db::repositories::materializations::MaterializationsRepo;
use crate::state::db::repositories::source_manifests::SourceManifestsRepo;
use crate::state::db::repositories::sources::SourcesRepo;
use crate::state::db::repositories::workspaces::WorkspacesRepo;

pub(crate) trait DbSession {
    async fn execute(&mut self, statement: InsertStatement) -> Result<(), DbError>;

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

pub(crate) trait DbWriteSession: DbSession {
    async fn execute_delete(&mut self, statement: DeleteStatement) -> Result<(), DbError>;

    async fn execute_update(&mut self, statement: UpdateStatement) -> Result<(), DbError>;
}

pub(crate) trait DbRepos: DbSession + Sized {
    fn workspaces(&mut self) -> WorkspacesRepo<'_, Self> {
        WorkspacesRepo::new(self)
    }

    fn sources(&mut self) -> SourcesRepo<'_, Self> {
        SourcesRepo::new(self)
    }

    fn source_manifests(&mut self) -> SourceManifestsRepo<'_, Self> {
        SourceManifestsRepo::new(self)
    }

    fn materializations(&mut self) -> MaterializationsRepo<'_, Self> {
        MaterializationsRepo::new(self)
    }

    fn feedback_reports(&mut self) -> FeedbackReportsRepo<'_, Self> {
        FeedbackReportsRepo::new(self)
    }

    fn app_state_markers(&mut self) -> AppStateMarkersRepo<'_, Self> {
        AppStateMarkersRepo::new(self)
    }
}

impl<T> DbRepos for T where T: DbSession + Sized {}

impl DbSession for &CoralDb {
    async fn execute(&mut self, statement: InsertStatement) -> Result<(), DbError> {
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

impl DbWriteSession for CoralTx<'_> {
    async fn execute_delete(&mut self, statement: DeleteStatement) -> Result<(), DbError> {
        self.execute_delete(statement).await
    }

    async fn execute_update(&mut self, statement: UpdateStatement) -> Result<(), DbError> {
        self.execute_update(statement).await
    }
}

impl DbSession for CoralTx<'_> {
    async fn execute(&mut self, statement: InsertStatement) -> Result<(), DbError> {
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

pub(super) async fn execute_statement(
    backend: &CoralDbBackend,
    statement: InsertStatement,
) -> Result<(), DbError> {
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
