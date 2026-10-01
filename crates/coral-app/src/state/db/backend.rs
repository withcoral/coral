use sqlx::{PgPool, SqlitePool};

#[derive(Debug)]
pub(super) enum CoralDbBackend {
    Sqlite(SqliteCoralDb),
    Postgres(PostgresCoralDb),
}

#[derive(Debug)]
pub(super) struct SqliteCoralDb {
    pub(super) pool: SqlitePool,
}

#[derive(Debug)]
pub(super) struct PostgresCoralDb {
    pub(super) pool: PgPool,
}
