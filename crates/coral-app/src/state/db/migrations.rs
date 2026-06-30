use super::backend::CoralDbBackend;
use super::{CoralDb, DbError};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

impl CoralDb {
    pub(crate) async fn migrate(&self) -> Result<(), DbError> {
        match &self.backend {
            CoralDbBackend::Sqlite(db) => {
                if sqlite_migrations_are_current(&db.pool).await? {
                    return Ok(());
                }
                MIGRATOR.run(&db.pool).await?;
            }
            CoralDbBackend::Postgres(db) => {
                if postgres_migrations_are_current(&db.pool).await? {
                    return Ok(());
                }
                MIGRATOR.run(&db.pool).await?;
            }
        }
        Ok(())
    }
}

async fn sqlite_migrations_are_current(pool: &sqlx::SqlitePool) -> Result<bool, DbError> {
    let table_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'",
    )
    .fetch_one(pool)
    .await?;
    if table_count.0 == 0 {
        return Ok(false);
    }
    let rows: Vec<(i64, Vec<u8>, bool)> =
        sqlx::query_as("SELECT version, checksum, success FROM _sqlx_migrations ORDER BY version")
            .fetch_all(pool)
            .await?;
    Ok(rows_match_current_migrations(&rows))
}

async fn postgres_migrations_are_current(pool: &sqlx::PgPool) -> Result<bool, DbError> {
    let table_exists: (Option<String>,) =
        sqlx::query_as("SELECT to_regclass('_sqlx_migrations')::text")
            .fetch_one(pool)
            .await?;
    if table_exists.0.is_none() {
        return Ok(false);
    }
    let rows: Vec<(i64, Vec<u8>, bool)> =
        sqlx::query_as("SELECT version, checksum, success FROM _sqlx_migrations ORDER BY version")
            .fetch_all(pool)
            .await?;
    Ok(rows_match_current_migrations(&rows))
}

fn rows_match_current_migrations(rows: &[(i64, Vec<u8>, bool)]) -> bool {
    let migrations = MIGRATOR.migrations.as_ref();
    rows.len() == migrations.len()
        && rows
            .iter()
            .zip(migrations)
            .all(|((version, checksum, success), migration)| {
                *success
                    && *version == migration.version
                    && checksum.as_slice() == migration.checksum.as_ref()
            })
}

#[cfg(test)]
mod tests {
    use sea_query::{Alias, Expr, ExprTrait, Func, OnConflict, Query};
    use tempfile::tempdir;

    use super::{MIGRATOR, rows_match_current_migrations};
    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::schema::{SourceSecretKeys, SourceVariables, Sources, Workspaces};
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbError, DbSession, DbWriteSession, ResolvedDatabaseConfig,
    };

    #[derive(Debug, sqlx::FromRow)]
    struct CountRow {
        count: i64,
    }

    #[test]
    fn current_migration_rows_must_match_versions_checksums_and_success() {
        let current_rows: Vec<_> = MIGRATOR
            .migrations
            .iter()
            .map(|migration| {
                (
                    migration.version,
                    migration.checksum.as_ref().to_vec(),
                    true,
                )
            })
            .collect();
        assert!(rows_match_current_migrations(&current_rows));

        let mut missing_row = current_rows.clone();
        missing_row.pop();
        assert!(!rows_match_current_migrations(&missing_row));

        let mut failed_row = current_rows.clone();
        failed_row
            .first_mut()
            .expect("at least one embedded migration")
            .2 = false;
        assert!(!rows_match_current_migrations(&failed_row));

        let mut bad_checksum = current_rows;
        bad_checksum
            .first_mut()
            .expect("at least one embedded migration")
            .1 = b"not the embedded checksum".to_vec();
        assert!(!rows_match_current_migrations(&bad_checksum));
    }

    #[tokio::test]
    async fn source_catalog_migration_contract_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let config = DatabaseConfig::load(&layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");

        assert_source_catalog_migration_contract(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run migration contract coverage against Postgres"]
    async fn source_catalog_migration_contract_against_postgres() {
        let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL") else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_source_catalog_migration_contract(&db).await;
    }

    async fn assert_source_catalog_migration_contract(db: &CoralDb) {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let workspace_id = format!("workspace_{suffix}");
        let source_name = format!("source_{suffix}");
        let mut session = db.begin().await.expect("begin migration contract tx");

        insert_source_catalog_rows(&mut session, &workspace_id, &source_name)
            .await
            .expect("insert source catalog rows");
        assert_eq!(
            source_variable_count(&mut session, &workspace_id, &source_name)
                .await
                .expect("count variables"),
            1
        );
        assert_eq!(
            source_secret_key_count(&mut session, &workspace_id, &source_name)
                .await
                .expect("count secret keys"),
            1
        );
        assert_source_catalog_uniqueness_contract(&mut session, &workspace_id, &source_name).await;

        let alternate_workspace_id = format!("alternate_workspace_{suffix}");
        insert_source_catalog_rows(&mut session, &alternate_workspace_id, &source_name)
            .await
            .expect("insert same source name in another workspace");
        assert_eq!(
            source_count(&mut session, &alternate_workspace_id)
                .await
                .expect("count alternate workspace source"),
            1
        );
        delete_workspace(&mut session, &alternate_workspace_id)
            .await
            .expect("delete alternate workspace");

        delete_source(&mut session, &workspace_id, &source_name)
            .await
            .expect("delete source");
        assert_eq!(
            source_variable_count(&mut session, &workspace_id, &source_name)
                .await
                .expect("count variables after source delete"),
            0
        );
        assert_eq!(
            source_secret_key_count(&mut session, &workspace_id, &source_name)
                .await
                .expect("count secret keys after source delete"),
            0
        );

        insert_source_catalog_rows(&mut session, &workspace_id, &source_name)
            .await
            .expect("reinsert source catalog rows");
        delete_workspace(&mut session, &workspace_id)
            .await
            .expect("delete workspace");
        assert_eq!(
            source_count(&mut session, &workspace_id)
                .await
                .expect("count sources after workspace delete"),
            0
        );
        assert_eq!(
            source_variable_count(&mut session, &workspace_id, &source_name)
                .await
                .expect("count variables after workspace delete"),
            0
        );
        assert_eq!(
            source_secret_key_count(&mut session, &workspace_id, &source_name)
                .await
                .expect("count secret keys after workspace delete"),
            0
        );
        session
            .rollback()
            .await
            .expect("rollback migration contract tx");
    }

    async fn insert_source_catalog_rows<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<(), DbError>
    where
        S: DbSession,
    {
        insert_workspace_row(session, workspace_id).await?;
        insert_source_row(session, workspace_id, source_name).await?;
        insert_source_variable_row(session, workspace_id, source_name, "REGION", "us-east-1")
            .await?;
        insert_source_secret_key_row(session, workspace_id, source_name, 0, "API_TOKEN").await
    }

    async fn assert_source_catalog_uniqueness_contract<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
    ) where
        S: DbSession,
    {
        assert!(
            insert_source_row(session, workspace_id, source_name)
                .await
                .is_err(),
            "duplicate source identity should fail"
        );
        assert!(
            insert_source_variable_row(session, workspace_id, source_name, "REGION", "eu-west-1")
                .await
                .is_err(),
            "duplicate source variable key should fail"
        );
        assert!(
            insert_source_secret_key_row(session, workspace_id, source_name, 0, "OTHER_TOKEN")
                .await
                .is_err(),
            "duplicate source secret key position should fail"
        );
    }

    async fn insert_workspace_row<S>(session: &mut S, workspace_id: &str) -> Result<(), DbError>
    where
        S: DbSession,
    {
        session
            .execute(
                Query::insert()
                    .into_table(Workspaces::Table)
                    .columns([Workspaces::Id, Workspaces::CreatedAtUnixNanos])
                    .values_panic([Expr::val(workspace_id), Expr::val(1)])
                    .on_conflict(OnConflict::column(Workspaces::Id).do_nothing().to_owned())
                    .to_owned(),
            )
            .await
    }

    async fn insert_source_row<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<(), DbError>
    where
        S: DbSession,
    {
        session
            .execute(
                Query::insert()
                    .into_table(Sources::Table)
                    .columns([
                        Sources::WorkspaceId,
                        Sources::Name,
                        Sources::Version,
                        Sources::OriginKind,
                        Sources::CredentialStorage,
                        Sources::CreatedAtUnixNanos,
                        Sources::UpdatedAtUnixNanos,
                    ])
                    .values_panic([
                        Expr::val(workspace_id),
                        Expr::val(source_name),
                        Expr::val("1.0.0"),
                        Expr::val("imported"),
                        Expr::val("file"),
                        Expr::val(2),
                        Expr::val(3),
                    ])
                    .to_owned(),
            )
            .await
    }

    async fn insert_source_variable_row<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
        key: &str,
        value: &str,
    ) -> Result<(), DbError>
    where
        S: DbSession,
    {
        session
            .execute(
                Query::insert()
                    .into_table(SourceVariables::Table)
                    .columns([
                        SourceVariables::WorkspaceId,
                        SourceVariables::SourceName,
                        SourceVariables::Key,
                        SourceVariables::Value,
                    ])
                    .values_panic([
                        Expr::val(workspace_id),
                        Expr::val(source_name),
                        Expr::val(key),
                        Expr::val(value),
                    ])
                    .to_owned(),
            )
            .await
    }

    async fn insert_source_secret_key_row<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
        position: i32,
        key: &str,
    ) -> Result<(), DbError>
    where
        S: DbSession,
    {
        session
            .execute(
                Query::insert()
                    .into_table(SourceSecretKeys::Table)
                    .columns([
                        SourceSecretKeys::WorkspaceId,
                        SourceSecretKeys::SourceName,
                        SourceSecretKeys::Position,
                        SourceSecretKeys::Key,
                    ])
                    .values_panic([
                        Expr::val(workspace_id),
                        Expr::val(source_name),
                        Expr::val(position),
                        Expr::val(key),
                    ])
                    .to_owned(),
            )
            .await
    }

    async fn delete_source<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<(), DbError>
    where
        S: DbWriteSession,
    {
        session
            .execute_delete(
                Query::delete()
                    .from_table(Sources::Table)
                    .and_where(Expr::col(Sources::WorkspaceId).eq(workspace_id))
                    .and_where(Expr::col(Sources::Name).eq(source_name))
                    .to_owned(),
            )
            .await
    }

    async fn delete_workspace<S>(session: &mut S, workspace_id: &str) -> Result<(), DbError>
    where
        S: DbWriteSession,
    {
        session
            .execute_delete(
                Query::delete()
                    .from_table(Workspaces::Table)
                    .and_where(Expr::col(Workspaces::Id).eq(workspace_id))
                    .to_owned(),
            )
            .await
    }

    async fn source_count<S>(session: &mut S, workspace_id: &str) -> Result<i64, DbError>
    where
        S: DbSession,
    {
        let row: CountRow = session
            .fetch_optional(
                Query::select()
                    .expr_as(Func::count(Expr::val(1)), Alias::new("count"))
                    .from(Sources::Table)
                    .and_where(Expr::col(Sources::WorkspaceId).eq(workspace_id))
                    .to_owned(),
            )
            .await?
            .expect("count row");
        Ok(row.count)
    }

    async fn source_variable_count<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<i64, DbError>
    where
        S: DbSession,
    {
        let row: CountRow = session
            .fetch_optional(
                Query::select()
                    .expr_as(Func::count(Expr::val(1)), Alias::new("count"))
                    .from(SourceVariables::Table)
                    .and_where(Expr::col(SourceVariables::WorkspaceId).eq(workspace_id))
                    .and_where(Expr::col(SourceVariables::SourceName).eq(source_name))
                    .to_owned(),
            )
            .await?
            .expect("count row");
        Ok(row.count)
    }

    async fn source_secret_key_count<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<i64, DbError>
    where
        S: DbSession,
    {
        let row: CountRow = session
            .fetch_optional(
                Query::select()
                    .expr_as(Func::count(Expr::val(1)), Alias::new("count"))
                    .from(SourceSecretKeys::Table)
                    .and_where(Expr::col(SourceSecretKeys::WorkspaceId).eq(workspace_id))
                    .and_where(Expr::col(SourceSecretKeys::SourceName).eq(source_name))
                    .to_owned(),
            )
            .await?
            .expect("count row");
        Ok(row.count)
    }
}
