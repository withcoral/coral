use sea_query::{Expr, ExprTrait, OnConflict, Query};

use crate::state::db::schema::Functions;
use crate::state::db::{DbError, DbSession};

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct FunctionRecord {
    pub(crate) workspace_id: String,
    pub(crate) name: String,
    pub(crate) artifact_sql: String,
}

pub(crate) struct FunctionsRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> FunctionsRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    pub(crate) async fn list(
        &mut self,
        workspace_id: &str,
    ) -> Result<Vec<FunctionRecord>, DbError> {
        let statement = Query::select()
            .columns([
                Functions::WorkspaceId,
                Functions::Name,
                Functions::ArtifactSql,
            ])
            .from(Functions::Table)
            .and_where(Expr::col(Functions::WorkspaceId).eq(workspace_id))
            .to_owned();
        let mut functions: Vec<FunctionRecord> = self.session.fetch_all(statement).await?;
        functions.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(functions)
    }

    pub(crate) async fn upsert(
        &mut self,
        workspace_id: &str,
        name: &str,
        artifact_sql: &str,
    ) -> Result<(), DbError> {
        let statement = Query::insert()
            .into_table(Functions::Table)
            .columns([
                Functions::WorkspaceId,
                Functions::Name,
                Functions::ArtifactSql,
            ])
            .values_panic([
                Expr::val(workspace_id.to_owned()),
                Expr::val(name.to_owned()),
                Expr::val(artifact_sql.to_owned()),
            ])
            .on_conflict(
                OnConflict::columns([Functions::WorkspaceId, Functions::Name])
                    .update_column(Functions::ArtifactSql)
                    .to_owned(),
            )
            .to_owned();
        self.session.execute(statement).await
    }

    pub(crate) async fn delete(&mut self, workspace_id: &str, name: &str) -> Result<bool, DbError> {
        let statement = Query::delete()
            .from_table(Functions::Table)
            .and_where(Expr::col(Functions::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(Functions::Name).eq(name))
            .to_owned();
        Ok(self.session.execute_affected(statement).await? > 0)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use tempfile::tempdir;

    use super::FunctionRecord;
    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::{CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig};

    #[tokio::test]
    async fn function_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("default test database should be SQLite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");

        assert_function_repository_round_trip(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn function_repository_round_trips_against_postgres() {
        let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
        else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_function_repository_round_trip(&db).await;
    }

    async fn assert_function_repository_round_trip(db: &CoralDb) {
        let workspace_id = format!(
            "functions_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        );
        let mut session = db;
        session
            .workspaces()
            .ensure(&workspace_id, 1)
            .await
            .expect("ensure workspace");
        session
            .functions()
            .upsert(&workspace_id, "a_name", "select 1")
            .await
            .expect("insert function");
        session
            .functions()
            .upsert(&workspace_id, "aaname", "select 2")
            .await
            .expect("insert collation pair");
        session
            .functions()
            .upsert(&workspace_id, "a_name", "select 3")
            .await
            .expect("replace function");

        assert_eq!(
            session.functions().list(&workspace_id).await.expect("list"),
            vec![
                FunctionRecord {
                    workspace_id: workspace_id.clone(),
                    name: "a_name".to_string(),
                    artifact_sql: "select 3".to_string(),
                },
                FunctionRecord {
                    workspace_id: workspace_id.clone(),
                    name: "aaname".to_string(),
                    artifact_sql: "select 2".to_string(),
                },
            ]
        );
        assert!(
            !session
                .functions()
                .delete(&workspace_id, "missing")
                .await
                .expect("delete missing")
        );
        assert!(
            session
                .functions()
                .delete(&workspace_id, "aaname")
                .await
                .expect("delete existing")
        );

        session
            .workspaces()
            .delete(&workspace_id)
            .await
            .expect("delete workspace");
        assert!(
            session
                .functions()
                .list(&workspace_id)
                .await
                .expect("list after cascade")
                .is_empty()
        );
    }
}
