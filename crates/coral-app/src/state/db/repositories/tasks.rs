use sea_query::{Expr, ExprTrait, Query};

use crate::state::db::schema::Tasks;
use crate::state::db::{DbError, DbSession};

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct TaskRecord {
    pub(crate) id: String,
    pub(crate) intent: String,
    pub(crate) status: Option<String>,
    pub(crate) started_at_unix_nanos: i64,
    pub(crate) ended_at_unix_nanos: Option<i64>,
}

pub(crate) struct TasksRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> TasksRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    pub(crate) async fn insert(
        &mut self,
        workspace_id: &str,
        task: &TaskRecord,
    ) -> Result<(), DbError> {
        let statement = task_insert(workspace_id, task);
        self.session.execute(statement).await
    }

    pub(crate) async fn update_status(
        &mut self,
        workspace_id: &str,
        task_id: &str,
        status: &str,
        ended_at_unix_nanos: i64,
    ) -> Result<(), DbError> {
        let statement = Query::update()
            .table(Tasks::Table)
            .values([
                (Tasks::Status, Expr::val(status.to_string())),
                (
                    Tasks::EndedAtUnixNanos,
                    Expr::val(Some(ended_at_unix_nanos)),
                ),
            ])
            .and_where(Expr::col(Tasks::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(Tasks::Id).eq(task_id))
            .to_owned();
        self.session.execute(statement).await
    }

    pub(crate) async fn get(
        &mut self,
        workspace_id: &str,
        task_id: &str,
    ) -> Result<Option<TaskRecord>, DbError> {
        let statement = Query::select()
            .columns(task_columns())
            .from(Tasks::Table)
            .and_where(Expr::col(Tasks::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(Tasks::Id).eq(task_id))
            .to_owned();
        self.session.fetch_optional(statement).await
    }
}

fn task_insert(workspace_id: &str, task: &TaskRecord) -> sea_query::InsertStatement {
    Query::insert()
        .into_table(Tasks::Table)
        .columns([
            Tasks::WorkspaceId,
            Tasks::Id,
            Tasks::Intent,
            Tasks::Status,
            Tasks::StartedAtUnixNanos,
            Tasks::EndedAtUnixNanos,
        ])
        .values_panic([
            Expr::val(workspace_id.to_string()),
            Expr::val(task.id.clone()),
            Expr::val(task.intent.clone()),
            Expr::val(task.status.clone()),
            Expr::val(task.started_at_unix_nanos),
            Expr::val(task.ended_at_unix_nanos),
        ])
        .to_owned()
}

fn task_columns() -> [Tasks; 5] {
    [
        Tasks::Id,
        Tasks::Intent,
        Tasks::Status,
        Tasks::StartedAtUnixNanos,
        Tasks::EndedAtUnixNanos,
    ]
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::TaskRecord;
    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};

    #[tokio::test]
    async fn task_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;

        assert_task_repository_round_trip(&db, "sqlite_task_round_trip").await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn task_repository_round_trips_against_postgres() {
        let Some(url) = postgres_test_url() else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        let suffix = uuid::Uuid::new_v4().simple().to_string();
        assert_task_repository_round_trip(&db, &format!("postgres_task_{suffix}")).await;
    }

    async fn open_sqlite(layout: &AppStateLayout) -> CoralDb {
        let config = DatabaseConfig::load(layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        db
    }

    async fn assert_task_repository_round_trip(db: &CoralDb, workspace_id: &str) {
        let task = TaskRecord {
            id: uuid::Uuid::new_v4().to_string(),
            intent: "Find renewal risk".to_string(),
            status: None,
            started_at_unix_nanos: 41,
            ended_at_unix_nanos: None,
        };
        let mut tx = db.begin().await.expect("begin tx");
        tx.workspaces()
            .ensure(workspace_id, 40)
            .await
            .expect("workspace");
        tx.tasks().insert(workspace_id, &task).await.expect("task");
        tx.commit().await.expect("commit task");

        let mut session = db;
        assert_eq!(
            session
                .tasks()
                .get(workspace_id, &task.id)
                .await
                .expect("get"),
            Some(task.clone())
        );

        let mut tx = db.begin().await.expect("begin status tx");
        tx.tasks()
            .update_status(workspace_id, &task.id, "success", 42)
            .await
            .expect("update status");
        tx.commit().await.expect("commit status");

        assert_eq!(
            session
                .tasks()
                .get(workspace_id, &task.id)
                .await
                .expect("get ended")
                .expect("task")
                .status
                .as_deref(),
            Some("success")
        );
    }

    fn postgres_test_url() -> Option<String> {
        bootstrap::env_var("CORAL_TEST_POSTGRES_URL").expect("read CORAL_TEST_POSTGRES_URL")
    }
}
