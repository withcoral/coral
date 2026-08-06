use sea_query::{Expr, ExprTrait, JoinType, Order, Query};

use crate::state::db::schema::{TaskQueries, Tasks};
use crate::state::db::{CoralTx, DbError, DbSession};

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(in crate::state::db) struct TaskQueryRow {
    pub(in crate::state::db) id: String,
    pub(in crate::state::db) task_id: String,
    pub(in crate::state::db) intent: String,
    pub(in crate::state::db) sql: String,
    pub(in crate::state::db) status: String,
    pub(in crate::state::db) started_at_unix_nanos: i64,
}

pub(crate) struct TaskQueriesRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> TaskQueriesRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    pub(in crate::state::db) async fn list_for_workspace(
        &mut self,
        workspace_id: &str,
    ) -> Result<Vec<TaskQueryRow>, DbError> {
        let statement = Query::select()
            .columns([
                (TaskQueries::Table, TaskQueries::Id),
                (TaskQueries::Table, TaskQueries::TaskId),
                (TaskQueries::Table, TaskQueries::Intent),
                (TaskQueries::Table, TaskQueries::Sql),
                (TaskQueries::Table, TaskQueries::Status),
                (TaskQueries::Table, TaskQueries::StartedAtUnixNanos),
            ])
            .from(TaskQueries::Table)
            .join(
                JoinType::InnerJoin,
                Tasks::Table,
                Expr::col((TaskQueries::Table, TaskQueries::TaskId))
                    .equals((Tasks::Table, Tasks::Id)),
            )
            .and_where(Expr::col((Tasks::Table, Tasks::WorkspaceId)).eq(workspace_id))
            .order_by(
                (TaskQueries::Table, TaskQueries::StartedAtUnixNanos),
                Order::Asc,
            )
            .order_by((TaskQueries::Table, TaskQueries::Id), Order::Asc)
            .to_owned();
        self.session.fetch_all(statement).await
    }
}

impl TaskQueriesRepo<'_, CoralTx<'_>> {
    pub(in crate::state::db) async fn insert(&mut self, row: &TaskQueryRow) -> Result<(), DbError> {
        let statement = Query::insert()
            .into_table(TaskQueries::Table)
            .columns([
                TaskQueries::Id,
                TaskQueries::TaskId,
                TaskQueries::Intent,
                TaskQueries::Sql,
                TaskQueries::Status,
                TaskQueries::StartedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(row.id.clone()),
                Expr::val(row.task_id.clone()),
                Expr::val(row.intent.clone()),
                Expr::val(row.sql.clone()),
                Expr::val(row.status.clone()),
                Expr::val(row.started_at_unix_nanos),
            ])
            .to_owned();
        self.session.execute(statement).await
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, TaskCreation, TaskCreationResult,
        TaskQueryRecord, TaskQueryWrite, TaskQueryWriteResult,
    };

    #[tokio::test]
    async fn task_query_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("layout");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("config") else {
            panic!("default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");

        assert_task_query_repository(&db, "sqlite_task_queries").await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn task_query_repository_round_trips_against_postgres() {
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
        let workspace = format!("postgres_task_queries_{}", uuid::Uuid::new_v4().simple());

        assert_task_query_repository(&db, &workspace).await;
    }

    async fn assert_task_query_repository(db: &CoralDb, workspace_id: &str) {
        let other_workspace_id = format!("{workspace_id}_other");
        let (task_id, other_task_id) =
            create_task_query_parents(db, workspace_id, &other_workspace_id).await;

        assert_task_query_records(
            db,
            workspace_id,
            &other_workspace_id,
            &task_id,
            &other_task_id,
        )
        .await;
    }

    async fn create_task_query_parents(
        db: &CoralDb,
        workspace_id: &str,
        other_workspace_id: &str,
    ) -> (String, String) {
        let mut tx = db.begin().await.expect("begin workspace tx");
        tx.workspaces()
            .ensure(workspace_id, 1)
            .await
            .expect("workspace");
        tx.workspaces()
            .ensure(other_workspace_id, 2)
            .await
            .expect("other workspace");
        tx.commit().await.expect("commit workspaces");

        let task_id = uuid::Uuid::new_v4().to_string();
        let other_task_id = uuid::Uuid::new_v4().to_string();
        for (workspace, task) in [
            (workspace_id, task_id.as_str()),
            (other_workspace_id, other_task_id.as_str()),
        ] {
            assert_eq!(
                db.task_state()
                    .create(
                        TaskCreation {
                            id: task,
                            workspace_id: workspace,
                            created_by_principal_id: "product:principal:test",
                            intent: "Test task",
                            created_at_unix_nanos: 3,
                        },
                        10,
                    )
                    .await
                    .expect("create task"),
                TaskCreationResult::Created
            );
        }
        (task_id, other_task_id)
    }

    async fn assert_task_query_records(
        db: &CoralDb,
        workspace_id: &str,
        other_workspace_id: &str,
        task_id: &str,
        other_task_id: &str,
    ) {
        let first = TaskQueryRecord {
            id: "00000000-0000-0000-0000-000000000001".to_string(),
            task_id: task_id.to_string(),
            intent: "First query".to_string(),
            sql: "SELECT 1".to_string(),
            status: "success".to_string(),
            started_at_unix_nanos: 10,
        };
        let second = TaskQueryRecord {
            id: "00000000-0000-0000-0000-000000000002".to_string(),
            task_id: task_id.to_string(),
            intent: "Second query".to_string(),
            sql: "SELECT broken".to_string(),
            status: "error".to_string(),
            started_at_unix_nanos: 10,
        };
        let other = TaskQueryRecord {
            id: "00000000-0000-0000-0000-000000000003".to_string(),
            task_id: other_task_id.to_string(),
            intent: "Other workspace".to_string(),
            sql: "SELECT 3".to_string(),
            status: "success".to_string(),
            started_at_unix_nanos: 9,
        };
        for row in [&second, &other, &first] {
            let workspace = if row.task_id == task_id {
                workspace_id
            } else {
                other_workspace_id
            };
            assert_eq!(
                db.task_query_state()
                    .record(TaskQueryWrite {
                        workspace_id: workspace,
                        id: &row.id,
                        task_id: &row.task_id,
                        intent: &row.intent,
                        sql: &row.sql,
                        status: &row.status,
                        started_at_unix_nanos: row.started_at_unix_nanos,
                    })
                    .await
                    .expect("record task query"),
                TaskQueryWriteResult::Recorded
            );
        }

        assert_eq!(
            db.task_query_state()
                .record(TaskQueryWrite {
                    workspace_id: other_workspace_id,
                    id: "00000000-0000-0000-0000-000000000004",
                    task_id,
                    intent: "Wrong workspace",
                    sql: "SELECT 4",
                    status: "success",
                    started_at_unix_nanos: 11,
                })
                .await
                .expect("reject cross-workspace task query"),
            TaskQueryWriteResult::TaskNotFound
        );

        assert_eq!(
            db.task_query_state()
                .list_for_workspace(workspace_id)
                .await
                .expect("list task queries"),
            vec![first.clone(), second]
        );
        assert_eq!(
            db.task_query_state()
                .list_for_workspace(other_workspace_id)
                .await
                .expect("list other workspace task queries"),
            vec![other]
        );

        let mut tx = db.begin().await.expect("begin delete tx");
        tx.tasks()
            .delete(workspace_id, task_id)
            .await
            .expect("delete task");
        tx.commit().await.expect("commit task delete");
        assert!(
            db.task_query_state()
                .list_for_workspace(workspace_id)
                .await
                .expect("list cascaded task queries")
                .is_empty()
        );
    }
}
