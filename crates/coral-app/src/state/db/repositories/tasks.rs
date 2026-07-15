use sea_query::{Expr, ExprTrait, Func, Order, Query};

use crate::state::db::schema::Tasks;
use crate::state::db::{CoralTx, DbError, DbSession};

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
struct TaskRow {
    id: String,
    created_by_principal_id: String,
    intent: String,
    outcome: Option<String>,
    created_at_unix_nanos: i64,
    completed_at_unix_nanos: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskCompletionUpdate {
    Completed,
    AlreadyCompleted,
    NotFound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskLifecycleState {
    Active,
    Completed,
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

    pub(crate) async fn find_state(
        &mut self,
        workspace_id: &str,
        task_id: &str,
    ) -> Result<Option<TaskLifecycleState>, DbError> {
        let statement = Query::select()
            .column(Tasks::Outcome)
            .from(Tasks::Table)
            .and_where(Expr::col(Tasks::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(Tasks::Id).eq(task_id))
            .to_owned();
        let row: Option<(Option<String>,)> = self.session.fetch_optional(statement).await?;
        Ok(row.map(|(outcome,)| {
            if outcome.is_some() {
                TaskLifecycleState::Completed
            } else {
                TaskLifecycleState::Active
            }
        }))
    }

    pub(in crate::state::db) async fn count(&mut self, workspace_id: &str) -> Result<u64, DbError> {
        let statement = Query::select()
            .expr(Func::count(Expr::col(Tasks::Id)))
            .from(Tasks::Table)
            .and_where(Expr::col(Tasks::WorkspaceId).eq(workspace_id))
            .to_owned();
        let (count,): (i64,) = self
            .session
            .fetch_optional(statement)
            .await?
            .unwrap_or_default();
        Ok(u64::try_from(count).unwrap_or(0))
    }

    pub(in crate::state::db) async fn oldest_completed_task_id(
        &mut self,
        workspace_id: &str,
    ) -> Result<Option<String>, DbError> {
        let statement = Query::select()
            .column(Tasks::Id)
            .from(Tasks::Table)
            .and_where(Expr::col(Tasks::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(Tasks::CompletedAtUnixNanos).is_not_null())
            .order_by(Tasks::CompletedAtUnixNanos, Order::Asc)
            .order_by(Tasks::CreatedAtUnixNanos, Order::Asc)
            .order_by(Tasks::Id, Order::Asc)
            .limit(1)
            .to_owned();
        let row: Option<(String,)> = self.session.fetch_optional(statement).await?;
        Ok(row.map(|(task_id,)| task_id))
    }

    #[cfg(test)]
    async fn get(&mut self, workspace_id: &str, task_id: &str) -> Result<Option<TaskRow>, DbError> {
        let statement = Query::select()
            .columns(task_columns())
            .from(Tasks::Table)
            .and_where(Expr::col(Tasks::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(Tasks::Id).eq(task_id))
            .to_owned();
        self.session.fetch_optional(statement).await
    }
}

impl TasksRepo<'_, CoralTx<'_>> {
    pub(crate) async fn insert(
        &mut self,
        workspace_id: &str,
        created_by_principal_id: &str,
        task_id: &str,
        intent: &str,
        created_at_unix_nanos: i64,
    ) -> Result<(), DbError> {
        let statement = task_insert(
            workspace_id,
            created_by_principal_id,
            task_id,
            intent,
            created_at_unix_nanos,
        );
        self.session.execute(statement).await
    }

    pub(crate) async fn complete(
        &mut self,
        workspace_id: &str,
        task_id: &str,
        outcome: &str,
        completed_at_unix_nanos: i64,
    ) -> Result<TaskCompletionUpdate, DbError> {
        let statement = Query::update()
            .table(Tasks::Table)
            .values([
                (Tasks::Outcome, Expr::val(outcome.to_string())),
                (
                    Tasks::CompletedAtUnixNanos,
                    Expr::val(Some(completed_at_unix_nanos)),
                ),
            ])
            .and_where(Expr::col(Tasks::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(Tasks::Id).eq(task_id))
            .and_where(Expr::col(Tasks::Outcome).is_null())
            .to_owned();
        if self.session.execute_rows_affected(statement).await? > 0 {
            return Ok(TaskCompletionUpdate::Completed);
        }

        let statement = Query::select()
            .column(Tasks::Outcome)
            .from(Tasks::Table)
            .and_where(Expr::col(Tasks::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(Tasks::Id).eq(task_id))
            .to_owned();
        let row: Option<(Option<String>,)> = self.session.fetch_optional(statement).await?;
        Ok(match row {
            Some((Some(_),)) => TaskCompletionUpdate::AlreadyCompleted,
            Some((None,)) | None => TaskCompletionUpdate::NotFound,
        })
    }

    /// Rewrites pre-v1 task attribution onto an internal user id.
    ///
    /// The match is on the stored attribution value alone. This is an
    /// attribution migration, so it must never consult a user, membership,
    /// workspace, or permission, and it deliberately spans every workspace the
    /// pre-v1 identity wrote in.
    pub(in crate::state::db) async fn reattribute_pre_v1_creator(
        &mut self,
        pre_v1_principal_id: &str,
        user_id: &str,
    ) -> Result<(), DbError> {
        let statement = Query::update()
            .table(Tasks::Table)
            .value(Tasks::CreatedByPrincipalId, user_id)
            .and_where(Expr::col(Tasks::CreatedByPrincipalId).eq(pre_v1_principal_id))
            .to_owned();
        self.session.execute(statement).await
    }

    pub(in crate::state::db) async fn delete(
        &mut self,
        workspace_id: &str,
        task_id: &str,
    ) -> Result<(), DbError> {
        let statement = Query::delete()
            .from_table(Tasks::Table)
            .and_where(Expr::col(Tasks::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(Tasks::Id).eq(task_id))
            .to_owned();
        self.session.execute(statement).await
    }
}

fn task_insert(
    workspace_id: &str,
    created_by_principal_id: &str,
    task_id: &str,
    intent: &str,
    created_at_unix_nanos: i64,
) -> sea_query::InsertStatement {
    Query::insert()
        .into_table(Tasks::Table)
        .columns([
            Tasks::Id,
            Tasks::WorkspaceId,
            Tasks::CreatedByPrincipalId,
            Tasks::Intent,
            Tasks::Outcome,
            Tasks::CreatedAtUnixNanos,
            Tasks::CompletedAtUnixNanos,
        ])
        .values_panic([
            Expr::val(task_id.to_string()),
            Expr::val(workspace_id.to_string()),
            Expr::val(created_by_principal_id.to_string()),
            Expr::val(intent.to_string()),
            Expr::val(Option::<String>::None),
            Expr::val(created_at_unix_nanos),
            Expr::val(Option::<i64>::None),
        ])
        .to_owned()
}

#[cfg(test)]
fn task_columns() -> [Tasks; 6] {
    [
        Tasks::Id,
        Tasks::CreatedByPrincipalId,
        Tasks::Intent,
        Tasks::Outcome,
        Tasks::CreatedAtUnixNanos,
        Tasks::CompletedAtUnixNanos,
    ]
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sea_query::{Expr, Query};
    use tempfile::tempdir;

    use super::{TaskCompletionUpdate, TaskLifecycleState, TaskRow};
    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::schema::Tasks;
    use crate::state::db::session::{DbRepos, DbSession};
    use crate::state::db::{
        CoralDb, DatabaseConfig, ResolvedDatabaseConfig, TaskCreation, TaskCreationResult,
        TaskMutationBarrier,
    };

    #[tokio::test]
    async fn task_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;

        assert_task_repository_round_trip(&db, "sqlite_task_round_trip").await;
    }

    #[tokio::test]
    async fn task_repository_contract_on_postgres() {
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

    #[expect(
        clippy::too_many_lines,
        reason = "The shared SQLite/Postgres harness verifies schema, identity, lifecycle, retention, concurrency, and cascade semantics together."
    )]
    async fn assert_task_repository_round_trip(db: &CoralDb, workspace_id: &str) {
        let mut tx = db.begin().await.expect("begin workspace tx");
        tx.workspaces()
            .ensure(workspace_id, 40)
            .await
            .expect("workspace");
        tx.commit().await.expect("commit workspace");
        assert_task_constraints(db, workspace_id).await;

        let created_by_principal_id = "product:principal:saul";
        let task = TaskRow {
            created_by_principal_id: created_by_principal_id.to_string(),
            id: uuid::Uuid::new_v4().to_string(),
            intent: "  Find renewal risk  ".to_string(),
            outcome: None,
            created_at_unix_nanos: 41,
            completed_at_unix_nanos: None,
        };
        assert_eq!(
            db.task_state()
                .create(
                    TaskCreation {
                        id: &task.id,
                        workspace_id,
                        created_by_principal_id,
                        intent: &task.intent,
                        created_at_unix_nanos: task.created_at_unix_nanos,
                    },
                    10,
                )
                .await
                .expect("create task"),
            TaskCreationResult::Created
        );

        let mut session = db;
        assert_eq!(
            session
                .tasks()
                .find_state(workspace_id, &task.id)
                .await
                .expect("find active state"),
            Some(TaskLifecycleState::Active)
        );
        assert_eq!(
            session
                .tasks()
                .find_state("other-workspace", &task.id)
                .await
                .expect("isolate state by workspace"),
            None
        );
        assert_eq!(
            session
                .tasks()
                .get(workspace_id, &task.id)
                .await
                .expect("get"),
            Some(task.clone())
        );

        let duplicate_workspace_id = format!("{workspace_id}_duplicate");
        let mut tx = db.begin().await.expect("begin duplicate workspace tx");
        tx.workspaces()
            .ensure(&duplicate_workspace_id, 41)
            .await
            .expect("duplicate workspace");
        tx.commit().await.expect("commit duplicate workspace");
        db.task_state()
            .create(
                TaskCreation {
                    id: &task.id,
                    workspace_id: &duplicate_workspace_id,
                    created_by_principal_id: "product:principal:planner",
                    intent: "Duplicate task id",
                    created_at_unix_nanos: 42,
                },
                10,
            )
            .await
            .expect_err("task UUID must be globally unique");

        assert_eq!(
            db.task_state()
                .complete(&duplicate_workspace_id, &task.id, "success", 42)
                .await
                .expect("isolate other workspace"),
            TaskCompletionUpdate::NotFound
        );
        assert_eq!(
            db.task_state()
                .complete(workspace_id, "missing-task", "success", 42)
                .await
                .expect("complete missing task"),
            TaskCompletionUpdate::NotFound
        );
        assert_eq!(
            db.task_state()
                .complete(workspace_id, &task.id, "success", 42)
                .await
                .expect("complete task"),
            TaskCompletionUpdate::Completed
        );
        assert_eq!(
            db.task_state()
                .complete(workspace_id, &task.id, "failure", 43)
                .await
                .expect("task is already terminal"),
            TaskCompletionUpdate::AlreadyCompleted
        );

        let completed = session
            .tasks()
            .get(workspace_id, &task.id)
            .await
            .expect("get completed")
            .expect("task");
        assert_eq!(completed.outcome.as_deref(), Some("success"));
        assert_eq!(completed.completed_at_unix_nanos, Some(42));
        assert_eq!(completed.created_by_principal_id, "product:principal:saul");
        assert_eq!(
            session
                .tasks()
                .find_state(workspace_id, &task.id)
                .await
                .expect("find completed state"),
            Some(TaskLifecycleState::Completed)
        );

        let second_id = uuid::Uuid::new_v4().to_string();
        assert_eq!(
            db.task_state()
                .create(
                    TaskCreation {
                        id: &second_id,
                        workspace_id,
                        created_by_principal_id: "product:principal:planner",
                        intent: "Second completed task",
                        created_at_unix_nanos: 43,
                    },
                    10,
                )
                .await
                .expect("create second task"),
            TaskCreationResult::Created
        );
        assert_eq!(
            db.task_state()
                .complete(workspace_id, &second_id, "failure", 42)
                .await
                .expect("complete second task"),
            TaskCompletionUpdate::Completed
        );
        let third_id = uuid::Uuid::new_v4().to_string();
        assert_eq!(
            db.task_state()
                .create(
                    TaskCreation {
                        id: &third_id,
                        workspace_id,
                        created_by_principal_id,
                        intent: "Retained active task",
                        created_at_unix_nanos: 44,
                    },
                    2,
                )
                .await
                .expect("create retained task"),
            TaskCreationResult::Created
        );
        assert_eq!(
            session
                .tasks()
                .get(workspace_id, &task.id)
                .await
                .expect("get deterministic eviction"),
            None
        );
        assert!(
            session
                .tasks()
                .get(workspace_id, &second_id)
                .await
                .expect("get newer completed task")
                .is_some()
        );
        assert!(
            session
                .tasks()
                .get(workspace_id, &third_id)
                .await
                .expect("get retained active task")
                .is_some()
        );

        assert_capacity_rollback_restores_evicted_tasks(db, workspace_id).await;
        assert_concurrent_capacity_and_cascade(db, workspace_id).await;
        assert_create_serializes_with_workspace_delete(db, workspace_id).await;
        assert_completion_serializes_with_workspace_delete(db, workspace_id).await;
    }

    async fn assert_capacity_rollback_restores_evicted_tasks(db: &CoralDb, workspace_id: &str) {
        let rollback_workspace_id = format!("{workspace_id}_capacity_rollback");
        let mut tx = db.begin().await.expect("begin rollback workspace tx");
        tx.workspaces()
            .ensure(&rollback_workspace_id, 45)
            .await
            .expect("rollback workspace");
        tx.commit().await.expect("commit rollback workspace");

        let completed_id = uuid::Uuid::new_v4().to_string();
        let first_active_id = uuid::Uuid::new_v4().to_string();
        let second_active_id = uuid::Uuid::new_v4().to_string();
        for (task_id, intent) in [
            (&completed_id, "Completed task"),
            (&first_active_id, "First active task"),
            (&second_active_id, "Second active task"),
        ] {
            assert_eq!(
                db.task_state()
                    .create(
                        TaskCreation {
                            id: task_id,
                            workspace_id: &rollback_workspace_id,
                            created_by_principal_id: "product:principal:saul",
                            intent,
                            created_at_unix_nanos: 46,
                        },
                        10,
                    )
                    .await
                    .expect("seed task"),
                TaskCreationResult::Created
            );
        }
        assert_eq!(
            db.task_state()
                .complete(&rollback_workspace_id, &completed_id, "success", 47,)
                .await
                .expect("complete retained task"),
            TaskCompletionUpdate::Completed
        );

        let rejected_id = uuid::Uuid::new_v4().to_string();
        assert_eq!(
            db.task_state()
                .create(
                    TaskCreation {
                        id: &rejected_id,
                        workspace_id: &rollback_workspace_id,
                        created_by_principal_id: "product:principal:planner",
                        intent: "Rejected task",
                        created_at_unix_nanos: 48,
                    },
                    2,
                )
                .await
                .expect("reject task at active capacity"),
            TaskCreationResult::WorkspaceCapacityExceeded
        );

        let mut session = db;
        assert_eq!(
            session
                .tasks()
                .count(&rollback_workspace_id)
                .await
                .expect("count restored tasks"),
            3
        );
        assert!(
            session
                .tasks()
                .get(&rollback_workspace_id, &completed_id)
                .await
                .expect("get restored completed task")
                .is_some(),
            "capacity rollback must restore provisional eviction"
        );
    }

    async fn assert_concurrent_capacity_and_cascade(db: &CoralDb, workspace_id: &str) {
        let capacity_workspace_id = format!("{workspace_id}_capacity");
        let mut tx = db.begin().await.expect("begin capacity workspace tx");
        tx.workspaces()
            .ensure(&capacity_workspace_id, 50)
            .await
            .expect("capacity workspace");
        tx.commit().await.expect("commit capacity workspace");
        let first_id = uuid::Uuid::new_v4().to_string();
        let second_id = uuid::Uuid::new_v4().to_string();
        let (first_result, second_result) = tokio::join!(
            create_task_at_capacity(
                db,
                &capacity_workspace_id,
                "product:principal:saul",
                &first_id,
            ),
            create_task_at_capacity(
                db,
                &capacity_workspace_id,
                "product:principal:planner",
                &second_id
            )
        );
        assert_ne!(
            first_result, second_result,
            "the shared workspace capacity must admit exactly one concurrent task"
        );
        assert!([first_result, second_result].contains(&TaskCreationResult::Created));
        assert!(
            [first_result, second_result].contains(&TaskCreationResult::WorkspaceCapacityExceeded)
        );
        let mut session = db;
        assert_eq!(
            session
                .tasks()
                .count(&capacity_workspace_id)
                .await
                .expect("count retained tasks"),
            1
        );
        let first_exists = session
            .tasks()
            .get(&capacity_workspace_id, &first_id)
            .await
            .expect("get first concurrent task")
            .is_some();
        let second_exists = session
            .tasks()
            .get(&capacity_workspace_id, &second_id)
            .await
            .expect("get second concurrent task")
            .is_some();
        assert_ne!(
            first_exists, second_exists,
            "the workspace lock must serialize concurrent retention decisions"
        );

        let mut tx = db.begin().await.expect("begin cascade tx");
        assert!(
            tx.workspaces()
                .delete(&capacity_workspace_id)
                .await
                .expect("delete capacity workspace")
        );
        tx.commit().await.expect("commit workspace delete");
        assert_eq!(
            session
                .tasks()
                .count(&capacity_workspace_id)
                .await
                .expect("count cascaded tasks"),
            0
        );
    }

    async fn assert_create_serializes_with_workspace_delete(db: &CoralDb, workspace_id: &str) {
        let workspace_id = format!("{workspace_id}_create_delete");
        let mut tx = db.begin().await.expect("begin create/delete workspace tx");
        tx.workspaces()
            .ensure(&workspace_id, 52)
            .await
            .expect("create create/delete workspace");
        tx.commit().await.expect("commit create/delete workspace");

        let task_id = uuid::Uuid::new_v4().to_string();
        let mutation_barrier = TaskMutationBarrier::new();
        let task_state = db.task_state_with_mutation_barrier(&mutation_barrier);
        let (creation, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                task_state.create(
                    TaskCreation {
                        id: &task_id,
                        workspace_id: &workspace_id,
                        created_by_principal_id: "product:principal:saul",
                        intent: "Task racing workspace deletion",
                        created_at_unix_nanos: 53,
                    },
                    10,
                ),
                delete_workspace_after_task_holds(db, &workspace_id, &mutation_barrier),
            )
        })
        .await
        .expect("create/delete race should finish");
        assert_eq!(
            creation.expect("create task racing workspace deletion"),
            TaskCreationResult::Created
        );
        assert_workspace_and_tasks_deleted(db, &workspace_id, &task_id).await;
    }

    async fn assert_completion_serializes_with_workspace_delete(db: &CoralDb, workspace_id: &str) {
        let workspace_id = format!("{workspace_id}_complete_delete");
        let mut tx = db
            .begin()
            .await
            .expect("begin completion/delete workspace tx");
        tx.workspaces()
            .ensure(&workspace_id, 54)
            .await
            .expect("create completion/delete workspace");
        tx.commit()
            .await
            .expect("commit completion/delete workspace");

        let task_id = uuid::Uuid::new_v4().to_string();
        assert_eq!(
            db.task_state()
                .create(
                    TaskCreation {
                        id: &task_id,
                        workspace_id: &workspace_id,
                        created_by_principal_id: "product:principal:saul",
                        intent: "Task completion racing workspace deletion",
                        created_at_unix_nanos: 55,
                    },
                    10,
                )
                .await
                .expect("seed task racing workspace deletion"),
            TaskCreationResult::Created
        );

        let mutation_barrier = TaskMutationBarrier::new();
        let task_state = db.task_state_with_mutation_barrier(&mutation_barrier);
        let (completion, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                task_state.complete(&workspace_id, &task_id, "success", 56),
                delete_workspace_after_task_holds(db, &workspace_id, &mutation_barrier),
            )
        })
        .await
        .expect("complete/delete race should finish");
        assert_eq!(
            completion.expect("complete task racing workspace deletion"),
            TaskCompletionUpdate::Completed
        );
        assert_workspace_and_tasks_deleted(db, &workspace_id, &task_id).await;
    }

    async fn delete_workspace_after_task_holds(
        db: &CoralDb,
        workspace_id: &str,
        mutation_barrier: &TaskMutationBarrier,
    ) {
        mutation_barrier.wait_until_workspace_held().await;
        let deletion = async {
            let deletion = db
                .begin_workspace_deletion(workspace_id)
                .await
                .expect("begin concurrent workspace delete")
                .expect("workspace must exist before concurrent deletion");
            deletion
                .commit()
                .await
                .expect("commit concurrent workspace delete");
        };
        tokio::pin!(deletion);
        assert!(
            tokio::time::timeout(Duration::from_millis(250), deletion.as_mut())
                .await
                .is_err(),
            "workspace deletion must wait while the task mutation holds the parent lock"
        );
        mutation_barrier.release_mutation().await;
        deletion.await;
    }

    async fn assert_workspace_and_tasks_deleted(db: &CoralDb, workspace_id: &str, task_id: &str) {
        let mut session = db;
        assert_eq!(
            session
                .workspaces()
                .get(workspace_id)
                .await
                .expect("get concurrently deleted workspace"),
            None
        );
        assert_eq!(
            session
                .tasks()
                .count(workspace_id)
                .await
                .expect("count orphaned tasks"),
            0
        );
        assert_eq!(
            session
                .tasks()
                .get(workspace_id, task_id)
                .await
                .expect("get orphaned task"),
            None
        );
    }

    async fn create_task_at_capacity(
        db: &CoralDb,
        workspace_id: &str,
        created_by_principal_id: &str,
        task_id: &str,
    ) -> TaskCreationResult {
        db.task_state()
            .create(
                TaskCreation {
                    id: task_id,
                    workspace_id,
                    created_by_principal_id,
                    intent: "Concurrent task",
                    created_at_unix_nanos: 51,
                },
                1,
            )
            .await
            .expect("create concurrent task")
    }

    async fn assert_task_constraints(db: &CoralDb, workspace_id: &str) {
        assert_invalid_task_row(
            db,
            workspace_id,
            Some("cancelled"),
            Some(41),
            "reject unsupported outcome",
        )
        .await;
        assert_invalid_task_row(
            db,
            workspace_id,
            Some("success"),
            None,
            "reject outcome without completion time",
        )
        .await;
        assert_invalid_task_row(
            db,
            workspace_id,
            None,
            Some(41),
            "reject completion time without outcome",
        )
        .await;
    }

    async fn assert_invalid_task_row(
        db: &CoralDb,
        workspace_id: &str,
        outcome: Option<&str>,
        completed_at_unix_nanos: Option<i64>,
        expectation: &str,
    ) {
        let statement = Query::insert()
            .into_table(Tasks::Table)
            .columns([
                Tasks::Id,
                Tasks::WorkspaceId,
                Tasks::CreatedByPrincipalId,
                Tasks::Intent,
                Tasks::Outcome,
                Tasks::CreatedAtUnixNanos,
                Tasks::CompletedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(uuid::Uuid::new_v4().to_string()),
                Expr::val(workspace_id.to_string()),
                Expr::val("product:test:constraint".to_string()),
                Expr::val("Invalid task row".to_string()),
                Expr::val(outcome.map(str::to_string)),
                Expr::val(40_i64),
                Expr::val(completed_at_unix_nanos),
            ])
            .to_owned();
        let mut tx = db.begin().await.expect("begin constraint tx");
        DbSession::execute(&mut tx, statement)
            .await
            .expect_err(expectation);
        tx.rollback().await.expect("roll back invalid task row");
    }

    fn postgres_test_url() -> Option<String> {
        bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
    }
}
