use sea_query::{Expr, ExprTrait, OnConflict, Query};
#[cfg(test)]
use sea_query::{JoinType, Order};

use crate::state::db::schema::{TaskQueries, TaskQueryRelations, Tasks};
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

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(in crate::state::db) struct TaskQueryRelationRow {
    pub(in crate::state::db) query_id: String,
    pub(in crate::state::db) relation_kind: String,
    pub(in crate::state::db) catalog_name: String,
    pub(in crate::state::db) schema_name: String,
    pub(in crate::state::db) relation_name: String,
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

    #[cfg(test)]
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

    #[cfg(test)]
    pub(in crate::state::db) async fn exists_for_task(
        &mut self,
        task_id: &str,
    ) -> Result<bool, DbError> {
        let statement = Query::select()
            .column(TaskQueries::Id)
            .from(TaskQueries::Table)
            .and_where(Expr::col(TaskQueries::TaskId).eq(task_id))
            .limit(1)
            .to_owned();
        let row: Option<(String,)> = self.session.fetch_optional(statement).await?;
        Ok(row.is_some())
    }

    #[cfg(test)]
    pub(in crate::state::db) async fn list_relations_for_query(
        &mut self,
        query_id: &str,
    ) -> Result<Vec<TaskQueryRelationRow>, DbError> {
        let statement = Query::select()
            .columns([
                TaskQueryRelations::QueryId,
                TaskQueryRelations::RelationKind,
                TaskQueryRelations::CatalogName,
                TaskQueryRelations::SchemaName,
                TaskQueryRelations::RelationName,
            ])
            .from(TaskQueryRelations::Table)
            .and_where(Expr::col(TaskQueryRelations::QueryId).eq(query_id))
            .order_by(TaskQueryRelations::RelationKind, Order::Asc)
            .order_by(TaskQueryRelations::CatalogName, Order::Asc)
            .order_by(TaskQueryRelations::SchemaName, Order::Asc)
            .order_by(TaskQueryRelations::RelationName, Order::Asc)
            .to_owned();
        self.session.fetch_all(statement).await
    }
}

impl TaskQueriesRepo<'_, CoralTx<'_>> {
    pub(in crate::state::db) async fn insert_for_workspace(
        &mut self,
        workspace_id: &str,
        row: &TaskQueryRow,
    ) -> Result<bool, DbError> {
        let selected_values = Query::select()
            .expr(Expr::val(row.id.clone()))
            .expr(Expr::val(row.task_id.clone()))
            .expr(Expr::val(row.intent.clone()))
            .expr(Expr::val(row.sql.clone()))
            .expr(Expr::val(row.status.clone()))
            .expr(Expr::val(row.started_at_unix_nanos))
            .from(Tasks::Table)
            .and_where(Expr::col((Tasks::Table, Tasks::Id)).eq(row.task_id.clone()))
            .and_where(Expr::col((Tasks::Table, Tasks::WorkspaceId)).eq(workspace_id))
            .to_owned();
        let mut statement = Query::insert();
        statement
            .into_table(TaskQueries::Table)
            .columns([
                TaskQueries::Id,
                TaskQueries::TaskId,
                TaskQueries::Intent,
                TaskQueries::Sql,
                TaskQueries::Status,
                TaskQueries::StartedAtUnixNanos,
            ])
            .select_from(selected_values)
            .expect("task query insert columns match selected values");
        Ok(self.session.execute_rows_affected(statement).await? == 1)
    }

    pub(in crate::state::db) async fn insert_relations(
        &mut self,
        rows: &[TaskQueryRelationRow],
    ) -> Result<(), DbError> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut statement = Query::insert();
        statement.into_table(TaskQueryRelations::Table).columns([
            TaskQueryRelations::QueryId,
            TaskQueryRelations::RelationKind,
            TaskQueryRelations::CatalogName,
            TaskQueryRelations::SchemaName,
            TaskQueryRelations::RelationName,
        ]);
        for row in rows {
            statement.values_panic([
                Expr::val(row.query_id.clone()),
                Expr::val(row.relation_kind.clone()),
                Expr::val(row.catalog_name.clone()),
                Expr::val(row.schema_name.clone()),
                Expr::val(row.relation_name.clone()),
            ]);
        }
        statement.on_conflict(
            OnConflict::columns([
                TaskQueryRelations::QueryId,
                TaskQueryRelations::RelationKind,
                TaskQueryRelations::CatalogName,
                TaskQueryRelations::SchemaName,
                TaskQueryRelations::RelationName,
            ])
            .do_nothing()
            .to_owned(),
        );
        self.session.execute(statement).await
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::TaskQueryRow;
    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, TaskCompletionUpdate,
        TaskCreation, TaskCreationResult, TaskQueryRelationRecord, TaskQueryRelationWrite,
        TaskQueryWrite, TaskQueryWriteResult,
    };

    const FIRST_QUERY_RELATIONS: &[TaskQueryRelationWrite<'static>] = &[
        TaskQueryRelationWrite {
            relation_kind: "table",
            catalog_name: Some("github"),
            schema_name: "actions",
            relation_name: "runs",
        },
        TaskQueryRelationWrite {
            relation_kind: "table_function",
            catalog_name: None,
            schema_name: "github",
            relation_name: "search_runs",
        },
        TaskQueryRelationWrite {
            relation_kind: "table",
            catalog_name: Some("github"),
            schema_name: "actions",
            relation_name: "runs",
        },
    ];
    const OTHER_QUERY_RELATIONS: &[TaskQueryRelationWrite<'static>] = &[TaskQueryRelationWrite {
        relation_kind: "table",
        catalog_name: None,
        schema_name: "linear",
        relation_name: "issues",
    }];

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
        assert_eq!(
            db.task_state()
                .complete(workspace_id, &task_id, "success", 4)
                .await
                .expect("complete task before recording query activity"),
            TaskCompletionUpdate::Completed
        );

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
        let id_base = uuid::Uuid::new_v4().as_u128() & !0xff;
        let first_id = uuid::Uuid::from_u128(id_base + 1).to_string();
        let second_id = uuid::Uuid::from_u128(id_base + 2).to_string();
        let other_id = uuid::Uuid::from_u128(id_base + 3).to_string();
        let first = TaskQueryRow {
            id: first_id,
            task_id: task_id.to_string(),
            intent: "First query".to_string(),
            sql: "SELECT 1".to_string(),
            status: "success".to_string(),
            started_at_unix_nanos: 10,
        };
        let second = TaskQueryRow {
            id: second_id,
            task_id: task_id.to_string(),
            intent: "Second query".to_string(),
            sql: "SELECT broken".to_string(),
            status: "error".to_string(),
            started_at_unix_nanos: 10,
        };
        let other = TaskQueryRow {
            id: other_id,
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
                        relations: query_relations(&row.id, &first.id, &other.id),
                    })
                    .await
                    .expect("record task query"),
                TaskQueryWriteResult::Recorded
            );
        }

        assert_wrong_workspace_rejected(db, other_workspace_id, task_id).await;
        assert_invalid_values_roll_back(db, workspace_id, task_id).await;

        assert_eq!(
            task_queries_for_workspace(db, workspace_id)
                .await
                .expect("list task queries"),
            vec![first.clone(), second.clone()]
        );
        assert_eq!(
            task_queries_for_workspace(db, other_workspace_id)
                .await
                .expect("list other workspace task queries"),
            vec![other]
        );
        assert_recorded_relations(db, &first.id, &second.id).await;

        let mut tx = db.begin().await.expect("begin delete tx");
        tx.tasks()
            .delete(workspace_id, task_id)
            .await
            .expect("delete task");
        tx.commit().await.expect("commit task delete");
        assert!(
            !task_query_exists_for_task(db, task_id)
                .await
                .expect("check cascaded task queries")
        );
        assert_no_relations(db, &first.id, "list cascaded task query relations").await;
        assert!(
            task_queries_for_workspace(db, workspace_id)
                .await
                .expect("list cascaded task queries")
                .is_empty()
        );
    }

    async fn assert_wrong_workspace_rejected(
        db: &CoralDb,
        other_workspace_id: &str,
        task_id: &str,
    ) {
        assert_eq!(
            db.task_query_state()
                .record(TaskQueryWrite {
                    workspace_id: other_workspace_id,
                    id: &uuid::Uuid::new_v4().to_string(),
                    task_id,
                    intent: "Wrong workspace",
                    sql: "SELECT 4",
                    status: "success",
                    started_at_unix_nanos: 11,
                    relations: &[],
                })
                .await
                .expect("reject cross-workspace task query"),
            TaskQueryWriteResult::TaskNotFound
        );
    }

    async fn assert_invalid_values_roll_back(db: &CoralDb, workspace_id: &str, task_id: &str) {
        let invalid_status_id = uuid::Uuid::new_v4().to_string();
        assert!(
            db.task_query_state()
                .record(TaskQueryWrite {
                    workspace_id,
                    id: &invalid_status_id,
                    task_id,
                    intent: "Invalid status",
                    sql: "SELECT 5",
                    status: "pending",
                    started_at_unix_nanos: 12,
                    relations: &[],
                })
                .await
                .is_err(),
            "task query status constraint should reject unknown values"
        );

        let invalid_relation_id = uuid::Uuid::new_v4().to_string();
        let invalid_relations = [TaskQueryRelationWrite {
            relation_kind: "view",
            catalog_name: None,
            schema_name: "github",
            relation_name: "issues",
        }];
        assert!(
            db.task_query_state()
                .record(TaskQueryWrite {
                    workspace_id,
                    id: &invalid_relation_id,
                    task_id,
                    intent: "Invalid relation",
                    sql: "SELECT 6",
                    status: "success",
                    started_at_unix_nanos: 13,
                    relations: &invalid_relations,
                })
                .await
                .is_err(),
            "task query relation kind constraint should reject unknown values"
        );

        let rows = task_queries_for_workspace(db, workspace_id)
            .await
            .expect("list task queries after rejected writes");
        assert!(
            rows.iter()
                .all(|row| row.id != invalid_status_id && row.id != invalid_relation_id),
            "failed task query writes should roll back their parent row"
        );
    }

    fn query_relations(
        query_id: &str,
        first_query_id: &str,
        other_query_id: &str,
    ) -> &'static [TaskQueryRelationWrite<'static>] {
        if query_id == first_query_id {
            FIRST_QUERY_RELATIONS
        } else if query_id == other_query_id {
            OTHER_QUERY_RELATIONS
        } else {
            &[]
        }
    }

    async fn assert_recorded_relations(db: &CoralDb, first_query_id: &str, error_query_id: &str) {
        assert_eq!(
            db.task_query_state()
                .list_relations_for_query(first_query_id)
                .await
                .expect("list task query relations"),
            vec![
                TaskQueryRelationRecord {
                    query_id: first_query_id.to_string(),
                    relation_kind: "table".to_string(),
                    catalog_name: Some("github".to_string()),
                    schema_name: "actions".to_string(),
                    relation_name: "runs".to_string(),
                },
                TaskQueryRelationRecord {
                    query_id: first_query_id.to_string(),
                    relation_kind: "table_function".to_string(),
                    catalog_name: None,
                    schema_name: "github".to_string(),
                    relation_name: "search_runs".to_string(),
                },
            ]
        );
        assert_no_relations(db, error_query_id, "list error query relations").await;
    }

    async fn assert_no_relations(db: &CoralDb, query_id: &str, context: &str) {
        assert!(
            db.task_query_state()
                .list_relations_for_query(query_id)
                .await
                .unwrap_or_else(|error| panic!("{context}: {error}"))
                .is_empty()
        );
    }

    async fn task_queries_for_workspace(
        db: &CoralDb,
        workspace_id: &str,
    ) -> Result<Vec<TaskQueryRow>, crate::state::db::DbError> {
        let mut session = db;
        session
            .task_queries()
            .list_for_workspace(workspace_id)
            .await
    }

    async fn task_query_exists_for_task(
        db: &CoralDb,
        task_id: &str,
    ) -> Result<bool, crate::state::db::DbError> {
        let mut session = db;
        session.task_queries().exists_for_task(task_id).await
    }
}
