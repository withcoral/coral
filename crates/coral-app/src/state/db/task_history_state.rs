//! Workspace-scoped read projections for retained task history.

use super::repositories::task_queries::{TaskQueryRelationHistoryRow, TaskQueryRow};
use super::repositories::tasks::TaskHistoryRow;
use super::{CoralDb, DbError, DbRepos};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct TaskHistoryTaskScan<'a> {
    pub(crate) task_id: Option<&'a str>,
    pub(crate) limit: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct TaskHistoryQueryScan<'a> {
    pub(crate) task_id: Option<&'a str>,
    pub(crate) query_id: Option<&'a str>,
    pub(crate) limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskHistoryTask {
    pub(crate) task_id: String,
    pub(crate) intent: String,
    pub(crate) outcome: Option<String>,
    pub(crate) started_at_unix_nanos: i64,
    pub(crate) completed_at_unix_nanos: Option<i64>,
    pub(crate) query_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskHistoryQuery {
    pub(crate) query_id: String,
    pub(crate) task_id: String,
    pub(crate) intent: String,
    pub(crate) started_at_unix_nanos: i64,
    pub(crate) sql: String,
    pub(crate) status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskHistoryRelation {
    pub(crate) query_id: String,
    pub(crate) task_id: String,
    pub(crate) relation_kind: String,
    pub(crate) catalog_name: String,
    pub(crate) schema_name: String,
    pub(crate) relation_name: String,
}

pub(crate) struct TaskHistoryState<'a> {
    db: &'a CoralDb,
}

impl CoralDb {
    pub(crate) fn task_history_state(&self) -> TaskHistoryState<'_> {
        TaskHistoryState { db: self }
    }
}

impl TaskHistoryState<'_> {
    pub(crate) async fn tasks(
        &self,
        workspace_id: &str,
        scan: TaskHistoryTaskScan<'_>,
    ) -> Result<Vec<TaskHistoryTask>, DbError> {
        let mut session = self.db;
        session
            .tasks()
            .list_history(workspace_id, scan.task_id, scan.limit)
            .await
            .map(|rows| rows.into_iter().map(TaskHistoryTask::from).collect())
    }

    pub(crate) async fn queries(
        &self,
        workspace_id: &str,
        scan: TaskHistoryQueryScan<'_>,
    ) -> Result<Vec<TaskHistoryQuery>, DbError> {
        let mut session = self.db;
        session
            .task_queries()
            .list_history(workspace_id, scan.task_id, scan.query_id, scan.limit)
            .await
            .map(|rows| rows.into_iter().map(TaskHistoryQuery::from).collect())
    }

    pub(crate) async fn relations(
        &self,
        workspace_id: &str,
        scan: TaskHistoryQueryScan<'_>,
    ) -> Result<Vec<TaskHistoryRelation>, DbError> {
        let mut session = self.db;
        session
            .task_queries()
            .list_relation_history(workspace_id, scan.task_id, scan.query_id, scan.limit)
            .await
            .map(|rows| rows.into_iter().map(TaskHistoryRelation::from).collect())
    }
}

impl From<TaskHistoryRow> for TaskHistoryTask {
    fn from(row: TaskHistoryRow) -> Self {
        Self {
            task_id: row.task_id,
            intent: row.intent,
            outcome: row.outcome,
            started_at_unix_nanos: row.started_at_unix_nanos,
            completed_at_unix_nanos: row.completed_at_unix_nanos,
            query_count: row.query_count,
        }
    }
}

impl From<TaskQueryRow> for TaskHistoryQuery {
    fn from(row: TaskQueryRow) -> Self {
        Self {
            query_id: row.id,
            task_id: row.task_id,
            intent: row.intent,
            started_at_unix_nanos: row.started_at_unix_nanos,
            sql: row.sql,
            status: row.status,
        }
    }
}

impl From<TaskQueryRelationHistoryRow> for TaskHistoryRelation {
    fn from(row: TaskQueryRelationHistoryRow) -> Self {
        Self {
            query_id: row.query_id,
            task_id: row.task_id,
            relation_kind: row.relation_kind,
            catalog_name: row.catalog_name,
            schema_name: row.schema_name,
            relation_name: row.relation_name,
        }
    }
}
