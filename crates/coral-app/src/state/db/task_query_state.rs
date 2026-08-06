//! Transactional persistence for workspace-scoped task query activity.

use super::repositories::task_queries::TaskQueryRow;
use super::{CoralDb, DbError, DbRepos};

pub(crate) struct TaskQueryWrite<'a> {
    pub(crate) workspace_id: &'a str,
    pub(crate) id: &'a str,
    pub(crate) task_id: &'a str,
    pub(crate) intent: &'a str,
    pub(crate) sql: &'a str,
    pub(crate) status: &'a str,
    pub(crate) started_at_unix_nanos: i64,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskQueryRecord {
    pub(crate) id: String,
    pub(crate) task_id: String,
    pub(crate) intent: String,
    pub(crate) sql: String,
    pub(crate) status: String,
    pub(crate) started_at_unix_nanos: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskQueryWriteResult {
    Recorded,
    TaskNotFound,
}

pub(crate) struct TaskQueryState<'a> {
    db: &'a CoralDb,
}

impl CoralDb {
    pub(crate) fn task_query_state(&self) -> TaskQueryState<'_> {
        TaskQueryState { db: self }
    }
}

impl TaskQueryState<'_> {
    pub(crate) async fn record(
        &self,
        query: TaskQueryWrite<'_>,
    ) -> Result<TaskQueryWriteResult, DbError> {
        let mut tx = self.db.begin().await?;
        if !tx
            .workspaces()
            .hold_for_child_mutation(query.workspace_id)
            .await?
            || tx
                .tasks()
                .find_state(query.workspace_id, query.task_id)
                .await?
                .is_none()
        {
            tx.rollback().await?;
            return Ok(TaskQueryWriteResult::TaskNotFound);
        }

        tx.task_queries()
            .insert(&TaskQueryRow {
                id: query.id.to_string(),
                task_id: query.task_id.to_string(),
                intent: query.intent.to_string(),
                sql: query.sql.to_string(),
                status: query.status.to_string(),
                started_at_unix_nanos: query.started_at_unix_nanos,
            })
            .await?;
        tx.commit().await?;
        Ok(TaskQueryWriteResult::Recorded)
    }

    #[cfg(test)]
    pub(crate) async fn list_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<TaskQueryRecord>, DbError> {
        let mut session = self.db;
        session
            .task_queries()
            .list_for_workspace(workspace_id)
            .await
            .map(|rows| rows.into_iter().map(TaskQueryRecord::from).collect())
    }
}

#[cfg(test)]
impl From<TaskQueryRow> for TaskQueryRecord {
    fn from(row: TaskQueryRow) -> Self {
        Self {
            id: row.id,
            task_id: row.task_id,
            intent: row.intent,
            sql: row.sql,
            status: row.status,
            started_at_unix_nanos: row.started_at_unix_nanos,
        }
    }
}
