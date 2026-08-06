//! Durable SQL activity for validated tasks.

use std::sync::Arc;

use crate::state::db::{CoralDb, DbError, TaskQueryWrite, TaskQueryWriteResult};
use crate::task::id::TaskId;
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskQueryStatus {
    Success,
    Error,
}

impl TaskQueryStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
        }
    }
}

pub(crate) struct TaskQueryRecord<'a> {
    pub(crate) id: uuid::Uuid,
    pub(crate) task_id: TaskId,
    pub(crate) intent: &'a str,
    pub(crate) sql: &'a str,
    pub(crate) status: TaskQueryStatus,
    pub(crate) started_at_unix_nanos: i64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TaskActivityError {
    #[error(transparent)]
    Database(#[from] DbError),
    #[error("task '{task_id}' was not found in workspace '{workspace}'")]
    TaskNotFound { task_id: String, workspace: String },
}

#[derive(Clone)]
pub(crate) struct TaskActivityRecorder {
    db: Arc<CoralDb>,
}

impl TaskActivityRecorder {
    pub(crate) fn new(db: Arc<CoralDb>) -> Self {
        Self { db }
    }

    pub(crate) async fn record_query(
        &self,
        workspace: &WorkspaceName,
        record: TaskQueryRecord<'_>,
    ) -> Result<(), TaskActivityError> {
        let id = record.id.to_string();
        let task_id = record.task_id.to_string();
        match self
            .db
            .task_query_state()
            .record(TaskQueryWrite {
                workspace_id: workspace.as_str(),
                id: &id,
                task_id: &task_id,
                intent: record.intent,
                sql: record.sql,
                status: record.status.as_str(),
                started_at_unix_nanos: record.started_at_unix_nanos,
            })
            .await?
        {
            TaskQueryWriteResult::Recorded => Ok(()),
            TaskQueryWriteResult::TaskNotFound => Err(TaskActivityError::TaskNotFound {
                task_id,
                workspace: workspace.to_string(),
            }),
        }
    }
}
