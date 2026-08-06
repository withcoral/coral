//! Durable SQL activity for validated tasks.

use std::sync::Arc;

use crate::bootstrap::AppError;
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

struct TaskQueryRecord<'a> {
    id: uuid::Uuid,
    task_id: TaskId,
    intent: &'a str,
    sql: &'a str,
    status: TaskQueryStatus,
    started_at_unix_nanos: i64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TaskActivityError {
    #[error("could not timestamp task query activity: {0}")]
    Timestamp(#[from] AppError),
    #[error(transparent)]
    Database(#[from] DbError),
    #[error("task '{task_id}' was not found in workspace '{workspace}'")]
    TaskNotFound { task_id: String, workspace: String },
}

#[derive(Clone)]
pub(crate) struct TaskActivityRecorder {
    db: Arc<CoralDb>,
}

pub(crate) struct PendingTaskQuery<'a> {
    recorder: TaskActivityRecorder,
    workspace: &'a WorkspaceName,
    id: uuid::Uuid,
    task_id: TaskId,
    intent: &'a str,
    started_at_unix_nanos: i64,
}

impl TaskActivityRecorder {
    pub(crate) fn new(db: Arc<CoralDb>) -> Self {
        Self { db }
    }

    pub(crate) fn begin_query<'a>(
        &self,
        workspace: &'a WorkspaceName,
        task_id: TaskId,
        intent: &'a str,
    ) -> Result<PendingTaskQuery<'a>, TaskActivityError> {
        Ok(PendingTaskQuery {
            recorder: self.clone(),
            workspace,
            id: uuid::Uuid::new_v4(),
            task_id,
            intent,
            started_at_unix_nanos: crate::state::db::now_unix_nanos_i64()?,
        })
    }

    async fn record_query(
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

impl PendingTaskQuery<'_> {
    pub(crate) fn id(&self) -> uuid::Uuid {
        self.id
    }

    pub(crate) fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub(crate) async fn finish(
        self,
        sql: &str,
        status: TaskQueryStatus,
    ) -> Result<(), TaskActivityError> {
        self.recorder
            .record_query(
                self.workspace,
                TaskQueryRecord {
                    id: self.id,
                    task_id: self.task_id,
                    intent: self.intent,
                    sql,
                    status,
                    started_at_unix_nanos: self.started_at_unix_nanos,
                },
            )
            .await
    }
}
