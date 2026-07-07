//! App-domain task lifecycle orchestration.

use super::id::TaskId;
use super::store::{TaskEnd, TaskStart, TaskStatus, TaskStore, TaskStoreError};
use crate::workspaces::WorkspaceName;

/// Coordinates task lifecycle persistence and validation.
#[derive(Clone)]
pub(crate) struct TaskManager {
    store: TaskStore,
}

impl TaskManager {
    pub(crate) fn new(store: TaskStore) -> Self {
        Self { store }
    }

    pub(crate) fn start_task(
        &self,
        workspace: WorkspaceName,
        intent: String,
    ) -> Result<TaskStart, TaskManagerError> {
        let start = TaskStart {
            id: TaskId::new(),
            workspace,
            intent,
        };
        self.store.start_task(&start)?;
        Ok(start)
    }

    pub(crate) fn end_task(
        &self,
        workspace: WorkspaceName,
        task_id: TaskId,
        status: TaskStatus,
    ) -> Result<TaskEnd, TaskManagerError> {
        if !self.store.contains_started_task(&workspace, &task_id)? {
            return Err(TaskManagerError::TaskNotFound {
                task_id: task_id.to_string(),
            });
        }
        let end = TaskEnd {
            id: task_id,
            workspace,
            status,
        };
        self.store.end_task(&end)?;
        Ok(end)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TaskManagerError {
    #[error("task '{task_id}' was not found")]
    TaskNotFound { task_id: String },
    #[error(transparent)]
    Store(#[from] TaskStoreError),
}
