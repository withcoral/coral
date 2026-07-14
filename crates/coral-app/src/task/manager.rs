//! App-domain task lifecycle orchestration.

use super::id::TaskId;
use super::store::{TaskEnd, TaskStart, TaskStatus, TaskStore, TaskStoreError};
use crate::bootstrap::AppError;
use crate::trajectory_memory::TrajectoryMemoryManager;
use crate::workspaces::WorkspaceName;

/// Coordinates task lifecycle persistence and validation.
#[derive(Clone)]
pub(crate) struct TaskManager {
    store: TaskStore,
    trajectory_memory: TrajectoryMemoryManager,
}

impl TaskManager {
    pub(crate) fn new(store: TaskStore, trajectory_memory: TrajectoryMemoryManager) -> Self {
        Self {
            store,
            trajectory_memory,
        }
    }

    pub(crate) async fn start_task(
        &self,
        workspace: WorkspaceName,
        intent: String,
    ) -> Result<TaskStart, TaskManagerError> {
        let suggested_paths = self
            .trajectory_memory
            .suggested_paths(&workspace, &intent)
            .await?;
        let start = TaskStart {
            id: TaskId::new(),
            workspace,
            intent,
            suggested_paths,
        };
        self.store.start_task(&start).await?;
        Ok(start)
    }

    pub(crate) async fn end_task(
        &self,
        workspace: WorkspaceName,
        task_id: TaskId,
        status: TaskStatus,
    ) -> Result<TaskEnd, TaskManagerError> {
        if self.store.task(&workspace, &task_id).await?.is_none() {
            return Err(TaskManagerError::TaskNotFound {
                task_id: task_id.to_string(),
            });
        }
        let end = TaskEnd {
            id: task_id,
            workspace,
            status,
        };
        self.store.end_task(&end).await?;
        if status == TaskStatus::Success {
            self.trajectory_memory
                .distill_and_index(&end.workspace, &end.id)
                .await?;
        }
        Ok(end)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TaskManagerError {
    #[error("task '{task_id}' was not found")]
    TaskNotFound { task_id: String },
    #[error(transparent)]
    Store(#[from] TaskStoreError),
    #[error(transparent)]
    TrajectoryMemory(#[from] AppError),
}
