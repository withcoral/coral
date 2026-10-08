//! App-domain task lifecycle orchestration.

use super::id::TaskId;
use super::store::{
    TaskCompletion, TaskCompletionResult, TaskOutcome, TaskStart, TaskStore, TaskStoreError,
};
use crate::bootstrap::AppError;
use crate::identity::Principal;
use crate::state::db::{TaskLifecycleState, now_unix_nanos_i64};
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

    pub(crate) async fn start_task(
        &self,
        workspace: WorkspaceName,
        created_by: Principal,
        intent: String,
    ) -> Result<TaskStart, TaskManagerError> {
        let start = TaskStart {
            id: TaskId::new(),
            workspace,
            created_by,
            intent,
            created_at_unix_nanos: now_unix_nanos_i64()?,
        };
        self.store.start_task(&start).await?;
        Ok(start)
    }

    pub(crate) async fn complete_task(
        &self,
        workspace: WorkspaceName,
        task_id: TaskId,
        outcome: TaskOutcome,
    ) -> Result<TaskCompletion, TaskManagerError> {
        let completion = TaskCompletion {
            id: task_id,
            workspace,
            outcome,
            completed_at_unix_nanos: now_unix_nanos_i64()?,
        };
        match self.store.complete_task(&completion).await? {
            TaskCompletionResult::Completed => Ok(completion),
            TaskCompletionResult::AlreadyCompleted => Err(TaskManagerError::TaskAlreadyCompleted {
                task_id: task_id.to_string(),
            }),
            TaskCompletionResult::NotFound => Err(TaskManagerError::TaskNotFound {
                task_id: task_id.to_string(),
            }),
        }
    }

    pub(crate) async fn validate_attribution(
        &self,
        workspace: &WorkspaceName,
        task_id: Option<TaskId>,
    ) -> Result<Option<TaskId>, TaskManagerError> {
        let Some(task_id) = task_id else {
            return Ok(None);
        };
        match self.store.task_state(workspace, task_id).await? {
            Some(TaskLifecycleState::Active) => Ok(Some(task_id)),
            Some(TaskLifecycleState::Completed) => Err(TaskManagerError::TaskAlreadyCompleted {
                task_id: task_id.to_string(),
            }),
            None => Err(TaskManagerError::TaskNotFound {
                task_id: task_id.to_string(),
            }),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TaskManagerError {
    #[error("task '{task_id}' was not found")]
    TaskNotFound { task_id: String },
    #[error("task '{task_id}' has already completed")]
    TaskAlreadyCompleted { task_id: String },
    #[error(transparent)]
    App(#[from] AppError),
    #[error(transparent)]
    Store(#[from] TaskStoreError),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::{TaskManager, TaskManagerError};
    use crate::identity::{Principal, PrincipalKind};
    use crate::state::AppStateLayout;
    use crate::state::db::{CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig};
    use crate::task::id::TaskId;
    use crate::task::store::{TaskOutcome, TaskStore};
    use crate::workspaces::WorkspaceName;

    async fn manager() -> (TempDir, TaskManager) {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("default test database must be SQLite");
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");
        let mut tx = db.begin().await.expect("begin workspace tx");
        tx.workspaces()
            .ensure(WorkspaceName::default().as_str(), 1)
            .await
            .expect("seed default workspace");
        tx.commit().await.expect("commit workspace");
        (temp, TaskManager::new(TaskStore::new(db)))
    }

    #[tokio::test]
    async fn validates_only_active_tasks_in_the_exact_workspace() {
        let (_temp, manager) = manager().await;
        let workspace = WorkspaceName::default();
        let created_by =
            Principal::parse("product:principal:saul", PrincipalKind::User).expect("creator");
        let task = manager
            .start_task(
                workspace.clone(),
                created_by,
                "Investigate renewal risk".to_string(),
            )
            .await
            .expect("start task");

        assert_eq!(
            manager
                .validate_attribution(&workspace, Some(task.id))
                .await
                .expect("validate active task"),
            Some(task.id)
        );
        assert_eq!(
            manager
                .validate_attribution(&workspace, None)
                .await
                .expect("allow untagged non-MCP request"),
            None
        );
        assert!(matches!(
            manager
                .validate_attribution(
                    &WorkspaceName::parse("other").expect("workspace"),
                    Some(task.id),
                )
                .await,
            Err(TaskManagerError::TaskNotFound { .. })
        ));
        assert!(matches!(
            manager
                .validate_attribution(&workspace, Some(TaskId::new()))
                .await,
            Err(TaskManagerError::TaskNotFound { .. })
        ));

        manager
            .complete_task(workspace.clone(), task.id, TaskOutcome::Success)
            .await
            .expect("end task");
        assert!(matches!(
            manager
                .validate_attribution(&workspace, Some(task.id))
                .await,
            Err(TaskManagerError::TaskAlreadyCompleted { .. })
        ));
    }
}
