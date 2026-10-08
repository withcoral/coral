//! Database-backed task lifecycle store.

use std::sync::Arc;

use coral_api::CORAL_TASK_INTENT_MAX_CHARS;

use super::id::TaskId;
use crate::identity::Principal;
use crate::state::db::{
    CoralDb, DbError, TaskCompletionUpdate, TaskCreation, TaskCreationResult, TaskLifecycleState,
};
use crate::workspaces::WorkspaceName;

const MAX_RETAINED_TASKS_PER_WORKSPACE: u64 = 10_000;

/// Final task outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskOutcome {
    /// Task succeeded.
    Success,
    /// Task failed.
    Failure,
}

impl TaskOutcome {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
        }
    }
}

/// A task start event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskStart {
    pub(crate) id: TaskId,
    pub(crate) workspace: WorkspaceName,
    pub(crate) created_by: Principal,
    pub(crate) intent: String,
    pub(crate) created_at_unix_nanos: i64,
}

/// A terminal task completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskCompletion {
    pub(crate) id: TaskId,
    pub(crate) workspace: WorkspaceName,
    pub(crate) outcome: TaskOutcome,
    pub(crate) completed_at_unix_nanos: i64,
}

/// Result of attempting the one-way transition to a terminal task outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskCompletionResult {
    /// The active task was completed.
    Completed,
    /// The task exists but was already terminal.
    AlreadyCompleted,
    /// No task with this id exists in the workspace.
    NotFound,
}

/// Errors from the task lifecycle store.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TaskStoreError {
    /// Database error reading or writing the store.
    #[error(transparent)]
    Database(#[from] DbError),
    /// The task intent is empty or exceeds the maximum length.
    #[error("task intent must be non-empty and at most {max} characters")]
    InvalidIntent {
        /// The configured maximum intent length, in characters.
        max: usize,
    },
    /// The task's workspace was removed before persistence began.
    #[error("workspace '{workspace}' was not found")]
    WorkspaceNotFound {
        /// Missing workspace identifier.
        workspace: String,
    },
    /// The workspace has reached its retained active-task limit.
    #[error("workspace '{workspace}' already has {max} active tasks")]
    WorkspaceCapacityExceeded {
        /// Full workspace identifier.
        workspace: String,
        /// Maximum retained tasks.
        max: u64,
    },
}

/// Database-backed task lifecycle persistence.
#[derive(Clone)]
pub(crate) struct TaskStore {
    db: Arc<CoralDb>,
    max_retained_tasks: u64,
}

impl TaskStore {
    pub(crate) fn new(db: Arc<CoralDb>) -> Self {
        Self {
            db,
            max_retained_tasks: MAX_RETAINED_TASKS_PER_WORKSPACE,
        }
    }

    #[cfg(test)]
    fn with_max_retained_tasks(mut self, max_retained_tasks: u64) -> Self {
        self.max_retained_tasks = max_retained_tasks;
        self
    }

    pub(crate) async fn start_task(&self, start: &TaskStart) -> Result<(), TaskStoreError> {
        let intent = validate_intent(&start.intent)?;
        let task_id = start.id.to_string();
        let result = self
            .db
            .task_state()
            .create(
                TaskCreation {
                    id: &task_id,
                    workspace_id: start.workspace.as_str(),
                    created_by_principal_id: start.created_by.id().as_str(),
                    intent,
                    created_at_unix_nanos: start.created_at_unix_nanos,
                },
                self.max_retained_tasks,
            )
            .await?;
        match result {
            TaskCreationResult::Created => Ok(()),
            TaskCreationResult::WorkspaceNotFound => Err(TaskStoreError::WorkspaceNotFound {
                workspace: start.workspace.to_string(),
            }),
            TaskCreationResult::WorkspaceCapacityExceeded => {
                Err(TaskStoreError::WorkspaceCapacityExceeded {
                    workspace: start.workspace.to_string(),
                    max: self.max_retained_tasks,
                })
            }
        }
    }

    pub(crate) async fn complete_task(
        &self,
        completion: &TaskCompletion,
    ) -> Result<TaskCompletionResult, TaskStoreError> {
        let update = self
            .db
            .task_state()
            .complete(
                completion.workspace.as_str(),
                &completion.id.to_string(),
                completion.outcome.as_str(),
                completion.completed_at_unix_nanos,
            )
            .await?;
        Ok(match update {
            TaskCompletionUpdate::Completed => TaskCompletionResult::Completed,
            TaskCompletionUpdate::AlreadyCompleted => TaskCompletionResult::AlreadyCompleted,
            TaskCompletionUpdate::NotFound => TaskCompletionResult::NotFound,
        })
    }

    pub(crate) async fn task_state(
        &self,
        workspace: &WorkspaceName,
        task_id: TaskId,
    ) -> Result<Option<TaskLifecycleState>, TaskStoreError> {
        let task_id = task_id.to_string();
        self.db
            .task_state()
            .lifecycle(workspace.as_str(), &task_id)
            .await
            .map_err(Into::into)
    }
}

fn validate_intent(intent: &str) -> Result<&str, TaskStoreError> {
    if intent.trim().is_empty() || intent.chars().count() > CORAL_TASK_INTENT_MAX_CHARS {
        return Err(TaskStoreError::InvalidIntent {
            max: CORAL_TASK_INTENT_MAX_CHARS,
        });
    }
    Ok(intent)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::{
        TaskCompletion, TaskCompletionResult, TaskOutcome, TaskStart, TaskStore, TaskStoreError,
        validate_intent,
    };
    use crate::identity::{Principal, PrincipalKind};
    use crate::state::AppStateLayout;
    use crate::state::db::{CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig};
    use crate::task::id::TaskId;
    use crate::workspaces::WorkspaceName;

    const TASK_ID_1: &str = "550e8400-e29b-41d4-a716-446655440000";
    const TASK_ID_2: &str = "650e8400-e29b-41d4-a716-446655440000";

    async fn store() -> (TempDir, TaskStore) {
        let dir = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(dir.path().join("coral-config")))
            .expect("layout should resolve");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("default database is sqlite");
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
        (dir, TaskStore::new(db))
    }

    fn start(workspace: &WorkspaceName, task_id: &str, intent: &str) -> TaskStart {
        start_for_principal(workspace, task_id, intent, Principal::local())
    }

    fn start_for_principal(
        workspace: &WorkspaceName,
        task_id: &str,
        intent: &str,
        created_by: Principal,
    ) -> TaskStart {
        TaskStart {
            id: TaskId::parse(task_id).expect("valid task id"),
            workspace: workspace.clone(),
            created_by,
            intent: intent.to_string(),
            created_at_unix_nanos: 2,
        }
    }

    fn completion(start: &TaskStart) -> TaskCompletion {
        TaskCompletion {
            id: start.id,
            workspace: start.workspace.clone(),
            outcome: TaskOutcome::Success,
            completed_at_unix_nanos: 3,
        }
    }

    #[tokio::test]
    async fn stores_task_lifecycle_in_database() {
        let (_dir, store) = store().await;
        let workspace = WorkspaceName::default();
        let start = start(&workspace, TASK_ID_1, "  Find renewal risk  ");

        assert_eq!(
            store
                .complete_task(&completion(&start))
                .await
                .expect("end missing task"),
            TaskCompletionResult::NotFound
        );
        store.start_task(&start).await.expect("start task");
        assert_eq!(
            store
                .complete_task(&completion(&start))
                .await
                .expect("complete stored task"),
            TaskCompletionResult::Completed
        );
        assert_eq!(
            store
                .complete_task(&completion(&start))
                .await
                .expect("end already terminal task"),
            TaskCompletionResult::AlreadyCompleted
        );
    }

    #[tokio::test]
    async fn task_lifecycle_uses_workspace_and_id() {
        let (_dir, store) = store().await;
        let workspace = WorkspaceName::default();
        let start = start(&workspace, TASK_ID_1, "Find renewal risk");
        store.start_task(&start).await.expect("start task");

        assert_eq!(
            store
                .complete_task(&completion(&start))
                .await
                .expect("end task by workspace and id"),
            TaskCompletionResult::Completed
        );
        assert_eq!(
            store
                .complete_task(&completion(&start))
                .await
                .expect("task is already terminal"),
            TaskCompletionResult::AlreadyCompleted
        );
    }

    #[tokio::test]
    async fn evicts_oldest_completed_task_at_retention_limit() {
        let (_dir, store) = store().await;
        let store = store.with_max_retained_tasks(1);
        let workspace = WorkspaceName::default();
        let first = start(&workspace, TASK_ID_1, "First task");
        store.start_task(&first).await.expect("start first task");
        assert_eq!(
            store
                .complete_task(&completion(&first))
                .await
                .expect("complete first task"),
            TaskCompletionResult::Completed
        );

        let second = start(&workspace, TASK_ID_2, "Second task");
        store.start_task(&second).await.expect("start second task");
        assert_eq!(
            store
                .complete_task(&completion(&first))
                .await
                .expect("oldest completed task was evicted"),
            TaskCompletionResult::NotFound
        );
        assert_eq!(
            store
                .complete_task(&completion(&second))
                .await
                .expect("new task remains"),
            TaskCompletionResult::Completed
        );
    }

    #[tokio::test]
    async fn rejects_new_task_instead_of_evicting_an_active_task() {
        let (_dir, store) = store().await;
        let store = store.with_max_retained_tasks(1);
        let workspace = WorkspaceName::default();
        let first = start(&workspace, TASK_ID_1, "First task");
        store.start_task(&first).await.expect("start first task");

        let second = start_for_principal(
            &workspace,
            TASK_ID_2,
            "Second task",
            Principal::parse("product:principal:other", PrincipalKind::Agent)
                .expect("other principal"),
        );
        assert!(matches!(
            store.start_task(&second).await,
            Err(TaskStoreError::WorkspaceCapacityExceeded { .. })
        ));
        assert_eq!(
            store
                .complete_task(&completion(&first))
                .await
                .expect("existing active task remains"),
            TaskCompletionResult::Completed
        );
    }

    #[tokio::test]
    async fn validates_intent_before_writing() {
        let (_dir, store) = store().await;
        let error = store
            .start_task(&start(&WorkspaceName::default(), TASK_ID_1, " "))
            .await
            .expect_err("blank intent");
        assert!(matches!(error, TaskStoreError::InvalidIntent { .. }));
    }

    #[test]
    fn intent_validation_preserves_authored_text() {
        let authored = "  Find renewal risk  ";
        assert_eq!(
            validate_intent(authored).expect("authored intent should be valid"),
            authored
        );
    }
}
