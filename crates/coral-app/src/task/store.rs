//! Database-backed task lifecycle store.

use std::sync::Arc;

use coral_api::CORAL_TASK_INTENT_MAX_CHARS;

use super::id::TaskId;
use crate::state::db::{CoralDb, DbError, DbRepos, TaskRecord, now_unix_nanos_i64};
use crate::trajectory_memory::SuggestedPath;
use crate::workspaces::WorkspaceName;

/// Final task status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskStatus {
    /// Task succeeded.
    Success,
    /// Task failed.
    Failure,
}

impl TaskStatus {
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
    pub(crate) intent: String,
    pub(crate) suggested_paths: Vec<SuggestedPath>,
}

/// A task end event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskEnd {
    pub(crate) id: TaskId,
    pub(crate) workspace: WorkspaceName,
    pub(crate) status: TaskStatus,
}

/// Errors from the task lifecycle store.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TaskStoreError {
    /// Database error reading or writing the store.
    #[error(transparent)]
    Database(#[from] DbError),
    /// System clock error while timestamping a lifecycle event.
    #[error("task store clock: {0}")]
    Clock(String),
    /// The task intent is empty or exceeds the maximum length.
    #[error("task intent must be non-empty and at most {max} characters")]
    InvalidIntent {
        /// The configured maximum intent length, in characters.
        max: usize,
    },
}

/// Database-backed task lifecycle persistence.
#[derive(Clone)]
pub(crate) struct TaskStore {
    db: Arc<CoralDb>,
}

impl TaskStore {
    pub(crate) fn new(db: Arc<CoralDb>) -> Self {
        Self { db }
    }

    pub(crate) async fn start_task(&self, start: &TaskStart) -> Result<(), TaskStoreError> {
        let intent = start.intent.trim();
        if intent.is_empty() || intent.chars().count() > CORAL_TASK_INTENT_MAX_CHARS {
            return Err(TaskStoreError::InvalidIntent {
                max: CORAL_TASK_INTENT_MAX_CHARS,
            });
        }
        let started_at_unix_nanos =
            now_unix_nanos_i64().map_err(|error| TaskStoreError::Clock(error.to_string()))?;
        let record = TaskRecord {
            id: start.id.to_string(),
            intent: intent.to_string(),
            status: None,
            started_at_unix_nanos,
            ended_at_unix_nanos: None,
        };
        let mut tx = self.db.begin().await?;
        tx.workspaces()
            .ensure(start.workspace.as_str(), started_at_unix_nanos)
            .await?;
        tx.tasks().insert(start.workspace.as_str(), &record).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn end_task(&self, end: &TaskEnd) -> Result<(), TaskStoreError> {
        let ended_at_unix_nanos =
            now_unix_nanos_i64().map_err(|error| TaskStoreError::Clock(error.to_string()))?;
        let mut tx = self.db.begin().await?;
        tx.tasks()
            .update_status(
                end.workspace.as_str(),
                &end.id.to_string(),
                end.status.as_str(),
                ended_at_unix_nanos,
            )
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn task(
        &self,
        workspace: &WorkspaceName,
        task_id: &TaskId,
    ) -> Result<Option<TaskRecord>, TaskStoreError> {
        let mut session = self.db.as_ref();
        session
            .tasks()
            .get(workspace.as_str(), &task_id.to_string())
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::{TaskEnd, TaskStart, TaskStatus, TaskStore, TaskStoreError};
    use crate::state::AppStateLayout;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::task::id::TaskId;
    use crate::workspaces::WorkspaceName;

    const TASK_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

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
        (dir, TaskStore::new(db))
    }

    fn start(workspace: &WorkspaceName, intent: &str) -> TaskStart {
        TaskStart {
            id: TaskId::parse(TASK_ID).expect("valid task id"),
            workspace: workspace.clone(),
            intent: intent.to_string(),
            suggested_paths: Vec::new(),
        }
    }

    #[tokio::test]
    async fn stores_task_lifecycle_in_database() {
        let (_dir, store) = store().await;
        let workspace = WorkspaceName::default();
        let start = start(&workspace, "  Find renewal risk  ");

        store.start_task(&start).await.expect("start task");
        let stored = store
            .task(&workspace, &start.id)
            .await
            .expect("get task")
            .expect("task");
        assert_eq!(stored.intent, "Find renewal risk");
        assert_eq!(stored.status, None);

        store
            .end_task(&TaskEnd {
                id: start.id,
                workspace: workspace.clone(),
                status: TaskStatus::Success,
            })
            .await
            .expect("end task");

        let ended = store
            .task(&workspace, &start.id)
            .await
            .expect("get ended task")
            .expect("ended task");
        assert_eq!(ended.status.as_deref(), Some("success"));
        assert!(ended.ended_at_unix_nanos.is_some());
    }

    #[tokio::test]
    async fn validates_intent_before_writing() {
        let (_dir, store) = store().await;
        let error = store
            .start_task(&start(&WorkspaceName::default(), " "))
            .await
            .expect_err("blank intent");
        assert!(matches!(error, TaskStoreError::InvalidIntent { .. }));
    }
}
