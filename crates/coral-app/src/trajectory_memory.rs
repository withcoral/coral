//! Raw task-attributed trajectory capture.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::bootstrap::AppError;
use crate::state::db::{CoralDb, DbRepos, RawTrajectoryStepRecord};
use crate::task::id::TaskId;
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct TrajectoryOutputSummary {
    pub(crate) sources: Vec<String>,
    pub(crate) relations: Vec<String>,
    #[serde(default)]
    pub(crate) column_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawTrajectoryStep {
    pub(crate) task_id: TaskId,
    pub(crate) started_at_unix_nanos: i64,
    pub(crate) completed_at_unix_nanos: i64,
    pub(crate) operation: String,
    pub(crate) input: String,
    pub(crate) status: &'static str,
    pub(crate) row_count: Option<u64>,
    pub(crate) output_summary: Option<TrajectoryOutputSummary>,
    pub(crate) error_kind: Option<String>,
    pub(crate) error_type: Option<String>,
    pub(crate) error_message: Option<String>,
}

#[derive(Clone)]
pub(crate) struct TrajectoryMemoryManager {
    db: Arc<CoralDb>,
}

impl TrajectoryMemoryManager {
    pub(crate) fn new(db: Arc<CoralDb>) -> Self {
        Self { db }
    }

    pub(crate) async fn record_raw_step(
        &self,
        workspace: &WorkspaceName,
        step: RawTrajectoryStep,
    ) -> Result<(), AppError> {
        let task_id = step.task_id.to_string();
        let mut session = self.db.as_ref();
        if session
            .tasks()
            .get(workspace.as_str(), &task_id)
            .await?
            .is_none()
        {
            return Ok(());
        }
        let row_count = step
            .row_count
            .map(i64::try_from)
            .transpose()
            .map_err(|error| {
                AppError::FailedPrecondition(format!("trajectory row count exceeds i64: {error}"))
            })?;
        let output_summary_json = step
            .output_summary
            .map(|summary| serde_json::to_string(&summary))
            .transpose()?;
        session
            .trajectory_memory()
            .insert_raw_step(
                workspace.as_str(),
                &RawTrajectoryStepRecord {
                    id: format!("raw_{}", uuid::Uuid::new_v4().simple()),
                    task_id,
                    started_at_unix_nanos: step.started_at_unix_nanos,
                    completed_at_unix_nanos: step.completed_at_unix_nanos,
                    operation: step.operation,
                    input: step.input,
                    status: step.status.to_string(),
                    row_count,
                    output_summary_json,
                    error_kind: step.error_kind,
                    error_type: step.error_type,
                    error_message: step.error_message,
                },
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::{RawTrajectoryStep, TrajectoryMemoryManager, TrajectoryOutputSummary};
    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::{CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig};
    use crate::task::id::TaskId;
    use crate::task::manager::TaskManager;
    use crate::task::store::TaskStore;
    use crate::workspaces::WorkspaceName;

    #[tokio::test]
    async fn raw_capture_round_trips_against_sqlite() {
        let (temp, db) = open_sqlite().await;

        assert_raw_capture(db, WorkspaceName::default()).await;

        drop(temp);
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared raw trajectory harness against Postgres"]
    async fn raw_capture_round_trips_against_postgres() {
        let Some(url) = postgres_test_url() else {
            return;
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
                .await
                .expect("open postgres"),
        );
        db.migrate().await.expect("migrate postgres");
        let workspace =
            WorkspaceName::parse(&format!("trajectory_{}", uuid::Uuid::new_v4().simple()))
                .expect("workspace");

        assert_raw_capture(db, workspace).await;
    }

    async fn assert_raw_capture(db: Arc<CoralDb>, workspace: WorkspaceName) {
        let memory = TrajectoryMemoryManager::new(Arc::clone(&db));
        let tasks = TaskManager::new(TaskStore::new(Arc::clone(&db)));
        let task = tasks
            .start_task(workspace.clone(), "Find renewal risk".to_string())
            .await
            .expect("start task");

        memory
            .record_raw_step(
                &workspace,
                RawTrajectoryStep {
                    task_id: task.id,
                    started_at_unix_nanos: 20,
                    completed_at_unix_nanos: 21,
                    operation: "execute_sql".to_string(),
                    input: "SELECT customer_id FROM crm.accounts WHERE region = 'emea'".to_string(),
                    status: "success",
                    row_count: Some(3),
                    output_summary: Some(TrajectoryOutputSummary {
                        sources: vec!["crm".to_string()],
                        relations: vec!["crm.accounts".to_string()],
                        column_count: Some(1),
                    }),
                    error_kind: None,
                    error_type: None,
                    error_message: None,
                },
            )
            .await
            .expect("record successful step");
        memory
            .record_raw_step(
                &workspace,
                RawTrajectoryStep {
                    task_id: task.id,
                    started_at_unix_nanos: 10,
                    completed_at_unix_nanos: 11,
                    operation: "search".to_string(),
                    input: "renewal risk".to_string(),
                    status: "error",
                    row_count: None,
                    output_summary: None,
                    error_kind: Some("app".to_string()),
                    error_type: Some("SEARCH".to_string()),
                    error_message: Some("search failed".to_string()),
                },
            )
            .await
            .expect("record failed step");
        memory
            .record_raw_step(
                &workspace,
                RawTrajectoryStep {
                    task_id: TaskId::new(),
                    started_at_unix_nanos: 30,
                    completed_at_unix_nanos: 31,
                    operation: "execute_sql".to_string(),
                    input: "SELECT 1".to_string(),
                    status: "success",
                    row_count: Some(1),
                    output_summary: None,
                    error_kind: None,
                    error_type: None,
                    error_message: None,
                },
            )
            .await
            .expect("unknown task is ignored");

        let mut session = db.as_ref();
        let raw = session
            .trajectory_memory()
            .list_raw_steps_for_task(workspace.as_str(), &task.id.to_string())
            .await
            .expect("list raw steps");
        let mut raw = raw.into_iter();
        let search = raw.next().expect("search step");
        assert_eq!(search.operation, "search");
        assert_eq!(search.status, "error");
        assert_eq!(search.error_type.as_deref(), Some("SEARCH"));
        let sql = raw.next().expect("SQL step");
        assert_eq!(sql.operation, "execute_sql");
        assert_eq!(sql.row_count, Some(3));
        let summary = sql.output_summary_json.as_deref().expect("output summary");
        assert!(summary.contains("crm.accounts"));
        assert!(!summary.contains("customer_id"));
        assert!(raw.next().is_none());
    }

    async fn open_sqlite() -> (TempDir, Arc<CoralDb>) {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
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
        (temp, db)
    }

    fn postgres_test_url() -> Option<String> {
        bootstrap::env_var("CORAL_TEST_POSTGRES_URL").expect("read CORAL_TEST_POSTGRES_URL")
    }
}
