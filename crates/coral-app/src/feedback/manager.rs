use std::future::Future;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::feedback::publisher::FeedbackPublisher;
#[cfg(test)]
use crate::feedback::publisher::NoopFeedbackPublisher;
use crate::state::AppStateLayout;
use crate::state::db::{CoralDb, DbRepos, FeedbackReportRecord};
use crate::storage::fs::{self as storage_fs, FileLock};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone)]
pub(crate) struct FeedbackReport {
    pub(crate) id: String,
    pub(crate) workspace: WorkspaceName,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) trying_to_do: String,
    pub(crate) tried: String,
    pub(crate) stuck: String,
}

#[derive(Debug, Clone)]
pub(crate) struct FeedbackSubmission {
    pub(crate) report: FeedbackReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FeedbackUploadStatus {
    Accepted,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FeedbackUpload {
    pub(crate) status: FeedbackUploadStatus,
    pub(crate) error_message: Option<String>,
}

impl FeedbackUpload {
    pub(crate) fn accepted() -> Self {
        Self {
            status: FeedbackUploadStatus::Accepted,
            error_message: None,
        }
    }

    pub(crate) fn failed(error_message: String) -> Self {
        Self {
            status: FeedbackUploadStatus::Failed,
            error_message: Some(error_message),
        }
    }
}

#[derive(Debug, Serialize)]
struct PersistedFeedbackReport<'a> {
    id: &'a str,
    workspace: &'a str,
    created_at: String,
    trying_to_do: &'a str,
    tried: &'a str,
    stuck: &'a str,
}

#[derive(Clone)]
pub(crate) struct FeedbackManager {
    layout: AppStateLayout,
    publisher: Arc<dyn FeedbackPublisher>,
    catalog_db: Option<Arc<CoralDb>>,
}

impl FeedbackManager {
    #[cfg(test)]
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self::with_publisher(layout, Arc::new(NoopFeedbackPublisher))
    }

    #[cfg(test)]
    pub(crate) fn with_publisher(
        layout: AppStateLayout,
        publisher: Arc<dyn FeedbackPublisher>,
    ) -> Self {
        Self {
            layout,
            publisher,
            catalog_db: None,
        }
    }

    pub(crate) fn with_db(
        layout: AppStateLayout,
        publisher: Arc<dyn FeedbackPublisher>,
        catalog_db: Arc<CoralDb>,
    ) -> Self {
        Self {
            layout,
            publisher,
            catalog_db: Some(catalog_db),
        }
    }

    pub(crate) fn submit_feedback(
        &self,
        workspace: &WorkspaceName,
        trying_to_do: &str,
        tried: &str,
        stuck: &str,
    ) -> Result<FeedbackSubmission, AppError> {
        let report = FeedbackReport {
            id: Uuid::new_v4().to_string(),
            workspace: workspace.clone(),
            created_at: Utc::now(),
            trying_to_do: required_text("trying_to_do", trying_to_do)?,
            tried: required_text("tried", tried)?,
            stuck: required_text("stuck", stuck)?,
        };
        self.append_report(&report)?;
        self.spawn_publish(report.clone());
        Ok(FeedbackSubmission { report })
    }

    fn spawn_publish(&self, report: FeedbackReport) {
        let publisher = Arc::clone(&self.publisher);
        let upload_task = tokio::spawn(async move {
            let _upload = publisher.publish(&report).await;
        });
        drop(upload_task);
    }

    fn append_report(&self, report: &FeedbackReport) -> Result<(), AppError> {
        if let Some(db) = self.catalog_db.clone() {
            let workspace = report.workspace.clone();
            let record = feedback_record(report)?;
            return run_feedback_db_operation(async move {
                let mut tx = db.begin().await?;
                if tx.workspaces().get(workspace.as_str()).await?.is_none() {
                    return Err(AppError::WorkspaceNotFound(workspace.to_string()));
                }
                tx.feedback_reports().append(&workspace, &record).await?;
                tx.commit().await?;
                Ok(())
            });
        }
        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        let file = self.layout.feedback_reports_file(&report.workspace);
        let persisted = PersistedFeedbackReport {
            id: &report.id,
            workspace: report.workspace.as_str(),
            created_at: report.created_at.to_rfc3339(),
            trying_to_do: &report.trying_to_do,
            tried: &report.tried,
            stuck: &report.stuck,
        };
        let mut line = serde_json::to_vec(&persisted)?;
        line.push(b'\n');
        storage_fs::append_file_private(&file, &line)?;
        Ok(())
    }
}

fn feedback_record(report: &FeedbackReport) -> Result<FeedbackReportRecord, AppError> {
    let created_at_unix_nanos = report.created_at.timestamp_nanos_opt().ok_or_else(|| {
        AppError::FailedPrecondition("feedback timestamp exceeds nanosecond range".to_string())
    })?;
    Ok(FeedbackReportRecord {
        id: report.id.clone(),
        created_at_unix_nanos,
        trying_to_do: report.trying_to_do.clone(),
        tried: report.tried.clone(),
        stuck: report.stuck.clone(),
        publish_status: None,
        publish_error: None,
        published_at_unix_nanos: None,
    })
}

fn run_feedback_db_operation<T, F>(operation: F) -> Result<T, AppError>
where
    T: Send + 'static,
    F: Future<Output = Result<T, AppError>> + Send + 'static,
{
    fn run_on_runtime<T, F>(operation: F) -> Result<T, AppError>
    where
        F: Future<Output = Result<T, AppError>>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                AppError::FailedPrecondition(format!(
                    "failed to create feedback database runtime: {error}"
                ))
            })?;
        runtime.block_on(operation)
    }

    if tokio::runtime::Handle::try_current().is_ok() {
        return std::thread::spawn(move || run_on_runtime(operation))
            .join()
            .map_err(|_panic| {
                AppError::FailedPrecondition(
                    "feedback database operation thread panicked".to_string(),
                )
            })?;
    }

    run_on_runtime(operation)
}

fn required_text(field: &str, value: &str) -> Result<String, AppError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(AppError::InvalidInput(format!(
            "missing string argument '{field}'"
        )));
    }
    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "JSONL shape assertions intentionally fail loudly in tests"
    )]

    use std::{fs, sync::Arc, time::Duration};

    use serde_json::Value;
    use tempfile::TempDir;

    use super::{FeedbackManager, FeedbackReport, FeedbackUpload};
    use crate::feedback::publisher::{FeedbackPublisher, NoopFeedbackPublisher};
    use crate::state::AppStateLayout;
    use crate::state::db::{CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig};
    use crate::workspaces::WorkspaceName;

    #[tokio::test]
    async fn submit_feedback_appends_workspace_jsonl_record() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral-config")))
            .expect("layout should resolve");
        let workspace = WorkspaceName::default();
        let manager = FeedbackManager::new(layout.clone());

        let submission = manager
            .submit_feedback(&workspace, " trying ", " tried ", " stuck ")
            .expect("feedback should submit");
        let report = submission.report;

        assert_eq!(report.workspace.as_str(), "default");
        assert_eq!(report.trying_to_do, "trying");
        let raw = fs::read_to_string(layout.feedback_reports_file(&workspace))
            .expect("feedback file should exist");
        let lines = raw.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 1);
        let value: Value = serde_json::from_str(lines[0]).expect("jsonl record should parse");
        assert_eq!(value["id"], report.id);
        assert_eq!(value["workspace"], "default");
        assert_eq!(value["trying_to_do"], "trying");
        assert_eq!(value["tried"], "tried");
        assert_eq!(value["stuck"], "stuck");
        assert!(
            value["created_at"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
    }

    #[tokio::test]
    async fn submit_feedback_rejects_blank_fields_before_persisting() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral-config")))
            .expect("layout should resolve");
        let workspace = WorkspaceName::default();
        let manager = FeedbackManager::new(layout.clone());

        let error = manager
            .submit_feedback(&workspace, "trying", " ", "stuck")
            .expect_err("blank feedback should fail");

        assert!(
            error
                .to_string()
                .contains("missing string argument 'tried'")
        );
        assert!(!layout.feedback_reports_file(&workspace).exists());
    }

    #[tokio::test]
    async fn submit_feedback_with_db_appends_database_record_without_jsonl() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral-config")))
            .expect("layout should resolve");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("default feedback test DB should be sqlite");
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");
        let workspace = WorkspaceName::default();
        let mut tx = db.begin().await.expect("begin workspace tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("ensure workspace");
        tx.commit().await.expect("commit workspace tx");
        let manager = FeedbackManager::with_db(
            layout.clone(),
            Arc::new(NoopFeedbackPublisher),
            Arc::clone(&db),
        );

        let submission = manager
            .submit_feedback(&workspace, " trying ", " tried ", " stuck ")
            .expect("feedback should submit");

        let mut session = db.as_ref();
        let reports = session
            .feedback_reports()
            .list_workspace_reports(&workspace)
            .await
            .expect("list DB feedback reports");
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].id, submission.report.id);
        assert_eq!(reports[0].trying_to_do, "trying");
        assert!(
            !layout.feedback_reports_file(&workspace).exists(),
            "DB-backed feedback writes should not append legacy JSONL"
        );
    }

    #[tokio::test]
    async fn submit_feedback_with_db_rejects_missing_workspace_without_creating_it() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral-config")))
            .expect("layout should resolve");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("default feedback test DB should be sqlite");
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");
        let workspace = WorkspaceName::parse("missing").expect("workspace");
        let manager = FeedbackManager::with_db(
            layout.clone(),
            Arc::new(NoopFeedbackPublisher),
            Arc::clone(&db),
        );

        let error = manager
            .submit_feedback(&workspace, "trying", "tried", "stuck")
            .expect_err("feedback should reject missing workspace");

        assert!(
            error.to_string().contains("workspace 'missing' not found"),
            "unexpected error: {error}"
        );
        let mut session = db.as_ref();
        assert!(
            session
                .workspaces()
                .get(workspace.as_str())
                .await
                .expect("get workspace")
                .is_none()
        );
        assert!(!layout.feedback_reports_file(&workspace).exists());
    }

    #[tokio::test]
    async fn submit_feedback_does_not_wait_for_hosted_publish() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral-config")))
            .expect("layout should resolve");
        let workspace = WorkspaceName::default();
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let manager = FeedbackManager::with_publisher(
            layout.clone(),
            Arc::new(PendingFeedbackPublisher {
                started: started_tx,
            }),
        );

        let submission = manager
            .submit_feedback(&workspace, "trying", "tried", "stuck")
            .expect("feedback should submit");

        assert!(!submission.report.id.is_empty());
        tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
            .await
            .expect("hosted publish task should start")
            .expect("hosted publish task should signal start");
        assert!(layout.feedback_reports_file(&workspace).exists());
    }

    struct PendingFeedbackPublisher {
        started: tokio::sync::mpsc::UnboundedSender<()>,
    }

    impl FeedbackPublisher for PendingFeedbackPublisher {
        fn publish<'a>(
            &'a self,
            _report: &'a FeedbackReport,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<FeedbackUpload>> + Send + 'a>>
        {
            let started = self.started.clone();
            Box::pin(async move {
                if started.send(()).is_err() {
                    return None;
                }
                std::future::pending().await
            })
        }
    }
}
