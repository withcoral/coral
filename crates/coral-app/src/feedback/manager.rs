use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::feedback::publisher::FeedbackPublisher;
use crate::storage::app::{AppStore, StoredFeedbackReport};
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

#[derive(Clone)]
pub(crate) struct FeedbackManager {
    store: AppStore,
    publisher: Arc<dyn FeedbackPublisher>,
}

impl FeedbackManager {
    pub(crate) fn with_publisher(store: AppStore, publisher: Arc<dyn FeedbackPublisher>) -> Self {
        Self { store, publisher }
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
        let persisted = StoredFeedbackReport {
            id: report.id.clone(),
            workspace: report.workspace.as_str().to_string(),
            created_at_rfc3339: report.created_at.to_rfc3339(),
            trying_to_do: report.trying_to_do.clone(),
            tried: report.tried.clone(),
            stuck: report.stuck.clone(),
        };
        let mut uow = self.store.begin_write()?;
        {
            let mut feedback = uow.feedback();
            feedback.append_report(&persisted)?;
        }
        uow.commit()?;
        Ok(())
    }
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
    use std::{sync::Arc, time::Duration};

    use tempfile::TempDir;

    use super::{FeedbackManager, FeedbackReport, FeedbackUpload};
    use crate::feedback::publisher::{FeedbackPublisher, NoopFeedbackPublisher};
    use crate::state::AppStateLayout;
    use crate::storage::app::AppStore;
    use crate::workspaces::WorkspaceName;

    fn manager() -> (TempDir, AppStore, FeedbackManager) {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral-config")))
            .expect("layout should resolve");
        let store = AppStore::sqlite(layout.app_database_file()).expect("sqlite app store");
        let manager =
            FeedbackManager::with_publisher(store.clone(), Arc::new(NoopFeedbackPublisher));
        (temp, store, manager)
    }

    #[tokio::test]
    async fn submit_feedback_appends_workspace_record() {
        let (_temp, store, manager) = manager();
        let workspace = WorkspaceName::default();

        let submission = manager
            .submit_feedback(&workspace, " trying ", " tried ", " stuck ")
            .expect("feedback should submit");
        let report = submission.report;

        assert_eq!(report.workspace.as_str(), "default");
        assert_eq!(report.trying_to_do, "trying");
        let reports = store
            .test_read_feedback_reports(workspace.as_str())
            .expect("read feedback reports");
        assert_eq!(reports.len(), 1);
        let persisted = reports.first().expect("one report");
        assert_eq!(persisted.id, report.id);
        assert_eq!(persisted.workspace, "default");
        assert_eq!(persisted.trying_to_do, "trying");
        assert_eq!(persisted.tried, "tried");
        assert_eq!(persisted.stuck, "stuck");
        assert!(
            !persisted.created_at_rfc3339.is_empty(),
            "created_at should be persisted"
        );
    }

    #[tokio::test]
    async fn submit_feedback_rejects_blank_fields_before_persisting() {
        let (_temp, store, manager) = manager();
        let workspace = WorkspaceName::default();

        let error = manager
            .submit_feedback(&workspace, "trying", " ", "stuck")
            .expect_err("blank feedback should fail");

        assert!(
            error
                .to_string()
                .contains("missing string argument 'tried'")
        );
        assert!(
            store
                .test_read_feedback_reports(workspace.as_str())
                .expect("read feedback reports")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn submit_feedback_does_not_wait_for_hosted_publish() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral-config")))
            .expect("layout should resolve");
        let store = AppStore::sqlite(layout.app_database_file()).expect("sqlite app store");
        let workspace = WorkspaceName::default();
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let manager = FeedbackManager::with_publisher(
            store.clone(),
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
        assert_eq!(
            store
                .test_read_feedback_reports(workspace.as_str())
                .expect("read feedback reports")
                .len(),
            1
        );
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
