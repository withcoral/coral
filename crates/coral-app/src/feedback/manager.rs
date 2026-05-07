use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::state::AppStateLayout;
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

#[derive(Debug, Serialize)]
struct PersistedFeedbackReport<'a> {
    id: &'a str,
    workspace: &'a str,
    created_at: String,
    trying_to_do: &'a str,
    tried: &'a str,
    stuck: &'a str,
}

#[derive(Debug, Clone)]
pub(crate) struct FeedbackManager {
    layout: AppStateLayout,
}

impl FeedbackManager {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    pub(crate) fn submit_feedback(
        &self,
        workspace: &WorkspaceName,
        trying_to_do: &str,
        tried: &str,
        stuck: &str,
    ) -> Result<FeedbackReport, AppError> {
        let report = FeedbackReport {
            id: Uuid::new_v4().to_string(),
            workspace: workspace.clone(),
            created_at: Utc::now(),
            trying_to_do: required_text("trying_to_do", trying_to_do)?,
            tried: required_text("tried", tried)?,
            stuck: required_text("stuck", stuck)?,
        };
        self.append_report(&report)?;
        Ok(report)
    }

    fn append_report(&self, report: &FeedbackReport) -> Result<(), AppError> {
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
    use std::fs;

    use serde_json::Value;
    use tempfile::TempDir;

    use super::FeedbackManager;
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;

    #[test]
    fn submit_feedback_appends_workspace_jsonl_record() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral-config")))
            .expect("layout should resolve");
        let workspace = WorkspaceName::default();
        let manager = FeedbackManager::new(layout.clone());

        let report = manager
            .submit_feedback(&workspace, " trying ", " tried ", " stuck ")
            .expect("feedback should submit");

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

    #[test]
    fn submit_feedback_rejects_blank_fields_before_persisting() {
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
}
