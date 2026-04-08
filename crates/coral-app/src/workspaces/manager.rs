//! Workspace identity and validation for app-owned services.

use coral_api::v1::Workspace;
use tonic::Status;

use crate::bootstrap::AppError;

/// Canonical default workspace name used across local Coral surfaces.
pub const DEFAULT_WORKSPACE_ID: &str = "default";

#[derive(Debug, Clone)]
pub(crate) struct WorkspaceManager {
    default_workspace: Workspace,
    forbidden_path_chars: [char; 2],
}

impl WorkspaceManager {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "used by internal tests and bootstrap helpers")
    )]
    #[must_use]
    pub(crate) fn default_workspace(&self) -> Workspace {
        self.default_workspace.clone()
    }

    pub(crate) fn require_app(&self, workspace: Option<&Workspace>) -> Result<Workspace, AppError> {
        let workspace =
            workspace.ok_or_else(|| AppError::InvalidInput("missing workspace".to_string()))?;
        self.normalize(workspace)
    }

    pub(crate) fn require(&self, workspace: Option<&Workspace>) -> Result<Workspace, Status> {
        self.require_app(workspace).map_err(app_error_to_status)
    }

    pub(crate) fn normalize(&self, workspace: &Workspace) -> Result<Workspace, AppError> {
        Ok(Workspace {
            name: self.validate_path_name("workspace name", &workspace.name)?,
        })
    }

    pub(crate) fn validate_name(&self, label: &str, value: &str) -> Result<String, AppError> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(AppError::InvalidInput(format!("missing {label}")));
        }
        if self
            .forbidden_path_chars
            .iter()
            .copied()
            .any(|ch| trimmed.contains(ch))
        {
            return Err(AppError::InvalidInput(format!(
                "{label} must not contain '/' or '\\\\'"
            )));
        }
        Ok(trimmed.to_string())
    }

    pub(crate) fn validate_path_name(&self, label: &str, value: &str) -> Result<String, AppError> {
        let trimmed = self.validate_name(label, value)?;
        if trimmed == "." || trimmed == ".." {
            return Err(AppError::InvalidInput(format!(
                "{label} must not be '.' or '..'"
            )));
        }
        Ok(trimmed)
    }

    pub(crate) fn status_validate_path_name(
        &self,
        label: &str,
        value: &str,
    ) -> Result<String, Status> {
        self.validate_path_name(label, value)
            .map_err(app_error_to_status)
    }
}

impl Default for WorkspaceManager {
    fn default() -> Self {
        Self {
            default_workspace: Workspace {
                name: DEFAULT_WORKSPACE_ID.to_string(),
            },
            forbidden_path_chars: ['/', '\\'],
        }
    }
}

fn app_error_to_status(error: AppError) -> Status {
    match error {
        AppError::InvalidInput(detail) => Status::invalid_argument(detail),
        other => Status::internal(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_WORKSPACE_ID, WorkspaceManager};

    #[test]
    fn rejects_forward_and_backward_slashes() {
        let manager = WorkspaceManager::new();
        let workspace = manager.default_workspace();
        let error = manager
            .normalize(&coral_api::v1::Workspace {
                name: r"bad\workspace".to_string(),
            })
            .expect_err("workspace should fail");
        assert!(error.to_string().contains("'/' or '\\\\'"));

        let error = manager
            .validate_path_name("source name", "bad/source")
            .expect_err("name should fail");
        assert!(error.to_string().contains("'/' or '\\\\'"));

        assert_eq!(workspace.name, DEFAULT_WORKSPACE_ID);
    }

    #[test]
    fn rejects_path_traversal() {
        let manager = WorkspaceManager::new();

        let error = manager
            .validate_path_name("workspace name", "..")
            .expect_err("'..' should be rejected");
        assert!(error.to_string().contains("'.' or '..'"));

        let error = manager
            .validate_path_name("source name", ".")
            .expect_err("'.' should be rejected");
        assert!(error.to_string().contains("'.' or '..'"));

        // Padded with whitespace should also be rejected after trimming
        let error = manager
            .validate_path_name("source name", " .. ")
            .expect_err("' .. ' should be rejected");
        assert!(error.to_string().contains("'.' or '..'"));
    }

    #[test]
    fn allows_dot_only_logical_binding_keys() {
        let manager = WorkspaceManager::new();

        assert_eq!(
            manager
                .validate_name("source variable key", "..")
                .expect("logical binding key named '..' should remain valid"),
            ".."
        );
    }
}
