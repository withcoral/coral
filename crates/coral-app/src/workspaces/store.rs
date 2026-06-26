use crate::bootstrap::AppError;
use crate::sources::model::InstalledSource;
use crate::workspaces::{WorkspaceName, WorkspaceRecord};

/// Repository boundary for workspace metadata and workspace-owned source state.
pub(crate) trait WorkspaceStore: Send + Sync + 'static {
    fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, AppError>;

    fn create_workspace(&self, workspace_name: &WorkspaceName)
    -> Result<WorkspaceRecord, AppError>;

    fn list_workspace_sources(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledSource>, AppError>;

    fn delete_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Option<WorkspaceRecord>, AppError>;
}
