use crate::bootstrap::AppError;
use crate::workspaces::{DeletedWorkspace, WorkspaceName, WorkspaceRecord};

/// Repository boundary for workspace metadata and workspace-owned source state.
pub(crate) trait WorkspaceStore: Send + Sync + 'static {
    fn create_workspace(&self, workspace_name: &WorkspaceName)
    -> Result<WorkspaceRecord, AppError>;

    fn delete_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Option<DeletedWorkspace>, AppError>;
}
