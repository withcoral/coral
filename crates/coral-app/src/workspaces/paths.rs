use std::path::PathBuf;

use crate::workspaces::WorkspaceName;

/// Path capability for workspace-owned filesystem artifacts.
pub(crate) trait WorkspacePaths: Send + Sync + 'static {
    fn workspace_dir(&self, workspace_name: &WorkspaceName) -> PathBuf;

    /// Where a deletion stages a workspace directory, outside the root the
    /// live ones live in.
    fn deleted_workspaces_root(&self) -> PathBuf;
}
