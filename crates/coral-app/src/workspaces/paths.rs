use std::path::PathBuf;

use crate::workspaces::WorkspaceName;

/// Path capability for workspace-owned filesystem artifacts.
pub(crate) trait WorkspacePaths: Send + Sync + 'static {
    fn workspace_dir(&self, workspace_name: &WorkspaceName) -> PathBuf;
}
