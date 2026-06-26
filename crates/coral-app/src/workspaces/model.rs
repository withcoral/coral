use crate::workspaces::WorkspaceName;

/// App-owned workspace metadata record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceRecord {
    pub(crate) name: WorkspaceName,
}
