use std::sync::{Arc, Mutex, MutexGuard};

use crate::sources::model::InstalledSource;
use crate::workspaces::WorkspaceName;

/// App-owned workspace metadata record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceRecord {
    pub(crate) name: WorkspaceName,
}

/// Workspace metadata and source catalog state captured before deletion.
#[derive(Debug, Clone)]
pub(crate) struct DeletedWorkspaceRecord {
    pub(crate) workspace: WorkspaceRecord,
    pub(crate) sources: Vec<InstalledSource>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct WorkspaceLifecycleLock {
    inner: Arc<Mutex<()>>,
}

impl WorkspaceLifecycleLock {
    pub(crate) fn lock(&self) -> WorkspaceLifecycleGuard<'_> {
        WorkspaceLifecycleGuard {
            _guard: self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        }
    }
}

pub(crate) struct WorkspaceLifecycleGuard<'a> {
    _guard: MutexGuard<'a, ()>,
}
