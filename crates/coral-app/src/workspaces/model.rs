use std::sync::{Arc, Mutex, MutexGuard};

use crate::sources::model::InstalledSource;
use crate::workspaces::WorkspaceName;

/// App-owned workspace metadata record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceRecord {
    pub(crate) name: WorkspaceName,
}

/// Workspace metadata and scoped source state captured at the config deletion
/// commit point for post-commit artifact cleanup.
#[derive(Debug, Clone)]
pub(crate) struct DeletedWorkspace {
    pub(crate) workspace: WorkspaceRecord,
    pub(crate) sources: Vec<InstalledSource>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct WorkspaceLifecycleLock {
    inner: Arc<Mutex<()>>,
}

impl WorkspaceLifecycleLock {
    #[must_use = "bind the returned guard for the full critical section"]
    pub(crate) fn lock(&self) -> WorkspaceLifecycleGuard<'_> {
        WorkspaceLifecycleGuard {
            _guard: self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        }
    }
}

#[must_use]
pub(crate) struct WorkspaceLifecycleGuard<'a> {
    _guard: MutexGuard<'a, ()>,
}
