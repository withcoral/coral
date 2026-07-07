use std::sync::{Arc, Condvar, Mutex};

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
    inner: Arc<WorkspaceLifecycleLockInner>,
}

impl WorkspaceLifecycleLock {
    #[must_use = "bind the returned guard for the full critical section"]
    pub(crate) fn lock(&self) -> WorkspaceLifecycleGuard {
        let mut locked = self
            .inner
            .locked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while *locked {
            locked = self
                .inner
                .available
                .wait(locked)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        *locked = true;
        WorkspaceLifecycleGuard {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[derive(Debug, Default)]
struct WorkspaceLifecycleLockInner {
    locked: Mutex<bool>,
    available: Condvar,
}

#[must_use]
pub(crate) struct WorkspaceLifecycleGuard {
    inner: Arc<WorkspaceLifecycleLockInner>,
}

impl Drop for WorkspaceLifecycleGuard {
    fn drop(&mut self) {
        let mut locked = self
            .inner
            .locked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *locked = false;
        self.inner.available.notify_one();
    }
}
