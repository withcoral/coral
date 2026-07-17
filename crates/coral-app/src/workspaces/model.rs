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

#[derive(Debug, Default)]
struct WorkspaceLifecycleState {
    revision: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct WorkspaceLifecycleLock {
    inner: Arc<Mutex<WorkspaceLifecycleState>>,
}

impl WorkspaceLifecycleLock {
    /// Serializes a workspace lifecycle write and advances the revision when
    /// the write guard is released.
    #[must_use = "bind the returned guard for the full critical section"]
    pub(crate) fn lock(&self) -> WorkspaceLifecycleGuard<'_> {
        WorkspaceLifecycleGuard {
            guard: self.lock_inner(),
        }
    }

    #[must_use = "bind the returned snapshot while loading workspace state"]
    pub(crate) fn snapshot(&self) -> WorkspaceLifecycleSnapshot<'_> {
        WorkspaceLifecycleSnapshot {
            guard: self.lock_inner(),
        }
    }

    pub(crate) fn lock_if_unchanged(
        &self,
        revision: WorkspaceLifecycleRevision,
    ) -> Option<WorkspaceLifecycleGuard<'_>> {
        let guard = self.lock_inner();
        if guard.revision == revision.0 {
            Some(WorkspaceLifecycleGuard { guard })
        } else {
            None
        }
    }

    fn lock_inner(&self) -> MutexGuard<'_, WorkspaceLifecycleState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[must_use]
pub(crate) struct WorkspaceLifecycleGuard<'a> {
    guard: MutexGuard<'a, WorkspaceLifecycleState>,
}

impl Drop for WorkspaceLifecycleGuard<'_> {
    fn drop(&mut self) {
        self.guard.revision = self.guard.revision.wrapping_add(1);
    }
}

#[must_use]
pub(crate) struct WorkspaceLifecycleSnapshot<'a> {
    guard: MutexGuard<'a, WorkspaceLifecycleState>,
}

impl WorkspaceLifecycleSnapshot<'_> {
    pub(crate) fn revision(&self) -> WorkspaceLifecycleRevision {
        WorkspaceLifecycleRevision(self.guard.revision)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkspaceLifecycleRevision(u64);
