use std::collections::BTreeSet;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

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
    deleting_workspaces: BTreeSet<WorkspaceName>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct WorkspaceLifecycleLock {
    inner: Arc<RwLock<WorkspaceLifecycleState>>,
}

impl WorkspaceLifecycleLock {
    /// Serializes a workspace lifecycle write and advances the revision when
    /// the write guard is released.
    #[must_use = "bind the returned guard for the full critical section"]
    pub(crate) fn lock(&self) -> WorkspaceLifecycleGuard<'_> {
        WorkspaceLifecycleGuard {
            guard: self.write_inner(),
        }
    }

    #[must_use = "bind the returned snapshot while loading workspace state"]
    pub(crate) fn snapshot(&self) -> WorkspaceLifecycleSnapshot<'_> {
        WorkspaceLifecycleSnapshot {
            guard: self.read_inner(),
        }
    }

    pub(crate) fn snapshot_if_unchanged(
        &self,
        revision: WorkspaceLifecycleRevision,
        workspace_name: &WorkspaceName,
    ) -> Option<WorkspaceLifecycleSnapshot<'_>> {
        let guard = self.read_inner();
        (guard.revision == revision.0 && !guard.deleting_workspaces.contains(workspace_name))
            .then_some(WorkspaceLifecycleSnapshot { guard })
    }

    pub(crate) fn revision_if_active(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Option<WorkspaceLifecycleRevision> {
        let snapshot = self.snapshot();
        (!snapshot.workspace_is_deleting(workspace_name)).then(|| snapshot.revision())
    }

    pub(crate) fn mark_workspace_deleting(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Option<WorkspaceDeletionMarker> {
        let mut guard = self.write_inner();
        if !guard.deleting_workspaces.insert(workspace_name.clone()) {
            return None;
        }
        guard.revision = guard.revision.wrapping_add(1);
        Some(WorkspaceDeletionMarker {
            lifecycle: self.clone(),
            workspace_name: workspace_name.clone(),
        })
    }

    pub(crate) fn lock_if_unchanged(
        &self,
        revision: WorkspaceLifecycleRevision,
    ) -> Option<WorkspaceLifecycleGuard<'_>> {
        let guard = self.write_inner();
        if guard.revision == revision.0 {
            Some(WorkspaceLifecycleGuard { guard })
        } else {
            None
        }
    }

    fn read_inner(&self) -> RwLockReadGuard<'_, WorkspaceLifecycleState> {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_inner(&self) -> RwLockWriteGuard<'_, WorkspaceLifecycleState> {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn unmark_workspace_deleting(&self, workspace_name: &WorkspaceName) {
        let mut guard = self.write_inner();
        if guard.deleting_workspaces.remove(workspace_name) {
            guard.revision = guard.revision.wrapping_add(1);
        }
    }
}

#[must_use]
pub(crate) struct WorkspaceLifecycleGuard<'a> {
    guard: RwLockWriteGuard<'a, WorkspaceLifecycleState>,
}

impl Drop for WorkspaceLifecycleGuard<'_> {
    fn drop(&mut self) {
        self.guard.revision = self.guard.revision.wrapping_add(1);
    }
}

#[must_use]
pub(crate) struct WorkspaceLifecycleSnapshot<'a> {
    guard: RwLockReadGuard<'a, WorkspaceLifecycleState>,
}

impl WorkspaceLifecycleSnapshot<'_> {
    pub(crate) fn revision(&self) -> WorkspaceLifecycleRevision {
        WorkspaceLifecycleRevision(self.guard.revision)
    }

    pub(crate) fn workspace_is_deleting(&self, workspace_name: &WorkspaceName) -> bool {
        self.guard.deleting_workspaces.contains(workspace_name)
    }
}

#[must_use]
pub(crate) struct WorkspaceDeletionMarker {
    lifecycle: WorkspaceLifecycleLock,
    workspace_name: WorkspaceName,
}

impl Drop for WorkspaceDeletionMarker {
    fn drop(&mut self) {
        self.lifecycle
            .unmark_workspace_deleting(&self.workspace_name);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkspaceLifecycleRevision(u64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deletion_marker_marks_workspace_and_advances_revision_at_each_transition() {
        let lifecycle = WorkspaceLifecycleLock::default();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let initial_revision = lifecycle
            .revision_if_active(&workspace)
            .expect("workspace starts active");

        let marker = lifecycle
            .mark_workspace_deleting(&workspace)
            .expect("mark workspace deleting");

        let deleting_snapshot = lifecycle.snapshot();
        assert_eq!(
            deleting_snapshot.revision().0,
            initial_revision.0.wrapping_add(1)
        );
        assert!(deleting_snapshot.workspace_is_deleting(&workspace));
        let deleting_revision = deleting_snapshot.revision();
        drop(deleting_snapshot);
        assert_eq!(lifecycle.revision_if_active(&workspace), None);
        assert!(
            lifecycle
                .snapshot_if_unchanged(deleting_revision, &workspace)
                .is_none()
        );
        assert!(lifecycle.mark_workspace_deleting(&workspace).is_none());

        drop(marker);

        let active_snapshot = lifecycle.snapshot();
        assert_eq!(
            active_snapshot.revision().0,
            initial_revision.0.wrapping_add(2)
        );
        assert!(!active_snapshot.workspace_is_deleting(&workspace));
    }

    #[test]
    fn stale_revision_cannot_acquire_a_snapshot_or_write_guard() {
        let lifecycle = WorkspaceLifecycleLock::default();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let stale_revision = lifecycle.snapshot().revision();

        drop(lifecycle.lock());

        assert!(
            lifecycle
                .snapshot_if_unchanged(stale_revision, &workspace)
                .is_none()
        );
        assert!(lifecycle.lock_if_unchanged(stale_revision).is_none());

        let current_revision = lifecycle.snapshot().revision();
        assert!(
            lifecycle
                .snapshot_if_unchanged(current_revision, &workspace)
                .is_some()
        );
        assert!(lifecycle.lock_if_unchanged(current_revision).is_some());
    }

    #[test]
    fn read_snapshot_excludes_lifecycle_writes_until_it_is_released() {
        let lifecycle = WorkspaceLifecycleLock::default();
        let snapshot = lifecycle.snapshot();
        let initial_revision = snapshot.revision();
        let Err(_write_error) = lifecycle.inner.try_write() else {
            panic!("read snapshot should exclude lifecycle writes");
        };

        drop(snapshot);
        drop(lifecycle.lock());

        assert_eq!(
            lifecycle.snapshot().revision().0,
            initial_revision.0.wrapping_add(1)
        );
    }
}
