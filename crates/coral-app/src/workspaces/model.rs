use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use async_lock::{RwLock, RwLockReadGuardArc, RwLockWriteGuardArc};
#[cfg(test)]
use async_lock::{RwLockReadGuard, RwLockWriteGuard};

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
    inner: Arc<RwLock<WorkspaceLifecycleState>>,
    deleting_workspaces: Arc<Mutex<BTreeSet<WorkspaceName>>>,
}

impl WorkspaceLifecycleLock {
    #[cfg(test)]
    /// Serializes a workspace lifecycle write and advances the revision when
    /// the write guard is released.
    #[must_use = "bind the returned guard for the full critical section"]
    pub(crate) fn lock(&self) -> WorkspaceLifecycleGuard<'_> {
        WorkspaceLifecycleGuard {
            guard: self.write_inner(),
        }
    }

    /// Serializes an asynchronous workspace lifecycle write without blocking
    /// a Tokio worker while a search lease is active.
    #[must_use = "bind the returned guard for the full critical section"]
    pub(crate) async fn lock_async(&self) -> WorkspaceLifecycleOwnedGuard {
        WorkspaceLifecycleOwnedGuard {
            guard: self.inner.write_arc().await,
        }
    }

    #[cfg(test)]
    #[must_use = "bind the returned snapshot while loading workspace state"]
    pub(crate) fn snapshot(&self) -> WorkspaceLifecycleSnapshot<'_> {
        WorkspaceLifecycleSnapshot {
            guard: self.read_inner(),
            deleting_workspaces: Arc::clone(&self.deleting_workspaces),
        }
    }

    #[must_use = "bind the returned snapshot while loading workspace state"]
    pub(crate) async fn snapshot_async(&self) -> WorkspaceLifecycleReadLease {
        WorkspaceLifecycleReadLease {
            guard: self.inner.read_arc().await,
            deleting_workspaces: Arc::clone(&self.deleting_workspaces),
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot_if_unchanged(
        &self,
        revision: WorkspaceLifecycleRevision,
        workspace_name: &WorkspaceName,
    ) -> Option<WorkspaceLifecycleSnapshot<'_>> {
        if self.workspace_is_deleting(workspace_name) {
            return None;
        }
        let guard = self.read_inner();
        (guard.revision == revision.0 && !self.workspace_is_deleting(workspace_name)).then_some(
            WorkspaceLifecycleSnapshot {
                guard,
                deleting_workspaces: Arc::clone(&self.deleting_workspaces),
            },
        )
    }

    /// Acquires an owned read lease that can follow detached asynchronous work.
    ///
    /// Unlike [`Self::snapshot_if_unchanged`], the returned lease is
    /// `Send + 'static`, so provider tasks can retain it after the request
    /// future that started them is cancelled.
    pub(crate) async fn read_lease_if_unchanged(
        &self,
        revision: WorkspaceLifecycleRevision,
        workspace_name: &WorkspaceName,
    ) -> Option<WorkspaceLifecycleReadLease> {
        if self.workspace_is_deleting(workspace_name) {
            return None;
        }
        let guard = self.inner.read_arc().await;
        (guard.revision == revision.0 && !self.workspace_is_deleting(workspace_name)).then_some(
            WorkspaceLifecycleReadLease {
                guard,
                deleting_workspaces: Arc::clone(&self.deleting_workspaces),
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn revision_if_active(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Option<WorkspaceLifecycleRevision> {
        let snapshot = self.snapshot();
        (!snapshot.workspace_is_deleting(workspace_name)).then(|| snapshot.revision())
    }

    pub(crate) async fn revision_if_active_async(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Option<WorkspaceLifecycleRevision> {
        let snapshot = self.snapshot_async().await;
        (!snapshot.workspace_is_deleting(workspace_name)).then(|| snapshot.revision())
    }

    /// Marks a workspace as deleting and drains outstanding lifecycle readers.
    ///
    /// The returned marker owns the global lifecycle write lease for the full
    /// deletion. Workspace deletion spans shared database, configuration, and
    /// credential state, so lifecycle work in every workspace stays excluded
    /// until the marker is dropped.
    pub(crate) async fn mark_workspace_deleting(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Option<WorkspaceDeletionMarker> {
        {
            let mut deleting_workspaces = self.deleting_workspaces();
            if !deleting_workspaces.insert(workspace_name.clone()) {
                return None;
            }
        }

        // Register deletion before waiting for outstanding readers so new
        // search leases fail closed. The marker removes the registration if
        // this future is cancelled while waiting for the write guard.
        let mut marker = WorkspaceDeletionMarker {
            deleting_workspaces: Arc::clone(&self.deleting_workspaces),
            workspace_name: workspace_name.clone(),
            guard: None,
        };
        let mut guard = self.inner.write_arc().await;
        guard.revision = guard.revision.wrapping_add(1);
        marker.guard = Some(guard);
        Some(marker)
    }

    #[cfg(test)]
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

    pub(crate) async fn lock_if_unchanged_async(
        &self,
        revision: WorkspaceLifecycleRevision,
    ) -> Option<WorkspaceLifecycleOwnedGuard> {
        let guard = self.inner.write_arc().await;
        if guard.revision == revision.0 {
            Some(WorkspaceLifecycleOwnedGuard { guard })
        } else {
            None
        }
    }

    #[cfg(test)]
    fn read_inner(&self) -> RwLockReadGuard<'_, WorkspaceLifecycleState> {
        self.inner.read_blocking()
    }

    #[cfg(test)]
    fn write_inner(&self) -> RwLockWriteGuard<'_, WorkspaceLifecycleState> {
        self.inner.write_blocking()
    }

    fn workspace_is_deleting(&self, workspace_name: &WorkspaceName) -> bool {
        self.deleting_workspaces().contains(workspace_name)
    }

    fn deleting_workspaces(&self) -> std::sync::MutexGuard<'_, BTreeSet<WorkspaceName>> {
        self.deleting_workspaces
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
#[must_use]
pub(crate) struct WorkspaceLifecycleGuard<'a> {
    guard: RwLockWriteGuard<'a, WorkspaceLifecycleState>,
}

#[cfg(test)]
impl Drop for WorkspaceLifecycleGuard<'_> {
    fn drop(&mut self) {
        self.guard.revision = self.guard.revision.wrapping_add(1);
    }
}

#[must_use]
pub(crate) struct WorkspaceLifecycleOwnedGuard {
    guard: RwLockWriteGuardArc<WorkspaceLifecycleState>,
}

impl Drop for WorkspaceLifecycleOwnedGuard {
    fn drop(&mut self) {
        self.guard.revision = self.guard.revision.wrapping_add(1);
    }
}

#[cfg(test)]
#[must_use]
pub(crate) struct WorkspaceLifecycleSnapshot<'a> {
    guard: RwLockReadGuard<'a, WorkspaceLifecycleState>,
    deleting_workspaces: Arc<Mutex<BTreeSet<WorkspaceName>>>,
}

#[cfg(test)]
impl WorkspaceLifecycleSnapshot<'_> {
    pub(crate) fn revision(&self) -> WorkspaceLifecycleRevision {
        WorkspaceLifecycleRevision(self.guard.revision)
    }

    pub(crate) fn workspace_is_deleting(&self, workspace_name: &WorkspaceName) -> bool {
        self.deleting_workspaces
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(workspace_name)
    }
}

/// Owned snapshot of workspace lifecycle state.
///
/// Validation uses the captured revision, while search providers retain the
/// read lease so detached blocking work cannot overlap lifecycle mutations.
#[must_use]
pub(crate) struct WorkspaceLifecycleReadLease {
    guard: RwLockReadGuardArc<WorkspaceLifecycleState>,
    deleting_workspaces: Arc<Mutex<BTreeSet<WorkspaceName>>>,
}

impl WorkspaceLifecycleReadLease {
    pub(crate) fn revision(&self) -> WorkspaceLifecycleRevision {
        WorkspaceLifecycleRevision(self.guard.revision)
    }

    pub(crate) fn workspace_is_deleting(&self, workspace_name: &WorkspaceName) -> bool {
        self.deleting_workspaces
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(workspace_name)
    }
}

#[must_use]
pub(crate) struct WorkspaceDeletionMarker {
    deleting_workspaces: Arc<Mutex<BTreeSet<WorkspaceName>>>,
    workspace_name: WorkspaceName,
    guard: Option<RwLockWriteGuardArc<WorkspaceLifecycleState>>,
}

impl Drop for WorkspaceDeletionMarker {
    fn drop(&mut self) {
        let mut deleting_workspaces = self
            .deleting_workspaces
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if deleting_workspaces.remove(&self.workspace_name)
            && let Some(guard) = &mut self.guard
        {
            guard.revision = guard.revision.wrapping_add(1);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkspaceLifecycleRevision(u64);

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn deletion_marker_advances_revision_at_each_transition() {
        let lifecycle = WorkspaceLifecycleLock::default();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let initial_revision = lifecycle
            .revision_if_active(&workspace)
            .expect("workspace starts active");

        let marker = lifecycle
            .mark_workspace_deleting(&workspace)
            .await
            .expect("mark workspace deleting");

        assert!(lifecycle.workspace_is_deleting(&workspace));
        assert!(
            lifecycle.inner.try_read_arc().is_none(),
            "workspace deletion intentionally excludes all lifecycle readers"
        );
        assert!(
            lifecycle
                .mark_workspace_deleting(&workspace)
                .await
                .is_none()
        );
        assert!(
            lifecycle
                .read_lease_if_unchanged(initial_revision, &workspace)
                .await
                .is_none()
        );

        drop(marker);

        let active_snapshot = lifecycle.snapshot();
        assert_eq!(
            active_snapshot.revision().0,
            initial_revision.0.wrapping_add(2)
        );
        assert!(!active_snapshot.workspace_is_deleting(&workspace));
    }

    #[tokio::test]
    async fn cancelling_pending_deletion_clears_marker() {
        let lifecycle = WorkspaceLifecycleLock::default();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let initial_revision = lifecycle
            .revision_if_active_async(&workspace)
            .await
            .expect("workspace starts active");
        let read_lease = lifecycle.snapshot_async().await;
        let mut pending_deletion = Box::pin(lifecycle.mark_workspace_deleting(&workspace));

        tokio::select! {
            biased;
            _ = &mut pending_deletion => panic!("deletion should wait for the read lease"),
            () = tokio::task::yield_now() => {}
        }
        assert!(lifecycle.workspace_is_deleting(&workspace));

        drop(pending_deletion);
        assert!(!lifecycle.workspace_is_deleting(&workspace));
        drop(read_lease);
        assert_eq!(
            lifecycle.revision_if_active_async(&workspace).await,
            Some(initial_revision)
        );
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
        let None = lifecycle.inner.try_write() else {
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
