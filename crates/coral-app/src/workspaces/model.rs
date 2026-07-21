use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

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
    revisions: BTreeMap<WorkspaceName, u64>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct WorkspaceLifecycleLock {
    inner: Arc<RwLock<WorkspaceLifecycleState>>,
}

impl WorkspaceLifecycleLock {
    /// Serializes a workspace lifecycle write and advances the revision when
    /// the write guard is released.
    #[must_use = "bind the returned guard for the full critical section"]
    pub(crate) async fn lock(&self, workspace_name: &WorkspaceName) -> WorkspaceLifecycleGuard {
        WorkspaceLifecycleGuard {
            guard: Arc::clone(&self.inner).write_owned().await,
            workspace_name: workspace_name.clone(),
        }
    }

    #[must_use = "bind the returned snapshot while loading workspace state"]
    pub(crate) async fn snapshot(
        &self,
        workspace_name: &WorkspaceName,
    ) -> WorkspaceLifecycleSnapshot {
        WorkspaceLifecycleSnapshot {
            guard: Arc::clone(&self.inner).read_owned().await,
            workspace_name: workspace_name.clone(),
        }
    }

    pub(crate) async fn snapshot_if_unchanged(
        &self,
        revision: WorkspaceLifecycleRevision,
        workspace_name: &WorkspaceName,
    ) -> Option<WorkspaceLifecycleSnapshot> {
        let guard = Arc::clone(&self.inner).read_owned().await;
        (workspace_revision(&guard, workspace_name) == revision.0).then(|| {
            WorkspaceLifecycleSnapshot {
                guard,
                workspace_name: workspace_name.clone(),
            }
        })
    }

    pub(crate) async fn revision_if_active(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Option<WorkspaceLifecycleRevision> {
        Some(self.snapshot(workspace_name).await.revision())
    }

    pub(crate) async fn lock_if_unchanged(
        &self,
        workspace_name: &WorkspaceName,
        revision: WorkspaceLifecycleRevision,
    ) -> Option<WorkspaceLifecycleGuard> {
        let guard = Arc::clone(&self.inner).write_owned().await;
        if workspace_revision(&guard, workspace_name) == revision.0 {
            Some(WorkspaceLifecycleGuard {
                guard,
                workspace_name: workspace_name.clone(),
            })
        } else {
            None
        }
    }
}

#[must_use]
pub(crate) struct WorkspaceLifecycleGuard {
    guard: OwnedRwLockWriteGuard<WorkspaceLifecycleState>,
    workspace_name: WorkspaceName,
}

impl Drop for WorkspaceLifecycleGuard {
    fn drop(&mut self) {
        let revision = self
            .guard
            .revisions
            .entry(self.workspace_name.clone())
            .or_default();
        *revision = revision.wrapping_add(1);
    }
}

#[must_use]
pub(crate) struct WorkspaceLifecycleSnapshot {
    guard: OwnedRwLockReadGuard<WorkspaceLifecycleState>,
    workspace_name: WorkspaceName,
}

impl WorkspaceLifecycleSnapshot {
    pub(crate) fn revision(&self) -> WorkspaceLifecycleRevision {
        WorkspaceLifecycleRevision(workspace_revision(&self.guard, &self.workspace_name))
    }
}

fn workspace_revision(state: &WorkspaceLifecycleState, workspace_name: &WorkspaceName) -> u64 {
    state
        .revisions
        .get(workspace_name)
        .copied()
        .unwrap_or_default()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkspaceLifecycleRevision(u64);

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn write_guard_advances_revision_when_released() {
        let lifecycle = WorkspaceLifecycleLock::default();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let initial_revision = lifecycle
            .revision_if_active(&workspace)
            .await
            .expect("workspace starts active");

        drop(lifecycle.lock(&workspace).await);

        assert_eq!(
            lifecycle.snapshot(&workspace).await.revision().0,
            initial_revision.0.wrapping_add(1)
        );
    }

    #[tokio::test]
    async fn stale_revision_cannot_acquire_a_snapshot_or_write_guard() {
        let lifecycle = WorkspaceLifecycleLock::default();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let stale_revision = lifecycle.snapshot(&workspace).await.revision();

        drop(lifecycle.lock(&workspace).await);

        assert!(
            lifecycle
                .snapshot_if_unchanged(stale_revision, &workspace)
                .await
                .is_none()
        );
        assert!(
            lifecycle
                .lock_if_unchanged(&workspace, stale_revision)
                .await
                .is_none()
        );

        let current_revision = lifecycle.snapshot(&workspace).await.revision();
        assert!(
            lifecycle
                .snapshot_if_unchanged(current_revision, &workspace)
                .await
                .is_some()
        );
        assert!(
            lifecycle
                .lock_if_unchanged(&workspace, current_revision)
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn unrelated_workspace_write_does_not_invalidate_revision() {
        let lifecycle = WorkspaceLifecycleLock::default();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let unrelated = WorkspaceName::parse("other").expect("workspace");
        let revision = lifecycle.snapshot(&workspace).await.revision();

        drop(lifecycle.lock(&unrelated).await);

        assert!(
            lifecycle
                .snapshot_if_unchanged(revision, &workspace)
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn read_snapshot_excludes_lifecycle_writes_until_it_is_released() {
        let lifecycle = WorkspaceLifecycleLock::default();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let snapshot = lifecycle.snapshot(&workspace).await;
        let initial_revision = snapshot.revision();
        let Err(_write_error) = Arc::clone(&lifecycle.inner).try_write_owned() else {
            panic!("read snapshot should exclude lifecycle writes");
        };

        drop(snapshot);
        drop(lifecycle.lock(&workspace).await);

        assert_eq!(
            lifecycle.snapshot(&workspace).await.revision().0,
            initial_revision.0.wrapping_add(1)
        );
    }
}
