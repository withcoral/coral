#![expect(dead_code, reason = "used in next stack PR")]

use std::sync::Arc;

use tracing::warn;

use crate::bootstrap::AppError;
use crate::credentials::{CredentialManager, CredentialMaterialSnapshot, CredentialSetId};
use crate::state::AppStateLayout;
use crate::storage::fs::DirectoryBackup;
use crate::workspaces::{DEFAULT_WORKSPACE_ID, WorkspaceName, WorkspaceRecord, WorkspaceStore};

/// App-owned workspace lifecycle behavior.
#[derive(Clone)]
pub(crate) struct WorkspaceManager {
    store: Arc<dyn WorkspaceStore>,
    credential_manager: CredentialManager,
    layout: AppStateLayout,
}

impl WorkspaceManager {
    pub(crate) fn new(
        store: impl WorkspaceStore,
        credential_manager: CredentialManager,
        layout: AppStateLayout,
    ) -> Self {
        Self {
            store: Arc::new(store),
            credential_manager,
            layout,
        }
    }

    pub(crate) fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, AppError> {
        self.store.list_workspaces()
    }

    pub(crate) fn create_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<WorkspaceRecord, AppError> {
        self.store.create_workspace(workspace_name)
    }

    pub(crate) fn delete_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<WorkspaceRecord, AppError> {
        if workspace_name.as_str() == DEFAULT_WORKSPACE_ID {
            return Err(AppError::FailedPrecondition(
                "default workspace cannot be removed".to_string(),
            ));
        }

        let credential_snapshots = self.remove_workspace_credentials(workspace_name)?;
        let workspace_dir = self.layout.workspace_dir(workspace_name);
        let backup = match DirectoryBackup::move_for_delete(&workspace_dir, workspace_name) {
            Ok(backup) => backup,
            Err(error) => {
                self.restore_workspace_credentials(workspace_name, &credential_snapshots);
                return Err(error.into());
            }
        };

        match self.store.delete_workspace(workspace_name) {
            Ok(Some(record)) => {
                backup.commit()?;
                Ok(record)
            }
            Ok(None) => {
                backup.commit()?;
                Err(AppError::WorkspaceNotFound(workspace_name.to_string()))
            }
            Err(error) => {
                let restore_dir_result = backup.restore();
                self.restore_workspace_credentials(workspace_name, &credential_snapshots);
                restore_dir_result?;
                Err(error)
            }
        }
    }

    fn remove_workspace_credentials(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<WorkspaceCredentialSnapshot>, AppError> {
        let mut snapshots = Vec::new();
        for source in self.store.list_workspace_sources(workspace_name)? {
            let Some(storage) = source.credential_storage_for_material() else {
                continue;
            };
            let credential_set_id = CredentialSetId::for_source(&source.name);
            let guard = match self
                .credential_manager
                .material_guard(workspace_name, &credential_set_id)
            {
                Ok(guard) => guard,
                Err(error) => {
                    return self.rollback_removed_workspace_credentials(
                        workspace_name,
                        &snapshots,
                        error,
                    );
                }
            };
            let snapshot = match guard.snapshot_material(storage) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    return self.rollback_removed_workspace_credentials(
                        workspace_name,
                        &snapshots,
                        error,
                    );
                }
            };
            if let Err(error) = guard.remove_material(storage) {
                return self.rollback_removed_workspace_credentials_with_snapshot(
                    workspace_name,
                    &mut snapshots,
                    WorkspaceCredentialSnapshot {
                        credential_set_id,
                        snapshot,
                    },
                    error,
                );
            }
            snapshots.push(WorkspaceCredentialSnapshot {
                credential_set_id,
                snapshot,
            });
        }
        Ok(snapshots)
    }

    fn rollback_removed_workspace_credentials<T>(
        &self,
        workspace_name: &WorkspaceName,
        snapshots: &[WorkspaceCredentialSnapshot],
        error: AppError,
    ) -> Result<T, AppError> {
        self.restore_workspace_credentials(workspace_name, snapshots);
        Err(error)
    }

    fn rollback_removed_workspace_credentials_with_snapshot<T>(
        &self,
        workspace_name: &WorkspaceName,
        snapshots: &mut Vec<WorkspaceCredentialSnapshot>,
        snapshot: WorkspaceCredentialSnapshot,
        error: AppError,
    ) -> Result<T, AppError> {
        snapshots.push(snapshot);
        self.rollback_removed_workspace_credentials(workspace_name, snapshots, error)
    }

    fn restore_workspace_credentials(
        &self,
        workspace_name: &WorkspaceName,
        snapshots: &[WorkspaceCredentialSnapshot],
    ) {
        for snapshot in snapshots.iter().rev() {
            let guard = match self
                .credential_manager
                .material_guard(workspace_name, &snapshot.credential_set_id)
            {
                Ok(guard) => guard,
                Err(error) => {
                    warn!(
                        credential_set_id = %snapshot.credential_set_id,
                        "rollback: failed to access workspace credential material: {error}"
                    );
                    continue;
                }
            };
            if let Err(error) = guard.restore_material(&snapshot.snapshot) {
                warn!(
                    credential_set_id = %snapshot.credential_set_id,
                    "rollback: failed to restore workspace credential material: {error}"
                );
            }
        }
    }
}

struct WorkspaceCredentialSnapshot {
    credential_set_id: CredentialSetId,
    snapshot: CredentialMaterialSnapshot,
}
