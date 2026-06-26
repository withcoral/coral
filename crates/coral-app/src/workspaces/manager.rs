#![expect(dead_code, reason = "used in next stack PR")]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tracing::warn;
use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::credentials::{CredentialManager, CredentialMaterialSnapshot, CredentialSetId};
use crate::state::AppStateLayout;
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
        let backup = workspace_delete_backup_path(&workspace_dir, workspace_name);
        let had_workspace_dir = workspace_dir.exists();
        if had_workspace_dir {
            if backup.exists()
                && let Err(error) = std::fs::remove_dir_all(&backup)
            {
                self.restore_workspace_credentials(workspace_name, &credential_snapshots);
                return Err(error.into());
            }
            if let Err(error) = std::fs::rename(&workspace_dir, &backup) {
                self.restore_workspace_credentials(workspace_name, &credential_snapshots);
                return Err(error.into());
            }
        }

        match self.store.delete_workspace(workspace_name) {
            Ok(Some(record)) => {
                if backup.exists() {
                    std::fs::remove_dir_all(&backup)?;
                }
                Ok(record)
            }
            Ok(None) => {
                if backup.exists() {
                    std::fs::remove_dir_all(&backup)?;
                }
                Err(AppError::WorkspaceNotFound(workspace_name.to_string()))
            }
            Err(error) => {
                let restore_dir_result =
                    restore_workspace_dir(&workspace_dir, &backup, had_workspace_dir);
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
                    self.restore_workspace_credentials(workspace_name, &snapshots);
                    return Err(error);
                }
            };
            let snapshot = match guard.snapshot_material(storage) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    self.restore_workspace_credentials(workspace_name, &snapshots);
                    return Err(error);
                }
            };
            if let Err(error) = guard.remove_material(storage) {
                snapshots.push(WorkspaceCredentialSnapshot {
                    credential_set_id,
                    snapshot,
                });
                self.restore_workspace_credentials(workspace_name, &snapshots);
                return Err(error);
            }
            snapshots.push(WorkspaceCredentialSnapshot {
                credential_set_id,
                snapshot,
            });
        }
        Ok(snapshots)
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

fn workspace_delete_backup_path(workspace_dir: &Path, workspace_name: &WorkspaceName) -> PathBuf {
    workspace_dir.with_file_name(format!(
        "{}.delete.rollback.{}",
        workspace_name,
        Uuid::new_v4()
    ))
}

fn restore_workspace_dir(
    workspace_dir: &Path,
    backup: &Path,
    had_workspace_dir: bool,
) -> Result<(), AppError> {
    if had_workspace_dir && backup.exists() {
        if workspace_dir.exists() {
            std::fs::remove_dir_all(workspace_dir)?;
        }
        std::fs::rename(backup, workspace_dir)?;
    }
    Ok(())
}
