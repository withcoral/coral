use std::path::{Path, PathBuf};
use std::sync::Arc;

use tracing::warn;
use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::credentials::{CredentialManager, CredentialMaterialSnapshot, CredentialSetId};
use crate::workspaces::{
    DEFAULT_WORKSPACE_ID, WorkspaceLifecycleLock, WorkspaceName, WorkspaceRecord, WorkspaceStore,
};

/// App-owned workspace lifecycle behavior.
#[derive(Clone)]
pub(crate) struct WorkspaceManager {
    store: Arc<dyn WorkspaceStore>,
    credential_manager: CredentialManager,
    workspaces_root: PathBuf,
    trace_store_dir: Option<PathBuf>,
    lifecycle_lock: WorkspaceLifecycleLock,
}

impl WorkspaceManager {
    #[cfg(test)]
    pub(crate) fn new(
        store: impl WorkspaceStore,
        credential_manager: CredentialManager,
        workspaces_root: impl Into<PathBuf>,
        trace_store_dir: Option<PathBuf>,
    ) -> Self {
        Self::new_with_lifecycle_lock(
            store,
            credential_manager,
            workspaces_root,
            trace_store_dir,
            WorkspaceLifecycleLock::default(),
        )
    }

    pub(crate) fn new_with_lifecycle_lock(
        store: impl WorkspaceStore,
        credential_manager: CredentialManager,
        workspaces_root: impl Into<PathBuf>,
        trace_store_dir: Option<PathBuf>,
        lifecycle_lock: WorkspaceLifecycleLock,
    ) -> Self {
        Self {
            store: Arc::new(store),
            credential_manager,
            workspaces_root: workspaces_root.into(),
            trace_store_dir,
            lifecycle_lock,
        }
    }

    pub(crate) fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, AppError> {
        self.store.list_workspaces()
    }

    pub(crate) fn create_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<WorkspaceRecord, AppError> {
        let _lifecycle_guard = self.lifecycle_lock.lock();
        self.store.create_workspace(workspace_name)
    }

    pub(crate) fn delete_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<WorkspaceRecord, AppError> {
        let _lifecycle_guard = self.lifecycle_lock.lock();
        if workspace_name.as_str() == DEFAULT_WORKSPACE_ID {
            return Err(AppError::FailedPrecondition(
                "default workspace cannot be removed".to_string(),
            ));
        }

        let credential_snapshots = self.remove_workspace_credentials(workspace_name)?;
        let workspace_dir = self.workspace_dir(workspace_name);
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
            Ok(Some(deleted)) => {
                let record = deleted.workspace.clone();
                if let Err(error) = self.delete_workspace_traces(workspace_name) {
                    let restore_config_result = self.store.restore_workspace(deleted);
                    let restore_dir_result =
                        restore_workspace_dir(&workspace_dir, &backup, had_workspace_dir);
                    self.restore_workspace_credentials(workspace_name, &credential_snapshots);
                    restore_config_result?;
                    restore_dir_result?;
                    return Err(error);
                }
                if backup.exists()
                    && let Err(error) = std::fs::remove_dir_all(&backup)
                {
                    warn!(
                        workspace = %workspace_name,
                        backup_path = %backup.display(),
                        "workspace deleted, but failed to remove workspace artifact backup: {error}"
                    );
                }
                Ok(record)
            }
            Ok(None) => {
                restore_workspace_dir(&workspace_dir, &backup, had_workspace_dir)?;
                self.restore_workspace_credentials(workspace_name, &credential_snapshots);
                Err(AppError::WorkspaceNotFound(workspace_name.to_string()))
            }
            Err(error) => {
                restore_workspace_dir(&workspace_dir, &backup, had_workspace_dir)?;
                self.restore_workspace_credentials(workspace_name, &credential_snapshots);
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
            let Ok(guard) = self
                .credential_manager
                .material_guard(workspace_name, &snapshot.credential_set_id)
            else {
                continue;
            };
            if let Err(error) = guard.restore_material(&snapshot.snapshot) {
                warn!(
                    credential_set_id = %snapshot.credential_set_id,
                    "rollback: failed to restore workspace credential material: {error}"
                );
            }
        }
    }

    fn workspace_dir(&self, workspace_name: &WorkspaceName) -> PathBuf {
        self.workspaces_root.join(workspace_name.as_str())
    }

    fn delete_workspace_traces(&self, workspace_name: &WorkspaceName) -> Result<(), AppError> {
        let Some(trace_store_dir) = &self.trace_store_dir else {
            return Ok(());
        };
        crate::telemetry::delete_workspace_traces(trace_store_dir.clone(), workspace_name)
            .map(|_removed| ())
            .map_err(|error| {
                AppError::FailedPrecondition(format!(
                    "workspace '{workspace_name}' cannot be removed until local trace history can be cleaned up: {error}"
                ))
            })
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::Duration;

    use tempfile::TempDir;

    use super::WorkspaceManager;
    use crate::bootstrap::AppError;
    use crate::credentials::{
        CredentialManager, CredentialSetId, CredentialStorageKind, CredentialStoragePreference,
        CredentialStore,
    };
    use crate::sources::SourceName;
    use crate::sources::manager::{ImportSourceCommand, SourceBindings, SourceManager};
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::{AppStateLayout, ConfigStore, ConfigWorkspaceStore};
    use crate::workspaces::{
        DeletedWorkspaceRecord, WorkspaceName, WorkspaceRecord, WorkspaceStore,
    };

    #[test]
    fn delete_workspace_removes_keychain_routed_source_credentials() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::with_available_keychain_for_test(
            layout.clone(),
            CredentialStoragePreference::Auto,
        );
        let credential_manager = CredentialManager::new(credential_store);
        let workspace_manager = WorkspaceManager::new(
            ConfigWorkspaceStore::new(config_store.clone()),
            credential_manager.clone(),
            layout.workspaces_root(),
            Some(layout.local_trace_store_dir()),
        );
        let workspace_name = WorkspaceName::parse("work").expect("workspace");
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);

        workspace_manager
            .create_workspace(&workspace_name)
            .expect("create workspace");
        config_store
            .upsert_source(
                &workspace_name,
                InstalledSource {
                    name: source_name,
                    version: None,
                    variables: BTreeMap::new(),
                    secrets: vec!["API_TOKEN".to_string()],
                    credential_storage: Some(CredentialStorageKind::Keychain),
                    origin: SourceOrigin::Imported,
                },
            )
            .expect("store source");
        credential_manager
            .replace_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::Keychain,
                &BTreeMap::from([("API_TOKEN".to_string(), "secret-token".to_string())]),
            )
            .expect("store keychain material");

        workspace_manager
            .delete_workspace(&workspace_name)
            .expect("delete workspace");

        let material = credential_manager
            .read_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::Keychain,
            )
            .expect("read removed keychain material");
        assert!(
            material.is_empty(),
            "workspace delete should remove keychain-routed source material"
        );
        assert_eq!(
            config_store
                .list_workspaces()
                .expect("list workspaces")
                .into_iter()
                .map(|workspace| workspace.name.to_string())
                .collect::<Vec<_>>(),
            vec!["default".to_string()]
        );
    }

    #[cfg(unix)]
    #[test]
    fn delete_workspace_rollback_serializes_concurrent_recreate() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let workspace_name = WorkspaceName::parse("work").expect("workspace");
        let source_name = SourceName::parse("local_messages").expect("source");
        let source = InstalledSource {
            name: source_name,
            version: None,
            variables: BTreeMap::new(),
            secrets: Vec::new(),
            credential_storage: None,
            origin: SourceOrigin::Imported,
        };
        let (delete_started_tx, delete_started_rx) = mpsc::channel();
        let (release_delete_tx, release_delete_rx) = mpsc::channel();
        let store = BlockingDeleteWorkspaceStore::new(
            workspace_name.clone(),
            vec![source],
            delete_started_tx,
            release_delete_rx,
        );
        let credential_manager = CredentialManager::new(CredentialStore::new(layout.clone()));
        let workspace_manager = WorkspaceManager::new(
            store.clone(),
            credential_manager,
            layout.workspaces_root(),
            Some(layout.local_trace_store_dir()),
        );
        let trace_dir = layout.local_trace_store_dir();
        std::fs::create_dir_all(&trace_dir).expect("create trace dir");
        let trace_file = trace_dir.join("spans-00000000000000000001-1-0000000000000000.jsonl");
        std::fs::write(
            &trace_file,
            r#"{"trace_id":"trace","span_id":"span","attributes_json":"{\"workspace\":\"work\"}"}"#,
        )
        .expect("write trace file");
        std::fs::set_permissions(&trace_file, std::fs::Permissions::from_mode(0o000))
            .expect("make trace file unreadable");

        let deleting_manager = workspace_manager.clone();
        let deleting_workspace = workspace_name.clone();
        let delete_thread =
            std::thread::spawn(move || deleting_manager.delete_workspace(&deleting_workspace));
        delete_started_rx.recv().expect("delete started");

        let creating_manager = workspace_manager.clone();
        let creating_workspace = workspace_name.clone();
        let (create_done_tx, create_done_rx) = mpsc::channel();
        let create_thread = std::thread::spawn(move || {
            create_done_tx
                .send(creating_manager.create_workspace(&creating_workspace))
                .expect("send create result");
        });
        assert!(
            create_done_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "workspace create should wait for delete rollback while lifecycle lock is held"
        );

        release_delete_tx.send(()).expect("release delete");
        let delete_error = delete_thread
            .join()
            .expect("delete thread")
            .expect_err("trace cleanup should fail");
        assert!(matches!(delete_error, AppError::FailedPrecondition(_)));
        let create_error = create_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("create result")
            .expect_err("recreate should see restored original workspace");
        assert!(matches!(create_error, AppError::WorkspaceAlreadyExists(_)));
        create_thread.join().expect("create thread");
        assert_eq!(
            store
                .list_workspace_sources(&workspace_name)
                .expect("list restored sources")
                .len(),
            1
        );
    }

    #[cfg(unix)]
    #[test]
    fn shared_lifecycle_lock_serializes_source_import_during_delete_rollback() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let workspace_name = WorkspaceName::parse("work").expect("workspace");
        let source = InstalledSource {
            name: SourceName::parse("existing_messages").expect("source"),
            version: None,
            variables: BTreeMap::new(),
            secrets: Vec::new(),
            credential_storage: None,
            origin: SourceOrigin::Imported,
        };
        let (delete_started_tx, delete_started_rx) = mpsc::channel();
        let (release_delete_tx, release_delete_rx) = mpsc::channel();
        let store = BlockingDeleteWorkspaceStore::new(
            workspace_name.clone(),
            vec![source],
            delete_started_tx,
            release_delete_rx,
        );
        let credential_manager = CredentialManager::new(CredentialStore::new(layout.clone()));
        let lifecycle_lock = crate::workspaces::WorkspaceLifecycleLock::default();
        let workspace_manager = WorkspaceManager::new_with_lifecycle_lock(
            store.clone(),
            credential_manager.clone(),
            layout.workspaces_root(),
            Some(layout.local_trace_store_dir()),
            lifecycle_lock.clone(),
        );
        let config_store = ConfigStore::new(layout.clone());
        config_store
            .create_workspace(&workspace_name)
            .expect("create config workspace");
        let source_manager = SourceManager::new_with_lifecycle_lock(
            config_store.clone(),
            credential_manager,
            layout.clone(),
            lifecycle_lock,
        );
        let trace_dir = layout.local_trace_store_dir();
        std::fs::create_dir_all(&trace_dir).expect("create trace dir");
        let trace_file = trace_dir.join("spans-00000000000000000001-1-0000000000000000.jsonl");
        std::fs::write(
            &trace_file,
            r#"{"trace_id":"trace","span_id":"span","attributes_json":"{\"workspace\":\"work\"}"}"#,
        )
        .expect("write trace file");
        std::fs::set_permissions(&trace_file, std::fs::Permissions::from_mode(0o000))
            .expect("make trace file unreadable");

        let deleting_manager = workspace_manager.clone();
        let deleting_workspace = workspace_name.clone();
        let delete_thread =
            std::thread::spawn(move || deleting_manager.delete_workspace(&deleting_workspace));
        delete_started_rx.recv().expect("delete started");

        let importing_workspace = workspace_name.clone();
        let import_thread = std::thread::spawn(move || {
            source_manager.import_source(
                &importing_workspace,
                &ImportSourceCommand {
                    manifest_yaml: public_messages_manifest(),
                    bindings: SourceBindings::default(),
                },
            )
        });
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            config_store
                .list_workspace_sources(&workspace_name)
                .expect("list sources while delete is blocked")
                .is_empty(),
            "source import should wait while workspace delete holds the lifecycle lock"
        );

        release_delete_tx.send(()).expect("release delete");
        let delete_error = delete_thread
            .join()
            .expect("delete thread")
            .expect_err("trace cleanup should fail");
        assert!(matches!(delete_error, AppError::FailedPrecondition(_)));
        let imported = import_thread
            .join()
            .expect("import thread")
            .expect("import after delete rollback");
        assert_eq!(imported.name.as_str(), "public_messages");
        assert_eq!(
            config_store
                .list_workspace_source_names(&workspace_name)
                .expect("list sources after import")
                .into_iter()
                .collect::<Vec<_>>(),
            vec!["public_messages"]
        );
    }

    fn public_messages_manifest() -> String {
        r#"
name: public_messages
version: 0.1.0
dsl_version: 3
backend: http
base_url: "https://example.com"
tables:
  - name: messages
    description: Public messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#
        .to_string()
    }

    #[derive(Clone)]
    struct BlockingDeleteWorkspaceStore {
        inner: Arc<BlockingDeleteWorkspaceStoreInner>,
    }

    struct BlockingDeleteWorkspaceStoreInner {
        sources: Mutex<BTreeMap<WorkspaceName, Vec<InstalledSource>>>,
        delete_started: mpsc::Sender<()>,
        release_delete: Mutex<mpsc::Receiver<()>>,
    }

    impl BlockingDeleteWorkspaceStore {
        fn new(
            workspace_name: WorkspaceName,
            sources: Vec<InstalledSource>,
            delete_started: mpsc::Sender<()>,
            release_delete: mpsc::Receiver<()>,
        ) -> Self {
            Self {
                inner: Arc::new(BlockingDeleteWorkspaceStoreInner {
                    sources: Mutex::new(BTreeMap::from([(workspace_name, sources)])),
                    delete_started,
                    release_delete: Mutex::new(release_delete),
                }),
            }
        }

        fn sources(
            &self,
        ) -> std::sync::MutexGuard<'_, BTreeMap<WorkspaceName, Vec<InstalledSource>>> {
            self.inner
                .sources
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    }

    impl WorkspaceStore for BlockingDeleteWorkspaceStore {
        fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, AppError> {
            Ok(self
                .sources()
                .keys()
                .cloned()
                .map(|name| WorkspaceRecord { name })
                .collect())
        }

        fn create_workspace(
            &self,
            workspace_name: &WorkspaceName,
        ) -> Result<WorkspaceRecord, AppError> {
            let mut sources = self.sources();
            if sources.contains_key(workspace_name) {
                return Err(AppError::WorkspaceAlreadyExists(workspace_name.to_string()));
            }
            sources.insert(workspace_name.clone(), Vec::new());
            Ok(WorkspaceRecord {
                name: workspace_name.clone(),
            })
        }

        fn list_workspace_sources(
            &self,
            workspace_name: &WorkspaceName,
        ) -> Result<Vec<InstalledSource>, AppError> {
            self.sources()
                .get(workspace_name)
                .cloned()
                .ok_or_else(|| AppError::WorkspaceNotFound(workspace_name.to_string()))
        }

        fn delete_workspace(
            &self,
            workspace_name: &WorkspaceName,
        ) -> Result<Option<DeletedWorkspaceRecord>, AppError> {
            let removed =
                self.sources()
                    .remove(workspace_name)
                    .map(|sources| DeletedWorkspaceRecord {
                        workspace: WorkspaceRecord {
                            name: workspace_name.clone(),
                        },
                        sources,
                    });
            self.inner.delete_started.send(()).map_err(|_closed| {
                AppError::FailedPrecondition("test delete-started channel closed".to_string())
            })?;
            self.inner
                .release_delete
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .recv()
                .map_err(|_closed| {
                    AppError::FailedPrecondition("test delete-release channel closed".to_string())
                })?;
            Ok(removed)
        }

        fn restore_workspace(&self, deleted: DeletedWorkspaceRecord) -> Result<(), AppError> {
            let mut sources = self.sources();
            if sources.contains_key(&deleted.workspace.name) {
                return Err(AppError::WorkspaceAlreadyExists(
                    deleted.workspace.name.to_string(),
                ));
            }
            sources.insert(deleted.workspace.name, deleted.sources);
            Ok(())
        }
    }
}
