use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use tracing::warn;

use crate::bootstrap::AppError;
use crate::credentials::{CredentialManager, CredentialSetId};
use crate::state::db::{CoralDb, DbRepos, now_unix_nanos_i64};
use crate::storage::fs::DirectoryBackup;
use crate::workspaces::{
    DeletedWorkspace, WorkspaceLifecycleLock, WorkspaceName, WorkspacePaths, WorkspaceRecord,
    WorkspaceStore,
};

/// App-owned workspace lifecycle behavior.
#[derive(Clone)]
pub(crate) struct WorkspaceManager {
    store: Arc<dyn WorkspaceStore>,
    credential_manager: CredentialManager,
    paths: Arc<dyn WorkspacePaths>,
    trace_store_dir: Option<PathBuf>,
    lifecycle_lock: WorkspaceLifecycleLock,
    db: Arc<CoralDb>,
}

impl WorkspaceManager {
    #[cfg(test)]
    pub(crate) fn new_for_tests(
        store: impl WorkspaceStore,
        credential_manager: CredentialManager,
        paths: impl WorkspacePaths,
        trace_store_dir: Option<PathBuf>,
        db: Arc<CoralDb>,
    ) -> Self {
        Self::new(
            store,
            credential_manager,
            paths,
            trace_store_dir,
            WorkspaceLifecycleLock::default(),
            db,
        )
    }

    pub(crate) fn new(
        store: impl WorkspaceStore,
        credential_manager: CredentialManager,
        paths: impl WorkspacePaths,
        trace_store_dir: Option<PathBuf>,
        lifecycle_lock: WorkspaceLifecycleLock,
        db: Arc<CoralDb>,
    ) -> Self {
        Self {
            store: Arc::new(store),
            credential_manager,
            paths: Arc::new(paths),
            trace_store_dir,
            lifecycle_lock,
            db,
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
        let created = self.store.create_workspace(workspace_name)?;
        if let Err(error) = self.ensure_workspace_record(&created.name) {
            if let Err(rollback_error) = self.store.delete_workspace(&created.name) {
                warn!(
                    workspace = %created.name,
                    "workspace database write failed, and legacy config rollback also failed: {rollback_error}"
                );
            }
            return Err(error);
        }
        Ok(created)
    }

    pub(crate) async fn delete_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<WorkspaceRecord, AppError> {
        if workspace_name.is_default() {
            return Err(AppError::FailedPrecondition(
                "default workspace cannot be removed".to_string(),
            ));
        }

        let (deleted, workspace_dir_backup) = {
            let _lifecycle_guard = self.lifecycle_lock.lock();
            let deleted = self
                .store
                .delete_workspace(workspace_name)?
                .ok_or_else(|| AppError::WorkspaceNotFound(workspace_name.to_string()))?;
            self.delete_workspace_record(&deleted.workspace.name)?;
            self.remove_deleted_workspace_credentials(&deleted);
            let workspace_dir_backup = self.stage_deleted_workspace_dir(&deleted.workspace.name);
            (deleted, workspace_dir_backup)
        };

        let deleted_workspace_name = deleted.workspace.name.clone();
        Self::commit_deleted_workspace_dir(&deleted_workspace_name, workspace_dir_backup);
        self.prune_deleted_workspace_traces(&deleted_workspace_name)
            .await;
        Ok(deleted.workspace)
    }

    fn remove_deleted_workspace_credentials(&self, deleted: &DeletedWorkspace) {
        let workspace_name = &deleted.workspace.name;
        for source in &deleted.sources {
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
                    warn!(
                        workspace = %workspace_name,
                        source = %source.name,
                        credential_set_id = %credential_set_id,
                        "workspace deleted, but failed to access credential material for cleanup: {error}"
                    );
                    continue;
                }
            };
            if let Err(error) = guard.remove_material(storage) {
                warn!(
                    workspace = %workspace_name,
                    source = %source.name,
                    credential_set_id = %credential_set_id,
                    %storage,
                    "workspace deleted, but failed to remove credential material: {error}"
                );
            }
        }
    }

    fn stage_deleted_workspace_dir(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Option<DirectoryBackup> {
        let workspace_dir = self.workspace_dir(workspace_name);
        match DirectoryBackup::move_for_delete(&workspace_dir, workspace_name) {
            Ok(backup) => Some(backup),
            Err(error) => {
                warn!(
                    workspace = %workspace_name,
                    workspace_dir = %workspace_dir.display(),
                    "workspace deleted, but failed to stage workspace directory cleanup: {error}"
                );
                None
            }
        }
    }

    fn commit_deleted_workspace_dir(
        workspace_name: &WorkspaceName,
        backup: Option<DirectoryBackup>,
    ) {
        let Some(backup) = backup else {
            return;
        };
        if let Err(error) = backup.commit() {
            warn!(
                workspace = %workspace_name,
                backup_path = %backup.backup_path().display(),
                "workspace deleted, but failed to remove workspace artifact backup: {error}"
            );
        }
    }

    fn workspace_dir(&self, workspace_name: &WorkspaceName) -> std::path::PathBuf {
        self.paths.workspace_dir(workspace_name)
    }

    fn ensure_workspace_record(&self, workspace_name: &WorkspaceName) -> Result<(), AppError> {
        let db = Arc::clone(&self.db);
        let workspace_name = workspace_name.clone();
        run_workspace_db_operation(async move {
            let mut tx = db.begin().await?;
            tx.workspaces()
                .ensure(workspace_name.as_str(), now_unix_nanos_i64()?)
                .await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn delete_workspace_record(&self, workspace_name: &WorkspaceName) -> Result<(), AppError> {
        let db = Arc::clone(&self.db);
        let workspace_name = workspace_name.clone();
        run_workspace_db_operation(async move {
            let mut tx = db.begin().await?;
            tx.workspaces().delete(workspace_name.as_str()).await?;
            tx.commit().await?;
            Ok(())
        })
    }

    async fn prune_deleted_workspace_traces(&self, workspace_name: &WorkspaceName) {
        let Some(trace_store_dir) = &self.trace_store_dir else {
            return;
        };
        if let Err(error) =
            crate::telemetry::delete_workspace_traces(trace_store_dir.clone(), workspace_name).await
        {
            warn!(
                workspace = %workspace_name,
                "workspace deleted, but failed to prune local trace history: {error}"
            );
        }
    }
}

fn run_workspace_db_operation<T, F>(operation: F) -> Result<T, AppError>
where
    T: Send + 'static,
    F: Future<Output = Result<T, AppError>> + Send + 'static,
{
    fn run_on_runtime<T, F>(operation: F) -> Result<T, AppError>
    where
        F: Future<Output = Result<T, AppError>>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                AppError::FailedPrecondition(format!(
                    "failed to create workspace database runtime: {error}"
                ))
            })?;
        runtime.block_on(operation)
    }

    if tokio::runtime::Handle::try_current().is_ok() {
        return std::thread::spawn(move || run_on_runtime(operation))
            .join()
            .map_err(|_panic| {
                AppError::FailedPrecondition(
                    "workspace database operation thread panicked".to_string(),
                )
            })?;
    }

    run_on_runtime(operation)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::WorkspaceManager;
    use crate::credentials::{CredentialManager, CredentialSetId, CredentialStore};
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::WorkspaceName;

    fn test_layout(temp: &TempDir) -> AppStateLayout {
        AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout")
    }

    fn installed_source(name: &str) -> InstalledSource {
        InstalledSource {
            name: SourceName::parse(name).expect("source"),
            version: Some("1.0.0".to_string()),
            variables: BTreeMap::new(),
            secrets: vec!["TOKEN".to_string()],
            credential_storage: None,
            origin: SourceOrigin::Imported,
        }
    }

    async fn test_db(layout: &AppStateLayout) -> Arc<CoralDb> {
        let config = DatabaseConfig::load(layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        Arc::new(db)
    }

    #[tokio::test]
    async fn delete_workspace_commits_config_then_cleans_artifacts() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        let store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let db = test_db(&layout).await;
        let manager = WorkspaceManager::new_for_tests(
            store.clone(),
            credential_manager.clone(),
            layout.clone(),
            None,
            Arc::clone(&db),
        );
        let workspace_name = WorkspaceName::parse("work").expect("workspace");
        let source = installed_source("github");
        let credential_set_id = CredentialSetId::for_source(&source.name);

        manager
            .create_workspace(&workspace_name)
            .expect("create workspace");
        store
            .upsert_source(&workspace_name, source)
            .expect("upsert source");
        credential_manager
            .replace_material(
                &workspace_name,
                &credential_set_id,
                crate::credentials::CredentialStorageKind::File,
                &BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]),
            )
            .expect("write credential material");
        std::fs::create_dir_all(layout.feedback_dir(&workspace_name)).expect("create feedback dir");
        std::fs::write(
            layout.feedback_reports_file(&workspace_name),
            b"{\"message\":\"report\"}\n",
        )
        .expect("write workspace artifact");

        let deleted = manager
            .delete_workspace(&workspace_name)
            .await
            .expect("delete workspace");

        assert_eq!(deleted.name, workspace_name);
        assert!(matches!(
            store.list_workspace_sources(&workspace_name),
            Err(crate::bootstrap::AppError::WorkspaceNotFound(_))
        ));
        assert!(
            !layout.workspace_dir(&workspace_name).exists(),
            "workspace artifact directory should be removed after config commit"
        );
        let material = credential_manager
            .read_material(
                &workspace_name,
                &credential_set_id,
                crate::credentials::CredentialStorageKind::File,
            )
            .expect("read credential material");
        assert!(
            material.is_empty(),
            "credential material should be removed during best-effort cleanup"
        );
    }
}
