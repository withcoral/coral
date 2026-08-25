use std::path::PathBuf;
use std::sync::Arc;

use tracing::warn;

use crate::bootstrap::AppError;
use crate::credentials::{CredentialManager, CredentialSetId};
use crate::search::store::SearchStorage;
use crate::sources::materialization::SourceDiagnosticReporter;
use crate::state::ConfigStore;
use crate::state::db::{CoralDb, DbRepos, now_unix_nanos_i64};
use crate::storage::fs::DirectoryBackup;
use crate::workspaces::{
    DeletedWorkspace, WorkspaceLifecycleLock, WorkspaceLifecycleRevision, WorkspaceName,
    WorkspacePaths, WorkspacePoolRegistry, WorkspaceRecord,
};

/// App-owned workspace lifecycle behavior.
#[derive(Clone)]
pub(crate) struct WorkspaceManager {
    config_store: ConfigStore,
    credential_manager: CredentialManager,
    paths: Arc<dyn WorkspacePaths>,
    trace_store_dir: Option<PathBuf>,
    lifecycle_lock: WorkspaceLifecycleLock,
    db: Arc<CoralDb>,
    diagnostic_reporter: SourceDiagnosticReporter,
    pool_registry: Arc<WorkspacePoolRegistry>,
    search_storage: Option<SearchStorage>,
}

impl WorkspaceManager {
    #[cfg(test)]
    pub(crate) fn new_for_tests(
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        paths: impl WorkspacePaths,
        trace_store_dir: Option<PathBuf>,
        db: Arc<CoralDb>,
    ) -> Self {
        Self::new(
            config_store,
            credential_manager,
            paths,
            trace_store_dir,
            WorkspaceLifecycleLock::default(),
            db,
            SourceDiagnosticReporter::default(),
        )
    }

    pub(crate) fn new(
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        paths: impl WorkspacePaths,
        trace_store_dir: Option<PathBuf>,
        lifecycle_lock: WorkspaceLifecycleLock,
        db: Arc<CoralDb>,
        diagnostic_reporter: SourceDiagnosticReporter,
    ) -> Self {
        Self {
            config_store,
            credential_manager,
            paths: Arc::new(paths),
            trace_store_dir,
            lifecycle_lock,
            db,
            diagnostic_reporter,
            pool_registry: Arc::new(WorkspacePoolRegistry::default()),
            search_storage: None,
        }
    }

    pub(crate) fn with_pool_registry(mut self, pool_registry: Arc<WorkspacePoolRegistry>) -> Self {
        self.pool_registry = pool_registry;
        self
    }

    /// Search storage whose per-Workspace state is removed on deletion.
    ///
    /// Optional because only shared backends keep state outside the Workspace
    /// directory: the `SQLite` sidecar goes with the directory. The server
    /// bootstrap always sets it; a constructor parameter would need an
    /// `AppStateLayout` that the generic `WorkspacePaths` tests use cannot give.
    pub(crate) fn with_search_storage(mut self, search_storage: SearchStorage) -> Self {
        self.search_storage = Some(search_storage);
        self
    }

    pub(crate) async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, AppError> {
        let mut session = self.db.as_ref();
        session
            .workspaces()
            .list()
            .await?
            .into_iter()
            .map(|workspace| {
                let name = WorkspaceName::parse(&workspace.id).map_err(|error| {
                    AppError::Database(format!("invalid workspace id '{}': {error}", workspace.id))
                })?;
                Ok(WorkspaceRecord { name })
            })
            .collect()
    }

    pub(crate) async fn require_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<(), AppError> {
        let mut session = self.db.as_ref();
        if session
            .workspaces()
            .get(workspace_name.as_str())
            .await?
            .is_some()
        {
            Ok(())
        } else {
            Err(AppError::WorkspaceNotFound(workspace_name.to_string()))
        }
    }

    /// Verifies the canonical workspace row while holding one active lifecycle
    /// snapshot, then returns the revision a long-running writer must preserve.
    pub(crate) async fn require_active_workspace_revision(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<WorkspaceLifecycleRevision, AppError> {
        let snapshot = self.lifecycle_lock.snapshot_async().await;
        if snapshot.workspace_is_deleting(workspace_name) {
            return Err(AppError::WorkspaceNotFound(workspace_name.to_string()));
        }
        self.require_workspace(workspace_name).await?;
        Ok(snapshot.revision())
    }

    #[cfg(test)]
    pub(crate) fn lifecycle_lock(&self) -> WorkspaceLifecycleLock {
        self.lifecycle_lock.clone()
    }

    pub(crate) async fn create_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<WorkspaceRecord, AppError> {
        let _lifecycle_guard = self.lifecycle_lock.lock_async().await;
        let mut tx = self.db.begin().await?;
        if tx
            .workspaces()
            .get(workspace_name.as_str())
            .await?
            .is_some()
        {
            return Err(AppError::WorkspaceAlreadyExists(workspace_name.to_string()));
        }
        if let Err(error) = tx
            .workspaces()
            .create(workspace_name.as_str(), now_unix_nanos_i64()?)
            .await
        {
            if error.is_unique_violation() {
                return Err(AppError::WorkspaceAlreadyExists(workspace_name.to_string()));
            }
            return Err(error.into());
        }
        tx.commit().await?;
        Ok(WorkspaceRecord {
            name: workspace_name.clone(),
        })
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

        let deletion_marker = self
            .lifecycle_lock
            .mark_workspace_deleting(workspace_name)
            .await
            .ok_or_else(|| {
                AppError::FailedPrecondition(format!(
                    "workspace '{workspace_name}' is already being deleted"
                ))
            })?;

        let (deleted, workspace_dir_backup) = {
            let Some(deletion) = self
                .db
                .begin_workspace_deletion(workspace_name.as_str())
                .await?
            else {
                return Err(AppError::WorkspaceNotFound(workspace_name.to_string()));
            };
            let deleted = self
                .config_store
                .remove_workspace_config_entries(workspace_name);
            let deleted = match deleted {
                Ok(deleted) => deleted.unwrap_or_else(|| DeletedWorkspace {
                    workspace: WorkspaceRecord {
                        name: workspace_name.clone(),
                    },
                    sources: Vec::new(),
                }),
                Err(error) => {
                    if let Err(rollback_error) = deletion.rollback().await {
                        warn!(
                            workspace = %workspace_name,
                            "workspace config cleanup failed, and database rollback also failed: {rollback_error}"
                        );
                    }
                    return Err(error);
                }
            };
            deletion.commit().await?;
            self.pool_registry.remove(workspace_name);
            self.remove_deleted_workspace_credentials(&deleted);
            let workspace_dir_backup = self.stage_deleted_workspace_dir(&deleted.workspace.name);
            (deleted, workspace_dir_backup)
        };
        drop(deletion_marker);

        let deleted_workspace_name = deleted.workspace.name.clone();
        self.diagnostic_reporter
            .clear_workspace(&deleted_workspace_name);
        Self::commit_deleted_workspace_dir(&deleted_workspace_name, workspace_dir_backup);
        self.prune_deleted_workspace_traces(&deleted_workspace_name)
            .await;
        self.delete_workspace_search_state(&deleted_workspace_name)
            .await;
        Ok(deleted.workspace)
    }

    /// Best effort, after the deletion committed: a Workspace that no longer
    /// exists must not keep catalog rows in shared search storage. A failure
    /// here is finished by the next boot's sweep, which prunes registered
    /// Workspaces that the database no longer lists.
    async fn delete_workspace_search_state(&self, workspace_name: &WorkspaceName) {
        let Some(search_storage) = &self.search_storage else {
            return;
        };
        match search_storage.delete_workspace(workspace_name).await {
            Ok(deleted) => {
                tracing::debug!(
                    workspace = %workspace_name,
                    search_backend = search_storage.backend_name(),
                    deleted,
                    "removed deleted workspace's search state"
                );
            }
            Err(error) => {
                warn!(
                    workspace = %workspace_name,
                    search_backend = search_storage.backend_name(),
                    error = %error,
                    "workspace deleted, but failed to remove its search state; the next boot sweep removes it"
                );
            }
        }
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::WorkspaceManager;
    use crate::bootstrap;
    use crate::credentials::{CredentialManager, CredentialSetId, CredentialStore};
    use crate::search::store::SearchStorage;
    use crate::sources::SourceName;
    use crate::sources::materialization::{SourceDiagnosticReporter, SourceLoadDiagnosticStage};
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::{WorkspaceName, WorkspacePoolRegistry};

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
            credential_revision: uuid::Uuid::default(),
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

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run Workspace deletion against Postgres search storage"]
    async fn delete_workspace_removes_search_state_contract_on_postgres() {
        let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
        else {
            return;
        };
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url: url.clone() })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");
        let search_storage = SearchStorage::postgres(&url, tokio::runtime::Handle::current())
            .await
            .expect("open postgres search storage");
        let manager = WorkspaceManager::new(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout,
            None,
            crate::workspaces::WorkspaceLifecycleLock::default(),
            Arc::new(db),
            SourceDiagnosticReporter::default(),
        )
        .with_search_storage(search_storage.clone());
        let workspace_name =
            WorkspaceName::parse(&format!("usp-delete-{}", uuid::Uuid::new_v4().simple()))
                .expect("workspace");
        manager
            .create_workspace(&workspace_name)
            .await
            .expect("create workspace");
        let open_storage = search_storage.clone();
        let open_workspace = workspace_name.clone();
        tokio::task::spawn_blocking(move || {
            open_storage
                .open_workspace(&open_workspace)
                .expect("first search registers the workspace's search schema");
        })
        .await
        .expect("open search state");

        manager
            .delete_workspace(&workspace_name)
            .await
            .expect("delete workspace");

        let probe_workspace = workspace_name.clone();
        let remaining = tokio::task::spawn_blocking(move || {
            search_storage
                .open_existing_workspace(&probe_workspace)
                .expect("probe search state")
                .is_some()
        })
        .await
        .expect("probe search state");
        assert!(
            !remaining,
            "deleting the workspace must remove its search state"
        );
    }

    #[tokio::test]
    async fn delete_workspace_commits_config_then_cleans_artifacts() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        let store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let db = test_db(&layout).await;
        let diagnostic_reporter = SourceDiagnosticReporter::default();
        let pool_registry = Arc::new(WorkspacePoolRegistry::default());
        let manager = WorkspaceManager::new(
            store.clone(),
            credential_manager.clone(),
            layout.clone(),
            None,
            crate::workspaces::WorkspaceLifecycleLock::default(),
            Arc::clone(&db),
            diagnostic_reporter.clone(),
        )
        .with_pool_registry(Arc::clone(&pool_registry));
        let workspace_name = WorkspaceName::parse("work").expect("workspace");
        let pool_registry_before_delete = pool_registry.for_workspace(&workspace_name);
        let source = installed_source("github");
        let source_name = source.name.clone();
        let credential_set_id = CredentialSetId::for_source(&source.name);

        manager
            .create_workspace(&workspace_name)
            .await
            .expect("create workspace");
        store
            .upsert_source(&workspace_name, source)
            .expect("upsert source");
        diagnostic_reporter.report_source_load_failure(
            SourceLoadDiagnosticStage::Query,
            &workspace_name,
            &source_name,
            "test failure",
        );
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
        assert!(
            store
                .list_workspace_sources(&workspace_name)
                .expect("list source definitions")
                .is_empty()
        );
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
        assert!(!diagnostic_reporter.tracks_diagnostic(
            &workspace_name,
            &source_name,
            "query-source",
            "test failure",
        ));
        let pool_registry_after_delete = pool_registry.for_workspace(&workspace_name);
        assert!(!Arc::ptr_eq(
            &pool_registry_before_delete,
            &pool_registry_after_delete
        ));
    }

    #[tokio::test]
    async fn failed_workspace_delete_keeps_diagnostic_state() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        let store = ConfigStore::new(layout.clone());
        let credential_manager = CredentialManager::new(CredentialStore::new(layout.clone()));
        let db = test_db(&layout).await;
        let diagnostic_reporter = SourceDiagnosticReporter::default();
        let pool_registry = Arc::new(WorkspacePoolRegistry::default());
        let manager = WorkspaceManager::new(
            store,
            credential_manager,
            layout,
            None,
            crate::workspaces::WorkspaceLifecycleLock::default(),
            db,
            diagnostic_reporter.clone(),
        )
        .with_pool_registry(Arc::clone(&pool_registry));
        let workspace_name = WorkspaceName::default();
        let pool_registry_before_delete = pool_registry.for_workspace(&workspace_name);
        let source_name = SourceName::parse("github").expect("source name");
        diagnostic_reporter.report_source_load_failure(
            SourceLoadDiagnosticStage::Query,
            &workspace_name,
            &source_name,
            "test failure",
        );

        manager
            .delete_workspace(&workspace_name)
            .await
            .expect_err("default workspace deletion should fail");

        assert!(diagnostic_reporter.tracks_diagnostic(
            &workspace_name,
            &source_name,
            "query-source",
            "test failure",
        ));
        let pool_registry_after_delete = pool_registry.for_workspace(&workspace_name);
        assert!(Arc::ptr_eq(
            &pool_registry_before_delete,
            &pool_registry_after_delete
        ));
    }
}
