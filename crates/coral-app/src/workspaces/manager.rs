use std::path::PathBuf;
use std::sync::Arc;

use tracing::warn;

use crate::bootstrap::AppError;
use crate::credentials::{CredentialManager, CredentialSetId};
use crate::identity::{Principal, PrincipalKind};
use crate::sources::materialization::SourceDiagnosticReporter;
use crate::state::ConfigStore;
use crate::state::db::{
    AddMemberOutcome, CoralDb, DbRepos, RemoveMemberOutcome, WorkspaceCreationOutcome,
    WorkspaceMemberView, now_unix_nanos_i64,
};
use crate::storage::fs::DirectoryBackup;
use crate::workspaces::{
    DeletedWorkspace, LocalPrincipalPolicy, MemberRole, WorkspaceLifecycleLock,
    WorkspaceLifecycleRevision, WorkspaceName, WorkspacePaths, WorkspacePoolRegistry,
    WorkspaceRecord,
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
    local_principal: LocalPrincipalPolicy,
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
            local_principal: LocalPrincipalPolicy::default(),
        }
    }

    pub(crate) fn with_pool_registry(mut self, pool_registry: Arc<WorkspacePoolRegistry>) -> Self {
        self.pool_registry = pool_registry;
        self
    }

    /// Treats the local principal as owner of every workspace.
    ///
    /// Only a state directory without `[auth]` may be served this way; the
    /// default conceals workspaces the caller holds no membership in.
    pub(crate) fn trusting_local_principal(mut self) -> Self {
        self.local_principal = LocalPrincipalPolicy::ImplicitOwner;
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

    pub(crate) async fn list_workspaces_for(
        &self,
        principal: &Principal,
    ) -> Result<Vec<(WorkspaceRecord, MemberRole)>, AppError> {
        if principal.is_local() && self.local_principal == LocalPrincipalPolicy::ImplicitOwner {
            return Ok(self
                .list_workspaces()
                .await?
                .into_iter()
                .map(|workspace| (workspace, MemberRole::Owner))
                .collect());
        }
        let mut session = self.db.as_ref();
        session
            .workspace_members()
            .workspaces_for_user_id(principal.id().as_str())
            .await?
            .into_iter()
            .map(|(workspace_id, role)| {
                WorkspaceName::parse(&workspace_id)
                    .map(|name| (WorkspaceRecord { name }, role))
                    .map_err(|error| {
                        AppError::Database(format!(
                            "invalid workspace id '{workspace_id}': {error}"
                        ))
                    })
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

    /// Creates a workspace with no membership rows.
    ///
    /// Every production path records a creator as owner, because a workspace
    /// nobody owns is unreachable. Tests use this to build the state an
    /// upgraded deployment starts from.
    #[cfg(test)]
    pub(crate) async fn create_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<WorkspaceRecord, AppError> {
        reject_reserved_personal_default(workspace_name)?;
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

    pub(crate) async fn create_workspace_for_user(
        &self,
        workspace_name: &WorkspaceName,
        principal: &Principal,
    ) -> Result<WorkspaceRecord, AppError> {
        if principal.is_local() && self.local_principal == LocalPrincipalPolicy::Ordinary {
            return Err(AppError::PermissionDenied(
                "the local principal cannot create workspaces in a shared deployment".to_string(),
            ));
        }
        reject_reserved_personal_default(workspace_name)?;
        if principal.kind() != PrincipalKind::User {
            return Err(AppError::PermissionDenied(
                "workspace creation requires a human principal".to_string(),
            ));
        }
        let _lifecycle_guard = self.lifecycle_lock.lock_async().await;
        match self
            .db
            .create_workspace_with_owner(
                workspace_name.as_str(),
                principal.id().as_str(),
                now_unix_nanos_i64()?,
            )
            .await?
        {
            WorkspaceCreationOutcome::Created => Ok(WorkspaceRecord {
                name: workspace_name.clone(),
            }),
            WorkspaceCreationOutcome::AlreadyExists => {
                Err(AppError::WorkspaceAlreadyExists(workspace_name.to_string()))
            }
            WorkspaceCreationOutcome::UserNotFound => {
                Err(AppError::UserNotFound(principal.id().to_string()))
            }
        }
    }

    pub(crate) async fn list_workspace_members(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<WorkspaceMemberView>, AppError> {
        self.db
            .list_workspace_members(workspace_name.as_str())
            .await?
            .ok_or_else(|| AppError::WorkspaceNotFound(workspace_name.to_string()))
    }

    pub(crate) async fn add_workspace_member(
        &self,
        workspace_name: &WorkspaceName,
        user_id: &str,
        role: MemberRole,
    ) -> Result<WorkspaceMemberView, AppError> {
        let mut session = self.db.as_ref();
        match session
            .workspaces()
            .add_member(
                workspace_name.as_str(),
                user_id,
                role,
                now_unix_nanos_i64()?,
            )
            .await?
        {
            AddMemberOutcome::Added(member)
            | AddMemberOutcome::ExistingSameRole(member)
            | AddMemberOutcome::RoleUpdated(member) => Ok(member),
            AddMemberOutcome::LastOwnerProtected => {
                Err(AppError::LastWorkspaceOwner(workspace_name.to_string()))
            }
            AddMemberOutcome::WorkspaceNotFound => {
                Err(AppError::WorkspaceNotFound(workspace_name.to_string()))
            }
            AddMemberOutcome::UserNotFound => Err(AppError::UserNotFound(user_id.to_string())),
        }
    }

    pub(crate) async fn remove_workspace_member(
        &self,
        workspace_name: &WorkspaceName,
        user_id: &str,
    ) -> Result<(), AppError> {
        let mut session = self.db.as_ref();
        match session
            .workspaces()
            .remove_member(workspace_name.as_str(), user_id)
            .await?
        {
            RemoveMemberOutcome::Removed => Ok(()),
            RemoveMemberOutcome::WorkspaceNotFound => {
                Err(AppError::WorkspaceNotFound(workspace_name.to_string()))
            }
            RemoveMemberOutcome::MemberNotFound => Err(AppError::UserNotFound(user_id.to_string())),
            RemoveMemberOutcome::LastOwnerProtected => {
                Err(AppError::LastWorkspaceOwner(workspace_name.to_string()))
            }
        }
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

fn reject_reserved_personal_default(workspace_name: &WorkspaceName) -> Result<(), AppError> {
    if workspace_name.has_reserved_personal_default_prefix() {
        Err(AppError::InvalidInput(
            "workspace names beginning with 'default-' are reserved for personal default workspaces"
                .to_string(),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::WorkspaceManager;
    use crate::bootstrap::AppError;
    use crate::credentials::{CredentialManager, CredentialSetId, CredentialStore};
    use crate::identity::{Principal, PrincipalKind};
    use crate::sources::SourceName;
    use crate::sources::materialization::{SourceDiagnosticReporter, SourceLoadDiagnosticStage};
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig, UpsertLoginOutcome};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::{MemberRole, WorkspaceName, WorkspacePoolRegistry};

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

    fn test_manager(layout: &AppStateLayout, db: Arc<CoralDb>) -> WorkspaceManager {
        WorkspaceManager::new_for_tests(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout.clone(),
            None,
            db,
        )
    }

    async fn provision_user(db: &CoralDb, subject: &str) -> String {
        let UpsertLoginOutcome::Upserted(user) = db
            .upsert_user_and_ensure_default_workspace("issuer", subject, None, 1)
            .await
            .expect("provision user")
        else {
            panic!("new subject should create a user")
        };
        user.user_id
    }

    #[tokio::test]
    async fn reserved_personal_default_prefix_is_rejected_for_user_creation() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        let db = test_db(&layout).await;
        let user_id = provision_user(&db, "workspace-creator").await;
        let principal = Principal::parse(&user_id, PrincipalKind::User).expect("principal");
        let manager = test_manager(&layout, Arc::clone(&db));
        let reserved = WorkspaceName::parse(" default-persisted-id ")
            .expect("persisted personal default name remains parseable");

        assert!(matches!(
            manager
                .create_workspace_for_user(&reserved, &principal)
                .await,
            Err(AppError::InvalidInput(ref message))
                if message == "workspace names beginning with 'default-' are reserved for personal default workspaces"
        ));
        assert!(matches!(
            manager.create_workspace(&reserved).await,
            Err(AppError::InvalidInput(_))
        ));

        let default = WorkspaceName::default();
        manager
            .create_workspace_for_user(&default, &principal)
            .await
            .expect("the exact default workspace name is not reserved by the prefix rule");
        let members = manager
            .list_workspace_members(&default)
            .await
            .expect("list exact default members");
        assert_eq!(
            members
                .first()
                .expect("exact default workspace should have an owner")
                .role,
            MemberRole::Owner
        );
    }

    #[tokio::test]
    async fn membership_lifecycle_filters_listing_and_preserves_the_last_owner() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        let db = test_db(&layout).await;
        let owner_id = provision_user(&db, "owner").await;
        let member_id = provision_user(&db, "member").await;
        let owner = Principal::parse(&owner_id, PrincipalKind::User).expect("owner principal");
        let member = Principal::parse(&member_id, PrincipalKind::User).expect("member principal");
        let manager = test_manager(&layout, db);
        let workspace = WorkspaceName::parse("team").expect("workspace");

        manager
            .create_workspace_for_user(&workspace, &owner)
            .await
            .expect("create owned workspace");
        assert!(
            manager
                .list_workspaces_for(&member)
                .await
                .expect("list member workspaces")
                .iter()
                .all(|(record, _)| record.name != workspace)
        );

        let first = manager
            .add_workspace_member(&workspace, &member_id, MemberRole::Member)
            .await
            .expect("add member");
        let repeated = manager
            .add_workspace_member(&workspace, &member_id, MemberRole::Member)
            .await
            .expect("repeat identical add");
        assert_eq!(first, repeated);

        // Adding someone who is already a member states the role they should
        // hold, so it moves them to it.
        let promoted = manager
            .add_workspace_member(&workspace, &member_id, MemberRole::Owner)
            .await
            .expect("adding an existing member with a new role promotes them");
        assert_eq!(promoted.role, MemberRole::Owner);
        let demoted = manager
            .add_workspace_member(&workspace, &member_id, MemberRole::Member)
            .await
            .expect("and moves them back");
        assert_eq!(demoted.role, MemberRole::Member);

        // The one move that cannot be honored leaves nobody in charge.
        assert!(matches!(
            manager
                .add_workspace_member(&workspace, &owner_id, MemberRole::Member)
                .await,
            Err(AppError::LastWorkspaceOwner(_))
        ));

        let members = manager
            .list_workspace_members(&workspace)
            .await
            .expect("list members");
        assert_eq!(members.len(), 2);
        assert!(
            members
                .iter()
                .any(|entry| { entry.user_id == owner_id && entry.role == MemberRole::Owner })
        );
        assert!(
            manager
                .list_workspaces_for(&member)
                .await
                .expect("list newly shared workspace")
                .iter()
                .any(|(record, role)| record.name == workspace && *role == MemberRole::Member)
        );

        manager
            .remove_workspace_member(&workspace, &member_id)
            .await
            .expect("remove member");
        assert!(
            manager
                .list_workspace_members(&workspace)
                .await
                .expect("list after removal")
                .iter()
                .all(|entry| entry.user_id != member_id)
        );
        assert!(matches!(
            manager
                .remove_workspace_member(&workspace, &owner_id)
                .await,
            Err(AppError::LastWorkspaceOwner(ref name)) if name == workspace.as_str()
        ));
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
