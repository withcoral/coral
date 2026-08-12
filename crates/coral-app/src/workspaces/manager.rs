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

    /// Allows the built-in local principal to control every workspace.
    ///
    /// Only a single-user deployment may opt into this policy.
    pub(crate) fn trusting_local_principal(mut self) -> Self {
        self.local_principal = LocalPrincipalPolicy::ImplicitOwner;
        self
    }

    pub(crate) fn workspace_authorizer(&self) -> crate::workspaces::WorkspaceAuthorizer {
        if self.local_principal.is_implicit_owner() {
            crate::workspaces::WorkspaceAuthorizer::trusting_local_principal(Arc::clone(&self.db))
        } else {
            crate::workspaces::WorkspaceAuthorizer::new(Arc::clone(&self.db))
        }
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
        self.local_principal.validate_request_principal(principal)?;
        if principal.is_local() && self.local_principal.is_implicit_owner() {
            return Ok(self
                .list_workspaces()
                .await?
                .into_iter()
                .map(|workspace| (workspace, MemberRole::Owner))
                .collect());
        }
        let mut session = self.db.as_ref();
        let memberships = if self.local_principal == LocalPrincipalPolicy::NoLocalPrincipal {
            session
                .workspace_members()
                .workspaces_for_user_id_with_non_local_owner(principal.id().as_str())
                .await?
        } else {
            session
                .workspace_members()
                .workspaces_for_user_id(principal.id().as_str())
                .await?
        };
        memberships
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

    pub(crate) async fn create_workspace_for_user(
        &self,
        workspace_name: &WorkspaceName,
        principal: &Principal,
    ) -> Result<WorkspaceRecord, AppError> {
        self.local_principal.validate_request_principal(principal)?;
        if principal.kind() != PrincipalKind::User {
            return Err(AppError::PermissionDenied(
                "workspace creation requires a human principal".to_string(),
            ));
        }
        let _lifecycle_guard = self.lifecycle_lock.lock_async().await;
        let mut session = self.db.as_ref();
        let created_at = now_unix_nanos_i64()?;
        let outcome = if principal.is_local() && self.local_principal.is_implicit_owner() {
            session
                .workspaces()
                .create_with_local_owner(workspace_name.as_str(), created_at)
                .await?
        } else {
            session
                .workspaces()
                .create_with_owner(workspace_name.as_str(), principal.id().as_str(), created_at)
                .await?
        };
        match outcome {
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
            AddMemberOutcome::Added(member) | AddMemberOutcome::ExistingSameRole(member) => {
                Ok(member)
            }
            AddMemberOutcome::RoleConflict => Err(AppError::WorkspaceMemberRoleConflict {
                workspace: workspace_name.to_string(),
                user_id: user_id.to_string(),
            }),
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use coral_api::v1::workspace_service_server::WorkspaceService as _;
    use coral_api::v1::{
        AddWorkspaceMemberRequest, CreateWorkspaceRequest, ListWorkspaceMembersRequest,
        ListWorkspacesRequest, RemoveWorkspaceMemberRequest, Workspace, WorkspaceMember,
        WorkspaceRole,
    };
    use tempfile::TempDir;
    use tonic::{Code, Request};

    use super::WorkspaceManager;
    use crate::bootstrap::AppError;
    use crate::credentials::{CredentialManager, CredentialSetId, CredentialStore};
    use crate::identity::{Principal, PrincipalKind};
    use crate::request_context::RequestContext;
    use crate::sources::SourceName;
    use crate::sources::materialization::{SourceDiagnosticReporter, SourceLoadDiagnosticStage};
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, UpsertLoginOutcome,
    };
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::{
        LocalPrincipalPolicy, MemberRole, WorkspaceName, WorkspacePoolRegistry, WorkspaceService,
    };

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
        let db = unmigrated_test_db(layout).await;
        db.migrate().await.expect("migrate sqlite");
        db
    }

    async fn unmigrated_test_db(layout: &AppStateLayout) -> Arc<CoralDb> {
        let config = DatabaseConfig::load(layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
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

    async fn create_directory_user(db: &CoralDb, subject: &str) -> String {
        let mut session = db;
        let UpsertLoginOutcome::Upserted(user) = session
            .users()
            .upsert_login("issuer", subject, None, 1)
            .await
            .expect("create directory user")
        else {
            panic!("new subject should create a user")
        };
        user.user_id
    }

    fn request<T>(message: T, principal: &Principal) -> Request<T> {
        let mut request = Request::new(message);
        request
            .extensions_mut()
            .insert(RequestContext::new(principal.clone()));
        request
    }

    fn workspace(name: &str) -> Workspace {
        Workspace { name: name.into() }
    }

    async fn list_members(
        service: &WorkspaceService,
        principal: &Principal,
    ) -> Vec<WorkspaceMember> {
        service
            .list_workspace_members(request(
                ListWorkspaceMembersRequest {
                    workspace: Some(workspace("team")),
                },
                principal,
            ))
            .await
            .expect("list members")
            .into_inner()
            .members
    }

    async fn add_member(
        service: &WorkspaceService,
        principal: &Principal,
        member: Option<WorkspaceMember>,
    ) -> Result<WorkspaceMember, tonic::Status> {
        service
            .add_workspace_member(request(
                AddWorkspaceMemberRequest {
                    workspace: Some(workspace("team")),
                    member,
                },
                principal,
            ))
            .await
            .map(|response| response.into_inner().member.expect("added member"))
    }

    #[tokio::test]
    async fn workspace_manager_applies_local_principal_policy() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        let db = unmigrated_test_db(&layout).await;
        let manager = test_manager(&layout, Arc::clone(&db));
        let workspace = WorkspaceName::parse("local-workspace").expect("workspace");

        assert!(matches!(
            manager.list_workspaces_for(&Principal::local()).await,
            Err(AppError::PermissionDenied(_))
        ));
        assert!(matches!(
            manager
                .create_workspace_for_user(&workspace, &Principal::local())
                .await,
            Err(AppError::PermissionDenied(_))
        ));
        db.migrate().await.expect("migrate sqlite");
        assert!(
            db.as_ref()
                .users()
                .get_by_user_id(crate::identity::LOCAL_PRINCIPAL_ID)
                .await
                .expect("read local user")
                .is_none()
        );
        let manager = test_manager(&layout, db).trusting_local_principal();
        manager
            .create_workspace_for_user(&workspace, &Principal::local())
            .await
            .expect("implicit owner creates workspace");
        assert!(matches!(
            manager.list_workspaces_for(&Principal::local()).await.as_deref(),
            Ok([(record, MemberRole::Owner)]) if record.name == workspace
        ));
    }

    #[tokio::test]
    async fn workspace_service_enforces_membership_lifecycle() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        let db = test_db(&layout).await;
        let owner_id = create_directory_user(&db, "owner").await;
        let member_id = create_directory_user(&db, "member").await;
        let owner = Principal::parse(&owner_id, PrincipalKind::User).expect("owner principal");
        let member = Principal::parse(&member_id, PrincipalKind::User).expect("member principal");
        let manager = test_manager(&layout, Arc::clone(&db));
        let service = WorkspaceService::new(manager, LocalPrincipalPolicy::NoLocalPrincipal);

        for name in ["default", "default-user-id", "team"] {
            service
                .create_workspace(request(
                    CreateWorkspaceRequest {
                        workspace: Some(workspace(name)),
                    },
                    &owner,
                ))
                .await
                .expect("create ordinary default name");
        }
        let member_record = |role| WorkspaceMember {
            user_id: member_id.clone(),
            role,
            display_name: String::new(),
        };
        let member_role = WorkspaceRole::Member as i32;
        let added = add_member(&service, &owner, Some(member_record(member_role)))
            .await
            .expect("add member");
        assert_eq!(
            (added.user_id, added.role),
            (member_id.clone(), member_role)
        );
        let error = add_member(
            &service,
            &owner,
            Some(member_record(WorkspaceRole::Owner as i32)),
        )
        .await
        .expect_err("changing a member role must fail");
        assert_eq!(error.code(), Code::AlreadyExists);

        let mut tx = db.begin().await.expect("begin legacy workspace setup");
        tx.workspaces()
            .create("ownerless", 1)
            .await
            .expect("create ownerless workspace");
        tx.workspace_members()
            .insert("ownerless", &member_id, MemberRole::Member, 1)
            .await
            .expect("seed ownerless member");
        tx.commit().await.expect("commit ownerless workspace");
        db.as_ref()
            .workspaces()
            .create_with_local_owner("local-only", 1)
            .await
            .expect("create local-only workspace");
        db.as_ref()
            .workspaces()
            .add_member("local-only", &member_id, MemberRole::Member, 1)
            .await
            .expect("seed local-only member");

        let memberships = service
            .list_workspaces(request(ListWorkspacesRequest {}, &member))
            .await
            .expect("list member workspaces")
            .into_inner()
            .memberships;
        assert!(matches!(memberships.as_slice(), [membership]
            if membership.workspace.as_ref().is_some_and(|workspace| workspace.name == "team")
                && membership.role == WorkspaceRole::Member as i32));

        let members = list_members(&service, &owner).await;
        assert!(
            [WorkspaceRole::Owner, WorkspaceRole::Member]
                .into_iter()
                .all(|role| members.iter().any(|member| member.role == role as i32))
        );
        assert_eq!(
            service
                .list_workspace_members(request(
                    ListWorkspaceMembersRequest {
                        workspace: Some(workspace("team")),
                    },
                    &member,
                ))
                .await
                .expect_err("member cannot manage memberships")
                .code(),
            Code::PermissionDenied
        );

        for member in [
            None,
            Some(member_record(WorkspaceRole::Unspecified as i32)),
            Some(member_record(i32::MAX)),
        ] {
            let error = add_member(&service, &owner, member)
                .await
                .expect_err("adding an invalid member must fail");
            assert_eq!(error.code(), Code::InvalidArgument);
        }

        service
            .remove_workspace_member(request(
                RemoveWorkspaceMemberRequest {
                    workspace: Some(workspace("team")),
                    user_id: member_id.clone(),
                },
                &owner,
            ))
            .await
            .expect("remove member");
        let error = service
            .remove_workspace_member(request(
                RemoveWorkspaceMemberRequest {
                    workspace: Some(workspace("team")),
                    user_id: owner_id,
                },
                &owner,
            ))
            .await
            .expect_err("last owner removal must fail");
        assert_eq!(error.code(), Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn delete_workspace_commits_config_then_cleans_artifacts() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        let store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let db = test_db(&layout).await;
        let creator_id = create_directory_user(&db, "delete-workspace-owner").await;
        let creator = Principal::parse(&creator_id, PrincipalKind::User).expect("creator");
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
            .create_workspace_for_user(&workspace_name, &creator)
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
    async fn missing_workspace_delete_keeps_diagnostic_state() {
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
        let workspace_name = WorkspaceName::parse("missing").expect("workspace");
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
            .expect_err("missing workspace deletion should fail");

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
