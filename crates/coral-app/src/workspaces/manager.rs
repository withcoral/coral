use std::path::PathBuf;
use std::sync::Arc;

use tracing::warn;

use crate::bootstrap::AppError;
use crate::credentials::{CredentialManager, CredentialSetId};
use crate::identity::Principal;
use crate::sources::materialization::SourceDiagnosticReporter;
use crate::sources::model::InstalledSource;
use crate::state::db::{
    AddMemberOutcome, CoralDb, CreateWorkspaceOutcome, DbError, DbRepos, RemoveMemberOutcome,
    WorkspaceDeletion, WorkspaceMemberRecord, now_unix_nanos_i64,
};
use crate::state::{ConfigStore, RemovedWorkspaceConfig};
use crate::storage::fs::DirectoryBackup;
use crate::workspaces::{
    DeletedWorkspace, MemberRole, WorkspaceLifecycleLock, WorkspaceLifecycleRevision,
    WorkspaceMember, WorkspaceMembership, WorkspaceName, WorkspacePaths, WorkspacePoolRegistry,
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
    /// Makes the deletion transaction refuse to commit, so a test can drive
    /// the recovery that puts the removed config entries back.
    #[cfg(test)]
    deletion_commit_fails: bool,
    #[cfg(test)]
    config_restore_fails: bool,
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
            #[cfg(test)]
            deletion_commit_fails: false,
            #[cfg(test)]
            config_restore_fails: false,
        }
    }

    pub(crate) fn with_pool_registry(mut self, pool_registry: Arc<WorkspacePoolRegistry>) -> Self {
        self.pool_registry = pool_registry;
        self
    }

    #[cfg(test)]
    pub(crate) const fn with_failing_deletion_commit(mut self) -> Self {
        self.deletion_commit_fails = true;
        self
    }

    #[cfg(test)]
    pub(crate) const fn with_failing_config_restore(mut self) -> Self {
        self.config_restore_fails = true;
        self
    }

    pub(crate) async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, AppError> {
        let mut session = self.db.as_ref();
        session
            .workspaces()
            .list()
            .await?
            .into_iter()
            .map(|workspace| workspace_record(&workspace.id))
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
            // Global lock order: take the app state file lock before beginning a
            // database transaction, never the other way around. Source install
            // and removal take the same order, so inverting it here would let
            // two processes sharing one config directory deadlock across the
            // file lock and the database.
            let state_lock = self.config_store.state_lock_exclusive()?;
            // Captured before the deletion transaction removes the workspace row,
            // which cascades the source rows away. The exclusive state lock is
            // already held, so the catalog cannot change between this read and
            // the delete.
            let db_sources = {
                let mut session = self.db.as_ref();
                session
                    .sources()
                    .list_workspace_sources(workspace_name)
                    .await?
            };
            let Some(deletion) = self
                .db
                .begin_workspace_deletion(workspace_name.as_str())
                .await?
            else {
                return Err(AppError::WorkspaceNotFound(workspace_name.to_string()));
            };
            let removed = match self
                .config_store
                .remove_workspace_config_entries_unlocked(workspace_name)
            {
                Ok(removed) => removed,
                Err(error) => {
                    Self::roll_back_workspace_deletion(workspace_name, deletion).await;
                    return Err(error);
                }
            };
            if let Err(commit_error) = self.commit_workspace_deletion(deletion).await {
                // The transaction rolled back, so the workspace is still in
                // the catalog while its config entries are already gone. The
                // removal is durable and carries everything it took away, so
                // put it back rather than leave the two disagreeing.
                if let Err(restore_error) = self.restore_workspace_config_entries(removed) {
                    // Both halves failed, which is the split this restore
                    // exists to prevent. Report it instead of the commit
                    // failure alone, so the caller knows the deployment needs
                    // attention rather than a retry.
                    return Err(AppError::Internal(format!(
                        "deleting workspace '{workspace_name}' failed to commit ({commit_error}), \
                         and its config entries could not be put back ({restore_error}); the \
                         workspace is still in the catalog while its sources and functions are \
                         missing from config.toml"
                    )));
                }
                return Err(commit_error.into());
            }
            let deleted = removed.map_or_else(
                || DeletedWorkspace {
                    workspace: WorkspaceRecord {
                        name: workspace_name.clone(),
                    },
                    sources: Vec::new(),
                },
                RemovedWorkspaceConfig::into_deleted_workspace,
            );
            let deleted = Self::merge_deleted_sources(deleted, db_sources);
            self.pool_registry.remove(workspace_name);
            // Credential cleanup re-acquires the state lock through the file
            // credential backend, and `flock` conflicts per open file
            // description, so the guard must be released first.
            drop(state_lock);
            self.remove_deleted_workspace_credentials(&deleted);
            // Staging comes last, and failing to move the directory only
            // warns. Everything above it is already durable, so an abort here
            // would report a failure for a deletion that happened. The cost is
            // an accepted one: a directory left in the workspaces root carries
            // no evidence that it was deleted, so a later scan — the legacy
            // catalog cutover — reads it as a live workspace and resurrects it.
            // Staging before the commit would close that, but a crash between
            // the rename and the commit would then leave a live workspace with
            // no directory at all, which is worse.
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

    fn merge_deleted_sources(
        mut deleted: DeletedWorkspace,
        db_sources: Vec<InstalledSource>,
    ) -> DeletedWorkspace {
        for source in db_sources {
            if !deleted
                .sources
                .iter()
                .any(|existing| existing.name == source.name)
            {
                deleted.sources.push(source);
            }
        }
        deleted
    }

    /// Erases the credential material of a workspace that has just been deleted.
    ///
    /// Runs while the workspace directory is still in place, because staging
    /// it aside is the step after this one: file-backed material is where the
    /// live layout says it is, and keychain material lives outside the
    /// directory either way.
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

    async fn roll_back_workspace_deletion(
        workspace_name: &WorkspaceName,
        deletion: WorkspaceDeletion<'_>,
    ) {
        if let Err(rollback_error) = deletion.rollback().await {
            warn!(
                workspace = %workspace_name,
                "workspace deletion was aborted, and the database rollback also failed: {rollback_error}"
            );
        }
    }

    /// Commits the deletion transaction.
    ///
    /// A seam rather than a call at the one site that needs it, because
    /// nothing a test can do to a real transaction makes its commit fail on
    /// demand, and the recovery that failure drives — putting the config
    /// entries back — is the part worth pinning.
    async fn commit_workspace_deletion(
        &self,
        deletion: WorkspaceDeletion<'_>,
    ) -> Result<(), DbError> {
        #[cfg(test)]
        if self.deletion_commit_fails {
            deletion.rollback().await?;
            return Err(DbError::Config(
                "workspace deletion commit failed for tests".to_string(),
            ));
        }
        deletion.commit().await
    }

    /// Undoes the config removal of a deletion that could not go through.
    ///
    /// Best effort, like every other recovery here: a restore that fails
    /// leaves the divergence it was meant to close, and saying so is all that
    /// is left to do about it.
    /// Puts back what the aborted deletion removed from `config.toml`.
    ///
    /// The failure is returned rather than logged: it is the one that leaves
    /// the database holding a workspace whose sources and functions are gone
    /// from the file, and a caller that only hears about the step which
    /// triggered the abort would never learn the two now disagree.
    fn restore_workspace_config_entries(
        &self,
        removed: Option<RemovedWorkspaceConfig>,
    ) -> Result<(), AppError> {
        let Some(removed) = removed else {
            return Ok(());
        };
        #[cfg(test)]
        if self.config_restore_fails {
            return Err(AppError::Internal(
                "workspace config restore failed for tests".to_string(),
            ));
        }
        self.config_store
            .restore_workspace_config_entries_unlocked(removed)
    }

    fn stage_deleted_workspace_dir(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Option<DirectoryBackup> {
        let workspace_dir = self.workspace_dir(workspace_name);
        match DirectoryBackup::move_for_delete_into(
            &workspace_dir,
            &self.paths.deleted_workspaces_root(),
            workspace_name,
        ) {
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

/// The creator-owned workspace and membership seam.
///
/// Ownership is explicit here and nowhere else: a workspace is created for the
/// caller who asked for it, and every later change to who may reach it goes
/// through one of these methods. Only creation takes the lifecycle lock, which
/// guards filesystem artifacts, because it makes them and must not race a
/// deletion removing them. The membership methods do not: their writes are
/// serialized by the workspace parent row inside the transaction instead, so
/// granting or revoking access cannot stall a query.
impl WorkspaceManager {
    /// Creates one workspace owned by `creator_user_id`.
    ///
    /// The name has already been validated into a [`WorkspaceName`], and no
    /// name is special: `default` and every `default-*` shape take this path
    /// exactly as any other caller-chosen name does.
    pub(crate) async fn create_workspace_for_user(
        &self,
        workspace_name: &WorkspaceName,
        creator_user_id: &str,
    ) -> Result<WorkspaceRecord, AppError> {
        let _lifecycle_guard = self.lifecycle_lock.lock_async().await;
        let created = self
            .db
            .workspace_state()
            .create_owned_by(
                workspace_name.as_str(),
                creator_user_id,
                now_unix_nanos_i64()?,
            )
            .await?;
        match created {
            CreateWorkspaceOutcome::Created => Ok(WorkspaceRecord {
                name: workspace_name.clone(),
            }),
            CreateWorkspaceOutcome::AlreadyExists => {
                Err(AppError::WorkspaceAlreadyExists(workspace_name.to_string()))
            }
            CreateWorkspaceOutcome::CreatorNotFound => {
                Err(AppError::UserNotFound(creator_user_id.to_string()))
            }
        }
    }

    /// Lists what `principal` holds, which is not always what the membership
    /// rows say.
    ///
    /// A deployment that admits the local principal treats it as owner of
    /// everything, and it holds no membership rows a listing could be read
    /// from for the workspaces it never created. Which of the two readings
    /// applies is a property of the principal and the deployment, so it is
    /// settled here rather than at the transport, where it would be one more
    /// thing every caller of this had to remember.
    pub(crate) async fn list_memberships_for_principal(
        &self,
        principal: &Principal,
    ) -> Result<Vec<WorkspaceMembership>, AppError> {
        if principal.is_local() {
            return Ok(self
                .list_workspaces()
                .await?
                .into_iter()
                .map(|workspace| WorkspaceMembership {
                    workspace,
                    role: MemberRole::Owner,
                })
                .collect());
        }
        self.list_memberships_for_user(principal.id().as_str())
            .await
    }

    /// Lists the workspaces `user_id` belongs to, with the role they hold.
    ///
    /// This is one user's own view rather than the deployment's: it is what
    /// that person may reach, while [`Self::list_workspaces`] stays the
    /// host-wide inventory that only the local principal and host-scoped work
    /// may read.
    pub(crate) async fn list_memberships_for_user(
        &self,
        user_id: &str,
    ) -> Result<Vec<WorkspaceMembership>, AppError> {
        let mut session = self.db.as_ref();
        session
            .workspace_members()
            .workspaces_for_user_id(user_id)
            .await?
            .into_iter()
            .map(|(workspace_id, role)| {
                Ok(WorkspaceMembership {
                    workspace: workspace_record(&workspace_id)?,
                    role,
                })
            })
            .collect()
    }

    /// Lists one workspace's members, ordered by user id.
    ///
    /// The counterpart of [`Self::list_memberships_for_user`]: that answers
    /// "which workspaces are mine", this answers "who is in this one". A
    /// workspace nobody may reach never gets here, because the caller's
    /// authority over it is settled before the manager is asked.
    ///
    /// A workspace that does not exist is indistinguishable from one with no
    /// members, and deliberately so: both report an empty roster, and the
    /// caller has already been told which workspaces they may know about.
    pub(crate) async fn list_workspace_members(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<WorkspaceMember>, AppError> {
        let mut session = self.db.as_ref();
        Ok(session
            .workspace_members()
            .members_of_workspace(workspace_name.as_str())
            .await?
            .into_iter()
            .map(|(user_id, role, display_name)| WorkspaceMember {
                user_id,
                display_name,
                role,
            })
            .collect())
    }

    /// Grants one membership, moving an existing one onto `role`.
    ///
    /// Granting a role somebody already holds succeeds without writing, so a
    /// retried invitation reads the same as the first one.
    pub(crate) async fn add_workspace_member(
        &self,
        workspace_name: &WorkspaceName,
        user_id: &str,
        role: MemberRole,
    ) -> Result<WorkspaceMember, AppError> {
        let added = self
            .db
            .workspace_state()
            .add_member(
                workspace_name.as_str(),
                user_id,
                role,
                now_unix_nanos_i64()?,
            )
            .await?;
        match added {
            AddMemberOutcome::Added(member)
            | AddMemberOutcome::ExistingSameRole(member)
            | AddMemberOutcome::RoleUpdated(member) => Ok(workspace_member(member)),
            AddMemberOutcome::LastOwnerProtected => {
                Err(AppError::LastWorkspaceOwner(workspace_name.to_string()))
            }
            AddMemberOutcome::WorkspaceNotFound => {
                Err(AppError::WorkspaceNotFound(workspace_name.to_string()))
            }
            AddMemberOutcome::UserNotFound => Err(AppError::UserNotFound(user_id.to_string())),
        }
    }

    /// Revokes one membership unless it is the workspace's last owner.
    ///
    /// Revoking a membership that is not there reports the same miss as an
    /// unknown user: either way the named person holds nothing to revoke.
    pub(crate) async fn remove_workspace_member(
        &self,
        workspace_name: &WorkspaceName,
        user_id: &str,
    ) -> Result<(), AppError> {
        let removed = self
            .db
            .workspace_state()
            .remove_member(workspace_name.as_str(), user_id)
            .await?;
        match removed {
            RemoveMemberOutcome::Removed => Ok(()),
            RemoveMemberOutcome::LastOwnerProtected => {
                Err(AppError::LastWorkspaceOwner(workspace_name.to_string()))
            }
            RemoveMemberOutcome::WorkspaceNotFound => {
                Err(AppError::WorkspaceNotFound(workspace_name.to_string()))
            }
            RemoveMemberOutcome::MemberNotFound => Err(AppError::UserNotFound(user_id.to_string())),
        }
    }
}

/// Reads one persisted workspace id back into its checked app-local identity.
///
/// A stored id that no longer parses is corrupt state rather than caller input,
/// so it surfaces as a database error instead of an invalid argument.
/// Projects one persisted membership row onto the domain model.
///
/// The row shape stays inside `state::db`; managers and services above it deal
/// in [`WorkspaceMember`], exactly as the directory does with `users::model`.
fn workspace_member(record: WorkspaceMemberRecord) -> WorkspaceMember {
    WorkspaceMember {
        user_id: record.user_id,
        display_name: record.display_name,
        role: record.role,
    }
}

fn workspace_record(workspace_id: &str) -> Result<WorkspaceRecord, AppError> {
    WorkspaceName::parse(workspace_id)
        .map(|name| WorkspaceRecord { name })
        .map_err(|error| {
            AppError::Database(format!("invalid workspace id '{workspace_id}': {error}"))
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::WorkspaceManager;
    use crate::bootstrap::AppError;
    use crate::credentials::{CredentialManager, CredentialSetId, CredentialStore};
    use crate::sources::SourceName;
    use crate::sources::materialization::{SourceDiagnosticReporter, SourceLoadDiagnosticStage};
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::db::{
        CoralDb, CreateWorkspaceOutcome, DatabaseConfig, DbRepos, LoginIdentity, LoginProvisioning,
        ResolvedDatabaseConfig,
    };
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::{
        MemberRole, WorkspaceMember, WorkspaceMembership, WorkspaceName, WorkspacePoolRegistry,
        WorkspaceRecord,
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

    /// Builds a manager over a fresh migrated database.
    ///
    /// The temporary directory is returned because the layout it backs must
    /// outlive the manager, not because these tests read it.
    async fn membership_manager() -> (TempDir, Arc<CoralDb>, WorkspaceManager) {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        let db = test_db(&layout).await;
        let manager = WorkspaceManager::new_for_tests(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout,
            None,
            Arc::clone(&db),
        );
        (temp, db, manager)
    }

    /// Provisions one directory user through the production login seam, so the
    /// `user_id` the manager is handed is the one a real login would carry.
    async fn seed_user(db: &CoralDb, subject: &str) -> String {
        let provisioned = db
            .user_state()
            .provision_login(LoginIdentity {
                issuer: "https://issuer.test/workspace-manager",
                subject,
                display_name: Some("Seeded User"),
                principal_claim: subject,
                now_unix_nanos: 1,
            })
            .await
            .expect("provision user");
        match provisioned {
            LoginProvisioning::Provisioned(user) => user.user_id,
            LoginProvisioning::IssuerMismatch { stored_issuer } => {
                panic!("expected a provisioned user, got a mismatch with issuer {stored_issuer}")
            }
        }
    }

    async fn role_for(
        db: &CoralDb,
        workspace: &WorkspaceName,
        user_id: &str,
    ) -> Option<MemberRole> {
        let mut session = db;
        session
            .workspace_members()
            .role_for_user_id(workspace.as_str(), user_id)
            .await
            .expect("read role")
    }

    fn workspace(name: &str) -> WorkspaceName {
        WorkspaceName::parse(name).expect("workspace name")
    }

    /// The roster entry `seed_user` produces for one person in one role.
    fn member_record(user_id: &str, role: MemberRole) -> WorkspaceMember {
        WorkspaceMember {
            user_id: user_id.to_string(),
            display_name: Some("Seeded User".to_string()),
            role,
        }
    }

    fn membership(name: &WorkspaceName, role: MemberRole) -> WorkspaceMembership {
        WorkspaceMembership {
            workspace: WorkspaceRecord { name: name.clone() },
            role,
        }
    }

    /// Every valid name takes one path. `default` and the `default-*` shapes
    /// are ordinary caller-chosen names, so a caller cannot reach a workspace
    /// they never created by guessing a reserved-looking one.
    #[tokio::test]
    async fn deleting_a_workspace_owns_every_valid_name_alike() {
        let (_temp, db, manager) = membership_manager().await;
        let creator = seed_user(&db, "creator").await;

        for name in [
            "default".to_string(),
            format!("default-{}", uuid::Uuid::new_v4()),
            "default-team".to_string(),
            "work".to_string(),
        ] {
            let workspace = workspace(&name);
            manager
                .create_workspace_for_user(&workspace, &creator)
                .await
                .expect("create workspace");

            assert_eq!(
                manager
                    .delete_workspace(&workspace)
                    .await
                    .unwrap_or_else(|error| panic!("'{name}' should be deletable: {error}"))
                    .name,
                workspace
            );
            assert!(
                matches!(
                    manager.delete_workspace(&workspace).await,
                    Err(AppError::WorkspaceNotFound(_))
                ),
                "'{name}' must be gone once deleted"
            );
            manager
                .create_workspace_for_user(&workspace, &creator)
                .await
                .unwrap_or_else(|error| panic!("'{name}' should be creatable again: {error}"));
        }
    }

    /// Both halves of the recovery can fail. When they do, the caller must
    /// hear that the two disagree rather than only that the commit failed,
    /// because the deployment needs attention and a retry will not give it.
    #[tokio::test]
    async fn a_deletion_that_cannot_put_its_config_back_reports_the_split() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        let store = ConfigStore::new(layout.clone());
        let db = test_db(&layout).await;
        let manager = WorkspaceManager::new_for_tests(
            store.clone(),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout,
            None,
            Arc::clone(&db),
        )
        .with_failing_deletion_commit()
        .with_failing_config_restore();
        let workspace_name = workspace("work");
        let owner = seed_user(&db, "owner").await;
        manager
            .create_workspace_for_user(&workspace_name, &owner)
            .await
            .expect("create workspace");
        store
            .upsert_source(&workspace_name, installed_source("github"))
            .expect("upsert source");

        let error = manager
            .delete_workspace(&workspace_name)
            .await
            .expect_err("a deletion that cannot restore its config must report the failure");

        let reported = error.to_string();
        assert!(
            reported.contains("could not be put back"),
            "the failure must name the unrestored config, not just the commit: {reported}"
        );
        assert!(
            reported.contains("missing from config.toml"),
            "the failure must say what state the deployment is left in: {reported}"
        );
    }

    /// The config removal is durable the moment it returns, while the
    /// transaction it accompanies is not. A commit that fails after it has to
    /// put the entries back, or the workspace stays in the catalog with its
    /// sources missing from `config.toml`.
    #[tokio::test]
    async fn a_deletion_whose_commit_fails_leaves_config_and_database_agreeing() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        let store = ConfigStore::new(layout.clone());
        let db = test_db(&layout).await;
        let manager = WorkspaceManager::new_for_tests(
            store.clone(),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout,
            None,
            Arc::clone(&db),
        )
        .with_failing_deletion_commit();
        let workspace_name = workspace("work");
        let owner = seed_user(&db, "owner").await;
        manager
            .create_workspace_for_user(&workspace_name, &owner)
            .await
            .expect("create workspace");
        store
            .upsert_source(&workspace_name, installed_source("github"))
            .expect("upsert source");

        manager
            .delete_workspace(&workspace_name)
            .await
            .expect_err("a deletion whose commit fails must report the failure");

        assert_eq!(
            manager.list_workspaces().await.expect("list workspaces"),
            vec![WorkspaceRecord {
                name: workspace_name.clone()
            }],
            "a failed commit leaves the workspace in the catalog"
        );
        assert!(
            store
                .get_source(
                    &workspace_name,
                    &SourceName::parse("github").expect("source")
                )
                .is_ok(),
            "a workspace still in the catalog keeps its sources in the config",
        );
    }

    #[tokio::test]
    async fn creating_a_workspace_owns_every_valid_name_alike() {
        let (_temp, db, manager) = membership_manager().await;
        let creator = seed_user(&db, "creator").await;

        for name in [
            "default".to_string(),
            format!("default-{}", uuid::Uuid::new_v4()),
            "default-team".to_string(),
            "work".to_string(),
        ] {
            let workspace = workspace(&name);
            let created = manager
                .create_workspace_for_user(&workspace, &creator)
                .await
                .expect("create workspace");

            assert_eq!(created.name, workspace);
            assert_eq!(
                role_for(&db, &workspace, &creator).await,
                Some(MemberRole::Owner),
                "'{name}' must belong to whoever created it"
            );
            assert!(
                matches!(
                    manager
                        .create_workspace_for_user(&workspace, &creator)
                        .await,
                    Err(AppError::WorkspaceAlreadyExists(_))
                ),
                "'{name}' must not be creatable twice"
            );
        }
    }

    #[tokio::test]
    async fn creating_a_workspace_for_an_unknown_user_leaves_nothing_behind() {
        let (_temp, _db, manager) = membership_manager().await;
        let workspace = workspace("unowned");

        assert!(matches!(
            manager
                .create_workspace_for_user(&workspace, "nobody")
                .await,
            Err(AppError::UserNotFound(_))
        ));
        assert!(
            manager.require_workspace(&workspace).await.is_err(),
            "a creation that cannot grant ownership must not leave a workspace nobody owns"
        );
    }

    #[tokio::test]
    async fn listing_memberships_answers_each_caller_for_themselves() {
        let (_temp, db, manager) = membership_manager().await;
        let owner = seed_user(&db, "owner").await;
        let member = seed_user(&db, "member").await;
        let solo = workspace("solo");
        let shared = workspace("shared");
        for name in [&solo, &shared] {
            manager
                .create_workspace_for_user(name, &owner)
                .await
                .expect("create workspace");
        }
        manager
            .add_workspace_member(&shared, &member, MemberRole::Member)
            .await
            .expect("grant membership");

        assert_eq!(
            manager
                .list_memberships_for_user(&owner)
                .await
                .expect("list the owner's memberships"),
            vec![
                membership(&shared, MemberRole::Owner),
                membership(&solo, MemberRole::Owner)
            ]
        );
        assert_eq!(
            manager
                .list_memberships_for_user(&member)
                .await
                .expect("list the member's memberships"),
            vec![membership(&shared, MemberRole::Member)],
            "a caller must see only the workspaces they belong to, with their own role"
        );
        assert_eq!(
            manager
                .list_workspaces()
                .await
                .expect("list workspaces")
                .len(),
            2,
            "the host-wide inventory stays caller-independent"
        );
    }

    /// The roster is the other half of the membership view, and it is one
    /// query: a workspace's whole membership arrives with each person's
    /// display name already attached, without a per-row directory lookup.
    #[tokio::test]
    async fn listing_a_workspace_roster_names_every_member_once() {
        let (_temp, db, manager) = membership_manager().await;
        let owner = seed_user(&db, "owner").await;
        let member = seed_user(&db, "member").await;
        let team = workspace("team");
        let other = workspace("other");
        manager
            .create_workspace_for_user(&team, &owner)
            .await
            .expect("create team");
        manager
            .create_workspace_for_user(&other, &member)
            .await
            .expect("create other");
        manager
            .add_workspace_member(&team, &member, MemberRole::Member)
            .await
            .expect("grant membership");

        let mut expected = vec![
            member_record(&owner, MemberRole::Owner),
            member_record(&member, MemberRole::Member),
        ];
        expected.sort_by(|left, right| left.user_id.cmp(&right.user_id));
        assert_eq!(
            manager
                .list_workspace_members(&team)
                .await
                .expect("list the team roster"),
            expected,
            "the roster must carry each member exactly once, ordered by user id"
        );
        assert_eq!(
            manager
                .list_workspace_members(&other)
                .await
                .expect("list the other roster"),
            vec![member_record(&member, MemberRole::Owner)],
            "a roster must not leak a membership held in another workspace"
        );

        // A promotion moves the person's row rather than adding a second one.
        manager
            .add_workspace_member(&team, &member, MemberRole::Owner)
            .await
            .expect("promote the member");
        assert_eq!(
            manager
                .list_workspace_members(&team)
                .await
                .expect("list the promoted roster")
                .into_iter()
                .filter(|found| found.user_id == member)
                .collect::<Vec<_>>(),
            vec![member_record(&member, MemberRole::Owner)],
        );

        assert_eq!(
            manager
                .list_workspace_members(&workspace("never-created"))
                .await
                .expect("a workspace nobody may know about lists nothing"),
            vec![],
        );
    }

    #[tokio::test]
    async fn membership_changes_are_idempotent_and_keep_one_owner() {
        let (_temp, db, manager) = membership_manager().await;
        let owner = seed_user(&db, "owner").await;
        let member = seed_user(&db, "member").await;
        let team = workspace("team");
        manager
            .create_workspace_for_user(&team, &owner)
            .await
            .expect("create workspace");

        let granted = manager
            .add_workspace_member(&team, &member, MemberRole::Member)
            .await
            .expect("grant membership");
        assert_eq!(granted.user_id, member);
        assert_eq!(granted.role, MemberRole::Member);
        assert_eq!(
            manager
                .add_workspace_member(&team, &member, MemberRole::Member)
                .await
                .expect("repeat the same grant")
                .role,
            MemberRole::Member,
            "re-granting a role somebody already holds must succeed"
        );

        // Both routes out of ownership answer to the same floor.
        assert!(matches!(
            manager.remove_workspace_member(&team, &owner).await,
            Err(AppError::LastWorkspaceOwner(_))
        ));
        assert!(matches!(
            manager
                .add_workspace_member(&team, &owner, MemberRole::Member)
                .await,
            Err(AppError::LastWorkspaceOwner(_))
        ));

        // Promoting the member lifts the floor, so the first owner may leave.
        manager
            .add_workspace_member(&team, &member, MemberRole::Owner)
            .await
            .expect("promote the member");
        manager
            .remove_workspace_member(&team, &owner)
            .await
            .expect("remove the demoted owner");
        assert_eq!(role_for(&db, &team, &owner).await, None);
        assert!(matches!(
            manager.remove_workspace_member(&team, &owner).await,
            Err(AppError::UserNotFound(_))
        ));
        assert!(matches!(
            manager
                .add_workspace_member(&team, "nobody", MemberRole::Member)
                .await,
            Err(AppError::UserNotFound(_))
        ));
        assert!(matches!(
            manager
                .add_workspace_member(&workspace("never-created"), &member, MemberRole::Member)
                .await,
            Err(AppError::WorkspaceNotFound(_))
        ));
    }

    /// Deleting a workspace takes its memberships with it, and the manager
    /// never sweeps them: the foreign key does. The directory rows stay, so a
    /// deleted workspace does not take its people's identities with it.
    #[tokio::test]
    async fn deleting_a_workspace_cascades_its_memberships() {
        let (_temp, db, manager) = membership_manager().await;
        let owner = seed_user(&db, "owner").await;
        let disposable = workspace("disposable");
        manager
            .create_workspace_for_user(&disposable, &owner)
            .await
            .expect("create workspace");
        assert_eq!(
            role_for(&db, &disposable, &owner).await,
            Some(MemberRole::Owner)
        );

        manager
            .delete_workspace(&disposable)
            .await
            .expect("delete workspace");

        assert_eq!(role_for(&db, &disposable, &owner).await, None);
        let mut session = db.as_ref();
        assert!(
            session
                .users()
                .get_by_user_id(&owner)
                .await
                .expect("read the directory row")
                .is_some(),
            "deleting a workspace must not delete its members' directory rows"
        );
    }

    /// Credential cleanup re-acquires the exclusive state lock, so `delete_workspace` must have
    /// released its own guard first. A regression there deadlocks the deletion inside a blocking
    /// `flock(2)` call, which no `tokio::time::timeout` can interrupt, so the scenario runs on a
    /// helper thread behind a wall-clock deadline and fails instead of hanging the suite.
    #[test]
    fn delete_workspace_commits_config_then_cleans_artifacts() {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("current-thread runtime");
            let name = runtime.block_on(delete_workspace_and_clean_artifacts());
            // The receiver is gone only when the deadline below already failed the test.
            drop(done_tx.send(name));
        });

        let name = done_rx
            .recv_timeout(std::time::Duration::from_secs(20))
            .expect("workspace deletion must release the state lock before credential cleanup");
        assert_eq!(name, "work");
    }

    async fn delete_workspace_and_clean_artifacts() -> String {
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
        let owner = seed_user(&db, "owner").await;

        manager
            .create_workspace_for_user(&workspace_name, &owner)
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
        assert!(matches!(
            store.get_source(&workspace_name, &source_name),
            Err(crate::bootstrap::AppError::SourceNotFound(_))
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

        deleted.name.to_string()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_workspace_waits_for_the_state_lock_before_opening_a_transaction() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        let store = ConfigStore::new(layout.clone());
        let credential_manager = CredentialManager::new(CredentialStore::new(layout.clone()));
        let db = test_db(&layout).await;
        let manager = WorkspaceManager::new(
            store.clone(),
            credential_manager,
            layout,
            None,
            crate::workspaces::WorkspaceLifecycleLock::default(),
            Arc::clone(&db),
            SourceDiagnosticReporter::default(),
        );
        let doomed_name = WorkspaceName::parse("work").expect("workspace");
        let other_name = WorkspaceName::parse("other").expect("workspace");
        let owner = seed_user(&db, "owner").await;
        manager
            .create_workspace_for_user(&doomed_name, &owner)
            .await
            .expect("create workspace");

        let state_lock = store.state_lock_exclusive().expect("hold state lock");
        let delete_manager = manager.clone();
        let delete =
            tokio::spawn(async move { delete_manager.delete_workspace(&doomed_name).await });
        // Give the deletion time to reach the state lock it cannot take yet.
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;

        // Bypass the lifecycle gate held by the deletion and probe the database
        // directly. The blocked deletion must not have opened a write
        // transaction before reaching the state lock.
        let created = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            db.workspace_state()
                .create_owned_by(other_name.as_str(), &owner, 1),
        )
        .await
        .expect("blocked deletion should not hold a database transaction")
        .expect("create unrelated workspace");
        assert_eq!(created, CreateWorkspaceOutcome::Created);

        drop(state_lock);
        delete
            .await
            .expect("deletion task")
            .expect("delete workspace");
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
        let workspace_name = WorkspaceName::parse("work").expect("workspace");
        let pool_registry_before_delete = pool_registry.for_workspace(&workspace_name);
        let source_name = SourceName::parse("github").expect("source name");
        diagnostic_reporter.report_source_load_failure(
            SourceLoadDiagnosticStage::Query,
            &workspace_name,
            &source_name,
            "test failure",
        );

        // The workspace was never created, so deletion fails before it can
        // touch any of the state a successful deletion sweeps.
        manager
            .delete_workspace(&workspace_name)
            .await
            .expect_err("deleting a workspace that was never created should fail");

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
