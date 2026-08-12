#![cfg_attr(not(test), expect(dead_code, reason = "used higher in the PR stack"))]

use std::sync::Arc;

use crate::bootstrap::AppError;
use crate::identity::{Principal, PrincipalKind};
use crate::state::db::{CoralDb, DbRepos};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkspaceAction {
    Read,
    Manage,
}

/// Whether requests may use the built-in local principal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum LocalPrincipalPolicy {
    /// Reject the local principal before consulting user or membership state.
    #[default]
    NoLocalPrincipal,
    /// Treat the local principal as owner without consulting membership state.
    ImplicitOwner,
}

impl LocalPrincipalPolicy {
    /// Rejects a local request principal unless this deployment explicitly trusts it.
    pub(crate) fn validate_request_principal(self, principal: &Principal) -> Result<(), AppError> {
        if principal.is_local() && self == Self::NoLocalPrincipal {
            Err(AppError::PermissionDenied(
                "the local principal is not available under this server policy".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) const fn is_implicit_owner(self) -> bool {
        matches!(self, Self::ImplicitOwner)
    }
}

#[derive(Clone)]
pub(crate) struct WorkspaceAuthorizer {
    db: Arc<CoralDb>,
    local_principal: LocalPrincipalPolicy,
}

impl WorkspaceAuthorizer {
    /// Authorizes requests and rejects the local principal.
    pub(crate) fn new(db: Arc<CoralDb>) -> Self {
        Self {
            db,
            local_principal: LocalPrincipalPolicy::default(),
        }
    }

    /// Treats the local principal as owner of every workspace.
    ///
    /// Only a single-user deployment may opt into this policy.
    pub(crate) fn trusting_local_principal(db: Arc<CoralDb>) -> Self {
        Self {
            db,
            local_principal: LocalPrincipalPolicy::ImplicitOwner,
        }
    }

    pub(crate) const fn local_principal_policy(&self) -> LocalPrincipalPolicy {
        self.local_principal
    }

    pub(crate) async fn authorize(
        &self,
        principal: &Principal,
        workspace: &WorkspaceName,
        action: WorkspaceAction,
    ) -> Result<(), AppError> {
        self.local_principal.validate_request_principal(principal)?;
        if principal.is_local() {
            return Ok(());
        }
        if principal.kind() == PrincipalKind::Agent && action == WorkspaceAction::Manage {
            return Err(AppError::PermissionDenied(
                "agent principals cannot manage workspaces".to_string(),
            ));
        }

        let mut session = self.db.as_ref();
        let role = if self.local_principal == LocalPrincipalPolicy::NoLocalPrincipal {
            session
                .workspace_members()
                .role_for_user_id_with_non_local_owner(workspace.as_str(), principal.id().as_str())
                .await?
        } else {
            session
                .workspace_members()
                .role_for_user_id(workspace.as_str(), principal.id().as_str())
                .await?
        }
        .ok_or_else(|| AppError::WorkspaceNotFound(workspace.to_string()))?;
        if role.allows(action) {
            Ok(())
        } else {
            Err(AppError::PermissionDenied(format!(
                "owner access is required for workspace '{workspace}'"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::{LocalPrincipalPolicy, WorkspaceAction, WorkspaceAuthorizer};
    use crate::bootstrap::AppError;
    use crate::identity::{Principal, PrincipalKind};
    use crate::state::AppStateLayout;
    use crate::state::db::{
        AddMemberOutcome, CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig,
        UpsertLoginOutcome, UserRecord,
    };
    use crate::workspaces::{MemberRole, WorkspaceName};

    #[tokio::test]
    async fn strict_policy_rejects_local_principal_before_database_lookup() {
        let (_temp, db) = database(false).await;
        let authorizer = WorkspaceAuthorizer::new(db);

        assert_eq!(
            authorizer.local_principal_policy(),
            LocalPrincipalPolicy::NoLocalPrincipal
        );
        assert!(matches!(
            authorizer
                .authorize(
                    &Principal::local(),
                    &WorkspaceName::parse("schema-is-not-migrated").expect("workspace"),
                    WorkspaceAction::Manage,
                )
                .await,
            Err(AppError::PermissionDenied(_))
        ));
    }

    #[tokio::test]
    async fn implicit_owner_policy_bypasses_database_lookup() {
        let (_temp, db) = database(false).await;
        let authorizer = WorkspaceAuthorizer::trusting_local_principal(db);

        assert_eq!(
            authorizer.local_principal_policy(),
            LocalPrincipalPolicy::ImplicitOwner
        );
        authorizer
            .authorize(
                &Principal::local(),
                &WorkspaceName::parse("schema-is-not-migrated").expect("workspace"),
                WorkspaceAction::Manage,
            )
            .await
            .expect("implicit owner bypasses membership lookup");
    }

    #[tokio::test]
    async fn agent_manage_is_denied_before_membership_lookup() {
        let (_temp, db) = database(false).await;
        let authorizer = WorkspaceAuthorizer::new(db);
        let agent = Principal::parse("agent-without-a-row", PrincipalKind::Agent).expect("agent");

        assert!(matches!(
            authorizer
                .authorize(
                    &agent,
                    &WorkspaceName::parse("schema-is-not-migrated").expect("workspace"),
                    WorkspaceAction::Manage,
                )
                .await,
            Err(AppError::PermissionDenied(_))
        ));
    }

    #[tokio::test]
    async fn owner_manages_while_member_only_reads() {
        let (_temp, db) = database(true).await;
        let owner_id = create_directory_user(&db, "owner").await;
        let member_id = create_directory_user(&db, "member").await;
        let workspace = WorkspaceName::parse("team-access").expect("workspace");
        let mut session = db.as_ref();
        session
            .workspaces()
            .create_with_owner(workspace.as_str(), &owner_id, 1)
            .await
            .expect("create owner workspace");
        assert!(matches!(
            session
                .workspaces()
                .add_member(workspace.as_str(), &member_id, MemberRole::Member, 2)
                .await
                .expect("add member"),
            AddMemberOutcome::Added(_)
        ));
        let authorizer = WorkspaceAuthorizer::new(db);
        let owner = Principal::parse(&owner_id, PrincipalKind::User).expect("owner");
        let member = Principal::parse(&member_id, PrincipalKind::User).expect("member");

        for action in [WorkspaceAction::Read, WorkspaceAction::Manage] {
            authorizer
                .authorize(&owner, &workspace, action)
                .await
                .expect("owner is authorized");
        }
        authorizer
            .authorize(&member, &workspace, WorkspaceAction::Read)
            .await
            .expect("member can read");
        assert!(matches!(
            authorizer
                .authorize(&member, &workspace, WorkspaceAction::Manage)
                .await,
            Err(AppError::PermissionDenied(_))
        ));

        let member_agent = Principal::parse(&member_id, PrincipalKind::Agent).expect("agent");
        authorizer
            .authorize(&member_agent, &workspace, WorkspaceAction::Read)
            .await
            .expect("member agent can read");
    }

    #[tokio::test]
    async fn missing_membership_is_concealed_as_workspace_not_found() {
        let (_temp, db) = database(true).await;
        let owner_id = create_directory_user(&db, "owner").await;
        let nonmember_id = create_directory_user(&db, "nonmember").await;
        let workspace = WorkspaceName::parse("concealed-workspace").expect("workspace");
        let mut session = db.as_ref();
        session
            .workspaces()
            .create_with_owner(workspace.as_str(), &owner_id, 1)
            .await
            .expect("create owner workspace");
        let authorizer = WorkspaceAuthorizer::new(db);
        let nonmember = Principal::parse(&nonmember_id, PrincipalKind::User).expect("nonmember");

        assert!(matches!(
            authorizer
                .authorize(&nonmember, &workspace, WorkspaceAction::Read)
                .await,
            Err(AppError::WorkspaceNotFound(ref name)) if name == workspace.as_str()
        ));
    }

    #[tokio::test]
    async fn strict_policy_conceals_ownerless_workspace_from_member() {
        let (_temp, db) = database(true).await;
        let member_id = create_directory_user(&db, "ownerless-member").await;
        let workspace = WorkspaceName::parse("ownerless-workspace").expect("workspace");
        let mut tx = db.begin().await.expect("begin ownerless workspace setup");
        tx.workspaces()
            .create(workspace.as_str(), 1)
            .await
            .expect("create ownerless workspace");
        assert!(
            tx.workspaces()
                .hold_for_child_mutation(workspace.as_str())
                .await
                .expect("hold ownerless workspace")
        );
        tx.workspace_members()
            .insert(workspace.as_str(), &member_id, MemberRole::Member, 2)
            .await
            .expect("insert stale member");
        tx.commit().await.expect("commit ownerless workspace setup");
        let authorizer = WorkspaceAuthorizer::new(db);
        let member = Principal::parse(&member_id, PrincipalKind::User).expect("member");

        for action in [WorkspaceAction::Read, WorkspaceAction::Manage] {
            assert!(matches!(
                authorizer.authorize(&member, &workspace, action).await,
                Err(AppError::WorkspaceNotFound(ref name)) if name == workspace.as_str()
            ));
        }
    }

    #[tokio::test]
    async fn strict_policy_conceals_local_only_workspace_from_member() {
        let (_temp, db) = database(true).await;
        let member_id = create_directory_user(&db, "local-only-member").await;
        let workspace = WorkspaceName::parse("local-only-workspace").expect("workspace");
        let mut tx = db.begin().await.expect("begin local-only workspace setup");
        tx.users()
            .insert_for_test(&UserRecord {
                user_id: crate::identity::LOCAL_PRINCIPAL_ID.to_string(),
                issuer: crate::identity::LOCAL_PRINCIPAL_ID.to_string(),
                subject: String::new(),
                display_name: Some("Local".to_string()),
                created_at_unix_nanos: 1,
                last_login_at_unix_nanos: 1,
            })
            .await
            .expect("insert local principal");
        tx.workspaces()
            .create(workspace.as_str(), 1)
            .await
            .expect("create local-only workspace");
        assert!(
            tx.workspaces()
                .hold_for_child_mutation(workspace.as_str())
                .await
                .expect("hold local-only workspace")
        );
        tx.workspace_members()
            .insert(
                workspace.as_str(),
                crate::identity::LOCAL_PRINCIPAL_ID,
                MemberRole::Owner,
                2,
            )
            .await
            .expect("insert local owner");
        tx.workspace_members()
            .insert(workspace.as_str(), &member_id, MemberRole::Member, 3)
            .await
            .expect("insert stale member");
        tx.commit()
            .await
            .expect("commit local-only workspace setup");
        let authorizer = WorkspaceAuthorizer::new(db);
        let member = Principal::parse(&member_id, PrincipalKind::User).expect("member");

        for action in [WorkspaceAction::Read, WorkspaceAction::Manage] {
            assert!(matches!(
                authorizer.authorize(&member, &workspace, action).await,
                Err(AppError::WorkspaceNotFound(ref name)) if name == workspace.as_str()
            ));
        }
    }

    async fn database(migrate: bool) -> (TempDir, Arc<CoralDb>) {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("config") else {
            panic!("default test database must be SQLite")
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite"),
        );
        if migrate {
            db.migrate().await.expect("migrate");
        }
        (temp, db)
    }

    async fn create_directory_user(db: &CoralDb, subject: &str) -> String {
        let mut session = db;
        let UpsertLoginOutcome::Upserted(user) = session
            .users()
            .upsert_login("issuer", subject, None, 1)
            .await
            .expect("create directory user")
        else {
            panic!("new subject should create user")
        };
        user.user_id
    }
}
