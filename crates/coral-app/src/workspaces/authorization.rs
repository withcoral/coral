#![cfg_attr(
    not(test),
    expect(dead_code, reason = "wired to service handlers in later milestones")
)]

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

/// Whether the built-in local principal owns every workspace.
///
/// A state directory with no `[auth]` section is a single-user deployment: the
/// host user is the deployment, and `coral:local` reaches everything, exactly as
/// it did before access control existed. Once `[auth]` is configured the
/// deployment is shared and has no superuser — a lockout is repaired out of band
/// with the admin tool, not through a privileged request path.
///
/// [`Ordinary`](Self::Ordinary) is the default so that forgetting to state the
/// policy denies rather than grants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum LocalPrincipalPolicy {
    #[default]
    Ordinary,
    ImplicitOwner,
}

#[derive(Clone)]
pub(crate) struct WorkspaceAuthorizer {
    db: Arc<CoralDb>,
    local_principal: LocalPrincipalPolicy,
}

impl WorkspaceAuthorizer {
    /// Authorizes every principal from its membership, with no exceptions.
    pub(crate) fn new(db: Arc<CoralDb>) -> Self {
        Self {
            db,
            local_principal: LocalPrincipalPolicy::Ordinary,
        }
    }

    /// Treats the local principal as owner of every workspace.
    ///
    /// Only a state directory without `[auth]` may be served this way.
    pub(crate) fn trusting_local_principal(db: Arc<CoralDb>) -> Self {
        Self {
            db,
            local_principal: LocalPrincipalPolicy::ImplicitOwner,
        }
    }

    /// Reports the policy so sibling surfaces resolve local access identically.
    pub(crate) fn local_principal_policy(&self) -> LocalPrincipalPolicy {
        self.local_principal
    }

    pub(crate) async fn authorize(
        &self,
        principal: &Principal,
        workspace: &WorkspaceName,
        action: WorkspaceAction,
    ) -> Result<(), AppError> {
        if principal.is_local() && self.local_principal == LocalPrincipalPolicy::ImplicitOwner {
            return Ok(());
        }
        if principal.kind() == PrincipalKind::Agent && action == WorkspaceAction::Manage {
            return Err(AppError::PermissionDenied(
                "agent principals cannot manage workspaces".to_string(),
            ));
        }

        let mut session = self.db.as_ref();
        let role = session
            .workspace_members()
            .role_for_user_id(workspace.as_str(), principal.id().as_str())
            .await?
            .ok_or_else(|| AppError::WorkspaceNotFound(workspace.to_string()))?;
        if role.allows(action) {
            Ok(())
        } else {
            Err(AppError::PermissionDenied(format!(
                "owner access is required for workspace '{workspace}'"
            )))
        }
    }

    pub(crate) async fn owned_workspace_page_for_federated_user(
        &self,
        principal: &Principal,
        after_workspace: Option<&WorkspaceName>,
        limit: usize,
    ) -> Result<Vec<WorkspaceName>, AppError> {
        if principal.is_local() || principal.kind() != PrincipalKind::User {
            return Err(AppError::PermissionDenied(
                "owned workspace enumeration requires a federated user principal".to_string(),
            ));
        }

        let mut session = self.db.as_ref();
        session
            .workspace_members()
            .owned_workspaces_for_user_id(
                principal.id().as_str(),
                after_workspace.map(WorkspaceName::as_str),
                limit,
            )
            .await?
            .into_iter()
            .map(|workspace| WorkspaceName::parse(&workspace))
            .collect()
    }

    pub(crate) async fn unrestricted_workspace_page_for_local_principal(
        &self,
        principal: &Principal,
        after_workspace: Option<&WorkspaceName>,
        limit: usize,
    ) -> Result<Vec<WorkspaceName>, AppError> {
        if !principal.is_local() {
            return Err(AppError::PermissionDenied(
                "unrestricted workspace enumeration requires the local principal".to_string(),
            ));
        }

        let mut session = self.db.as_ref();
        let mut workspaces = session
            .workspaces()
            .list()
            .await?
            .into_iter()
            .map(|record| WorkspaceName::parse(&record.id))
            .collect::<Result<Vec<_>, _>>()?;
        workspaces.sort();
        Ok(workspaces
            .into_iter()
            .filter(|workspace| match after_workspace {
                Some(after) => workspace > after,
                None => true,
            })
            .take(limit)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::{WorkspaceAction, WorkspaceAuthorizer};
    use crate::bootstrap::AppError;
    use crate::identity::{Principal, PrincipalKind};
    use crate::state::AppStateLayout;
    use crate::state::db::{
        AddMemberOutcome, CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig,
        UpsertLoginOutcome,
    };
    use crate::workspaces::{MemberRole, WorkspaceName};

    #[tokio::test]
    async fn trusted_local_principal_returns_without_database_work() {
        let (_temp, db) = database(false).await;
        let authorizer = WorkspaceAuthorizer::trusting_local_principal(db);

        authorizer
            .authorize(
                &Principal::local(),
                &WorkspaceName::parse("schema-is-not-migrated").expect("workspace"),
                WorkspaceAction::Manage,
            )
            .await
            .expect("a single-user deployment reaches every workspace");
    }

    /// The deployments this covers are the shared ones, where a host process
    /// holds no membership and so must be concealed like any other stranger.
    /// Repairing that is the admin tool's job, not a privileged request path.
    #[tokio::test]
    async fn untrusted_local_principal_is_concealed_like_any_non_member() {
        let (_temp, db) = database(true).await;
        let owner_id = provision_user(&db, "owner").await;
        let workspace =
            WorkspaceName::parse(&format!("default-{owner_id}")).expect("owner default workspace");
        let authorizer = WorkspaceAuthorizer::new(db);

        for action in [WorkspaceAction::Read, WorkspaceAction::Manage] {
            assert!(
                matches!(
                    authorizer
                        .authorize(&Principal::local(), &workspace, action)
                        .await,
                    Err(AppError::WorkspaceNotFound(_))
                ),
                "the local principal must not see a workspace it does not belong to"
            );
        }
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
    async fn owner_and_member_permissions_follow_the_action() {
        let (_temp, db) = database(true).await;
        let owner_id = provision_user(&db, "owner").await;
        let member_id = provision_user(&db, "member").await;
        let workspace =
            WorkspaceName::parse(&format!("default-{owner_id}")).expect("owner default workspace");
        let mut session = db.as_ref();
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
        let owner_id = provision_user(&db, "owner").await;
        let nonmember_id = provision_user(&db, "nonmember").await;
        let workspace =
            WorkspaceName::parse(&format!("default-{owner_id}")).expect("owner default workspace");
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
    async fn owned_workspace_pages_are_scoped_and_local_pages_are_unrestricted() {
        let (_temp, db) = database(true).await;
        let owner_id = directory_user(&db, "enumeration-owner").await;
        let empty_id = directory_user(&db, "enumeration-empty").await;
        let persisted_workspaces = [" z-local ", "A-owned", "b-member", "c-owned", "d-member"];
        let ordered_workspaces = ["A-owned", "b-member", "c-owned", "d-member", "z-local"];
        let roles = [
            MemberRole::Member,
            MemberRole::Owner,
            MemberRole::Member,
            MemberRole::Owner,
            MemberRole::Member,
        ];
        let mut tx = db.begin().await.expect("begin workspace setup");
        for (index, (workspace, role)) in persisted_workspaces.iter().zip(roles).enumerate() {
            tx.workspaces()
                .create(workspace, i64::try_from(index).expect("small index"))
                .await
                .expect("create workspace");
            assert!(
                tx.workspaces()
                    .hold_for_child_mutation(workspace)
                    .await
                    .expect("hold workspace")
            );
            tx.workspace_members()
                .insert(workspace, &owner_id, role, 10)
                .await
                .expect("insert workspace membership");
        }
        tx.commit().await.expect("commit workspace setup");

        let authorizer = WorkspaceAuthorizer::new(db);
        let owner = Principal::parse(&owner_id, PrincipalKind::User).expect("owner");
        let empty = Principal::parse(&empty_id, PrincipalKind::User).expect("empty user");
        let cursor = WorkspaceName::parse("A-owned").expect("cursor");
        assert_eq!(
            authorizer
                .owned_workspace_page_for_federated_user(&empty, None, 10)
                .await
                .expect("empty owned page"),
            Vec::<WorkspaceName>::new()
        );
        assert_eq!(
            authorizer
                .owned_workspace_page_for_federated_user(&owner, None, 1)
                .await
                .expect("first owned page"),
            vec![WorkspaceName::parse("A-owned").expect("workspace")]
        );
        assert_eq!(
            authorizer
                .owned_workspace_page_for_federated_user(&owner, Some(&cursor), 10)
                .await
                .expect("remaining owned page"),
            vec![WorkspaceName::parse("c-owned").expect("workspace")]
        );

        assert_eq!(
            authorizer
                .unrestricted_workspace_page_for_local_principal(&Principal::local(), None, 2)
                .await
                .expect("local workspace page")
                .iter()
                .map(WorkspaceName::as_str)
                .collect::<Vec<_>>(),
            &ordered_workspaces[..2]
        );
        let local_cursor = WorkspaceName::parse("b-member").expect("local cursor");
        assert_eq!(
            authorizer
                .unrestricted_workspace_page_for_local_principal(
                    &Principal::local(),
                    Some(&local_cursor),
                    10,
                )
                .await
                .expect("remaining local workspace page")
                .iter()
                .map(WorkspaceName::as_str)
                .collect::<Vec<_>>(),
            &ordered_workspaces[2..]
        );
        assert!(matches!(
            authorizer
                .owned_workspace_page_for_federated_user(&Principal::local(), None, 10)
                .await,
            Err(AppError::PermissionDenied(_))
        ));
        let agent = Principal::parse(&owner_id, PrincipalKind::Agent).expect("agent");
        assert!(matches!(
            authorizer
                .owned_workspace_page_for_federated_user(&agent, None, 10)
                .await,
            Err(AppError::PermissionDenied(_))
        ));
        assert!(matches!(
            authorizer
                .unrestricted_workspace_page_for_local_principal(&owner, None, 10)
                .await,
            Err(AppError::PermissionDenied(_))
        ));
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

    async fn directory_user(db: &CoralDb, subject: &str) -> String {
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
}
