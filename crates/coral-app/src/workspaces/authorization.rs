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

#[derive(Clone)]
pub(crate) struct WorkspaceAuthorizer {
    db: Arc<CoralDb>,
}

impl WorkspaceAuthorizer {
    pub(crate) fn new(db: Arc<CoralDb>) -> Self {
        Self { db }
    }

    pub(crate) async fn authorize(
        &self,
        principal: &Principal,
        workspace: &WorkspaceName,
        action: WorkspaceAction,
    ) -> Result<(), AppError> {
        if principal.is_local() {
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
        AddMemberOutcome, CoralDb, DatabaseConfig, ResolvedDatabaseConfig, UpsertLoginOutcome,
    };
    use crate::workspaces::{MemberRole, WorkspaceName};

    #[tokio::test]
    async fn local_principal_returns_without_database_work() {
        let (_temp, db) = database(false).await;
        let authorizer = WorkspaceAuthorizer::new(db);

        authorizer
            .authorize(
                &Principal::local(),
                &WorkspaceName::parse("schema-is-not-migrated").expect("workspace"),
                WorkspaceAction::Manage,
            )
            .await
            .expect("local principal bypasses membership storage");
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
        assert!(matches!(
            db.add_workspace_member(workspace.as_str(), &member_id, MemberRole::Member, 2)
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
            .provision_login("issuer", subject, None, 1)
            .await
            .expect("provision user")
        else {
            panic!("new subject should create a user")
        };
        user.user_id
    }
}
