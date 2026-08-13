//! Transactional workspace-membership mutations.

use sea_query::{Expr, ExprTrait, JoinType, Order, Query};

use super::schema::{Users, WorkspaceMembers};
use super::{CoralDb, DbError, DbRepos, DbSession};
use crate::workspaces::MemberRole;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceMemberView {
    pub(crate) user_id: String,
    pub(crate) role: MemberRole,
    pub(crate) display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AddMemberOutcome {
    Added(WorkspaceMemberView),
    ExistingSameRole(WorkspaceMemberView),
    RoleUpdated(WorkspaceMemberView),
    LastOwnerProtected,
    WorkspaceNotFound,
    UserNotFound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoveMemberOutcome {
    Removed,
    WorkspaceNotFound,
    MemberNotFound,
    LastOwnerProtected,
}

impl CoralDb {
    pub(crate) async fn list_workspace_members(
        &self,
        workspace_id: &str,
    ) -> Result<Option<Vec<WorkspaceMemberView>>, DbError> {
        let mut session = self;
        if session.workspaces().get(workspace_id).await?.is_none() {
            return Ok(None);
        }
        let statement = Query::select()
            .column((WorkspaceMembers::Table, WorkspaceMembers::UserId))
            .column((WorkspaceMembers::Table, WorkspaceMembers::Role))
            .column((Users::Table, Users::DisplayName))
            .from(WorkspaceMembers::Table)
            .join(
                JoinType::InnerJoin,
                Users::Table,
                Expr::col((WorkspaceMembers::Table, WorkspaceMembers::UserId))
                    .equals((Users::Table, Users::UserId)),
            )
            .and_where(Expr::col(WorkspaceMembers::WorkspaceId).eq(workspace_id))
            .order_by(
                (WorkspaceMembers::Table, WorkspaceMembers::UserId),
                Order::Asc,
            )
            .to_owned();
        let rows: Vec<(String, String, Option<String>)> = session.fetch_all(statement).await?;
        rows.into_iter()
            .map(|(user_id, role, display_name)| {
                let role = MemberRole::parse(&role).ok_or_else(|| {
                    DbError::CorruptData(format!("invalid workspace member role '{role}'"))
                })?;
                Ok(WorkspaceMemberView {
                    user_id,
                    role,
                    display_name,
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }
}
#[cfg(test)]
mod tests {
    use sea_query::{Expr, ExprTrait, Query};
    use tempfile::tempdir;

    use super::{AddMemberOutcome, RemoveMemberOutcome};
    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::DbSession;
    use crate::state::db::repositories::users::UpsertLoginOutcome;
    use crate::state::db::schema::WorkspaceMembers;
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, WorkspaceCreationOutcome,
    };
    use crate::workspaces::MemberRole;

    #[tokio::test]
    async fn workspace_member_state_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let db = open_sqlite(&temp).await;

        assert_workspace_member_state_lifecycle(&db, &uuid::Uuid::new_v4().to_string()).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared state harness against Postgres"]
    async fn workspace_member_state_repository_contract_on_postgres() {
        let Some(url) = postgres_test_url() else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_workspace_member_state_lifecycle(&db, &uuid::Uuid::new_v4().to_string()).await;
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the shared backend harness verifies one transactional membership lifecycle"
    )]
    async fn assert_workspace_member_state_lifecycle(db: &CoralDb, suffix: &str) {
        let owner_id = create_user(db, &format!("owner-{suffix}")).await;
        let member_id = create_user(db, &format!("member-{suffix}")).await;
        let conflicting_member_id = create_user(db, &format!("conflicting-member-{suffix}")).await;
        let other_owner_id = create_user(db, &format!("other-owner-{suffix}")).await;

        let identical_add_workspace_id = format!("identical-add-{suffix}");
        let mut session = db;
        assert_eq!(
            session
                .workspaces()
                .create_with_owner(&identical_add_workspace_id, &owner_id, 10)
                .await
                .expect("create owned workspace"),
            WorkspaceCreationOutcome::Created
        );
        assert_eq!(
            session
                .workspace_members()
                .role_for_user_id(&identical_add_workspace_id, &owner_id)
                .await
                .expect("read creator membership"),
            Some(MemberRole::Owner)
        );

        let rolled_back_workspace_id = format!("rolled-back-create-{suffix}");
        let mut tx = db.begin().await.expect("begin rolled-back create");
        assert!(
            tx.workspaces()
                .try_create_with_owner(&rolled_back_workspace_id, &owner_id, 11)
                .await
                .expect("stage owned workspace")
        );
        tx.rollback().await.expect("roll back owned workspace");
        assert_eq!(
            session
                .workspaces()
                .get(&rolled_back_workspace_id)
                .await
                .expect("read rolled-back workspace"),
            None
        );
        assert_eq!(
            session
                .workspace_members()
                .role_for_user_id(&rolled_back_workspace_id, &owner_id)
                .await
                .expect("read rolled-back creator membership"),
            None
        );

        let mut first_session = db;
        let mut second_session = db;
        let mut first_workspaces = first_session.workspaces();
        let mut second_workspaces = second_session.workspaces();
        let (first, second) = tokio::join!(
            first_workspaces.add_member(
                &identical_add_workspace_id,
                &member_id,
                MemberRole::Member,
                11,
            ),
            second_workspaces.add_member(
                &identical_add_workspace_id,
                &member_id,
                MemberRole::Member,
                12,
            ),
        );
        let outcomes = [first.expect("first add"), second.expect("second add")];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, AddMemberOutcome::Added(_)))
                .count(),
            1
        );
        let workspace = identical_add_workspace_id.as_str();
        let user = member_id.as_str();
        let identical_before = membership(db, workspace, user).await;
        let identical = add(db, workspace, user, MemberRole::Member).await;
        assert!(matches!(identical, AddMemberOutcome::ExistingSameRole(_)));
        assert_eq!(
            membership(db, workspace, user).await,
            identical_before,
            "an identical add must not rewrite the membership"
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, AddMemberOutcome::ExistingSameRole(_)))
                .count(),
            1
        );

        let conflicting_add_workspace_id = format!("conflicting-add-{suffix}");
        assert_eq!(
            session
                .workspaces()
                .create_with_owner(&conflicting_add_workspace_id, &owner_id, 20)
                .await
                .expect("create conflicting-add workspace"),
            WorkspaceCreationOutcome::Created
        );
        let mut member_session = db;
        let mut owner_session = db;
        let mut member_workspaces = member_session.workspaces();
        let mut owner_workspaces = owner_session.workspaces();
        let (member_add, owner_add) = tokio::join!(
            member_workspaces.add_member(
                &conflicting_add_workspace_id,
                &conflicting_member_id,
                MemberRole::Member,
                21,
            ),
            owner_workspaces.add_member(
                &conflicting_add_workspace_id,
                &conflicting_member_id,
                MemberRole::Owner,
                22,
            ),
        );
        let outcomes = [
            member_add.expect("member-role add"),
            owner_add.expect("owner-role add"),
        ];
        let settled_role = outcomes
            .iter()
            .find_map(|outcome| match outcome {
                AddMemberOutcome::RoleUpdated(member) => Some(member.role),
                _ => None,
            })
            .expect("one serialized different-role add must update the membership");
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, AddMemberOutcome::Added(_)))
                .count(),
            1
        );
        let mut session = db;
        assert_eq!(
            session
                .workspace_members()
                .role_for_user_id(&conflicting_add_workspace_id, &conflicting_member_id)
                .await
                .expect("read role after different-role adds"),
            Some(settled_role)
        );
        let created_at = membership(db, &conflicting_add_workspace_id, &conflicting_member_id)
            .await
            .expect("membership after different-role adds")
            .1;
        let workspace = conflicting_add_workspace_id.as_str();
        let user = conflicting_member_id.as_str();
        let other_role = match settled_role {
            MemberRole::Owner => MemberRole::Member,
            MemberRole::Member => MemberRole::Owner,
        };
        for role in [other_role, settled_role] {
            assert!(matches!(
                add(db, workspace, user, role).await,
                AddMemberOutcome::RoleUpdated(member) if member.role == role
            ));
        }
        assert_eq!(
            membership(db, workspace, user).await,
            Some((settled_role, created_at)),
            "role moves must preserve membership identity and creation time"
        );

        let owner_removal_workspace_id = format!("owner-removal-{suffix}");
        assert_eq!(
            session
                .workspaces()
                .create_with_owner(&owner_removal_workspace_id, &owner_id, 30)
                .await
                .expect("create owner-removal workspace"),
            WorkspaceCreationOutcome::Created
        );
        let workspace = owner_removal_workspace_id.as_str();
        let demotion = add(db, workspace, &owner_id, MemberRole::Member).await;
        assert_eq!(demotion, AddMemberOutcome::LastOwnerProtected);
        let mut session = db;
        assert!(matches!(
            session
                .workspaces()
                .add_member(
                    &owner_removal_workspace_id,
                    &other_owner_id,
                    MemberRole::Owner,
                    32,
                )
                .await
                .expect("add owner"),
            AddMemberOutcome::Added(_)
        ));

        let (first, second) = tokio::join!(
            add(db, workspace, &owner_id, MemberRole::Member),
            add(db, workspace, &other_owner_id, MemberRole::Member),
        );
        assert!(matches!(
            (first, second),
            (
                AddMemberOutcome::RoleUpdated(_),
                AddMemberOutcome::LastOwnerProtected
            ) | (
                AddMemberOutcome::LastOwnerProtected,
                AddMemberOutcome::RoleUpdated(_)
            )
        ));
        for user_id in [&owner_id, &other_owner_id] {
            add(db, workspace, user_id, MemberRole::Owner).await;
        }
        let mut remove_session = db;
        let mut remove_workspaces = remove_session.workspaces();
        let (demote, remove) = tokio::join!(
            add(db, workspace, &owner_id, MemberRole::Member),
            remove_workspaces.remove_member(workspace, &other_owner_id),
        );
        assert!(matches!(
            (demote, remove.expect("remove owner during mixed move")),
            (
                AddMemberOutcome::RoleUpdated(_),
                RemoveMemberOutcome::LastOwnerProtected
            ) | (
                AddMemberOutcome::LastOwnerProtected,
                RemoveMemberOutcome::Removed
            )
        ));
    }

    async fn add(
        db: &CoralDb,
        workspace_id: &str,
        user_id: &str,
        role: MemberRole,
    ) -> AddMemberOutcome {
        let mut session = db;
        session
            .workspaces()
            .add_member(workspace_id, user_id, role, 99)
            .await
            .expect("add or update member")
    }

    async fn membership(
        db: &CoralDb,
        workspace_id: &str,
        user_id: &str,
    ) -> Option<(MemberRole, i64)> {
        let statement = Query::select()
            .columns([WorkspaceMembers::Role, WorkspaceMembers::CreatedAtUnixNanos])
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(WorkspaceMembers::UserId).eq(user_id))
            .to_owned();
        let mut session = db;
        let row: Option<(String, i64)> = session
            .fetch_optional(statement)
            .await
            .expect("read membership state");
        row.map(|(role, created_at)| (MemberRole::parse(&role).expect("valid role"), created_at))
    }

    async fn create_user(db: &CoralDb, suffix: &str) -> String {
        let mut session = db;
        let UpsertLoginOutcome::Upserted(user) = session
            .users()
            .upsert_login("issuer", &format!("subject-{suffix}"), None, 1)
            .await
            .expect("create user")
        else {
            panic!("unique subject should create user")
        };
        user.user_id
    }

    async fn open_sqlite(temp: &tempfile::TempDir) -> CoralDb {
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("config") else {
            panic!("test config must be sqlite")
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate");
        db
    }

    fn postgres_test_url() -> Option<String> {
        bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
    }
}
