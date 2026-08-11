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
    RoleConflict,
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
    use tempfile::tempdir;

    use super::{AddMemberOutcome, RemoveMemberOutcome};
    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::repositories::users::UpsertLoginOutcome;
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
    async fn workspace_member_state_repository_round_trips_against_postgres() {
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
        let added_role = outcomes
            .iter()
            .find_map(|outcome| match outcome {
                AddMemberOutcome::Added(member) => Some(member.role),
                AddMemberOutcome::ExistingSameRole(_)
                | AddMemberOutcome::RoleConflict
                | AddMemberOutcome::WorkspaceNotFound
                | AddMemberOutcome::UserNotFound => None,
            })
            .expect("one conflicting add must win");
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, AddMemberOutcome::Added(_)))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, AddMemberOutcome::RoleConflict))
                .count(),
            1
        );
        let mut session = db;
        assert_eq!(
            session
                .workspace_members()
                .role_for_user_id(&conflicting_add_workspace_id, &conflicting_member_id)
                .await
                .expect("read role after conflicting adds"),
            Some(added_role),
            "the losing conflicting add must not mutate the winning role"
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
        let mut session = db;
        assert!(matches!(
            session
                .workspaces()
                .add_member(
                    &owner_removal_workspace_id,
                    &other_owner_id,
                    MemberRole::Owner,
                    31,
                )
                .await
                .expect("add owner"),
            AddMemberOutcome::Added(_)
        ));

        let mut first_remove_session = db;
        let mut second_remove_session = db;
        let mut first_remove_workspaces = first_remove_session.workspaces();
        let mut second_remove_workspaces = second_remove_session.workspaces();
        let (first, second) = tokio::join!(
            first_remove_workspaces.remove_member(&owner_removal_workspace_id, &owner_id),
            second_remove_workspaces.remove_member(&owner_removal_workspace_id, &other_owner_id),
        );
        let outcomes = [
            first.expect("remove first owner"),
            second.expect("remove second owner"),
        ];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, RemoveMemberOutcome::Removed))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, RemoveMemberOutcome::LastOwnerProtected))
                .count(),
            1
        );
        let mut tx = db.begin().await.expect("begin owner verification tx");
        assert_eq!(
            tx.workspace_members()
                .owner_count(&owner_removal_workspace_id)
                .await
                .expect("count remaining owners"),
            1,
            "concurrent owner removals must preserve exactly one owner"
        );
        tx.rollback().await.expect("rollback owner verification tx");
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
