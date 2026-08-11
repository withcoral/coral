//! Transactional workspace-membership mutations.

#![cfg_attr(not(test), expect(dead_code, reason = "used higher in the PR stack"))]

use super::repositories::users::UserRecord;
use super::{CoralDb, DbError, DbRepos};
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
    pub(crate) async fn add_workspace_member(
        &self,
        workspace_id: &str,
        user_id: &str,
        role: MemberRole,
        created_at_unix_nanos: i64,
    ) -> Result<AddMemberOutcome, DbError> {
        let mut tx = self.begin().await?;
        if !tx
            .workspaces()
            .hold_for_child_mutation(workspace_id)
            .await?
        {
            tx.rollback().await?;
            return Ok(AddMemberOutcome::WorkspaceNotFound);
        }
        let Some(user) = tx.users().get_by_user_id(user_id).await? else {
            tx.rollback().await?;
            return Ok(AddMemberOutcome::UserNotFound);
        };
        if let Some(existing_role) = tx
            .workspace_members()
            .role_for_user_id(workspace_id, user_id)
            .await?
        {
            tx.rollback().await?;
            return Ok(existing_add_outcome(user, existing_role, role));
        }
        match tx
            .workspace_members()
            .insert(workspace_id, user_id, role, created_at_unix_nanos)
            .await
        {
            Ok(()) => {
                tx.commit().await?;
                Ok(AddMemberOutcome::Added(member_view(user, role)))
            }
            Err(error) if error.is_unique_violation() => {
                tx.rollback().await?;
                let mut session = self;
                let Some(existing_role) = session
                    .workspace_members()
                    .role_for_user_id(workspace_id, user_id)
                    .await?
                else {
                    return Err(error);
                };
                Ok(existing_add_outcome(user, existing_role, role))
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn remove_workspace_member(
        &self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<RemoveMemberOutcome, DbError> {
        let mut tx = self.begin().await?;
        if !tx
            .workspaces()
            .hold_for_child_mutation(workspace_id)
            .await?
        {
            tx.rollback().await?;
            return Ok(RemoveMemberOutcome::WorkspaceNotFound);
        }
        let Some(role) = tx
            .workspace_members()
            .role_for_user_id(workspace_id, user_id)
            .await?
        else {
            tx.rollback().await?;
            return Ok(RemoveMemberOutcome::MemberNotFound);
        };
        if role == MemberRole::Owner && tx.workspace_members().owner_count(workspace_id).await? <= 1
        {
            tx.rollback().await?;
            return Ok(RemoveMemberOutcome::LastOwnerProtected);
        }
        let removed = tx.workspace_members().delete(workspace_id, user_id).await?;
        if removed {
            tx.commit().await?;
            Ok(RemoveMemberOutcome::Removed)
        } else {
            tx.rollback().await?;
            Ok(RemoveMemberOutcome::MemberNotFound)
        }
    }
}

fn existing_add_outcome(
    user: UserRecord,
    existing_role: MemberRole,
    requested_role: MemberRole,
) -> AddMemberOutcome {
    if existing_role == requested_role {
        AddMemberOutcome::ExistingSameRole(member_view(user, existing_role))
    } else {
        AddMemberOutcome::RoleConflict
    }
}

fn member_view(user: UserRecord, role: MemberRole) -> WorkspaceMemberView {
    WorkspaceMemberView {
        user_id: user.user_id,
        role,
        display_name: user.display_name,
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
        assert_eq!(
            db.create_workspace_with_owner(&identical_add_workspace_id, &owner_id, 10)
                .await
                .expect("create owned workspace"),
            WorkspaceCreationOutcome::Created
        );

        let (first, second) = tokio::join!(
            db.add_workspace_member(
                &identical_add_workspace_id,
                &member_id,
                MemberRole::Member,
                11,
            ),
            db.add_workspace_member(
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
            db.create_workspace_with_owner(&conflicting_add_workspace_id, &owner_id, 20)
                .await
                .expect("create conflicting-add workspace"),
            WorkspaceCreationOutcome::Created
        );
        let (member_add, owner_add) = tokio::join!(
            db.add_workspace_member(
                &conflicting_add_workspace_id,
                &conflicting_member_id,
                MemberRole::Member,
                21,
            ),
            db.add_workspace_member(
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
            db.create_workspace_with_owner(&owner_removal_workspace_id, &owner_id, 30)
                .await
                .expect("create owner-removal workspace"),
            WorkspaceCreationOutcome::Created
        );
        assert!(matches!(
            db.add_workspace_member(
                &owner_removal_workspace_id,
                &other_owner_id,
                MemberRole::Owner,
                31,
            )
            .await
            .expect("add owner"),
            AddMemberOutcome::Added(_)
        ));

        let (first, second) = tokio::join!(
            db.remove_workspace_member(&owner_removal_workspace_id, &owner_id),
            db.remove_workspace_member(&owner_removal_workspace_id, &other_owner_id),
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
