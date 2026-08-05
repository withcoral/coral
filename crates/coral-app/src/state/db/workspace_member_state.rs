//! Transactional workspace-membership mutations.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "membership state is wired to production consumers in M3"
    )
)]

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
    use crate::state::AppStateLayout;
    use crate::state::db::repositories::users::UpsertLoginOutcome;
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, WorkspaceCreationOutcome,
    };
    use crate::workspaces::MemberRole;

    #[tokio::test]
    async fn membership_mutations_converge_and_protect_the_last_owner() {
        let temp = tempdir().expect("temp dir");
        let db = open_sqlite(&temp).await;
        let owner_id = create_user(&db, "owner").await;
        let member_id = create_user(&db, "member").await;
        let other_owner_id = create_user(&db, "other-owner").await;
        let workspace_id = "team";
        assert_eq!(
            db.create_workspace_with_owner(workspace_id, &owner_id, 10)
                .await
                .expect("create owned workspace"),
            WorkspaceCreationOutcome::Created
        );

        let (first, second) = tokio::join!(
            db.add_workspace_member(workspace_id, &member_id, MemberRole::Member, 11),
            db.add_workspace_member(workspace_id, &member_id, MemberRole::Member, 12),
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
        assert_eq!(
            db.add_workspace_member(workspace_id, &member_id, MemberRole::Owner, 13)
                .await
                .expect("conflicting add"),
            AddMemberOutcome::RoleConflict
        );

        assert_eq!(
            db.remove_workspace_member(workspace_id, &owner_id)
                .await
                .expect("protect owner"),
            RemoveMemberOutcome::LastOwnerProtected
        );
        assert!(matches!(
            db.add_workspace_member(workspace_id, &other_owner_id, MemberRole::Owner, 14)
                .await
                .expect("add owner"),
            AddMemberOutcome::Added(_)
        ));
        assert_eq!(
            db.remove_workspace_member(workspace_id, &owner_id)
                .await
                .expect("remove one owner"),
            RemoveMemberOutcome::Removed
        );
        assert_eq!(
            db.remove_workspace_member(workspace_id, &other_owner_id)
                .await
                .expect("protect remaining owner"),
            RemoveMemberOutcome::LastOwnerProtected
        );
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
}
