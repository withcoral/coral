//! Transactional login provisioning and pre-v1 task-attribution rekeying.

#![cfg_attr(not(test), expect(dead_code, reason = "used higher in the PR stack"))]

use super::repositories::users::UpsertLoginOutcome;
use super::workspace_state::{hold_user_for_workspace_creation, try_create_workspace_with_owner};
use super::{CoralDb, DbError, DbRepos};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DefaultWorkspaceProvisioningOutcome {
    Created(String),
    AlreadyExists(String),
    UserNotFound,
}

impl CoralDb {
    pub(crate) async fn upsert_user_and_ensure_default_workspace(
        &self,
        issuer: &str,
        subject: &str,
        display_name: Option<&str>,
        now_unix_nanos: i64,
    ) -> Result<UpsertLoginOutcome, DbError> {
        let mut tx = self.begin().await?;
        let outcome = tx
            .users()
            .upsert_login(issuer, subject, display_name, now_unix_nanos)
            .await?;
        let UpsertLoginOutcome::Upserted(user) = &outcome else {
            tx.rollback().await?;
            return Ok(outcome);
        };
        let workspace_id = default_workspace_id(&user.user_id);
        try_create_workspace_with_owner(&mut tx, &workspace_id, &user.user_id, now_unix_nanos)
            .await?;
        tx.commit().await?;
        Ok(outcome)
    }

    pub(crate) async fn ensure_user_default_workspace(
        &self,
        user_id: &str,
        now_unix_nanos: i64,
    ) -> Result<DefaultWorkspaceProvisioningOutcome, DbError> {
        let mut tx = self.begin().await?;
        if !hold_user_for_workspace_creation(&mut tx, user_id).await? {
            tx.rollback().await?;
            return Ok(DefaultWorkspaceProvisioningOutcome::UserNotFound);
        }
        let workspace_id = default_workspace_id(user_id);
        if try_create_workspace_with_owner(&mut tx, &workspace_id, user_id, now_unix_nanos).await? {
            tx.commit().await?;
            Ok(DefaultWorkspaceProvisioningOutcome::Created(workspace_id))
        } else {
            tx.rollback().await?;
            Ok(DefaultWorkspaceProvisioningOutcome::AlreadyExists(
                workspace_id,
            ))
        }
    }

    pub(crate) async fn reattribute_pre_v1_tasks_to_user(
        &self,
        pre_v1_task_attribution_id: &str,
        user_id: &str,
    ) -> Result<u64, DbError> {
        let mut tx = self.begin().await?;
        let updated = tx
            .tasks()
            .reattribute_pre_v1_tasks_to_user(pre_v1_task_attribution_id, user_id)
            .await?;
        tx.commit().await?;
        Ok(updated)
    }
}

fn default_workspace_id(user_id: &str) -> String {
    format!("default-{user_id}")
}

#[cfg(test)]
mod tests {
    use sea_query::{Expr, ExprTrait, Query};
    use tempfile::tempdir;

    use super::DefaultWorkspaceProvisioningOutcome;
    use crate::state::AppStateLayout;
    use crate::state::db::repositories::users::UpsertLoginOutcome;
    use crate::state::db::schema::Tasks;
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbRepos, DbSession, ResolvedDatabaseConfig, TaskCreation,
        TaskCreationResult,
    };
    use crate::workspaces::MemberRole;

    #[tokio::test]
    async fn login_creates_only_a_free_personal_workspace() {
        let temp = tempdir().expect("temp dir");
        let db = open_sqlite(&temp).await;
        let UpsertLoginOutcome::Upserted(user) = db
            .upsert_user_and_ensure_default_workspace("issuer", "subject", Some("Name"), 10)
            .await
            .expect("provision login")
        else {
            panic!("new login should provision")
        };
        let workspace_id = format!("default-{}", user.user_id);
        let mut session = &db;
        assert_eq!(
            session
                .workspace_members()
                .role_for_user_id(&workspace_id, &user.user_id)
                .await
                .expect("personal membership"),
            Some(MemberRole::Owner)
        );

        let collision_user = create_user(&db, "collision").await;
        let collision_workspace = format!("default-{collision_user}");
        let mut tx = db.begin().await.expect("begin collision tx");
        tx.workspaces()
            .create(&collision_workspace, 20)
            .await
            .expect("seed collision");
        tx.commit().await.expect("commit collision");
        assert_eq!(
            db.ensure_user_default_workspace(&collision_user, 21)
                .await
                .expect("detect collision"),
            DefaultWorkspaceProvisioningOutcome::AlreadyExists(collision_workspace.clone())
        );
        assert_eq!(
            session
                .workspace_members()
                .role_for_user_id(&collision_workspace, &collision_user)
                .await
                .expect("collision membership"),
            None,
            "an existing workspace must never be granted"
        );

        let concurrent_user = create_user(&db, "concurrent").await;
        let (first, second) = tokio::join!(
            db.ensure_user_default_workspace(&concurrent_user, 30),
            db.ensure_user_default_workspace(&concurrent_user, 31),
        );
        assert!(matches!(
            (first.expect("first ensure"), second.expect("second ensure")),
            (
                DefaultWorkspaceProvisioningOutcome::Created(_),
                DefaultWorkspaceProvisioningOutcome::AlreadyExists(_)
            ) | (
                DefaultWorkspaceProvisioningOutcome::AlreadyExists(_),
                DefaultWorkspaceProvisioningOutcome::Created(_)
            )
        ));
    }

    #[tokio::test]
    async fn reattributes_only_the_pre_v1_task_digest() {
        let temp = tempdir().expect("temp dir");
        let db = open_sqlite(&temp).await;
        let user_id = create_user(&db, "rekey").await;
        let workspace_id = "legacy-task-workspace";
        let mut tx = db.begin().await.expect("begin workspace tx");
        tx.workspaces()
            .create(workspace_id, 1)
            .await
            .expect("workspace");
        tx.commit().await.expect("commit workspace");
        assert_eq!(
            db.task_state()
                .create(
                    TaskCreation {
                        id: "legacy-task",
                        workspace_id,
                        created_by_principal_id: "pre-v1-digest",
                        intent: "legacy",
                        created_at_unix_nanos: 2,
                    },
                    10,
                )
                .await
                .expect("create legacy task"),
            TaskCreationResult::Created
        );
        assert_eq!(
            db.reattribute_pre_v1_tasks_to_user("pre-v1-digest", &user_id)
                .await
                .expect("reattribute"),
            1
        );
        let statement = Query::select()
            .column(Tasks::CreatedByPrincipalId)
            .from(Tasks::Table)
            .and_where(Expr::col(Tasks::Id).eq("legacy-task"))
            .to_owned();
        let mut session = &db;
        let row: Option<(String,)> = session.fetch_optional(statement).await.expect("read task");
        assert_eq!(row, Some((user_id,)));
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
