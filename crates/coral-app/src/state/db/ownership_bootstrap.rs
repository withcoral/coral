use crate::bootstrap::AppError;
use crate::identity::LOCAL_PRINCIPAL_ID;
use crate::state::db::{CoralDb, DbRepos, now_unix_nanos_i64};
use crate::workspaces::MemberRole;

const LOCAL_OWNERSHIP_MIGRATION_ID: &str = "local_workspace_ownership_v1";

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct SharedWorkspaceWarnings {
    pub(crate) ownerless: Vec<String>,
    pub(crate) local_only_owned: Vec<String>,
}

/// Runs the one-time single-user cutover for legacy ownerless workspaces.
///
/// The migration marker, the local user, and every ownership change are
/// committed in one transaction. Callers must have resolved the local principal
/// policy to `ImplicitOwner` before invoking it.
///
/// Returns `true` if this call performed the migration.
pub(crate) async fn migrate_local_ownership_once(db: &CoralDb) -> Result<bool, AppError> {
    migrate_local_ownership(db, false).await
}

async fn migrate_local_ownership(
    db: &CoralDb,
    fail_after_first_assignment: bool,
) -> Result<bool, AppError> {
    let now = now_unix_nanos_i64()?;
    let mut tx = db.begin().await?;
    if !tx
        .state_migrations()
        .try_claim(LOCAL_OWNERSHIP_MIGRATION_ID, now)
        .await?
    {
        tx.rollback().await?;
        return Ok(false);
    }
    tx.users().ensure_local_user(now).await?;

    for workspace in tx.workspaces().list().await? {
        if !tx
            .workspaces()
            .hold_for_child_mutation(&workspace.id)
            .await?
            || tx.workspace_members().owner_count(&workspace.id).await? != 0
        {
            continue;
        }
        tx.workspace_members()
            .delete(&workspace.id, LOCAL_PRINCIPAL_ID)
            .await?;
        tx.workspace_members()
            .insert(&workspace.id, LOCAL_PRINCIPAL_ID, MemberRole::Owner, now)
            .await?;
        if fail_after_first_assignment {
            return Err(AppError::FailedPrecondition(
                "injected local ownership migration failure".to_string(),
            ));
        }
    }
    tx.commit().await?;
    Ok(true)
}

/// Classifies legacy workspaces that authenticated users cannot reach.
pub(crate) async fn shared_workspace_warnings(
    db: &CoralDb,
) -> Result<SharedWorkspaceWarnings, AppError> {
    let mut tx = db.begin().await?;
    let mut warnings = SharedWorkspaceWarnings::default();
    for workspace in tx.workspaces().list().await? {
        let owner_count = tx.workspace_members().owner_count(&workspace.id).await?;
        if owner_count == 0 {
            warnings.ownerless.push(workspace.id);
        } else if owner_count == 1
            && tx
                .workspace_members()
                .role_for_user_id(&workspace.id, LOCAL_PRINCIPAL_ID)
                .await?
                == Some(MemberRole::Owner)
        {
            warnings.local_only_owned.push(workspace.id);
        }
    }
    tx.commit().await?;
    Ok(warnings)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{
        LOCAL_OWNERSHIP_MIGRATION_ID, SharedWorkspaceWarnings, migrate_local_ownership,
        migrate_local_ownership_once, shared_workspace_warnings,
    };
    use crate::bootstrap;
    use crate::identity::LOCAL_PRINCIPAL_ID;
    use crate::state::AppStateLayout;
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, UpsertLoginOutcome,
    };
    use crate::workspaces::MemberRole;

    #[tokio::test]
    async fn local_ownership_migration_is_atomic_and_one_time_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;

        assert_local_ownership_contract(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the ownership migration against Postgres"]
    async fn local_ownership_migration_repository_contract_on_postgres() {
        let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
        else {
            return;
        };
        let schema = format!("ownership_{}", uuid::Uuid::new_v4().simple());
        let admin = sqlx::PgPool::connect(&url).await.expect("connect Postgres");
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .expect("create isolated schema");
        let separator = if url.contains('?') { '&' } else { '?' };
        let url = format!("{url}{separator}options=-csearch_path%3D{schema}");
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open Postgres");
        db.migrate().await.expect("migrate Postgres");

        assert_local_ownership_contract(&db).await;
    }

    async fn open_sqlite(layout: &AppStateLayout) -> CoralDb {
        layout.ensure().expect("layout dirs");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(layout).expect("db config")
        else {
            panic!("default test database must be SQLite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open SQLite");
        db.migrate().await.expect("migrate SQLite");
        db
    }

    async fn assert_local_ownership_contract(db: &CoralDb) {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let ownerless = format!("ownerless-{suffix}");
        let local_member = format!("local-member-{suffix}");
        let human_owned = format!("human-owned-{suffix}");
        let post_migration = format!("post-migration-{suffix}");
        let human_id = seed_workspaces(db, &ownerless, &local_member, &human_owned).await;

        migrate_local_ownership(db, true)
            .await
            .expect_err("injected failure must roll back the transaction");
        assert!(!migration_completed(db).await);
        assert!(local_user(db).await.is_none());
        assert_eq!(role(db, &ownerless, LOCAL_PRINCIPAL_ID).await, None);
        add_local_member(db, &local_member).await;

        assert!(
            migrate_local_ownership_once(db)
                .await
                .expect("run migration")
        );
        assert!(migration_completed(db).await);
        let local = local_user(db).await.expect("local user");
        assert_eq!(local.issuer, LOCAL_PRINCIPAL_ID);
        assert_eq!(local.subject, "");
        assert_eq!(local.display_name.as_deref(), Some("Local"));
        assert_eq!(
            role(db, &ownerless, LOCAL_PRINCIPAL_ID).await,
            Some(MemberRole::Owner)
        );
        assert_eq!(
            role(db, &local_member, LOCAL_PRINCIPAL_ID).await,
            Some(MemberRole::Owner)
        );
        assert_eq!(
            role(db, &human_owned, &human_id).await,
            Some(MemberRole::Owner)
        );
        assert_eq!(role(db, &human_owned, LOCAL_PRINCIPAL_ID).await, None);

        let mut session = db;
        session
            .workspaces()
            .ensure(&post_migration, 40)
            .await
            .expect("create later workspace");
        assert!(
            !migrate_local_ownership_once(db)
                .await
                .expect("skip completed migration")
        );
        assert_eq!(role(db, &post_migration, LOCAL_PRINCIPAL_ID).await, None);

        assert_eq!(
            shared_workspace_warnings(db)
                .await
                .expect("classify shared warnings"),
            SharedWorkspaceWarnings {
                ownerless: vec![post_migration],
                local_only_owned: vec![local_member, ownerless],
            }
        );
    }

    async fn seed_workspaces(
        db: &CoralDb,
        ownerless: &str,
        local_member: &str,
        human_owned: &str,
    ) -> String {
        let mut tx = db.begin().await.expect("begin setup");
        for workspace in [ownerless, local_member, human_owned] {
            tx.workspaces()
                .ensure(workspace, 10)
                .await
                .expect("create workspace");
        }
        let human = match tx
            .users()
            .upsert_login(
                "issuer",
                &format!("subject-{human_owned}"),
                Some("Human"),
                10,
            )
            .await
            .expect("create human owner")
        {
            UpsertLoginOutcome::Upserted(user) => user,
            UpsertLoginOutcome::IssuerMismatch { .. } => panic!("test issuer must match"),
        };
        tx.workspace_members()
            .insert(human_owned, &human.user_id, MemberRole::Owner, 10)
            .await
            .expect("add human owner");
        tx.commit().await.expect("commit setup");
        human.user_id
    }

    async fn add_local_member(db: &CoralDb, workspace: &str) {
        let mut tx = db.begin().await.expect("begin local-member setup");
        tx.users()
            .ensure_local_user(20)
            .await
            .expect("create local user");
        tx.workspace_members()
            .insert(workspace, LOCAL_PRINCIPAL_ID, MemberRole::Member, 20)
            .await
            .expect("add local member");
        tx.commit().await.expect("commit local-member setup");
    }

    async fn role(db: &CoralDb, workspace: &str, user_id: &str) -> Option<MemberRole> {
        let mut session = db;
        session
            .workspace_members()
            .role_for_user_id(workspace, user_id)
            .await
            .expect("read membership")
    }

    async fn local_user(db: &CoralDb) -> Option<crate::state::db::UserRecord> {
        let mut session = db;
        session
            .users()
            .get_by_user_id(LOCAL_PRINCIPAL_ID)
            .await
            .expect("read local user")
    }

    async fn migration_completed(db: &CoralDb) -> bool {
        let mut session = db;
        session
            .state_migrations()
            .has_completed(LOCAL_OWNERSHIP_MIGRATION_ID)
            .await
            .expect("read migration marker")
    }
}
