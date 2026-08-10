use sea_query::{Expr, OnConflict, Query};

use super::schema::Users;
use super::{CoralDb, DbRepos, DbSession, now_unix_nanos_i64};
use crate::bootstrap::AppError;
use crate::workspaces::MemberRole;

const LOCAL_IDENTITY: &str = "coral:local";
// OIDC requires a non-empty `sub`, so this synthetic subject cannot collide
// with any identity accepted by the authentication boundary.
const LOCAL_SUBJECT: &str = "";
const LOCAL_DISPLAY_NAME: &str = "Local";

/// Records the host as owner of every workspace that has none.
///
/// Only a state directory without `[auth]` is bootstrapped this way: there the
/// host user is the deployment. Touching only zero-owner workspaces is what
/// keeps a removed owner from being resurrected.
pub(crate) async fn stamp_local_ownership(db: &CoralDb) -> Result<(), AppError> {
    let now = now_unix_nanos_i64()?;
    let mut tx = db.begin().await?;
    let statement = Query::insert()
        .into_table(Users::Table)
        .columns([
            Users::UserId,
            Users::Issuer,
            Users::Subject,
            Users::DisplayName,
            Users::CreatedAtUnixNanos,
            Users::LastLoginAtUnixNanos,
        ])
        .values_panic([
            Expr::val(LOCAL_IDENTITY),
            Expr::val(LOCAL_IDENTITY),
            Expr::val(LOCAL_SUBJECT),
            Expr::val(LOCAL_DISPLAY_NAME),
            Expr::val(now),
            Expr::val(now),
        ])
        .on_conflict(
            OnConflict::column(Users::UserId)
                .update_columns([
                    Users::Issuer,
                    Users::Subject,
                    Users::DisplayName,
                    Users::LastLoginAtUnixNanos,
                ])
                .to_owned(),
        )
        .to_owned();
    DbSession::execute(&mut tx, statement).await?;

    for workspace in tx.workspaces().list().await? {
        if !tx
            .workspaces()
            .hold_for_child_mutation(&workspace.id)
            .await?
        {
            continue;
        }
        if tx.workspace_members().owner_count(&workspace.id).await? == 0 {
            tx.workspace_members()
                .delete(&workspace.id, LOCAL_IDENTITY)
                .await?;
            tx.workspace_members()
                .insert(&workspace.id, LOCAL_IDENTITY, MemberRole::Owner, now)
                .await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

/// Names the workspaces that no one owns.
///
/// A workspace with no owner is not a hazard to serve: membership lookups
/// conceal it even if stale non-owner rows remain. It is a hazard to leave
/// *unnoticed*, which is why startup says so. Repair is out of band — the admin
/// tool appoints an owner — because a shared deployment has no privileged
/// request path that could do it.
pub(crate) async fn ownerless_workspaces(db: &CoralDb) -> Result<Vec<String>, AppError> {
    let mut tx = db.begin().await?;
    let mut ownerless = Vec::new();
    for workspace in tx.workspaces().list().await? {
        if tx.workspace_members().owner_count(&workspace.id).await? == 0 {
            ownerless.push(workspace.id);
        }
    }
    tx.commit().await?;
    Ok(ownerless)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{
        LOCAL_DISPLAY_NAME, LOCAL_IDENTITY, LOCAL_SUBJECT, ownerless_workspaces,
        stamp_local_ownership,
    };
    use crate::bootstrap;
    use crate::credentials::{CredentialManager, CredentialStore};
    use crate::identity::Principal;
    use crate::state::AppStateLayout;
    use crate::state::ConfigStore;
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, UpsertLoginOutcome,
        now_unix_nanos_i64,
    };
    use crate::workspaces::{MemberRole, WorkspaceManager, WorkspaceName};

    #[tokio::test]
    async fn ownership_bootstrap_contract_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("default test database must be sqlite");
        };
        let db = std::sync::Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");

        assert_ownership_bootstrap_contract(&db).await;
        assert_shared_transition_blocks_local_creation(&layout, db).await;
    }

    #[tokio::test]
    #[ignore = "requires CORAL_TEST_POSTGRES_URL"]
    async fn ownership_bootstrap_repository_round_trips_against_postgres() {
        let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
        else {
            return;
        };
        let admin = sqlx::PgPool::connect(&url).await.expect("connect postgres");
        sqlx::query("CREATE SCHEMA IF NOT EXISTS ownership_bootstrap")
            .execute(&admin)
            .await
            .expect("create isolated schema");
        let separator = if url.contains('?') { '&' } else { '?' };
        let url = format!("{url}{separator}options=-csearch_path%3Downership_bootstrap");
        let db = std::sync::Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
                .await
                .expect("open postgres"),
        );
        db.migrate().await.expect("migrate postgres");
        assert_ownership_bootstrap_contract(&db).await;
    }

    /// Single-user bootstrap adopts what nobody owns and leaves owned
    /// workspaces alone, so a removed owner is never resurrected. What it
    /// leaves behind is what `ownerless_workspaces` reports on a shared
    /// deployment, where nothing is stamped at all.
    async fn assert_ownership_bootstrap_contract(db: &CoralDb) {
        let now = now_unix_nanos_i64().expect("system time");
        let mut tx = db.begin().await.expect("begin setup");
        tx.workspaces()
            .delete_all()
            .await
            .expect("clear workspaces");
        for workspace_id in ["default", "ownerless", "already_owned"] {
            tx.workspaces()
                .ensure(workspace_id, now)
                .await
                .expect("create workspace");
        }
        let owner = match tx
            .users()
            .upsert_login("issuer", "existing-owner", Some("Existing"), now)
            .await
            .expect("create existing owner")
        {
            UpsertLoginOutcome::Upserted(owner) => owner,
            UpsertLoginOutcome::IssuerMismatch { .. } => panic!("test issuer must match"),
        };
        let oidc_collision = match tx
            .users()
            .upsert_login(
                "https://issuer.example.test",
                LOCAL_IDENTITY,
                Some("OIDC User"),
                now,
            )
            .await
            .expect("create OIDC subject matching the synthetic local ID")
        {
            UpsertLoginOutcome::Upserted(user) => user,
            UpsertLoginOutcome::IssuerMismatch { .. } => panic!("test issuer must match"),
        };
        tx.workspace_members()
            .insert("already_owned", &owner.user_id, MemberRole::Owner, now)
            .await
            .expect("add existing owner");
        tx.commit().await.expect("commit setup");

        let mut before = ownerless_workspaces(db).await.expect("ownerless before");
        before.sort();
        assert_eq!(before, vec!["default".to_string(), "ownerless".to_string()]);

        stamp_local_ownership(db).await.expect("stamp local owner");

        let mut session = db;
        let local = session
            .users()
            .get_by_user_id(LOCAL_IDENTITY)
            .await
            .expect("load local user")
            .expect("local user row");
        assert_eq!(local.display_name.as_deref(), Some(LOCAL_DISPLAY_NAME));
        assert_eq!(local.subject, LOCAL_SUBJECT);
        assert_ne!(local.user_id, oidc_collision.user_id);
        let refreshed_collision = session
            .users()
            .upsert_login(
                "https://issuer.example.test",
                LOCAL_IDENTITY,
                Some("OIDC User Again"),
                now + 1,
            )
            .await
            .expect("refresh OIDC collision subject after local bootstrap");
        assert!(matches!(
            refreshed_collision,
            UpsertLoginOutcome::Upserted(ref user) if user.user_id == oidc_collision.user_id
        ));
        for workspace_id in ["default", "ownerless"] {
            assert_eq!(
                session
                    .workspace_members()
                    .role_for_user_id(workspace_id, LOCAL_IDENTITY)
                    .await
                    .expect("read local membership"),
                Some(MemberRole::Owner),
                "single-user bootstrap adopts {workspace_id}"
            );
        }
        assert!(
            session
                .workspace_members()
                .role_for_user_id("already_owned", LOCAL_IDENTITY)
                .await
                .expect("read local membership")
                .is_none(),
            "a workspace that already has an owner is left alone"
        );
        assert!(
            ownerless_workspaces(db)
                .await
                .expect("ownerless after")
                .is_empty()
        );
    }

    async fn assert_shared_transition_blocks_local_creation(
        layout: &AppStateLayout,
        db: std::sync::Arc<CoralDb>,
    ) {
        let manager = WorkspaceManager::new_for_tests(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout.clone(),
            None,
            db,
        );
        let workspace = WorkspaceName::parse("shared-created").expect("workspace");

        assert!(matches!(
            manager
                .create_workspace_for_user(&workspace, &Principal::local())
                .await,
            Err(crate::bootstrap::AppError::PermissionDenied(_))
        ));
        assert!(
            manager
                .list_workspaces()
                .await
                .expect("list workspaces")
                .iter()
                .all(|record| record.name != workspace)
        );
    }
}
