//! The one-time upgrade that gives legacy ownerless workspaces a local owner.

use super::repositories::state_migrations::LOCAL_WORKSPACE_OWNERSHIP_MIGRATION_ID;
use super::session::DbRepos;
use super::{CoralDb, CoralTx, now_unix_nanos_i64};
use crate::bootstrap::AppError;
use crate::identity::LOCAL_PRINCIPAL_ID;

/// What one attempt at the local ownership migration did.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the single-user composition root logs this after the deployment policy lands"
    )
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LocalOwnershipMigrationReport {
    /// Whether this process was the one that claimed and ran the migration.
    pub(crate) performed: bool,
    /// How many workspaces this run gave the local principal ownership of.
    pub(crate) workspaces_claimed: usize,
}

/// Gives the built-in local user ownership of every legacy ownerless
/// workspace, exactly once for the lifetime of a state directory.
///
/// Only single-user deployments may call this: a shared deployment leaves its
/// legacy workspaces ownerless and concealed until an operator appoints real
/// owners. Every write lands in one transaction with the migration marker, so
/// a failed upgrade leaves no half-migrated state and the next start retries
/// the whole thing. Workspaces that already have an owner are untouched, and
/// nothing here creates, renames, or deletes a workspace.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the single-user composition root runs this after the deployment policy lands"
    )
)]
pub(crate) async fn migrate_local_ownership_once(
    db: &CoralDb,
) -> Result<LocalOwnershipMigrationReport, AppError> {
    migrate_local_ownership_once_at(db, now_unix_nanos_i64()?).await
}

async fn migrate_local_ownership_once_at(
    db: &CoralDb,
    now_unix_nanos: i64,
) -> Result<LocalOwnershipMigrationReport, AppError> {
    let mut tx = db.begin().await?;
    if !tx
        .state_migrations()
        .try_claim(LOCAL_WORKSPACE_OWNERSHIP_MIGRATION_ID, now_unix_nanos)
        .await?
    {
        tx.rollback().await?;
        return Ok(LocalOwnershipMigrationReport {
            performed: false,
            workspaces_claimed: 0,
        });
    }

    match claim_ownerless_workspaces(&mut tx, now_unix_nanos).await {
        Ok(workspaces_claimed) => {
            tx.commit().await?;
            Ok(LocalOwnershipMigrationReport {
                performed: true,
                workspaces_claimed,
            })
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

/// Holds each workspace in turn and gives the local principal the ones that
/// still have no owner, reporting how many it wrote.
///
/// A workspace that disappears between the listing and its hold is skipped:
/// it no longer exists to be given an owner.
async fn claim_ownerless_workspaces(
    tx: &mut CoralTx<'_>,
    now_unix_nanos: i64,
) -> Result<usize, AppError> {
    tx.users().ensure_local(now_unix_nanos).await?;

    let workspaces = tx.workspaces().list().await?;
    let mut workspaces_claimed = 0;
    for workspace in workspaces {
        if tx
            .workspaces()
            .hold_for_child_mutation(&workspace.id)
            .await?
            && tx
                .workspace_members()
                .claim_ownership_if_unowned(&workspace.id, LOCAL_PRINCIPAL_ID, now_unix_nanos)
                .await?
        {
            workspaces_claimed += 1;
        }
    }
    Ok(workspaces_claimed)
}

#[cfg(test)]
mod tests {
    use sea_query::{Expr, ExprTrait, Query};
    use tempfile::{TempDir, tempdir};

    use super::{
        LOCAL_WORKSPACE_OWNERSHIP_MIGRATION_ID, LocalOwnershipMigrationReport,
        migrate_local_ownership_once, migrate_local_ownership_once_at,
    };
    use crate::identity::LOCAL_PRINCIPAL_ID;
    use crate::state::db::repositories::users::{UpsertLoginOutcome, UserRecord};
    use crate::state::db::schema::Users;
    use crate::state::db::{CoralDb, DbRepos, DbSession, ResolvedDatabaseConfig};
    use crate::workspaces::MemberRole;

    /// The upgrade gives the local principal every ownerless workspace, leaves
    /// an already-owned one alone, and never runs a second time.
    #[tokio::test]
    async fn migrates_every_ownerless_workspace_exactly_once() {
        let (db, _temp) = open_sqlite().await;
        let human = seed_user(&db, "human").await;
        seed_workspaces(&db, &["unowned", "local_member", "owned"]).await;
        // An older single-user install could already hold a plain local
        // membership, which the upgrade has to promote rather than duplicate.
        seed_local_user(&db).await;
        grant(
            &db,
            "local_member",
            LOCAL_PRINCIPAL_ID,
            MemberRole::Member,
            30,
        )
        .await;
        grant(&db, "owned", &human, MemberRole::Owner, 40).await;

        let report = migrate_local_ownership_once(&db)
            .await
            .expect("migrate local ownership");

        assert_eq!(
            report,
            LocalOwnershipMigrationReport {
                performed: true,
                workspaces_claimed: 2,
            }
        );
        assert_eq!(
            role_for(&db, "unowned", LOCAL_PRINCIPAL_ID).await,
            Some(MemberRole::Owner)
        );
        assert_eq!(
            role_for(&db, "local_member", LOCAL_PRINCIPAL_ID).await,
            Some(MemberRole::Owner),
            "an existing local member is promoted rather than duplicated"
        );
        assert_eq!(
            role_for(&db, "owned", LOCAL_PRINCIPAL_ID).await,
            None,
            "an already-owned workspace must be left completely untouched"
        );
        assert_eq!(
            role_for(&db, "owned", &human).await,
            Some(MemberRole::Owner)
        );
        assert!(marker_completed(&db).await);

        // A workspace that loses its owner after the upgrade stays ownerless:
        // the marker retires the migration for good.
        revoke(&db, "unowned", LOCAL_PRINCIPAL_ID).await;
        let second = migrate_local_ownership_once(&db)
            .await
            .expect("re-run the completed migration");

        assert_eq!(
            second,
            LocalOwnershipMigrationReport {
                performed: false,
                workspaces_claimed: 0,
            }
        );
        assert_eq!(role_for(&db, "unowned", LOCAL_PRINCIPAL_ID).await, None);
    }

    /// A marker another process committed stops this one before it scans, so a
    /// workspace it would otherwise have claimed is left exactly as it was.
    #[tokio::test]
    async fn a_completed_marker_skips_workspace_scanning() {
        let (db, _temp) = open_sqlite().await;
        seed_workspaces(&db, &["unowned"]).await;
        let mut tx = db.begin().await.expect("begin foreign claim");
        assert!(
            tx.state_migrations()
                .try_claim(LOCAL_WORKSPACE_OWNERSHIP_MIGRATION_ID, 5)
                .await
                .expect("claim the migration elsewhere")
        );
        tx.commit().await.expect("commit foreign claim");

        let report = migrate_local_ownership_once_at(&db, 70)
            .await
            .expect("skip the completed migration");

        assert_eq!(
            report,
            LocalOwnershipMigrationReport {
                performed: false,
                workspaces_claimed: 0,
            }
        );
        assert_eq!(role_for(&db, "unowned", LOCAL_PRINCIPAL_ID).await, None);
        let mut session = &db;
        assert!(
            session
                .users()
                .get_by_user_id(LOCAL_PRINCIPAL_ID)
                .await
                .expect("read the local user")
                .is_none(),
            "a skipped migration writes nothing at all"
        );
    }

    /// A fresh single-user install has no workspaces. The upgrade still records
    /// itself and the local user, and must not invent a workspace to own.
    #[tokio::test]
    async fn fresh_state_records_the_marker_and_local_user_without_a_workspace() {
        let (db, _temp) = open_sqlite().await;

        let report = migrate_local_ownership_once_at(&db, 70)
            .await
            .expect("migrate empty state");

        assert_eq!(
            report,
            LocalOwnershipMigrationReport {
                performed: true,
                workspaces_claimed: 0,
            }
        );
        let mut session = &db;
        assert!(
            session
                .workspaces()
                .list()
                .await
                .expect("list workspaces")
                .is_empty(),
            "the upgrade must not create a workspace"
        );
        let local_user = local_user(&db).await;
        assert_eq!(local_user.user_id, LOCAL_PRINCIPAL_ID);
        assert_eq!(local_user.created_at_unix_nanos, 70);
        assert!(marker_completed(&db).await);
    }

    /// A row squatting on the local user's unique empty subject makes
    /// `ensure_local` a no-op, so the membership insert fails its user foreign
    /// key after the upgrade has already claimed its marker. That claim must
    /// not survive, or no later start could ever retry the upgrade. The
    /// repository contract covers the sibling direction — an ownership row
    /// that was written before a rollback disappears with it.
    #[tokio::test]
    async fn a_failure_part_way_through_rolls_back_every_write_and_retries_later() {
        let (db, _temp) = open_sqlite().await;
        seed_workspaces(&db, &["first", "second"]).await;
        // A verified identity always carries a non-empty `sub`, so only a
        // corrupted directory can hold the local user's empty subject.
        let squatter = seed_user(&db, "").await;

        let error = migrate_local_ownership_once_at(&db, 70)
            .await
            .expect_err("the membership insert must fail its user foreign key");

        assert!(
            error.to_string().contains("FOREIGN KEY constraint failed"),
            "unexpected error: {error}"
        );
        assert!(
            !marker_completed(&db).await,
            "a failed upgrade must leave no marker behind, so a later start retries"
        );
        for workspace_id in ["first", "second"] {
            assert_eq!(
                role_for(&db, workspace_id, LOCAL_PRINCIPAL_ID).await,
                None,
                "{workspace_id} kept an ownership write from a rolled-back upgrade"
            );
        }

        remove_user(&db, &squatter).await;
        let retry = migrate_local_ownership_once_at(&db, 80)
            .await
            .expect("retry the upgrade");

        assert_eq!(
            retry,
            LocalOwnershipMigrationReport {
                performed: true,
                workspaces_claimed: 2,
            }
        );
        assert_eq!(
            role_for(&db, "first", LOCAL_PRINCIPAL_ID).await,
            Some(MemberRole::Owner)
        );
        assert_eq!(
            role_for(&db, "second", LOCAL_PRINCIPAL_ID).await,
            Some(MemberRole::Owner)
        );
    }

    async fn seed_workspaces(db: &CoralDb, workspace_ids: &[&str]) {
        let mut tx = db.begin().await.expect("begin workspace seed");
        for workspace_id in workspace_ids {
            tx.workspaces()
                .create(workspace_id, 1)
                .await
                .expect("create legacy workspace");
        }
        tx.commit().await.expect("commit workspace seed");
    }

    async fn seed_local_user(db: &CoralDb) {
        let mut tx = db.begin().await.expect("begin local user seed");
        tx.users().ensure_local(5).await.expect("ensure local user");
        tx.commit().await.expect("commit local user seed");
    }

    async fn seed_user(db: &CoralDb, subject: &str) -> String {
        let mut session = db;
        match session
            .users()
            .upsert_login("https://issuer.test/members", subject, None, 1)
            .await
            .expect("provision user")
        {
            UpsertLoginOutcome::Upserted(user) => user.user_id,
            UpsertLoginOutcome::IssuerMismatch { stored_issuer } => {
                panic!("expected a provisioned user, got a mismatch with issuer {stored_issuer}")
            }
        }
    }

    async fn remove_user(db: &CoralDb, user_id: &str) {
        let mut session = db;
        let statement = Query::delete()
            .from_table(Users::Table)
            .and_where(Expr::col(Users::UserId).eq(user_id))
            .to_owned();
        session.execute(statement).await.expect("remove user");
    }

    async fn local_user(db: &CoralDb) -> UserRecord {
        let mut session = db;
        session
            .users()
            .get_by_user_id(LOCAL_PRINCIPAL_ID)
            .await
            .expect("read the local user")
            .expect("the local user row exists")
    }

    async fn marker_completed(db: &CoralDb) -> bool {
        let mut session = db;
        session
            .state_migrations()
            .has_completed(LOCAL_WORKSPACE_OWNERSHIP_MIGRATION_ID)
            .await
            .expect("read the migration marker")
    }

    async fn grant(db: &CoralDb, workspace_id: &str, user_id: &str, role: MemberRole, at: i64) {
        let mut session = db;
        session
            .workspace_members()
            .upsert(workspace_id, user_id, role, at)
            .await
            .expect("grant membership");
    }

    async fn revoke(db: &CoralDb, workspace_id: &str, user_id: &str) {
        let mut session = db;
        assert!(
            session
                .workspace_members()
                .remove(workspace_id, user_id)
                .await
                .expect("revoke membership")
        );
    }

    async fn role_for(db: &CoralDb, workspace_id: &str, user_id: &str) -> Option<MemberRole> {
        let mut session = db;
        session
            .workspace_members()
            .role_for_user_id(workspace_id, user_id)
            .await
            .expect("read role")
    }

    async fn open_sqlite() -> (CoralDb, TempDir) {
        let temp = tempdir().expect("temp dir");
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        (db, temp)
    }
}
