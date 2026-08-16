use sea_query::{
    Alias, Expr, ExprTrait, Func, OnConflict, Order, Query, SelectStatement, SimpleExpr,
};

use crate::identity::LOCAL_PRINCIPAL_ID;
use crate::state::db::schema::{Users, WorkspaceMembers};
use crate::state::db::session::DbSession;
use crate::state::db::{CoralTx, DbError};
use crate::workspaces::MemberRole;

pub(crate) struct WorkspaceMembersRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> WorkspaceMembersRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    /// Reads one caller's role in one workspace.
    ///
    /// The lookup is keyed on the `(workspace_id, user_id)` primary key. The
    /// request hot path never joins through `users`, so authorizing a request
    /// reads no issuer, subject, or other upstream identifier.
    pub(crate) async fn role_for_user_id(
        &mut self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<Option<MemberRole>, DbError> {
        let statement = Query::select()
            .column(WorkspaceMembers::Role)
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(WorkspaceMembers::UserId).eq(user_id))
            .to_owned();
        let stored: Option<(String,)> = self.session.fetch_optional(statement).await?;
        stored.map(|(role,)| decode_role(&role)).transpose()
    }

    /// Lists one caller's memberships, ordered by workspace id.
    ///
    /// A workspace with no owner grants nothing, so it is concealed here
    /// exactly as it is from authorization: a stale member of an ownerless
    /// workspace must not see it in their own listing either.
    ///
    /// The ordering is applied in Rust rather than in SQL because a workspace
    /// id is a name its creator chose: ordering it in the database would order
    /// it under the backend's collation, and `SQLite`'s binary comparison and
    /// Postgres's locale-aware default disagree on names that differ only by
    /// case or punctuation. One listing must not depend on which backend a
    /// deployment happens to run.
    pub(crate) async fn workspaces_for_user_id(
        &mut self,
        user_id: &str,
    ) -> Result<Vec<(String, MemberRole)>, DbError> {
        let statement = Query::select()
            .columns([WorkspaceMembers::WorkspaceId, WorkspaceMembers::Role])
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::UserId).eq(user_id))
            .and_where(Expr::exists(owner_bearing_workspace(Expr::col((
                WorkspaceMembers::Table,
                WorkspaceMembers::WorkspaceId,
            )))))
            .to_owned();
        let rows: Vec<(String, String)> = self.session.fetch_all(statement).await?;
        let mut memberships = rows
            .into_iter()
            .map(|(workspace_id, role)| Ok((workspace_id, decode_role(&role)?)))
            .collect::<Result<Vec<_>, DbError>>()?;
        memberships.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(memberships)
    }

    /// Lists one workspace's roster, ordered by user id.
    ///
    /// The join through `users` is what makes the roster one statement rather
    /// than a role lookup per person, and it reads only the display name: the
    /// issuer and subject stay inside the directory row, as they do everywhere
    /// outside the login seam.
    ///
    /// Unlike the request hot path this is deliberately unfiltered by owner
    /// count. An ownerless workspace is already concealed upstream, so nothing
    /// reaches here to be listed; filtering again would instead hide the very
    /// rows an owner-appointment tool needs to see.
    pub(crate) async fn members_of_workspace(
        &mut self,
        workspace_id: &str,
    ) -> Result<Vec<(String, MemberRole, Option<String>)>, DbError> {
        let statement = Query::select()
            .column((WorkspaceMembers::Table, WorkspaceMembers::UserId))
            .column((WorkspaceMembers::Table, WorkspaceMembers::Role))
            .column((Users::Table, Users::DisplayName))
            .from(WorkspaceMembers::Table)
            .inner_join(
                Users::Table,
                Expr::col((Users::Table, Users::UserId))
                    .equals((WorkspaceMembers::Table, WorkspaceMembers::UserId)),
            )
            .and_where(
                Expr::col((WorkspaceMembers::Table, WorkspaceMembers::WorkspaceId))
                    .eq(workspace_id),
            )
            .order_by(
                (WorkspaceMembers::Table, WorkspaceMembers::UserId),
                Order::Asc,
            )
            .to_owned();
        let rows: Vec<(String, String, Option<String>)> = self.session.fetch_all(statement).await?;
        rows.into_iter()
            .map(|(user_id, role, display_name)| Ok((user_id, decode_role(&role)?, display_name)))
            .collect()
    }

    /// Counts the owners one workspace still has.
    pub(crate) async fn owner_count(&mut self, workspace_id: &str) -> Result<i64, DbError> {
        let statement = Query::select()
            .expr(Func::count(Expr::val(1)))
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(WorkspaceMembers::Role).eq(MemberRole::Owner.as_storage_str()))
            .to_owned();
        let counted: Option<(i64,)> = self.session.fetch_optional(statement).await?;
        Ok(counted.unwrap_or_default().0)
    }

    /// Grants one membership, moving an existing row onto `role`.
    ///
    /// Identical concurrent adds converge instead of colliding: the conflict
    /// target is the membership primary key, so a losing writer updates the
    /// row the winner inserted and both succeed. The insert time is written
    /// once and left alone afterwards, so it keeps recording the first grant.
    pub(crate) async fn upsert(
        &mut self,
        workspace_id: &str,
        user_id: &str,
        role: MemberRole,
        created_at_unix_nanos: i64,
    ) -> Result<(), DbError> {
        let statement = Query::insert()
            .into_table(WorkspaceMembers::Table)
            .columns([
                WorkspaceMembers::WorkspaceId,
                WorkspaceMembers::UserId,
                WorkspaceMembers::Role,
                WorkspaceMembers::CreatedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(workspace_id.to_string()),
                Expr::val(user_id.to_string()),
                Expr::val(role.as_storage_str()),
                Expr::val(created_at_unix_nanos),
            ])
            .on_conflict(
                OnConflict::columns([WorkspaceMembers::WorkspaceId, WorkspaceMembers::UserId])
                    .update_column(WorkspaceMembers::Role)
                    .to_owned(),
            )
            .to_owned();
        self.session.execute(statement).await
    }

    /// Revokes one membership, reporting whether a row was there to revoke.
    pub(crate) async fn remove(
        &mut self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<bool, DbError> {
        let statement = Query::delete()
            .from_table(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(WorkspaceMembers::UserId).eq(user_id))
            .to_owned();
        Ok(self.session.execute_rows_affected(statement).await? == 1)
    }
}

impl WorkspaceMembersRepo<'_, CoralTx<'_>> {
    /// Grants `user_id` ownership of `workspace_id`, but only while that
    /// workspace has no owner at all, reporting whether it wrote.
    ///
    /// The caller must already hold the workspace parent row through
    /// [`hold_for_child_mutation`]: that hold is the one serialization point
    /// for owner-floor writes, and without it this count and write could
    /// straddle a concurrent membership mutation. A workspace that already has
    /// any owner is left completely untouched — no row is added for `user_id`
    /// there — while an existing non-owner row for `user_id` is promoted in
    /// place rather than duplicated.
    ///
    /// [`hold_for_child_mutation`]: super::workspaces::WorkspacesRepo::hold_for_child_mutation
    pub(crate) async fn claim_ownership_if_unowned(
        &mut self,
        workspace_id: &str,
        user_id: &str,
        created_at_unix_nanos: i64,
    ) -> Result<bool, DbError> {
        if self.owner_count(workspace_id).await? > 0 {
            return Ok(false);
        }
        self.upsert(
            workspace_id,
            user_id,
            MemberRole::Owner,
            created_at_unix_nanos,
        )
        .await?;
        Ok(true)
    }
}

/// Selects every workspace id that still has at least one owner.
///
/// The set form, for callers that need the whole list — `NOT IN` cannot be
/// correlated away. The per-row question has its own helper below.
pub(super) fn owner_bearing_workspaces() -> SelectStatement {
    Query::select()
        .column(WorkspaceMembers::WorkspaceId)
        .from(WorkspaceMembers::Table)
        .and_where(Expr::col(WorkspaceMembers::Role).eq(MemberRole::Owner.as_storage_str()))
        .to_owned()
}

/// Table alias the ownership probe reads under, so its predicates cannot be
/// mistaken for the outer row's.
fn owner_probe() -> Alias {
    Alias::new("owner_probe")
}

/// Asks whether `workspace_id` still has at least one owner.
///
/// Correlated to the row being read rather than a standalone `IN (...)` list.
/// `SQLite` does not flatten an uncorrelated `IN (SELECT ...)` into a semi-join,
/// so it rebuilds the full set of owner-bearing workspaces on every execution:
/// the cost follows the size of the whole install rather than the size of the
/// answer, on a path every directory read takes. Correlating it turns that into
/// one index probe per candidate row against `(role, workspace_id)`. `Postgres`
/// flattens either spelling, so this is `SQLite`'s requirement rather than the
/// query's meaning.
fn owner_bearing_workspace(workspace_id: SimpleExpr) -> SelectStatement {
    Query::select()
        .expr(Expr::val(1))
        .from_as(WorkspaceMembers::Table, owner_probe())
        .and_where(
            Expr::col((owner_probe(), WorkspaceMembers::Role))
                .eq(MemberRole::Owner.as_storage_str()),
        )
        .and_where(Expr::col((owner_probe(), WorkspaceMembers::WorkspaceId)).eq(workspace_id))
        .to_owned()
}

/// Selects every workspace id owned by someone other than the synthetic local
/// principal — that is, every workspace an authenticated caller can still
/// reach through an owner.
pub(super) fn non_local_owner_bearing_workspaces() -> SelectStatement {
    owner_bearing_workspaces()
        .and_where(Expr::col(WorkspaceMembers::UserId).ne(LOCAL_PRINCIPAL_ID))
        .to_owned()
}

/// Decodes one stored role, failing closed on anything unrecognized.
fn decode_role(value: &str) -> Result<MemberRole, DbError> {
    MemberRole::from_storage_str(value).ok_or_else(|| {
        DbError::CorruptData(format!(
            "workspace membership row has an unrecognized role '{value}'"
        ))
    })
}

#[cfg(test)]
mod tests {
    use sea_query::{Expr, ExprTrait, Func, Query};
    use tempfile::{TempDir, tempdir};

    use super::decode_role;
    use crate::bootstrap;
    use crate::identity::LOCAL_PRINCIPAL_ID;
    use crate::state::db::repositories::state_migrations::LOCAL_WORKSPACE_OWNERSHIP_MIGRATION_ID;
    use crate::state::db::repositories::users::{UpsertLoginOutcome, UserRecord};
    use crate::state::db::repositories::workspaces::InaccessibleWorkspaces;
    use crate::state::db::schema::WorkspaceMembers;
    use crate::state::db::{CoralDb, DbError, DbRepos, DbSession, ResolvedDatabaseConfig};
    use crate::workspaces::MemberRole;

    /// One workspace pair and one user trio, all named for a single test run.
    ///
    /// Every id carries the run's suffix so a shared contract touches only its
    /// own rows: `make postgres-tests` runs every `contract_on_postgres` test
    /// concurrently against one database.
    const OWNER_DISPLAY_NAME: &str = "Ada Owner";

    struct Fixture {
        owned: String,
        ownerless: String,
        owner: String,
        member: String,
        stranger: String,
    }

    #[tokio::test]
    async fn workspace_members_repository_contract_holds_against_sqlite() {
        let (db, _temp) = open_sqlite().await;
        assert_membership_contract(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared contract against Postgres"]
    async fn workspace_members_repository_contract_on_postgres() {
        let Some(db) = open_postgres().await else {
            return;
        };
        assert_membership_contract(&db).await;
    }

    #[tokio::test]
    async fn workspace_members_repository_add_race_contract_holds_against_sqlite() {
        let (db, _temp) = open_sqlite().await;
        assert_identical_adds_converge(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared contract against Postgres"]
    async fn workspace_members_repository_add_race_contract_on_postgres() {
        let Some(db) = open_postgres().await else {
            return;
        };
        assert_identical_adds_converge(&db).await;
    }

    #[tokio::test]
    async fn local_ownership_repository_contract_holds_against_sqlite() {
        let (db, _temp) = open_sqlite().await;
        assert_local_ownership_contract(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared contract against Postgres"]
    async fn local_ownership_repository_contract_on_postgres() {
        let Some(db) = open_postgres().await else {
            return;
        };
        assert_local_ownership_contract(&db).await;
    }

    #[test]
    fn workspace_members_repository_fails_closed_on_an_unrecognized_stored_role() {
        assert_eq!(
            decode_role("owner").expect("owner decodes"),
            MemberRole::Owner
        );
        assert_eq!(
            decode_role("member").expect("member decodes"),
            MemberRole::Member
        );

        // The check constraint rejects this on the way in, so it can only
        // arrive from an out-of-band write. Reading it must not yield a role.
        let error = decode_role("admin").expect_err("an unknown role must not decode");
        assert!(
            matches!(&error, DbError::CorruptData(detail) if detail.contains("admin")),
            "unexpected error: {error}"
        );
    }

    async fn assert_membership_contract(db: &CoralDb) {
        let fixture = seed(db).await;
        grant(db, &fixture.owned, &fixture.owner, MemberRole::Owner, 10).await;
        grant(db, &fixture.owned, &fixture.member, MemberRole::Member, 20).await;
        // This workspace is deliberately left without an owner.
        grant(
            db,
            &fixture.ownerless,
            &fixture.member,
            MemberRole::Member,
            30,
        )
        .await;

        assert_eq!(
            role_for(db, &fixture.owned, &fixture.owner).await,
            Some(MemberRole::Owner)
        );
        assert_eq!(
            role_for(db, &fixture.owned, &fixture.member).await,
            Some(MemberRole::Member)
        );
        assert_eq!(
            role_for(db, &fixture.owned, &fixture.stranger).await,
            None,
            "a non-member must have no role"
        );
        assert_eq!(
            role_for(db, &fixture.ownerless, &fixture.owner).await,
            None,
            "a role must not leak across workspaces"
        );
        assert_eq!(owner_count(db, &fixture.owned).await, 1);
        assert_eq!(owner_count(db, &fixture.ownerless).await, 0);

        assert_eq!(
            memberships_of(db, &fixture.member).await,
            vec![(fixture.owned.clone(), MemberRole::Member)],
            "an ownerless workspace must be concealed from its own member"
        );
        assert_eq!(
            memberships_of(db, &fixture.owner).await,
            vec![(fixture.owned.clone(), MemberRole::Owner)]
        );
        assert_eq!(memberships_of(db, &fixture.stranger).await, vec![]);

        // The roster is the one query that joins through `users`, so it is
        // also the one that decodes a display name — present for the owner,
        // absent for the member — on whichever backend is running.
        let mut expected_roster = vec![
            (
                fixture.owner.clone(),
                MemberRole::Owner,
                Some(OWNER_DISPLAY_NAME.to_string()),
            ),
            (fixture.member.clone(), MemberRole::Member, None),
        ];
        expected_roster.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(roster_of(db, &fixture.owned).await, expected_roster);
        assert_eq!(
            roster_of(db, &fixture.ownerless).await,
            vec![(fixture.member.clone(), MemberRole::Member, None)],
            "the roster is deliberately unfiltered by owner count"
        );

        assert_promotion_moves_one_row(db, &fixture).await;
        assert_revocation_is_idempotent(db, &fixture).await;
    }

    /// Adding an existing member under another role moves that row in place,
    /// and un-conceals the workspace it makes owner-bearing.
    async fn assert_promotion_moves_one_row(db: &CoralDb, fixture: &Fixture) {
        grant(
            db,
            &fixture.ownerless,
            &fixture.member,
            MemberRole::Owner,
            40,
        )
        .await;

        assert_eq!(
            role_for(db, &fixture.ownerless, &fixture.member).await,
            Some(MemberRole::Owner)
        );
        assert_eq!(owner_count(db, &fixture.ownerless).await, 1);
        assert_eq!(
            granted_at(db, &fixture.ownerless, &fixture.member).await,
            30,
            "a promotion must keep recording the first grant"
        );

        let mut expected = vec![
            (fixture.owned.clone(), MemberRole::Member),
            (fixture.ownerless.clone(), MemberRole::Owner),
        ];
        expected.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(memberships_of(db, &fixture.member).await, expected);
    }

    async fn assert_revocation_is_idempotent(db: &CoralDb, fixture: &Fixture) {
        assert!(revoke(db, &fixture.owned, &fixture.member).await);
        assert!(
            !revoke(db, &fixture.owned, &fixture.member).await,
            "revoking an absent membership must report no row"
        );
        assert_eq!(role_for(db, &fixture.owned, &fixture.member).await, None);
        assert_eq!(
            owner_count(db, &fixture.owned).await,
            1,
            "revoking a member must not touch the owner floor"
        );
    }

    /// One legacy state per test run: a workspace with nothing, one whose only
    /// local row is a plain membership, and one an authenticated person owns.
    struct LegacyOwnership {
        migration_id: String,
        unowned: String,
        local_member: String,
        owned: String,
        human: String,
    }

    /// Drives the whole one-time local ownership migration through the
    /// primitives it is built from, inside the caller's own transaction.
    async fn assert_local_ownership_contract(db: &CoralDb) {
        let legacy = seed_legacy_ownership(db).await;

        let before = inaccessible(db).await;
        assert!(before.without_owner.contains(&legacy.unowned));
        assert!(
            before.without_owner.contains(&legacy.local_member),
            "a workspace whose only member is not an owner has no owner"
        );
        assert!(!before.without_owner.contains(&legacy.owned));
        assert!(!before.local_owner_only.contains(&legacy.owned));

        assert_rolled_back_migration_writes_nothing(db, &legacy).await;
        migrate_local_ownership(db, &legacy).await;
        assert_migration_wrote_only_where_unowned(db, &legacy).await;

        let after = inaccessible(db).await;
        assert!(!after.without_owner.contains(&legacy.unowned));
        assert!(!after.without_owner.contains(&legacy.local_member));
        assert!(after.local_owner_only.contains(&legacy.unowned));
        assert!(after.local_owner_only.contains(&legacy.local_member));
        assert!(!after.local_owner_only.contains(&legacy.owned));

        // A workspace an authenticated owner shares with the local principal
        // is reachable, so holding a local owner is not on its own enough to
        // report it.
        grant(
            db,
            &legacy.local_member,
            &legacy.human,
            MemberRole::Owner,
            110,
        )
        .await;
        let shared = inaccessible(db).await;
        assert!(!shared.local_owner_only.contains(&legacy.local_member));
        assert!(!shared.without_owner.contains(&legacy.local_member));

        let mut tx = db.begin().await.expect("begin duplicate migration");
        assert!(
            !tx.state_migrations()
                .try_claim(&legacy.migration_id, 100)
                .await
                .expect("reject a completed migration"),
            "a completed marker must stop the migration before it scans"
        );
        tx.rollback()
            .await
            .expect("roll back the duplicate migration");
    }

    async fn seed_legacy_ownership(db: &CoralDb) -> LegacyOwnership {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let legacy = LegacyOwnership {
            // The real marker id would stay claimed in the shared Postgres
            // test database and fail every later run, so the shape is
            // exercised under a per-run id derived from it.
            migration_id: format!("{LOCAL_WORKSPACE_OWNERSHIP_MIGRATION_ID}_{suffix}"),
            unowned: format!("workspace_unowned_{suffix}"),
            local_member: format!("workspace_local_member_{suffix}"),
            owned: format!("workspace_owned_{suffix}"),
            human: seed_user(db, &format!("human_{suffix}"), None).await,
        };

        let mut tx = db.begin().await.expect("begin legacy seed");
        for workspace_id in [&legacy.unowned, &legacy.local_member, &legacy.owned] {
            tx.workspaces()
                .create(workspace_id, 1)
                .await
                .expect("create legacy workspace");
        }
        tx.users().ensure_local(5).await.expect("ensure local user");
        tx.workspace_members()
            .upsert(
                &legacy.local_member,
                LOCAL_PRINCIPAL_ID,
                MemberRole::Member,
                30,
            )
            .await
            .expect("seed a local member");
        tx.workspace_members()
            .upsert(&legacy.owned, &legacy.human, MemberRole::Owner, 40)
            .await
            .expect("seed a human owner");
        tx.commit().await.expect("commit legacy seed");

        assert_eq!(
            local_user(db).await,
            UserRecord {
                user_id: LOCAL_PRINCIPAL_ID.to_string(),
                issuer: LOCAL_PRINCIPAL_ID.to_string(),
                subject: String::new(),
                display_name: Some("Local".to_string()),
                created_at_unix_nanos: 5,
                last_login_at_unix_nanos: 5,
            }
        );
        legacy
    }

    /// A failed migration must leave neither its marker nor its ownership
    /// writes behind, so the next startup can retry the whole thing.
    async fn assert_rolled_back_migration_writes_nothing(db: &CoralDb, legacy: &LegacyOwnership) {
        let mut tx = db.begin().await.expect("begin rolled-back migration");
        assert!(
            tx.state_migrations()
                .try_claim(&legacy.migration_id, 50)
                .await
                .expect("claim the migration")
        );
        assert!(
            tx.workspaces()
                .hold_for_child_mutation(&legacy.unowned)
                .await
                .expect("hold the workspace parent")
        );
        assert!(
            tx.workspace_members()
                .claim_ownership_if_unowned(&legacy.unowned, LOCAL_PRINCIPAL_ID, 60)
                .await
                .expect("claim ownership")
        );
        tx.rollback().await.expect("roll back the migration");

        assert_eq!(
            role_for(db, &legacy.unowned, LOCAL_PRINCIPAL_ID).await,
            None,
            "a rolled-back migration must leave no ownership behind"
        );
    }

    /// Runs the migration shape once: one claim, one local user, and one
    /// hold-then-conditional-write per workspace, all in one transaction.
    async fn migrate_local_ownership(db: &CoralDb, legacy: &LegacyOwnership) {
        let mut tx = db.begin().await.expect("begin migration");
        assert!(
            tx.state_migrations()
                .try_claim(&legacy.migration_id, 70)
                .await
                .expect("claim the migration after a rollback")
        );
        tx.users()
            .ensure_local(80)
            .await
            .expect("ensure local user again");
        for (workspace_id, claimed) in [
            (&legacy.unowned, true),
            (&legacy.local_member, true),
            (&legacy.owned, false),
        ] {
            assert!(
                tx.workspaces()
                    .hold_for_child_mutation(workspace_id)
                    .await
                    .expect("hold the workspace parent")
            );
            assert_eq!(
                tx.workspace_members()
                    .claim_ownership_if_unowned(workspace_id, LOCAL_PRINCIPAL_ID, 90)
                    .await
                    .expect("claim ownership"),
                claimed,
                "{workspace_id} took the wrong conditional ownership decision"
            );
        }
        assert!(
            !tx.workspaces()
                .hold_for_child_mutation(&format!("{}_missing", legacy.unowned))
                .await
                .expect("hold a missing workspace")
        );
        tx.commit().await.expect("commit migration");
    }

    async fn assert_migration_wrote_only_where_unowned(db: &CoralDb, legacy: &LegacyOwnership) {
        assert_eq!(
            role_for(db, &legacy.unowned, LOCAL_PRINCIPAL_ID).await,
            Some(MemberRole::Owner)
        );
        assert_eq!(
            role_for(db, &legacy.local_member, LOCAL_PRINCIPAL_ID).await,
            Some(MemberRole::Owner),
            "an existing local member must be promoted rather than duplicated"
        );
        assert_eq!(
            granted_at(db, &legacy.local_member, LOCAL_PRINCIPAL_ID).await,
            30,
            "a promotion must keep recording the first grant"
        );
        assert_eq!(
            role_for(db, &legacy.owned, LOCAL_PRINCIPAL_ID).await,
            None,
            "an already-owned workspace must be left completely untouched"
        );
        assert_eq!(
            role_for(db, &legacy.owned, &legacy.human).await,
            Some(MemberRole::Owner)
        );
        assert_eq!(
            local_user(db).await.last_login_at_unix_nanos,
            5,
            "re-ensuring the local user must not rewrite the stored row"
        );
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

    async fn inaccessible(db: &CoralDb) -> InaccessibleWorkspaces {
        let mut session = db;
        session
            .workspaces()
            .inaccessible()
            .await
            .expect("report inaccessible workspaces")
    }

    async fn assert_identical_adds_converge(db: &CoralDb) {
        let fixture = seed(db).await;
        let (mut first, mut second, mut third) = (db, db, db);
        let (mut first, mut second, mut third) = (
            first.workspace_members(),
            second.workspace_members(),
            third.workspace_members(),
        );

        let (left, middle, right) = tokio::join!(
            first.upsert(&fixture.owned, &fixture.member, MemberRole::Member, 10),
            second.upsert(&fixture.owned, &fixture.member, MemberRole::Member, 20),
            third.upsert(&fixture.owned, &fixture.member, MemberRole::Member, 30),
        );
        left.expect("first concurrent add");
        middle.expect("second concurrent add");
        right.expect("third concurrent add");

        assert_eq!(
            member_count(db, &fixture.owned).await,
            1,
            "identical concurrent adds must converge on one row"
        );
        assert_eq!(
            role_for(db, &fixture.owned, &fixture.member).await,
            Some(MemberRole::Member)
        );
    }

    /// Creates two workspaces and three directory users for one test run.
    ///
    /// The two workspace names are deliberately ordered one way by byte value
    /// and the other way by a locale-aware collation: `SQLite` sorts the
    /// capital `B` before the lowercase `a`, while Postgres's default
    /// collation sorts `alpha` before `beta`. A listing ordered in SQL would
    /// therefore disagree between the two backends, and the expectations below
    /// pin one order for both.
    ///
    /// The owner carries a display name and the member deliberately carries
    /// none, so the roster join decodes both the present and the absent case.
    async fn seed(db: &CoralDb) -> Fixture {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let fixture = Fixture {
            owned: format!("Beta_owned_{suffix}"),
            ownerless: format!("alpha_ownerless_{suffix}"),
            owner: seed_user(db, &format!("owner_{suffix}"), Some(OWNER_DISPLAY_NAME)).await,
            member: seed_user(db, &format!("member_{suffix}"), None).await,
            stranger: seed_user(db, &format!("stranger_{suffix}"), Some("Stranger")).await,
        };

        let mut tx = db.begin().await.expect("begin seed");
        for workspace_id in [&fixture.owned, &fixture.ownerless] {
            tx.workspaces()
                .create(workspace_id, 1)
                .await
                .expect("create workspace");
        }
        tx.commit().await.expect("commit seed");
        fixture
    }

    async fn seed_user(db: &CoralDb, subject: &str, display_name: Option<&str>) -> String {
        let mut session = db;
        match session
            .users()
            .upsert_login("https://issuer.test/members", subject, display_name, 1)
            .await
            .expect("provision user")
        {
            UpsertLoginOutcome::Upserted(user) => user.user_id,
            UpsertLoginOutcome::IssuerMismatch { stored_issuer } => {
                panic!("expected a provisioned user, got a mismatch with issuer {stored_issuer}")
            }
        }
    }

    async fn grant(db: &CoralDb, workspace_id: &str, user_id: &str, role: MemberRole, at: i64) {
        let mut session = db;
        session
            .workspace_members()
            .upsert(workspace_id, user_id, role, at)
            .await
            .expect("grant membership");
    }

    async fn revoke(db: &CoralDb, workspace_id: &str, user_id: &str) -> bool {
        let mut session = db;
        session
            .workspace_members()
            .remove(workspace_id, user_id)
            .await
            .expect("revoke membership")
    }

    async fn role_for(db: &CoralDb, workspace_id: &str, user_id: &str) -> Option<MemberRole> {
        let mut session = db;
        session
            .workspace_members()
            .role_for_user_id(workspace_id, user_id)
            .await
            .expect("read role")
    }

    async fn memberships_of(db: &CoralDb, user_id: &str) -> Vec<(String, MemberRole)> {
        let mut session = db;
        session
            .workspace_members()
            .workspaces_for_user_id(user_id)
            .await
            .expect("list memberships")
    }

    async fn roster_of(
        db: &CoralDb,
        workspace_id: &str,
    ) -> Vec<(String, MemberRole, Option<String>)> {
        let mut session = db;
        session
            .workspace_members()
            .members_of_workspace(workspace_id)
            .await
            .expect("list workspace members")
    }

    async fn owner_count(db: &CoralDb, workspace_id: &str) -> i64 {
        let mut session = db;
        session
            .workspace_members()
            .owner_count(workspace_id)
            .await
            .expect("count owners")
    }

    /// Counts every membership row of one workspace, which the repository
    /// deliberately does not expose.
    async fn member_count(db: &CoralDb, workspace_id: &str) -> i64 {
        let mut session = db;
        let statement = Query::select()
            .expr(Func::count(Expr::val(1)))
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::WorkspaceId).eq(workspace_id))
            .to_owned();
        let counted: Option<(i64,)> = session.fetch_optional(statement).await.expect("count rows");
        counted.unwrap_or_default().0
    }

    async fn granted_at(db: &CoralDb, workspace_id: &str, user_id: &str) -> i64 {
        let mut session = db;
        let statement = Query::select()
            .column(WorkspaceMembers::CreatedAtUnixNanos)
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(WorkspaceMembers::UserId).eq(user_id))
            .to_owned();
        let granted: Option<(i64,)> = session
            .fetch_optional(statement)
            .await
            .expect("read grant time");
        granted.expect("membership exists").0
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

    async fn open_postgres() -> Option<CoralDb> {
        let url = bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())?;
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");
        Some(db)
    }
}
