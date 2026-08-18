//! Transactional persistence for workspace creation, membership, and deletion.
//!
//! Every membership mutation takes the workspace parent row first, through
//! [`WorkspacesRepo::hold_for_child_mutation`], and only then reads the owner
//! count it decides on. That ordering is what makes the owner floor safe on
//! both backends without a backend-specific isolation level: the hold is the
//! transaction's first statement, so a second writer blocks on the parent row
//! before it has read anything, and reaches its own owner count only once the
//! first writer has committed or rolled back.
//!
//! [`WorkspacesRepo::hold_for_child_mutation`]: super::repositories::workspaces::WorkspacesRepo::hold_for_child_mutation

use super::{CoralDb, CoralTx, DbError, DbRepos};
use crate::workspaces::MemberRole;

/// One membership as it stands after a mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceMemberRecord {
    pub(crate) user_id: String,
    pub(crate) display_name: Option<String>,
    pub(crate) role: MemberRole,
}

/// Outcome of creating one workspace owned by its creator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreateWorkspaceOutcome {
    Created,
    AlreadyExists,
    /// The creator has no directory row, so the workspace would have been left
    /// without an owner. Nothing is written.
    CreatorNotFound,
}

/// Outcome of granting one membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AddMemberOutcome {
    Added(WorkspaceMemberRecord),
    /// The membership already stood at this role, so no row was written.
    ExistingSameRole(WorkspaceMemberRecord),
    RoleUpdated(WorkspaceMemberRecord),
    /// The change would have demoted the workspace's last owner.
    LastOwnerProtected,
    WorkspaceNotFound,
    UserNotFound,
}

/// Outcome of revoking one membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoveMemberOutcome {
    Removed,
    WorkspaceNotFound,
    MemberNotFound,
    /// The revocation would have left the workspace with no owner.
    LastOwnerProtected,
}

pub(crate) struct WorkspaceState<'a> {
    db: &'a CoralDb,
    #[cfg(test)]
    mutation_barrier: Option<&'a MembershipMutationBarrier>,
}

pub(crate) struct WorkspaceDeletion<'a> {
    tx: CoralTx<'a>,
}

/// Pauses one membership mutation while it holds the workspace parent, so a
/// test can drive a second mutation into the hold instead of hoping for it.
#[cfg(test)]
pub(crate) struct MembershipMutationBarrier {
    workspace_held: tokio::sync::Barrier,
    release_mutation: tokio::sync::Barrier,
}

#[cfg(test)]
impl MembershipMutationBarrier {
    pub(crate) fn new() -> Self {
        Self {
            workspace_held: tokio::sync::Barrier::new(2),
            release_mutation: tokio::sync::Barrier::new(2),
        }
    }

    async fn pause_after_workspace_hold(&self) {
        self.workspace_held.wait().await;
        self.release_mutation.wait().await;
    }

    pub(crate) async fn wait_until_workspace_held(&self) {
        self.workspace_held.wait().await;
    }

    pub(crate) async fn release_mutation(&self) {
        self.release_mutation.wait().await;
    }
}

impl CoralDb {
    pub(crate) const fn workspace_state(&self) -> WorkspaceState<'_> {
        WorkspaceState {
            db: self,
            #[cfg(test)]
            mutation_barrier: None,
        }
    }

    /// Builds a [`WorkspaceState`] whose membership mutations pause while
    /// holding the workspace parent, so tests can contend for that hold.
    #[cfg(test)]
    pub(crate) const fn workspace_state_with_mutation_barrier<'a>(
        &'a self,
        mutation_barrier: &'a MembershipMutationBarrier,
    ) -> WorkspaceState<'a> {
        WorkspaceState {
            db: self,
            mutation_barrier: Some(mutation_barrier),
        }
    }

    pub(crate) async fn begin_workspace_deletion(
        &self,
        workspace_id: &str,
    ) -> Result<Option<WorkspaceDeletion<'_>>, DbError> {
        let mut tx = self.begin().await?;
        if tx.workspaces().delete(workspace_id).await? {
            Ok(Some(WorkspaceDeletion { tx }))
        } else {
            tx.rollback().await?;
            Ok(None)
        }
    }
}

impl WorkspaceState<'_> {
    /// Creates one workspace together with its creator's Owner membership.
    ///
    /// The two inserts share one transaction, so no production path can leave
    /// behind a workspace that nobody owns: a creator who has no directory row
    /// rolls the workspace back with the membership that could not be granted.
    pub(crate) async fn create_owned_by(
        &self,
        workspace_id: &str,
        owner_user_id: &str,
        created_at_unix_nanos: i64,
    ) -> Result<CreateWorkspaceOutcome, DbError> {
        let mut tx = self.db.begin().await?;
        if let Err(error) = tx
            .workspaces()
            .create(workspace_id, created_at_unix_nanos)
            .await
        {
            tx.rollback().await?;
            return if error.is_unique_violation() {
                Ok(CreateWorkspaceOutcome::AlreadyExists)
            } else {
                Err(error)
            };
        }
        if tx.users().get_by_user_id(owner_user_id).await?.is_none() {
            tx.rollback().await?;
            return Ok(CreateWorkspaceOutcome::CreatorNotFound);
        }
        tx.workspace_members()
            .upsert(
                workspace_id,
                owner_user_id,
                MemberRole::Owner,
                created_at_unix_nanos,
            )
            .await?;
        tx.commit().await?;
        Ok(CreateWorkspaceOutcome::Created)
    }

    /// Grants one membership, moving an existing one onto `role`.
    ///
    /// Adding a member who already holds `role` is a success that writes
    /// nothing, so a retried grant is indistinguishable from the first one.
    pub(crate) async fn add_member(
        &self,
        workspace_id: &str,
        user_id: &str,
        role: MemberRole,
        granted_at_unix_nanos: i64,
    ) -> Result<AddMemberOutcome, DbError> {
        let mut tx = self.db.begin().await?;
        if !tx
            .workspaces()
            .hold_for_child_mutation(workspace_id)
            .await?
        {
            tx.rollback().await?;
            return Ok(AddMemberOutcome::WorkspaceNotFound);
        }
        #[cfg(test)]
        if let Some(mutation_barrier) = self.mutation_barrier {
            mutation_barrier.pause_after_workspace_hold().await;
        }
        let Some(user) = tx.users().get_by_user_id(user_id).await? else {
            tx.rollback().await?;
            return Ok(AddMemberOutcome::UserNotFound);
        };
        let member = WorkspaceMemberRecord {
            user_id: user.user_id,
            display_name: user.display_name,
            role,
        };
        let current = tx
            .workspace_members()
            .role_for_user_id(workspace_id, user_id)
            .await?;
        if current == Some(role) {
            tx.rollback().await?;
            return Ok(AddMemberOutcome::ExistingSameRole(member));
        }
        // Demoting the last owner strands the workspace exactly as removing
        // them would, so it answers to the same floor.
        if current == Some(MemberRole::Owner) && is_last_owner(&mut tx, workspace_id).await? {
            tx.rollback().await?;
            return Ok(AddMemberOutcome::LastOwnerProtected);
        }
        tx.workspace_members()
            .upsert(workspace_id, user_id, role, granted_at_unix_nanos)
            .await?;
        tx.commit().await?;
        Ok(if current.is_some() {
            AddMemberOutcome::RoleUpdated(member)
        } else {
            AddMemberOutcome::Added(member)
        })
    }

    /// Revokes one membership unless it is the workspace's last owner.
    pub(crate) async fn remove_member(
        &self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<RemoveMemberOutcome, DbError> {
        let mut tx = self.db.begin().await?;
        if !tx
            .workspaces()
            .hold_for_child_mutation(workspace_id)
            .await?
        {
            tx.rollback().await?;
            return Ok(RemoveMemberOutcome::WorkspaceNotFound);
        }
        #[cfg(test)]
        if let Some(mutation_barrier) = self.mutation_barrier {
            mutation_barrier.pause_after_workspace_hold().await;
        }
        let Some(role) = tx
            .workspace_members()
            .role_for_user_id(workspace_id, user_id)
            .await?
        else {
            tx.rollback().await?;
            return Ok(RemoveMemberOutcome::MemberNotFound);
        };
        if role == MemberRole::Owner && is_last_owner(&mut tx, workspace_id).await? {
            tx.rollback().await?;
            return Ok(RemoveMemberOutcome::LastOwnerProtected);
        }
        tx.workspace_members().remove(workspace_id, user_id).await?;
        tx.commit().await?;
        Ok(RemoveMemberOutcome::Removed)
    }
}

impl WorkspaceDeletion<'_> {
    pub(crate) async fn commit(self) -> Result<(), DbError> {
        self.tx.commit().await
    }

    pub(crate) async fn rollback(self) -> Result<(), DbError> {
        self.tx.rollback().await
    }
}

/// Reports whether the workspace has at most the one owner being changed.
///
/// Sound only under the parent hold taken above: without it the count could be
/// stale by the time the caller acts on it.
async fn is_last_owner(tx: &mut CoralTx<'_>, workspace_id: &str) -> Result<bool, DbError> {
    Ok(tx.workspace_members().owner_count(workspace_id).await? <= 1)
}
#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::time::Duration;

    use sea_query::{Expr, ExprTrait, Func, Query};
    use tempfile::{TempDir, tempdir};

    use super::{
        AddMemberOutcome, CreateWorkspaceOutcome, MembershipMutationBarrier, RemoveMemberOutcome,
        WorkspaceMemberRecord,
    };
    use crate::bootstrap;
    use crate::state::db::repositories::users::UpsertLoginOutcome;
    use crate::state::db::schema::WorkspaceMembers;
    use crate::state::db::{CoralDb, DbError, DbRepos, DbSession, ResolvedDatabaseConfig};
    use crate::workspaces::MemberRole;
    use AddMemberOutcome::{UserNotFound, WorkspaceNotFound};
    use CreateWorkspaceOutcome::{AlreadyExists, Created, CreatorNotFound};
    use RemoveMemberOutcome::{LastOwnerProtected, MemberNotFound, Removed};

    /// Every seeded directory row carries this name, so a returned membership
    /// proves it was read from the directory rather than synthesized.
    const SEEDED_DISPLAY_NAME: &str = "Seeded User";

    /// One workspace and three directory users, all named for a single run.
    ///
    /// Every id carries the run's suffix so a shared contract touches only its
    /// own rows: `make postgres-tests` runs every `contract_on_postgres` test
    /// concurrently against one database.
    struct Fixture {
        workspace: String,
        owner: String,
        second_owner: String,
        member: String,
    }

    #[tokio::test]
    async fn workspace_state_contract_holds_against_sqlite() {
        let (db, _temp) = open_sqlite().await;
        assert_workspace_state_contract(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared contract against Postgres"]
    async fn workspace_state_contract_on_postgres() {
        let Some(db) = open_postgres().await else {
            return;
        };
        assert_workspace_state_contract(&db).await;
    }

    #[tokio::test]
    async fn workspace_owner_floor_race_contract_holds_against_sqlite() {
        let (db, _temp) = open_sqlite().await;
        assert_owner_floor_survives_races(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared contract against Postgres"]
    async fn workspace_owner_floor_race_contract_on_postgres() {
        let Some(db) = open_postgres().await else {
            return;
        };
        assert_owner_floor_survives_races(&db).await;
    }

    async fn assert_workspace_state_contract(db: &CoralDb) {
        assert_creator_owned_creation(db).await;
        assert_membership_mutations(db).await;
    }

    /// Creation grants the creator ownership, or writes nothing at all.
    async fn assert_creator_owned_creation(db: &CoralDb) {
        let fixture = seed(db).await;
        let (workspace, owner) = (&fixture.workspace, &fixture.owner);

        assert_eq!(create(db, workspace, owner).await, Created);
        assert_eq!(
            role_for(db, workspace, owner).await,
            Some(MemberRole::Owner)
        );
        assert_eq!(create(db, workspace, owner).await, AlreadyExists);

        let unowned = format!("{workspace}_unowned");
        assert_eq!(
            create(db, &unowned, "user_that_never_logged_in").await,
            CreatorNotFound
        );
        assert!(
            !workspace_exists(db, &unowned).await,
            "a creation that cannot grant ownership must roll the workspace back"
        );
    }

    async fn assert_membership_mutations(db: &CoralDb) {
        let fixture = seed(db).await;
        let (workspace, owner, member) = (&fixture.workspace, &fixture.owner, &fixture.member);
        let missing = format!("{workspace}_missing");
        assert_eq!(create(db, workspace, owner).await, Created);

        assert_eq!(
            add(db, workspace, member, MemberRole::Member, 10).await,
            granted(member, MemberRole::Member)
        );
        assert_eq!(
            add(db, workspace, member, MemberRole::Member, 20).await,
            unchanged(member, MemberRole::Member)
        );
        assert_eq!(
            granted_at(db, workspace, member).await,
            10,
            "a same-role add must succeed without writing the membership row"
        );
        assert_eq!(
            add(db, workspace, member, MemberRole::Owner, 30).await,
            promoted(member, MemberRole::Owner)
        );
        assert_eq!(owner_count(db, workspace).await, 2);

        assert_eq!(
            add(db, &missing, owner, MemberRole::Member, 40).await,
            WorkspaceNotFound
        );
        assert_eq!(
            add(db, workspace, "nobody", MemberRole::Member, 40).await,
            UserNotFound
        );

        // With the second owner gone the first one is the floor, and neither
        // revoking nor demoting them may breach it.
        assert_eq!(remove(db, workspace, member).await, Removed);
        assert_eq!(remove(db, workspace, owner).await, LastOwnerProtected);
        assert_eq!(
            add(db, workspace, owner, MemberRole::Member, 50).await,
            AddMemberOutcome::LastOwnerProtected
        );
        assert_eq!(remove(db, workspace, member).await, MemberNotFound);
        assert_eq!(
            remove(db, &missing, owner).await,
            RemoveMemberOutcome::WorkspaceNotFound
        );
        assert_eq!(
            role_for(db, workspace, owner).await,
            Some(MemberRole::Owner),
            "a refused change must leave the last owner in place"
        );
    }

    async fn assert_owner_floor_survives_races(db: &CoralDb) {
        // Each race holds two open transactions, so its future is boxed rather
        // than carried on this one's stack frame.
        Box::pin(assert_racing_removals_keep_an_owner(db)).await;
        Box::pin(assert_racing_demotions_keep_an_owner(db)).await;
        Box::pin(assert_racing_identical_adds_converge(db)).await;
    }

    /// Two owners revoked at once: the floor is a race, not a formality.
    async fn assert_racing_removals_keep_an_owner(db: &CoralDb) {
        let fixture = seed_two_owners(db).await;
        let (workspace, first, second) =
            (&fixture.workspace, &fixture.owner, &fixture.second_owner);
        let barrier = MembershipMutationBarrier::new();
        let holder = db.workspace_state_with_mutation_barrier(&barrier);
        let waiter = db.workspace_state();

        let (held, contended) = race_for_the_workspace_parent(
            Box::pin(holder.remove_member(workspace, first)),
            &barrier,
            Box::pin(waiter.remove_member(workspace, second)),
        )
        .await;

        assert_eq!(held.expect("the holding removal"), Removed);
        assert_contended(contended, &LastOwnerProtected);
        assert_eq!(
            owner_count(db, workspace).await,
            1,
            "racing removals must not empty the owner floor"
        );
    }

    /// The same race through demotion, which reaches the floor by another path.
    async fn assert_racing_demotions_keep_an_owner(db: &CoralDb) {
        let fixture = seed_two_owners(db).await;
        let (workspace, first, second) =
            (&fixture.workspace, &fixture.owner, &fixture.second_owner);
        let barrier = MembershipMutationBarrier::new();
        let holder = db.workspace_state_with_mutation_barrier(&barrier);
        let waiter = db.workspace_state();

        let (held, contended) = race_for_the_workspace_parent(
            Box::pin(holder.add_member(workspace, first, MemberRole::Member, 60)),
            &barrier,
            Box::pin(waiter.add_member(workspace, second, MemberRole::Member, 60)),
        )
        .await;

        assert_eq!(
            held.expect("the holding demotion"),
            promoted(first, MemberRole::Member)
        );
        assert_contended(contended, &AddMemberOutcome::LastOwnerProtected);
        assert_eq!(
            owner_count(db, workspace).await,
            1,
            "racing demotions must not empty the owner floor"
        );
    }

    /// Identical adds are the opposite case: both callers asked for the state
    /// the workspace ends in, so both must succeed on one row.
    async fn assert_racing_identical_adds_converge(db: &CoralDb) {
        let fixture = seed_two_owners(db).await;
        let (workspace, member) = (&fixture.workspace, &fixture.member);
        let barrier = MembershipMutationBarrier::new();
        let holder = db.workspace_state_with_mutation_barrier(&barrier);
        let waiter = db.workspace_state();

        let (held, contended) = race_for_the_workspace_parent(
            Box::pin(holder.add_member(workspace, member, MemberRole::Member, 70)),
            &barrier,
            Box::pin(waiter.add_member(workspace, member, MemberRole::Member, 80)),
        )
        .await;

        assert_eq!(
            held.expect("the holding add"),
            granted(member, MemberRole::Member)
        );
        // Convergence is a stronger contract than the owner floor's: there the
        // contender may legitimately lose the lock race, here it must see the
        // row the holder committed and report it unchanged.
        match contended {
            Ok(outcome) => assert_eq!(outcome, unchanged(member, MemberRole::Member)),
            Err(error) => panic!(
                "identical concurrent adds must both succeed on one row, but the contender failed: {error}"
            ),
        }
        assert_eq!(
            member_count(db, workspace).await,
            3,
            "identical concurrent adds must converge on one membership row"
        );
    }

    /// Drives `contending` into the parent-row hold that `held` sits on.
    ///
    /// The inner timeout is the proof, not a delay: the contender must still be
    /// unfinished while the holder owns the parent row, which is what makes the
    /// owner count it reads afterwards a fresh one rather than a lucky one.
    async fn race_for_the_workspace_parent<H, C>(
        held: impl Future<Output = H>,
        barrier: &MembershipMutationBarrier,
        contending: impl Future<Output = C>,
    ) -> (H, C) {
        let contending = async {
            barrier.wait_until_workspace_held().await;
            tokio::pin!(contending);
            assert!(
                tokio::time::timeout(Duration::from_millis(250), contending.as_mut())
                    .await
                    .is_err(),
                "a membership mutation must wait while another holds the workspace parent"
            );
            barrier.release_mutation().await;
            contending.await
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(held, contending)
        })
        .await
        .expect("the membership race must finish")
    }

    /// The contender either sees the state the holder committed or loses the
    /// lock race outright; any other failure is a defect, not contention.
    fn assert_contended<T>(contended: Result<T, DbError>, expected: &T)
    where
        T: std::fmt::Debug + PartialEq,
    {
        match contended {
            Ok(outcome) => assert_eq!(&outcome, expected),
            Err(error) => assert!(
                error.is_serialization_conflict(),
                "a racing mutation failed outside a lock race: {error}"
            ),
        }
    }

    fn granted(user_id: &str, role: MemberRole) -> AddMemberOutcome {
        AddMemberOutcome::Added(member_record(user_id, role))
    }

    fn unchanged(user_id: &str, role: MemberRole) -> AddMemberOutcome {
        AddMemberOutcome::ExistingSameRole(member_record(user_id, role))
    }

    fn promoted(user_id: &str, role: MemberRole) -> AddMemberOutcome {
        AddMemberOutcome::RoleUpdated(member_record(user_id, role))
    }

    fn member_record(user_id: &str, role: MemberRole) -> WorkspaceMemberRecord {
        WorkspaceMemberRecord {
            user_id: user_id.to_string(),
            display_name: Some(SEEDED_DISPLAY_NAME.to_string()),
            role,
        }
    }

    /// Mints one workspace id and three directory users for one test run.
    async fn seed(db: &CoralDb) -> Fixture {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        Fixture {
            workspace: format!("workspace_{suffix}"),
            owner: seed_user(db, &format!("owner_{suffix}")).await,
            second_owner: seed_user(db, &format!("second_owner_{suffix}")).await,
            member: seed_user(db, &format!("member_{suffix}")).await,
        }
    }

    /// Seeds a workspace that two owners share, so the floor is one write away.
    async fn seed_two_owners(db: &CoralDb) -> Fixture {
        let fixture = seed(db).await;
        assert_eq!(
            create(db, &fixture.workspace, &fixture.owner).await,
            Created
        );
        assert_eq!(
            add(
                db,
                &fixture.workspace,
                &fixture.second_owner,
                MemberRole::Owner,
                10
            )
            .await,
            granted(&fixture.second_owner, MemberRole::Owner)
        );
        fixture
    }

    async fn seed_user(db: &CoralDb, subject: &str) -> String {
        let mut session = db;
        match session
            .users()
            .upsert_login(
                "https://issuer.test/workspace-state",
                subject,
                Some(SEEDED_DISPLAY_NAME),
                1,
            )
            .await
            .expect("provision user")
        {
            UpsertLoginOutcome::Upserted(user) => user.user_id,
            UpsertLoginOutcome::IssuerMismatch { stored_issuer } => {
                panic!("expected a provisioned user, got a mismatch with issuer {stored_issuer}")
            }
        }
    }

    async fn create(db: &CoralDb, workspace: &str, owner: &str) -> CreateWorkspaceOutcome {
        db.workspace_state()
            .create_owned_by(workspace, owner, 1)
            .await
            .expect("create workspace")
    }

    async fn add(
        db: &CoralDb,
        workspace: &str,
        user: &str,
        role: MemberRole,
        at: i64,
    ) -> AddMemberOutcome {
        db.workspace_state()
            .add_member(workspace, user, role, at)
            .await
            .expect("add member")
    }

    async fn remove(db: &CoralDb, workspace: &str, user: &str) -> RemoveMemberOutcome {
        db.workspace_state()
            .remove_member(workspace, user)
            .await
            .expect("remove member")
    }

    async fn workspace_exists(db: &CoralDb, workspace: &str) -> bool {
        let mut session = db;
        session
            .workspaces()
            .get(workspace)
            .await
            .expect("get workspace")
            .is_some()
    }

    async fn role_for(db: &CoralDb, workspace: &str, user: &str) -> Option<MemberRole> {
        let mut session = db;
        session
            .workspace_members()
            .role_for_user_id(workspace, user)
            .await
            .expect("read role")
    }

    async fn owner_count(db: &CoralDb, workspace: &str) -> i64 {
        let mut session = db;
        session
            .workspace_members()
            .owner_count(workspace)
            .await
            .expect("count owners")
    }

    /// Counts every membership row of one workspace, owners included, which the
    /// repository deliberately does not expose.
    async fn member_count(db: &CoralDb, workspace: &str) -> i64 {
        let statement = Query::select()
            .expr(Func::count(Expr::val(1)))
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::WorkspaceId).eq(workspace))
            .to_owned();
        let counted: Option<(i64,)> = fetch(db, statement).await;
        counted.unwrap_or_default().0
    }

    async fn granted_at(db: &CoralDb, workspace: &str, user: &str) -> i64 {
        let statement = Query::select()
            .column(WorkspaceMembers::CreatedAtUnixNanos)
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::WorkspaceId).eq(workspace))
            .and_where(Expr::col(WorkspaceMembers::UserId).eq(user))
            .to_owned();
        let granted: Option<(i64,)> = fetch(db, statement).await;
        granted.expect("membership exists").0
    }

    async fn fetch(db: &CoralDb, statement: sea_query::SelectStatement) -> Option<(i64,)> {
        let mut session = db;
        session.fetch_optional(statement).await.expect("read row")
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
