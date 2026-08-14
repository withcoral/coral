//! Transactional persistence for login identity provisioning.
//!
//! Provisioning is identity-only. It refreshes the directory row for a verified
//! login and rewrites that caller's pre-v1 task attribution in the same
//! transaction; it never creates, selects, or grants access to a workspace.

use super::repositories::users::{UpsertLoginOutcome, UserRecord};
use super::{CoralDb, DbError, DbRepos};

/// A verified upstream login to provision.
pub(crate) struct LoginIdentity<'a> {
    pub(crate) issuer: &'a str,
    pub(crate) subject: &'a str,
    pub(crate) display_name: Option<&'a str>,
    /// Value of the configured `principal_claim`.
    ///
    /// It is carried only to recompute the pre-v1 task-attribution id. It is
    /// not the durable identity key and never reaches the directory row.
    pub(crate) principal_claim: &'a str,
    pub(crate) now_unix_nanos: i64,
}

/// Outcome of provisioning one verified login.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LoginProvisioning {
    Provisioned(UserRecord),
    /// The subject is already bound to another issuer, so login must fail
    /// rather than rebind automatically.
    IssuerMismatch {
        stored_issuer: String,
    },
}

pub(crate) struct UserState<'a> {
    db: &'a CoralDb,
    #[cfg(test)]
    fail_before_commit: bool,
}

impl CoralDb {
    pub(crate) fn user_state(&self) -> UserState<'_> {
        UserState {
            db: self,
            #[cfg(test)]
            fail_before_commit: false,
        }
    }

    /// Builds a [`UserState`] that fails after both writes and before the
    /// commit, so tests can observe the transaction boundary.
    #[cfg(test)]
    pub(crate) fn user_state_failing_before_commit(&self) -> UserState<'_> {
        UserState {
            db: self,
            fail_before_commit: true,
        }
    }
}

impl UserState<'_> {
    /// Refreshes the directory row for a verified login and reattributes that
    /// caller's pre-v1 task rows in one transaction.
    pub(crate) async fn provision_login(
        &self,
        login: LoginIdentity<'_>,
    ) -> Result<LoginProvisioning, DbError> {
        let mut tx = self.db.begin().await?;
        let outcome = tx
            .users()
            .upsert_login(
                login.issuer,
                login.subject,
                login.display_name,
                login.now_unix_nanos,
            )
            .await?;
        let user = match outcome {
            UpsertLoginOutcome::Upserted(user) => user,
            UpsertLoginOutcome::IssuerMismatch { stored_issuer } => {
                tx.rollback().await?;
                return Ok(LoginProvisioning::IssuerMismatch { stored_issuer });
            }
        };
        tx.tasks()
            .reattribute_pre_v1_creator(
                &pre_v1_task_attribution_id(login.principal_claim),
                &user.user_id,
            )
            .await?;
        #[cfg(test)]
        if self.fail_before_commit {
            // Dropping `tx` unsent rolls the whole transaction back, which is
            // the same path a repository failure takes.
            return Err(DbError::CorruptData(
                "injected login provisioning failure".to_string(),
            ));
        }
        tx.commit().await?;
        Ok(LoginProvisioning::Provisioned(user))
    }
}

/// Recomputes the pre-v1 task-attribution id for a configured principal claim.
///
/// Before the user directory existed, `tasks.created_by_principal_id` held an
/// unkeyed digest of the configured principal claim. This helper exists solely
/// to rewrite those historical rows onto the internal `user_id`; it must never
/// be used to look up a user, membership, workspace, or permission. Its
/// preimage is frozen by contract with already-written rows, so it cannot be
/// changed without abandoning their attribution.
fn pre_v1_task_attribution_id(principal_claim: &str) -> String {
    let mut preimage = Vec::with_capacity(principal_claim.len() + 32);
    preimage.extend_from_slice(b"coral-federated-user-v1\0");
    preimage.extend_from_slice(&(principal_claim.len() as u64).to_be_bytes());
    preimage.extend_from_slice(principal_claim.as_bytes());
    format!("federated-{}", crate::hash::sha256_hex(&preimage))
}

#[cfg(test)]
mod tests {
    use sea_query::{Expr, ExprTrait, Func, IntoColumnRef, IntoTableRef, Query};
    use tempfile::{TempDir, tempdir};

    use super::{LoginIdentity, LoginProvisioning, UserRecord, pre_v1_task_attribution_id};
    use crate::bootstrap;
    use crate::state::db::schema::{Tasks, WorkspaceMembers, Workspaces};
    use crate::state::db::{CoralDb, DbRepos, DbSession, ResolvedDatabaseConfig};

    /// Row counts that login provisioning must never move.
    #[derive(Debug, PartialEq, Eq)]
    struct AccessCounts {
        workspaces: i64,
        workspace_members: i64,
    }

    /// Workspace access a login must never gain, scoped to one login's own rows.
    ///
    /// The shared contract cannot assert this with an instance-wide `COUNT(*)`:
    /// `make postgres-tests` runs every `contract_on_postgres` test
    /// concurrently against **one** database, so a global count drifts under
    /// sibling inserts that have nothing to do with this login. Scoping to the
    /// provisioned user and the seeded workspace states the same invariant —
    /// this login gained no workspace access — exactly and independently of
    /// whatever else is running. The instance-wide claim, that provisioning
    /// creates no workspace or membership *anywhere*, is asserted by the `SQLite`
    /// test below, whose database is private to it.
    #[derive(Debug, PartialEq, Eq)]
    struct GrantedAccess {
        memberships_for_user: i64,
        members_of_seeded_workspace: i64,
    }

    impl GrantedAccess {
        const NONE: Self = Self {
            memberships_for_user: 0,
            members_of_seeded_workspace: 0,
        };
    }

    #[tokio::test]
    async fn login_provisioning_contract_holds_against_sqlite() {
        let (db, _temp) = open_sqlite().await;
        let seeded_workspaces = assert_login_provisioning_contract(&db).await;

        // Sound only here: this database is private to this test, so an
        // instance-wide count proves the stronger claim the scoped assertions
        // inside the contract cannot — that none of those logins created a
        // workspace or a membership row anywhere, only the ones seeded for them.
        assert_eq!(
            access_counts(&db).await,
            AccessCounts {
                workspaces: seeded_workspaces,
                workspace_members: 0,
            },
            "login provisioning must not write a workspace or a membership"
        );
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared contract against Postgres"]
    async fn login_provisioning_contract_on_postgres() {
        let Some(db) = open_postgres().await else {
            return;
        };
        assert_login_provisioning_contract(&db).await;
    }

    #[tokio::test]
    async fn login_provisioning_failure_rolls_back_identity_and_reattribution() {
        let (db, _temp) = open_sqlite().await;
        let login = seed_legacy_login(&db, "rollback").await;
        let counts = access_counts(&db).await;

        db.user_state_failing_before_commit()
            .provision_login(login.identity(400))
            .await
            .expect_err("injected failure fails provisioning");

        assert_eq!(
            task_creator(&db, &login.workspace_id, &login.legacy_task_id).await,
            pre_v1_task_attribution_id(&login.principal_claim),
            "a failed login must leave pre-v1 task attribution in place"
        );
        let mut session = &db;
        assert!(
            session
                .users()
                .list()
                .await
                .expect("list users")
                .iter()
                .all(|user| user.subject != login.subject),
            "a failed login must not leave a directory row behind"
        );
        assert_eq!(access_counts(&db).await, counts);
    }

    #[tokio::test]
    async fn pre_v1_task_attribution_id_matches_the_legacy_task_digest() {
        // Pinned against the pre-v1 derivation that wrote the rows this helper
        // migrates. Changing it orphans their attribution.
        assert_eq!(
            pre_v1_task_attribution_id("alice"),
            "federated-9421daa937c9833c60a142d34d89edef203c0af23e4d93c4ef4e968c26e80b6a"
        );
    }

    /// Returns how many workspaces the contract seeded, so a caller running on
    /// a private database can hold provisioning to an instance-wide count.
    async fn assert_login_provisioning_contract(db: &CoralDb) -> i64 {
        let login = seed_legacy_login(db, "contract").await;

        let user = expect_provisioned(
            db.user_state()
                .provision_login(login.identity(100))
                .await
                .expect("first login"),
        );
        assert_eq!(
            task_creator(db, &login.workspace_id, &login.legacy_task_id).await,
            user.user_id,
            "a matching pre-v1 task must be reattributed to the internal user id"
        );
        assert_eq!(
            task_creator(db, &login.workspace_id, &login.other_task_id).await,
            login.other_principal_id,
            "another identity's pre-v1 task must keep its attribution"
        );
        assert_eq!(
            granted_access(db, &user.user_id, &login.workspace_id).await,
            GrantedAccess::NONE,
            "login provisioning must not write a workspace membership"
        );

        let refreshed = expect_provisioned(
            db.user_state()
                .provision_login(login.identity(200))
                .await
                .expect("second login"),
        );
        assert_eq!(refreshed.user_id, user.user_id);
        assert_eq!(refreshed.last_login_at_unix_nanos, 200);

        assert_issuer_mismatch_leaves_attribution(db, &login).await;
        // One for this contract, one for the rebound login it asserts against.
        2
    }

    async fn assert_issuer_mismatch_leaves_attribution(db: &CoralDb, login: &SeededLogin) {
        let rebound = seed_legacy_login(db, &format!("{}-rebound", login.suffix)).await;
        // Mint the subject under its own issuer with a claim that matches no
        // task, so its seeded pre-v1 row is still waiting to be reattributed.
        let bound_user = expect_provisioned(
            db.user_state()
                .provision_login(LoginIdentity {
                    principal_claim: "unmatched claim",
                    ..rebound.identity(300)
                })
                .await
                .expect("rebound first login"),
        );

        let mismatch = db
            .user_state()
            .provision_login(LoginIdentity {
                issuer: &login.issuer,
                ..rebound.identity(400)
            })
            .await
            .expect("rebound login under another issuer");

        assert_eq!(
            mismatch,
            LoginProvisioning::IssuerMismatch {
                stored_issuer: rebound.issuer.clone(),
            }
        );
        assert_eq!(
            task_creator(db, &rebound.workspace_id, &rebound.legacy_task_id).await,
            pre_v1_task_attribution_id(&rebound.principal_claim),
            "a mismatched login must not reattribute anything"
        );
        assert_eq!(
            granted_access(db, &bound_user.user_id, &rebound.workspace_id).await,
            GrantedAccess::NONE,
            "a refused login must not grant the bound user any workspace access"
        );
    }

    struct SeededLogin {
        suffix: String,
        issuer: String,
        subject: String,
        principal_claim: String,
        workspace_id: String,
        legacy_task_id: String,
        other_task_id: String,
        other_principal_id: String,
    }

    impl SeededLogin {
        fn identity(&self, now_unix_nanos: i64) -> LoginIdentity<'_> {
            LoginIdentity {
                issuer: &self.issuer,
                subject: &self.subject,
                display_name: Some("Seeded User"),
                principal_claim: &self.principal_claim,
                now_unix_nanos,
            }
        }
    }

    /// Creates a workspace holding one pre-v1 task attributed to the returned
    /// identity's claim digest and one attributed to an unrelated digest. No
    /// directory row is minted; the login under test does that.
    async fn seed_legacy_login(db: &CoralDb, label: &str) -> SeededLogin {
        let suffix = format!("{label}_{}", uuid::Uuid::new_v4().simple());
        let login = SeededLogin {
            issuer: format!("https://issuer.test/{suffix}"),
            subject: format!("subject_{suffix}"),
            principal_claim: format!("claim_{suffix}"),
            workspace_id: format!("workspace_{suffix}"),
            legacy_task_id: format!("legacy_task_{suffix}"),
            other_task_id: format!("other_task_{suffix}"),
            other_principal_id: pre_v1_task_attribution_id(&format!("other_{suffix}")),
            suffix,
        };

        let mut tx = db.begin().await.expect("begin seed");
        tx.workspaces()
            .create(&login.workspace_id, 1)
            .await
            .expect("create workspace");
        tx.tasks()
            .insert(
                &login.workspace_id,
                &pre_v1_task_attribution_id(&login.principal_claim),
                &login.legacy_task_id,
                "legacy intent",
                1,
            )
            .await
            .expect("insert legacy task");
        tx.tasks()
            .insert(
                &login.workspace_id,
                &login.other_principal_id,
                &login.other_task_id,
                "other intent",
                1,
            )
            .await
            .expect("insert unrelated task");
        tx.commit().await.expect("commit seed");
        login
    }

    async fn access_counts(db: &CoralDb) -> AccessCounts {
        AccessCounts {
            workspaces: count_rows(db, Workspaces::Table).await,
            workspace_members: count_rows(db, WorkspaceMembers::Table).await,
        }
    }

    /// The workspace access one login holds: its memberships anywhere, and the
    /// members of the workspace seeded for it.
    async fn granted_access(db: &CoralDb, user_id: &str, workspace_id: &str) -> GrantedAccess {
        GrantedAccess {
            memberships_for_user: count_matching(db, WorkspaceMembers::UserId, user_id).await,
            members_of_seeded_workspace: count_matching(
                db,
                WorkspaceMembers::WorkspaceId,
                workspace_id,
            )
            .await,
        }
    }

    async fn count_matching<C>(db: &CoralDb, column: C, value: &str) -> i64
    where
        C: IntoColumnRef,
    {
        let mut session = db;
        let statement = Query::select()
            .expr(Func::count(Expr::val(1)))
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(column).eq(value))
            .to_owned();
        let (count,): (i64,) = session
            .fetch_optional(statement)
            .await
            .expect("count matching rows")
            .unwrap_or_default();
        count
    }

    async fn count_rows<T>(db: &CoralDb, table: T) -> i64
    where
        T: IntoTableRef,
    {
        let mut session = db;
        let statement = Query::select()
            .expr(Func::count(Expr::val(1)))
            .from(table)
            .to_owned();
        let (count,): (i64,) = session
            .fetch_optional(statement)
            .await
            .expect("count rows")
            .unwrap_or_default();
        count
    }

    async fn task_creator(db: &CoralDb, workspace_id: &str, task_id: &str) -> String {
        let mut session = db;
        let statement = Query::select()
            .column(Tasks::CreatedByPrincipalId)
            .from(Tasks::Table)
            .and_where(Expr::col(Tasks::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(Tasks::Id).eq(task_id))
            .to_owned();
        let (creator,): (String,) = session
            .fetch_optional(statement)
            .await
            .expect("read task attribution")
            .expect("task exists");
        creator
    }

    fn expect_provisioned(outcome: LoginProvisioning) -> UserRecord {
        match outcome {
            LoginProvisioning::Provisioned(user) => user,
            LoginProvisioning::IssuerMismatch { stored_issuer } => {
                panic!("expected a provisioned login, got a mismatch with issuer {stored_issuer}")
            }
        }
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
