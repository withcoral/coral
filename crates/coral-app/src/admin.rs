//! Operator recovery over a deployment's persisted state database.
//!
//! Gated behind the `admin` feature, which the shipped `coral` binary leaves
//! off. This is deliberately not a product surface: it authenticates nobody and
//! authorizes nothing, so possessing it is equivalent to possessing the state
//! database itself.
//!
//! It exists because a workspace with no owner cannot be repaired through any
//! RPC. Once `[auth]` is configured, `coral:local` is not an implicit owner, so
//! a deployment that upgraded into memberships has workspaces that every caller
//! sees as `NotFound` — the operator included. The repair therefore has to
//! happen underneath the authorization plane, from a process that already holds
//! the state directory. The server need not be stopped: membership is read per
//! request, so an appointed owner takes effect on their next call.
//!
//! Locality follows the backend and is not stronger than it: `SQLite` requires
//! filesystem access to the state directory, while Postgres requires only
//! possession of the connection URL, which no code here can confine to the
//! machine running the server.

use std::path::PathBuf;

use crate::bootstrap::{discover_app_state_layout, env_var};
use crate::state::db::{
    CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, now_unix_nanos_i64,
};
use crate::workspaces::MemberRole;

/// A recovery operation could not be carried out.
///
/// Failures to reach or read the state database are errors; a named workspace
/// or user that simply does not exist is reported as an outcome instead, so
/// callers can phrase it for an operator.
#[derive(Debug)]
pub struct AdminError(String);

impl std::fmt::Display for AdminError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for AdminError {}

/// One workspace with the membership counts that expose a lock-out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceSummary {
    /// Persisted workspace name — the value [`AdminDb::set_owner`] takes.
    pub name: String,
    /// Members holding the `owner` role. Zero means nobody can reach it.
    pub owner_count: usize,
    /// Members in any role, owners included.
    pub member_count: usize,
}

/// One provisioned user, as [`AdminDb::set_owner`] identifies them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserSummary {
    /// Internal user ID, minted at first login.
    pub user_id: String,
    /// Name the provider sent at the last login, when it sent one.
    pub display_name: Option<String>,
    /// OIDC issuer this identity is bound to.
    pub issuer: String,
    /// Provider's subject claim for this person.
    ///
    /// Identifying, and frequently an email address. Renderers should leave it
    /// out unless the operator asks for it; see `xtask workspace-admin
    /// list-users --show-subjects`.
    pub subject: String,
}

/// What [`AdminDb::set_owner`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOwnerOutcome {
    /// The user was not a member and is now an owner.
    Added,
    /// The user was a plain member and is now an owner.
    Promoted,
    /// The user was already an owner; nothing was written.
    Unchanged,
    /// No workspace by that name exists.
    WorkspaceNotFound,
    /// No user by that ID exists. Users appear at their first login.
    UserNotFound,
}

/// A repair session over one deployment's state database.
#[derive(Debug)]
pub struct AdminDb {
    db: CoralDb,
}

impl AdminDb {
    /// Opens the same state database the server reads.
    ///
    /// `config_dir` overrides `CORAL_CONFIG_DIR`, which in turn overrides the
    /// platform app-state directory. `[database]` is resolved exactly as the
    /// server resolves it, including reading `url_env` from the environment for
    /// the Postgres backend.
    ///
    /// Migrations are never run. A tool built from a newer checkout must not be
    /// able to move a running deployment's schema forward as a side effect of
    /// listing its workspaces.
    ///
    /// # Errors
    ///
    /// Returns an error when the state directory, `[database]` configuration,
    /// or the database itself cannot be resolved or opened.
    pub async fn open(config_dir: Option<PathBuf>) -> Result<Self, AdminError> {
        let layout = discover_app_state_layout(config_dir).map_err(describe)?;
        let config = match DatabaseConfig::load(&layout).map_err(describe)? {
            DatabaseConfig::Sqlite { path } => {
                if !path.try_exists().map_err(describe)? {
                    return Err(AdminError(format!(
                        "no state database at {}; point --state-dir or CORAL_CONFIG_DIR at the directory the server uses",
                        path.display()
                    )));
                }
                ResolvedDatabaseConfig::Sqlite { path }
            }
            DatabaseConfig::Postgres { url_env } => {
                let url = env_var(&url_env)
                    .map_err(|_error| {
                        AdminError(format!(
                            "[database].url_env `{url_env}` must contain valid UTF-8"
                        ))
                    })?
                    .ok_or_else(|| {
                        AdminError(format!(
                            "[database].backend is 'postgres'; set `{url_env}` to the connection URL"
                        ))
                    })?;
                ResolvedDatabaseConfig::Postgres { url }
            }
        };
        let db = CoralDb::open(config).await.map_err(describe)?;
        Ok(Self { db })
    }

    /// Lists every workspace with its owner and member counts.
    ///
    /// This is how an operator finds ownerless workspaces: nothing else reports
    /// them, because an ownerless workspace is invisible to every RPC caller.
    ///
    /// # Errors
    ///
    /// Returns an error when the state database cannot be read.
    pub async fn list_workspaces(&self) -> Result<Vec<WorkspaceSummary>, AdminError> {
        let workspaces = {
            let mut session = &self.db;
            session.workspaces().list().await.map_err(describe)?
        };
        let mut summaries = Vec::with_capacity(workspaces.len());
        for workspace in workspaces {
            let members = self
                .db
                .list_workspace_members(&workspace.id)
                .await
                .map_err(describe)?
                .unwrap_or_default();
            summaries.push(WorkspaceSummary {
                name: workspace.id,
                owner_count: members
                    .iter()
                    .filter(|member| member.role == MemberRole::Owner)
                    .count(),
                member_count: members.len(),
            });
        }
        Ok(summaries)
    }

    /// Lists every provisioned user, ordered by internal user ID.
    ///
    /// Users are created at their first successful login, so this is empty on a
    /// deployment nobody has signed into yet.
    ///
    /// # Errors
    ///
    /// Returns an error when the state database cannot be read.
    pub async fn list_users(&self) -> Result<Vec<UserSummary>, AdminError> {
        let mut session = &self.db;
        let users = session.users().list().await.map_err(describe)?;
        Ok(users
            .into_iter()
            .map(|user| UserSummary {
                user_id: user.user_id,
                display_name: user.display_name,
                issuer: user.issuer,
                subject: user.subject,
            })
            .collect())
    }

    /// Makes `user_id` an owner of `workspace`, in one transaction.
    ///
    /// This only ever adds an owner, so the at-least-one-owner rule the
    /// membership RPCs enforce cannot be violated here — and a workspace that
    /// starts with zero owners is exactly the case this repairs, not a case it
    /// refuses. A promotion from `member` replaces the row inside the same
    /// transaction, so the workspace is never observably ownerless partway
    /// through. Re-running it is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error when the state database cannot be read or written.
    pub async fn set_owner(
        &self,
        workspace: &str,
        user_id: &str,
    ) -> Result<SetOwnerOutcome, AdminError> {
        let now_unix_nanos = now_unix_nanos_i64().map_err(describe)?;
        let mut tx = self.db.begin().await.map_err(describe)?;
        if !tx
            .workspaces()
            .hold_for_child_mutation(workspace)
            .await
            .map_err(describe)?
        {
            tx.rollback().await.map_err(describe)?;
            return Ok(SetOwnerOutcome::WorkspaceNotFound);
        }
        if tx
            .users()
            .get_by_user_id(user_id)
            .await
            .map_err(describe)?
            .is_none()
        {
            tx.rollback().await.map_err(describe)?;
            return Ok(SetOwnerOutcome::UserNotFound);
        }

        let outcome = match tx
            .workspace_members()
            .role_for_user_id_for_recovery(workspace, user_id)
            .await
            .map_err(describe)?
        {
            Some(MemberRole::Owner) => {
                tx.rollback().await.map_err(describe)?;
                return Ok(SetOwnerOutcome::Unchanged);
            }
            Some(MemberRole::Member) => {
                tx.workspace_members()
                    .delete(workspace, user_id)
                    .await
                    .map_err(describe)?;
                SetOwnerOutcome::Promoted
            }
            None => SetOwnerOutcome::Added,
        };
        tx.workspace_members()
            .insert(workspace, user_id, MemberRole::Owner, now_unix_nanos)
            .await
            .map_err(describe)?;
        tx.commit().await.map_err(describe)?;
        Ok(outcome)
    }

    /// Rebinds every identity on `from_issuer` to `to_issuer`.
    ///
    /// Retires the raw `UPDATE users SET issuer = ...` the recovery runbook used
    /// to document. Returns the number of user rows the update changed, so the
    /// operator can compare it against the population they expected to move.
    ///
    /// # Errors
    ///
    /// Returns an error when the state database cannot be written.
    pub async fn rebind_issuer(
        &self,
        from_issuer: &str,
        to_issuer: &str,
    ) -> Result<u64, AdminError> {
        let mut session = &self.db;
        session
            .users()
            .rebind_issuer(from_issuer, to_issuer)
            .await
            .map_err(describe)
    }
}

fn describe<E: std::fmt::Display>(error: E) -> AdminError {
    AdminError(error.to_string())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{AdminDb, SetOwnerOutcome, WorkspaceSummary};
    use crate::state::AppStateLayout;
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, UpsertLoginOutcome,
        now_unix_nanos_i64,
    };
    use crate::workspaces::MemberRole;

    struct Fixture {
        _temp: TempDir,
        config_dir: std::path::PathBuf,
        db: CoralDb,
    }

    impl Fixture {
        async fn migrated() -> Self {
            let temp = tempfile::tempdir().expect("temp dir");
            let config_dir = temp.path().join("coral");
            let layout = AppStateLayout::discover(Some(config_dir.clone())).expect("layout");
            layout.ensure().expect("ensure layout");
            let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
            else {
                panic!("the default test configuration must be SQLite");
            };
            let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite");
            db.migrate().await.expect("migrate sqlite");
            Self {
                _temp: temp,
                config_dir,
                db,
            }
        }

        async fn admin(&self) -> AdminDb {
            AdminDb::open(Some(self.config_dir.clone()))
                .await
                .expect("open admin database")
        }

        async fn add_user(
            &self,
            issuer: &str,
            subject: &str,
            display_name: Option<&str>,
        ) -> String {
            let now = now_unix_nanos_i64().expect("clock");
            let mut tx = self.db.begin().await.expect("begin");
            let outcome = tx
                .users()
                .upsert_login(issuer, subject, display_name, now)
                .await
                .expect("upsert login");
            tx.commit().await.expect("commit");
            match outcome {
                UpsertLoginOutcome::Upserted(user) => user.user_id,
                UpsertLoginOutcome::IssuerMismatch { stored_issuer } => {
                    panic!("unexpected issuer mismatch against {stored_issuer}")
                }
            }
        }

        async fn add_workspace(&self, name: &str, members: &[(&str, MemberRole)]) {
            let now = now_unix_nanos_i64().expect("clock");
            let mut tx = self.db.begin().await.expect("begin");
            tx.workspaces().create(name, now).await.expect("create");
            for (user_id, role) in members {
                tx.workspace_members()
                    .insert(name, user_id, *role, now)
                    .await
                    .expect("insert member");
            }
            tx.commit().await.expect("commit");
        }
    }

    fn summary(name: &str, owner_count: usize, member_count: usize) -> WorkspaceSummary {
        WorkspaceSummary {
            name: name.to_string(),
            owner_count,
            member_count,
        }
    }

    #[tokio::test]
    async fn list_workspaces_separates_owned_from_ownerless_workspaces() {
        let fixture = Fixture::migrated().await;
        let owner = fixture
            .add_user("https://issuer", "owner@example.com", None)
            .await;
        let member = fixture
            .add_user("https://issuer", "member@example.com", None)
            .await;
        fixture
            .add_workspace(
                "healthy",
                &[
                    (owner.as_str(), MemberRole::Owner),
                    (member.as_str(), MemberRole::Member),
                ],
            )
            .await;
        fixture
            .add_workspace("stranded", &[(member.as_str(), MemberRole::Member)])
            .await;
        fixture.add_workspace("abandoned", &[]).await;

        let workspaces = fixture.admin().await.list_workspaces().await.expect("list");

        assert_eq!(
            workspaces,
            vec![
                summary("abandoned", 0, 0),
                summary("healthy", 1, 2),
                summary("stranded", 0, 1),
            ]
        );
    }

    #[tokio::test]
    async fn list_users_maps_a_person_to_the_internal_user_id() {
        let fixture = Fixture::migrated().await;
        let user_id = fixture
            .add_user("https://issuer", "ada@example.com", Some("Ada"))
            .await;

        let users = fixture.admin().await.list_users().await.expect("list");

        assert_eq!(users.len(), 1);
        let user = users.first().expect("one user");
        assert_eq!(user.user_id, user_id);
        assert_eq!(user.display_name.as_deref(), Some("Ada"));
        assert_eq!(user.issuer, "https://issuer");
        assert_eq!(user.subject, "ada@example.com");
    }

    #[tokio::test]
    async fn set_owner_repairs_a_zero_owner_workspace_and_repeats_cleanly() {
        let fixture = Fixture::migrated().await;
        let member = fixture
            .add_user("https://issuer", "member@example.com", None)
            .await;
        let stranger = fixture
            .add_user("https://issuer", "stranger@example.com", None)
            .await;
        fixture
            .add_workspace("stranded", &[(member.as_str(), MemberRole::Member)])
            .await;
        fixture.add_workspace("abandoned", &[]).await;
        let admin = fixture.admin().await;

        assert_eq!(
            admin.set_owner("stranded", &member).await.expect("promote"),
            SetOwnerOutcome::Promoted
        );
        assert_eq!(
            admin.set_owner("abandoned", &stranger).await.expect("add"),
            SetOwnerOutcome::Added
        );
        assert_eq!(
            admin.list_workspaces().await.expect("list"),
            vec![summary("abandoned", 1, 1), summary("stranded", 1, 1)]
        );

        assert_eq!(
            admin.set_owner("stranded", &member).await.expect("rerun"),
            SetOwnerOutcome::Unchanged
        );
        assert_eq!(
            admin.list_workspaces().await.expect("list"),
            vec![summary("abandoned", 1, 1), summary("stranded", 1, 1)]
        );
    }

    #[tokio::test]
    async fn set_owner_reports_unknown_names_without_writing() {
        let fixture = Fixture::migrated().await;
        let user = fixture
            .add_user("https://issuer", "ada@example.com", None)
            .await;
        fixture.add_workspace("stranded", &[]).await;
        let admin = fixture.admin().await;

        assert_eq!(
            admin
                .set_owner("stranded", "not-a-user")
                .await
                .expect("unknown user"),
            SetOwnerOutcome::UserNotFound
        );
        assert_eq!(
            admin
                .set_owner("not-a-workspace", &user)
                .await
                .expect("unknown workspace"),
            SetOwnerOutcome::WorkspaceNotFound
        );
        assert_eq!(
            admin.list_workspaces().await.expect("list"),
            vec![summary("stranded", 0, 0)]
        );
    }

    #[tokio::test]
    async fn rebind_issuer_moves_exactly_the_counted_rows() {
        let fixture = Fixture::migrated().await;
        for subject in ["ada@example.com", "grace@example.com"] {
            fixture.add_user("https://old", subject, None).await;
        }
        fixture
            .add_user("https://other", "alan@example.com", None)
            .await;
        let admin = fixture.admin().await;

        let moved = admin
            .rebind_issuer("https://old", "https://new")
            .await
            .expect("rebind");

        assert_eq!(moved, 2);
        let mut issuers: Vec<String> = admin
            .list_users()
            .await
            .expect("list")
            .into_iter()
            .map(|user| user.issuer)
            .collect();
        issuers.sort();
        assert_eq!(issuers, ["https://new", "https://new", "https://other"]);

        assert_eq!(
            admin
                .rebind_issuer("https://old", "https://new")
                .await
                .expect("rerun"),
            0
        );
    }

    #[tokio::test]
    async fn open_names_the_missing_state_database() {
        let temp = tempfile::tempdir().expect("temp dir");
        let error = AdminDb::open(Some(temp.path().join("absent")))
            .await
            .expect_err("a state directory with no database must be refused");

        assert!(
            error.to_string().contains("no state database at"),
            "unexpected error: {error}"
        );
    }
}
