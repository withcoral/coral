//! Repository-only recovery over an existing Coral state database.
//!
//! The `admin` feature is off by default and is used only by `xtask`. Database
//! possession is the authority: this surface authenticates nobody, never runs
//! migrations, and is not part of the shipped Coral product.

#![expect(
    clippy::missing_errors_doc,
    reason = "AdminError preserves backend context"
)]

use std::path::{Path, PathBuf};

use crate::bootstrap::discover_app_state_layout;
use crate::identity::LOCAL_PRINCIPAL_ID;
use crate::state::db::{
    CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, now_unix_nanos_i64,
};
use crate::workspaces::MemberRole;

/// A repository recovery operation failed.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct AdminError(String);

/// One persisted workspace and its shared-mode ownership state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceSummary {
    /// Persisted workspace name accepted by [`AdminDb::set_owner`].
    pub name: String,
    /// Non-local owners. Zero means shared callers cannot reach the workspace.
    pub owner_count: usize,
    /// All persisted memberships, including a retained local membership.
    pub member_count: usize,
}

/// One authenticated identity already present in the user directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserSummary {
    /// Stable internal identifier accepted by [`AdminDb::set_owner`].
    pub user_id: String,
    /// Latest display name supplied by the identity provider, when present.
    pub display_name: Option<String>,
    /// Identity-provider issuer currently bound to this user.
    pub issuer: String,
    /// Provider subject; render only after an explicit operator request.
    pub subject: String,
}

/// Result of appointing an owner beneath the authorization plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOwnerOutcome {
    /// The user had no membership and was added as an owner.
    Added,
    /// The user was already a member and was promoted to owner.
    Promoted,
    /// The user was already an owner, so no membership changed.
    Unchanged,
    /// No workspace matched the requested name.
    WorkspaceNotFound,
    /// No user matched the requested internal identifier.
    UserNotFound,
}

/// Recovery access to one deployment's existing state database.
#[derive(Debug)]
pub struct AdminDb {
    db: CoralDb,
}

impl AdminDb {
    /// Opens an existing database without running schema or state migrations.
    ///
    /// `state_dir` wins over `CORAL_CONFIG_DIR`, which wins over Coral's local
    /// state directory. A missing `SQLite` file is refused rather than created.
    pub async fn open(state_dir: Option<PathBuf>) -> Result<Self, AdminError> {
        let layout = discover_app_state_layout(state_dir).map_err(describe)?;
        let resolved = match DatabaseConfig::load(&layout).map_err(describe)? {
            DatabaseConfig::Sqlite { path } => {
                require_existing_database_file(&path)?;
                ResolvedDatabaseConfig::Sqlite { path }
            }
            DatabaseConfig::Postgres { url_env } => ResolvedDatabaseConfig::Postgres {
                url: read_database_url(&url_env)?,
            },
        };
        Self::open_resolved(resolved).await
    }

    async fn open_resolved(resolved: ResolvedDatabaseConfig) -> Result<Self, AdminError> {
        Ok(Self {
            db: CoralDb::open_existing(resolved).await.map_err(describe)?,
        })
    }

    /// Lists every workspace, including state concealed from normal callers.
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
                    .filter(|member| {
                        member.role == MemberRole::Owner
                            && member.user_id.as_str() != LOCAL_PRINCIPAL_ID
                    })
                    .count(),
                member_count: members.len(),
            });
        }
        Ok(summaries)
    }

    /// Lists authenticated identities in deterministic internal-ID order.
    pub async fn list_users(&self) -> Result<Vec<UserSummary>, AdminError> {
        let mut session = &self.db;
        Ok(session
            .users()
            .list()
            .await
            .map_err(describe)?
            .into_iter()
            .filter(|user| user.user_id != LOCAL_PRINCIPAL_ID)
            .map(|user| UserSummary {
                user_id: user.user_id,
                display_name: user.display_name,
                issuer: user.issuer,
                subject: user.subject,
            })
            .collect())
    }

    /// Adds or promotes an existing non-local user to Owner atomically.
    pub async fn set_owner(
        &self,
        workspace: &str,
        user_id: &str,
    ) -> Result<SetOwnerOutcome, AdminError> {
        reject_local(user_id)?;
        let now = now_unix_nanos_i64().map_err(describe)?;
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
            .role_for_user_id(workspace, user_id)
            .await
            .map_err(describe)?
        {
            Some(MemberRole::Owner) => {
                tx.rollback().await.map_err(describe)?;
                return Ok(SetOwnerOutcome::Unchanged);
            }
            Some(MemberRole::Member) => {
                if !tx
                    .workspace_members()
                    .delete(workspace, user_id)
                    .await
                    .map_err(describe)?
                {
                    return Err(AdminError(
                        "membership disappeared while appointing its owner".to_string(),
                    ));
                }
                SetOwnerOutcome::Promoted
            }
            None => SetOwnerOutcome::Added,
        };
        tx.workspace_members()
            .insert(workspace, user_id, MemberRole::Owner, now)
            .await
            .map_err(describe)?;
        tx.commit().await.map_err(describe)?;
        Ok(outcome)
    }

    /// Rebinds non-local identities to a replacement issuer without migration.
    pub async fn rebind_issuer(
        &self,
        from_issuer: &str,
        to_issuer: &str,
    ) -> Result<u64, AdminError> {
        validate_issuer(from_issuer)?;
        validate_issuer(to_issuer)?;
        if from_issuer == to_issuer {
            return Ok(0);
        }
        let mut session = &self.db;
        session
            .users()
            .rebind_issuer(from_issuer, to_issuer)
            .await
            .map_err(describe)
    }
}

/// Rejects the synthetic identity before a caller attempts database access.
pub fn reject_local(user_or_issuer: &str) -> Result<(), AdminError> {
    if user_or_issuer == LOCAL_PRINCIPAL_ID {
        Err(AdminError(
            "coral:local cannot be targeted by shared-mode recovery".to_string(),
        ))
    } else {
        Ok(())
    }
}

fn validate_issuer(issuer: &str) -> Result<(), AdminError> {
    reject_local(issuer)?;
    if issuer.is_empty() {
        return Err(AdminError("issuer must not be empty".to_string()));
    }
    Ok(())
}

fn require_existing_database_file(path: &Path) -> Result<(), AdminError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| {
        AdminError(format!(
            "cannot open existing state database at {}: {error}",
            path.display()
        ))
    })?;
    if !metadata.file_type().is_file() {
        return Err(AdminError(format!(
            "state database path is not a regular file: {}",
            path.display()
        )));
    }
    Ok(())
}

#[expect(
    clippy::disallowed_methods,
    reason = "coral-app owns process environment access; admin resolves configured Postgres exactly once"
)]
fn read_database_url(name: &str) -> Result<String, AdminError> {
    std::env::var(name).map_err(|error| match error {
        std::env::VarError::NotPresent => AdminError(format!(
            "database backend 'postgres' requires environment variable `{name}`"
        )),
        std::env::VarError::NotUnicode(_) => AdminError(format!(
            "database environment variable `{name}` must contain valid UTF-8"
        )),
    })
}

fn describe(error: impl std::fmt::Display) -> AdminError {
    AdminError(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::{AdminDb, SetOwnerOutcome};
    use crate::identity::{LOCAL_PRINCIPAL_ID, Principal, PrincipalKind};
    use crate::state::AppStateLayout;
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, UpsertLoginOutcome, UserRecord,
        now_unix_nanos_i64,
    };
    use crate::workspaces::{MemberRole, WorkspaceAction, WorkspaceAuthorizer, WorkspaceName};

    async fn set(admin: &AdminDb, workspace: &str, user: &str) -> SetOwnerOutcome {
        admin.set_owner(workspace, user).await.expect("set owner")
    }

    async fn rebind(admin: &AdminDb, from: &str, to: &str) -> u64 {
        admin.rebind_issuer(from, to).await.expect("rebind issuer")
    }

    fn counts<const N: usize>(
        summaries: &[super::WorkspaceSummary],
        names: [&str; N],
    ) -> [(usize, usize); N] {
        names.map(|name| {
            summaries
                .iter()
                .find(|item| item.name == name)
                .map(|item| (item.owner_count, item.member_count))
                .expect("workspace summary")
        })
    }

    async fn user_record(db: &CoralDb, user_id: &str) -> UserRecord {
        let mut session = db;
        session
            .users()
            .get_by_user_id(user_id)
            .await
            .expect("read user")
            .expect("user")
    }

    async fn isolated_postgres(url: &str, prefix: &str) -> (sqlx::PgPool, String, String) {
        let schema = format!("{prefix}_{}", uuid::Uuid::new_v4().simple());
        let pool = sqlx::PgPool::connect(url).await.expect("connect Postgres");
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&pool)
            .await
            .expect("create isolated schema");
        let separator = if url.contains('?') { '&' } else { '?' };
        let scoped = format!("{url}{separator}options=-csearch_path%3D{schema}");
        (pool, scoped, schema)
    }

    async fn drop_schema(pool: &sqlx::PgPool, schema: &str) {
        sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(pool)
            .await
            .expect("drop isolated schema");
    }

    struct Fixture {
        _temp: Option<TempDir>,
        config: ResolvedDatabaseConfig,
        db: Arc<CoralDb>,
    }

    impl Fixture {
        async fn sqlite() -> Self {
            let temp = tempfile::tempdir().expect("temp dir");
            let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
            layout.ensure().expect("layout");
            let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("config")
            else {
                panic!("default database must be sqlite");
            };
            Self::open(Some(temp), ResolvedDatabaseConfig::Sqlite { path }).await
        }

        async fn open(temp: Option<TempDir>, config: ResolvedDatabaseConfig) -> Self {
            let db = Arc::new(CoralDb::open(config.clone()).await.expect("open database"));
            db.migrate().await.expect("migrate test fixture");
            Self {
                _temp: temp,
                config,
                db,
            }
        }

        async fn admin(&self) -> AdminDb {
            AdminDb {
                db: CoralDb::open_existing(self.config.clone())
                    .await
                    .expect("open admin connection"),
            }
        }

        async fn user(&self, issuer: &str, subject: &str) -> String {
            let mut session = self.db.as_ref();
            let UpsertLoginOutcome::Upserted(user) = session
                .users()
                .upsert_login(issuer, subject, Some(subject), 10)
                .await
                .expect("create user")
            else {
                panic!("new subject must create a user");
            };
            user.user_id
        }

        async fn local_user(&self, issuer: &str) {
            let mut session = self.db.as_ref();
            session
                .users()
                .insert_for_test(&UserRecord {
                    user_id: LOCAL_PRINCIPAL_ID.into(),
                    issuer: issuer.into(),
                    subject: String::new(),
                    display_name: Some("Local".into()),
                    created_at_unix_nanos: 10,
                    last_login_at_unix_nanos: 10,
                })
                .await
                .expect("create local user");
        }

        async fn workspace(&self, name: &str, members: &[(&str, MemberRole)]) {
            let mut tx = self.db.begin().await.expect("begin fixture");
            tx.workspaces().create(name, 10).await.expect("workspace");
            for (user, role) in members {
                tx.workspace_members()
                    .insert(name, user, *role, 10)
                    .await
                    .expect("membership");
            }
            tx.commit().await.expect("commit fixture");
        }
    }

    #[tokio::test]
    async fn admin_recovery_contract_on_sqlite() {
        assert_admin_recovery_contract(Fixture::sqlite().await).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the admin contract against Postgres"]
    async fn admin_recovery_contract_on_postgres() {
        let Ok(url) = super::read_database_url("CORAL_TEST_POSTGRES_URL") else {
            return;
        };
        let (pool, url, schema) = isolated_postgres(&url, "admin_contract").await;
        assert_admin_recovery_contract(
            Fixture::open(None, ResolvedDatabaseConfig::Postgres { url }).await,
        )
        .await;
        drop_schema(&pool, &schema).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to prove admin open leaves an empty Postgres schema untouched"]
    async fn admin_open_does_not_migrate_empty_schema_contract_on_postgres() {
        let Ok(url) = super::read_database_url("CORAL_TEST_POSTGRES_URL") else {
            return;
        };
        let (inspection, scoped, schema) = isolated_postgres(&url, "admin_empty").await;
        let admin = AdminDb::open_resolved(ResolvedDatabaseConfig::Postgres { url: scoped })
            .await
            .expect("open existing Postgres schema");
        let workspaces = admin.list_workspaces().await;
        assert!(workspaces.is_err(), "schema must remain absent");
        let tables: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1",
        )
        .bind(&schema)
        .fetch_one(&inspection)
        .await
        .expect("inspect empty schema");
        assert_eq!(tables, 0, "admin open must not create migration tables");
        drop_schema(&inspection, &schema).await;
    }

    async fn assert_admin_recovery_contract(fixture: Fixture) {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let issuer = format!("https://old-{suffix}");
        let member = fixture.user(&issuer, &format!("member-{suffix}")).await;
        let other = fixture.user(&issuer, &format!("other-{suffix}")).await;
        fixture.local_user(&issuer).await;
        let [m, o, l, h] =
            ["member", "ownerless", "local", "healthy"].map(|kind| format!("{kind}-{suffix}"));
        fixture
            .workspace(&m, &[(&member, MemberRole::Member)])
            .await;
        fixture.workspace(&o, &[]).await;
        fixture
            .workspace(&l, &[(LOCAL_PRINCIPAL_ID, MemberRole::Owner)])
            .await;
        fixture
            .workspace(
                &h,
                &[(&other, MemberRole::Owner), (&member, MemberRole::Member)],
            )
            .await;

        let authorizer = WorkspaceAuthorizer::new(Arc::clone(&fixture.db));
        let principal = Principal::parse(&member, PrincipalKind::User).expect("principal");
        let workspace = WorkspaceName::parse(&m).expect("workspace name");
        assert!(
            authorizer
                .authorize(&principal, &workspace, WorkspaceAction::Manage)
                .await
                .is_err()
        );
        let admin = fixture.admin().await;
        let summaries = admin.list_workspaces().await.expect("list workspaces");
        assert_eq!(
            counts(&summaries, [&m, &o, &l, &h]),
            [(0, 1), (0, 0), (0, 1), (1, 2)]
        );

        assert_eq!(set(&admin, &m, &member).await, SetOwnerOutcome::Promoted);
        authorizer
            .authorize(&principal, &workspace, WorkspaceAction::Manage)
            .await
            .expect("same authorizer must observe recovery on the next request");
        assert_eq!(set(&admin, &m, &member).await, SetOwnerOutcome::Unchanged);
        assert_eq!(set(&admin, &o, &other).await, SetOwnerOutcome::Added);
        assert_eq!(set(&admin, &l, &member).await, SetOwnerOutcome::Added);
        assert_eq!(set(&admin, &h, &other).await, SetOwnerOutcome::Unchanged);
        assert_eq!(
            set(&admin, &m, "missing-user").await,
            SetOwnerOutcome::UserNotFound
        );
        assert_eq!(
            set(&admin, "missing-workspace", &other).await,
            SetOwnerOutcome::WorkspaceNotFound
        );
        admin.set_owner(&m, LOCAL_PRINCIPAL_ID).await.unwrap_err();

        let summaries = admin
            .list_workspaces()
            .await
            .expect("list recovered workspaces");
        assert_eq!(
            counts(&summaries, [&m, &o, &l, &h]),
            [(1, 1), (1, 1), (1, 2), (1, 2)]
        );
        let local_members = fixture
            .db
            .list_workspace_members(&l)
            .await
            .expect("list local members")
            .expect("workspace exists");
        assert!(
            local_members
                .iter()
                .any(|item| item.user_id == LOCAL_PRINCIPAL_ID)
        );

        let before = user_record(&fixture.db, &member).await;
        assert_eq!(rebind(&admin, &issuer, &issuer).await, 0);
        let new_issuer = format!("https://new-{suffix}");
        assert_eq!(rebind(&admin, &issuer, &new_issuer).await, 2);
        assert_eq!(rebind(&admin, &issuer, &new_issuer).await, 0);
        admin
            .rebind_issuer(LOCAL_PRINCIPAL_ID, &new_issuer)
            .await
            .unwrap_err();
        let users = admin.list_users().await.expect("list users");
        assert!(
            users
                .iter()
                .filter(|user| user.user_id == member || user.user_id == other)
                .all(|user| user.issuer == new_issuer)
        );
        let mut expected = before;
        expected.issuer = new_issuer;
        assert_eq!(user_record(&fixture.db, &member).await, expected);
        assert_eq!(
            user_record(&fixture.db, LOCAL_PRINCIPAL_ID).await.issuer,
            issuer
        );
    }

    #[tokio::test]
    async fn open_refuses_missing_state_and_never_migrates_an_existing_file() {
        #[cfg(unix)]
        use std::os::unix::fs::PermissionsExt as _;

        let temp = tempfile::tempdir().expect("temp dir");
        let state = temp.path().join("state");
        let missing = AdminDb::open(Some(state.clone()))
            .await
            .expect_err("missing database must be refused");
        assert!(
            missing
                .to_string()
                .contains("cannot open existing state database")
        );
        assert!(
            !state.exists(),
            "recovery must not create the state directory"
        );

        std::fs::create_dir(&state).expect("state dir");
        let database = state.join("coral.db");
        std::fs::File::create(&database).expect("empty database file");
        #[cfg(unix)]
        {
            std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o755))
                .expect("state permissions");
            std::fs::set_permissions(&database, std::fs::Permissions::from_mode(0o644))
                .expect("database permissions");
        }
        let admin = AdminDb::open(Some(state.clone()))
            .await
            .expect("open existing empty sqlite file");
        let workspaces = admin.list_workspaces().await;
        assert!(workspaces.is_err(), "schema must remain absent");
        assert!(
            !state.join(".lock").exists(),
            "recovery must not create a state lock"
        );
        let connection = rusqlite::Connection::open(&database).expect("inspect empty database");
        let migration_tables: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'",
                [],
                |row| row.get(0),
            )
            .expect("inspect migration table");
        assert_eq!(migration_tables, 0, "recovery must not run migrations");
        #[cfg(unix)]
        {
            let mode = |path: &std::path::Path| {
                std::fs::metadata(path)
                    .expect("state metadata")
                    .permissions()
                    .mode()
                    & 0o777
            };
            assert_eq!(
                (mode(&state), mode(&database)),
                (0o755, 0o644),
                "recovery must not alter state permissions"
            );
        }
    }

    #[test]
    fn local_targets_are_rejected_without_a_clock_or_database() {
        assert!(super::reject_local(LOCAL_PRINCIPAL_ID).is_err());
        assert!(super::validate_issuer(LOCAL_PRINCIPAL_ID).is_err());
        assert!(super::validate_issuer("").is_err());
        now_unix_nanos_i64().expect("clock remains available for ordinary users");
    }
}
