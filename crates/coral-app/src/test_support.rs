//! Fixtures the crate's own unit tests share.
//!
//! What lives here is the shape of a deployment several suites all need to
//! stand up before they can say anything about access: a migrated database,
//! the workspace a suite runs in, and principals provisioned through the
//! production login seam rather than invented as strings. Kept in one place,
//! a change to `LoginIdentity` or to membership writes is one edit rather than
//! six, and no copy can quietly drift from the others.

use std::sync::Arc;

use tempfile::TempDir;

use crate::credentials::{CredentialManager, CredentialStore};
use crate::identity::{Principal, PrincipalKind};
use crate::state::db::{
    CoralDb, DatabaseConfig, DbRepos as _, LoginIdentity, LoginProvisioning,
    ResolvedDatabaseConfig, run_state_migrations,
};
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::manager::WorkspaceManager;
use crate::workspaces::{MemberRole, WorkspaceName};

/// A shared deployment over one migrated database, so every caller's
/// authority comes from a membership row.
///
/// It stops at the state every service needs: each suite creates the
/// workspace it runs in and builds its own managers on top, because that part
/// is what the suite is about. An install provisions no workspace, so there is
/// none here to inherit.
pub(crate) struct MigratedDeployment {
    /// Held so the config directory outlives the deployment built over it.
    pub(crate) temp: TempDir,
    pub(crate) layout: AppStateLayout,
    pub(crate) config_store: ConfigStore,
    pub(crate) credentials: CredentialManager,
    pub(crate) db: Arc<CoralDb>,
    pub(crate) workspaces: WorkspaceManager,
}

/// Opens and migrates one deployment's state.
pub(crate) async fn migrated_deployment() -> MigratedDeployment {
    let temp = TempDir::new().expect("temp dir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    layout.ensure().expect("ensure layout");
    let config_store = ConfigStore::new(layout.clone());
    let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config") else {
        panic!("the default test database is sqlite")
    };
    let db = Arc::new(
        CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite"),
    );
    db.migrate().await.expect("migrate sqlite");
    run_state_migrations(&db, &config_store, &layout)
        .await
        .expect("run state migrations");
    let credentials = CredentialManager::new(CredentialStore::new(layout.clone()));
    let workspaces = WorkspaceManager::new_for_tests(
        config_store.clone(),
        credentials.clone(),
        layout.clone(),
        None,
        Arc::clone(&db),
    );
    MigratedDeployment {
        temp,
        layout,
        config_store,
        credentials,
        db,
        workspaces,
    }
}

/// Provisions one directory user through the production login seam and, when
/// `role` names one, grants it on `workspace`.
///
/// The `user_id` a service is then handed is the one a real login carries,
/// rather than an identifier a test made up. `issuer` is the only thing that
/// differs between suites: each provisions under its own, so a subject seeded
/// by one is a different person from the same subject seeded by another. The
/// workspace is named rather than assumed, because a fixture that spelled a
/// well-known name would prove a workspace is resolved by being well known
/// instead of by having been created.
pub(crate) async fn seed_principal(
    db: &Arc<CoralDb>,
    issuer: &str,
    workspace: &WorkspaceName,
    subject: &str,
    role: Option<MemberRole>,
) -> Principal {
    let LoginProvisioning::Provisioned(user) = db
        .user_state()
        .provision_login(LoginIdentity {
            issuer,
            subject,
            display_name: None,
            principal_claim: subject,
            now_unix_nanos: 1,
        })
        .await
        .expect("provision user")
    else {
        panic!("expected a provisioned user rather than an issuer mismatch")
    };
    if let Some(role) = role {
        let mut session = db.as_ref();
        session
            .workspace_members()
            .upsert(workspace.as_str(), &user.user_id, role, 2)
            .await
            .expect("grant membership");
    }
    Principal::parse(&user.user_id, PrincipalKind::User).expect("federated principal")
}

/// Creates one ordinary workspace for a suite to run in.
///
/// An install provisions none, so a fixture that needs one creates it the way
/// a caller would rather than relying on a name that happens to be there.
pub(crate) async fn create_workspace(db: &Arc<CoralDb>, workspace: &WorkspaceName) {
    let mut tx = db.begin().await.expect("begin workspace creation");
    tx.workspaces()
        .create(workspace.as_str(), 1)
        .await
        .expect("create workspace");
    tx.commit().await.expect("commit workspace creation");
}
