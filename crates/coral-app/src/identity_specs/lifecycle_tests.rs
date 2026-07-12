use std::sync::Arc;
use std::time::Duration;

use coral_api::v1::DeleteIdentitySpecRequest;
use coral_api::v1::identity_spec_service_server::IdentitySpecService as IdentitySpecServiceApi;
use tempfile::tempdir;
use tonic::{Code, Request};

use super::tests::TestKeyProvider;
use super::{IdentitySpecManager, checked_orphan_count, identity_spec_fingerprint};
use crate::bootstrap::AppError;
use crate::credentials::encryption::{CredentialEncryptionKey, CredentialKeyProvider};
use crate::identities::manager::IdentityManager;
use crate::identities::model::{IdentityName, IdentityOwner, IdentitySpecReference};
use crate::identity::UserPrincipal;
use crate::identity_specs::IdentitySpecService;
use crate::state::db::{
    CoralDb, DbRepos, IdentitySpecKey, IdentitySpecScope, IdentitySpecWrite, ResolvedDatabaseConfig,
};
use crate::workspaces::WorkspaceName;

const RACE_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn sqlite_identity_spec_lifecycle_contract() {
    let temp = tempdir().expect("temp dir");
    let db = Arc::new(
        CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open SQLite database"),
    );
    db.migrate().await.expect("migrate SQLite database");

    Box::pin(assert_identity_spec_lifecycle_contract(&db)).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "shared exact-spec lifecycle contract"
)]
pub(crate) async fn assert_identity_spec_lifecycle_contract(db: &Arc<CoralDb>) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let local_workspace = workspace("local", &suffix);
    let fallback_workspace = workspace("fallback", &suffix);
    seed_workspaces(db, [&local_workspace, &fallback_workspace]).await;
    let key_provider: Arc<dyn CredentialKeyProvider> = Arc::new(TestKeyProvider(vec![
        CredentialEncryptionKey::from_static_bytes_for_test([73; 32]),
    ]));
    let specs = IdentitySpecManager::new(Arc::clone(db), Arc::clone(&key_provider));
    let identities = IdentityManager::new(Arc::clone(db), Arc::clone(&key_provider));
    let name = format!("lifecycle_{suffix}");
    let global_key = IdentitySpecKey::global(&name).expect("global key");
    let workspace_key =
        IdentitySpecKey::workspace(local_workspace.clone(), &name).expect("workspace key");
    let baseline = fixed_manifest(&name, "1.0.0");
    let equivalent = equivalent_fixed_manifest(&name, "1.0.0");
    let changed = fixed_manifest(&name, "2.0.0");

    install(&specs, IdentitySpecScope::global(), &baseline)
        .await
        .expect("install global spec");
    install(
        &specs,
        IdentitySpecScope::workspace(local_workspace.clone()),
        &baseline,
    )
    .await
    .expect("install workspace spec");
    let user_identity = format!("user_{suffix}");
    let fallback_identity = format!("fallback_{suffix}");
    let local_identity = format!("local_{suffix}");
    identities
        .create_or_replace_user_fixed_token(
            &UserPrincipal::local(),
            &user_identity,
            &name,
            "user-token".to_string(),
        )
        .await
        .expect("create user identity");
    identities
        .create_or_replace_workspace_fixed_token(
            &fallback_workspace,
            &fallback_identity,
            &name,
            "fallback-token".to_string(),
        )
        .await
        .expect("create global-fallback workspace identity");
    identities
        .create_or_replace_workspace_fixed_token(
            &local_workspace,
            &local_identity,
            &name,
            "workspace-token".to_string(),
        )
        .await
        .expect("create workspace-local identity");
    let counts = dependent_counts(db, &global_key, &workspace_key).await;
    assert_eq!(counts, (2, 1));

    assert!(
        install(&specs, IdentitySpecScope::global(), &equivalent)
            .await
            .expect("equivalent replacement")
            .1
    );
    assert_equivalent_rejection(
        &install(&specs, IdentitySpecScope::global(), &changed)
            .await
            .expect_err("changed replacement must reject dependents"),
        2,
    );

    let service = IdentitySpecService::new(specs.clone());
    let guarded =
        IdentitySpecServiceApi::delete_identity_spec(&service, delete_request(&name, false))
            .await
            .expect_err("guarded RPC delete must reject dependents");
    assert_eq!(guarded.code(), Code::FailedPrecondition);
    specs.get_global(&name).await.expect("guarded spec remains");
    assert_eq!(user_identity_pair(db, &user_identity).await, (true, true));
    let deleted =
        IdentitySpecServiceApi::delete_identity_spec(&service, delete_request(&name, true))
            .await
            .expect("force delete exact spec")
            .into_inner();
    assert_eq!(deleted.orphaned_identities, 2);
    assert_eq!(user_identity_pair(db, &user_identity).await, (true, true));
    assert!(matches!(
        specs.get_global(&name).await,
        Err(AppError::IdentitySpecNotFound { .. })
    ));
    assert!(matches!(
        specs.delete_exact(&global_key, true).await,
        Err(AppError::IdentitySpecNotFound { .. })
    ));
    specs
        .get_exact(&workspace_key)
        .await
        .expect("same-name workspace spec remains installed");

    assert_equivalent_rejection(
        &install(&specs, IdentitySpecScope::global(), &changed)
            .await
            .expect_err("changed re-add must reject orphan dependents"),
        2,
    );
    assert!(
        !install(&specs, IdentitySpecScope::global(), &equivalent)
            .await
            .expect("equivalent re-add heals exact orphans")
            .1
    );
    let stale_write = IdentitySpecWrite::new(
        "2.0.0",
        "lifecycle 2.0.0",
        "lifecycle",
        "fixed_token",
        &changed,
    )
    .expect("valid stale write");
    let mut tx = db.begin().await.expect("begin stale write");
    tx.identity_specs()
        .upsert(&global_key, &stale_write, 50)
        .await
        .unwrap();
    tx.commit().await.expect("commit stale write");
    let (_, repaired) = install(&specs, IdentitySpecScope::global(), &equivalent)
        .await
        .expect("dependent fingerprints permit stale-row repair");
    assert!(repaired);

    let owner = IdentityOwner::for_user(UserPrincipal::local());
    let identity_name = IdentityName::parse(&user_identity).expect("mixed identity name");
    let mixed = IdentitySpecReference::new(
        &owner,
        global_key.clone(),
        "legacy-mixed-fingerprint",
        "lifecycle",
        "fixed_token",
    )
    .expect("mixed legacy reference");
    let mut tx = db.begin().await.expect("begin mixed reference write");
    tx.identities()
        .upsert(&owner, &identity_name, &mixed, 51)
        .await
        .unwrap();
    tx.commit().await.expect("commit mixed reference write");
    assert_equivalent_rejection(
        &install(&specs, IdentitySpecScope::global(), &equivalent)
            .await
            .expect_err("candidate matching only one dependent must reject"),
        2,
    );
    assert_eq!(
        specs.get_global(&name).await.unwrap().manifest.version,
        "1.0.0"
    );

    checked_orphan_count(&global_key, u64::from(u32::MAX) + 1).unwrap_err();
    assert_changed_replace_create_race(db, Arc::clone(&key_provider), &suffix).await;
    assert_delete_create_race(db, Arc::clone(&key_provider), &suffix, false).await;
    assert_delete_create_race(db, key_provider, &suffix, true).await;
}

async fn assert_changed_replace_create_race(
    db: &Arc<CoralDb>,
    key_provider: Arc<dyn CredentialKeyProvider>,
    suffix: &str,
) {
    let name = format!("replace_race_{suffix}");
    let changed = fixed_manifest(&name, "2.0.0");
    let specs = IdentitySpecManager::new(Arc::clone(db), Arc::clone(&key_provider));
    install(
        &specs,
        IdentitySpecScope::global(),
        &fixed_manifest(&name, "1.0.0"),
    )
    .await
    .expect("seed replacement race spec");
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let gated_specs = IdentitySpecManager::new(Arc::clone(db), Arc::clone(&key_provider))
        .with_before_lifecycle_write(Arc::clone(&barrier));
    let gated_identities =
        IdentityManager::new(Arc::clone(db), key_provider).with_before_upsert_gate(barrier);
    let principal = UserPrincipal::local();
    let (created, replaced) = tokio::time::timeout(RACE_TIMEOUT, async {
        tokio::join!(
            gated_identities.create_or_replace_user_fixed_token(
                &principal,
                &name,
                &name,
                "race-token".to_string(),
            ),
            install(&gated_specs, IdentitySpecScope::global(), &changed),
        )
    })
    .await
    .expect("changed replacement race must not deadlock");
    let created = created.expect("identity create must converge on one spec generation");
    assert_eq!(user_identity_pair(db, &name).await, (true, true));
    let current = specs.get_global(&name).await.expect("race spec remains");
    let current_fingerprint =
        identity_spec_fingerprint(&current.manifest).expect("fingerprint current race spec");
    assert_eq!(created.spec_reference.fingerprint(), current_fingerprint);
    match replaced {
        Ok((_installed, true)) => assert_eq!(current.manifest.version, "2.0.0"),
        Err(AppError::FailedPrecondition(detail)) => {
            assert!(detail.contains("equivalent manifest"));
            assert_eq!(current.manifest.version, "1.0.0");
        }
        other => panic!("unexpected changed replacement race result: {other:?}"),
    }
}

async fn assert_delete_create_race(
    db: &Arc<CoralDb>,
    key_provider: Arc<dyn CredentialKeyProvider>,
    suffix: &str,
    force: bool,
) {
    let label = if force { "force" } else { "guarded" };
    let name = format!("delete_{label}_{suffix}");
    let key = IdentitySpecKey::global(&name).expect("delete race key");
    let specs = IdentitySpecManager::new(Arc::clone(db), Arc::clone(&key_provider));
    install(
        &specs,
        IdentitySpecScope::global(),
        &fixed_manifest(&name, "1.0.0"),
    )
    .await
    .expect("seed delete race spec");
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let gated_specs = IdentitySpecManager::new(Arc::clone(db), Arc::clone(&key_provider))
        .with_before_lifecycle_write(Arc::clone(&barrier));
    let gated_identities =
        IdentityManager::new(Arc::clone(db), key_provider).with_before_upsert_gate(barrier);
    let principal = UserPrincipal::local();
    let (created, deleted) = tokio::time::timeout(RACE_TIMEOUT, async {
        tokio::join!(
            gated_identities.create_or_replace_user_fixed_token(
                &principal,
                &name,
                &name,
                "race-token".to_string(),
            ),
            gated_specs.delete_exact(&key, force),
        )
    })
    .await
    .expect("delete/create race must not deadlock");
    let pair_expected = created.is_ok();

    if force {
        let orphaned = deleted.expect("force delete must converge");
        match created {
            Ok(_record) => assert_eq!(orphaned, 1),
            Err(AppError::IdentitySpecNotFound { .. }) => assert_eq!(orphaned, 0),
            other => panic!("unexpected force-delete race create result: {other:?}"),
        }
        assert!(matches!(
            specs.get_global(&name).await,
            Err(AppError::IdentitySpecNotFound { .. })
        ));
    } else {
        match (created, deleted) {
            (Ok(_record), Err(AppError::FailedPrecondition(detail))) => {
                assert!(detail.contains("retry with force"));
                specs
                    .get_global(&name)
                    .await
                    .expect("guarded delete leaves spec installed");
            }
            (Err(AppError::IdentitySpecNotFound { .. }), Ok(0)) => assert!(matches!(
                specs.get_global(&name).await,
                Err(AppError::IdentitySpecNotFound { .. })
            )),
            other => panic!("unexpected guarded-delete race result: {other:?}"),
        }
    }
    assert_eq!(
        user_identity_pair(db, &name).await,
        (pair_expected, pair_expected)
    );
}

async fn install(
    manager: &IdentitySpecManager,
    scope: IdentitySpecScope,
    manifest: &str,
) -> Result<(super::InstalledIdentitySpec, bool), AppError> {
    manager.add_or_replace_exact(scope, manifest, vec![]).await
}

async fn seed_workspaces<const N: usize>(db: &Arc<CoralDb>, workspaces: [&WorkspaceName; N]) {
    let mut tx = db.begin().await.expect("begin workspace seed");
    for (index, workspace) in workspaces.into_iter().enumerate() {
        tx.workspaces()
            .ensure(
                workspace.as_str(),
                i64::try_from(index + 1).expect("workspace generation"),
            )
            .await
            .expect("seed lifecycle workspace");
    }
    tx.commit().await.expect("commit workspace seed");
}

async fn dependent_counts(
    db: &Arc<CoralDb>,
    global: &IdentitySpecKey,
    workspace: &IdentitySpecKey,
) -> (u64, u64) {
    let mut session = db.as_ref();
    let global = session
        .identities()
        .count_dependents(global)
        .await
        .expect("count global dependents");
    let workspace = session
        .identities()
        .count_dependents(workspace)
        .await
        .expect("count workspace dependents");
    (global, workspace)
}

async fn user_identity_pair(db: &Arc<CoralDb>, name: &str) -> (bool, bool) {
    let owner = IdentityOwner::for_user(UserPrincipal::local());
    let name = IdentityName::parse(name).expect("valid identity name");
    let mut session = db.as_ref();
    let identity = session
        .identities()
        .load_optional(&owner, &name)
        .await
        .unwrap();
    let document = session
        .identity_documents()
        .load_optional(&owner, &name)
        .await
        .unwrap();
    (identity.is_some(), document.is_some())
}

fn assert_equivalent_rejection(error: &AppError, count: u64) {
    assert!(
        matches!(&error, AppError::FailedPrecondition(detail)
            if detail.contains(&format!("{count} stored "))
                && detail.contains("equivalent manifest")),
        "unexpected dependent replacement error: {error}"
    );
}

fn delete_request(name: &str, force: bool) -> Request<DeleteIdentitySpecRequest> {
    Request::new(DeleteIdentitySpecRequest {
        name: name.to_string(),
        workspace: None,
        force,
    })
}

fn workspace(label: &str, suffix: &str) -> WorkspaceName {
    WorkspaceName::parse(&format!("{label}{suffix}")).expect("valid lifecycle workspace")
}

fn fixed_manifest(name: &str, version: &str) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: {version}\ndescription: lifecycle {version}\nissuer: lifecycle\ntype: fixed_token\n"
    )
}

fn equivalent_fixed_manifest(name: &str, version: &str) -> String {
    format!(
        "# semantically equivalent reordered YAML\ntype: fixed_token\nissuer: lifecycle\ndescription: lifecycle {version}\nversion: {version}\nname: {name}\nspec_version: 1\nkind: identity\n"
    )
}
