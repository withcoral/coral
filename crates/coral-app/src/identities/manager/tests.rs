use std::sync::Arc;
use std::time::Duration;

use coral_spec::parse_identity_manifest_yaml;
use tempfile::tempdir;

use super::IdentityManager;
use crate::bootstrap::AppError;
use crate::credentials::CredentialsError;
use crate::credentials::encryption::{
    CredentialEncryptionKey, CredentialKeyProvider, EncryptedEnvelopeDocument,
};
use crate::identities::model::{IdentityName, IdentityOwner};
use crate::identity::{UserPrincipal, decrypt_identity_document};
use crate::identity_specs::identity_spec_fingerprint;
use crate::state::db::{
    CoralDb, DbRepos, IdentityDocumentRecord, IdentityRecord, IdentitySpecKey, IdentitySpecWrite,
    ResolvedDatabaseConfig,
};
use crate::workspaces::WorkspaceName;

struct TestKeyProvider(Vec<CredentialEncryptionKey>);

impl CredentialKeyProvider for TestKeyProvider {
    fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
        self.0
            .last()
            .cloned()
            .ok_or_else(|| CredentialsError::Crypto("missing test key".to_string()))
    }

    fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
        self.0
            .iter()
            .find(|key| key.key_id() == key_id)
            .cloned()
            .ok_or_else(|| CredentialsError::Crypto("missing test key".to_string()))
    }
}

#[tokio::test]
async fn sqlite_fixed_token_manager_contract() {
    let temp = tempdir().expect("temp dir");
    let db = Arc::new(
        CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite"),
    );
    db.migrate().await.expect("migrate sqlite");
    Box::pin(assert_fixed_token_manager_contract(&db)).await;
}

pub(crate) async fn assert_fixed_token_manager_contract(db: &Arc<CoralDb>) {
    assert_user_global_fixed_token_manager_contract(db).await;
    assert_workspace_fixed_token_manager_contract(db).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "shared SQLite/Postgres manager contract"
)]
async fn assert_user_global_fixed_token_manager_contract(db: &Arc<CoralDb>) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let spec_a = format!("fixed_a_{suffix}");
    let spec_b = format!("fixed_b_{suffix}");
    let oauth = format!("oauth_{suffix}");
    let workspace_only = format!("workspace_only_{suffix}");
    let race = format!("race_{suffix}");
    let workspace = WorkspaceName::parse(&format!("work{suffix}")).expect("workspace");
    let workspace_key =
        IdentitySpecKey::workspace(workspace.clone(), &workspace_only).expect("workspace key");
    put_workspace(db, &workspace).await;
    for (key, yaml) in [
        (
            IdentitySpecKey::global(&spec_a).unwrap(),
            fixed_manifest(&spec_a, "a"),
        ),
        (
            IdentitySpecKey::global(&spec_b).unwrap(),
            fixed_manifest(&spec_b, "b"),
        ),
        (
            IdentitySpecKey::global(&oauth).unwrap(),
            oauth_manifest(&oauth),
        ),
        (workspace_key, fixed_manifest(&workspace_only, "workspace")),
        (
            IdentitySpecKey::global(&race).unwrap(),
            fixed_manifest(&race, "before"),
        ),
    ] {
        put_spec(db, &key, &yaml).await;
    }

    let old_key = CredentialEncryptionKey::from_static_bytes_for_test([71; 32]);
    let old_provider = Arc::new(TestKeyProvider(vec![old_key.clone()]));
    let manager = IdentityManager::new(db.clone(), old_provider.clone());
    let principal_a = UserPrincipal::for_user(&format!("a-{suffix}")).unwrap();
    let principal_b = UserPrincipal::for_user(&format!("b-{suffix}")).unwrap();
    let owner_a = IdentityOwner::for_user(principal_a.clone());
    let owner_b = IdentityOwner::for_user(principal_b.clone());
    let identity = format!("shared-{suffix}");
    assert!(matches!(
        manager
            .create_or_replace_user_fixed_token(&principal_a, "missing", &workspace_only, "x".into())
            .await,
        Err(AppError::IdentitySpecNotFound { scope, .. }) if scope == "global"
    ));
    assert!(matches!(
        manager
            .create_or_replace_user_fixed_token(&principal_a, "wrong", &oauth, "x".into())
            .await,
        Err(AppError::InvalidInput(_))
    ));
    assert!(manager.list_for_owner(&owner_a).await.unwrap().is_empty());

    let created_a = manager
        .create_or_replace_user_fixed_token(
            &principal_a,
            &identity,
            &spec_a,
            "  alpha-token  ".into(),
        )
        .await
        .expect("create owner A");
    let created_b = manager
        .create_or_replace_user_fixed_token(&principal_b, &identity, &spec_a, " beta-token ".into())
        .await
        .expect("create owner B");
    assert_reference(&created_a, &spec_a, "a");
    assert_reference(&created_b, &spec_a, "a");
    let before_a = load_pair(db, &owner_a, &identity).await;
    let before_b = load_pair(db, &owner_b, &identity).await;
    assert_material(
        before_a.1.as_ref().unwrap(),
        &owner_a,
        "alpha-token",
        old_provider.as_ref(),
    );
    assert_material(
        before_b.1.as_ref().unwrap(),
        &owner_b,
        "beta-token",
        old_provider.as_ref(),
    );
    assert!(matches!(
        manager
            .create_or_replace_user_fixed_token(&principal_a, &identity, &spec_b, " \t ".into())
            .await,
        Err(AppError::InvalidInput(_))
    ));
    assert!(load_pair(db, &owner_a, &identity).await == before_a);

    let new_key = CredentialEncryptionKey::from_static_bytes_for_test([72; 32]);
    let rotated_provider = Arc::new(TestKeyProvider(vec![old_key, new_key.clone()]));
    let rotated = IdentityManager::new(db.clone(), rotated_provider.clone());
    let replaced = rotated
        .create_or_replace_user_fixed_token(
            &principal_a,
            &identity,
            &spec_b,
            " gamma-token ".into(),
        )
        .await
        .expect("replace owner A");
    assert_reference(&replaced, &spec_b, "b");
    let after_a = load_pair(db, &owner_a, &identity).await;
    let after_document = after_a.1.as_ref().unwrap();
    assert_eq!(
        replaced.created_at_unix_nanos,
        created_a.created_at_unix_nanos
    );
    assert_eq!(after_document.document_version, 2);
    assert_eq!(
        after_document.created_at_unix_nanos,
        before_a.1.unwrap().created_at_unix_nanos
    );
    assert_eq!(after_document.key_id, new_key.key_id());
    assert_material(
        after_document,
        &owner_a,
        "gamma-token",
        rotated_provider.as_ref(),
    );
    assert!(load_pair(db, &owner_b, &identity).await == before_b);

    let conflict_name = format!("conflict-{suffix}");
    let conflict_gate = Arc::new(tokio::sync::Barrier::new(2));
    let left = rotated
        .clone()
        .with_before_upsert_gate(conflict_gate.clone());
    let right = rotated.clone().with_before_upsert_gate(conflict_gate);
    let (left, right) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            left.create_or_replace_user_fixed_token(
                &principal_a,
                &conflict_name,
                &spec_a,
                "left-token".into(),
            ),
            right.create_or_replace_user_fixed_token(
                &principal_a,
                &conflict_name,
                &spec_b,
                "right-token".into(),
            ),
        )
    })
    .await
    .expect("conflicting writes must not deadlock");
    let left = left.expect("left conflicting write");
    let right = right.expect("right conflicting write");
    assert_reference(&left, &spec_a, "a");
    assert_reference(&right, &spec_b, "b");
    let conflict = load_pair(db, &owner_a, &conflict_name).await;
    let conflict_record = conflict.0.as_ref().unwrap();
    assert_eq!(conflict.1.as_ref().unwrap().document_version, 2);
    let expected_token = match conflict_record.spec_reference.key().name() {
        name if name == spec_a => "left-token",
        name if name == spec_b => "right-token",
        name => panic!("unexpected winning spec {name}"),
    };
    assert_material(
        conflict.1.as_ref().unwrap(),
        &owner_a,
        expected_token,
        rotated_provider.as_ref(),
    );

    delete_spec(db, &IdentitySpecKey::global(&spec_a).unwrap()).await;
    assert_eq!(rotated.get(&owner_b, &identity).await.unwrap(), created_b);
    assert_eq!(rotated.list_for_owner(&owner_b).await.unwrap().len(), 1);
    rotated
        .delete(&owner_b, &identity)
        .await
        .expect("delete orphan");
    assert!(load_pair(db, &owner_b, &identity).await == (None, None));
    assert!(matches!(
        rotated.get(&owner_b, &identity).await,
        Err(AppError::IdentityNotFound(_))
    ));
    assert!(matches!(
        rotated.delete(&owner_b, &identity).await,
        Err(AppError::IdentityNotFound(_))
    ));
    assert_eq!(rotated.get(&owner_a, &identity).await.unwrap(), replaced);

    let selected = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let gated = rotated
        .clone()
        .with_before_write_gate(selected.clone(), resume.clone());
    let raced_name = format!("raced-{suffix}");
    let race_result = tokio::time::timeout(Duration::from_secs(10), async {
        let create = gated.create_or_replace_user_fixed_token(
            &principal_a,
            &raced_name,
            &race,
            " race-token ".into(),
        );
        let replace = async {
            selected.wait().await;
            put_spec(
                db,
                &IdentitySpecKey::global(&race).unwrap(),
                &fixed_manifest(&race, "after"),
            )
            .await;
            resume.wait().await;
        };
        let (created, ()) = tokio::join!(create, replace);
        created
    })
    .await
    .expect("race must not deadlock")
    .expect("race create");
    assert_reference(&race_result, &race, "after");
    let raced = load_pair(db, &owner_a, &raced_name).await;
    assert_eq!(raced.0.unwrap(), race_result);
    assert_eq!(raced.1.as_ref().unwrap().document_version, 1);
    assert_material(
        raced.1.as_ref().unwrap(),
        &owner_a,
        "race-token",
        rotated_provider.as_ref(),
    );
}

#[expect(
    clippy::too_many_lines,
    reason = "shared SQLite/Postgres workspace manager contract"
)]
async fn assert_workspace_fixed_token_manager_contract(db: &Arc<CoralDb>) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let workspace = WorkspaceName::parse(&format!("ws{suffix}")).expect("workspace");
    let other_workspace = WorkspaceName::parse(&format!("other{suffix}")).expect("other workspace");
    let deleted_workspace =
        WorkspaceName::parse(&format!("deleted{suffix}")).expect("deleted workspace");
    let recreated_workspace =
        WorkspaceName::parse(&format!("recreated{suffix}")).expect("recreated workspace");
    let retry_workspace = WorkspaceName::parse(&format!("retry{suffix}")).expect("retry workspace");
    let missing_workspace =
        WorkspaceName::parse(&format!("missing{suffix}")).expect("missing workspace");
    put_workspace(db, &workspace).await;
    put_workspace(db, &other_workspace).await;
    put_workspace(db, &deleted_workspace).await;
    put_workspace(db, &recreated_workspace).await;
    put_workspace(db, &retry_workspace).await;

    let fallback = format!("fallback_{suffix}");
    let shadowed = format!("shadowed_{suffix}");
    let wrong_type = format!("wrong_type_{suffix}");
    let exact_race = format!("exact_race_{suffix}");
    let shadow_race = format!("shadow_race_{suffix}");
    let delete_race = format!("delete_race_{suffix}");
    let recreate_race = format!("recreate_race_{suffix}");
    let retry_recreate_race = format!("retry_recreate_race_{suffix}");
    let retry_recreate_global_key = IdentitySpecKey::global(&retry_recreate_race).unwrap();
    let retry_recreate_workspace_key =
        IdentitySpecKey::workspace(retry_workspace.clone(), &retry_recreate_race).unwrap();
    let fallback_global_key = IdentitySpecKey::global(&fallback).unwrap();
    let fallback_workspace_key = IdentitySpecKey::workspace(workspace.clone(), &fallback).unwrap();
    let shadowed_workspace_key = IdentitySpecKey::workspace(workspace.clone(), &shadowed).unwrap();
    let exact_race_workspace_key =
        IdentitySpecKey::workspace(workspace.clone(), &exact_race).unwrap();
    let shadow_race_workspace_key =
        IdentitySpecKey::workspace(workspace.clone(), &shadow_race).unwrap();
    for (key, yaml) in [
        (
            fallback_global_key.clone(),
            fixed_manifest(&fallback, "fallback"),
        ),
        (
            IdentitySpecKey::workspace(other_workspace, &fallback).unwrap(),
            fixed_manifest(&fallback, "other_workspace"),
        ),
        (
            IdentitySpecKey::global(&shadowed).unwrap(),
            fixed_manifest(&shadowed, "global_shadowed"),
        ),
        (
            shadowed_workspace_key.clone(),
            fixed_manifest(&shadowed, "workspace_shadowed"),
        ),
        (
            IdentitySpecKey::global(&wrong_type).unwrap(),
            fixed_manifest(&wrong_type, "global_fixed"),
        ),
        (
            IdentitySpecKey::workspace(workspace.clone(), &wrong_type).unwrap(),
            oauth_manifest(&wrong_type),
        ),
        (
            exact_race_workspace_key.clone(),
            fixed_manifest(&exact_race, "before"),
        ),
        (
            IdentitySpecKey::global(&shadow_race).unwrap(),
            fixed_manifest(&shadow_race, "global_race"),
        ),
        (
            IdentitySpecKey::global(&delete_race).unwrap(),
            fixed_manifest(&delete_race, "deleted_workspace"),
        ),
        (
            IdentitySpecKey::global(&recreate_race).unwrap(),
            fixed_manifest(&recreate_race, "recreated_workspace"),
        ),
        (
            retry_recreate_global_key.clone(),
            fixed_manifest(&retry_recreate_race, "before_retry"),
        ),
    ] {
        put_spec(db, &key, &yaml).await;
    }

    let key = CredentialEncryptionKey::from_static_bytes_for_test([73; 32]);
    let provider = Arc::new(TestKeyProvider(vec![key]));
    let manager = IdentityManager::new(db.clone(), provider.clone());
    let owner = IdentityOwner::workspace(workspace.clone());
    assert!(matches!(
        manager
            .create_or_replace_workspace_fixed_token(
                &missing_workspace,
                "missing-workspace",
                &fallback,
                "token".into(),
            )
            .await,
        Err(AppError::WorkspaceNotFound(name)) if name == missing_workspace.as_str()
    ));
    assert!(matches!(
        manager
            .create_or_replace_workspace_fixed_token(
                &workspace,
                "missing-spec",
                &format!("missing_{suffix}"),
                "token".into(),
            )
            .await,
        Err(AppError::IdentitySpecNotFound { scope, .. })
            if scope == format!("workspace:{workspace}")
    ));
    assert!(matches!(
        manager
            .create_or_replace_workspace_fixed_token(
                &workspace,
                "wrong-type",
                &wrong_type,
                "token".into(),
            )
            .await,
        Err(AppError::InvalidInput(_))
    ));
    assert!(manager.list_for_owner(&owner).await.unwrap().is_empty());

    let fallback_identity = format!("fallback-identity-{suffix}");
    let fallback_created = manager
        .create_or_replace_workspace_fixed_token(
            &workspace,
            &fallback_identity,
            &fallback,
            " fallback-token ".into(),
        )
        .await
        .expect("create global fallback");
    assert_reference_key(&fallback_created, &fallback_global_key, "fallback");
    let fallback_pair = load_pair(db, &owner, &fallback_identity).await;
    assert_eq!(fallback_pair.0.as_ref(), Some(&fallback_created));
    assert_eq!(fallback_pair.1.as_ref().unwrap().document_version, 1);
    assert_material(
        fallback_pair.1.as_ref().unwrap(),
        &owner,
        "fallback-token",
        provider.as_ref(),
    );

    let shadowed_identity = format!("shadowed-identity-{suffix}");
    let shadowed_created = manager
        .create_or_replace_workspace_fixed_token(
            &workspace,
            &shadowed_identity,
            &shadowed,
            " workspace-token ".into(),
        )
        .await
        .expect("create workspace shadow");
    assert_reference_key(
        &shadowed_created,
        &shadowed_workspace_key,
        "workspace_shadowed",
    );
    let shadowed_pair = load_pair(db, &owner, &shadowed_identity).await;
    assert_eq!(shadowed_pair.1.as_ref().unwrap().document_version, 1);
    assert_material(
        shadowed_pair.1.as_ref().unwrap(),
        &owner,
        "workspace-token",
        provider.as_ref(),
    );

    put_spec(
        db,
        &fallback_workspace_key,
        &fixed_manifest(&fallback, "late_shadow"),
    )
    .await;
    assert_eq!(
        manager.get(&owner, &fallback_identity).await.unwrap(),
        fallback_created
    );

    let selected = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let gated = manager
        .clone()
        .with_before_write_gate(selected.clone(), resume.clone());
    let exact_race_identity = format!("exact-race-identity-{suffix}");
    let exact_race_result = tokio::time::timeout(Duration::from_secs(10), async {
        let create = gated.create_or_replace_workspace_fixed_token(
            &workspace,
            &exact_race_identity,
            &exact_race,
            " exact-race-token ".into(),
        );
        let replace = async {
            selected.wait().await;
            put_spec(
                db,
                &exact_race_workspace_key,
                &fixed_manifest(&exact_race, "after"),
            )
            .await;
            resume.wait().await;
        };
        let (created, ()) = tokio::join!(create, replace);
        created
    })
    .await
    .expect("workspace spec replacement race must not deadlock")
    .expect("workspace spec replacement create");
    assert_reference_key(&exact_race_result, &exact_race_workspace_key, "after");
    let exact_raced = load_pair(db, &owner, &exact_race_identity).await;
    assert_eq!(exact_raced.0.as_ref(), Some(&exact_race_result));
    assert_eq!(exact_raced.1.as_ref().unwrap().document_version, 1);
    assert_material(
        exact_raced.1.as_ref().unwrap(),
        &owner,
        "exact-race-token",
        provider.as_ref(),
    );

    let selected = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let gated = manager
        .clone()
        .with_before_write_gate(selected.clone(), resume.clone());
    let shadow_race_identity = format!("shadow-race-identity-{suffix}");
    let shadow_race_result = tokio::time::timeout(Duration::from_secs(10), async {
        let create = gated.create_or_replace_workspace_fixed_token(
            &workspace,
            &shadow_race_identity,
            &shadow_race,
            " shadow-race-token ".into(),
        );
        let insert_shadow = async {
            selected.wait().await;
            put_spec(
                db,
                &shadow_race_workspace_key,
                &fixed_manifest(&shadow_race, "workspace_race"),
            )
            .await;
            resume.wait().await;
        };
        let (created, ()) = tokio::join!(create, insert_shadow);
        created
    })
    .await
    .expect("workspace shadow race must not deadlock")
    .expect("workspace shadow race create");
    assert_reference_key(
        &shadow_race_result,
        &shadow_race_workspace_key,
        "workspace_race",
    );
    let shadow_raced = load_pair(db, &owner, &shadow_race_identity).await;
    assert_eq!(shadow_raced.0.as_ref(), Some(&shadow_race_result));
    assert_eq!(shadow_raced.1.as_ref().unwrap().document_version, 1);
    assert_material(
        shadow_raced.1.as_ref().unwrap(),
        &owner,
        "shadow-race-token",
        provider.as_ref(),
    );

    let selected = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let gated = manager
        .clone()
        .with_before_write_gate(selected.clone(), resume.clone());
    let deleted_identity = format!("deleted-workspace-identity-{suffix}");
    let deleted_result = tokio::time::timeout(Duration::from_secs(10), async {
        let create = gated.create_or_replace_workspace_fixed_token(
            &deleted_workspace,
            &deleted_identity,
            &delete_race,
            " deleted-workspace-token ".into(),
        );
        let delete = async {
            selected.wait().await;
            delete_workspace(db, &deleted_workspace).await;
            resume.wait().await;
        };
        let (created, ()) = tokio::join!(create, delete);
        created
    })
    .await
    .expect("workspace deletion race must not deadlock");
    assert!(matches!(
        deleted_result,
        Err(AppError::WorkspaceNotFound(name)) if name == deleted_workspace.as_str()
    ));
    let deleted_owner = IdentityOwner::workspace(deleted_workspace);
    assert_eq!(
        load_pair(db, &deleted_owner, &deleted_identity).await,
        (None, None)
    );

    let selected = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let gated = manager
        .clone()
        .with_before_write_gate(selected.clone(), resume.clone());
    let recreated_identity = format!("recreated-workspace-identity-{suffix}");
    let recreated_result = tokio::time::timeout(Duration::from_secs(10), async {
        let create = gated.create_or_replace_workspace_fixed_token(
            &recreated_workspace,
            &recreated_identity,
            &recreate_race,
            " recreated-workspace-token ".into(),
        );
        let recreate = async {
            selected.wait().await;
            delete_workspace(db, &recreated_workspace).await;
            put_workspace_at(db, &recreated_workspace, 2).await;
            resume.wait().await;
        };
        let (created, ()) = tokio::join!(create, recreate);
        created
    })
    .await
    .expect("workspace recreation race must not deadlock");
    assert!(matches!(
        recreated_result,
        Err(AppError::WorkspaceNotFound(name)) if name == recreated_workspace.as_str()
    ));
    let recreated_owner = IdentityOwner::workspace(recreated_workspace);
    assert_eq!(
        load_pair(db, &recreated_owner, &recreated_identity).await,
        (None, None)
    );

    let selected = Arc::new(tokio::sync::Barrier::new(2));
    let resume_write = Arc::new(tokio::sync::Barrier::new(2));
    let retry_reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume_retry = Arc::new(tokio::sync::Barrier::new(2));
    let gated = manager
        .clone()
        .with_before_write_gate(selected.clone(), resume_write.clone())
        .with_before_retry_gate(retry_reached.clone(), resume_retry.clone());
    let retry_identity = format!("retry-recreated-workspace-identity-{suffix}");
    let retry_result = tokio::time::timeout(Duration::from_secs(10), async {
        let create = gated.create_or_replace_workspace_fixed_token(
            &retry_workspace,
            &retry_identity,
            &retry_recreate_race,
            " retry-recreated-workspace-token ".into(),
        );
        let recreate = async {
            selected.wait().await;
            put_spec(
                db,
                &retry_recreate_global_key,
                &fixed_manifest(&retry_recreate_race, "after_retry"),
            )
            .await;
            resume_write.wait().await;
            retry_reached.wait().await;
            delete_workspace(db, &retry_workspace).await;
            put_workspace_at(db, &retry_workspace, 2).await;
            resume_retry.wait().await;
        };
        let (created, ()) = tokio::join!(create, recreate);
        created
    })
    .await
    .expect("cross-attempt workspace recreation race must not deadlock");
    assert!(matches!(
        retry_result,
        Err(AppError::WorkspaceNotFound(name)) if name == retry_workspace.as_str()
    ));
    let retry_owner = IdentityOwner::workspace(retry_workspace.clone());
    assert_eq!(
        load_pair(db, &retry_owner, &retry_identity).await,
        (None, None)
    );
    put_spec(
        db,
        &retry_recreate_workspace_key,
        &oauth_manifest(&retry_recreate_race),
    )
    .await;
    assert!(matches!(
        manager
            .load_fixed_token_spec(
                &retry_owner,
                &retry_recreate_workspace_key,
                Some(1),
            )
            .await,
        Err(AppError::WorkspaceNotFound(name)) if name == retry_workspace.as_str()
    ));
    assert_eq!(manager.list_for_owner(&owner).await.unwrap().len(), 4);
}

async fn put_workspace(db: &Arc<CoralDb>, workspace: &WorkspaceName) {
    put_workspace_at(db, workspace, 1).await;
}

async fn put_workspace_at(
    db: &Arc<CoralDb>,
    workspace: &WorkspaceName,
    created_at_unix_nanos: i64,
) {
    let mut tx = db.begin().await.expect("begin workspace write");
    tx.workspaces()
        .ensure(workspace.as_str(), created_at_unix_nanos)
        .await
        .expect("write workspace");
    tx.commit().await.expect("commit workspace");
}

async fn delete_workspace(db: &Arc<CoralDb>, workspace: &WorkspaceName) {
    let mut tx = db.begin().await.expect("begin workspace delete");
    tx.workspaces()
        .delete(workspace.as_str())
        .await
        .expect("delete workspace");
    tx.commit().await.expect("commit workspace delete");
}

async fn put_spec(db: &Arc<CoralDb>, key: &IdentitySpecKey, yaml: &str) {
    let manifest = parse_identity_manifest_yaml(yaml).expect("valid identity manifest");
    let write = IdentitySpecWrite::new(
        manifest.version,
        manifest.description,
        manifest.issuer,
        manifest.identity_type.label(),
        yaml,
    )
    .expect("valid identity write");
    let mut tx = db.begin().await.expect("begin spec write");
    tx.identity_specs()
        .upsert(key, &write, 1)
        .await
        .expect("write spec");
    tx.commit().await.expect("commit spec");
}

async fn delete_spec(db: &Arc<CoralDb>, key: &IdentitySpecKey) {
    let mut tx = db.begin().await.unwrap();
    assert!(tx.identity_specs().delete(key).await.unwrap());
    tx.commit().await.unwrap();
}

async fn load_pair(
    db: &Arc<CoralDb>,
    owner: &IdentityOwner,
    name: &str,
) -> (Option<IdentityRecord>, Option<IdentityDocumentRecord>) {
    let name = IdentityName::parse(name).unwrap();
    let mut db = db.as_ref();
    let record = db.identities().load_optional(owner, &name).await.unwrap();
    let document = db
        .identity_documents()
        .load_optional(owner, &name)
        .await
        .unwrap();
    (record, document)
}

fn assert_reference(record: &IdentityRecord, spec_name: &str, label: &str) {
    assert_reference_key(record, &IdentitySpecKey::global(spec_name).unwrap(), label);
}

fn assert_reference_key(record: &IdentityRecord, key: &IdentitySpecKey, label: &str) {
    let manifest = parse_identity_manifest_yaml(&fixed_manifest(key.name(), label)).unwrap();
    assert_eq!(record.spec_reference.key(), key);
    assert_eq!(
        record.spec_reference.fingerprint(),
        identity_spec_fingerprint(&manifest).unwrap()
    );
    assert_eq!(record.spec_reference.issuer(), format!("issuer_{label}"));
    assert_eq!(record.spec_reference.identity_type(), "fixed_token");
}

fn assert_material(
    document: &IdentityDocumentRecord,
    owner: &IdentityOwner,
    token: &str,
    key_provider: &dyn CredentialKeyProvider,
) {
    let envelope = EncryptedEnvelopeDocument {
        ciphertext: document.ciphertext.clone(),
        nonce: document.nonce.clone(),
        wrapped_dek: document.wrapped_dek.clone(),
        wrapped_dek_nonce: document.wrapped_dek_nonce.clone(),
        key_id: document.key_id.clone(),
        algorithm: document.algorithm.clone(),
        aad_version: document.aad_version,
    };
    let values = decrypt_identity_document(
        owner.kind(),
        owner.key(),
        document.name.as_str(),
        &envelope,
        key_provider,
    )
    .expect("decrypt identity material");
    assert_eq!(
        values,
        std::collections::BTreeMap::from([("TOKEN".to_string(), token.to_string())])
    );
    assert!(
        !document
            .ciphertext
            .windows(token.len())
            .any(|window| window == token.as_bytes())
    );
}

fn fixed_manifest(name: &str, label: &str) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: {label}\ndescription: {label}\nissuer: issuer_{label}\ntype: fixed_token\n"
    )
}

fn oauth_manifest(name: &str) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: oauth\ndescription: oauth\nissuer: oauth_issuer\ntype: oauth\noauth:\n  method:\n    flow:\n      type: authorization_code\n      pkce: disabled\n    redirect_uri: http://127.0.0.1:53682/oauth/callback\n    endpoints:\n      authorization_url: https://provider.example.com/authorize\n      token_url: https://provider.example.com/token\n    client:\n      id:\n        default: client\n"
    )
}
