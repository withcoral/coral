use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coral_spec::parse_identity_manifest_yaml;
use sea_query::{Alias, Expr, ExprTrait, Query};
use tempfile::tempdir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{IdentityManager, IdentityOAuthCreationEvent, ResolvedIdentityForUse};
use crate::bootstrap::AppError;
use crate::credentials::CredentialsError;
use crate::credentials::encryption::{
    CredentialEncryptionKey, CredentialKeyProvider, EncryptedEnvelopeDocument,
};
use crate::identities::model::{IdentityName, IdentityOwner};
use crate::identity::{
    IDENTITY_DOCUMENT_AAD_VERSION, IdentityDocumentBinding, UserPrincipal,
    decrypt_identity_document, encrypt_identity_spec_document,
};
use crate::identity_specs::identity_spec_fingerprint;
use crate::identity_specs::manager::{IdentitySpecInputValue, IdentitySpecManager};
use crate::state::db::{
    CoralDb, DbError, DbRepos, DbSession, IdentityDocumentRecord, IdentityRecord,
    IdentitySpecDocumentRecord, IdentitySpecDocumentWrite, IdentitySpecKey, IdentitySpecScope,
    IdentitySpecWrite, ResolvedDatabaseConfig,
};
use crate::workspaces::WorkspaceName;

struct TestKeyProvider(Vec<CredentialEncryptionKey>);

impl CredentialKeyProvider for TestKeyProvider {
    fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
        self.0
            .last()
            .cloned()
            .ok_or_else(|| CredentialsError::Unavailable("missing test key".to_string()))
    }

    fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
        self.0
            .iter()
            .find(|key| key.key_id() == key_id)
            .cloned()
            .ok_or_else(|| CredentialsError::Unavailable("missing test key".to_string()))
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

#[tokio::test]
async fn sqlite_oauth_creation_core_contract() {
    let temp = tempdir().expect("temp dir");
    let db = Arc::new(
        CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite"),
    );
    db.migrate().await.expect("migrate sqlite");
    let provider = device_oauth_provider().await;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let oauth_spec = format!("oauth_{suffix}");
    let oauth_key = IdentitySpecKey::global(&oauth_spec).unwrap();
    let oauth_yaml = device_oauth_manifest(&oauth_spec, &provider.uri());
    let key_provider = Arc::new(TestKeyProvider(vec![
        CredentialEncryptionKey::from_static_bytes_for_test([70; 32]),
    ]));
    IdentitySpecManager::new(db.clone(), key_provider.clone())
        .add_or_replace_exact(
            IdentitySpecScope::global(),
            &oauth_yaml,
            vec![IdentitySpecInputValue::new(
                "OAUTH_CLIENT_ID",
                "spec-client-id",
            )],
        )
        .await
        .expect("install OAuth identity spec input");
    let manager = IdentityManager::new(db.clone(), key_provider.clone());
    let principal = UserPrincipal::for_user(&format!("oauth-{suffix}")).unwrap();
    let owner = IdentityOwner::for_user(principal.clone());
    let identity_name = format!("oauth-{suffix}");
    let events = Arc::new(Mutex::new(Vec::new()));
    let captured_events = events.clone();
    let created = manager
        .create_or_replace_user_oauth(&principal, &identity_name, &oauth_spec, move |event| {
            let events = captured_events.clone();
            async move {
                events.lock().expect("event lock").push(event);
                Ok(())
            }
        })
        .await
        .expect("create user OAuth identity");
    let expected_safe = BTreeMap::from([
        ("scope".to_string(), "repo user".to_string()),
        ("token_type".to_string(), "Bearer".to_string()),
    ]);
    assert_eq!(created.spec_reference.key(), &oauth_key);
    assert_eq!(
        created.spec_reference.fingerprint(),
        identity_spec_fingerprint(&parse_identity_manifest_yaml(&oauth_yaml).unwrap()).unwrap()
    );
    assert_eq!(created.spec_reference.identity_type(), "oauth");
    assert_eq!(created.safe_metadata, expected_safe);
    {
        let events = events.lock().expect("event lock");
        assert!(matches!(
            events.as_slice(),
            [IdentityOAuthCreationEvent::Authorization(authorization), IdentityOAuthCreationEvent::Completed(metadata)]
                if authorization.user_code.as_deref() == Some("ABCD-1234")
                    && authorization.authorization_url == "https://provider.example/device?user_code=ABCD-1234"
                    && metadata == &expected_safe
        ));
    }
    let (_, document) = load_pair(&db, &owner, &identity_name).await;
    let material = decrypt_material(
        &created,
        document.as_ref().expect("OAuth identity document"),
        key_provider.as_ref(),
    );
    assert_eq!(
        material.get("ACCESS_TOKEN").map(String::as_str),
        Some("access-token")
    );
    assert!(
        material
            .iter()
            .any(|(key, value)| key.ends_with(".refresh_token") && value == "refresh-token")
    );
    assert!(!material.keys().any(|key| {
        key.ends_with(".client_id")
            || key.ends_with(".client_secret")
            || key.ends_with(".token_url")
    }));
    assert!(
        material
            .values()
            .all(|value| value != "spec-client-id" && !value.contains(&provider.uri()))
    );
    let keyless = IdentityManager::new(db.clone(), Arc::new(TestKeyProvider(vec![])));
    assert_eq!(keyless.get(&owner, &identity_name).await.unwrap(), created);
    let requests = provider
        .received_requests()
        .await
        .expect("recorded requests");
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| {
        String::from_utf8_lossy(&request.body).contains("client_id=spec-client-id")
    }));
}

pub(crate) async fn assert_fixed_token_manager_contract(db: &Arc<CoralDb>) {
    Box::pin(assert_safe_metadata_keyless_read_contract(db)).await;
    Box::pin(assert_fixed_token_for_use_contract(db)).await;
    Box::pin(assert_fixed_token_for_use_race_contract(db)).await;
    Box::pin(assert_user_global_fixed_token_manager_contract(db)).await;
    Box::pin(assert_workspace_fixed_token_manager_contract(db)).await;
}

async fn assert_safe_metadata_keyless_read_contract(db: &Arc<CoralDb>) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let spec_name = format!("keyless_metadata_{suffix}");
    let identity_name = format!("keyless-metadata-{suffix}");
    let principal = UserPrincipal::for_user(&format!("keyless-metadata-{suffix}")).unwrap();
    let owner = IdentityOwner::for_user(principal.clone());
    let key = CredentialEncryptionKey::from_static_bytes_for_test([60; 32]);
    put_spec(
        db,
        &IdentitySpecKey::global(&spec_name).unwrap(),
        &fixed_manifest(&spec_name, "keyless"),
    )
    .await;
    let manager = IdentityManager::new(db.clone(), Arc::new(TestKeyProvider(vec![key])));
    let created = manager
        .create_or_replace_user_fixed_token(
            &principal,
            &identity_name,
            &spec_name,
            "secret-token".into(),
        )
        .await
        .expect("create identity with encrypted material");
    let expected_metadata = BTreeMap::from([
        ("scope".to_string(), "repo:read user:email".to_string()),
        ("token_type".to_string(), "Bearer".to_string()),
    ]);
    let name = IdentityName::parse(&identity_name).unwrap();
    let mut tx = db.begin_serializable().await.expect("begin metadata seed");
    let seeded = tx
        .identities()
        .upsert(
            &owner,
            &name,
            &created.spec_reference,
            &expected_metadata,
            created.updated_at_unix_nanos + 1,
        )
        .await
        .expect("persist safe metadata");
    tx.commit().await.expect("commit safe metadata");

    let unavailable = IdentityManager::new(db.clone(), Arc::new(TestKeyProvider(vec![])));
    let listed = unavailable
        .list_for_owner(&owner)
        .await
        .expect("list safe metadata without a key");
    assert_eq!(listed, vec![seeded.clone()]);
    assert_eq!(
        listed.first().expect("listed identity").safe_metadata,
        expected_metadata
    );
    let loaded = unavailable
        .get(&owner, &identity_name)
        .await
        .expect("get safe metadata without a key");
    assert_eq!(loaded, seeded);
    assert_eq!(loaded.safe_metadata, expected_metadata);
    assert!(matches!(
        unavailable.get_for_use(&owner, &identity_name).await,
        Err(AppError::Credentials(CredentialsError::Unavailable(_)))
    ));
}

async fn assert_fixed_token_for_use_contract(db: &Arc<CoralDb>) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let spec_name = format!("for_use_{suffix}");
    let identity_name = format!("for-use-{suffix}");
    let spec_key = IdentitySpecKey::global(&spec_name).unwrap();
    let principal = UserPrincipal::for_user(&format!("for-use-{suffix}")).unwrap();
    let owner = IdentityOwner::for_user(principal.clone());
    let old_key = CredentialEncryptionKey::from_static_bytes_for_test([61; 32]);
    let old_provider = Arc::new(TestKeyProvider(vec![old_key.clone()]));
    put_spec(db, &spec_key, &fixed_manifest(&spec_name, "use")).await;
    let manager = IdentityManager::new(db.clone(), old_provider);
    let created = manager
        .create_or_replace_user_fixed_token(
            &principal,
            &identity_name,
            &spec_name,
            " token-value ".into(),
        )
        .await
        .expect("create identity for use");
    let before_identity = load_pair(db, &owner, &identity_name).await.1.unwrap();

    let resolved = manager
        .get_for_use(&owner, &identity_name)
        .await
        .expect("resolve identity for use");
    assert_eq!(resolved.identity, created);
    assert_eq!(resolved.material().get("TOKEN").unwrap(), "token-value");
    assert_eq!(resolved.identity_spec.spec.key, spec_key);
    assert!(resolved.identity_spec.inputs.variables().is_empty());
    assert!(resolved.identity_spec.inputs.secrets().is_empty());
    let rendered = format!("{resolved:?}");
    assert!(!rendered.contains("token-value"));
    let _revision = resolved.revision();
    assert_eq!(
        load_pair(db, &owner, &identity_name).await.1.unwrap(),
        before_identity
    );

    let new_key = CredentialEncryptionKey::from_static_bytes_for_test([62; 32]);
    let rotated = IdentityManager::new(
        db.clone(),
        Arc::new(TestKeyProvider(vec![old_key, new_key.clone()])),
    );
    let rotated_result = rotated
        .get_for_use(&owner, &identity_name)
        .await
        .expect("resolve and rewrap identity for use");
    assert_eq!(
        rotated_result.material().get("TOKEN").unwrap(),
        "token-value"
    );
    let after_identity = load_pair(db, &owner, &identity_name).await.1.unwrap();
    assert_eq!(after_identity.document_version, 2);
    assert_eq!(after_identity.key_id, new_key.key_id());
    assert_eq!(after_identity.ciphertext, before_identity.ciphertext);
    assert_eq!(after_identity.nonce, before_identity.nonce);
    let reopened = rotated
        .get_for_use(&owner, &identity_name)
        .await
        .expect("reopen rewrapped identity");
    assert_eq!(reopened.material().get("TOKEN").unwrap(), "token-value");
    assert_eq!(
        load_pair(db, &owner, &identity_name).await.1.unwrap(),
        after_identity
    );

    let unavailable = IdentityManager::new(db.clone(), Arc::new(TestKeyProvider(vec![])));
    assert!(matches!(
        unavailable.get_for_use(&owner, &identity_name).await,
        Err(AppError::Credentials(CredentialsError::Unavailable(_)))
    ));
    assert_eq!(
        load_pair(db, &owner, &identity_name).await.1.unwrap(),
        after_identity
    );

    set_identity_aad_version(db, &owner, &identity_name, 1).await;
    assert!(matches!(
        unavailable.get_for_use(&owner, &identity_name).await,
        Err(AppError::FailedPrecondition(detail))
            if detail.contains("legacy") && detail.contains("recreate")
    ));
    set_identity_aad_version(db, &owner, &identity_name, IDENTITY_DOCUMENT_AAD_VERSION).await;
    put_spec(db, &spec_key, &oauth_manifest(&spec_name)).await;
    assert!(matches!(
        unavailable.get_for_use(&owner, &identity_name).await,
        Err(AppError::FailedPrecondition(detail))
            if detail.contains("no longer matches") && detail.contains("recreate")
    ));
    delete_spec(db, &spec_key).await;
    assert!(matches!(
        rotated.get_for_use(&owner, &identity_name).await,
        Err(AppError::FailedPrecondition(detail))
            if detail.contains("orphaned") && detail.contains("restore")
    ));
    assert_eq!(rotated.get(&owner, &identity_name).await.unwrap(), created);
}

async fn assert_fixed_token_for_use_race_contract(db: &Arc<CoralDb>) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let old_key = CredentialEncryptionKey::from_static_bytes_for_test([63; 32]);
    let new_key = CredentialEncryptionKey::from_static_bytes_for_test([64; 32]);
    Box::pin(assert_use_replacement_race(db, &suffix, &old_key, &new_key)).await;
    Box::pin(assert_use_delete_recreate_race(
        db, &suffix, &old_key, &new_key,
    ))
    .await;
    assert_use_spec_mutation_race(db, &suffix, &old_key, &new_key).await;
    Box::pin(assert_concurrent_rewrap_race(
        db, &suffix, &old_key, &new_key,
    ))
    .await;
}

struct UserUseRace {
    principal: UserPrincipal,
    owner: IdentityOwner,
    name: String,
    key: IdentitySpecKey,
}

async fn seed_user_use_race(
    db: &Arc<CoralDb>,
    suffix: &str,
    label: &str,
    old_key: &CredentialEncryptionKey,
) -> UserUseRace {
    let name = format!("use_{label}_{suffix}");
    let key = IdentitySpecKey::global(&name).unwrap();
    let principal = UserPrincipal::for_user(&format!("{label}-{suffix}")).unwrap();
    let owner = IdentityOwner::for_user(principal.clone());
    put_spec(db, &key, &fixed_manifest(&name, "race")).await;
    manager_with_keys(db, vec![old_key.clone()])
        .create_or_replace_user_fixed_token(&principal, &name, &name, "stale-token".into())
        .await
        .expect("seed identity-use race");
    UserUseRace {
        principal,
        owner,
        name,
        key,
    }
}

fn manager_with_keys(db: &Arc<CoralDb>, keys: Vec<CredentialEncryptionKey>) -> IdentityManager {
    IdentityManager::new(db.clone(), Arc::new(TestKeyProvider(keys)))
}

async fn race_before_use_cas<T, F, Fut>(
    manager: IdentityManager,
    owner: &IdentityOwner,
    name: &str,
    mutate: F,
) -> (Result<ResolvedIdentityForUse, AppError>, T)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let selected = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let gated = manager.with_before_use_cas_gate(selected.clone(), resume.clone());
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(gated.get_for_use(owner, name), async {
            selected.wait().await;
            let result = mutate().await;
            resume.wait().await;
            result
        })
    })
    .await
    .expect("identity-use race must not deadlock")
}

async fn assert_use_replacement_race(
    db: &Arc<CoralDb>,
    suffix: &str,
    old_key: &CredentialEncryptionKey,
    new_key: &CredentialEncryptionKey,
) {
    let race = seed_user_use_race(db, suffix, "replace", old_key).await;
    let replacement_spec = format!("{}_winner", race.name);
    let replacement_key = IdentitySpecKey::global(&replacement_spec).unwrap();
    put_spec(
        db,
        &replacement_key,
        &fixed_manifest(&replacement_spec, "winner"),
    )
    .await;
    let writer = manager_with_keys(db, vec![old_key.clone()]);
    let (resolved, replacement) = race_before_use_cas(
        manager_with_keys(db, vec![old_key.clone(), new_key.clone()]),
        &race.owner,
        &race.name,
        || {
            writer.create_or_replace_user_fixed_token(
                &race.principal,
                &race.name,
                &replacement_spec,
                "winner-token".into(),
            )
        },
    )
    .await;
    replacement.expect("concurrent replacement");
    let resolved = resolved.expect("replacement race resolution");
    assert_use_token(&resolved, "winner-token");
    assert_eq!(resolved.identity.spec_reference.key(), &replacement_key);
    assert_identity_document(db, &race.owner, &race.name, 3, new_key).await;
    let reopened =
        assert_reopens_without_repair(db, &race.owner, &race.name, new_key, "winner-token").await;
    assert_eq!(reopened.identity.spec_reference.key(), &replacement_key);
}

async fn assert_use_delete_recreate_race(
    db: &Arc<CoralDb>,
    suffix: &str,
    old_key: &CredentialEncryptionKey,
    new_key: &CredentialEncryptionKey,
) {
    let race = seed_user_use_race(db, suffix, "recreate", old_key).await;
    let writer = manager_with_keys(db, vec![old_key.clone()]);
    let (resolved, recreated) = race_before_use_cas(
        manager_with_keys(db, vec![old_key.clone(), new_key.clone()]),
        &race.owner,
        &race.name,
        || async {
            writer.delete(&race.owner, &race.name).await.unwrap();
            writer
                .create_or_replace_user_fixed_token(
                    &race.principal,
                    &race.name,
                    &race.name,
                    "winner-token".into(),
                )
                .await
        },
    )
    .await;
    recreated.expect("concurrent delete/recreate");
    assert_use_token(&resolved.expect("ABA race resolution"), "winner-token");
    assert_identity_document(db, &race.owner, &race.name, 2, new_key).await;
    assert_reopens_without_repair(db, &race.owner, &race.name, new_key, "winner-token").await;
}

async fn assert_use_spec_mutation_race(
    db: &Arc<CoralDb>,
    suffix: &str,
    old_key: &CredentialEncryptionKey,
    new_key: &CredentialEncryptionKey,
) {
    let race = seed_user_use_race(db, suffix, "spec", old_key).await;
    let before = load_pair(db, &race.owner, &race.name).await.1.unwrap();
    let (resolved, ()) = race_before_use_cas(
        manager_with_keys(db, vec![old_key.clone(), new_key.clone()]),
        &race.owner,
        &race.name,
        || async {
            let changed = fixed_manifest(&race.name, "changed");
            put_spec(db, &race.key, &changed).await;
        },
    )
    .await;
    assert!(matches!(
        resolved,
        Err(AppError::FailedPrecondition(detail)) if detail.contains("no longer matches")
    ));
    assert_eq!(
        load_pair(db, &race.owner, &race.name).await.1.unwrap(),
        before
    );
}

async fn assert_concurrent_rewrap_race(
    db: &Arc<CoralDb>,
    suffix: &str,
    old_key: &CredentialEncryptionKey,
    new_key: &CredentialEncryptionKey,
) {
    let race = seed_user_use_race(db, suffix, "rewrap", old_key).await;
    let before_identity = load_pair(db, &race.owner, &race.name).await.1.unwrap();
    let old_provider = TestKeyProvider(vec![old_key.clone()]);
    let before_spec = put_empty_spec_document(db, &race.key, &old_provider, 1).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let base = manager_with_keys(db, vec![old_key.clone(), new_key.clone()]);
    let left = base.clone().with_before_upsert_gate(Arc::clone(&barrier));
    let right = base.with_before_upsert_gate(barrier);
    let (left, right) = Box::pin(tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            left.get_for_use(&race.owner, &race.name),
            right.get_for_use(&race.owner, &race.name),
        )
    }))
    .await
    .expect("concurrent rewrap must not deadlock");
    assert_use_token(&left.expect("left rewrap"), "stale-token");
    assert_use_token(&right.expect("right rewrap"), "stale-token");
    let after_identity = assert_identity_document(db, &race.owner, &race.name, 2, new_key).await;
    assert_eq!(after_identity.ciphertext, before_identity.ciphertext);
    assert_eq!(after_identity.nonce, before_identity.nonce);
    assert_ne!(after_identity.wrapped_dek, before_identity.wrapped_dek);
    assert_ne!(
        after_identity.wrapped_dek_nonce,
        before_identity.wrapped_dek_nonce
    );
    let after_spec = load_spec_document(db, &race.key).await;
    assert_eq!(after_spec.document_version, 2);
    assert_eq!(after_spec.key_id, new_key.key_id());
    assert_eq!(after_spec.ciphertext, before_spec.ciphertext);
    assert_eq!(after_spec.nonce, before_spec.nonce);
    assert_ne!(after_spec.wrapped_dek, before_spec.wrapped_dek);
    assert_ne!(after_spec.wrapped_dek_nonce, before_spec.wrapped_dek_nonce);
    assert_reopens_without_repair(db, &race.owner, &race.name, new_key, "stale-token").await;
    assert_eq!(load_spec_document(db, &race.key).await, after_spec);
}

fn assert_use_token(resolved: &ResolvedIdentityForUse, token: &str) {
    assert_eq!(resolved.material().get("TOKEN").unwrap(), token);
}

async fn assert_identity_document(
    db: &Arc<CoralDb>,
    owner: &IdentityOwner,
    name: &str,
    version: i64,
    key: &CredentialEncryptionKey,
) -> IdentityDocumentRecord {
    let document = load_pair(db, owner, name).await.1.unwrap();
    assert_eq!(document.document_version, version);
    assert_eq!(document.key_id, key.key_id());
    document
}

async fn assert_reopens_without_repair(
    db: &Arc<CoralDb>,
    owner: &IdentityOwner,
    name: &str,
    key: &CredentialEncryptionKey,
    token: &str,
) -> ResolvedIdentityForUse {
    let before = load_pair(db, owner, name).await;
    let resolved = manager_with_keys(db, vec![key.clone()])
        .get_for_use(owner, name)
        .await
        .expect("reopen raced identity with only the new key");
    assert_use_token(&resolved, token);
    assert!(resolved.identity_spec.inputs.variables().is_empty());
    assert!(resolved.identity_spec.inputs.secrets().is_empty());
    assert_eq!(load_pair(db, owner, name).await, before);
    resolved
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
        before_a.0.as_ref().unwrap(),
        before_a.1.as_ref().unwrap(),
        "alpha-token",
        old_provider.as_ref(),
    );
    assert_material(
        before_b.0.as_ref().unwrap(),
        before_b.1.as_ref().unwrap(),
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
        after_a.0.as_ref().unwrap(),
        after_document,
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
        conflict_record,
        conflict.1.as_ref().unwrap(),
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
    assert_eq!(raced.0.as_ref(), Some(&race_result));
    assert_eq!(raced.1.as_ref().unwrap().document_version, 1);
    assert_material(
        raced.0.as_ref().unwrap(),
        raced.1.as_ref().unwrap(),
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
        fallback_pair.0.as_ref().unwrap(),
        fallback_pair.1.as_ref().unwrap(),
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
        shadowed_pair.0.as_ref().unwrap(),
        shadowed_pair.1.as_ref().unwrap(),
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
    let fallback_for_use = manager
        .get_for_use(&owner, &fallback_identity)
        .await
        .expect("resolve persisted global fallback after late shadow");
    assert_eq!(fallback_for_use.identity_spec.spec.key, fallback_global_key);
    assert_eq!(
        fallback_for_use.material().get("TOKEN").unwrap(),
        "fallback-token"
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
        exact_raced.0.as_ref().unwrap(),
        exact_raced.1.as_ref().unwrap(),
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
        shadow_raced.0.as_ref().unwrap(),
        shadow_raced.1.as_ref().unwrap(),
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

    let atomic_delete_name = format!("delete-retry-{suffix}");
    manager
        .create_or_replace_workspace_fixed_token(
            &retry_workspace,
            &atomic_delete_name,
            &fallback,
            "delete-token".into(),
        )
        .await
        .expect("create identity for delete race");
    assert_workspace_delete_race(db, &manager, &retry_workspace, &atomic_delete_name).await;
    assert_workspace_list_race(db, &manager, &workspace, &owner).await;
}

async fn assert_workspace_list_race(
    db: &Arc<CoralDb>,
    manager: &IdentityManager,
    workspace: &WorkspaceName,
    owner: &IdentityOwner,
) {
    let selected = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let gated = manager
        .clone()
        .with_before_write_gate(selected.clone(), resume.clone());
    let (listed, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(gated.list_for_owner(owner), async {
            selected.wait().await;
            let mut tx = db.begin().await.expect("begin concurrent workspace delete");
            let sqlite = tx
                .disable_sqlite_busy_wait()
                .await
                .expect("configure concurrent workspace delete");
            tx.workspaces()
                .delete(workspace.as_str())
                .await
                .expect("stage concurrent workspace delete");
            let commit = tx.commit().await;
            if sqlite {
                assert!(matches!(
                    commit,
                    Err(DbError::RetryableTransactionConflict(_))
                ));
            } else {
                commit.expect("commit concurrent Postgres workspace delete");
            }
            resume.wait().await;
        })
    })
    .await
    .expect("workspace list/delete race must not deadlock");
    assert_eq!(listed.expect("list from one workspace snapshot").len(), 4);
}

async fn assert_workspace_delete_race(
    db: &Arc<CoralDb>,
    manager: &IdentityManager,
    workspace: &WorkspaceName,
    identity: &str,
) {
    let selected = Arc::new(tokio::sync::Barrier::new(2));
    let resume_write = Arc::new(tokio::sync::Barrier::new(2));
    let retry_reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume_retry = Arc::new(tokio::sync::Barrier::new(2));
    let gated = manager
        .clone()
        .with_before_write_gate(selected.clone(), resume_write.clone())
        .with_before_retry_gate(retry_reached.clone(), resume_retry.clone());
    let owner = IdentityOwner::workspace(workspace.clone());
    let (deleted, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(gated.delete(&owner, identity), async {
            selected.wait().await;
            let mut tx = db.begin().await.expect("begin raced workspace delete");
            let sqlite = tx
                .disable_sqlite_busy_wait()
                .await
                .expect("configure raced workspace delete");
            tx.workspaces()
                .delete(workspace.as_str())
                .await
                .expect("stage raced workspace delete");
            if sqlite {
                resume_write.wait().await;
                retry_reached.wait().await;
                tx.commit()
                    .await
                    .expect("commit raced SQLite workspace delete");
            } else {
                tx.commit()
                    .await
                    .expect("commit raced Postgres workspace delete");
                resume_write.wait().await;
                retry_reached.wait().await;
            }
            resume_retry.wait().await;
        })
    })
    .await
    .expect("workspace identity delete race must not deadlock");
    assert!(matches!(
        deleted,
        Err(AppError::WorkspaceNotFound(name)) if name == workspace.as_str()
    ));
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

async fn put_empty_spec_document(
    db: &Arc<CoralDb>,
    key: &IdentitySpecKey,
    key_provider: &dyn CredentialKeyProvider,
    now: i64,
) -> IdentitySpecDocumentRecord {
    let (scope_kind, scope_id, name) = key.document_aad_parts();
    let document =
        encrypt_identity_spec_document(scope_kind, scope_id, name, &BTreeMap::new(), key_provider)
            .unwrap();
    let write = IdentitySpecDocumentWrite::new(
        document.ciphertext,
        document.nonce,
        document.wrapped_dek,
        document.wrapped_dek_nonce,
        document.key_id,
        document.algorithm,
        document.aad_version,
    )
    .unwrap();
    let mut tx = db.begin().await.unwrap();
    let record = tx
        .identity_spec_documents()
        .upsert(key, &write, now)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    record
}

async fn load_spec_document(
    db: &Arc<CoralDb>,
    key: &IdentitySpecKey,
) -> IdentitySpecDocumentRecord {
    let mut db = db.as_ref();
    db.identity_spec_documents()
        .load_optional(key)
        .await
        .unwrap()
        .unwrap()
}

async fn set_identity_aad_version(
    db: &Arc<CoralDb>,
    owner: &IdentityOwner,
    name: &str,
    aad_version: i64,
) {
    let mut db = db.as_ref();
    db.execute(
        Query::update()
            .table(Alias::new("identity_documents"))
            .value(Alias::new("aad_version"), aad_version)
            .and_where(Expr::col(Alias::new("owner_kind")).eq(owner.kind()))
            .and_where(Expr::col(Alias::new("owner_key")).eq(owner.key()))
            .and_where(Expr::col(Alias::new("name")).eq(name))
            .to_owned(),
    )
    .await
    .unwrap();
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
    record: &IdentityRecord,
    document: &IdentityDocumentRecord,
    token: &str,
    key_provider: &dyn CredentialKeyProvider,
) {
    let values = decrypt_material(record, document, key_provider);
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

fn decrypt_material(
    record: &IdentityRecord,
    document: &IdentityDocumentRecord,
    key_provider: &dyn CredentialKeyProvider,
) -> BTreeMap<String, String> {
    assert_eq!(document.aad_version, IDENTITY_DOCUMENT_AAD_VERSION);
    let reference = &record.spec_reference;
    let (spec_scope_kind, spec_scope_id, spec_name) = reference.key().document_aad_parts();
    let binding = IdentityDocumentBinding::new(
        record.owner.kind(),
        record.owner.key(),
        record.name.as_str(),
        spec_scope_kind,
        spec_scope_id,
        spec_name,
        reference.fingerprint(),
    );
    let envelope = EncryptedEnvelopeDocument {
        ciphertext: document.ciphertext.clone(),
        nonce: document.nonce.clone(),
        wrapped_dek: document.wrapped_dek.clone(),
        wrapped_dek_nonce: document.wrapped_dek_nonce.clone(),
        key_id: document.key_id.clone(),
        algorithm: document.algorithm.clone(),
        aad_version: document.aad_version,
    };
    decrypt_identity_document(&binding, &envelope, key_provider).expect("decrypt identity material")
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

fn device_oauth_manifest(name: &str, base_url: &str) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: oauth\ndescription: oauth\nissuer: oauth_issuer\ntype: oauth\ninputs:\n  OAUTH_CLIENT_ID:\n    kind: variable\n    required: true\noauth:\n  method:\n    flow:\n      type: device_code\n    endpoints:\n      device_authorization_url: {base_url}/device\n      token_url: {base_url}/token\n    client:\n      id:\n        input: OAUTH_CLIENT_ID\n"
    )
}

async fn device_oauth_provider() -> MockServer {
    let provider = MockServer::start().await;
    for (endpoint, response) in [
        (
            "/device",
            r#"{"device_code":"device-code","user_code":"ABCD-1234","verification_uri":"https://provider.example/device","verification_uri_complete":"https://provider.example/device?user_code=ABCD-1234","expires_in":60,"interval":1}"#,
        ),
        (
            "/token",
            r#"{"access_token":"access-token","refresh_token":"refresh-token","token_type":"Bearer","scope":"repo user"}"#,
        ),
    ] {
        Mock::given(method("POST"))
            .and(path(endpoint))
            .respond_with(ResponseTemplate::new(200).set_body_raw(response, "application/json"))
            .mount(&provider)
            .await;
    }
    provider
}
