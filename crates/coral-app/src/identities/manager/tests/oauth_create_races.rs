use super::*;

use crate::identity::decrypt_identity_spec_document;
use crate::state::db::IdentitySpecRecord;
use wiremock::Request;

#[tokio::test]
async fn sqlite_oauth_creation_race_contract() {
    let temp = tempdir().expect("temp dir");
    let db = Arc::new(
        CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite"),
    );
    db.migrate().await.expect("migrate sqlite");
    Box::pin(assert_oauth_creation_race_contract(&db)).await;
}

#[tokio::test]
async fn sqlite_oauth_creation_document_rewrap_race_contract() {
    let temp = tempdir().expect("temp dir");
    let db = Arc::new(
        CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite"),
    );
    db.migrate().await.expect("migrate sqlite");
    Box::pin(assert_oauth_creation_document_rewrap_race_contract(&db)).await;
}

pub(crate) async fn assert_oauth_creation_race_contract(db: &Arc<CoralDb>) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let key_provider = Arc::new(TestKeyProvider(vec![
        CredentialEncryptionKey::from_static_bytes_for_test([71; 32]),
    ]));

    Box::pin(assert_creation_cancellation(db, &key_provider, &suffix)).await;
    Box::pin(assert_target_replacement(db, &key_provider, &suffix)).await;
    Box::pin(assert_spec_input_mutation(db, &key_provider, &suffix)).await;
    Box::pin(assert_fallback_shadow_insertion(db, &key_provider, &suffix)).await;
    Box::pin(assert_workspace_generation_aba(db, &key_provider, &suffix)).await;
}

pub(crate) async fn assert_oauth_creation_document_rewrap_race_contract(db: &Arc<CoralDb>) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let provider = device_oauth_provider().await;
    let fixed_spec = format!("fixed_rewrap_{suffix}");
    let oauth_spec = format!("oauth_rewrap_{suffix}");
    let identity_name = format!("oauth-rewrap-{suffix}");
    let principal = UserPrincipal::for_user(&format!("oauth-rewrap-{suffix}")).unwrap();
    let owner = IdentityOwner::for_user(principal.clone());
    let client_id = format!("rewrap-client-{suffix}");
    let fixed_key = IdentitySpecKey::global(&fixed_spec).unwrap();
    let oauth_key = IdentitySpecKey::global(&oauth_spec).unwrap();
    let old_key = CredentialEncryptionKey::from_static_bytes_for_test([72; 32]);
    let new_key = CredentialEncryptionKey::from_static_bytes_for_test([73; 32]);

    put_spec(db, &fixed_key, &fixed_manifest(&fixed_spec, "oauth_rewrap")).await;
    put_spec(
        db,
        &oauth_key,
        &device_default_client_oauth_manifest(&oauth_spec, &provider.uri(), &client_id),
    )
    .await;
    assert_spec_document_absent(db, &fixed_key).await;
    assert_spec_document_absent(db, &oauth_key).await;

    let seeded = manager_with_keys(db, vec![old_key.clone()])
        .create_or_replace_user_fixed_token(
            &principal,
            &identity_name,
            &fixed_spec,
            "rewrap-token".into(),
        )
        .await
        .expect("seed OAuth document-rewrap target");
    let (before_record, before_document) = load_pair(db, &owner, &identity_name).await;
    let before_record = before_record.expect("seeded identity row");
    let before_document = before_document.expect("seeded identity document");
    assert_eq!(before_record, seeded);
    assert_eq!(before_document.document_version, 1);
    assert_eq!(before_document.key_id, old_key.key_id());

    let rotating = manager_with_keys(db, vec![old_key.clone(), new_key.clone()]);
    let rewrap_manager = rotating.clone();
    let (oauth_result, resolved) = Box::pin(race_at_event(
        rotating,
        OAuthRaceOwner::User(principal),
        identity_name.clone(),
        oauth_spec,
        GatedOAuthEvent::Completed,
        || rewrap_manager.get_for_use(&owner, &identity_name),
    ))
    .await;
    assert_use_token(
        &resolved.expect("rewrap target identity while OAuth is completed"),
        "rewrap-token",
    );
    assert!(matches!(
        oauth_result,
        Err(AppError::RetryableTransactionConflict)
    ));

    let (after_record, after_document) = load_pair(db, &owner, &identity_name).await;
    let after_record = after_record.expect("identity row after document rewrap");
    let after_document = after_document.expect("rewrapped identity document");
    assert_eq!(after_record, before_record);
    assert_eq!(
        after_document.document_version,
        before_document.document_version + 1
    );
    assert_eq!(after_document.key_id, new_key.key_id());
    assert_eq!(after_document.ciphertext, before_document.ciphertext);
    assert_eq!(after_document.nonce, before_document.nonce);
    assert_ne!(after_document.wrapped_dek, before_document.wrapped_dek);
    assert_ne!(
        after_document.wrapped_dek_nonce,
        before_document.wrapped_dek_nonce
    );
    assert_eq!(after_document.algorithm, before_document.algorithm);
    assert_eq!(after_document.aad_version, before_document.aad_version);
    assert_eq!(
        after_document.created_at_unix_nanos,
        before_document.created_at_unix_nanos
    );

    let new_key_only = TestKeyProvider(vec![new_key.clone()]);
    assert_material(
        &after_record,
        &after_document,
        "rewrap-token",
        &new_key_only,
    );
    assert_reopens_without_repair(db, &owner, &identity_name, &new_key, "rewrap-token").await;
    assert_spec_document_absent(db, &fixed_key).await;
    assert_spec_document_absent(db, &oauth_key).await;
    assert_static_provider_requests(&provider, &client_id, None).await;
}

enum OAuthRaceOwner {
    User(UserPrincipal),
    Workspace(WorkspaceName),
}

#[derive(Clone, Copy)]
enum GatedOAuthEvent {
    Authorization,
    Completed,
}

impl GatedOAuthEvent {
    fn matches(self, event: &IdentityOAuthCreationEvent) -> bool {
        match self {
            Self::Authorization => matches!(event, IdentityOAuthCreationEvent::Authorization(_)),
            Self::Completed => matches!(event, IdentityOAuthCreationEvent::Completed(_)),
        }
    }
}

async fn assert_creation_cancellation(
    db: &Arc<CoralDb>,
    key_provider: &Arc<TestKeyProvider>,
    suffix: &str,
) {
    let provider = device_oauth_provider().await;
    let spec_name = format!("oauth_cancel_{suffix}");
    let identity_name = format!("oauth-cancel-{suffix}");
    let principal = UserPrincipal::for_user(&format!("oauth-cancel-{suffix}")).unwrap();
    let client_id = format!("cancel-client-{suffix}");
    let manifest = device_oauth_manifest(&spec_name, &provider.uri());
    install_oauth_spec(
        db,
        key_provider,
        IdentitySpecScope::global(),
        &manifest,
        Some(&client_id),
    )
    .await;

    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let manager = IdentityManager::new(db.clone(), key_provider.clone())
        .with_before_write_gate(reached.clone(), resume);
    let commit_phase = IdentityOAuthCommitPhase::default();
    let task_principal = principal.clone();
    let task_identity_name = identity_name.clone();
    let task_spec_name = spec_name.clone();
    let task_commit_phase = commit_phase.clone();
    let task = tokio::spawn(async move {
        manager
            .create_or_replace_user_oauth(
                &task_principal,
                &task_identity_name,
                &task_spec_name,
                task_commit_phase,
                |_| async { Ok(()) },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), reached.wait())
        .await
        .expect("OAuth cancellation must reach the transactional identity upsert");
    assert!(commit_phase.has_started());
    task.abort();
    let cancelled = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("OAuth cancellation must terminate")
        .expect_err("OAuth creation must be cancelled");
    assert!(cancelled.is_cancelled());

    let owner = IdentityOwner::for_user(principal);
    assert_eq!(load_pair(db, &owner, &identity_name).await, (None, None));
    assert_static_provider_requests(&provider, &client_id, None).await;
}

async fn assert_target_replacement(
    db: &Arc<CoralDb>,
    key_provider: &Arc<TestKeyProvider>,
    suffix: &str,
) {
    let provider = device_dcr_oauth_provider().await;
    let oauth_spec = format!("oauth_replace_{suffix}");
    let oauth_manifest = device_dcr_oauth_manifest(&oauth_spec, &provider.uri());
    install_oauth_spec(
        db,
        key_provider,
        IdentitySpecScope::global(),
        &oauth_manifest,
        None,
    )
    .await;

    let first_spec = format!("fixed_replace_first_{suffix}");
    let winner_spec = format!("fixed_replace_winner_{suffix}");
    put_spec(
        db,
        &IdentitySpecKey::global(&first_spec).unwrap(),
        &fixed_manifest(&first_spec, "race_first"),
    )
    .await;
    put_spec(
        db,
        &IdentitySpecKey::global(&winner_spec).unwrap(),
        &fixed_manifest(&winner_spec, "race_winner"),
    )
    .await;

    let principal = UserPrincipal::for_user(&format!("oauth-replace-{suffix}")).unwrap();
    let owner = IdentityOwner::for_user(principal.clone());
    let identity_name = format!("oauth-replace-{suffix}");
    let manager = IdentityManager::new(db.clone(), key_provider.clone());
    manager
        .create_or_replace_user_fixed_token(
            &principal,
            &identity_name,
            &first_spec,
            "first-token".into(),
        )
        .await
        .expect("seed OAuth replacement target");

    let writer = manager.clone();
    let (oauth_result, replacement) = Box::pin(race_at_event(
        manager,
        OAuthRaceOwner::User(principal.clone()),
        identity_name.clone(),
        oauth_spec,
        GatedOAuthEvent::Completed,
        || {
            writer.create_or_replace_user_fixed_token(
                &principal,
                &identity_name,
                &winner_spec,
                "winner-token".into(),
            )
        },
    ))
    .await;
    assert!(matches!(
        oauth_result,
        Err(AppError::RetryableTransactionConflict)
    ));
    let replacement = replacement.expect("concurrent fixed-token replacement");
    let (record, document) = load_pair(db, &owner, &identity_name).await;
    let record = record.expect("replacement identity");
    let document = document.expect("replacement document");
    assert_eq!(record, replacement);
    assert_reference(&record, &winner_spec, "race_winner");
    assert_eq!(document.document_version, 2);
    assert_material(&record, &document, "winner-token", key_provider.as_ref());
    assert_dcr_provider_requests(&provider).await;
}

async fn assert_spec_input_mutation(
    db: &Arc<CoralDb>,
    key_provider: &Arc<TestKeyProvider>,
    suffix: &str,
) {
    let provider = device_oauth_provider().await;
    let spec_name = format!("oauth_input_{suffix}");
    let identity_name = format!("oauth-input-{suffix}");
    let principal = UserPrincipal::for_user(&format!("oauth-input-{suffix}")).unwrap();
    let owner = IdentityOwner::for_user(principal.clone());
    let old_client = format!("input-before-{suffix}");
    let new_client = format!("input-after-{suffix}");
    let manifest = device_oauth_manifest(&spec_name, &provider.uri());
    install_oauth_spec(
        db,
        key_provider,
        IdentitySpecScope::global(),
        &manifest,
        Some(&old_client),
    )
    .await;
    let key = IdentitySpecKey::global(&spec_name).unwrap();
    let before_spec = load_exact_spec(db, &key).await;
    let before_document = load_spec_document(db, &key).await;
    let next_timestamp = before_document
        .updated_at_unix_nanos
        .checked_add(1)
        .expect("identity spec document timestamp");
    let changed_values = BTreeMap::from([("OAUTH_CLIENT_ID".to_string(), new_client.clone())]);

    let (oauth_result, changed_document) = Box::pin(race_at_event(
        IdentityManager::new(db.clone(), key_provider.clone()),
        OAuthRaceOwner::User(principal),
        identity_name.clone(),
        spec_name,
        GatedOAuthEvent::Authorization,
        || {
            put_spec_document(
                db,
                &key,
                &changed_values,
                key_provider.as_ref(),
                next_timestamp,
            )
        },
    ))
    .await;
    assert!(matches!(
        oauth_result,
        Err(AppError::RetryableTransactionConflict)
    ));
    assert_eq!(load_exact_spec(db, &key).await, before_spec);
    assert_eq!(
        changed_document.document_version,
        before_document.document_version + 1
    );
    assert_eq!(
        decrypt_spec_values(&key, &changed_document, key_provider.as_ref()),
        changed_values
    );
    assert_eq!(load_pair(db, &owner, &identity_name).await, (None, None));
    assert_static_provider_requests(&provider, &old_client, Some(&new_client)).await;
}

async fn assert_fallback_shadow_insertion(
    db: &Arc<CoralDb>,
    key_provider: &Arc<TestKeyProvider>,
    suffix: &str,
) {
    let provider = device_oauth_provider().await;
    let workspace = WorkspaceName::parse(&format!("oauth_shadow_{suffix}")).unwrap();
    put_workspace(db, &workspace).await;
    let spec_name = format!("oauth_shadow_{suffix}");
    let identity_name = format!("oauth-shadow-{suffix}");
    let owner = IdentityOwner::workspace(workspace.clone());
    let old_client = format!("shadow-global-{suffix}");
    let new_client = format!("shadow-workspace-{suffix}");
    let global_key = IdentitySpecKey::global(&spec_name).unwrap();
    let global_manifest =
        device_default_client_oauth_manifest(&spec_name, &provider.uri(), &old_client);
    put_spec(db, &global_key, &global_manifest).await;

    let workspace_key = IdentitySpecKey::workspace(workspace.clone(), &spec_name).unwrap();
    let workspace_manifest =
        device_default_client_oauth_manifest(&spec_name, &provider.uri(), &new_client);
    let (oauth_result, ()) = Box::pin(race_at_event(
        IdentityManager::new(db.clone(), key_provider.clone()),
        OAuthRaceOwner::Workspace(workspace.clone()),
        identity_name.clone(),
        spec_name,
        GatedOAuthEvent::Completed,
        || put_spec(db, &workspace_key, &workspace_manifest),
    ))
    .await;
    assert!(matches!(
        oauth_result,
        Err(AppError::RetryableTransactionConflict)
    ));
    assert_eq!(load_exact_spec(db, &workspace_key).await.key, workspace_key);
    let mut session = db.as_ref();
    for key in [&global_key, &workspace_key] {
        assert!(
            session
                .identity_spec_documents()
                .load_optional(key)
                .await
                .expect("load absent shadow-race input document")
                .is_none()
        );
    }
    assert_eq!(load_pair(db, &owner, &identity_name).await, (None, None));
    assert_static_provider_requests(&provider, &old_client, Some(&new_client)).await;
}

fn device_default_client_oauth_manifest(name: &str, base_url: &str, client_id: &str) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: oauth\ndescription: oauth\nissuer: oauth_issuer\ntype: oauth\noauth:\n  method:\n    flow:\n      type: device_code\n    endpoints:\n      device_authorization_url: {base_url}/device\n      token_url: {base_url}/token\n    client:\n      id:\n        default: {client_id}\n"
    )
}

async fn assert_workspace_generation_aba(
    db: &Arc<CoralDb>,
    key_provider: &Arc<TestKeyProvider>,
    suffix: &str,
) {
    let provider = device_oauth_provider().await;
    let workspace = WorkspaceName::parse(&format!("oauth_aba_{suffix}")).unwrap();
    put_workspace_at(db, &workspace, 1).await;
    let spec_name = format!("oauth_aba_{suffix}");
    let identity_name = format!("oauth-aba-{suffix}");
    let owner = IdentityOwner::workspace(workspace.clone());
    let client_id = format!("aba-client-{suffix}");
    let manifest = device_oauth_manifest(&spec_name, &provider.uri());
    install_oauth_spec(
        db,
        key_provider,
        IdentitySpecScope::global(),
        &manifest,
        Some(&client_id),
    )
    .await;

    let (oauth_result, ()) = Box::pin(race_at_event(
        IdentityManager::new(db.clone(), key_provider.clone()),
        OAuthRaceOwner::Workspace(workspace.clone()),
        identity_name.clone(),
        spec_name,
        GatedOAuthEvent::Completed,
        || async {
            delete_workspace(db, &workspace).await;
            put_workspace_at(db, &workspace, 2).await;
        },
    ))
    .await;
    assert!(matches!(
        oauth_result,
        Err(AppError::WorkspaceNotFound(name)) if name == workspace.as_str()
    ));
    let current_workspace = {
        let mut session = db.as_ref();
        session
            .workspaces()
            .get(workspace.as_str())
            .await
            .expect("load recreated workspace")
            .expect("recreated workspace")
    };
    assert_eq!(current_workspace.created_at_unix_nanos, 2);
    assert_eq!(load_pair(db, &owner, &identity_name).await, (None, None));
    assert_static_provider_requests(&provider, &client_id, None).await;
}

async fn create_gated_oauth(
    manager: IdentityManager,
    owner: OAuthRaceOwner,
    identity_name: String,
    spec_name: String,
    gated_event: GatedOAuthEvent,
    reached: Arc<tokio::sync::Barrier>,
    resume: Arc<tokio::sync::Barrier>,
) -> Result<IdentityRecord, AppError> {
    match owner {
        OAuthRaceOwner::User(principal) => {
            manager
                .create_or_replace_user_oauth(
                    &principal,
                    &identity_name,
                    &spec_name,
                    IdentityOAuthCommitPhase::default(),
                    move |event| {
                        let reached = reached.clone();
                        let resume = resume.clone();
                        async move {
                            if gated_event.matches(&event) {
                                reached.wait().await;
                                resume.wait().await;
                            }
                            Ok(())
                        }
                    },
                )
                .await
        }
        OAuthRaceOwner::Workspace(workspace) => {
            manager
                .create_or_replace_workspace_oauth(
                    &workspace,
                    &identity_name,
                    &spec_name,
                    IdentityOAuthCommitPhase::default(),
                    move |event| {
                        let reached = reached.clone();
                        let resume = resume.clone();
                        async move {
                            if gated_event.matches(&event) {
                                reached.wait().await;
                                resume.wait().await;
                            }
                            Ok(())
                        }
                    },
                )
                .await
        }
    }
}

async fn race_at_event<T, F, Fut>(
    manager: IdentityManager,
    owner: OAuthRaceOwner,
    identity_name: String,
    spec_name: String,
    gated_event: GatedOAuthEvent,
    mutate: F,
) -> (Result<IdentityRecord, AppError>, T)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let creation = create_gated_oauth(
        manager,
        owner,
        identity_name,
        spec_name,
        gated_event,
        reached.clone(),
        resume.clone(),
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(creation, async {
            reached.wait().await;
            let result = mutate().await;
            resume.wait().await;
            result
        })
    })
    .await
    .expect("OAuth creation race must not deadlock")
}

async fn install_oauth_spec(
    db: &Arc<CoralDb>,
    key_provider: &Arc<TestKeyProvider>,
    scope: IdentitySpecScope,
    manifest: &str,
    client_id: Option<&str>,
) {
    let inputs = client_id
        .into_iter()
        .map(|value| IdentitySpecInputValue::new("OAUTH_CLIENT_ID", value))
        .collect();
    IdentitySpecManager::new(db.clone(), key_provider.clone())
        .add_or_replace_exact(scope, manifest, inputs)
        .await
        .expect("install OAuth race identity spec");
}

async fn load_exact_spec(db: &Arc<CoralDb>, key: &IdentitySpecKey) -> IdentitySpecRecord {
    let mut session = db.as_ref();
    session
        .identity_specs()
        .load_optional(key)
        .await
        .expect("load exact identity spec")
        .expect("exact identity spec")
}

async fn assert_spec_document_absent(db: &Arc<CoralDb>, key: &IdentitySpecKey) {
    let mut session = db.as_ref();
    assert!(
        session
            .identity_spec_documents()
            .load_optional(key)
            .await
            .expect("load absent identity spec document")
            .is_none()
    );
}

fn decrypt_spec_values(
    key: &IdentitySpecKey,
    document: &IdentitySpecDocumentRecord,
    key_provider: &dyn CredentialKeyProvider,
) -> BTreeMap<String, String> {
    let envelope = EncryptedEnvelopeDocument {
        ciphertext: document.ciphertext.clone(),
        nonce: document.nonce.clone(),
        wrapped_dek: document.wrapped_dek.clone(),
        wrapped_dek_nonce: document.wrapped_dek_nonce.clone(),
        key_id: document.key_id.clone(),
        algorithm: document.algorithm.clone(),
        aad_version: document.aad_version,
    };
    let (scope_kind, scope_id, name) = key.document_aad_parts();
    decrypt_identity_spec_document(scope_kind, scope_id, name, &envelope, key_provider)
        .expect("decrypt identity spec race inputs")
}

async fn device_dcr_oauth_provider() -> MockServer {
    let provider = device_oauth_provider().await;
    Mock::given(method("POST"))
        .and(path("/register"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"{"client_id":"registered-client","token_endpoint_auth_method":"none"}"#,
            "application/json",
        ))
        .mount(&provider)
        .await;
    provider
}

fn device_dcr_oauth_manifest(name: &str, base_url: &str) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: oauth\ndescription: oauth\nissuer: oauth_issuer\ntype: oauth\noauth:\n  method:\n    flow:\n      type: device_code\n    endpoints:\n      device_authorization_url: {base_url}/device\n      token_url: {base_url}/token\n    client:\n      dynamic_registration:\n        registration_url: {base_url}/register\n        token_endpoint_auth_method: none\n"
    )
}

async fn assert_static_provider_requests(
    provider: &MockServer,
    expected_client: &str,
    forbidden_client: Option<&str>,
) {
    let requests = provider
        .received_requests()
        .await
        .expect("recorded OAuth race requests");
    assert_request_paths(&requests, &["/device", "/token"]);
    let expected = format!("client_id={expected_client}");
    assert!(
        requests
            .iter()
            .all(|request| request_body_contains(request, &expected))
    );
    if let Some(forbidden) = forbidden_client {
        assert!(
            requests
                .iter()
                .all(|request| !request_body_contains(request, forbidden))
        );
    }
}

async fn assert_dcr_provider_requests(provider: &MockServer) {
    let requests = provider
        .received_requests()
        .await
        .expect("recorded DCR race requests");
    assert_request_paths(&requests, &["/device", "/register", "/token"]);
    assert!(
        requests
            .iter()
            .filter(|request| request.url.path() != "/register")
            .all(|request| request_body_contains(request, "client_id=registered-client"))
    );
}

fn assert_request_paths(requests: &[Request], expected: &[&str]) {
    let mut actual = requests
        .iter()
        .map(|request| request.url.path().to_string())
        .collect::<Vec<_>>();
    actual.sort();
    let mut expected = expected
        .iter()
        .map(|path| (*path).to_string())
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(actual, expected);
}

fn request_body_contains(request: &Request, expected: &str) -> bool {
    String::from_utf8_lossy(&request.body).contains(expected)
}
