use super::super::OAUTH_ACCESS_TOKEN_KEY;
use super::*;

const REFRESH_RESILIENCE_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn sqlite_oauth_refresh_resilience_contract() {
    let temp = tempdir().expect("temp dir");
    let db = Arc::new(
        CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite"),
    );
    db.migrate().await.expect("migrate sqlite");
    Box::pin(assert_oauth_refresh_resilience_contract(&db)).await;
}

pub(crate) async fn assert_oauth_refresh_resilience_contract(db: &Arc<CoralDb>) {
    Box::pin(assert_spec_input_drift_wins(db)).await;
    Box::pin(assert_unsafe_rendered_endpoint_fails_before_claim(db)).await;
    Box::pin(assert_token_and_key_rotation_survive_restart(db)).await;
    Box::pin(assert_interrupted_refresh_remains_failed_closed_after_restart(db)).await;
}

async fn assert_spec_input_drift_wins(db: &Arc<CoralDb>) {
    let fixture = create_refresh_manager_fixture(db, false).await;
    let before_pair = load_pair(db, &fixture.owner, fixture.name.as_str()).await;
    let spec_key = before_pair
        .0
        .as_ref()
        .expect("refresh identity")
        .spec_reference
        .key()
        .clone();
    let before_inputs = load_spec_document(db, &spec_key).await;
    let changed_client = "refresh-input-after-provider-canary";
    let changed_values =
        BTreeMap::from([("OAUTH_CLIENT_ID".to_string(), changed_client.to_string())]);
    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let manager = IdentityManager::new(db.clone(), fixture.keys.clone())
        .with_before_refresh_finalize_gate(reached.clone(), resume.clone());
    let owner = fixture.owner.clone();
    let name = fixture.name.as_str().to_string();

    let (result, changed_inputs) = tokio::time::timeout(REFRESH_RESILIENCE_TIMEOUT, async {
        let refresh = tokio::spawn(async move { manager.get_for_use(&owner, &name).await });
        reached.wait().await;
        let changed_inputs = put_spec_document(
            db,
            &spec_key,
            &changed_values,
            fixture.keys.as_ref(),
            before_inputs
                .updated_at_unix_nanos
                .checked_add(1)
                .expect("next input timestamp"),
        )
        .await;
        resume.wait().await;
        let result = refresh.await.expect("refresh task");
        (result, changed_inputs)
    })
    .await
    .expect("spec-input refresh race must not deadlock");

    assert_refresh_reconnect(result, &[changed_client]);
    assert_eq!(
        changed_inputs.document_version,
        before_inputs.document_version + 1
    );
    assert_eq!(load_spec_document(db, &spec_key).await, changed_inputs);
    assert_eq!(
        load_pair(db, &fixture.owner, fixture.name.as_str()).await,
        before_pair,
        "provider result must not overwrite the pre-refresh identity document"
    );
    let claim = load_use_snapshot(db, &fixture.owner, &fixture.name)
        .await
        .oauth_refresh_claim
        .expect("failed refresh retains its exact claim");
    assert!(claim.deadline_unix_nanos() <= now_unix_nanos_i64().unwrap());
    let requests = refresh_request_bodies(&fixture.provider).await;
    let [request] = requests.as_slice() else {
        panic!("spec-input drift must perform exactly one refresh request");
    };
    assert!(request.contains("client_id=spec-client-id"));
    assert!(!request.contains(changed_client));
}

async fn assert_unsafe_rendered_endpoint_fails_before_claim(db: &Arc<CoralDb>) {
    let provider = refresh_device_oauth_provider(false).await;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let spec_name = format!("refresh_endpoint_{suffix}");
    let identity_name = IdentityName::parse(&format!("refresh-endpoint-{suffix}")).unwrap();
    let principal = UserPrincipal::for_user(&format!("refresh-endpoint-{suffix}")).unwrap();
    let owner = IdentityOwner::for_user(principal.clone());
    let key = CredentialEncryptionKey::from_static_bytes_for_test([72; 32]);
    let keys = Arc::new(TestKeyProvider(vec![key]));
    let manifest = templated_token_endpoint_manifest(&spec_name, &provider.uri());
    let original_values = BTreeMap::from([
        ("OAUTH_CLIENT_ID".to_string(), "endpoint-client".to_string()),
        ("PROVIDER_ORIGIN".to_string(), provider.uri()),
    ]);
    IdentitySpecManager::new(db.clone(), keys.clone())
        .add_or_replace_exact(
            IdentitySpecScope::global(),
            &manifest,
            original_values
                .iter()
                .map(|(key, value)| IdentitySpecInputValue::new(key, value))
                .collect(),
        )
        .await
        .expect("install rendered-endpoint identity spec");
    let manager = IdentityManager::new(db.clone(), keys.clone());
    manager
        .create_or_replace_user_oauth(
            &principal,
            identity_name.as_str(),
            &spec_name,
            IdentityOAuthCommitPhase::default(),
            |_event| async { Ok(()) },
        )
        .await
        .expect("create rendered-endpoint OAuth identity");
    let before_pair = load_pair(db, &owner, identity_name.as_str()).await;
    let spec_key = IdentitySpecKey::global(&spec_name).unwrap();
    let before_inputs = load_spec_document(db, &spec_key).await;
    let unsafe_origin = "http://unsafe-endpoint-canary.example.test";
    let changed_values = BTreeMap::from([
        ("OAUTH_CLIENT_ID".to_string(), "endpoint-client".to_string()),
        ("PROVIDER_ORIGIN".to_string(), unsafe_origin.to_string()),
    ]);
    put_spec_document(
        db,
        &spec_key,
        &changed_values,
        keys.as_ref(),
        before_inputs.updated_at_unix_nanos + 1,
    )
    .await;

    let result = manager.get_for_use(&owner, identity_name.as_str()).await;
    assert_refresh_reconnect(result, &[unsafe_origin]);
    assert_eq!(
        load_pair(db, &owner, identity_name.as_str()).await,
        before_pair
    );
    assert!(
        load_use_snapshot(db, &owner, &identity_name)
            .await
            .oauth_refresh_claim
            .is_none(),
        "unsafe endpoint must fail before claim acquisition"
    );
    assert!(refresh_request_bodies(&provider).await.is_empty());
}

#[expect(clippy::used_underscore_binding, reason = "opaque revision contract")]
async fn assert_token_and_key_rotation_survive_restart(db: &Arc<CoralDb>) {
    let provider = rotating_refresh_provider().await;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let spec_name = format!("refresh_rotation_{suffix}");
    let identity_name = IdentityName::parse(&format!("refresh-rotation-{suffix}")).unwrap();
    let principal = UserPrincipal::for_user(&format!("refresh-rotation-{suffix}")).unwrap();
    let owner = IdentityOwner::for_user(principal.clone());
    let old_key = CredentialEncryptionKey::from_static_bytes_for_test([73; 32]);
    let new_key = CredentialEncryptionKey::from_static_bytes_for_test([74; 32]);
    let old_keys = Arc::new(TestKeyProvider(vec![old_key.clone()]));
    IdentitySpecManager::new(db.clone(), old_keys.clone())
        .add_or_replace_exact(
            IdentitySpecScope::global(),
            &default_client_device_manifest(&spec_name, &provider.uri()),
            Vec::new(),
        )
        .await
        .expect("install rotation identity spec");
    IdentityManager::new(db.clone(), old_keys.clone())
        .create_or_replace_user_oauth(
            &principal,
            identity_name.as_str(),
            &spec_name,
            IdentityOAuthCommitPhase::default(),
            |_event| async { Ok(()) },
        )
        .await
        .expect("create rotation OAuth identity");
    let before_document = load_pair(db, &owner, identity_name.as_str())
        .await
        .1
        .expect("initial rotation document");
    assert_eq!(before_document.key_id, old_key.key_id());

    let rotating_keys = Arc::new(TestKeyProvider(vec![old_key.clone(), new_key.clone()]));
    let first = IdentityManager::new(db.clone(), rotating_keys)
        .get_for_use(&owner, identity_name.as_str())
        .await
        .expect("refresh token zero under rotating keyring");
    assert_access_token(&first, "access-1");
    let (first_record, first_document) = load_pair(db, &owner, identity_name.as_str()).await;
    let first_record = first_record.expect("first rotated identity");
    let first_document = first_document.expect("first rotated document");
    assert_eq!(first_document.key_id, new_key.key_id());
    assert_eq!(
        first_document.document_version,
        before_document.document_version + 1
    );
    let new_keys = Arc::new(TestKeyProvider(vec![new_key.clone()]));
    assert_refresh_token(
        &first_record,
        &first_document,
        new_keys.as_ref(),
        "refresh-1",
    );

    let second = IdentityManager::new(db.clone(), new_keys.clone())
        .get_for_use(&owner, identity_name.as_str())
        .await
        .expect("refresh token one after restart");
    assert_access_token(&second, "access-2");
    let (second_record, second_document) = load_pair(db, &owner, identity_name.as_str()).await;
    let second_record = second_record.expect("second rotated identity");
    let second_document = second_document.expect("second rotated document");
    assert_eq!(second_document.key_id, new_key.key_id());
    assert_eq!(
        second_document.document_version,
        before_document.document_version + 2
    );
    assert_refresh_token(
        &second_record,
        &second_document,
        new_keys.as_ref(),
        "refresh-2",
    );
    assert!(second.revision()._snapshot.oauth_refresh_claim.is_none());

    let reopened = IdentityManager::new(db.clone(), new_keys)
        .get_for_use(&owner, identity_name.as_str())
        .await
        .expect("reopen durable refreshed identity");
    assert_access_token(&reopened, "access-2");
    let requests = refresh_request_bodies(&provider).await;
    let [first_request, second_request] = requests.as_slice() else {
        panic!("rotation must perform exactly two refresh requests");
    };
    assert!(first_request.contains("refresh_token=refresh-0"));
    assert!(second_request.contains("refresh_token=refresh-1"));

    let old_only = IdentityManager::new(db.clone(), old_keys);
    assert!(matches!(
        old_only.get_for_use(&owner, identity_name.as_str()).await,
        Err(AppError::Credentials(CredentialsError::Unavailable(_)))
    ));
    assert_eq!(refresh_request_bodies(&provider).await.len(), 2);
}

async fn assert_interrupted_refresh_remains_failed_closed_after_restart(db: &Arc<CoralDb>) {
    let fixture = create_refresh_manager_fixture(db, false).await;
    let before_pair = load_pair(db, &fixture.owner, fixture.name.as_str()).await;
    let reached = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let manager = IdentityManager::new(db.clone(), fixture.keys.clone())
        .with_before_refresh_finalize_gate(reached.clone(), resume);
    let owner = fixture.owner.clone();
    let name = fixture.name.as_str().to_string();
    let refresh = tokio::spawn(async move { manager.get_for_use(&owner, &name).await });
    tokio::time::timeout(REFRESH_RESILIENCE_TIMEOUT, reached.wait())
        .await
        .expect("refresh reaches pre-finalize gate");
    assert_eq!(refresh_request_bodies(&fixture.provider).await.len(), 1);
    refresh.abort();
    let cancelled = tokio::time::timeout(REFRESH_RESILIENCE_TIMEOUT, refresh)
        .await
        .expect("aborted refresh task exits")
        .expect_err("refresh task must be cancelled");
    assert!(cancelled.is_cancelled());
    assert_eq!(
        load_pair(db, &fixture.owner, fixture.name.as_str()).await,
        before_pair
    );
    let claimed = load_use_snapshot(db, &fixture.owner, &fixture.name).await;
    let claim = claimed
        .oauth_refresh_claim
        .as_ref()
        .expect("interrupted refresh retains its claim")
        .clone();
    assert!(claim.deadline_unix_nanos() > now_unix_nanos_i64().unwrap());

    let mut tx = db.begin_serializable().await.expect("begin claim expiry");
    assert!(
        tx.identities()
            .expire_oauth_refresh_claim(
                &fixture.owner,
                &fixture.name,
                claim.id(),
                now_unix_nanos_i64().unwrap(),
            )
            .await
            .expect("expire interrupted claim")
    );
    tx.commit().await.expect("commit claim expiry");

    let restarted = IdentityManager::new(db.clone(), fixture.keys.clone());
    let result = restarted
        .get_for_use(&fixture.owner, fixture.name.as_str())
        .await;
    assert_refresh_reconnect(result, &[]);
    assert_eq!(refresh_request_bodies(&fixture.provider).await.len(), 1);
    assert_eq!(
        load_pair(db, &fixture.owner, fixture.name.as_str()).await,
        before_pair
    );
    let after = load_use_snapshot(db, &fixture.owner, &fixture.name).await;
    let after_claim = after
        .oauth_refresh_claim
        .expect("expired interrupted claim remains owned");
    assert_eq!(after_claim.id(), claim.id());
    assert!(after_claim.deadline_unix_nanos() <= now_unix_nanos_i64().unwrap());
}

fn templated_token_endpoint_manifest(name: &str, device_base: &str) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: oauth\ndescription: oauth\nissuer: oauth_issuer\ntype: oauth\ninputs:\n  OAUTH_CLIENT_ID:\n    kind: variable\n    required: true\n  PROVIDER_ORIGIN:\n    kind: variable\n    required: true\noauth:\n  method:\n    flow:\n      type: device_code\n    endpoints:\n      device_authorization_url: {device_base}/device\n      token_url: '{{{{input.PROVIDER_ORIGIN}}}}/token'\n    client:\n      id:\n        input: OAUTH_CLIENT_ID\n"
    )
}

fn default_client_device_manifest(name: &str, base_url: &str) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: oauth\ndescription: oauth\nissuer: oauth_issuer\ntype: oauth\noauth:\n  method:\n    flow:\n      type: device_code\n    endpoints:\n      device_authorization_url: {base_url}/device\n      token_url: {base_url}/token\n    client:\n      id:\n        default: spec-client-id\n"
    )
}

async fn rotating_refresh_provider() -> MockServer {
    let provider = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(|request: &wiremock::Request| {
            if request.url.path() == "/device" {
                return ResponseTemplate::new(200).set_body_raw(
                    r#"{"device_code":"device-code","user_code":"ABCD-1234","verification_uri":"https://provider.example/device","verification_uri_complete":"https://provider.example/device?user_code=ABCD-1234","expires_in":60,"interval":1}"#,
                    "application/json",
                );
            }
            let body = String::from_utf8_lossy(&request.body);
            let response = if body.contains("refresh_token=refresh-0") {
                r#"{"access_token":"access-1","refresh_token":"refresh-1","token_type":"Bearer","scope":"repo one","expires_in":-300}"#
            } else if body.contains("refresh_token=refresh-1") {
                r#"{"access_token":"access-2","refresh_token":"refresh-2","token_type":"Bearer","scope":"repo two","expires_in":3600}"#
            } else {
                r#"{"access_token":"access-0","refresh_token":"refresh-0","token_type":"Bearer","scope":"repo zero","expires_in":-300}"#
            };
            ResponseTemplate::new(200).set_body_raw(response, "application/json")
        })
        .mount(&provider)
        .await;
    provider
}

fn assert_access_token(resolved: &ResolvedIdentityForUse, expected: &str) {
    assert_eq!(
        resolved
            .material()
            .get(OAUTH_ACCESS_TOKEN_KEY)
            .map(String::as_str),
        Some(expected)
    );
}

fn assert_refresh_token(
    record: &IdentityRecord,
    document: &IdentityDocumentRecord,
    keys: &dyn CredentialKeyProvider,
    expected: &str,
) {
    let material = decrypt_material(record, document, keys);
    let refresh_key = crate::credentials::oauth::refresh_token_material_key(OAUTH_ACCESS_TOKEN_KEY);
    assert_eq!(
        material.get(&refresh_key).map(String::as_str),
        Some(expected)
    );
}

fn assert_refresh_reconnect(
    result: Result<ResolvedIdentityForUse, AppError>,
    extra_canaries: &[&str],
) {
    let Err(AppError::FailedPrecondition(detail)) = result else {
        panic!("refresh must fail closed with reconnect guidance");
    };
    assert!(detail.contains("reconnect the identity"));
    for canary in [
        "access-token",
        "refresh-token",
        "refreshed-token",
        "access-1",
        "refresh-1",
    ]
    .into_iter()
    .chain(extra_canaries.iter().copied())
    {
        assert!(
            !detail.contains(canary),
            "refresh diagnostic leaked {canary}"
        );
    }
}

async fn refresh_request_bodies(provider: &MockServer) -> Vec<String> {
    provider
        .received_requests()
        .await
        .expect("recorded OAuth provider requests")
        .into_iter()
        .filter(|request| {
            String::from_utf8_lossy(&request.body).contains("grant_type=refresh_token")
        })
        .map(|request| String::from_utf8_lossy(&request.body).into_owned())
        .collect()
}
