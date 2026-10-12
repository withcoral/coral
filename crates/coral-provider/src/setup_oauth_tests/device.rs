use super::*;

/// A Device OpenAPI Setup after its owner submitted device-code authentication.
struct DeviceSetup {
    harness: Harness,
    context: coral_workspace::ChangeContext,
    workspace: WorkspaceId,
    setup: crate::ProviderSetupId,
    /// The revision the owner reviewed and the values submitted against it.
    reviewed_revision: i64,
    values: coral_catalog::ProviderConfiguration,
    confirmation: crate::SetupConfigurationReview,
}

/// Prepares the Device OpenAPI Setup and submits device-code authentication
/// with `client`. `edit` first changes the default read-scope contract.
async fn confirmed_device_setup(
    api: &OAuthApi,
    user: &str,
    edit: impl FnOnce(&mut AuthenticationConfiguration),
    client: Option<SetupOAuthClient>,
) -> DeviceSetup {
    let harness = harness(None).await;
    let (owner, workspace) = workspace_owner(&harness, user).await;
    let context = change_context(owner.clone(), 10, '1');
    let setup = harness
        .providers
        .prepare_setup(
            &context,
            workspace.id(),
            ProviderSetupSource::OpenApiDocument(api.document("Device")),
            None,
        )
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(&owner, workspace.id(), setup.id())
        .await
        .unwrap();
    let mut authentication = AuthenticationConfiguration::OAuthDeviceCode {
        scope_format: coral_catalog::OAuthScopeFormat::Space,
        resource: None,
        issuer: None,
        device_authorization_url: format!("{}/device", api.server.uri()),
        token_url: format!("{}/token", api.server.uri()),
        scopes: vec!["read".into()],
        client: None,
    };
    edit(&mut authentication);
    let mut values = review.document.unwrap().suggested;
    values.authentication = Some(authentication);
    let confirmation = harness
        .accounts
        .confirm_setup_configuration(
            &context,
            workspace.id(),
            setup.id(),
            review.revision,
            values.clone(),
            client,
        )
        .await
        .unwrap();
    DeviceSetup {
        harness,
        context,
        workspace: workspace.id().clone(),
        setup: setup.id().clone(),
        reviewed_revision: review.revision,
        values,
        confirmation,
    }
}

/// Polls the Setup's authentication until `until` accepts it. `until` may
/// also assert on every intermediate state.
async fn poll_setup(
    harness: &Harness,
    context: &coral_workspace::ChangeContext,
    workspace: &WorkspaceId,
    setup: &crate::ProviderSetupId,
    until: impl Fn(&crate::account::SetupAuthentication) -> bool,
) -> crate::account::SetupAuthentication {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let progress = harness
                .providers
                .setup_authentication(
                    context.actor(),
                    workspace,
                    setup,
                    context.occurred_at_unix_nanos(),
                )
                .await
                .unwrap();
            if until(&progress) {
                return progress;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("device authorization reaches the awaited state without browser activity")
}

/// The device authorization has left its pending and exchanging states.
fn settled(progress: &crate::account::SetupAuthentication) -> bool {
    !matches!(
        progress.authorization.as_ref().unwrap().state,
        SetupAuthorizationState::Pending | SetupAuthorizationState::Exchanging
    )
}

#[tokio::test]
async fn device_publication_requires_a_client_even_when_everyone_connects_later() {
    let api = OAuthApi::start().await;
    let device = confirmed_device_setup(
        &api,
        "user:device-no-client",
        |authentication| {
            if let AuthenticationConfiguration::OAuthDeviceCode { scopes, .. } = authentication {
                scopes.clear();
            }
        },
        None,
    )
    .await;
    assert!(device.confirmation.errors.is_empty());
    let result = device
        .harness
        .providers
        .publish_workspace_provider(
            &device.context,
            publication_request(
                &device.workspace,
                &device.setup,
                "device",
                CredentialRoute::Caller,
                "d3210000-0000-4000-8000-000000000099",
            ),
        )
        .await;
    assert!(
        matches!(result, Err(Error::OAuthClientConfiguration)),
        "{result:?}"
    );
}

#[tokio::test]
async fn device_configuration_reports_all_invalid_fields_together() {
    let api = OAuthApi::start().await;
    let rejected = confirmed_device_setup(
        &api,
        "user:device-errors",
        |authentication| {
            *authentication = AuthenticationConfiguration::OAuthDeviceCode {
                scope_format: coral_catalog::OAuthScopeFormat::Space,
                resource: None,
                issuer: None,
                device_authorization_url: "invalid device URL".into(),
                token_url: "invalid token URL".into(),
                scopes: vec!["invalid scope".into()],
                client: Some(coral_catalog::OAuthClientPolicy::public(
                    OAuthClientBootstrap::Manual,
                    "client".into(),
                )),
            };
        },
        None,
    )
    .await
    .confirmation;
    let fields: std::collections::BTreeSet<_> = rejected
        .errors
        .iter()
        .map(|error| error.field.as_str())
        .collect();
    assert_eq!(
        fields,
        std::collections::BTreeSet::from([
            "authentication.device_authorization_url",
            "authentication.token_url",
            "authentication.scopes",
            "application",
        ])
    );
    assert!(!rejected.configuration_confirmed);
}

#[tokio::test]
async fn device_configuration_replay_ignores_inapplicable_pkce() {
    let api = OAuthApi::start().await;
    let application = || {
        SetupOAuthClient::application(
            "https://reef.example/callback".into(),
            crate::OAuthClientConfiguration::new(
                "manual-client".into(),
                crate::OAuthClientAuthentication::None,
            ),
        )
        .unwrap()
    };
    let device =
        confirmed_device_setup(&api, "user:device-replay", |_| {}, Some(application())).await;
    let confirmed = &device.confirmation;
    assert!(confirmed.errors.is_empty());
    let replay = device
        .harness
        .accounts
        .confirm_setup_configuration(
            &device.context,
            &device.workspace,
            &device.setup,
            device.reviewed_revision,
            device.values.clone(),
            Some(application()),
        )
        .await
        .expect("exact replay accepts the original application settings");
    assert_eq!(replay.revision, confirmed.revision);
    assert_eq!(replay.confirmed, confirmed.confirmed);
}

/// A Provider published from a public-client device-code Setup, with the
/// sealed grant. The device and token endpoints answer `exchanges` times.
async fn published_device_provider(
    api: &OAuthApi,
    user: &str,
    exchanges: u64,
) -> (
    DeviceSetup,
    crate::Provider,
    crate::account::SetupAuthentication,
) {
    let device = confirmed_device_setup(api, user, |_| {}, Some(explicit("manual-client"))).await;
    assert!(
        device.confirmation.errors.is_empty(),
        "{:?}",
        device.confirmation.errors
    );
    Mock::given(method("POST"))
        .and(path("/device"))
        .and(wiremock::matchers::body_string_contains(
            "client_id=manual-client",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code": "private-device-canary", "user_code": "ABCD-EFGH",
            "verification_uri": "http://device.fixture.test/activate",
            "expires_in": 60, "interval": 1
        })))
        .expect(exchanges)
        .mount(&api.server)
        .await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(wiremock::matchers::body_string_contains(
            "device_code=private-device-canary",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": ACCESS, "token_type": "Bearer", "scope": "read", "expires_at": "2026-09-23T12:00:00Z"
        })))
        .with_priority(1)
        .expect(exchanges)
        .mount(&api.server)
        .await;
    let DeviceSetup {
        harness,
        context,
        workspace,
        setup,
        confirmation,
        ..
    } = &device;
    harness
        .accounts
        .start_setup_authorization(
            context,
            workspace,
            setup,
            confirmation.revision,
            SetupOAuthClient::new("https://reef.example/callback".into(), None).unwrap(),
        )
        .await
        .expect("starts server-owned device authorization");
    let completed = poll_setup(harness, context, workspace, setup, |progress| {
        assert!(
            progress
                .authorization
                .as_ref()
                .unwrap()
                .failure_code
                .is_none()
        );
        progress.authorized
    })
    .await;
    let publication = harness
        .providers
        .publish_workspace_provider(
            context,
            publication_request(
                workspace,
                setup,
                "device",
                CredentialRoute::Fixed,
                "d3210000-0000-4000-8000-000000000001",
            )
            .with_account_grant(completed.provisional_grant_id.unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    let provider = publication.published_provider().unwrap().clone();
    (device, provider, completed)
}

#[tokio::test]
async fn device_code_configuration_accepts_a_public_application_without_pkce() {
    let api = OAuthApi::start().await;
    let (device, provider, completed) =
        published_device_provider(&api, "user:device-configuration", 1).await;
    let authentication = device
        .confirmation
        .confirmed
        .unwrap()
        .authentication
        .unwrap();
    assert_eq!(authentication.kind().name(), "oauth_device_code");
    let client = authentication.oauth_client().unwrap();
    assert_eq!(client.client_id, "manual-client");
    assert_eq!(client.pkce, coral_catalog::OAuthPkcePolicy::Disabled);
    assert!(client.authorization_parameters.is_empty());
    assert_eq!(completed.granted_scope, "read");
    let credentials = credential_for(
        &device.harness,
        &device.workspace,
        provider.id(),
        "device",
        "getData",
        &user_principal(device.context.actor()),
        2_000_000_000,
    )
    .await
    .unwrap();
    assert_eq!(credentials.get("token").unwrap().expose(), ACCESS);
}

#[tokio::test]
async fn device_reconnect_replays_the_same_authorization_across_a_restart() {
    let api = OAuthApi::start().await;
    let (device, provider, completed) =
        published_device_provider(&api, "user:device-reconnect", 2).await;
    let DeviceSetup {
        harness,
        context,
        workspace,
        ..
    } = &device;
    let account = completed.provisional_grant_id.unwrap();
    let request = crate::account_tests::authorization_request();
    let pending = harness
        .accounts
        .start_reconnect(
            context,
            workspace,
            provider.id(),
            account,
            request.clone(),
            None,
        )
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let progress = harness
                .accounts
                .get_authorization(context, workspace, pending.id)
                .await
                .unwrap();
            if progress
                .user_code
                .as_deref()
                .is_some_and(|code| !code.is_empty())
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    harness.accounts.stop_device_authorizations().await.unwrap();
    let restarted = harness.restarted_accounts();
    restarted
        .recover_device_authorizations(context.occurred_at_unix_nanos())
        .await
        .unwrap();
    let replay = restarted
        .start_reconnect(context, workspace, provider.id(), account, request, None)
        .await
        .unwrap();
    assert_eq!(pending.id, replay.id);
    let connected = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let progress = harness
                .accounts
                .get_authorization(context, workspace, pending.id)
                .await
                .unwrap();
            if progress.state == crate::account::AuthorizationState::Succeeded {
                break progress;
            }
            assert!(progress.failure_code().is_none());
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("reconnection polls without a callback");
    assert_eq!(connected.account_id, Some(account));
}

async fn pending_device_setup(
    api: &OAuthApi,
    authentication: crate::OAuthClientAuthentication,
) -> (
    Harness,
    coral_workspace::ChangeContext,
    WorkspaceId,
    crate::ProviderSetupId,
) {
    let device = confirmed_device_setup(
        api,
        "user:device-poll",
        |_| {},
        Some(
            SetupOAuthClient::application(
                "https://reef.example/callback".into(),
                crate::OAuthClientConfiguration::new("manual-client".into(), authentication),
            )
            .unwrap(),
        ),
    )
    .await;
    assert!(device.confirmation.errors.is_empty());
    device
        .harness
        .accounts
        .start_setup_authorization(
            &device.context,
            &device.workspace,
            &device.setup,
            device.confirmation.revision,
            SetupOAuthClient::new("https://reef.example/callback".into(), None).unwrap(),
        )
        .await
        .unwrap();
    (
        device.harness,
        device.context,
        device.workspace,
        device.setup,
    )
}

#[tokio::test]
async fn device_confidential_client_polls_pending_and_slows_down_without_exposing_secrets() {
    let api = OAuthApi::start().await;
    Mock::given(method("POST")).and(path("/device"))
        .and(wiremock::matchers::body_string_contains("client_secret=device-secret-canary"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code": "device-private-canary", "user_code": "ABCD-EFGH",
            "verification_uri": format!("{}/activate", api.server.uri()), "expires_in": 60, "interval": 1,
        }))).expect(1).mount(&api.server).await;
    let polls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = polls.clone();
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(wiremock::matchers::body_string_contains(
            "client_secret=device-secret-canary",
        ))
        .respond_with(move |request: &Request| {
            let body = String::from_utf8_lossy(&request.body);
            assert!(!body.contains("code_verifier"));
            assert!(!body.contains("redirect_uri"));
            let mut polls = seen.lock().unwrap();
            polls.push(std::time::Instant::now());
            ResponseTemplate::new(200).set_body_json(match polls.len() {
                1 => json!({"error": "authorization_pending"}),
                2 => json!({"error": "slow_down"}),
                _ => json!({"access_token": ACCESS, "token_type": "Bearer", "scope": "read"}),
            })
        })
        .with_priority(1)
        .expect(3)
        .mount(&api.server)
        .await;
    let (harness, context, workspace, setup) = pending_device_setup(
        &api,
        crate::OAuthClientAuthentication::ClientSecretPost {
            secret: Some(Secret::new("device-secret-canary".into()).unwrap()),
        },
    )
    .await;
    poll_setup(&harness, &context, &workspace, &setup, |progress| {
        let debug = format!("{progress:?}");
        for secret in ["device-secret-canary", "device-private-canary", ACCESS] {
            assert!(!debug.contains(secret));
        }
        progress.authorized
    })
    .await;
    let polls = polls.lock().unwrap();
    assert!(polls[1].duration_since(polls[0]) >= std::time::Duration::from_secs(1));
    assert!(polls[2].duration_since(polls[1]) >= std::time::Duration::from_secs(6));
}

#[tokio::test]
async fn canceling_device_setup_discards_a_late_token() {
    let api = OAuthApi::start().await;
    Mock::given(method("POST")).and(path("/device"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code": "device-private-canary", "user_code": "ABCD-EFGH",
            "verification_uri": format!("{}/activate", api.server.uri()), "expires_in": 60, "interval": 1,
        }))).expect(1).mount(&api.server).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(move |_: &Request| {
            signal.notify_one();
            ResponseTemplate::new(200)
                .set_body_json(json!({"access_token": ACCESS, "token_type": "Bearer"}))
                .set_delay(std::time::Duration::from_millis(200))
        })
        .with_priority(1)
        .expect(1)
        .mount(&api.server)
        .await;
    let (harness, context, workspace, setup) =
        pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    harness
        .accounts
        .cancel_setup(&context, &workspace, &setup)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    let progress = harness
        .providers
        .setup_authentication(context.actor(), &workspace, &setup, 10)
        .await
        .unwrap();
    assert!(!progress.authorized);
    assert_eq!(
        progress.authorization.unwrap().state,
        SetupAuthorizationState::Canceled
    );
}

#[tokio::test]
async fn device_start_reports_a_rejected_application_without_exposing_upstream_details() {
    let api = OAuthApi::start().await;
    Mock::given(method("POST"))
        .and(path("/device"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "error": "invalid_client", "error_description": "private-application-canary"
        })))
        .expect(1)
        .mount(&api.server)
        .await;
    let (harness, context, workspace, setup) =
        pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
    let failed = poll_setup(&harness, &context, &workspace, &setup, settled).await;
    assert_eq!(
        failed.authorization.as_ref().unwrap().state,
        SetupAuthorizationState::Failed
    );
    assert_eq!(
        failed
            .authorization
            .as_ref()
            .unwrap()
            .failure_code
            .as_deref(),
        Some("oauth_client_configuration")
    );
    assert!(!format!("{failed:?}").contains("private-application-canary"));
}

#[tokio::test]
async fn denied_device_authorization_fails_without_exposing_upstream_details() {
    let api = OAuthApi::start().await;
    Mock::given(method("POST"))
        .and(path("/device"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code": "private-device-canary", "user_code": "ABCD-EFGH",
            "verification_uri": format!("{}/activate", api.server.uri()),
            "expires_in": 60, "interval": 1
        })))
        .expect(1)
        .mount(&api.server)
        .await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "error": "access_denied", "error_description": "private-denial-canary"
        })))
        .with_priority(1)
        .expect(1)
        .mount(&api.server)
        .await;
    let (harness, context, workspace, setup) =
        pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
    let progress = poll_setup(&harness, &context, &workspace, &setup, settled).await;
    assert!(!progress.authorized);
    let authorization = progress.authorization.as_ref().unwrap();
    assert_eq!(authorization.state, SetupAuthorizationState::Failed);
    assert_eq!(
        authorization.failure_code.as_deref(),
        Some("oauth_access_denied")
    );
    assert!(authorization.user_code.is_none());
    let safe = format!("{progress:?}");
    assert!(!safe.contains("private-device-canary"));
    assert!(!safe.contains("private-denial-canary"));
}

#[tokio::test]
async fn device_authorization_expires_locally_and_when_the_service_rejects_the_code() {
    for upstream_expiry in [false, true] {
        let api = OAuthApi::start().await;
        Mock::given(method("POST"))
            .and(path("/device"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "device_code": "private-device-canary", "user_code": "ABCD-EFGH",
                "verification_uri": format!("{}/activate", api.server.uri()),
                "expires_in": if upstream_expiry { 60 } else { 1 },
                "interval": if upstream_expiry { 1 } else { 2 }
            })))
            .expect(1)
            .mount(&api.server)
            .await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "error": "expired_token"
            })))
            .with_priority(1)
            .expect(u64::from(upstream_expiry))
            .mount(&api.server)
            .await;
        let (harness, context, workspace, setup) =
            pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
        let progress = poll_setup(&harness, &context, &workspace, &setup, settled).await;
        assert!(!progress.authorized);
        let authorization = progress.authorization.unwrap();
        assert_eq!(authorization.state, SetupAuthorizationState::Expired);
        assert!(authorization.user_code.is_none());
        // Only a service rejection carries a failure code; local expiry has none.
        assert_eq!(
            authorization.failure_code.as_deref(),
            upstream_expiry.then_some("oauth_device_expired"),
            "upstream expiry: {upstream_expiry}"
        );
    }
}

#[tokio::test]
async fn malformed_device_instructions_fail_before_any_token_request() {
    for (field, invalid) in [
        ("device_code", json!("")),
        ("user_code", json!("ABCD\nEFGH")),
        ("verification_uri", json!("javascript:private-uri-canary")),
        ("expires_in", json!(0)),
        ("expires_in", json!(u64::MAX)),
        ("interval", json!(0)),
        ("interval", json!(3601)),
    ] {
        let api = OAuthApi::start().await;
        let mut response = json!({
            "device_code": "private-device-canary", "user_code": "ABCD-EFGH",
            "verification_uri": format!("{}/activate", api.server.uri()),
            "expires_in": 60, "interval": 1
        });
        response[field] = invalid;
        Mock::given(method("POST"))
            .and(path("/device"))
            .respond_with(ResponseTemplate::new(200).set_body_json(response))
            .expect(1)
            .mount(&api.server)
            .await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "access_token": ACCESS, "token_type": "Bearer"
            })))
            .with_priority(1)
            .expect(0)
            .mount(&api.server)
            .await;
        let (harness, context, workspace, setup) =
            pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
        let progress = poll_setup(&harness, &context, &workspace, &setup, settled).await;
        assert!(!progress.authorized);
        let authorization = progress.authorization.as_ref().unwrap();
        assert_eq!(
            authorization.state,
            SetupAuthorizationState::Failed,
            "{field}"
        );
        assert_eq!(
            authorization.failure_code.as_deref(),
            Some("oauth_token_response_invalid"),
            "{field}"
        );
        assert!(authorization.user_code.is_none());
        let safe = format!("{progress:?}");
        assert!(!safe.contains("private-device-canary"));
        assert!(!safe.contains("private-uri-canary"));
    }
}

#[tokio::test]
async fn device_recovery_keeps_the_code_and_slow_down_schedule_with_one_poller() {
    let api = OAuthApi::start().await;
    Mock::given(method("POST"))
        .and(path("/device"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code": "recover-code", "user_code": "KEEP-CODE",
            "verification_uri": "http://device.fixture.test/activate",
            "expires_in": 120, "interval": 1
        })))
        .expect(1)
        .mount(&api.server)
        .await;
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = polls.clone();
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(move |_request: &Request| {
            if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(400).set_body_json(json!({"error":"slow_down"}))
            } else {
                ResponseTemplate::new(200).set_body_json(
                    json!({"access_token": ACCESS, "token_type":"Bearer", "scope":"read"}),
                )
            }
        })
        .with_priority(1)
        .mount(&api.server)
        .await;
    let (h, context, owner, setup) =
        pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        while polls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    // Allow the received slow_down response to settle before stopping the process's workers.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    h.accounts.stop_device_authorizations().await.unwrap();
    let restarted = h.restarted_accounts();
    let competitor = h.restarted_accounts();
    let now = 2_000_000_010;
    let (first, second) = tokio::join!(
        restarted.recover_device_authorizations(now),
        competitor.recover_device_authorizations(now)
    );
    first.unwrap();
    second.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "recovery must retain slow_down rather than restart the interval"
    );
    let progress = h
        .providers
        .setup_authentication(context.actor(), &owner, &setup, now)
        .await
        .unwrap();
    assert_eq!(
        progress.authorization.unwrap().user_code.as_deref(),
        Some("KEEP-CODE")
    );
    poll_setup(&h, &context, &owner, &setup, |progress| progress.authorized).await;
    assert_eq!(
        polls.load(Ordering::SeqCst),
        2,
        "only one worker exchanges the retained device code"
    );
    restarted.stop_device_authorizations().await.unwrap();
    competitor.stop_device_authorizations().await.unwrap();
}

#[tokio::test]
async fn recovery_does_not_repeat_an_interrupted_token_request_immediately() {
    let api = OAuthApi::start().await;
    Mock::given(method("POST"))
        .and(path("/device"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code":"interrupted-code", "user_code":"WAIT-CODE",
            "verification_uri":"http://device.fixture.test/activate", "expires_in":120, "interval":1
        })))
        .expect(1)
        .mount(&api.server)
        .await;
    let polls = Arc::new(AtomicUsize::new(0));
    let seen = polls.clone();
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(move |_request: &Request| {
            seen.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(400)
                .set_body_json(json!({"error":"authorization_pending"}))
                .set_delay(std::time::Duration::from_secs(25))
        })
        .with_priority(1)
        .mount(&api.server)
        .await;
    let (h, _, _, _) = pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        while polls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    h.accounts.stop_device_authorizations().await.unwrap();
    let restarted = h.restarted_accounts();
    restarted
        .recover_device_authorizations(2_000_000_010)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    restarted.stop_device_authorizations().await.unwrap();
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "recovery must honor the in-flight request's cooldown"
    );

    // The request persisted a 36-second cooldown before it left Coral. A
    // worker recovering after it polls again at once.
    let later = h.restarted_accounts();
    later
        .recover_device_authorizations(50_000_000_000)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        while polls.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("recovery polls once the cooldown has passed");
    later.stop_device_authorizations().await.unwrap();
}

#[tokio::test]
async fn recovery_never_completes_for_an_initiator_who_lost_organization_access() {
    let api = OAuthApi::start().await;
    Mock::given(method("POST"))
        .and(path("/device"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code": "disabled-code", "user_code": "GONE-USER",
            "verification_uri": "http://device.fixture.test/activate",
            "expires_in": 120, "interval": 50
        })))
        .expect(1)
        .mount(&api.server)
        .await;
    let polls = Arc::new(AtomicUsize::new(0));
    let seen = polls.clone();
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(move |_request: &Request| {
            seen.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(
                json!({"access_token": ACCESS, "token_type": "Bearer", "scope": "read"}),
            )
        })
        .with_priority(1)
        .mount(&api.server)
        .await;
    let (h, context, owner, setup) =
        pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
    poll_setup(&h, &context, &owner, &setup, |progress| {
        progress
            .authorization
            .as_ref()
            .unwrap()
            .user_code
            .as_deref()
            == Some("GONE-USER")
    })
    .await;
    h.accounts.stop_device_authorizations().await.unwrap();
    let administrator = organization_user(
        h.database.clone(),
        "user:device-administrator",
        OrganizationRole::Owner,
    )
    .await;
    let organization = coral_workspace::OrganizationModule::new(h.database.clone());
    let disabled = organization
        .set_enabled(
            &change_context(administrator.clone(), 20, '2'),
            context.actor().user_id(),
            1,
            false,
        )
        .await
        .unwrap();

    // Past the device interval, a recovered worker would poll at once. It
    // runs as a User without Organization membership and must not.
    let restarted = h.restarted_accounts();
    restarted
        .recover_device_authorizations(60_000_000_000)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    restarted.stop_device_authorizations().await.unwrap();
    assert_eq!(
        polls.load(Ordering::SeqCst),
        0,
        "no token request leaves Coral"
    );
    organization
        .set_enabled(
            &change_context(administrator, 30, '3'),
            context.actor().user_id(),
            disabled.revision(),
            true,
        )
        .await
        .unwrap();
    let progress = h
        .providers
        .setup_authentication(context.actor(), &owner, &setup, 60_000_000_000)
        .await
        .unwrap();
    assert!(!progress.authorized, "no credential is sealed");
    assert_eq!(
        progress.authorization.unwrap().state,
        SetupAuthorizationState::Pending
    );

    // Control: with access restored, the same recovery completes the flow.
    let recovered = h.restarted_accounts();
    recovered
        .recover_device_authorizations(110_000_000_000)
        .await
        .unwrap();
    poll_setup(&h, &context, &owner, &setup, |progress| progress.authorized).await;
    recovered.stop_device_authorizations().await.unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn recovery_waits_for_an_abandoned_lease_and_keeps_the_original_deadline() {
    let api = OAuthApi::start().await;
    Mock::given(method("POST")).and(path("/device"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code":"crash-code", "user_code":"SAME-CODE",
            "verification_uri":"http://device.fixture.test/activate", "expires_in":120, "interval":50
        }))).expect(1).mount(&api.server).await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(
                json!({"access_token": ACCESS, "token_type":"Bearer", "scope":"read"}),
            ),
        )
        .with_priority(1)
        .expect(1)
        .mount(&api.server)
        .await;
    let (h, context, owner, setup) =
        pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
    let before = poll_setup(&h, &context, &owner, &setup, |progress| {
        progress
            .authorization
            .as_ref()
            .unwrap()
            .user_code
            .as_deref()
            == Some("SAME-CODE")
    })
    .await;
    let deadline = before.authorization.unwrap().expires_at_unix_nanos;
    h.accounts.abort_device_authorizations();
    let restarted = h.restarted_accounts();
    restarted
        .recover_device_authorizations(20_000_000_010)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    restarted
        .recover_device_authorizations(46_000_000_010)
        .await
        .unwrap();
    // The reclaimed lease keeps the device grant's original deadline.
    poll_setup(&h, &context, &owner, &setup, |progress| {
        assert_eq!(
            progress
                .authorization
                .as_ref()
                .unwrap()
                .expires_at_unix_nanos,
            deadline
        );
        progress.authorized
    })
    .await;
    restarted.stop_device_authorizations().await.unwrap();
}

/// An MCP Setup whose authorization server metadata offers the device-code
/// flow only when `advertised`, and its first review.
struct McpDeviceSetup {
    harness: Harness,
    context: coral_workspace::ChangeContext,
    workspace: WorkspaceId,
    setup: crate::ProviderSetupId,
    review: crate::SetupConfigurationReview,
}

async fn mcp_device_setup(upstream: &OAuthMcp, user: &str, advertised: bool) -> McpDeviceSetup {
    Mock::given(method("GET"))
        .and(path("/.well-known/oauth-authorization-server"))
        .respond_with(ResponseTemplate::new(200).set_body_json({
            let mut metadata = json!({
                "issuer": upstream.server.uri(),
                "authorization_endpoint": format!("{}/authorize", upstream.server.uri()),
                "token_endpoint": format!("{}/token", upstream.server.uri()),
                "code_challenge_methods_supported": ["S256"]
            });
            if advertised {
                metadata["device_authorization_endpoint"] =
                    json!(format!("{}/device", upstream.server.uri()));
            }
            metadata
        }))
        .with_priority(1)
        .mount(&upstream.server)
        .await;
    let harness = harness(None).await;
    let (owner, workspace) = workspace_owner(&harness, user).await;
    let context = change_context(owner.clone(), 10, '1');
    let setup = harness
        .providers
        .prepare_setup(
            &context,
            workspace.id(),
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(&owner, workspace.id(), setup.id())
        .await
        .unwrap();
    McpDeviceSetup {
        harness,
        context,
        workspace: workspace.id().clone(),
        setup: setup.id().clone(),
        review,
    }
}

/// Device-code values that name `upstream`'s endpoints.
fn mcp_device_values(
    upstream: &OAuthMcp,
    review: &crate::SetupConfigurationReview,
) -> coral_catalog::ProviderConfiguration {
    let mut values = review.document.clone().unwrap().suggested;
    values.authentication = Some(AuthenticationConfiguration::OAuthDeviceCode {
        scope_format: coral_catalog::OAuthScopeFormat::Space,
        resource: None,
        issuer: None,
        device_authorization_url: format!("{}/device", upstream.server.uri()),
        token_url: format!("{}/token", upstream.server.uri()),
        scopes: vec!["read".into(), "write".into()],
        client: None,
    });
    values
}

#[tokio::test]
async fn mcp_offers_device_code_only_when_discovered() {
    for advertised in [false, true] {
        let upstream = OAuthMcp::start().await;
        let McpDeviceSetup {
            harness,
            context,
            workspace,
            setup,
            review,
        } = mcp_device_setup(&upstream, "user:mcp-device-offer", advertised).await;
        assert_eq!(
            review
                .authentication_methods
                .contains(&coral_catalog::AuthenticationKind::OAuthDeviceCode),
            advertised
        );
        let confirmed = harness
            .accounts
            .confirm_setup_configuration(
                &context,
                &workspace,
                &setup,
                review.revision,
                mcp_device_values(&upstream, &review),
                Some(explicit("manual-client")),
            )
            .await
            .unwrap();
        assert_eq!(
            confirmed.errors.is_empty(),
            advertised,
            "{:?}",
            confirmed.errors
        );
        if advertised {
            let authentication = confirmed.confirmed.unwrap().authentication.unwrap();
            assert_eq!(
                authentication.kind(),
                coral_catalog::AuthenticationKind::OAuthDeviceCode
            );
            assert_eq!(
                authentication.oauth_client().unwrap().pkce,
                coral_catalog::OAuthPkcePolicy::Disabled
            );
        }
    }
}

#[tokio::test]
async fn mcp_device_code_pins_the_discovered_endpoints() {
    let upstream = OAuthMcp::start().await;
    let McpDeviceSetup {
        harness,
        context,
        workspace,
        setup,
        review,
    } = mcp_device_setup(&upstream, "user:mcp-device-pinned", true).await;
    let mut forged = mcp_device_values(&upstream, &review);
    if let Some(AuthenticationConfiguration::OAuthDeviceCode {
        device_authorization_url,
        ..
    }) = &mut forged.authentication
    {
        *device_authorization_url = "https://other.example/device".into();
    }
    let rejected = harness
        .accounts
        .confirm_setup_configuration(
            &context,
            &workspace,
            &setup,
            review.revision,
            forged,
            Some(explicit("manual-client")),
        )
        .await
        .unwrap();
    assert_eq!(
        rejected
            .errors
            .iter()
            .map(|error| (error.field.as_str(), error.message.as_str()))
            .collect::<Vec<_>>(),
        [(
            "authentication",
            "Choose a supported method. MCP OAuth endpoints are read-only."
        )],
        "an owner cannot override discovered endpoints"
    );
    assert!(!rejected.configuration_confirmed);
}

#[tokio::test]
async fn mcp_device_authorization_sends_the_resource_to_device_and_token_endpoints() {
    let upstream = OAuthMcp::start().await;
    let McpDeviceSetup {
        harness,
        context,
        workspace,
        setup,
        review,
    } = mcp_device_setup(&upstream, "user:mcp-device-resource", true).await;
    let confirmed = harness
        .accounts
        .confirm_setup_configuration(
            &context,
            &workspace,
            &setup,
            review.revision,
            mcp_device_values(&upstream, &review),
            Some(explicit("manual-client")),
        )
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty(), "{:?}", confirmed.errors);
    let resource = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("resource", &upstream.resource())
        .finish();
    Mock::given(method("POST")).and(path("/device"))
        .and(wiremock::matchers::body_string_contains(resource.clone()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code": "mcp-device-code", "user_code": "ABCD-EFGH",
            "verification_uri": "http://device.fixture.test/activate", "expires_in": 60, "interval": 1
        }))).expect(1).mount(&upstream.server).await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(wiremock::matchers::body_string_contains(
            "device_code=mcp-device-code",
        ))
        .and(wiremock::matchers::body_string_contains(resource))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": ACCESS, "token_type": "Bearer", "scope": "read write"
        })))
        .with_priority(1)
        .expect(1)
        .mount(&upstream.server)
        .await;
    harness
        .accounts
        .start_setup_authorization(
            &context,
            &workspace,
            &setup,
            confirmed.revision,
            SetupOAuthClient::new("https://reef.example/callback".into(), None).unwrap(),
        )
        .await
        .unwrap();
    let completed = poll_setup(&harness, &context, &workspace, &setup, |progress| {
        assert!(
            progress
                .authorization
                .as_ref()
                .unwrap()
                .failure_code
                .is_none(),
            "{:?}",
            progress.authorization
        );
        progress.authorized
    })
    .await;
    assert_eq!(completed.granted_scope, "read write");
}

#[tokio::test]
async fn device_authorization_sends_comma_scopes_as_independent_requested_tokens() {
    let api = OAuthApi::start().await;
    Mock::given(method("POST")).and(path("/device"))
        .and(wiremock::matchers::body_string_contains("scope=read%2Cwrite"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code": "device-private-canary", "user_code": "ABCD-EFGH",
            "verification_uri": format!("{}/activate", api.server.uri()), "expires_in": 60, "interval": 60
        }))).expect(1).mount(&api.server).await;
    let device = confirmed_device_setup(
        &api,
        "user:device-comma",
        |authentication| {
            if let AuthenticationConfiguration::OAuthDeviceCode {
                scope_format,
                scopes,
                ..
            } = authentication
            {
                *scope_format = coral_catalog::OAuthScopeFormat::Comma;
                *scopes = vec!["write".into(), "read".into()];
            }
        },
        Some(explicit("manual-client")),
    )
    .await;
    let confirmed = &device.confirmation;
    assert!(confirmed.errors.is_empty(), "{:?}", confirmed.errors);
    let started = device
        .harness
        .accounts
        .start_setup_authorization(
            &device.context,
            &device.workspace,
            &device.setup,
            confirmed.revision,
            explicit("manual-client"),
        )
        .await
        .unwrap();
    assert_eq!(
        started.authorization.unwrap().requested_scopes,
        ["read", "write"]
    );
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if !api.server.received_requests().await.unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    api.server.verify().await;
}
