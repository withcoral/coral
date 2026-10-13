//! Provider removal retires Update preparation without widening event visibility.
use super::*;
use crate::{OAuthClientAuthentication, OAuthClientConfiguration};

fn confidential_client() -> SetupOAuthClient {
    SetupOAuthClient::application(
        "https://reef.example/callback".into(),
        OAuthClientConfiguration::new(
            "manual-client".into(),
            OAuthClientAuthentication::ClientSecretPost {
                secret: Some(Secret::new("application-secret-canary".into()).unwrap()),
            },
        ),
    )
    .unwrap()
}

async fn deletion_target(
    h: &Harness,
    context: &coral_workspace::ChangeContext,
    owner: &WorkspaceId,
) -> crate::Provider {
    crate::tests::publish_provider(
        &h.providers,
        context,
        owner,
        crate::account_tests::CREDENTIAL_REQUIRED_OPENAPI,
        "deleted",
        CredentialRoute::Caller,
    )
    .await
}

#[tokio::test]
async fn provider_deletion_cancels_ready_updates_and_rejects_late_secrets() {
    let h = harness(None).await;
    let (actor, workspace) = workspace_owner(&h, "update-deletion-owner").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id();
    let document = || {
        crate::OpenApiDocument::parse(crate::account_tests::CREDENTIAL_REQUIRED_OPENAPI).unwrap()
    };
    let provider = deletion_target(&h, &context, owner).await;
    let update = h
        .providers
        .prepare_update_fixture(&context, owner, provider.id(), document())
        .await
        .unwrap();
    let unrelated = h
        .providers
        .prepare_openapi_fixture(&context, owner, document())
        .await
        .unwrap();
    let revision = h
        .providers
        .review_setup_configuration(&actor, owner, update.id())
        .await
        .unwrap()
        .revision;
    let grant = h
        .accounts
        .authorize_setup(
            &context,
            owner,
            update.id(),
            revision,
            coral_credentials::CredentialValues::token(
                Secret::new("temporary-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    h.providers
        .delete_provider(&context, owner, provider.id())
        .await
        .unwrap();
    assert_eq!(
        h.providers
            .get_setup(&actor, owner, update.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Canceled
    );
    let authentication = h
        .providers
        .setup_authentication(&actor, owner, update.id(), 11)
        .await
        .unwrap();
    assert!(!authentication.authorized);
    // Safe grant attribution survives; its credential no longer does.
    assert_eq!(
        authentication.provisional_grant_id,
        grant.provisional_grant_id
    );
    assert!(matches!(
        h.accounts
            .authorize_setup(
                &context,
                owner,
                update.id(),
                revision,
                coral_credentials::CredentialValues::token(
                    Secret::new("late-canary".into()).unwrap()
                ),
            )
            .await,
        Err(Error::ProviderSetupNotReady)
    ));
    assert_eq!(
        h.providers
            .get_setup(&actor, owner, unrelated.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Ready
    );
    let events = h
        .providers
        .list_lifecycle_events(&actor, owner)
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.action() == LifecycleAction::SetupCanceled)
            .map(crate::AdministrativeLifecycleEvent::target_id)
            .collect::<Vec<_>>(),
        vec![update.id().to_string()]
    );
}

#[tokio::test]
async fn provider_deletion_scrubs_update_application_secrets_before_consent() {
    let api = OAuthApi::start().await;
    let h = harness(None).await;
    let (actor, workspace) = workspace_owner(&h, "application-deletion-owner").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id();
    let provider = deletion_target(&h, &context, owner).await;
    let update = h
        .providers
        .prepare_update_fixture(&context, owner, provider.id(), api.document("Update"))
        .await
        .unwrap();
    let review = h
        .providers
        .review_setup_configuration(&actor, owner, update.id())
        .await
        .unwrap();
    h.accounts
        .confirm_setup_configuration(
            &context,
            owner,
            update.id(),
            review.revision,
            review.confirmed.unwrap(),
            Some(confidential_client()),
        )
        .await
        .unwrap();
    let before = h
        .providers
        .setup_authentication(&actor, owner, update.id(), 11)
        .await
        .unwrap();
    assert!(before.client_secret_configured);
    assert!(before.authorization.is_none());
    h.providers
        .delete_provider(&context, owner, provider.id())
        .await
        .unwrap();
    let after = h
        .providers
        .inspect_setup(&actor, owner, update.id(), 11)
        .await
        .unwrap();
    assert_eq!(after.setup.state(), ProviderSetupState::Canceled);
    assert!(!after.authentication.client_secret_configured);
    assert!(!after.authentication.authorized);
}

#[tokio::test]
async fn provider_deletion_rejects_pending_update_callbacks() {
    Box::pin(deletion_fences_callback(false)).await;
}

#[tokio::test]
async fn provider_deletion_discards_exchanging_update_callbacks() {
    Box::pin(deletion_fences_callback(true)).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "one OAuth race checks pending and in-flight callbacks through the same deletion journey"
)]
async fn deletion_fences_callback(in_flight: bool) {
    let api = OAuthApi::start().await;
    let h = harness(None).await;
    let (actor, workspace) = workspace_owner(&h, "callback-deletion-owner").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id();
    let provider = deletion_target(&h, &context, owner).await;
    let update = h
        .providers
        .prepare_update_fixture(&context, owner, provider.id(), api.document("Update"))
        .await
        .unwrap();
    let review = h
        .providers
        .review_setup_configuration(&actor, owner, update.id())
        .await
        .unwrap();
    let started = h
        .accounts
        .start_setup_authorization(
            &context,
            owner,
            update.id(),
            review.revision,
            confidential_client(),
        )
        .await
        .unwrap();
    let state = state_of(&started.authorization.unwrap().authorization_url.unwrap());
    let gate = Arc::new(crate::account::OAuthExchangeGate::default());
    let completing = if in_flight {
        let accounts = h.accounts.clone().with_exchange_gate(gate.clone());
        let context = context.clone();
        let owner = owner.clone();
        let id = update.id().clone();
        let state = state.clone();
        let task = tokio::spawn(async move {
            accounts
                .complete_setup_authorization(
                    &context,
                    Some((&owner, &id)),
                    &state,
                    Secret::new(CODE.into()).unwrap(),
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        Some(task)
    } else {
        None
    };
    h.providers
        .delete_provider(&context, owner, provider.id())
        .await
        .unwrap();
    if let Some(completing) = completing {
        gate.release.notify_one();
        let completion = completing.await.unwrap().unwrap();
        assert!(!completion.authentication.authorized);
        assert_eq!(
            completion.authentication.authorization.unwrap().state,
            SetupAuthorizationState::Canceled
        );
    }
    let after = h
        .providers
        .inspect_setup(&actor, owner, update.id(), 11)
        .await
        .unwrap();
    assert_eq!(after.setup.state(), ProviderSetupState::Canceled);
    assert!(!after.authentication.client_secret_configured);
    assert!(!after.authentication.authorized);
    assert_eq!(
        after.authentication.authorization.unwrap().state,
        SetupAuthorizationState::Canceled
    );
    let replay = h
        .accounts
        .complete_setup_authorization(
            &context,
            Some((owner, update.id())),
            &state,
            Secret::new(CODE.into()).unwrap(),
        )
        .await;
    if in_flight {
        assert!(!replay.unwrap().authentication.authorized);
    } else {
        assert!(matches!(replay, Err(Error::AccountInput)));
    }
    assert_eq!(
        api.knobs.tokens.load(Ordering::SeqCst),
        usize::from(in_flight)
    );
    assert!(matches!(
        h.accounts
            .start_setup_authorization(
                &context,
                owner,
                update.id(),
                review.revision,
                confidential_client()
            )
            .await,
        Err(Error::ProviderSetupNotApplicable)
    ));
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one authenticated discovery races Provider removal and verifies that the late catalog cannot revive its Setup"
)]
async fn provider_deletion_discards_in_flight_update_discovery() {
    let upstream = OAuthMcp::start().await;
    let h = harness(None).await;
    let (actor, workspace) = workspace_owner(&h, "discovery-deletion-owner").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id();
    let provider = deletion_target(&h, &context, owner).await;
    let update = h
        .providers
        .prepare_setup(
            &context,
            owner,
            crate::tests::mcp_url(upstream.mcp_url()),
            Some(provider.id()),
        )
        .await
        .unwrap();
    let review = h
        .providers
        .review_setup_configuration(&actor, owner, update.id())
        .await
        .unwrap();
    let confirmed = h
        .accounts
        .confirm_setup_configuration(
            &context,
            owner,
            update.id(),
            review.revision,
            review.document.unwrap().suggested,
            Some(confidential_client()),
        )
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty(), "{:?}", confirmed.errors);
    let started = h
        .accounts
        .start_setup_authorization(
            &context,
            owner,
            update.id(),
            confirmed.revision,
            confidential_client(),
        )
        .await
        .unwrap();
    let state = state_of(&started.authorization.unwrap().authorization_url.unwrap());
    let entered = Arc::new(tokio::sync::Notify::new());
    let discovery_entered = entered.clone();
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(header("authorization", format!("Bearer {ACCESS}").as_str()))
        .and(body_partial_json(json!({"method": "tools/list"})))
        .respond_with(move |request: &Request| {
            discovery_entered.notify_one();
            envelope(
                request,
                &json!({"tools": [{"name": "late", "inputSchema": {"type": "object"}}]}),
            )
            .set_delay(std::time::Duration::from_secs(2))
        })
        .with_priority(1)
        .mount(&upstream.server)
        .await;
    let completing = {
        let accounts = h.accounts.clone();
        let context = context.clone();
        let owner = owner.clone();
        let id = update.id().clone();
        tokio::spawn(async move {
            accounts
                .complete_setup_authorization(
                    &context,
                    Some((&owner, &id)),
                    &state,
                    Secret::new(CODE.into()).unwrap(),
                )
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    h.providers
        .delete_provider(&context, owner, provider.id())
        .await
        .unwrap();
    assert!(
        !completing.is_finished(),
        "deletion must commit while discovery is in flight"
    );
    assert!(!completing.await.unwrap().unwrap().authentication.authorized);
    let after = h
        .providers
        .inspect_setup(&actor, owner, update.id(), 11)
        .await
        .unwrap();
    assert_eq!(after.setup.state(), ProviderSetupState::Canceled);
    assert!(!after.authentication.authorized);
    assert!(!after.authentication.client_secret_configured);
    assert!(matches!(
        h.accounts
            .retry_setup_discovery(&context, owner, update.id())
            .await,
        Err(Error::ProviderSetupNotApplicable)
    ));
    assert!(
        !h.providers
            .list_lifecycle_events(&actor, owner)
            .await
            .unwrap()
            .iter()
            .any(
                |event| event.action() == LifecycleAction::SetupDiscoveryCompleted
                    && event.target_id() == update.id().to_string()
            )
    );
}
