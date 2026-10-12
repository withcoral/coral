//! Current-catalog Update and borrowed runtime authorization through the Account interface.
use super::*;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one current-catalog Update retains source metadata through application change, fresh consent, discovery and atomic replacement"
)]
async fn current_mcp_catalog_can_change_application_and_complete_fresh_discovery() {
    let upstream = OAuthMcp::start().await;
    let h = harness(None).await;
    let (actor, workspace) = workspace_owner(&h, "current-application-owner").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id();
    let setup = h
        .accounts
        .prepare_remote_fixture(
            &context,
            owner,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let started = h
        .accounts
        .start_setup_authorization(&context, owner, setup.id(), 1, client())
        .await
        .unwrap();
    let completed = h
        .accounts
        .complete_setup_authorization(
            &context,
            Some((owner, setup.id())),
            &state_of(&started.authorization.unwrap().authorization_url.unwrap()),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    let provider = h
        .providers
        .publish_workspace_provider(
            &context,
            WorkspaceProviderPublicationRequest::new(
                crate::account_tests::authorization_request(),
                owner.clone(),
                setup.id().clone(),
                crate::ProviderNamespace::parse("application_update").unwrap(),
                CredentialRoute::Fixed,
            )
            .with_account_grant(completed.authentication.provisional_grant_id.unwrap()),
        )
        .await
        .unwrap()
        .published_provider()
        .unwrap()
        .clone();
    let before = h
        .accounts
        .get_account(&context, owner, provider.id(), &AccountScope::Shared)
        .await
        .unwrap();
    let update = h
        .providers
        .prepare_setup(
            &context,
            owner,
            ProviderSetupSource::Current,
            Some(provider.id()),
        )
        .await
        .unwrap();
    let review = h
        .providers
        .review_setup_configuration(&actor, owner, update.id())
        .await
        .unwrap();
    let application = SetupOAuthClient::new(
        "https://reef.example/callback".into(),
        Some("replacement-application".into()),
    )
    .unwrap();
    let confirmed = h
        .accounts
        .confirm_setup_configuration(
            &context,
            owner,
            update.id(),
            review.revision,
            review.confirmed.unwrap(),
            Some(application),
        )
        .await
        .expect("configuration remains inspectable after discarding the old snapshot");
    assert!(confirmed.errors.is_empty(), "{:?}", confirmed.errors);
    let inspected = h
        .providers
        .inspect_setup(&actor, owner, update.id(), 11)
        .await
        .unwrap();
    assert_eq!(
        inspected.setup.state(),
        ProviderSetupState::AwaitingAuthorization
    );
    let authorization = h
        .accounts
        .start_setup_authorization(
            &context,
            owner,
            update.id(),
            confirmed.revision,
            SetupOAuthClient::new(
                "https://reef.example/callback".into(),
                Some("replacement-application".into()),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let navigation = authorization
        .authorization
        .unwrap()
        .authorization_url
        .unwrap();
    assert_eq!(
        query(&navigation, "client_id").as_deref(),
        Some("replacement-application")
    );
    let completed = h
        .accounts
        .complete_setup_authorization(
            &context,
            Some((owner, update.id())),
            &state_of(&navigation),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    assert!(completed.authentication.authorized);
    assert_eq!(
        h.providers
            .get_setup(&actor, owner, update.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Ready
    );
    assert_eq!(
        h.accounts
            .get_account(&context, owner, provider.id(), &AccountScope::Shared)
            .await
            .unwrap(),
        before
    );
    let result = h
        .providers
        .regenerate_provider(
            &context,
            crate::ProviderRegenerationRequest::new(
                crate::account_tests::authorization_request(),
                owner.clone(),
                provider.id().clone(),
                update.id().clone(),
                confirmed.revision,
            )
            .with_account_grant(completed.authentication.provisional_grant_id.unwrap())
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .unwrap();
    assert_eq!(result.outcome(), ProviderPublicationOutcome::Succeeded);
    assert_eq!(
        h.accounts
            .oauth_client_status(&context, owner, provider.id())
            .await
            .unwrap()
            .policy
            .unwrap()
            .client_id,
        "replacement-application"
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one real OAuth discovery verifies renewal and unchanged connection metadata"
)]
async fn authenticated_update_renews_expired_borrowed_material_without_reconnecting() {
    let upstream = OAuthMcp::start().await;
    let h = harness(None).await;
    let (actor, workspace) = workspace_owner(&h, "borrow-renewal-owner").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id();
    let setup = h
        .accounts
        .prepare_remote_fixture(
            &context,
            owner,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let started = h
        .accounts
        .start_setup_authorization(&context, owner, setup.id(), 1, client())
        .await
        .unwrap();
    let completed = h
        .accounts
        .complete_setup_authorization(
            &context,
            Some((owner, setup.id())),
            &state_of(&started.authorization.unwrap().authorization_url.unwrap()),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    let provider = h
        .providers
        .publish_workspace_provider(
            &context,
            WorkspaceProviderPublicationRequest::new(
                crate::account_tests::authorization_request(),
                owner.clone(),
                setup.id().clone(),
                crate::ProviderNamespace::parse("borrowed").unwrap(),
                CredentialRoute::Fixed,
            )
            .with_account_grant(completed.authentication.provisional_grant_id.unwrap()),
        )
        .await
        .unwrap()
        .published_provider()
        .unwrap()
        .clone();
    let before = h
        .accounts
        .get_account(&context, owner, provider.id(), &AccountScope::Shared)
        .await
        .unwrap();
    Mock::given(method("POST")).and(path("/token"))
        .and(wiremock::matchers::body_string_contains("grant_type=refresh_token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"access_token": ACCESS, "token_type":"Bearer", "expires_in":3600, "scope":"read write"})))
        .with_priority(1).expect(1).mount(&upstream.server).await;
    let later = change_context(actor.clone(), 2 * HOUR, '2');
    let update = h
        .providers
        .prepare_setup(
            &later,
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
    let review = h
        .providers
        .confirm_setup_configuration(
            &later,
            owner,
            update.id(),
            review.revision,
            review.confirmed.unwrap(),
        )
        .await
        .unwrap();
    assert!(review.errors.is_empty());
    let ready = h
        .accounts
        .discover_setup_with_account(&later, owner, update.id(), review.revision)
        .await
        .unwrap();
    assert_eq!(ready.state(), ProviderSetupState::Ready);
    assert!(
        !h.providers
            .setup_authentication(&actor, owner, update.id(), 2 * HOUR + 1)
            .await
            .unwrap()
            .authorized
    );
    assert_eq!(
        h.accounts
            .get_account(&later, owner, provider.id(), &AccountScope::Shared)
            .await
            .unwrap(),
        before
    );
}
