//! Explicit provisional grant retention and replacement at the authorization/publication seam.
use super::*;
use crate::account::{AccountScope, AccountState, ProviderAccountId};

#[tokio::test]
async fn caller_publication_discards_discovery_grant_and_preserves_application() {
    let upstream = OAuthMcp::start().await;
    let harness = harness(None).await;
    let (actor, workspace) = workspace_owner(&harness, "user:discard-discovery").await;
    let owner = workspace.id();
    let context = change_context(actor.clone(), 10, '1');
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &context,
            owner,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let started = harness
        .accounts
        .start_setup_authorization(&context, owner, setup.id(), 1, client())
        .await
        .unwrap();
    let navigation = started.authorization.unwrap().authorization_url.unwrap();
    let completed = harness
        .accounts
        .complete_setup_authorization(
            &context,
            Some((owner, setup.id())),
            &state_of(&navigation),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    assert!(completed.authentication.authorized);
    let request = WorkspaceProviderPublicationRequest::new(
        super::super::account_tests::authorization_request(),
        owner.clone(),
        setup.id().clone(),
        crate::access::ProviderNamespace::parse("discarded").unwrap(),
        CredentialRoute::Caller,
    );
    let publication = harness
        .providers
        .publish_workspace_provider(&context, request.clone())
        .await
        .unwrap();
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    let provider = publication.published_provider().unwrap();
    for scope in [AccountScope::User, AccountScope::Shared] {
        assert!(
            harness
                .accounts
                .get_account(&context, owner, provider.id(), &scope)
                .await
                .unwrap()
                .account
                .is_none()
        );
    }
    assert!(
        !harness
            .providers
            .setup_authentication(&actor, owner, setup.id(), 11)
            .await
            .unwrap()
            .authorized
    );
    assert_eq!(
        harness
            .providers
            .publish_workspace_provider(&context, request)
            .await
            .unwrap(),
        publication
    );
    assert_eq!(upstream.knobs.tokens.load(Ordering::SeqCst), 1);
    let application = harness
        .accounts
        .oauth_client_status(&context, owner, provider.id())
        .await
        .unwrap();
    assert_eq!(
        application.policy.unwrap().client_id,
        query(&navigation, "client_id").unwrap()
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey separates discarded discovery credentials from retained confidential application configuration"
)]
async fn discarding_discovery_grant_keeps_confidential_application_after_restart() {
    use crate::account::{AuthorizationState, OAuthClient};
    let upstream = OAuthApi::start().await;
    let harness = harness(None).await;
    let (actor, workspace) = workspace_owner(&harness, "user:discard-confidential").await;
    let owner = workspace.id();
    let context = change_context(actor.clone(), 10, '1');
    let setup = harness
        .providers
        .prepare_openapi_fixture(&context, owner, upstream.document("Application"))
        .await
        .unwrap();
    let revision = configure_publication_application(
        &harness,
        &context,
        owner,
        setup.id(),
        Some("application-secret-canary"),
    )
    .await;
    let started = harness
        .accounts
        .start_setup_authorization(&context, owner, setup.id(), revision, client())
        .await
        .unwrap();
    let completed = harness
        .accounts
        .complete_setup_authorization(
            &context,
            None,
            &state_of(
                started
                    .authorization
                    .unwrap()
                    .authorization_url
                    .as_deref()
                    .unwrap(),
            ),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    let discarded = completed.authentication.provisional_grant_id.unwrap();
    let publication = harness
        .providers
        .publish_workspace_provider(
            &context,
            WorkspaceProviderPublicationRequest::new(
                super::super::account_tests::authorization_request(),
                owner.clone(),
                setup.id().clone(),
                crate::access::ProviderNamespace::parse("application").unwrap(),
                CredentialRoute::Caller,
            ),
        )
        .await
        .unwrap();
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    let provider = publication.published_provider().unwrap();
    assert!(
        harness
            .accounts
            .get_account(&context, owner, provider.id(), &AccountScope::User)
            .await
            .unwrap()
            .account
            .is_none()
    );
    let restarted = harness.restarted_accounts();
    let application = restarted
        .oauth_client_status(&context, owner, provider.id())
        .await
        .unwrap();
    assert!(application.secret_configured);
    assert_eq!(application.policy.unwrap().client_id, "manual-client");
    let started = restarted
        .start_user_authorization(
            &context,
            owner,
            provider.id(),
            super::super::account_tests::authorization_request(),
            Some(
                OAuthClient::new(
                    "manual-client".into(),
                    "https://reef.example/callback".into(),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    let completed = restarted
        .complete_oauth(
            &context,
            owner,
            started.id,
            &state_of(started.authorization_url.as_deref().unwrap()),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(completed.state, AuthorizationState::Succeeded);
    let account = restarted
        .get_account(&context, owner, provider.id(), &AccountScope::User)
        .await
        .unwrap()
        .account
        .unwrap();
    assert_ne!(account.id, discarded);
    assert_eq!(account.state, AccountState::Ready);
    let requests = upstream.server.received_requests().await.unwrap();
    let exchanges: Vec<_> = requests
        .iter()
        .filter(|request| request.url.path() == "/token")
        .collect();
    assert_eq!(exchanges.len(), 2);
    for exchange in exchanges {
        let form: std::collections::BTreeMap<_, _> =
            url::form_urlencoded::parse(&exchange.body).collect();
        assert_eq!(
            form.get("client_id").map(std::convert::AsRef::as_ref),
            Some("manual-client")
        );
        assert_eq!(
            form.get("client_secret").map(std::convert::AsRef::as_ref),
            Some("application-secret-canary")
        );
    }
}

struct AuthorizedSetup {
    id: crate::ProviderSetupId,
    revision: i64,
    grant: ProviderAccountId,
}

async fn authorized_bearer_setup(
    harness: &Harness,
    context: &coral_workspace::ChangeContext,
    owner: &WorkspaceId,
    document: coral_catalog::OpenApiDocument,
) -> AuthorizedSetup {
    let setup = harness
        .providers
        .prepare_openapi_fixture(context, owner, document)
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(context.actor(), owner, setup.id())
        .await
        .unwrap();
    let mut values = review.confirmed.unwrap();
    values.authentication = Some(AuthenticationConfiguration::Bearer);
    let confirmed = harness
        .providers
        .confirm_setup_configuration(context, owner, setup.id(), review.revision, values)
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty());
    let grant = harness
        .accounts
        .authorize_setup(
            context,
            owner,
            setup.id(),
            confirmed.revision,
            coral_credentials::CredentialValues::token(
                Secret::new("reviewed-account".into()).unwrap(),
            ),
        )
        .await
        .unwrap()
        .provisional_grant_id
        .unwrap();
    AuthorizedSetup {
        id: setup.id().clone(),
        revision: confirmed.revision,
        grant,
    }
}

#[tokio::test]
async fn publication_preserves_exact_reviewed_grant_on_failure_and_replay() {
    let upstream = OAuthApi::start().await;
    let harness = harness(None).await;
    let (actor, workspace) = workspace_owner(&harness, "user:replaced-setup-grant").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id();
    let setup = Box::pin(authorized_bearer_setup(
        &harness,
        &context,
        owner,
        upstream.document("Exact grant"),
    ))
    .await;
    let request = WorkspaceProviderPublicationRequest::new(
        super::super::account_tests::authorization_request(),
        owner.clone(),
        setup.id.clone(),
        crate::access::ProviderNamespace::parse("retained").unwrap(),
        CredentialRoute::Fixed,
    )
    .with_account_grant(setup.grant);
    let replacement = harness
        .accounts
        .authorize_setup(
            &context,
            owner,
            &setup.id,
            setup.revision,
            coral_credentials::CredentialValues::token(
                Secret::new("replacement-account".into()).unwrap(),
            ),
        )
        .await
        .unwrap()
        .provisional_grant_id
        .unwrap();
    assert_ne!(setup.grant, replacement);
    let result = harness
        .providers
        .publish_workspace_provider(&context, request.clone())
        .await
        .unwrap();
    assert_eq!(result.outcome(), ProviderPublicationOutcome::Failed);
    assert_eq!(result.diagnostic().unwrap().code(), "account_required");
    assert!(
        !harness
            .providers
            .setup_authentication(&actor, owner, &setup.id, 11)
            .await
            .unwrap()
            .authorized
    );
    assert!(
        harness
            .providers
            .inspect_providers(&actor, owner)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        harness
            .providers
            .publish_workspace_provider(&context, request.clone())
            .await
            .unwrap(),
        result
    );
    assert!(matches!(
        harness
            .providers
            .publish_workspace_provider(&context, request.with_account_grant(replacement))
            .await,
        Err(Error::RequestIdConflict)
    ));
}

#[tokio::test]
async fn publication_cannot_retain_another_setups_grant_and_scrubs_only_its_own() {
    let upstream = OAuthApi::start().await;
    let harness = harness(None).await;
    let (actor, workspace) = workspace_owner(&harness, "user:other-setup-grant").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id();
    let first = Box::pin(authorized_bearer_setup(
        &harness,
        &context,
        owner,
        upstream.document("First"),
    ))
    .await;
    let other = Box::pin(authorized_bearer_setup(
        &harness,
        &context,
        owner,
        upstream.document("Other"),
    ))
    .await;
    let request = WorkspaceProviderPublicationRequest::new(
        super::super::account_tests::authorization_request(),
        owner.clone(),
        first.id.clone(),
        crate::access::ProviderNamespace::parse("retained").unwrap(),
        CredentialRoute::Fixed,
    )
    .with_account_grant(other.grant);
    let result = harness
        .providers
        .publish_workspace_provider(&context, request)
        .await
        .unwrap();
    assert_eq!(result.outcome(), ProviderPublicationOutcome::Failed);
    assert_eq!(result.diagnostic().unwrap().code(), "account_required");
    assert!(
        harness
            .providers
            .inspect_providers(&actor, owner)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        !harness
            .providers
            .setup_authentication(&actor, owner, &first.id, 11)
            .await
            .unwrap()
            .authorized
    );
    assert!(
        harness
            .providers
            .setup_authentication(&actor, owner, &other.id, 11)
            .await
            .unwrap()
            .authorized
    );
    let valid = WorkspaceProviderPublicationRequest::new(
        super::super::account_tests::authorization_request(),
        owner.clone(),
        other.id,
        crate::access::ProviderNamespace::parse("retained").unwrap(),
        CredentialRoute::Fixed,
    )
    .with_account_grant(other.grant);
    let result = harness
        .providers
        .publish_workspace_provider(&context, valid)
        .await
        .unwrap();
    assert_eq!(result.outcome(), ProviderPublicationOutcome::Succeeded);
    let account = harness
        .accounts
        .get_account(
            &context,
            owner,
            result.published_provider().unwrap().id(),
            &AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(account.id, other.grant);
    assert_eq!(account.state, AccountState::Ready);
    assert_eq!(account.connected_at_unix_nanos, Some(10));
    assert_eq!(
        account.configured_by_user_id.as_ref(),
        Some(actor.user_id())
    );
}

#[tokio::test]
async fn shared_publication_requires_both_compatible_credentials_and_explicit_retention() {
    let upstream = OAuthApi::start().await;
    let harness = harness(None).await;
    let (actor, workspace) = workspace_owner(&harness, "user:retain-without-consent").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id();
    for authorized in [false, true] {
        let setup = if authorized {
            Box::pin(authorized_bearer_setup(
                &harness,
                &context,
                owner,
                upstream.document("Authorized"),
            ))
            .await
            .id
        } else {
            let setup = harness
                .providers
                .prepare_openapi_fixture(&context, owner, upstream.document("Unconnected"))
                .await
                .unwrap();
            configure_publication_application(
                &harness,
                &context,
                owner,
                setup.id(),
                Some("application-secret-canary"),
            )
            .await;
            setup.id().clone()
        };
        let request = WorkspaceProviderPublicationRequest::new(
            super::super::account_tests::authorization_request(),
            owner.clone(),
            setup.clone(),
            crate::access::ProviderNamespace::parse("retained").unwrap(),
            CredentialRoute::Fixed,
        );
        let publication = harness
            .providers
            .publish_workspace_provider(&context, request.clone())
            .await
            .unwrap();
        assert_eq!(publication.outcome(), ProviderPublicationOutcome::Failed);
        assert_eq!(publication.diagnostic().unwrap().code(), "account_required");
        assert!(publication.published_provider().is_none());
        assert!(
            !harness
                .providers
                .setup_authentication(&actor, owner, &setup, 11)
                .await
                .unwrap()
                .authorized
        );
        assert_eq!(
            harness
                .providers
                .publish_workspace_provider(&context, request)
                .await
                .unwrap(),
            publication
        );
    }
    assert!(
        harness
            .providers
            .inspect_providers(&actor, owner)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one complete replacement journey checks accepted intent, obsolete callbacks and atomic Account promotion"
)]
async fn discovery_replacement_requires_exact_grant_and_obsolete_callbacks_cannot_win() {
    let upstream = OAuthMcp::start().await;
    let harness = harness(None).await;
    let (actor, workspace) = workspace_owner(&harness, "user:replace-discovery").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id();
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &context,
            owner,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let started = harness
        .accounts
        .start_setup_authorization(&context, owner, setup.id(), 1, client())
        .await
        .unwrap();
    let original_state = state_of(
        started
            .authorization
            .unwrap()
            .authorization_url
            .as_deref()
            .unwrap(),
    );
    let completed = harness
        .accounts
        .complete_setup_authorization(
            &context,
            None,
            &original_state,
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    let grant = completed.authentication.provisional_grant_id.unwrap();
    assert!(completed.authentication.authorized);
    assert!(
        harness
            .providers
            .inspect_providers(&actor, owner)
            .await
            .unwrap()
            .is_empty()
    );
    let review = harness
        .providers
        .review_setup_configuration(&actor, owner, setup.id())
        .await
        .unwrap();
    for requested in [
        client(),
        client().replacing_grant(
            ProviderAccountId::parse("f0000000-0000-4000-8000-000000000001").unwrap(),
        ),
    ] {
        assert!(matches!(
            harness
                .accounts
                .start_setup_authorization(&context, owner, setup.id(), review.revision, requested)
                .await,
            Err(Error::ProviderSetupNotApplicable)
        ));
    }
    let replacement = harness
        .accounts
        .start_setup_authorization(
            &context,
            owner,
            setup.id(),
            review.revision,
            client().replacing_grant(grant),
        )
        .await
        .unwrap();
    let replacement_grant = replacement.provisional_grant_id.unwrap();
    assert_ne!(grant, replacement_grant);
    assert!(!replacement.authorized);
    harness
        .accounts
        .complete_setup_authorization(
            &context,
            None,
            &original_state,
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .expect_err("a superseded discovery grant cannot complete");
    assert!(matches!(
        harness
            .accounts
            .start_setup_authorization(
                &context,
                owner,
                setup.id(),
                review.revision,
                client().replacing_grant(grant)
            )
            .await,
        Err(Error::ProviderSetupNotApplicable)
    ));
    let completed = harness
        .accounts
        .complete_setup_authorization(
            &context,
            None,
            &state_of(
                replacement
                    .authorization
                    .unwrap()
                    .authorization_url
                    .as_deref()
                    .unwrap(),
            ),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    assert!(completed.authentication.authorized);
    assert_eq!(
        completed.authentication.provisional_grant_id,
        Some(replacement_grant)
    );
    assert_eq!(upstream.knobs.tokens.load(Ordering::SeqCst), 2);
    let request = WorkspaceProviderPublicationRequest::new(
        super::super::account_tests::authorization_request(),
        owner.clone(),
        setup.id().clone(),
        crate::access::ProviderNamespace::parse("replacement").unwrap(),
        CredentialRoute::Fixed,
    )
    .with_account_grant(replacement_grant);
    let publication = harness
        .providers
        .publish_workspace_provider(&context, request.clone())
        .await
        .unwrap();
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    let account = harness
        .accounts
        .get_account(
            &context,
            owner,
            publication.published_provider().unwrap().id(),
            &AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(account.id, replacement_grant);
    assert_eq!(account.state, AccountState::Ready);
    assert_eq!(account.connected_at_unix_nanos, Some(10));
    assert_eq!(
        account.configured_by_user_id.as_ref(),
        Some(actor.user_id())
    );
    assert_eq!(
        harness
            .providers
            .publish_workspace_provider(&context, request.clone())
            .await
            .unwrap(),
        publication
    );
    assert!(matches!(
        harness
            .providers
            .publish_workspace_provider(&context, request.with_account_grant(grant))
            .await,
        Err(Error::RequestIdConflict)
    ));
    assert!(
        !harness
            .providers
            .setup_authentication(&actor, owner, setup.id(), 11)
            .await
            .unwrap()
            .authorized
    );
}

#[tokio::test]
async fn late_client_bootstrap_cannot_overwrite_a_newer_pending_authorization() {
    bootstrap_cannot_overwrite(false).await;
}

#[tokio::test]
async fn late_client_bootstrap_cannot_overwrite_a_newer_authorized_grant() {
    bootstrap_cannot_overwrite(true).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "one deterministic external-bootstrap race proves pending and successful grant supersession"
)]
async fn bootstrap_cannot_overwrite(complete_newer: bool) {
    let upstream = OAuthMcp::start().await;
    let harness = harness(None).await;
    let (actor, workspace) = workspace_owner(&harness, "user:bootstrap-race").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id();
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &context,
            owner,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let registration_entered = entered.clone();
    Mock::given(method("POST")).and(path("/register")).respond_with(move |request: &Request| {
        registration_entered.notify_one();
        let body: Value = request.body_json().unwrap();
        ResponseTemplate::new(201).set_delay(std::time::Duration::from_secs(2)).set_body_json(json!({
            "client_id": "late-client", "token_endpoint_auth_method": "none", "redirect_uris": body["redirect_uris"],
        }))
    }).with_priority(1).mount(&upstream.server).await;
    let old_module = harness.accounts.clone();
    let old_context = context.clone();
    let old_owner = owner.clone();
    let old_setup = setup.id().clone();
    let older = tokio::spawn(async move {
        old_module
            .start_setup_authorization(&old_context, &old_owner, &old_setup, 1, client())
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let newer = harness
        .accounts
        .start_setup_authorization(
            &context,
            owner,
            setup.id(),
            1,
            SetupOAuthClient::new(
                "https://reef.example/provider-setup/oauth/callback".into(),
                Some("manual-client".into()),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let newer_state = state_of(
        newer
            .authorization
            .as_ref()
            .unwrap()
            .authorization_url
            .as_deref()
            .unwrap(),
    );
    let newer = if complete_newer {
        harness
            .accounts
            .complete_setup_authorization(
                &context,
                None,
                &newer_state,
                Secret::new(CODE.into()).unwrap(),
            )
            .await
            .unwrap()
            .authentication
    } else {
        newer
    };
    assert!(matches!(
        older.await.unwrap(),
        Err(Error::ProviderSetupNotApplicable)
    ));
    let current = harness
        .providers
        .setup_authentication(&actor, owner, setup.id(), 11)
        .await
        .unwrap();
    assert_eq!(current.provisional_grant_id, newer.provisional_grant_id);
    assert_eq!(current.authorization, newer.authorization);
    let completed = harness
        .accounts
        .complete_setup_authorization(
            &context,
            None,
            &newer_state,
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    assert!(completed.authentication.authorized);
    assert_eq!(
        completed.authentication.provisional_grant_id,
        newer.provisional_grant_id
    );
    assert_eq!(upstream.knobs.tokens.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn replacement_rechecks_the_grant_deadline_after_client_bootstrap() {
    let upstream = OAuthMcp::start().await;
    let harness = harness(None).await;
    let (actor, workspace) = workspace_owner(&harness, "user:bootstrap-expiry").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id();
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &context,
            owner,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let started = harness
        .accounts
        .start_setup_authorization(&context, owner, setup.id(), 1, client())
        .await
        .unwrap();
    let completed = harness
        .accounts
        .complete_setup_authorization(
            &context,
            None,
            &state_of(
                started
                    .authorization
                    .unwrap()
                    .authorization_url
                    .as_deref()
                    .unwrap(),
            ),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    let grant = completed.authentication.provisional_grant_id.unwrap();
    let deadline = completed.authentication.expires_at_unix_nanos.unwrap();
    let revision = harness
        .providers
        .review_setup_configuration(&actor, owner, setup.id())
        .await
        .unwrap()
        .revision;
    // The grant is live at request start, but even local application bootstrap
    // crosses the final nanosecond. No sleeping or injected domain clock is needed.
    let expiring = change_context(actor.clone(), deadline - 1, '2');
    assert!(matches!(
        harness
            .accounts
            .start_setup_authorization(
                &expiring,
                owner,
                setup.id(),
                revision,
                client().replacing_grant(grant),
            )
            .await,
        Err(Error::ProviderSetupNotApplicable)
    ));
    let current = harness
        .providers
        .setup_authentication(&actor, owner, setup.id(), deadline)
        .await
        .unwrap();
    assert!(!current.authorized);
    assert_eq!(current.provisional_grant_id, Some(grant));
    assert!(
        harness
            .providers
            .inspect_providers(&actor, owner)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(upstream.knobs.tokens.load(Ordering::SeqCst), 1);
}
