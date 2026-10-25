//! Update acceptance through the Provider and Account interfaces on both databases.
mod expiry;
mod races;
use crate::setup_tests::{HOUR, Harness, credential_for, harness, user_principal, workspace_owner};
use crate::tests::change_context;
use crate::*;
use coral_credentials::Secret;
use coral_workspace::ChangeContext;

fn request_id() -> ManagementRequestId {
    ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap()
}

async fn setup(
    h: &Harness,
    context: &ChangeContext,
    workspace: &WorkspaceId,
    destination: &str,
    target: Option<&ProviderId>,
) -> ProviderSetup {
    let document = OpenApiDocument::parse(&serde_json::json!({
        "openapi": "3.0.3", "info": {"title": "Update", "version": "1"},
        "servers": [{"url": destination}], "security": [{"token": []}],
        "components": {"securitySchemes": {"token": {"type": "http", "scheme": "bearer"}}},
        "paths": {"/read": {"get": {"operationId": "read", "responses": {"200": {"description": "OK"}}}}}
    }).to_string()).unwrap();
    let values = document.configuration_review().unwrap().suggested;
    let setup = h
        .providers
        .prepare_setup(
            context,
            workspace,
            ProviderSetupSource::OpenApiDocument(document),
            target,
        )
        .await
        .unwrap();
    let review = h
        .providers
        .review_setup_configuration(context.actor(), workspace, setup.id())
        .await
        .unwrap();
    h.providers
        .confirm_setup_configuration(context, workspace, setup.id(), review.revision, values)
        .await
        .unwrap();
    h.providers
        .get_setup(context.actor(), workspace, setup.id())
        .await
        .unwrap()
}

async fn shared(h: &Harness, context: &ChangeContext, workspace: &WorkspaceId) -> Provider {
    let initial = setup(h, context, workspace, "https://old.example.test", None).await;
    let grant = h
        .accounts
        .authorize_setup(
            context,
            workspace,
            initial.id(),
            1,
            coral_credentials::CredentialValues::token(Secret::new("old-secret".into()).unwrap()),
        )
        .await
        .unwrap()
        .provisional_grant_id
        .unwrap();
    h.providers
        .publish_workspace_provider(
            context,
            WorkspaceProviderPublicationRequest::new(
                request_id(),
                workspace.clone(),
                initial.id().clone(),
                ProviderNamespace::parse("update").unwrap(),
                CredentialRoute::Fixed,
            )
            .with_account_grant(grant),
        )
        .await
        .unwrap()
        .published_provider()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn incompatible_shared_update_without_replacement_preserves_the_active_pair() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "update-owner").await;
    let context = change_context(actor.clone(), 10, '2');
    let provider = shared(&h, &context, workspace.id()).await;
    let candidate = setup(
        &h,
        &context,
        workspace.id(),
        "https://candidate.example.test",
        Some(provider.id()),
    )
    .await;
    let result = h
        .providers
        .regenerate_provider(
            &context,
            ProviderRegenerationRequest::new(
                request_id(),
                workspace.id().clone(),
                provider.id().clone(),
                candidate.id().clone(),
                2,
            )
            .with_incompatible_configuration_confirmation(),
        )
        .await;
    assert!(matches!(result, Err(crate::Error::AccountRequired)));
    let secret = credential_for(
        &h,
        workspace.id(),
        provider.id(),
        "update",
        "read",
        &user_principal(&actor),
        11,
    )
    .await
    .unwrap();
    assert_eq!(
        secret
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "old-secret"
    );
}

#[tokio::test]
async fn account_setting_change_scrubs_old_credentials_and_requires_fresh_shared_consent() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "setting-owner").await;
    let context = change_context(actor.clone(), 10, '2');
    let provider = shared(&h, &context, workspace.id()).await;
    let separate = setup(
        &h,
        &context,
        workspace.id(),
        "https://old.example.test",
        Some(provider.id()),
    )
    .await;
    let changed = h
        .providers
        .regenerate_provider(
            &context,
            ProviderRegenerationRequest::new(
                request_id(),
                workspace.id().clone(),
                provider.id().clone(),
                separate.id().clone(),
                2,
            )
            .with_account_setting(CredentialRoute::Caller)
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .unwrap();
    assert_eq!(changed.outcome(), ProviderPublicationOutcome::Succeeded);
    credential_for(
        &h,
        workspace.id(),
        provider.id(),
        "update",
        "read",
        &user_principal(&actor),
        11,
    )
    .await
    .unwrap_err();
    let shared = setup(
        &h,
        &context,
        workspace.id(),
        "https://old.example.test",
        Some(provider.id()),
    )
    .await;
    let request = ProviderRegenerationRequest::new(
        request_id(),
        workspace.id().clone(),
        provider.id().clone(),
        shared.id().clone(),
        2,
    )
    .with_account_setting(CredentialRoute::Fixed)
    .with_incompatible_configuration_confirmation();
    h.providers
        .regenerate_provider(&context, request.clone())
        .await
        .unwrap_err();
    let grant = h
        .accounts
        .authorize_setup(
            &context,
            workspace.id(),
            shared.id(),
            2,
            coral_credentials::CredentialValues::token(Secret::new("fresh-shared".into()).unwrap()),
        )
        .await
        .unwrap()
        .provisional_grant_id
        .unwrap();
    let changed = h
        .providers
        .regenerate_provider(&context, request.with_account_grant(grant))
        .await
        .unwrap();
    assert_eq!(changed.outcome(), ProviderPublicationOutcome::Succeeded);
    assert_eq!(
        credential_for(
            &h,
            workspace.id(),
            provider.id(),
            "update",
            "read",
            &user_principal(&actor),
            11
        )
        .await
        .unwrap()
        .get(coral_credentials::CredentialValues::TOKEN)
        .unwrap()
        .expose(),
        "fresh-shared"
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the competing reconnect preserves active credentials and terminally scrubs the obsolete staged Update"
)]
async fn reconnect_after_update_preparation_fences_the_staged_replacement() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "update-race-owner").await;
    let context = change_context(actor.clone(), 10, '2');
    let provider = shared(&h, &context, workspace.id()).await;
    let candidate = setup(
        &h,
        &context,
        workspace.id(),
        "https://candidate.example.test",
        Some(provider.id()),
    )
    .await;
    let grant = h
        .accounts
        .authorize_setup(
            &context,
            workspace.id(),
            candidate.id(),
            2,
            coral_credentials::CredentialValues::token(
                Secret::new("candidate-secret".into()).unwrap(),
            ),
        )
        .await
        .unwrap()
        .provisional_grant_id
        .unwrap();
    let account = h
        .accounts
        .get_account(
            &context,
            workspace.id(),
            provider.id(),
            &account::AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    let reconnect = h
        .accounts
        .start_reconnect(
            &context,
            workspace.id(),
            provider.id(),
            account.id,
            request_id(),
            None,
        )
        .await
        .unwrap();
    h.accounts
        .submit_secret(
            &context,
            workspace.id(),
            reconnect.id,
            coral_credentials::CredentialValues::token(
                Secret::new("reconnected-secret".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let result = h
        .providers
        .regenerate_provider(
            &context,
            ProviderRegenerationRequest::new(
                request_id(),
                workspace.id().clone(),
                provider.id().clone(),
                candidate.id().clone(),
                2,
            )
            .with_incompatible_configuration_confirmation()
            .with_account_grant(grant),
        )
        .await;
    let rejected = result.unwrap();
    assert_eq!(rejected.outcome(), ProviderPublicationOutcome::Failed);
    assert_eq!(
        rejected.diagnostic().unwrap().code(),
        "update_account_changed"
    );
    assert!(
        !h.providers
            .setup_authentication(&actor, workspace.id(), candidate.id(), 11)
            .await
            .unwrap()
            .authorized
    );
    assert!(matches!(
        h.providers
            .get_setup(&actor, workspace.id(), candidate.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Consumed(_)
    ));
    let secret = credential_for(
        &h,
        workspace.id(),
        provider.id(),
        "update",
        "read",
        &user_principal(&actor),
        11,
    )
    .await
    .unwrap();
    assert_eq!(
        secret
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "reconnected-secret"
    );
}

#[tokio::test]
async fn explicit_shared_replacement_publishes_configuration_and_account_together() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "replacement-owner").await;
    let context = change_context(actor.clone(), 10, '2');
    let provider = shared(&h, &context, workspace.id()).await;
    let candidate = setup(
        &h,
        &context,
        workspace.id(),
        "https://candidate.example.test",
        Some(provider.id()),
    )
    .await;
    let grant = h
        .accounts
        .authorize_setup(
            &context,
            workspace.id(),
            candidate.id(),
            2,
            coral_credentials::CredentialValues::token(Secret::new("new-secret".into()).unwrap()),
        )
        .await
        .unwrap()
        .provisional_grant_id
        .unwrap();
    let request = ProviderRegenerationRequest::new(
        request_id(),
        workspace.id().clone(),
        provider.id().clone(),
        candidate.id().clone(),
        2,
    )
    .with_incompatible_configuration_confirmation()
    .with_account_grant(grant);
    let result = h
        .providers
        .regenerate_provider(&context, request.clone())
        .await
        .unwrap();
    assert_eq!(result.outcome(), ProviderPublicationOutcome::Succeeded);
    assert_eq!(
        h.providers
            .regenerate_provider(&context, request)
            .await
            .unwrap(),
        result
    );
    let secret = credential_for(
        &h,
        workspace.id(),
        provider.id(),
        "update",
        "read",
        &user_principal(&actor),
        11,
    )
    .await
    .unwrap();
    assert_eq!(
        secret
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "new-secret"
    );
    assert!(
        !h.providers
            .setup_authentication(&actor, workspace.id(), candidate.id(), 11)
            .await
            .unwrap()
            .authorized
    );
}

// Catches: Edit configuration's Cancel failing, because an Update canceled or
// expired before it confirmed its saved values had no preview to inspect.
#[tokio::test]
async fn an_update_ended_before_confirmation_stays_inspectable() {
    for (cancel, inspected_at, expected) in [
        (true, 11, ProviderSetupState::Canceled),
        (false, 10 + 2 * HOUR, ProviderSetupState::Expired),
    ] {
        let h = harness().await;
        let (actor, workspace) = workspace_owner(&h, "ended-update-owner").await;
        let context = change_context(actor.clone(), 10, '2');
        let provider = shared(&h, &context, workspace.id()).await;
        let candidate = h
            .providers
            .prepare_setup(
                &context,
                workspace.id(),
                ProviderSetupSource::Current,
                Some(provider.id()),
            )
            .await
            .unwrap();
        if cancel {
            h.accounts
                .cancel_setup(&context, workspace.id(), candidate.id())
                .await
                .unwrap();
        }
        let inspected = h
            .providers
            .inspect_setup(&actor, workspace.id(), candidate.id(), inspected_at)
            .await
            .unwrap();
        assert_eq!(inspected.setup.state(), expected);
    }
}

/// An Update takes only a route that suits the replacement source, by the
/// rule Setup publishability and creation use. A refusal keeps the active
/// Provider and its shared Account.
#[tokio::test]
async fn an_update_to_a_credential_free_source_takes_only_the_none_route() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "fit-owner").await;
    let context = change_context(actor.clone(), 10, '2');
    let provider = shared(&h, &context, workspace.id()).await;
    for (route, refused) in [
        (CredentialRoute::Fixed, true),
        (CredentialRoute::Caller, true),
        (CredentialRoute::None, false),
    ] {
        let document = OpenApiDocument::parse(crate::tests::CREDENTIAL_FREE_OPENAPI).unwrap();
        let values = document.configuration_review().unwrap().suggested;
        let free = h
            .providers
            .prepare_setup(
                &context,
                workspace.id(),
                ProviderSetupSource::OpenApiDocument(document),
                Some(provider.id()),
            )
            .await
            .unwrap();
        let review = h
            .providers
            .review_setup_configuration(&actor, workspace.id(), free.id())
            .await
            .unwrap();
        let confirmed = h
            .providers
            .confirm_setup_configuration(
                &context,
                workspace.id(),
                free.id(),
                review.revision,
                values,
            )
            .await
            .unwrap();
        assert!(confirmed.errors.is_empty(), "{:?}", confirmed.errors);
        let result = h
            .providers
            .regenerate_provider(
                &context,
                ProviderRegenerationRequest::new(
                    request_id(),
                    workspace.id().clone(),
                    provider.id().clone(),
                    free.id().clone(),
                    confirmed.revision,
                )
                .with_account_setting(route)
                .with_incompatible_configuration_confirmation(),
            )
            .await;
        if refused {
            assert!(
                matches!(result, Err(crate::Error::AccountInput)),
                "{route:?}: {result:?}"
            );
            let current = h
                .providers
                .inspect_provider(&actor, workspace.id(), provider.id())
                .await
                .unwrap();
            assert_eq!(current.access().credential_route(), CredentialRoute::Fixed);
            assert_eq!(
                credential_for(
                    &h,
                    workspace.id(),
                    provider.id(),
                    "update",
                    "read",
                    &user_principal(&actor),
                    11,
                )
                .await
                .unwrap()
                .get(coral_credentials::CredentialValues::TOKEN)
                .unwrap()
                .expose(),
                "old-secret",
                "{route:?}: the refusal keeps the shared Account"
            );
        } else {
            assert_eq!(
                result.unwrap().outcome(),
                ProviderPublicationOutcome::Succeeded,
                "{route:?}"
            );
            let current = h
                .providers
                .inspect_provider(&actor, workspace.id(), provider.id())
                .await
                .unwrap();
            assert_eq!(current.access().credential_route(), CredentialRoute::None);
        }
    }
}
