use super::*;
use std::sync::Arc;

#[tokio::test]
async fn shared_replacement_and_setting_change_roll_back_accounts_configuration_and_events() {
    for setting in [false, true] {
        Box::pin(rejected_commit(setting)).await;
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "fresh interface reads verify every staged publication effect rolls back together"
)]
async fn rejected_commit(setting: bool) {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "rollback-update-owner").await;
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
    let before = h
        .providers
        .list_lifecycle_events(&actor, workspace.id())
        .await
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
        .unwrap();
    let hold = crate::tests::HoldForever::at(crate::PublicationPoint::TerminalCommit);
    let failing = ProviderModule::with_test_dependencies(
        h.database.clone(),
        h.workspaces.clone(),
        Arc::new(crate::publication::materialization::CatalogMaterializer(
            coral_outbound::DestinationPolicy::default()
                .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        )),
        hold.clone(),
    );
    let request = ProviderRegenerationRequest::new(
        request_id(),
        workspace.id().clone(),
        provider.id().clone(),
        candidate.id().clone(),
        2,
    )
    .with_incompatible_configuration_confirmation();
    let request = if setting {
        request.with_account_setting(CredentialRoute::Caller)
    } else {
        request.with_account_grant(grant)
    };
    // The regeneration stops after staging its writes, before they commit.
    let interrupted = tokio::spawn({
        let context = context.clone();
        async move { failing.regenerate_provider(&context, request).await }
    });
    hold.held().await;
    interrupted.abort();
    assert!(interrupted.await.unwrap_err().is_cancelled());
    assert_eq!(
        h.providers
            .inspect_provider(&actor, workspace.id(), provider.id())
            .await
            .unwrap(),
        provider
    );
    assert_eq!(
        h.providers
            .list_lifecycle_events(&actor, workspace.id())
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        h.accounts
            .get_account(
                &context,
                workspace.id(),
                provider.id(),
                &account::AccountScope::Shared
            )
            .await
            .unwrap(),
        account
    );
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
        "old-secret"
    );
    h.accounts
        .cancel_setup(&context, workspace.id(), candidate.id())
        .await
        .unwrap();
    assert!(
        !h.providers
            .setup_authentication(&actor, workspace.id(), candidate.id(), 11)
            .await
            .unwrap()
            .authorized
    );
}

#[tokio::test]
async fn first_shared_publication_wins_over_a_competing_prepared_replacement() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "competing-update-owner").await;
    let context = change_context(actor.clone(), 10, '2');
    let provider = shared(&h, &context, workspace.id()).await;
    let mut requests = Vec::new();
    for secret in ["winning-secret", "stale-secret"] {
        let candidate = setup(
            &h,
            &context,
            workspace.id(),
            "https://old.example.test",
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
                coral_credentials::CredentialValues::token(Secret::new(secret.into()).unwrap()),
            )
            .await
            .unwrap()
            .provisional_grant_id
            .unwrap();
        requests.push(
            ProviderRegenerationRequest::new(
                request_id(),
                workspace.id().clone(),
                provider.id().clone(),
                candidate.id().clone(),
                2,
            )
            .with_account_grant(grant),
        );
    }
    let mut requests = requests.into_iter();
    let winner = requests.next().unwrap();
    let loser = requests.next().unwrap();
    let first = h
        .providers
        .regenerate_provider(&context, winner.clone())
        .await
        .unwrap();
    assert_eq!(first.outcome(), ProviderPublicationOutcome::Succeeded);
    let rejected = h
        .providers
        .regenerate_provider(&context, loser.clone())
        .await
        .unwrap();
    assert_eq!(rejected.outcome(), ProviderPublicationOutcome::Failed);
    assert_eq!(
        rejected.diagnostic().unwrap().code(),
        "update_account_changed"
    );
    assert!(
        !h.providers
            .setup_authentication(&actor, workspace.id(), loser.setup_id(), 11)
            .await
            .unwrap()
            .authorized
    );
    assert_eq!(
        h.providers
            .regenerate_provider(&context, winner)
            .await
            .unwrap(),
        first
    );
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
        "winning-secret"
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "both setting transitions fence their already accepted initial and replacement authorizations"
)]
async fn setting_change_cancels_pending_connections_and_reconnects_before_their_completion() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "setting-race-owner").await;
    let context = change_context(actor.clone(), 10, '2');
    let provider = shared(&h, &context, workspace.id()).await;
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
    let pending = h
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
    let candidate = setup(
        &h,
        &context,
        workspace.id(),
        "https://old.example.test",
        Some(provider.id()),
    )
    .await;
    h.providers
        .regenerate_provider(
            &context,
            ProviderRegenerationRequest::new(
                request_id(),
                workspace.id().clone(),
                provider.id().clone(),
                candidate.id().clone(),
                2,
            )
            .with_account_setting(CredentialRoute::Caller)
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .unwrap();
    assert_eq!(
        h.accounts
            .submit_secret(
                &context,
                workspace.id(),
                pending.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("late-secret".into()).unwrap()
                )
            )
            .await
            .unwrap()
            .state,
        account::AuthorizationState::Canceled
    );
    let initial = h
        .accounts
        .start_user_authorization(&context, workspace.id(), provider.id(), request_id(), None)
        .await
        .unwrap();
    let candidate = setup(
        &h,
        &context,
        workspace.id(),
        "https://old.example.test",
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
            coral_credentials::CredentialValues::token(Secret::new("fresh-shared".into()).unwrap()),
        )
        .await
        .unwrap()
        .provisional_grant_id
        .unwrap();
    h.providers
        .regenerate_provider(
            &context,
            ProviderRegenerationRequest::new(
                request_id(),
                workspace.id().clone(),
                provider.id().clone(),
                candidate.id().clone(),
                2,
            )
            .with_account_setting(CredentialRoute::Fixed)
            .with_account_grant(grant)
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .unwrap();
    assert_eq!(
        h.accounts
            .submit_secret(
                &context,
                workspace.id(),
                initial.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("late-initial".into()).unwrap()
                )
            )
            .await
            .unwrap()
            .state,
        account::AuthorizationState::Canceled
    );
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
