use super::*;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one expiry journey proves terminal diagnostics, active-pair preservation, scrubbed material and exact replay without duplicate events"
)]
async fn expired_shared_replacement_records_one_terminal_failure_and_scrubs_the_grant() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "expired-update-owner").await;
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
                Secret::new("expired-replacement".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let request = ProviderRegenerationRequest::new(
        request_id(),
        workspace.id().clone(),
        provider.id().clone(),
        candidate.id().clone(),
        2,
    )
    .with_account_grant(grant.provisional_grant_id.unwrap())
    .with_incompatible_configuration_confirmation();
    let expired = change_context(actor.clone(), grant.expires_at_unix_nanos.unwrap() + 1, '3');
    let failed = h
        .providers
        .regenerate_provider(&expired, request.clone())
        .await
        .expect("expiry is a terminal domain diagnostic");
    assert_eq!(failed.outcome(), ProviderPublicationOutcome::Failed);
    assert_eq!(failed.diagnostic().unwrap().code(), "setup_expired");
    assert_eq!(
        h.providers
            .get_setup(&actor, workspace.id(), candidate.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Consumed(failed.id().clone())
    );
    assert_eq!(
        h.providers
            .inspect_provider(&actor, workspace.id(), provider.id())
            .await
            .unwrap(),
        provider
    );
    assert_eq!(
        credential_for(
            &h,
            workspace.id(),
            provider.id(),
            "update",
            "read",
            &user_principal(&actor),
            expired.occurred_at_unix_nanos()
        )
        .await
        .unwrap()
        .get(coral_credentials::CredentialValues::TOKEN)
        .unwrap()
        .expose(),
        "old-secret"
    );
    let events = h
        .providers
        .list_lifecycle_events(&actor, workspace.id())
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(
                |event| event.action() == LifecycleAction::PublicationCompleted
                    && event.outcome() == LifecycleOutcome::Failed
            )
            .count(),
        1
    );
    assert_eq!(
        h.providers
            .regenerate_provider(&expired, request)
            .await
            .unwrap(),
        failed
    );
    assert_eq!(
        h.providers
            .list_lifecycle_events(&actor, workspace.id())
            .await
            .unwrap(),
        events
    );
    // Inspect using the earlier clock: a false result proves scrubbing, not merely expiry.
    assert!(
        !h.providers
            .setup_authentication(
                &actor,
                workspace.id(),
                candidate.id(),
                context.occurred_at_unix_nanos()
            )
            .await
            .unwrap()
            .authorized
    );
}
