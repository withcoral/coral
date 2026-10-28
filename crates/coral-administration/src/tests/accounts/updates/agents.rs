use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn agent_removal_and_owner_publication_never_restore_the_deleted_agents_account() {
    for shared in [false, true] {
        for removal_first in [false, true] {
            Box::pin(agent_update(shared, removal_first)).await;
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "both commit orders preserve owner publication but retire the exact Agent, pending replacement and retained credential evidence"
)]
async fn agent_update(shared: bool, removal_first: bool) {
    let mut f = AccountFixture::new().await;
    let context = change(&f.member);
    let issued = f
        .workspaces
        .create_agent(
            &context,
            f.workspace.id(),
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = f
        .workspaces
        .authenticate_agent(f.workspace.id(), issued.credential.expose(), 100)
        .await
        .unwrap();
    let initial = f
        .accounts
        .start_agent_authorization(
            &context,
            f.workspace.id(),
            agent.principal().id(),
            &f.provider,
            request(),
            None,
        )
        .await
        .unwrap();
    let connected = f
        .accounts
        .submit_secret(
            &context,
            f.workspace.id(),
            initial.id,
            coral_credentials::CredentialValues::token(Secret::new("agent-secret".into()).unwrap()),
        )
        .await
        .unwrap();
    let pending = f
        .accounts
        .start_reconnect(
            &context,
            f.workspace.id(),
            &f.provider,
            connected.account_id.unwrap(),
            request(),
            None,
        )
        .await
        .unwrap();
    let database = f.db.database();
    let mut tx = database.begin().await.unwrap();
    let target = coral_provider::resolve_owner_tool_in_transaction(
        &mut tx,
        f.workspace.id(),
        "coral.providers.private.value",
    )
    .await
    .unwrap()
    .unwrap();
    let evidence =
        coral_provider::account::select_in_transaction(&mut tx, &target, agent.principal())
            .await
            .unwrap()
            .unwrap();
    tx.commit().await.unwrap();
    let providers = ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default()
            .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        database,
        f.workspaces.clone(),
    );
    let update = candidate(&f, &providers, shared, "candidate-secret").await;
    let result = if removal_first {
        let gate = Arc::new(RemovalGate::default());
        f.lifecycle.pause_after_prepare = Some(gate.clone());
        let removal = {
            let lifecycle = f.lifecycle.clone();
            let context = context.clone();
            let workspace = f.workspace.id().clone();
            let id = agent.principal().id().clone();
            tokio::spawn(async move { lifecycle.delete_agent(&context, &workspace, &id).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), gate.prepared.notified())
            .await
            .unwrap();
        let (result, ()) = tokio::join!(biased;
            providers.regenerate_provider(&context, update.clone()),
            async { gate.proceed.notify_one(); },
        );
        removal.await.unwrap().unwrap();
        result.unwrap()
    } else {
        let result = providers
            .regenerate_provider(&context, update.clone())
            .await
            .unwrap();
        f.lifecycle
            .delete_agent(&context, f.workspace.id(), agent.principal().id())
            .await
            .unwrap();
        result
    };
    assert_eq!(result.outcome(), ProviderPublicationOutcome::Succeeded);
    let before = providers
        .inspect_provider(&f.member, f.workspace.id(), &f.provider)
        .await
        .unwrap();
    assert_eq!(
        before.access().credential_route(),
        if shared {
            CredentialRoute::Fixed
        } else {
            CredentialRoute::Caller
        }
    );
    f.workspaces
        .authenticate_agent(f.workspace.id(), issued.credential.expose(), 100)
        .await
        .unwrap_err();
    f.workspaces
        .check_run_admission(&agent, f.workspace.id(), 100)
        .await
        .unwrap_err();
    f.accounts
        .credentials(f.workspace.id(), &f.provider, &evidence, 100)
        .await
        .unwrap_err();
    let events = providers
        .list_lifecycle_events(&f.member, f.workspace.id())
        .await
        .unwrap();
    // A late callback returns its terminal cancellation; neither it nor
    // committed Publication replay can revive the deleted Agent's account.
    assert_eq!(
        f.accounts
            .submit_secret(
                &context,
                f.workspace.id(),
                pending.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("late-agent-secret".into()).unwrap()
                ),
            )
            .await
            .unwrap()
            .state,
        AuthorizationState::Canceled
    );
    assert_eq!(
        providers
            .regenerate_provider(&context, update)
            .await
            .unwrap(),
        result
    );
    assert_eq!(
        providers
            .inspect_provider(&f.member, f.workspace.id(), &f.provider)
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        providers
            .list_lifecycle_events(&f.member, f.workspace.id())
            .await
            .unwrap(),
        events
    );
    if shared {
        let evidence = credential_evidence(&f.db.database(), f.workspace.id(), &f.member).await;
        assert_eq!(
            f.accounts
                .credentials(f.workspace.id(), &f.provider, &evidence, 100)
                .await
                .unwrap()
                .get(coral_credentials::CredentialValues::TOKEN)
                .unwrap()
                .expose(),
            "candidate-secret"
        );
    } else {
        assert!(
            f.accounts
                .get_account(&context, f.workspace.id(), &f.provider, &AccountScope::User)
                .await
                .unwrap()
                .account
                .is_none()
        );
    }
}
