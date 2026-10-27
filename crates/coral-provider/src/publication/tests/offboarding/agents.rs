use super::*;

#[tokio::test]
async fn agent_removal_after_owner_update_preflight_allows_publication_without_restoring_account() {
    for shared in [false, true] {
        Box::pin(agent_removal(shared)).await;
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "the surviving Owner publishes across Agent cleanup; late callbacks and replay must retain its fences"
)]
async fn agent_removal(shared: bool) {
    let h = harness().await;
    let (owner, workspace) = workspace_owner(&h, "user:agent-publisher").await;
    let context = change_context(owner.clone(), 10, '1');
    let cleanup = change_context(owner.clone(), 30, '3');
    let provider = publish_provider(
        &h.providers,
        &context,
        workspace.id(),
        CREDENTIAL_REQUIRED_OPENAPI,
        "updated",
        CredentialRoute::Caller,
    )
    .await;
    let issued = h
        .workspaces
        .create_agent(
            &context,
            workspace.id(),
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = h
        .workspaces
        .authenticate_agent(workspace.id(), issued.credential.expose(), 11)
        .await
        .unwrap();
    let initial = h
        .accounts
        .start_agent_authorization(
            &context,
            workspace.id(),
            agent.principal().id(),
            provider.id(),
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let connected = h
        .accounts
        .submit_secret(
            &context,
            workspace.id(),
            initial.id,
            coral_credentials::CredentialValues::token(Secret::new("agent-secret".into()).unwrap()),
        )
        .await
        .unwrap()
        .account_id
        .unwrap();
    let pending = h
        .accounts
        .start_reconnect(
            &context,
            workspace.id(),
            provider.id(),
            connected,
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let retained = evidence(&h, workspace.id(), "updated", agent.principal()).await;
    assert_eq!(
        h.accounts
            .credentials(workspace.id(), provider.id(), &retained, 11)
            .await
            .unwrap()
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "agent-secret"
    );
    let update = candidate(&h, &context, workspace.id(), provider.id(), shared).await;
    let before = h
        .providers
        .inspect_provider(&owner, workspace.id(), provider.id())
        .await
        .unwrap();
    let (gate, publishing) = pause_update(&h, &owner, &update).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        AccessLifecycleModule::new(h.database.clone(), h.workspaces.clone()).delete_agent(
            &cleanup,
            workspace.id(),
            agent.principal().id(),
        ),
    )
    .await
    .expect("Agent deletion commits while publication is paused")
    .unwrap();
    assert!(!publishing.is_finished());
    assert_eq!(
        h.providers
            .inspect_provider(&owner, workspace.id(), provider.id())
            .await
            .unwrap(),
        before
    );
    h.accounts
        .credentials(workspace.id(), provider.id(), &retained, 31)
        .await
        .unwrap_err();
    let removed_events = h
        .providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    assert_cleanup_events(&removed_events, &cleanup, connected, pending.id);
    let workspace_events = h
        .workspaces
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    let removed = workspace_events
        .iter()
        .filter(|event| event.action() == coral_workspace::LifecycleAction::AgentDeleted)
        .collect::<Vec<_>>();
    assert_eq!(removed.len(), 1);
    for event in removed {
        assert_eq!(event.trace_id(), cleanup.trace_id());
        assert_eq!(event.occurred_at_unix_nanos(), 30);
    }
    gate.release();
    let result = tokio::time::timeout(Duration::from_secs(5), publishing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.outcome(), ProviderPublicationOutcome::Succeeded);
    let after = h
        .providers
        .inspect_provider(&owner, workspace.id(), provider.id())
        .await
        .unwrap();
    assert_ne!(after, before);
    assert_eq!(
        after.access().credential_route(),
        if shared {
            CredentialRoute::Fixed
        } else {
            CredentialRoute::Caller
        }
    );
    h.providers
        .inspect_tool(
            &owner,
            workspace.id(),
            provider.id(),
            &ToolKey::parse("getUpdatedForecast").unwrap(),
        )
        .await
        .unwrap();
    if shared {
        assert_eq!(
            credential_for(
                &h,
                workspace.id(),
                provider.id(),
                "updated",
                "getUpdatedForecast",
                &user_principal(&owner),
                31
            )
            .await
            .unwrap()
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
            "candidate-secret"
        );
        assert!(
            !h.providers
                .setup_authentication(&owner, workspace.id(), update.setup_id(), 31)
                .await
                .unwrap()
                .authorized
        );
    }
    assert!(
        h.workspaces
            .list_agents(&owner, workspace.id())
            .await
            .unwrap()
            .is_empty()
    );
    h.workspaces
        .authenticate_agent(workspace.id(), issued.credential.expose(), 31)
        .await
        .unwrap_err();
    h.workspaces
        .check_run_admission(&agent, workspace.id(), 31)
        .await
        .unwrap_err();
    h.accounts
        .credentials(workspace.id(), provider.id(), &retained, 31)
        .await
        .unwrap_err();
    let events = h
        .providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    let added = events
        .iter()
        .filter(|event| !removed_events.contains(event))
        .collect::<Vec<_>>();
    assert_eq!(
        added
            .iter()
            .filter(
                |event| event.action() == LifecycleAction::PublicationCompleted
                    && event.outcome() == LifecycleOutcome::Succeeded
            )
            .count(),
        1
    );
    for event in &added {
        assert_eq!(
            event.trace_id(),
            change_context(owner.clone(), 20, '2').trace_id()
        );
    }
    assert_eq!(
        added
            .iter()
            .filter(|event| event.action() == LifecycleAction::AccountConnected)
            .count(),
        usize::from(shared)
    );
    assert_cleanup_events(&events, &cleanup, connected, pending.id);
    assert_eq!(
        h.accounts
            .submit_secret(
                &context,
                workspace.id(),
                pending.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("late-agent-secret".into()).unwrap()
                )
            )
            .await
            .unwrap()
            .state,
        AuthorizationState::Canceled
    );
    assert_eq!(
        h.providers
            .regenerate_provider(&change_context(owner.clone(), 20, '2'), update)
            .await
            .unwrap(),
        result
    );
    assert_eq!(
        h.providers
            .inspect_provider(&owner, workspace.id(), provider.id())
            .await
            .unwrap(),
        after
    );
    assert_eq!(
        h.providers
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap(),
        events
    );
    assert_eq!(
        h.workspaces
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap(),
        workspace_events
    );
    h.accounts
        .credentials(workspace.id(), provider.id(), &retained, 32)
        .await
        .unwrap_err();
}
