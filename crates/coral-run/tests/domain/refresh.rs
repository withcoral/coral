//! Concurrent renewal through Run/Account interfaces and a gated HTTP token endpoint.
use super::*;
use axum::http::StatusCode;
use coral_provider::account::OAuthClient;

async fn assert_waiting(runs: &RunModule, tool: &ToolFixture, run: &crate::RunId) {
    // Observe after the waiting interval, rather than canceling an observation
    // mid-transaction. This test controls token contention, not database cancellation.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let update = runs
        .observe(
            &tool.principal,
            &tool.workspace_id,
            run,
            &immediate(None, 100),
        )
        .await
        .unwrap();
    assert_eq!(
        update.state(),
        RunState::Running,
        "contention waits rather than ending the Invocation"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_waiting_invocations_cancellation_or_deadline_does_not_cancel_the_refresh() {
    for deadline in [false, true] {
        let endpoint = TokenEndpoint::new(StatusCode::OK).await;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/forecast"))
            .and(wiremock::matchers::header(
                "authorization",
                "Bearer renewed-canary",
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"temperature":18})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
        let tool = fixture
            .user_tool_at("user:stopped-refresh", "Stopped waiter", &server.uri())
            .await;
        let (_directory, accounts) = oauth_account(&fixture, &tool, &server, &endpoint.url).await;
        let runs = fixture.live_runs(Some(accounts));
        let source = "return await coral.providers.weather.getForecast({});";
        let first = runs
            .start(&tool.principal, &tool.workspace_id, submission(source))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), endpoint.entered.notified())
            .await
            .unwrap();
        let second = runs
            .start(
                &tool.principal,
                &tool.workspace_id,
                RunSubmission::new(
                    ProgramSource::parse(source).unwrap(),
                    RunTimeout::from_millis(if deadline { 1_000 } else { 60_000 }).unwrap(),
                ),
            )
            .await
            .unwrap();
        assert_waiting(&runs, &tool, second.run_id()).await;
        if !deadline {
            runs.cancel(
                &tool.principal,
                &tool.workspace_id,
                second.run_id(),
                &immediate(None, 100),
            )
            .await
            .unwrap();
        }
        // Production Run termination has a five-second durable shutdown grace.
        // It must settle independently of the still-blocked token exchange.
        let terminal = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let update = runs
                    .observe(
                        &tool.principal,
                        &tool.workspace_id,
                        second.run_id(),
                        &immediate(None, 100),
                    )
                    .await
                    .unwrap();
                if update.state().is_terminal() {
                    break update;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the waiter's termination does not wait for the token endpoint");
        assert_eq!(
            terminal.state(),
            if deadline {
                RunState::TimedOut
            } else {
                RunState::Canceled
            }
        );
        assert!(server.received_requests().await.unwrap().is_empty());
        endpoint.release.notify_one();
        let terminal =
            wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, first.run_id()).await;
        assert_eq!(terminal.state(), RunState::Succeeded, "{terminal:?}");
        assert_eq!(endpoint.exchanges.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "the Run/Account journey verifies both waiters and unchanged replacement state after a late exchange"
)]
async fn reconnect_disconnect_and_setting_changes_block_invocations_waiting_on_a_refresh_claim() {
    for mutation in ["reconnect", "disconnect", "setting"] {
        let endpoint = TokenEndpoint::new(StatusCode::OK).await;
        let server = MockServer::start().await;
        let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
        let tool = fixture
            .user_tool_at("user:enforced-refresh", "Enforced refresh", &server.uri())
            .await;
        let (_directory, accounts) = oauth_account(&fixture, &tool, &server, &endpoint.url).await;
        let context = change(tool.user.clone(), 40, '4');
        let account = accounts
            .get_account(
                &context,
                &tool.workspace_id,
                &tool.provider_id,
                &coral_provider::account::AccountScope::User,
            )
            .await
            .unwrap()
            .account
            .unwrap();
        let runs = fixture.live_runs(None).with_accounts(accounts.clone());
        let source = "try { await coral.providers.weather.getForecast({}); } catch (error) { return error.code; }";
        let first = runs
            .start(&tool.principal, &tool.workspace_id, submission(source))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), endpoint.entered.notified())
            .await
            .unwrap();
        let second = runs
            .start(&tool.principal, &tool.workspace_id, submission(source))
            .await
            .unwrap();
        assert_waiting(&runs, &tool, second.run_id()).await;
        if mutation == "reconnect" {
            let authorization = accounts
                .start_reconnect(
                    &context,
                    &tool.workspace_id,
                    &tool.provider_id,
                    account.id,
                    ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                    Some(
                        OAuthClient::new(
                            "public-client".into(),
                            "https://reef.example/callback".into(),
                        )
                        .unwrap(),
                    ),
                )
                .await
                .unwrap();
            complete_oauth(&accounts, &context, &tool.workspace_id, &authorization).await;
        } else if mutation == "setting" {
            let setup = fixture
                .providers
                .prepare_setup(
                    &context,
                    &tool.workspace_id,
                    coral_provider::ProviderSetupSource::Current,
                    Some(&tool.provider_id),
                )
                .await
                .unwrap();
            let review = fixture
                .providers
                .review_setup_configuration(&tool.user, &tool.workspace_id, setup.id())
                .await
                .unwrap();
            let mut values = review.confirmed.unwrap();
            values.authentication = Some(coral_catalog::AuthenticationConfiguration::None);
            let review = fixture
                .providers
                .confirm_setup_configuration(
                    &context,
                    &tool.workspace_id,
                    setup.id(),
                    review.revision,
                    values,
                )
                .await
                .unwrap();
            assert!(review.errors.is_empty());
            fixture
                .providers
                .regenerate_provider(
                    &context,
                    ProviderRegenerationRequest::new(
                        ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                        tool.workspace_id.clone(),
                        tool.provider_id.clone(),
                        setup.id().clone(),
                        review.revision,
                    )
                    .with_account_setting(CredentialRoute::None)
                    .with_incompatible_configuration_confirmation(),
                )
                .await
                .unwrap();
        } else {
            accounts
                .disconnect(&context, &tool.workspace_id, &tool.provider_id, account.id)
                .await
                .unwrap();
        }
        let winner = accounts
            .get_account(
                &context,
                &tool.workspace_id,
                &tool.provider_id,
                &coral_provider::account::AccountScope::User,
            )
            .await
            .unwrap();
        // The waiter need not wait for the upstream exchange to discover lost authorization.
        let terminal =
            wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, second.run_id()).await;
        assert!(
            matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded { final_value: FinalValue::Present(value) }) if value.as_json() == &serde_json::json!("account_required")),
            "{terminal:?}"
        );
        endpoint.release.notify_one();
        let terminal =
            wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, first.run_id()).await;
        assert!(
            matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded { final_value: FinalValue::Present(value) }) if value.as_json() == &serde_json::json!("account_required")),
            "{terminal:?}"
        );
        assert_eq!(endpoint.exchanges.load(Ordering::SeqCst), 1);
        assert!(server.received_requests().await.unwrap().is_empty());
        assert_eq!(
            accounts
                .get_account(
                    &context,
                    &tool.workspace_id,
                    &tool.provider_id,
                    &coral_provider::account::AccountScope::User
                )
                .await
                .unwrap(),
            winner
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn offboarding_and_rejoin_block_a_late_refresh_without_overwriting_the_new_account() {
    let endpoint = TokenEndpoint::new(StatusCode::OK).await;
    let server = MockServer::start().await;
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let tool = fixture
        .user_tool_at(
            "user:offboarded-refresh",
            "Offboarding refresh",
            &server.uri(),
        )
        .await;
    let (_directory, accounts) = oauth_account(&fixture, &tool, &server, &endpoint.url).await;
    let runs = fixture.live_runs(None).with_accounts(accounts.clone());
    let run = runs.start(&tool.principal, &tool.workspace_id,
        submission("try { await coral.providers.weather.getForecast({}); } catch (error) { return error.code; }")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), endpoint.entered.notified())
        .await
        .unwrap();
    let administrator = fixture.workspace_admin(&tool).await;
    coral_administration::AccessLifecycleModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
    )
    .remove_workspace_member(&administrator, &tool.workspace_id, tool.user.user_id())
    .await
    .unwrap();
    fixture
        .workspaces
        .set_membership(
            &administrator,
            &tool.workspace_id,
            &tool.user,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let context = change(tool.user.clone(), 40, '4');
    let authorization = accounts
        .start_user_authorization(
            &context,
            &tool.workspace_id,
            &tool.provider_id,
            ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
            Some(
                OAuthClient::new(
                    "public-client".into(),
                    "https://reef.example/callback".into(),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    complete_oauth(&accounts, &context, &tool.workspace_id, &authorization).await;
    let fresh = accounts
        .get_account(
            &context,
            &tool.workspace_id,
            &tool.provider_id,
            &coral_provider::account::AccountScope::User,
        )
        .await
        .unwrap();
    endpoint.release.notify_one();
    let terminal =
        wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, run.run_id()).await;
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded { final_value: FinalValue::Present(value) })
        if value.as_json() == &serde_json::json!("account_required")),
        "{terminal:?}"
    );
    assert_eq!(endpoint.exchanges.load(Ordering::SeqCst), 1);
    assert!(server.received_requests().await.unwrap().is_empty());
    assert_eq!(
        accounts
            .get_account(
                &context,
                &tool.workspace_id,
                &tool.provider_id,
                &coral_provider::account::AccountScope::User
            )
            .await
            .unwrap(),
        fresh
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_refresh_settles_waiting_invocations_without_retrying_the_exchange() {
    for (status, code) in [
        (StatusCode::SERVICE_UNAVAILABLE, "credential_refresh_failed"),
        (StatusCode::BAD_REQUEST, "account_required"),
    ] {
        let endpoint = TokenEndpoint::new(status).await;
        let server = MockServer::start().await;
        let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
        let tool = fixture
            .user_tool_at("user:failed-refresh", "Failed refresh", &server.uri())
            .await;
        let (_directory, accounts) = oauth_account(&fixture, &tool, &server, &endpoint.url).await;
        let context = change(tool.user.clone(), 40, '4');
        let before = accounts
            .get_account(
                &context,
                &tool.workspace_id,
                &tool.provider_id,
                &coral_provider::account::AccountScope::User,
            )
            .await
            .unwrap()
            .account
            .unwrap();
        let runs = fixture.live_runs(None).with_accounts(accounts.clone());
        let source = "try { await coral.providers.weather.getForecast({}); } catch (error) { return error.code; }";
        let first = runs
            .start(&tool.principal, &tool.workspace_id, submission(source))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), endpoint.entered.notified())
            .await
            .unwrap();
        let second = runs
            .start(&tool.principal, &tool.workspace_id, submission(source))
            .await
            .unwrap();
        assert_waiting(&runs, &tool, second.run_id()).await;
        endpoint.release.notify_one();
        for run in [first, second] {
            let terminal =
                wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, run.run_id()).await;
            assert!(
                matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded { final_value: FinalValue::Present(value) }) if value.as_json() == &serde_json::json!(code)),
                "{terminal:?}"
            );
            assert!(!format!("{terminal:?}").contains("canary"));
        }
        assert_eq!(endpoint.exchanges.load(Ordering::SeqCst), 1);
        assert!(server.received_requests().await.unwrap().is_empty());
        let after = accounts
            .get_account(
                &context,
                &tool.workspace_id,
                &tool.provider_id,
                &coral_provider::account::AccountScope::User,
            )
            .await
            .unwrap()
            .account
            .unwrap();
        if status == StatusCode::SERVICE_UNAVAILABLE {
            assert_eq!(after, before);
        } else {
            assert_eq!(
                after.state,
                coral_provider::account::AccountState::ReconnectRequired
            );
            assert_eq!(
                after.connected_at_unix_nanos,
                before.connected_at_unix_nanos
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one refresh claim survives a catalog publication and continues both original Invocations"
)]
async fn concurrent_invocations_wait_for_one_refresh_across_catalog_update_and_each_dispatch_once()
{
    let endpoint = TokenEndpoint::new(StatusCode::OK).await;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/forecast"))
        .and(wiremock::matchers::header(
            "authorization",
            "Bearer renewed-canary",
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"temperature":18})),
        )
        .expect(2)
        .mount(&server)
        .await;
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let tool = fixture
        .user_tool_at(
            "user:concurrent-refresh",
            "Concurrent refresh",
            &server.uri(),
        )
        .await;
    let (_directory, accounts) = oauth_account(&fixture, &tool, &server, &endpoint.url).await;
    let runs = fixture.live_runs(Some(accounts));
    let source = "coral.emit('once'); return await coral.providers.weather.getForecast({});";
    let first = runs
        .start(&tool.principal, &tool.workspace_id, submission(source))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), endpoint.entered.notified())
        .await
        .unwrap();
    let second = runs
        .start(&tool.principal, &tool.workspace_id, submission(source))
        .await
        .unwrap();
    assert_waiting(&runs, &tool, second.run_id()).await;
    let context = change(tool.user.clone(), 40, '4');
    let setup = fixture
        .providers
        .prepare_setup(
            &context,
            &tool.workspace_id,
            coral_provider::ProviderSetupSource::Current,
            Some(&tool.provider_id),
        )
        .await
        .unwrap();
    let review = fixture
        .providers
        .review_setup_configuration(&tool.user, &tool.workspace_id, setup.id())
        .await
        .unwrap();
    let mut values = review.confirmed.unwrap();
    values.display_name = "Updated while refreshing".into();
    let review = fixture
        .providers
        .confirm_setup_configuration(
            &context,
            &tool.workspace_id,
            setup.id(),
            review.revision,
            values,
        )
        .await
        .unwrap();
    assert!(review.errors.is_empty());
    fixture
        .providers
        .regenerate_provider(
            &context,
            ProviderRegenerationRequest::new(
                ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                tool.workspace_id.clone(),
                tool.provider_id.clone(),
                setup.id().clone(),
                review.revision,
            ),
        )
        .await
        .unwrap();
    assert_eq!(endpoint.exchanges.load(Ordering::SeqCst), 1);
    assert!(server.received_requests().await.unwrap().is_empty());
    endpoint.release.notify_one();
    for run in [first, second] {
        let terminal =
            wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, run.run_id()).await;
        assert_eq!(terminal.state(), RunState::Succeeded, "{terminal:?}");
        assert!(
            matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded { final_value: FinalValue::Present(value) })
            if value.as_json()["data"] == serde_json::json!({"temperature":18}))
        );
        let mut counts = [0; 4];
        for event in retained_events(
            &runs,
            &tool.principal,
            &tool.workspace_id,
            terminal.run_id(),
        )
        .await
        {
            match event.kind() {
                RunEventKind::Started => counts[0] += 1,
                RunEventKind::InvocationStarted { .. } => counts[1] += 1,
                RunEventKind::InvocationEnded { .. } => counts[2] += 1,
                RunEventKind::ProgramEmitted { .. } => counts[3] += 1,
                _ => {}
            }
        }
        assert_eq!(counts, [1, 1, 1, 1]);
        assert!(!format!("{terminal:?}").contains("canary"));
    }
    assert_eq!(endpoint.exchanges.load(Ordering::SeqCst), 1);
}
