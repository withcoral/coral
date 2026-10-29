//! Tool Invocation outcomes through real and scripted Programs.
use super::*;

#[tokio::test]
async fn program_tool_settles_after_durable_invocation_completion() {
    let upstream = Arc::new(ScriptedUpstream::json(
        200,
        serde_json::json!({
            "temperature": 21
        }),
    ));
    let fixture = TestModules::with_upstream(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        upstream.clone(),
    )
    .await;
    let tool = fixture.user_tool("user:tool-runner", "Call weather").await;

    let accepted = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("invoke-tool"),
        )
        .await
        .expect("accept Program that invokes one Tool");
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;

    assert!(matches!(
        terminal.terminal_outcome(),
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &scripted_http_result(&serde_json::json!({"temperature": 21}))
    ));
    assert!(matches!(
        retained_events(&fixture.runs, &tool.principal, &tool.workspace_id, terminal.run_id()).await.as_slice(),
        [started, invocation_started, invocation_ended, ended]
            if started.kind() == &RunEventKind::Started
                && matches!(invocation_started.kind(), RunEventKind::InvocationStarted { .. })
                && matches!(invocation_ended.kind(), RunEventKind::InvocationEnded { .. })
                && ended.kind() == &RunEventKind::Ended { state: RunState::Succeeded }
    ));
    let first_page = fixture
        .runs
        .inspect_run(
            &tool.principal,
            &tool.workspace_id,
            terminal.run_id(),
            &crate::RunInspection {
                event_limit: 2,
                ..Default::default()
            },
        )
        .await
        .expect("read mixed Run and Invocation Event page");
    assert_eq!(
        first_page
            .events
            .iter()
            .map(RunEvent::sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(first_page.has_more_events);
    let second_page = fixture
        .runs
        .inspect_run(
            &tool.principal,
            &tool.workspace_id,
            terminal.run_id(),
            &crate::RunInspection {
                event_after: first_page.events.last().unwrap().sequence(),
                event_limit: 2,
                ..Default::default()
            },
        )
        .await
        .expect("read remaining mixed Event page");
    assert_eq!(
        second_page
            .events
            .iter()
            .map(RunEvent::sequence)
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
    assert!(!second_page.has_more_events);
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn coral_failure_is_coded_and_durable_before_program_continues() {
    let fixture = TestModules::with_upstream(
        Arc::new(
            crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default())
                .expect("V8 engine"),
        ),
        Arc::new(ScriptedUpstream::json(200, serde_json::json!({}))),
    )
    .await;
    let tool = fixture
        .user_tool("user:coral-failure", "Catch a safe failure")
        .await;
    // The message names what to correct, never a submitted value.
    for (argument, message) in [
        (
            r#"{unexpected: "secret-canary"}"#,
            "Tool arguments are invalid: Tool argument `unexpected` is not declared",
        ),
        (
            "null",
            "Tool arguments are invalid: Tool arguments must be an object",
        ),
    ] {
        let accepted = fixture
            .runs
            .start(
                &tool.principal,
                &tool.workspace_id,
                submission(
                    &r#"
            try { await coral.providers.weather.getForecast(ARGUMENT); }
            catch (error) {
                if (!(error instanceof coral.ToolError)) throw error;
                await coral.emit({code: error.code, message: error.message});
                return "handled";
            }
        "#
                    .replace("ARGUMENT", argument),
                ),
            )
            .await
            .expect("accept Program");
        let terminal = wait_for_terminal(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
        )
        .await;
        assert_eq!(terminal.state(), RunState::Succeeded);
        assert!(
            matches!(retained_events(&fixture.runs, &tool.principal, &tool.workspace_id, terminal.run_id()).await.as_slice(), [_, started, ended, emission, _]
                if matches!(started.kind(), RunEventKind::InvocationStarted { .. })
                && matches!(ended.kind(), RunEventKind::InvocationEnded { result_kind: crate::InvocationResultKind::CoralFailure, .. })
                && matches!(emission.kind(), RunEventKind::ProgramEmitted { value } if value.as_json() == &serde_json::json!({
                    "code": "invalid_input", "message": message
                }))
            ),
            "{terminal:?}"
        );
    }
}

/// A Workspace whose Owner has just installed the Built-in Provider.
async fn builtin_scope(
    fixture: &TestModules,
    user: &str,
) -> (coral_workspace::WorkspaceId, AuthenticatedPrincipal) {
    let (workspace_id, owner) = fixture.user_scope(user, "Call Coral admin").await;
    let owner_user = owner.authenticated_user().expect("User").clone();
    let mut transaction = fixture.database.begin().await.unwrap();
    fixture
        .providers
        .install_builtin_in_transaction(
            &mut transaction,
            &change(owner_user, 11, '2'),
            &workspace_id,
        )
        .await
        .expect("install the Built-in Provider");
    transaction.commit().await.unwrap();
    (workspace_id, owner)
}

#[tokio::test]
async fn builtin_add_provider_waits_for_approval_by_default() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let (workspace_id, owner) = builtin_scope(&fixture, "user:builtin-policy").await;
    let mut transaction = fixture.database.begin().await.unwrap();
    let target = coral_provider::resolve_invocation_tool_in_transaction(
        &mut transaction,
        &workspace_id,
        coral_provider::InvocationCaller::new(&fixture.workspaces, &owner, 12),
        "coral.workspace.addProvider",
    )
    .await
    .unwrap()
    .expect("the Built-in Tool resolves");
    let decision = coral_policy::invocation_decision_in_transaction(
        &mut transaction,
        &ToolPolicyTarget::new(
            workspace_id.clone(),
            target.provider_access().id().clone(),
            target.tool().key().clone(),
        ),
        target.tool().hints(),
        target.tool().http_method(),
        coral_db::RowLock::Shared,
    )
    .await
    .unwrap();
    transaction.rollback().await.unwrap();
    // addProvider declares no read-only hint, so it takes the cautious default.
    assert_eq!(decision.access_state(), ToolAccessState::ApprovalRequired);
}

#[tokio::test]
async fn program_calls_a_provider_named_workspace_at_its_owner_call_root() {
    let fixture = TestModules::with_upstream(
        Arc::new(
            crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default())
                .expect("V8 engine"),
        ),
        Arc::new(ScriptedUpstream::json(
            200,
            serde_json::json!({"temperature": 21}),
        )),
    )
    .await;
    let tool = fixture
        .user_tool_under(
            "user:workspace-namespace",
            "Call the workspace Provider",
            "https://upstream.example",
            "workspace",
        )
        .await;
    let accepted = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("return (await coral.providers.workspace.getForecast()).data.temperature;"),
        )
        .await
        .expect("accept Program");
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert!(
        matches!(
            terminal.terminal_outcome(),
            Some(RunTerminalOutcome::Succeeded {
                final_value: FinalValue::Present(value)
            }) if value.as_json() == &serde_json::json!(21)
        ),
        "{terminal:?}"
    );
}

#[tokio::test]
async fn terminal_persistence_failure_settles_neither_invocation_promise_nor_run() {
    let upstream = Arc::new(ScriptedUpstream::json(
        200,
        serde_json::json!({"temperature": 21}),
    ));
    let observer = Arc::new(TerminalCommitFault::default());
    let engine = Arc::new(CompletionAttemptingEngine::new(
        "coral.providers.weather.getForecast",
    ));
    let fixture =
        TestModules::with_invocation_adapters(engine.clone(), upstream.clone(), observer.clone())
            .await;
    let tool = fixture
        .user_tool("user:persistence-fault", "Lose terminal commit")
        .await;
    let accepted = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("invoke-tool"),
        )
        .await
        .expect("accept Program");
    wait_until(|| observer.attempted.load(Ordering::SeqCst)).await;
    wait_until(|| engine.finished.load(Ordering::SeqCst)).await;
    wait_until(|| observer.completion_checked.load(Ordering::SeqCst)).await;

    let update = fixture
        .runs
        .observe(
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .expect("observe interrupted execution");

    assert_eq!(update.state(), RunState::Running);
    assert!(matches!(
        retained_events(&fixture.runs, &tool.principal, &tool.workspace_id, update.run_id()).await.as_slice(),
        [started, invocation_started]
            if started.kind() == &RunEventKind::Started
                && matches!(invocation_started.kind(), RunEventKind::InvocationStarted { .. })
    ));
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fatal_async_tool_stops_noncooperative_program_code() {
    let scripted =
        Arc::new(ScriptedUpstream::json(200, serde_json::json!({}))) as Arc<dyn UpstreamSender>;
    let valid_call =
        "coral.providers.weather.getForecast().catch(() => coral.emit('caught')); while (true) {}";
    let invalid_call = "coral.providers.weather.getForecast({unexpected:'secret-canary'}).catch(() => coral.emit('caught')); while (true) {}";
    for (upstream, source) in [
        (scripted.clone(), valid_call),
        (scripted, invalid_call),
        (Arc::new(DefectiveUpstream { panic: true }), valid_call),
        (Arc::new(DefectiveUpstream { panic: false }), valid_call),
    ] {
        let fixture = TestModules::with_invocation_adapters(
            Arc::new(
                crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default())
                    .expect("V8 engine"),
            ),
            upstream,
            Arc::new(TerminalCommitFault::default()),
        )
        .await;
        let tool = fixture
            .user_tool("user:async-host-fatal", "Stop after host failure")
            .await;
        let accepted = fixture
            .runs
            .start(&tool.principal, &tool.workspace_id, submission(source))
            .await
            .expect("accept Program");
        let terminal = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let update = fixture
                    .runs
                    .observe(
                        &tool.principal,
                        &tool.workspace_id,
                        accepted.run_id(),
                        &immediate(None, 100),
                    )
                    .await
                    .expect("observe");
                if update.state().is_terminal() {
                    break update;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        if terminal.is_err() {
            fixture
                .runs
                .cancel(
                    &tool.principal,
                    &tool.workspace_id,
                    accepted.run_id(),
                    &immediate(None, 100),
                )
                .await
                .expect("stop regression before runtime shutdown");
        }
        let terminal =
            terminal.expect("host failure must interrupt V8 without waiting for the Run deadline");
        assert_eq!(terminal.state(), RunState::Interrupted);
        assert!(
            !terminal
                .events()
                .iter()
                .any(|event| matches!(event.kind(), RunEventKind::ProgramEmitted { .. }))
        );
    }
}

#[tokio::test]
async fn known_incomplete_response_is_a_durable_coral_failure() {
    let upstream = Arc::new(FailingUpstream::default());
    let fixture = TestModules::with_upstream(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        upstream.clone(),
    )
    .await;
    let tool = fixture
        .user_tool("user:incomplete-response", "Observe an incomplete response")
        .await;
    let accepted = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("invoke-tool"),
        )
        .await
        .expect("accept Program");
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;

    assert_eq!(terminal.state(), RunState::Failed);
    assert!(
        retained_events(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            terminal.run_id()
        )
        .await
        .iter()
        .any(|event| matches!(
            event.kind(),
            RunEventKind::InvocationEnded {
                result_kind: crate::InvocationResultKind::CoralFailure,
                ..
            }
        ))
    );
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn upstream_send_failures_keep_their_cause_in_the_coral_failure() {
    use crate::invocation::UpstreamSendError;
    type SendFailure = fn() -> UpstreamSendError;
    let cases: [(SendFailure, &str, &str); 4] = [
        (
            || UpstreamSendError::Rejected(coral_upstream::McpRejection::InvalidParams),
            "invalid_input",
            "Tool arguments are invalid",
        ),
        (
            || UpstreamSendError::Rejected(coral_upstream::McpRejection::UnknownTool),
            "tool_unavailable",
            "Tool is unavailable",
        ),
        (
            || UpstreamSendError::Rejected(coral_upstream::McpRejection::Other(-32603)),
            "upstream_rejected",
            "Upstream rejected the request",
        ),
        (
            || UpstreamSendError::Incomplete,
            "upstream_incomplete",
            "Upstream result is incomplete",
        ),
    ];
    for (failure, code, message) in cases {
        let upstream = Arc::new(FailingUpstream::new(failure));
        let fixture = TestModules::with_upstream(
            Arc::new(
                crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default())
                    .expect("V8 engine"),
            ),
            upstream.clone(),
        )
        .await;
        let tool = fixture
            .user_tool("user:send-failure", "Catch an upstream send failure")
            .await;
        let accepted = fixture
            .runs
            .start(
                &tool.principal,
                &tool.workspace_id,
                submission(
                    r#"
            try { await coral.providers.weather.getForecast({}); }
            catch (error) {
                if (!(error instanceof coral.ToolError)) throw error;
                return {code: error.code, message: error.message};
            }
            return "resolved";
        "#,
                ),
            )
            .await
            .expect("accept Program");
        let terminal = wait_for_terminal(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
        )
        .await;
        assert_eq!(
            terminal.state(),
            RunState::Succeeded,
            "{code}: {terminal:?}"
        );
        assert!(
            matches!(
                terminal.terminal_outcome(),
                Some(RunTerminalOutcome::Succeeded { final_value: FinalValue::Present(value) })
                    if value.as_json() == &serde_json::json!({"code": code, "message": message})
            ),
            "{code}: {terminal:?}"
        );
        assert!(
            retained_events(
                &fixture.runs,
                &tool.principal,
                &tool.workspace_id,
                terminal.run_id()
            )
            .await
            .iter()
            .any(|event| matches!(
                event.kind(),
                RunEventKind::InvocationEnded {
                    result_kind: crate::InvocationResultKind::CoralFailure,
                    ..
                }
            )),
            "{code}"
        );
        assert_eq!(upstream.sends.load(Ordering::SeqCst), 1, "{code}");
    }
}

#[tokio::test]
async fn unknown_call_path_creates_no_invocation_or_send() {
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::Value::Null));
    let fixture = TestModules::with_upstream(
        Arc::new(InvokingEngine::new("coral.providers.weather.notPublished")),
        upstream.clone(),
    )
    .await;
    let tool = fixture
        .user_tool("user:unknown-tool", "Call unknown Tool")
        .await;
    let accepted = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("invoke-tool"),
        )
        .await
        .expect("accept Program before host resolution");
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;

    assert_eq!(terminal.state(), RunState::Failed);
    assert!(matches!(
        retained_events(&fixture.runs, &tool.principal, &tool.workspace_id, terminal.run_id()).await.as_slice(),
        [started, ended]
            if started.kind() == &RunEventKind::Started
                && ended.kind() == &RunEventKind::Ended { state: RunState::Failed }
    ));
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn real_program_dispatches_concurrent_tools_and_waits_for_both() {
    let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
        200,
        serde_json::json!({"temperature": 21}),
    )));
    let fixture = TestModules::with_upstream(
        Arc::new(
            crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default())
                .expect("V8 engine"),
        ),
        upstream.clone(),
    )
    .await;
    let tool = fixture
        .user_tool("user:parallel-program", "Concurrent Tools")
        .await;
    let accepted = fixture.runs.start(
        &tool.principal,
        &tool.workspace_id,
        submission("const results = await Promise.all([coral.providers.weather.getForecast(), coral.providers.weather.getForecast()]); return results.length;"),
    ).await.expect("accept Program");
    let concurrent = tokio::time::timeout(Duration::from_secs(2), async {
        while upstream.sends.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    // Release even on failure so the regression cannot strand runtime shutdown.
    upstream.release.store(true, Ordering::SeqCst);
    concurrent.expect("both Tools dispatch before either returns");
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert_eq!(terminal.state(), RunState::Succeeded);
    assert_eq!(
        retained_events(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            terminal.run_id()
        )
        .await
        .iter()
        .filter(|event| matches!(event.kind(), RunEventKind::InvocationEnded { .. }))
        .count(),
        2
    );
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded { final_value: FinalValue::Present(value) }) if value.as_json() == &serde_json::json!(2))
    );
}

#[tokio::test]
async fn upstream_send_holds_no_database_transaction_or_lock() {
    let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
        200,
        serde_json::json!({"temperature": 21}),
    )));
    let fixture = TestModules::with_upstream(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        upstream.clone(),
    )
    .await;
    let tool = fixture
        .user_tool("user:unlocked-send", "Send without database lock")
        .await;
    let accepted = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("invoke-tool"),
        )
        .await
        .expect("accept Program");
    wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;

    tokio::time::timeout(
        Duration::from_secs(2),
        fixture.policies.set_access_state(
            &change(tool.user.clone(), 100, 'c'),
            tool.policy_target.clone(),
            ToolAccessState::Disabled,
        ),
    )
    .await
    .expect("policy change must not wait for an upstream-held database lock")
    .expect("post-fence policy change commits");
    upstream.release.store(true, Ordering::SeqCst);

    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert_eq!(terminal.state(), RunState::Succeeded);
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
}

/// Runs `source` against one real `getForecast` Tool at `server`, then
/// reads back its terminal outcome and its only Invocation.
async fn run_real_forecast(
    user_id: &str,
    server: &MockServer,
    source: &str,
) -> (RunTerminalOutcome, crate::InvocationDetail) {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture.live_runs(None);
    let tool = fixture
        .user_tool_at(user_id, "Call real weather", &server.uri())
        .await;
    let accepted = runs
        .start(&tool.principal, &tool.workspace_id, submission(source))
        .await
        .expect("accept real Tool Program");
    let terminal = wait_for_terminal(
        &runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    let history = runs
        .activity_entries(
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
            &crate::RunEntryWindow::Start,
            100,
            None,
        )
        .await
        .expect("read Run activity");
    let Some(crate::RunEntry::Invocation(summary)) = history.entries.first() else {
        panic!("tool summary")
    };
    let detail = runs
        .inspect_invocation(&tool.principal, &tool.workspace_id, &summary.id)
        .await
        .expect("inspect Invocation");
    (
        terminal
            .terminal_outcome()
            .expect("terminal outcome")
            .clone(),
        detail,
    )
}

/// Runs a Program that returns one real `getForecast` Result from `server`,
/// then reads back its durable Final Value and its only Invocation.
async fn real_forecast(
    user_id: &str,
    server: &MockServer,
) -> (serde_json::Value, crate::InvocationDetail) {
    let (outcome, detail) = run_real_forecast(
        user_id,
        server,
        "return await coral.providers.weather.getForecast({ city: 'London' });",
    )
    .await;
    let RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value),
    } = outcome
    else {
        panic!("real HTTP Tool should produce a Final Value: {outcome:?}");
    };
    (value.as_json().clone(), detail)
}

async fn rejecting_forecast_server() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/forecast"))
        .and(query_param("city", "London"))
        .respond_with(ResponseTemplate::new(422).set_body_json(serde_json::json!({
            "temperature": 18
        })))
        .expect(1)
        .mount(&server)
        .await;
    server
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_success_http_outcome_rejects_the_call_and_is_retained() {
    let server = rejecting_forecast_server().await;

    let (outcome, detail) = run_real_forecast(
        "user:http-runner",
        &server,
        "return await coral.providers.weather.getForecast({ city: 'London' });",
    )
    .await;

    let RunTerminalOutcome::Failed(failure) = &outcome else {
        panic!("an uncaught upstream error fails the Run: {outcome:?}");
    };
    assert_eq!(failure.code(), crate::RunFailureCode::ProgramRejected);
    let tool = failure
        .error()
        .and_then(|error| error.tool.as_ref())
        .expect("trusted Tool error fields");
    assert_eq!(tool.code, "error_response");
    assert_eq!(
        detail.summary.outcome,
        Some(crate::InvocationResultKind::UpstreamOutcome)
    );
    assert_eq!(detail.arguments, serde_json::json!({"city": "London"}));
    let Some(crate::InvocationOutcome::UpstreamOutcome(retained)) = &detail.outcome else {
        panic!("the complete upstream outcome stays in history")
    };
    assert_eq!(retained["status"], 422);
    assert_eq!(retained["data"], serde_json::json!({"temperature": 18}));
    let metadata = &retained["metadata"];
    assert_eq!(metadata["kind"], "http");
    assert_eq!(metadata.get("status"), None);
    assert_eq!(metadata["mediaType"], "application/json");
    assert_eq!(metadata["charset"], "utf-8");
    assert!(
        metadata["finalUrl"]
            .as_str()
            .is_some_and(|url| { url.ends_with("/forecast") && !url.contains("city=London") })
    );
    assert_eq!(metadata["redirects"], serde_json::json!([]));
    assert_eq!(metadata["decodeDiagnostics"], serde_json::json!([]));
    assert!(metadata["headers"].as_array().is_some_and(|headers| {
        headers
            .iter()
            .any(|header| header == &serde_json::json!(["content-type", "application/json"]))
    }));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_caught_error_response_carries_status_data_and_metadata() {
    let server = rejecting_forecast_server().await;

    let (outcome, _detail) = run_real_forecast(
        "user:http-catcher",
        &server,
        r#"
        try {
          await coral.providers.weather.getForecast({ city: 'London' });
          return "returned";
        } catch (error) {
          return {
            isToolError: error instanceof coral.ToolError,
            code: error.code,
            status: error.status,
            data: error.data,
            kind: error.metadata.kind,
            mediaType: error.metadata.mediaType,
          };
        }
        "#,
    )
    .await;

    let RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value),
    } = outcome
    else {
        panic!("a caught upstream error lets the Run succeed: {outcome:?}");
    };
    assert_eq!(
        value.as_json(),
        &serde_json::json!({
            "isToolError": true,
            "code": "error_response",
            "status": 422,
            "data": {"temperature": 18},
            "kind": "http",
            "mediaType": "application/json",
        })
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn real_program_retains_no_sensitive_response_header_value() {
    let sent = [
        ("Set-Cookie", "session=canary-one"),
        ("Set-Cookie", "refresh=canary-two"),
        ("Authorization", "Bearer canary-authorization"),
        ("Proxy-Authorization", "Basic canary-proxy"),
        ("WWW-Authenticate", r#"Bearer realm="canary-realm""#),
        ("X-API-Key", "canary-api-key"),
        (
            "Link",
            "</forecast?page=2&access_token=canary-link>; rel=next",
        ),
        ("X-Quota", "first"),
        ("X-Quota", "second"),
    ];
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/forecast"))
        .respond_with(sent.iter().fold(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"temperature": 18})),
            |response, (name, value)| response.append_header(*name, *value),
        ))
        .mount(&server)
        .await;

    let (value, detail) = real_forecast("user:http-headers", &server).await;
    // A declared success carries its status beside its data.
    assert_eq!(value.get("status"), Some(&serde_json::json!(200)));
    assert_eq!(
        value.get("data"),
        Some(&serde_json::json!({"temperature": 18}))
    );

    let Some(crate::InvocationOutcome::UpstreamOutcome(retained)) = &detail.outcome else {
        panic!("retained upstream outcome")
    };
    // Both are read back from their durable record.
    for result in [&value, retained] {
        assert!(!result.to_string().contains("canary"), "{result}");
        let headers = result
            .pointer("/metadata/headers")
            .and_then(serde_json::Value::as_array)
            .expect("retained headers")
            .iter()
            .filter_map(|header| Some((header.get(0)?.as_str()?, header.get(1)?.as_str()?)))
            .filter(|(name, _value)| {
                sent.iter()
                    .any(|(sent, _value)| sent.eq_ignore_ascii_case(name))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            headers,
            [
                ("set-cookie", "[redacted]"),
                ("set-cookie", "[redacted]"),
                ("authorization", "[redacted]"),
                ("proxy-authorization", "[redacted]"),
                ("www-authenticate", "[redacted]"),
                ("x-api-key", "[redacted]"),
                (
                    "link",
                    "</forecast?page=2&access_token=[redacted]>; rel=next"
                ),
                ("x-quota", "first"),
                ("x-quota", "second"),
            ]
        );
    }
}

/// The production sender never launches a request for a stop that is already
/// set: the peer receives nothing and the dispatch ends `Canceled`.
#[tokio::test]
async fn a_stop_already_set_never_launches_the_upstream_send() {
    let sink = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::any())
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(0)
        .mount(&sink)
        .await;
    let policy = coral_outbound::DestinationPolicy::default()
        .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps);
    let document = serde_json::json!({
        "openapi": "3.1.0", "info": {"title": "Weather", "version": "1"},
        "servers": [{"url": sink.uri()}],
        "paths": {"/forecast": {"get": {
            "operationId": "getForecast",
            "responses": {"200": {"description": "Forecast"}}
        }}}
    });
    let input = coral_catalog::ProviderSetupInput::from(
        coral_catalog::OpenApiDocument::parse(&document.to_string()).expect("OpenAPI fixture"),
    );
    let prepared = match coral_catalog::materialize(policy, &input, &[]) {
        coral_catalog::MaterializationOutcome::Ready(prepared) => prepared,
        coral_catalog::MaterializationOutcome::Rejected(diagnostic) => {
            panic!("fixture should materialize: {}", diagnostic.message())
        }
    };
    let ready = coral_upstream::ReadyHttpDispatch::plan(
        prepared.tools().first().expect("Tool"),
        prepared.schema_graph(),
        &serde_json::json!({}),
        coral_value::ValueLimits::default(),
    )
    .expect("plan the request");
    let upstream = crate::invocation::ProductionUpstream::new(policy).expect("HTTP Adapter");
    let stop = Arc::new(ExecutionStop::new());
    stop.request();

    let outcome = upstream
        .send(coral_upstream::ReadyDispatch::Http(Box::new(ready)), stop)
        .await;
    assert!(
        matches!(outcome, Err(crate::invocation::UpstreamSendError::Canceled)),
        "a set stop cancels before the send"
    );
    assert!(
        sink.received_requests()
            .await
            .expect("recording sink")
            .is_empty(),
        "no request reaches the peer"
    );
}

#[tokio::test]
async fn blocked_destination_is_a_durable_catchable_tool_error() {
    let sink = wiremock::MockServer::start().await;
    let fixture =
        TestModules::with_upstream(
            Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
            Arc::new(
                crate::invocation::ProductionUpstream::new(
                    coral_outbound::DestinationPolicy::default(),
                )
                .unwrap(),
            ),
        )
        .await;
    let tool = fixture
        .user_tool_at(
            "user:blocked-destination",
            "Observe destination rejection",
            &sink.uri(),
        )
        .await;
    let accepted = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission(
                r#"
        try { await coral.providers.weather.getForecast({city: "canary"}); }
        catch (error) {
            if (!(error instanceof coral.ToolError)) throw error;
            await coral.emit({code: error.code, message: error.message});
            return "handled";
        }
    "#,
            ),
        )
        .await
        .unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert_eq!(terminal.state(), RunState::Succeeded);
    assert!(
        retained_events(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            terminal.run_id()
        )
        .await
        .iter()
        .any(|event| matches!(
            event.kind(),
            RunEventKind::InvocationEnded {
                result_kind: crate::InvocationResultKind::CoralFailure,
                ..
            }
        ))
    );
    assert!(terminal.events().iter().any(|event| matches!(event.kind(),
    RunEventKind::ProgramEmitted { value } if value.as_json() == &serde_json::json!({
        "code": "outbound_destination_rejected", "message": "Provider destination is not permitted"
    }))));
    assert!(sink.received_requests().await.unwrap().is_empty());
}
