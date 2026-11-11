//! Exact evidence through native private transport, with real owning-domain writes.
use super::{AuthenticatedPrincipal, Fixture, request, wire};
use coral_test_support::provider_setup_fixtures;
use serde_json::json;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

provider_setup_fixtures!(coral_provider);

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one private transport journey verifies original account selection after detachment and resolution without widening Run or Account authority"
)]
async fn historical_account_selection_verifies_the_original_run_reference() {
    let f = Fixture::new().await;
    let providers = coral_provider::ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default(),
        f.database.database(),
        f.workspaces.clone(),
    );
    let context = super::super::change(&f.owner);
    let setup = providers.prepare_openapi_fixture(&context, f.workspace.id(), coral_catalog::OpenApiDocument::parse(&json!({
        "openapi": "3.1.0", "info": {"title": "Evidence", "version": "1"},
        "servers": [{"url": "https://upstream.example"}],
        "components": {"securitySchemes": {"bearer": {"type": "http", "scheme": "bearer"}}},
        "security": [{"bearer": []}],
        "paths": {"/result": {"get": {"operationId": "read", "responses": {"200": {"description": "Native result"}}}}}
    }).to_string()).unwrap()).await.unwrap();
    providers
        .publish_workspace_provider(
            &context,
            coral_provider::WorkspaceProviderPublicationRequest::new(
                coral_provider::ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string())
                    .unwrap(),
                f.workspace.id().clone(),
                setup.id().clone(),
                coral_provider::ProviderNamespace::parse("evidence").unwrap(),
                coral_provider::CredentialRoute::Caller,
            ),
        )
        .await
        .unwrap()
        .skip_search()
        .published_provider()
        .unwrap();
    let run = start(&f, "await coral.providers.evidence.read({});").await;
    let mut client = f.activity();
    let token = f.token(&f.member, "reef");
    let queued = wait_for_action_page(&f, 0, |page| !page.actions.is_empty())
        .await
        .actions
        .remove(0);
    let group = queued.run.clone().unwrap();
    let Some(wire::activity_request_reference::Request::AccountConnection(original)) = &queued
        .action
        .as_ref()
        .unwrap()
        .request
        .as_ref()
        .unwrap()
        .request
    else {
        panic!("connection request")
    };
    cancel(&f, &run).await;
    let selection = wire::InspectRunRequest {
        name: group.name.clone(),
        account_connection: original.clone(),
        ..Default::default()
    };
    let detached = client
        .inspect_run(request(selection.clone(), &token, "reef"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(&detached.account_connection, original);
    assert_eq!(detached.run.unwrap().action_page.unwrap().total, 0);
    let mut accounts = f.accounts();
    accounts
        .resolve_account_connection_request(request(
            wire::ResolveAccountConnectionRequestRequest {
                name: original.clone(),
                decision: Some(
                    wire::resolve_account_connection_request_request::Decision::Decline(()),
                ),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap();
    let resolved = client
        .inspect_run(request(selection.clone(), &token, "reef"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(&resolved.account_connection, original);
    let exact = accounts
        .get_account_connection_request(request(
            wire::GetAccountConnectionRequestRequest {
                name: resolved.account_connection,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(exact.state(), wire::AccountConnectionState::Declined);
    let unrelated = start(&f, "return 42;").await;
    let unassociated = client
        .inspect_run(request(
            wire::InspectRunRequest {
                name: format!("workspaces/{}/runs/{unrelated}", f.workspace.id()),
                ..selection.clone()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(unassociated.account_connection.is_empty());
    let mismatched = client
        .inspect_run(request(
            wire::InspectRunRequest {
                account_connection: original.replace(
                    &f.workspace.id().to_string(),
                    &uuid::Uuid::new_v4().to_string(),
                ),
                ..selection.clone()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(mismatched.account_connection.is_empty());
    assert_eq!(
        client
            .inspect_run(request(selection, &f.token(&f.owner, "reef"), "reef"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
}

#[tokio::test]
async fn successful_terminal_preserves_absent_final_value_versus_explicit_null() {
    let f = Fixture::new().await;
    for (source, expected) in [
        ("return;", None),
        ("coral.emit(null); return null;", Some("null")),
    ] {
        let run = start(&f, source).await;
        let detail = terminal(&f, &run).await;
        let Some(wire::inspect_run_response::Terminal::Succeeded(success)) = detail.terminal else {
            panic!("even undefined is successful terminal evidence")
        };
        assert_eq!(success.final_value_json.as_deref(), expected);
        if expected.is_some() {
            assert!(detail.events.iter().any(|event| matches!(&event.payload,
                Some(wire::activity_run_event::Payload::ProgramEmitted(value)) if value.value_json == "null")));
        }
    }
}

/// coral-run owns what cancellation does to a waiting or dispatched Invocation
/// (`user_and_agent_cancellation_*`). This checks their projection: the queued
/// card, the review, a stale decision, and each typed outcome. What the Run
/// Status Page reads and sends stays out of the spans of its requests.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one Run per Invocation gate projects the card, review, stale decision, and typed outcome"
)]
async fn cancellation_retains_pending_and_dispatched_invocation_distinctions_and_typed_actor() {
    let capture = coral_telemetry::testing::process_spans();
    let f = Fixture::new().await;
    f.sign_in_profiles(&[(&f.member, "name-canary", "email-canary@example.test", true)])
        .await;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"private": "late"}))
                .set_delay(std::time::Duration::from_secs(30)),
        )
        .mount(&server)
        .await;
    let provider = publish_http(&f, &server.uri()).await;
    let source = "// source-canary\nreturn await coral.providers.evidence.read({body: {private: 'argument-canary'}});";
    // HTTP's default approval gate holds the first call before dispatch.
    let pending = start(&f, source).await;
    wait_invocation(&f, &pending, true).await;
    let before = invocation(&f, &pending).await;
    assert_eq!(
        before.invocation.as_ref().unwrap().state(),
        wire::ActivityInvocationState::Pending
    );
    assert!(before.outcome.is_none());
    let approval = before
        .requests
        .iter()
        .find_map(|reference| match &reference.request {
            Some(wire::activity_request_reference::Request::Approval(name)) => Some(name.clone()),
            _ => None,
        })
        .unwrap();
    let mut client = f.activity();
    let member = f.token(&f.member, "reef");
    let queued = client
        .browse_required_actions(request(
            wire::BrowseRequiredActionsRequest::default(),
            &member,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(queued.action_count, 1);
    let count = client
        .count_required_actions(request(
            wire::CountRequiredActionsRequest::default(),
            &member,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(count.action_count, 1);
    let [queued_action] = queued.actions.as_slice() else {
        panic!("one compact action")
    };
    let group = queued_action.run.as_ref().unwrap();
    assert_eq!(group.actor_label, "You");
    assert_eq!(group.workspace_label, f.workspace.display_name().as_str());
    assert!(
        group.intent.is_empty(),
        "standalone work has no invented intent"
    );
    assert!(
        group.action_page.is_none(),
        "the queue lists requests, not Runs"
    );
    let action = queued_action.action.as_ref().unwrap();
    assert_eq!(action.call_path, "coral.providers.evidence.read");
    assert_eq!(action.provider_label, "Evidence");
    assert_eq!(
        action.request.as_ref().unwrap().request,
        Some(wire::activity_request_reference::Request::Approval(
            approval.clone()
        ))
    );
    assert!(!String::from_utf8_lossy(&prost::Message::encode_to_vec(&queued)).contains("argument"));
    for (decision, token, code) in [
        ("approve", f.token(&f.owner, "reef"), tonic::Code::NotFound),
        ("maybe", member.clone(), tonic::Code::InvalidArgument),
    ] {
        let denied = client
            .decide_approval(request(
                wire::DecideApprovalRequest {
                    name: approval.clone(),
                    decision: decision.into(),
                    comment: None,
                },
                &token,
                "reef",
            ))
            .await
            .unwrap_err();
        assert_eq!(denied.code(), code, "{decision}");
    }
    let review = client
        .inspect_approval(request(
            wire::InspectApprovalRequest {
                name: approval.clone(),
            },
            &member,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(review.actionable, "a denied decision changed nothing");
    assert_eq!(review.provider_label, "Evidence");
    assert_eq!(review.actor_label, "You");
    assert!(review.approver_label.is_empty());
    assert_eq!(review.deadline_at_unix_nanos, action.deadline_at_unix_nanos);
    assert!(review.arguments_json.contains("argument"));
    cancel(&f, &pending).await;
    let stale = client
        .decide_approval(request(
            wire::DecideApprovalRequest {
                name: approval.clone(),
                decision: "approve".into(),
                comment: Some("comment-canary".into()),
            },
            &member,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stale.state, "closed");
    let detail = terminal(&f, &pending).await;
    assert!(matches!(
        detail.terminal,
        Some(wire::inspect_run_response::Terminal::Canceled(_))
    ));
    assert!(detail.events.iter().any(|event| matches!(&event.payload,
        Some(wire::activity_run_event::Payload::CancellationRequested(cancel))
            if cancel.actor == f.member.user_id().as_str() && cancel.actor_kind() == wire::ActivityPrincipalKind::User)));
    let exact = invocation(&f, &pending).await;
    assert!(matches!(
        exact.outcome,
        Some(wire::inspect_invocation_response::Outcome::RunTerminated(_))
    ));
    assert!(
        exact
            .requests
            .iter()
            .any(|reference| matches!(&reference.request,
        Some(wire::activity_request_reference::Request::Approval(name)) if name == &approval))
    );

    policy(&f, &provider, coral_policy::ToolAccessState::Enabled).await;
    let dispatched = start(&f, source).await;
    wait_invocation(&f, &dispatched, false).await;
    let before = invocation(&f, &dispatched).await;
    assert_eq!(
        before.invocation.as_ref().unwrap().state(),
        wire::ActivityInvocationState::Dispatching
    );
    assert!(before.outcome.is_none());
    cancel(&f, &dispatched).await;
    terminal(&f, &dispatched).await;
    let exact = invocation(&f, &dispatched).await;
    assert!(matches!(
        exact.outcome,
        Some(wire::inspect_invocation_response::Outcome::Indeterminate(_))
    ));
    assert_eq!(
        exact.invocation.unwrap().outcome(),
        wire::ActivityInvocationOutcome::Indeterminate
    );

    let spans = format!("{:?}", capture.finished());
    assert!(spans.contains("ActivityService/DecideApproval"), "{spans}");
    assert!(spans.contains("ActivityService/InspectApproval"), "{spans}");
    for private in [
        "source-canary",
        "argument-canary",
        "comment-canary",
        "name-canary",
        "email-canary",
        member.as_str(),
    ] {
        assert!(!spans.contains(private), "{private}: {spans}");
    }
}

async fn cancel(f: &Fixture, run: &coral_run::RunId) {
    f.activity()
        .cancel_activity_run(request(
            wire::CancelActivityRunRequest {
                name: format!("workspaces/{}/runs/{run}", f.workspace.id()),
            },
            &f.token(&f.member, "reef"),
            "reef",
        ))
        .await
        .unwrap();
}

async fn wait_invocation(f: &Fixture, run: &coral_run::RunId, requires_action: bool) {
    let expected = if requires_action {
        wire::ActivityInvocationState::Pending
    } else {
        wire::ActivityInvocationState::Dispatching
    };
    let mut client = f.activity();
    on_activity_change(f, async || {
        let page = client
            .browse_run_entries(request(
                wire::BrowseRunEntriesRequest {
                    name: format!("workspaces/{}/runs/{run}", f.workspace.id()),
                    ..Default::default()
                },
                &f.token(&f.member, "reef"),
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        page.entries.iter().any(|entry| matches!(&entry.record, Some(wire::activity_run_entry::Record::Invocation(row)) if row.state() == expected && row.requires_action == requires_action)).then_some(())
    })
    .await;
}

/// Rereads with `read` after each change `WatchActivity` reports to the member, until
/// `read` returns a value. The first message arrives at once, so the first read is current.
async fn on_activity_change<T>(f: &Fixture, mut read: impl AsyncFnMut() -> Option<T>) -> T {
    let mut changes = f
        .activity()
        .watch_activity(request(
            wire::WatchActivityRequest {},
            &f.token(&f.member, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let wake = changes.message().await.unwrap().expect("activity stream");
            if !wake.changed {
                continue;
            }
            if let Some(value) = read().await {
                break value;
            }
        }
    })
    .await
    .expect("activity reaches the expected state")
}

#[tokio::test]
async fn invocation_wire_rejects_an_mcp_is_error_and_keeps_its_native_data() {
    let f = Fixture::new().await;
    let server = MockServer::start().await;
    Mock::given(method("POST")).respond_with(|request: &wiremock::Request| {
        let request: serde_json::Value = request.body_json().unwrap();
        let result = match request.get("method").and_then(serde_json::Value::as_str).unwrap() {
            "initialize" => json!({"protocolVersion": "2025-11-25", "serverInfo": {"name": "fixture", "version": "1"}, "capabilities": {"tools": {}}}),
            "notifications/initialized" => return ResponseTemplate::new(202),
            "tools/call" => json!({"content": [{"type": "text", "text": "private native error"}], "isError": true, "structuredContent": {"private": [null, 42]}}),
            other => panic!("unexpected MCP method {other}"),
        };
        ResponseTemplate::new(200).set_body_json(json!({"jsonrpc": "2.0", "id": request.get("id").unwrap(), "result": result}))
    }).mount(&server).await;
    let providers = coral_provider::ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default()
            .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        f.database.database(),
        f.workspaces.clone(),
    );
    let context = super::super::change(&f.owner);
    let setup = providers.prepare_mcp_fixture(&context, f.workspace.id(), coral_catalog::McpDiscoverySnapshot::parse(&json!({
        "protocolVersion": "2025-11-25", "serverInfo": {"name": "fixture", "version": "1"},
        "server": {"transport": "streamable_http", "url": server.uri()}, "capabilities": {"tools": {}},
        "tools": [{"name": "read", "inputSchema": {"type": "object"}, "annotations": {"readOnlyHint": true}}]
    }).to_string()).unwrap()).await.unwrap();
    providers
        .publish_workspace_provider(
            &context,
            coral_provider::WorkspaceProviderPublicationRequest::new(
                coral_provider::ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string())
                    .unwrap(),
                f.workspace.id().clone(),
                setup.id().clone(),
                coral_provider::ProviderNamespace::parse("evidence").unwrap(),
                coral_provider::CredentialRoute::None,
            ),
        )
        .await
        .unwrap()
        .skip_search()
        .published_provider()
        .unwrap();
    let run = start(
        &f,
        "return await coral.providers.evidence.read({private: 'argument'});",
    )
    .await;
    let Some(wire::inspect_run_response::Terminal::Failed(failure)) =
        terminal(&f, &run).await.terminal
    else {
        panic!("an uncaught isError fails the Run")
    };
    let error = failure.error.expect("Program error");
    assert_eq!(error.tool_code, "error_response");
    assert_eq!(error.tool_status, 0, "an MCP outcome has no HTTP status");
    assert!(error.tool_body.contains("private native error"));
    let exact = invocation(&f, &run).await;
    assert_eq!(
        exact.invocation.unwrap().outcome(),
        wire::ActivityInvocationOutcome::UpstreamOutcome
    );
    let Some(wire::inspect_invocation_response::Outcome::UpstreamOutcomeJson(native)) =
        exact.outcome
    else {
        panic!("native MCP outcome")
    };
    let native: serde_json::Value = serde_json::from_str(&native).unwrap();
    assert_eq!(native.pointer("/data/isError"), Some(&json!(true)));
    assert_eq!(
        native.pointer("/data/content"),
        Some(&json!([{"type": "text", "text": "private native error"}]))
    );
    assert_eq!(
        native.pointer("/data/structuredContent"),
        Some(&json!({"private": [null, 42]}))
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one retained discovery journey verifies typed metadata, exact native response, summary isolation, and private audience"
)]
async fn discovery_wire_projects_typed_context_and_exact_deleted_private_tool_response_only_on_exact_read()
 {
    let f = Fixture::new().await;
    let actor = AuthenticatedPrincipal::user(f.member.clone());
    let goal = f.task(&actor).await;
    let commands = coral_task::TaskCommands::new(
        f.tasks.clone(),
        coral_search::UniversalSearchModule::new(f.database.database(), f.workspaces.clone()),
        f.runs.clone(),
    );
    let call = coral_task::AttributedCommand::new(
        actor.clone(),
        f.workspace.id().clone(),
        goal.id().clone(),
        coral_task::TaskIntent::parse("Retain exact description").unwrap(),
    );
    let functions = coral_tools::PrivateToolModule::new(
        f.database.database(),
        f.workspaces.clone(),
        coral_tools::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .unwrap();
    commands
        .record_retained(
            &functions,
            &call,
            coral_task::RetainedChange::Save {
                path: "answer",
                description: "Native historical answer",
                source: "type Answer = { value: string };\nexport default function answer(): Answer { return { value: 'private old source' }; }",
            },
        )
        .await
        .unwrap();
    let description = commands
        .describe(&call, "coral.private.answer", &["Answer".to_string()])
        .await
        .unwrap();
    commands
        .record_retained(
            &functions,
            &call,
            coral_task::RetainedChange::Delete { path: "answer" },
        )
        .await
        .unwrap();
    publish_http(&f, "https://evidence.example").await;
    let provider = "evidence";
    let searched = commands
        .search(
            &call,
            coral_search::SearchInput {
                query: Some("  private missing café  ".into()),
                provider: vec![provider.into()],
                limit: Some(7),
                offset: Some(3),
            },
        )
        .await
        .unwrap();
    let mut client = f.activity();
    let token = f.token(&f.member, "reef");
    let issued = f
        .workspaces
        .create_agent(
            &super::super::change(&f.owner),
            f.workspace.id(),
            coral_workspace::AgentDisplayName::parse("No history audience").unwrap(),
            None,
        )
        .await
        .unwrap();
    let name = format!("workspaces/{}/tasks/{}", f.workspace.id(), goal.id());
    let page = client
        .inspect_task(request(
            wire::InspectTaskRequest {
                name,
                page_size: 4,
                ..Default::default()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.interactions.len(), 4);
    assert!(
        page.next_after.is_empty(),
        "a full final child page has no next page"
    );
    let encoded = prost::Message::encode_to_vec(&page);
    assert!(!String::from_utf8_lossy(&encoded).contains("private old source"));
    assert!(!String::from_utf8_lossy(&encoded).contains("private missing"));
    for row in page
        .interactions
        .iter()
        .filter(|row| row.kind == "describe" || row.kind == "search")
    {
        let exact = client
            .inspect_task_interaction(request(
                wire::InspectTaskInteractionRequest {
                    name: row.name.clone(),
                },
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        let Some(wire::inspect_task_interaction_response::Evidence::Discovery(discovery)) =
            exact.evidence
        else {
            panic!("typed discovery evidence")
        };
        if row.kind == "describe" {
            assert_eq!(
                discovery.context,
                Some(wire::activity_discovery::Context::Describe(
                    wire::ActivityDescribeContext {
                        call_path: "coral.private.answer".into(),
                        types: vec!["Answer".into()],
                    }
                ))
            );
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&discovery.response_json).unwrap(),
                coral_search::projection::exact_description(&description)
            );
        } else {
            assert_eq!(
                discovery.context,
                Some(wire::activity_discovery::Context::Search(
                    wire::ActivitySearchContext {
                        query: Some("  private missing café  ".into()),
                        provider: Some(provider.into()),
                        limit: 7,
                        offset: 3,
                    }
                ))
            );
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&discovery.response_json).unwrap(),
                coral_search::projection::search(&searched)
            );
        }
        assert_eq!(
            client
                .inspect_task_interaction(request(
                    wire::InspectTaskInteractionRequest {
                        name: row.name.clone()
                    },
                    &f.token(&f.owner, "reef"),
                    "reef"
                ))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            client
                .inspect_task_interaction(request(
                    wire::InspectTaskInteractionRequest {
                        name: row.name.clone()
                    },
                    issued.credential.expose(),
                    "mcp"
                ))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }
    assert_eq!(
        page.interactions
            .iter()
            .filter(|row| row.kind == "describe" || row.kind == "search")
            .count(),
        2
    );
}

/// coral-run owns the 100-card window (`a_run_action_window_is_bounded_*`).
/// Over the wire, the queue pages by its cursor and a Run rejects a bad one.
#[tokio::test]
async fn action_queue_pages_by_cursor_and_a_run_rejects_a_malformed_action_cursor() {
    let f = Fixture::new().await;
    let server = MockServer::start().await;
    publish_http(&f, &server.uri()).await;
    let run = start(&f, "await Promise.all([1, 2].map(() => coral.providers.evidence.read({body: {private: 'never in a card'}})));").await;
    let first = wait_for_action_page(&f, 1, |page| page.action_count == 2).await;
    let [first_action] = first.actions.as_slice() else {
        panic!("the queue pages requests: {first:?}")
    };
    assert!(!first.next_after.is_empty());
    let token = f.token(&f.member, "reef");
    let mut client = f.activity();
    let second = client
        .browse_required_actions(request(
            wire::BrowseRequiredActionsRequest {
                after: first.next_after.clone(),
                page_size: 1,
                ..Default::default()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let [second_action] = second.actions.as_slice() else {
        panic!("the cursor reaches the second request: {second:?}")
    };
    assert_eq!(second.action_count, 2);
    assert!(second.next_after.is_empty());
    assert_ne!(
        second_action.action.as_ref().unwrap().request,
        first_action.action.as_ref().unwrap().request
    );
    let group = first_action.run.clone().unwrap();
    assert_eq!(
        client
            .inspect_run(request(
                wire::InspectRunRequest {
                    name: group.name.clone(),
                    action_after: "not-an-action-cursor".into(),
                    ..Default::default()
                },
                &token,
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    for page in [&first, &second] {
        assert!(
            !String::from_utf8_lossy(&prost::Message::encode_to_vec(page))
                .contains("never in a card")
        );
    }
    cancel(&f, &run).await;
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one exact review journey verifies Task intent and attribution before and after queue exclusion"
)]
async fn action_groups_keep_exec_intent_and_labels_after_selected_run_leaves_queue() {
    let capture = coral_telemetry::testing::process_spans();
    let f = Fixture::new().await;
    let server = MockServer::start().await;
    publish_http(&f, &server.uri()).await;
    let actor = AuthenticatedPrincipal::user(f.member.clone());
    let goal = f.task(&actor).await;
    let commands = coral_task::TaskCommands::new(
        f.tasks.clone(),
        coral_search::UniversalSearchModule::new(f.database.database(), f.workspaces.clone()),
        f.runs.clone(),
    );
    let call = coral_task::AttributedCommand::new(
        actor,
        f.workspace.id().clone(),
        goal.id().clone(),
        coral_task::TaskIntent::parse("Follow up on the review").unwrap(),
    );
    let accepted = commands.submit(&call,
        "await Promise.all([coral.providers.evidence.read({body: {private: 'first'}}), coral.providers.evidence.read({body: {private: 'second'}})]);", 60_000).await.unwrap();
    let mut client = f.activity();
    let token = f.token(&f.member, "reef");
    let queue = wait_for_action_page(&f, 0, |page| page.actions.len() == 2).await;
    let [first_queued, second_queued] = queue.actions.as_slice() else {
        panic!("two current actions")
    };
    let group = first_queued.run.as_ref().unwrap();
    assert_eq!(
        second_queued.run.as_ref().unwrap().name,
        group.name,
        "each request is listed with the Run that waits on it"
    );
    assert_eq!(group.intent, "Follow up on the review");
    assert_eq!(
        group.task,
        format!("workspaces/{}/tasks/{}", f.workspace.id(), goal.id())
    );
    assert_eq!(group.actor_label, "You");
    assert_eq!(group.workspace_label, "private");
    let (first, second) = (
        first_queued.action.as_ref().unwrap(),
        second_queued.action.as_ref().unwrap(),
    );
    assert_ne!(
        first.request, second.request,
        "identical Tools are separate exact decisions"
    );
    let encoded = String::from_utf8_lossy(&prost::Message::encode_to_vec(&queue)).into_owned();
    assert!(!encoded.contains("first"));
    assert!(!encoded.contains("second"));
    assert!(!encoded.contains("Promise.all"));
    for action in [first, second] {
        let Some(wire::activity_request_reference::Request::Approval(name)) =
            action.request.as_ref().unwrap().request.as_ref()
        else {
            panic!("approval")
        };
        client
            .decide_approval(request(
                wire::DecideApprovalRequest {
                    name: name.clone(),
                    decision: "decline".into(),
                    comment: Some("live-comment-canary".into()),
                },
                &token,
                "reef",
            ))
            .await
            .unwrap();
    }
    let detail = terminal(&f, accepted.run_id()).await;
    let retained = detail.run.unwrap();
    let retained_page = retained.action_page.as_ref().unwrap();
    assert!(retained_page.actions.is_empty());
    assert_eq!(retained.intent, group.intent);
    assert_eq!(retained.actor_label, group.actor_label);
    assert_eq!(retained.workspace_label, group.workspace_label);
    assert_eq!(retained_page.total, 0);
    assert!(retained_page.next_action.is_empty());
    let Some(wire::activity_request_reference::Request::Approval(name)) =
        first.request.as_ref().unwrap().request.as_ref()
    else {
        panic!("approval")
    };
    let review = client
        .inspect_approval(request(
            wire::InspectApprovalRequest { name: name.clone() },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(review.state, "declined");
    assert_eq!(review.approver_label, "You");
    assert_eq!(review.approver, f.member.user_id().as_str());
    let refreshed = client
        .browse_required_actions(request(
            wire::BrowseRequiredActionsRequest::default(),
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(refreshed.action_count, 0);
    assert!(refreshed.actions.is_empty());
    let spans = format!("{:?}", capture.finished());
    assert!(spans.contains("ActivityService/DecideApproval"), "{spans}");
    assert!(!spans.contains("live-comment-canary"), "{spans}");
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one attributed Run verifies ordinal projection across bounded Task browsing and independent exact reads"
)]
async fn submission_position_is_only_projected_for_activity_browsing_not_exact_evidence() {
    let f = Fixture::new().await;
    let actor = AuthenticatedPrincipal::user(f.member.clone());
    let goal = f.task(&actor).await;
    let commands = coral_task::TaskCommands::new(
        f.tasks.clone(),
        coral_search::UniversalSearchModule::new(f.database.database(), f.workspaces.clone()),
        f.runs.clone(),
    );
    let call = coral_task::AttributedCommand::new(
        actor,
        f.workspace.id().clone(),
        goal.id().clone(),
        coral_task::TaskIntent::parse("Locate the original submission").unwrap(),
    );
    for _ in 0..3 {
        commands
            .search(&call, coral_search::SearchInput::default())
            .await
            .unwrap();
    }
    let accepted = commands.submit(&call, "return 42;", 60_000).await.unwrap();
    let detail = terminal(&f, accepted.run_id()).await;
    let exact = detail.submission.unwrap();
    assert_eq!(
        exact.position, None,
        "exact Run evidence does not calculate a browse ordinal"
    );
    let name = detail.run.unwrap().name;
    let task_name = format!("workspaces/{}/tasks/{}", f.workspace.id(), goal.id());
    let mut client = f.activity();
    let token = f.token(&f.member, "reef");
    let first = client
        .inspect_task(request(
            wire::InspectTaskRequest {
                name: task_name.clone(),
                page_size: 2,
                ..Default::default()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.total, 4);
    assert_eq!(
        first
            .interactions
            .iter()
            .map(|row| row.position)
            .collect::<Vec<_>>(),
        vec![Some(0), Some(1)]
    );
    assert!(first.interactions.iter().all(|row| row.name != exact.name));
    let browse = client
        .browse_run_entries(request(
            wire::BrowseRunEntriesRequest {
                name: name.clone(),
                page_size: 1,
                ..Default::default()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let mut located = browse.submission.unwrap();
    assert_eq!(
        located.position,
        Some(3),
        "authorized ordinal does not depend on the loaded Task window"
    );
    located.position = None;
    assert_eq!(located, exact);
    let anchored = client
        .inspect_task(request(
            wire::InspectTaskRequest {
                name: task_name,
                anchor: name.clone(),
                page_size: 1,
                ..Default::default()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(anchored.offset, 3);
    assert_eq!(anchored.interactions.first().unwrap().position, Some(3));
    let exact_child = client
        .inspect_task_interaction(request(
            wire::InspectTaskInteractionRequest { name: exact.name },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(exact_child.interaction.unwrap().position, None);
    assert_eq!(
        client
            .browse_run_entries(request(
                wire::BrowseRunEntriesRequest {
                    name,
                    ..Default::default()
                },
                &f.token(&f.owner, "reef"),
                "reef",
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
}

/// Over the wire, counts split by kind and add up to the total, a connection
/// request two Runs share counts once, and the kind filter selects one list
/// that pages, while a read with no filter still lists every kind.
#[tokio::test]
async fn required_actions_count_and_filter_by_kind() {
    use wire::ActivityRequiredActionKind as Kind;
    let f = Fixture::new().await;
    let server = MockServer::start().await;
    publish_http(&f, &server.uri()).await;
    publish_individual(&f, &server.uri()).await;
    let token = f.token(&f.member, "reef");
    let mut client = f.activity();

    let count = async |client: &mut wire::activity_service_client::ActivityServiceClient<_>| {
        client
            .count_required_actions(request(
                wire::CountRequiredActionsRequest::default(),
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner()
    };
    let before = count(&mut client).await;

    start(
        &f,
        "await Promise.all([1, 2].map(() => coral.providers.evidence.read({body: {}})));",
    )
    .await;
    for _ in 0..2 {
        start(&f, "await coral.providers.personal.read({});").await;
    }
    wait_for_action_page(&f, 0, |page| page.action_count == before.action_count + 3).await;

    let after = count(&mut client).await;
    assert_eq!(after.approval_count - before.approval_count, 2);
    assert_eq!(
        after.connection_count - before.connection_count,
        1,
        "two Runs wait on one connection request"
    );
    assert_eq!(
        after.action_count,
        after.approval_count + after.connection_count
    );

    let browse = async |client: &mut wire::activity_service_client::ActivityServiceClient<_>,
                        kind: Kind,
                        after: String,
                        page_size: u32| {
        client
            .browse_required_actions(request(
                wire::BrowseRequiredActionsRequest {
                    kind: kind.into(),
                    after,
                    page_size,
                    ..Default::default()
                },
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner()
    };
    let is_approval = |action: &wire::ActivityRequiredAction| {
        matches!(
            action
                .action
                .as_ref()
                .unwrap()
                .request
                .as_ref()
                .unwrap()
                .request,
            Some(wire::activity_request_reference::Request::Approval(_))
        )
    };

    let first = browse(&mut client, Kind::Approval, String::new(), 1).await;
    assert_eq!(first.action_count, after.approval_count);
    assert!(first.actions.iter().all(is_approval));
    assert!(!first.next_after.is_empty());
    let rest = browse(&mut client, Kind::Approval, first.next_after, 100).await;
    assert_eq!(
        i64::try_from(1 + rest.actions.len()).unwrap(),
        after.approval_count,
        "the filtered list pages to the filtered count"
    );
    assert!(rest.actions.iter().all(is_approval));

    let connections = browse(&mut client, Kind::AccountConnection, String::new(), 100).await;
    assert_eq!(connections.action_count, after.connection_count);
    assert_eq!(
        i64::try_from(connections.actions.len()).unwrap(),
        after.connection_count
    );
    assert!(!connections.actions.iter().any(is_approval));

    let unfiltered = browse(&mut client, Kind::Unspecified, String::new(), 100).await;
    assert_eq!(unfiltered.action_count, after.action_count);
    assert_eq!(
        i64::try_from(unfiltered.actions.len()).unwrap(),
        after.action_count
    );
}

/// A filter this server does not know fails the read. It never lists every kind.
#[tokio::test]
async fn required_actions_reject_an_unknown_kind() {
    let f = Fixture::new().await;
    let status = f
        .activity()
        .browse_required_actions(request(
            wire::BrowseRequiredActionsRequest {
                kind: 99,
                ..Default::default()
            },
            &f.token(&f.member, "reef"),
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

/// A revision follows the set of one kind of pending request: every way a
/// request joins or leaves that set moves it, including one request ending as
/// another starts at the same count, and the other kind's stays put. A read with
/// nothing pending changed moves neither. Expiry is proved where the owner's
/// timer can be shortened, in `coral-run`'s approval tests.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey moves one set of pending requests through every way it changes"
)]
async fn required_action_revisions_follow_each_kinds_pending_set() {
    use wire::activity_request_reference::Request::{AccountConnection, Approval};
    let f = Fixture::new().await;
    let server = MockServer::start().await;
    let evidence = publish_http(&f, &server.uri()).await;
    publish_individual(&f, &server.uri()).await;
    let token = f.token(&f.member, "reef");
    let mut client = f.activity();
    let mut accounts = f.accounts();
    let count = async || {
        f.activity()
            .count_required_actions(request(
                wire::CountRequiredActionsRequest::default(),
                &f.token(&f.member, "reef"),
                "reef",
            ))
            .await
            .unwrap()
            .into_inner()
    };
    let requests = async |wanted: i64| {
        wait_for_action_page(&f, 100, |page| page.action_count == wanted)
            .await
            .actions
            .into_iter()
            .map(|action| action.action.unwrap().request.unwrap().request.unwrap())
            .collect::<Vec<_>>()
    };
    let approvals = async |wanted| -> Vec<String> {
        requests(wanted)
            .await
            .into_iter()
            .filter_map(|request| match request {
                Approval(name) => Some(name),
                AccountConnection(_) => None,
            })
            .collect()
    };
    let idle = count().await;
    let (approval, connection) = (&idle.approval_revision, &idle.connection_revision);
    assert_eq!(
        (idle.action_count, count().await),
        (0, idle.clone()),
        "nothing pending changed"
    );

    // Created.
    let first_run = start(&f, "await coral.providers.evidence.read({body: {}});").await;
    let first = approvals(1).await;
    let one = count().await;
    assert_ne!(&one.approval_revision, approval);
    assert_eq!(&one.connection_revision, connection);

    // Activity that creates no request moves neither revision.
    let unrelated = start(&f, "return 1;").await;
    terminal(&f, &unrelated).await;
    assert_eq!(count().await, one, "nothing pending changed");

    // An ended Run closes its request, and another request of the same count starts.
    let swap_run = start(&f, "await coral.providers.evidence.read({body: {}});").await;
    approvals(2).await;
    assert_ne!(count().await.approval_revision, one.approval_revision);
    cancel(&f, &first_run).await;
    assert_ne!(approvals(1).await, first);
    let swapped = count().await;
    assert_eq!(swapped.approval_count, one.approval_count);
    assert_ne!(swapped.approval_revision, one.approval_revision);
    assert_eq!(&swapped.connection_revision, connection);

    // Decided, while a third request keeps the count level.
    let [decided]: [String; 1] = approvals(1).await.try_into().unwrap();
    let closing_run = start(&f, "await coral.providers.evidence.read({body: {}});").await;
    approvals(2).await;
    client
        .decide_approval(request(
            wire::DecideApprovalRequest {
                name: decided,
                decision: "approve".into(),
                comment: None,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap();
    approvals(1).await;
    let after_decision = count().await;
    assert_eq!(after_decision.approval_count, swapped.approval_count);
    assert_ne!(after_decision.approval_revision, swapped.approval_revision);
    terminal(&f, &swap_run).await;

    // Closed by a policy that no longer asks.
    policy(&f, &evidence, coral_policy::ToolAccessState::Enabled).await;
    requests(0).await;
    let after_policy = count().await;
    assert_ne!(
        after_policy.approval_revision,
        after_decision.approval_revision
    );
    terminal(&f, &closing_run).await;

    // A connection request moves its own kind's revision alone.
    let waiting = start(&f, "await coral.providers.personal.read({});").await;
    let [AccountConnection(name)]: [_; 1] = requests(1).await.try_into().unwrap() else {
        panic!("connection request")
    };
    let waiting_count = count().await;
    assert_ne!(&waiting_count.connection_revision, connection);
    assert_eq!(
        waiting_count.approval_revision,
        after_policy.approval_revision
    );
    let joining = start(&f, "await coral.providers.personal.read({});").await;
    wait_invocation(&f, &joining, true).await;
    assert_eq!(
        count().await,
        waiting_count,
        "a second Run waits on the same request"
    );
    accounts
        .resolve_account_connection_request(request(
            wire::ResolveAccountConnectionRequestRequest {
                name,
                decision: Some(
                    wire::resolve_account_connection_request_request::Decision::Decline(()),
                ),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap();
    requests(0).await;
    assert_ne!(
        count().await.connection_revision,
        waiting_count.connection_revision
    );
    terminal(&f, &waiting).await;
    terminal(&f, &joining).await;
}

/// Browses the member's action queue until `ready` accepts a page. Actions appear asynchronously.
async fn wait_for_action_page(
    f: &Fixture,
    page_size: u32,
    ready: impl Fn(&wire::BrowseRequiredActionsResponse) -> bool,
) -> wire::BrowseRequiredActionsResponse {
    let mut client = f.activity();
    on_activity_change(f, async || {
        let page = client
            .browse_required_actions(request(
                wire::BrowseRequiredActionsRequest {
                    page_size,
                    ..Default::default()
                },
                &f.token(&f.member, "reef"),
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        ready(&page).then_some(page)
    })
    .await
}

/// Starts `source` as the member, with a minute to run.
async fn start(f: &Fixture, source: &str) -> coral_run::RunId {
    start_as(
        f,
        &AuthenticatedPrincipal::user(f.member.clone()),
        source,
        60_000,
    )
    .await
}

async fn terminal(f: &Fixture, id: &coral_run::RunId) -> wire::InspectRunResponse {
    f.settle(&AuthenticatedPrincipal::user(f.member.clone()), id)
        .await;
    let detail = f
        .activity()
        .inspect_run(request(
            wire::InspectRunRequest {
                name: format!("workspaces/{}/runs/{id}", f.workspace.id()),
                page_size: 100,
                event_after: 0,
                ..Default::default()
            },
            &f.token(&f.member, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(
        detail.terminal.is_some(),
        "a settled Run has terminal evidence"
    );
    detail
}

async fn publish_http(f: &Fixture, url: &str) -> coral_provider::ProviderId {
    publish_openapi(
        f,
        "evidence",
        &json!({
            "openapi": "3.1.0", "info": {"title": "Evidence", "version": "1"},
            "servers": [{"url": url}], "paths": {"/result": {"post": {
                "operationId": "read", "requestBody": {"content": {"application/json": {"schema": {"type": "object"}}}},
                "responses": {"200": {"description": "Native result", "content": {"application/json": {"schema": {"type": "object"}}}}}
            }}}
        }),
        coral_provider::CredentialRoute::None,
    )
    .await
}

/// Publishes a Provider whose Tools run with each User's own Account, so a call
/// by a User with no Account waits on a connection request.
async fn publish_individual(f: &Fixture, url: &str) -> coral_provider::ProviderId {
    publish_openapi(
        f,
        "personal",
        &json!({
            "openapi": "3.1.0", "info": {"title": "Personal", "version": "1"},
            "servers": [{"url": url}],
            "components": {"securitySchemes": {"bearer": {"type": "http", "scheme": "bearer"}}},
            "security": [{"bearer": []}],
            "paths": {"/result": {"get": {"operationId": "read", "responses": {"200": {"description": "Native result"}}}}}
        }),
        coral_provider::CredentialRoute::Caller,
    )
    .await
}

async fn publish_openapi(
    f: &Fixture,
    namespace: &str,
    document: &serde_json::Value,
    route: coral_provider::CredentialRoute,
) -> coral_provider::ProviderId {
    let providers = coral_provider::ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default()
            .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        f.database.database(),
        f.workspaces.clone(),
    );
    let context = super::super::change(&f.owner);
    let setup = providers
        .prepare_openapi_fixture(
            &context,
            f.workspace.id(),
            coral_catalog::OpenApiDocument::parse(&document.to_string()).unwrap(),
        )
        .await
        .unwrap();
    providers
        .publish_workspace_provider(
            &context,
            coral_provider::WorkspaceProviderPublicationRequest::new(
                coral_provider::ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string())
                    .unwrap(),
                f.workspace.id().clone(),
                setup.id().clone(),
                coral_provider::ProviderNamespace::parse(namespace).unwrap(),
                route,
            ),
        )
        .await
        .unwrap()
        .skip_search()
        .published_provider()
        .unwrap()
        .id()
        .clone()
}

async fn policy(
    f: &Fixture,
    provider: &coral_provider::ProviderId,
    state: coral_policy::ToolAccessState,
) {
    coral_policy::ToolPolicyModule::new(f.database.database(), f.workspaces.clone())
        .set_access_state(
            &super::super::change(&f.owner),
            coral_policy::ToolPolicyTarget::new(
                f.workspace.id().clone(),
                provider.clone(),
                coral_catalog::ToolKey::parse("read").unwrap(),
            ),
            state,
        )
        .await
        .unwrap();
}

async fn invocation(f: &Fixture, run: &coral_run::RunId) -> wire::InspectInvocationResponse {
    let mut client = f.activity();
    let rows = client
        .browse_run_entries(request(
            wire::BrowseRunEntriesRequest {
                name: format!("workspaces/{}/runs/{run}", f.workspace.id()),
                ..Default::default()
            },
            &f.token(&f.member, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let invocations: Vec<_> = rows
        .entries
        .iter()
        .filter_map(|entry| match &entry.record {
            Some(wire::activity_run_entry::Record::Invocation(row)) => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(invocations.len(), 1);
    // The authorized Workspace itself is named "private"; inspect child summaries
    // for the argument/result marker independently of that safe group label.
    let encoded: Vec<_> = rows
        .entries
        .iter()
        .flat_map(prost::Message::encode_to_vec)
        .collect();
    assert!(
        !String::from_utf8_lossy(&encoded).contains("private"),
        "summaries exclude logical arguments and native responses"
    );
    client
        .inspect_invocation(request(
            wire::InspectInvocationRequest {
                name: invocations.first().unwrap().name.clone(),
            },
            &f.token(&f.member, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test]
async fn invocation_wire_rejects_an_http_error_status_with_its_evidence_and_keeps_coral_failures_separate()
 {
    let f = Fixture::new().await;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(418).set_body_json(json!({"private": [null, 42]})))
        .expect(1)
        .mount(&server)
        .await;
    let provider = publish_http(&f, &server.uri()).await;
    policy(&f, &provider, coral_policy::ToolAccessState::Enabled).await;
    let source = "return await coral.providers.evidence.read({body: {private: 'argument'}});";
    let run = start(&f, source).await;
    let detail = terminal(&f, &run).await;
    let Some(wire::inspect_run_response::Terminal::Failed(failure)) = &detail.terminal else {
        panic!("an uncaught upstream error fails the Run")
    };
    let error = failure.error.as_ref().expect("Program error");
    assert_eq!(error.tool_code, "error_response");
    assert_eq!(error.tool_status, 418);
    assert_eq!(error.tool_body, r#"{"private":[null,42]}"#);
    assert!(!error.tool_body_truncated);
    let exact = invocation(&f, &run).await;
    assert_eq!(
        exact.invocation.as_ref().unwrap().outcome(),
        wire::ActivityInvocationOutcome::UpstreamOutcome
    );
    let Some(wire::inspect_invocation_response::Outcome::UpstreamOutcomeJson(native)) =
        exact.outcome
    else {
        panic!("complete native outcome")
    };
    let native: serde_json::Value = serde_json::from_str(&native).unwrap();
    assert_eq!(native.get("status"), Some(&json!(418)));
    assert_eq!(native.get("data"), Some(&json!({"private": [null, 42]})));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&exact.arguments_json).unwrap(),
        json!({"body": {"private": "argument"}})
    );
    assert!(detail.events.iter().any(|event| matches!(&event.payload,
        Some(wire::activity_run_event::Payload::InvocationStarted(started)) if started.invocation == exact.invocation.as_ref().unwrap().name)));
    assert!(detail.events.iter().any(|event| matches!(&event.payload,
        Some(wire::activity_run_event::Payload::InvocationEnded(ended)) if ended.outcome() == wire::ActivityInvocationOutcome::UpstreamOutcome)));

    policy(&f, &provider, coral_policy::ToolAccessState::Disabled).await;
    let run = start(&f, source).await;
    let detail = terminal(&f, &run).await;
    let Some(wire::inspect_run_response::Terminal::Failed(failure)) = detail.terminal else {
        panic!("typed Run failure")
    };
    assert_eq!(
        failure.code(),
        wire::ActivityRunFailureCode::ProgramRejected
    );
    assert!(failure.error.is_some());
    let exact = invocation(&f, &run).await;
    let Some(wire::inspect_invocation_response::Outcome::CoralFailure(failure)) = exact.outcome
    else {
        panic!("typed Coral Failure")
    };
    assert_eq!(failure.code, "tool_disabled");
    assert!(!failure.message.is_empty());
}

/// Asserts one exact location in the wire form a Reef reader receives.
#[expect(
    clippy::unnecessary_wraps,
    reason = "rows compare the optional location each record carries"
)]
fn located(
    document: wire::activity_source_location::Document,
    line: u32,
    column: u32,
) -> Option<wire::ActivitySourceLocation> {
    Some(wire::ActivitySourceLocation {
        document: Some(document),
        line,
        column,
    })
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one real Run read back through every activity projection that carries a location"
)]
async fn activity_records_carry_their_authored_location_on_every_projection() {
    // Capture and storage rules belong to coral-v8 and coral-run. This asserts
    // the wire contract: document naming and presence on each projection.
    use wire::activity_source_location::Document;
    let f = Fixture::new().await;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    let provider = publish_http(&f, &server.uri()).await;
    policy(&f, &provider, coral_policy::ToolAccessState::Enabled).await;
    f.private_tools
        .save(
            &coral_tools::PrivateToolChangeContext::new(
                AuthenticatedPrincipal::user(f.member.clone()),
                1_700_000_000_000_000_007,
                coral_workspace::TraceId::current_or_new(),
            ),
            f.workspace.id(),
            coral_tools::PrivateToolSave::new(
                coral_tools::PrivateToolPath::parse("report.forecast").unwrap(),
                coral_tools::AuthoredToolDescription::parse("Forecast one city").unwrap(),
                coral_tools::AuthoredToolSource::parse(
                    "export default async function forecast({ city }: { city: string }) {\n    console.info(city);\n    return coral.providers.evidence.read({ body: { city } });\n}",
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    let run = start(
        &f,
        "console.warn(\"w\");\nawait coral.emit({ step: 1 });\nawait coral.private.report.forecast({ city: \"Oslo\" });\nawait coral.providers.evidence.read({ body: {} });",
    )
    .await;
    let detail = terminal(&f, &run).await;
    let [snapshot] = detail.linked_private_tools.as_slice() else {
        panic!("one linked snapshot")
    };
    let program = |line, column| located(Document::Program(wire::ActivityEmpty {}), line, column);
    let in_snapshot = |line, column| {
        located(
            Document::PrivateTool(snapshot.private_tool.clone()),
            line,
            column,
        )
    };
    let outputs = vec![program(1, 9), program(2, 13), in_snapshot(2, 13)];

    // InspectRun: authored output Events carry a location; others never do.
    let mut output_sequences = Vec::new();
    let mut event_locations = Vec::new();
    for event in &detail.events {
        match &event.payload {
            Some(
                wire::activity_run_event::Payload::ProgramEmitted(_)
                | wire::activity_run_event::Payload::ProgramLogged(_),
            ) => {
                output_sequences.push(event.sequence);
                event_locations.push(event.source_location.clone());
            }
            _ => assert_eq!(event.source_location, None, "{event:?}"),
        }
    }
    assert_eq!(event_locations, outputs);

    // InspectRunEvent returns the same location as the page.
    let mut client = f.activity();
    let token = f.token(&f.member, "reef");
    for (sequence, expected) in output_sequences.iter().zip(&outputs) {
        let exact = client
            .inspect_run_event(request(
                wire::InspectRunEventRequest {
                    run: format!("workspaces/{}/runs/{run}", f.workspace.id()),
                    sequence: *sequence,
                },
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(&exact.event.unwrap().source_location, expected);
    }

    // BrowseRunEntries rows and InspectInvocation carry it too.
    let rows = client
        .browse_run_entries(request(
            wire::BrowseRunEntriesRequest {
                name: format!("workspaces/{}/runs/{run}", f.workspace.id()),
                page_size: 100,
                ..Default::default()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let mut row_locations = Vec::new();
    for entry in rows.entries {
        match entry.record {
            Some(wire::activity_run_entry::Record::Invocation(row)) => {
                let exact = client
                    .inspect_invocation(request(
                        wire::InspectInvocationRequest {
                            name: row.name.clone(),
                        },
                        &token,
                        "reef",
                    ))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(
                    exact.invocation.unwrap().source_location,
                    row.source_location
                );
                row_locations.push(row.source_location);
            }
            Some(wire::activity_run_entry::Record::Event(summary)) => {
                if summary.kind.starts_with("program.") {
                    row_locations.push(summary.source_location);
                } else {
                    assert_eq!(summary.source_location, None, "{summary:?}");
                }
            }
            None => panic!("unknown entry"),
        }
    }
    // The Private Tool call is an Invocation at its call site, admitted
    // before its body runs.
    let mut expected_rows = outputs;
    expected_rows.insert(2, program(3, 28));
    expected_rows.extend([in_snapshot(3, 37), program(4, 32)]);
    assert_eq!(row_locations, expected_rows);
}

/// A Run whose program calls the Private Tool `report.forecast`, whose body
/// calls a Provider Tool, and its two calls as `BrowseRunEntries` lists them.
async fn nested_call_run(f: &Fixture) -> (coral_run::RunId, Vec<wire::ActivityInvocation>) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    let provider = publish_http(f, &server.uri()).await;
    policy(f, &provider, coral_policy::ToolAccessState::Enabled).await;
    f.private_tools
        .save(
            &coral_tools::PrivateToolChangeContext::new(
                AuthenticatedPrincipal::user(f.member.clone()),
                1_700_000_000_000_000_007,
                coral_workspace::TraceId::current_or_new(),
            ),
            f.workspace.id(),
            coral_tools::PrivateToolSave::new(
                coral_tools::PrivateToolPath::parse("report.forecast").unwrap(),
                coral_tools::AuthoredToolDescription::parse("Forecast one city").unwrap(),
                coral_tools::AuthoredToolSource::parse(
                    "export default async () => (await coral.providers.evidence.read({ body: {} })).data;",
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    let run = start(f, "return await coral.private.report.forecast();").await;
    terminal(f, &run).await;
    let calls = run_calls(f, &run, None).await.unwrap();
    (run, calls)
}

/// The Tool calls `BrowseRunEntries` lists for `run` at `level`.
async fn run_calls(
    f: &Fixture,
    run: &coral_run::RunId,
    level: Option<wire::browse_run_entries_request::Level>,
) -> Result<Vec<wire::ActivityInvocation>, tonic::Status> {
    let response = f
        .activity()
        .browse_run_entries(request(
            wire::BrowseRunEntriesRequest {
                name: format!("workspaces/{}/runs/{run}", f.workspace.id()),
                page_size: 100,
                level,
                ..Default::default()
            },
            &f.token(&f.member, "reef"),
            "reef",
        ))
        .await?;
    Ok(response
        .into_inner()
        .entries
        .into_iter()
        .filter_map(|entry| match entry.record {
            Some(wire::activity_run_entry::Record::Invocation(row)) => Some(row),
            _ => None,
        })
        .collect())
}

#[tokio::test]
async fn a_tool_call_names_its_tool_kind_and_the_call_whose_body_made_it() {
    // coral-run owns which call is whose child. This asserts the wire
    // projection: the kind, and the parent's name, path and kind.
    let f = Fixture::new().await;
    let (_run, rows) = nested_call_run(&f).await;
    let [outer, inner] = rows.as_slice() else {
        panic!("two calls: {rows:?}")
    };
    assert_eq!(
        (outer.call_path.as_str(), outer.tool_kind(), &outer.parent),
        (
            "coral.private.report.forecast",
            wire::ActivityToolKind::Private,
            &None
        )
    );
    assert_eq!(
        (inner.tool_kind(), &inner.parent),
        (
            wire::ActivityToolKind::Provider,
            &Some(wire::ActivityInvocationParent {
                invocation: outer.name.clone(),
                call_path: outer.call_path.clone(),
                tool_kind: wire::ActivityToolKind::Private.into(),
                shared_tool_revision: 0,
            })
        )
    );
    assert_eq!(
        (outer.shared_tool_revision, inner.shared_tool_revision),
        (0, 0)
    );

    // InspectInvocation projects the same parent.
    let exact = f
        .activity()
        .inspect_invocation(request(
            wire::InspectInvocationRequest {
                name: inner.name.clone(),
            },
            &f.token(&f.member, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner()
        .invocation
        .unwrap();
    assert_eq!(exact.parent, inner.parent);
}

#[tokio::test]
async fn each_level_of_a_run_lists_only_its_own_calls() {
    // coral-run owns which call is at which level. This asserts the wire
    // mapping of each level, and the refusal of another Workspace's call.
    use wire::browse_run_entries_request::Level;
    let f = Fixture::new().await;
    let (run, rows) = nested_call_run(&f).await;
    let [outer, inner] = rows.as_slice() else {
        panic!("two calls: {rows:?}")
    };
    let names = |rows: Vec<wire::ActivityInvocation>| {
        rows.into_iter().map(|row| row.name).collect::<Vec<_>>()
    };
    let top = run_calls(&f, &run, Some(Level::TopLevel(wire::ActivityEmpty {})));
    assert_eq!(names(top.await.unwrap()), [outer.name.as_str()]);
    let inside = run_calls(&f, &run, Some(Level::Parent(outer.name.clone())));
    assert_eq!(names(inside.await.unwrap()), [inner.name.as_str()]);
    let foreign = outer.name.replace(
        &f.workspace.id().to_string(),
        "00000000-0000-4000-8000-000000000000",
    );
    let refused = run_calls(&f, &run, Some(Level::Parent(foreign))).await;
    assert_eq!(refused.unwrap_err().code(), tonic::Code::InvalidArgument);
}

/// A summary needs one Workspace, reports a rejected window as an invalid argument, hides
/// a Workspace the caller cannot read, and names its rows for Reef. `coral-run` owns
/// which windows it accepts.
#[tokio::test]
async fn activity_summary_wire_rejects_malformed_and_hidden_windows_and_labels_its_rows() {
    let f = Fixture::new().await;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"temperature": 21})))
        .mount(&server)
        .await;
    let provider = publish_http(&f, &server.uri()).await;
    policy(&f, &provider, coral_policy::ToolAccessState::Enabled).await;
    let run = start(
        &f,
        "await coral.providers.evidence.read({body: {}}); return 'done';",
    )
    .await;
    terminal(&f, &run).await;
    let mut client = f.activity();
    let token = f.token(&f.member, "reef");
    let workspace = format!("workspaces/{}", f.workspace.id());
    let absent = "workspaces/c0000000-0000-4000-8000-000000000001".to_owned();
    for (name, days, code) in [
        (String::new(), 7, tonic::Code::InvalidArgument),
        (workspace.clone(), 0, tonic::Code::InvalidArgument),
        (absent, 7, tonic::Code::NotFound),
    ] {
        let denied = client
            .summarize_activity(request(
                wire::SummarizeActivityRequest {
                    workspace: name.clone(),
                    days,
                },
                &token,
                "reef",
            ))
            .await
            .unwrap_err();
        assert_eq!(denied.code(), code, "{name:?} {days}");
    }
    let summary = client
        .summarize_activity(request(
            wire::SummarizeActivityRequest {
                workspace: workspace.clone(),
                days: 7,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let [tool] = summary.tools.as_slice() else {
        panic!("one Tool: {:?}", summary.tools)
    };
    assert_eq!(tool.call_path, "coral.providers.evidence.read");
    // The catalog's name for the Tool: its Provider and its encoded Tool Key, `read`.
    assert_eq!(
        tool.tool,
        format!("{workspace}/providers/{provider}/tools/cmVhZA")
    );
    // One entry per day of the 7-day window, oldest first; the call ended today.
    assert_eq!(tool.daily, [0, 0, 0, 0, 0, 0, 1]);
    let [actor] = summary.actors.as_slice() else {
        panic!("one actor: {:?}", summary.actors)
    };
    assert_eq!(actor.label, "You");
    let key = AuthenticatedPrincipal::user(f.member.clone())
        .principal()
        .id()
        .to_string();
    assert_eq!(actor.key, key);
    let you = wire::ActivityCallerCalls {
        key,
        label: "You".into(),
        agent: String::new(),
        calls: 1,
    };
    assert_eq!(summary.callers, std::slice::from_ref(&you));
    assert_eq!(tool.callers, [you]);
}

/// Who sees a settled row follows who invoked its Run, not who decided it: a User sees
/// their own Runs, and every Owner sees Agent Runs, including another Owner's decision.
/// Another User's Run stays hidden from an Owner. No row carries arguments or comments.
/// What history holds and how it pages are `coral-run`'s rules, tested there.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one role table over a single arranged history"
)]
async fn settled_approvals_follow_who_invoked_the_run() {
    use wire::ActivityApprovalOutcome::{Approved, Declined, Expired};
    let f = Fixture::new().await;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    publish_http(&f, &server.uri()).await;
    let co_owner = coral_workspace::AuthenticatedUser::organization_member(
        coral_workspace::UserId::parse("co-owner").unwrap(),
        coral_workspace::OrganizationRole::Member,
    );
    coral_workspace::OrganizationModule::new(f.database.database())
        .provision_login(&super::super::change(&co_owner))
        .await
        .unwrap();
    f.workspaces
        .set_membership(
            &super::super::change(&f.owner),
            f.workspace.id(),
            &co_owner,
            coral_workspace::WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    let issued = f
        .workspaces
        .create_agent(
            &super::super::change(&f.owner),
            f.workspace.id(),
            coral_workspace::AgentDisplayName::parse("History agent").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = f
        .workspaces
        .authenticate_agent(f.workspace.id(), issued.credential.expose(), 2)
        .await
        .unwrap();
    let member = AuthenticatedPrincipal::user(f.member.clone());
    let owner = AuthenticatedPrincipal::user(f.owner.clone());
    let (member_token, owner_token, co_owner_token) = (
        f.token(&f.member, "reef"),
        f.token(&f.owner, "reef"),
        f.token(&co_owner, "reef"),
    );
    let call = r#"await coral.providers.evidence.read({body: {secret: "private-argument"}});"#;
    let decide = async |token: &str, name: &str, decision: &str| {
        f.activity()
            .decide_approval(request(
                wire::DecideApprovalRequest {
                    name: name.to_owned(),
                    decision: decision.into(),
                    comment: Some("private-comment".into()),
                },
                token,
                "reef",
            ))
            .await
            .unwrap();
    };

    let run = start_as(&f, &member, call, 60_000).await;
    let member_approved = pending_approval(&f, &member_token, &run).await;
    decide(&member_token, &member_approved, "approve").await;
    f.settle(&member, &run).await;
    let run = start_as(&f, &owner, call, 60_000).await;
    let owner_declined = pending_approval(&f, &owner_token, &run).await;
    decide(&owner_token, &owner_declined, "decline").await;
    f.settle(&owner, &run).await;
    let run = start_as(&f, &agent, call, 60_000).await;
    let agent_approved = pending_approval(&f, &owner_token, &run).await;
    decide(&owner_token, &agent_approved, "approve").await;
    f.settle(&agent, &run).await;
    // Nobody answers before the Run's timeout.
    let run = start_as(&f, &member, call, 1_000).await;
    let member_expired = pending_approval(&f, &member_token, &run).await;
    f.settle(&member, &run).await;

    let browse = async |token: &str, workspace: String, after: String, page_size: u32| {
        f.activity()
            .browse_settled_approvals(request(
                wire::BrowseSettledApprovalsRequest {
                    workspace,
                    after,
                    page_size,
                },
                token,
                "reef",
            ))
            .await
            .map(tonic::Response::into_inner)
    };
    let workspace = format!("workspaces/{}", f.workspace.id());
    // (viewer, the rows they see as (request, outcome, approver label))
    let cases = [
        (
            "member",
            &member_token,
            vec![
                (&member_expired, Expired, ""),
                (&member_approved, Approved, "You"),
            ],
        ),
        (
            "owner",
            &owner_token,
            vec![
                (&agent_approved, Approved, "You"),
                (&owner_declined, Declined, "You"),
            ],
        ),
        (
            "co-owner",
            &co_owner_token,
            vec![(&agent_approved, Approved, "Unavailable user")],
        ),
    ];
    for (viewer, token, expected) in cases {
        let page = browse(token, workspace.clone(), String::new(), 100)
            .await
            .unwrap();
        let seen: Vec<_> = page
            .approvals
            .iter()
            .map(|row| (&row.name, row.outcome(), row.approver_label.as_str()))
            .collect();
        assert_eq!(seen, expected, "{viewer}");
        assert!(page.next_after.is_empty(), "{viewer}");
        let encoded = String::from_utf8_lossy(&prost::Message::encode_to_vec(&page)).into_owned();
        assert!(!encoded.contains("private-argument"), "{viewer}");
        assert!(!encoded.contains("private-comment"), "{viewer}");
    }

    let rows = browse(&member_token, workspace.clone(), String::new(), 100)
        .await
        .unwrap()
        .approvals;
    let expired = rows.iter().find(|row| row.name == member_expired).unwrap();
    assert!(expired.approver.is_empty(), "nobody decided");
    let agent_row = browse(&co_owner_token, workspace.clone(), String::new(), 100)
        .await
        .unwrap()
        .approvals
        .remove(0);
    assert_eq!(agent_row.approver, f.owner.user_id().as_str());
    assert_eq!(agent_row.call_path, "coral.providers.evidence.read");
    assert_eq!(
        agent_row.run.unwrap().agent,
        format!(
            "workspaces/{}/agents/{}",
            f.workspace.id(),
            agent.principal().id()
        )
    );
    // Another Workspace shows none of these rows, and one the member cannot open is refused.
    let other = f
        .workspaces
        .create(
            &super::super::change(&f.owner),
            coral_workspace::WorkspaceDisplayName::parse("elsewhere").unwrap(),
        )
        .await
        .unwrap();
    let elsewhere = format!("workspaces/{}", other.id());
    assert!(
        browse(&owner_token, elsewhere.clone(), String::new(), 100)
            .await
            .unwrap()
            .approvals
            .is_empty()
    );
    assert_eq!(
        browse(&member_token, elsewhere, String::new(), 100)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    for (workspace, after) in [
        (String::new(), String::new()),
        (workspace.clone(), "not-a-cursor".to_owned()),
    ] {
        assert_eq!(
            browse(&member_token, workspace, after, 100)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}

/// Starts `source` as `actor`, with `timeout_ms` to run.
async fn start_as(
    f: &Fixture,
    actor: &AuthenticatedPrincipal,
    source: &str,
    timeout_ms: u64,
) -> coral_run::RunId {
    f.runs
        .start(
            actor,
            f.workspace.id(),
            coral_run::RunSubmission::new(
                coral_run::ProgramSource::parse(source).unwrap(),
                coral_run::RunTimeout::from_millis(timeout_ms).unwrap(),
            ),
        )
        .await
        .unwrap()
        .run_id()
        .clone()
}

/// The name of the approval `run` waits on, as `token`'s holder reads it.
async fn pending_approval(f: &Fixture, token: &str, run: &coral_run::RunId) -> String {
    let mut client = f.activity();
    on_activity_change(f, async || {
        let detail = client
            .inspect_run(request(
                wire::InspectRunRequest {
                    name: format!("workspaces/{}/runs/{run}", f.workspace.id()),
                    page_size: 1,
                    ..Default::default()
                },
                token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        detail
            .run
            .and_then(|run| run.action_page)
            .and_then(|page| page.actions.into_iter().next())
            .and_then(|action| action.request)
            .and_then(|reference| match reference.request {
                Some(wire::activity_request_reference::Request::Approval(name)) => Some(name),
                _ => None,
            })
    })
    .await
}
