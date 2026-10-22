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
/// card, the review, a stale decision, and each typed outcome.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one Run per Invocation gate projects the card, review, stale decision, and typed outcome"
)]
async fn cancellation_retains_pending_and_dispatched_invocation_distinctions_and_typed_actor() {
    let f = Fixture::new().await;
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
    let source = "return await coral.providers.evidence.read({body: {private: 'argument'}});";
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
                comment: Some("too late".into()),
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

    policy(&f, &provider, coral_policy::OperationAccessState::Enabled).await;
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
    assert_eq!(error.operation_code, "error_response");
    assert_eq!(
        error.operation_status, 0,
        "an MCP outcome has no HTTP status"
    );
    assert!(error.operation_body.contains("private native error"));
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
async fn discovery_wire_projects_typed_context_and_exact_deleted_function_response_only_on_exact_read()
 {
    let f = Fixture::new().await;
    let actor = AuthenticatedPrincipal::user(f.member.clone());
    let goal = f.task(&actor).await;
    let commands = coral_task::TaskCommands::new(
        f.tasks.clone(),
        coral_functions::SavedFunctionModule::new(
            f.database.database(),
            f.workspaces.clone(),
            coral_functions::IsolateLimits::default(),
        )
        .unwrap(),
        coral_search::UniversalSearchModule::new(f.database.database(), f.workspaces.clone()),
        f.search_index.clone(),
        f.runs.clone(),
    );
    let call = coral_task::AttributedCommand::new(
        actor.clone(),
        f.workspace.id().clone(),
        goal.id().clone(),
        coral_task::TaskIntent::parse("Retain exact description").unwrap(),
        coral_workspace::TraceId::current_or_new(),
    );
    commands
        .save_function(
            &call,
            "answer",
            "Native historical answer",
            "type Answer = { value: string };\nexport default function answer(): Answer { return { value: 'private old source' }; }",
        )
        .await
        .unwrap();
    let description = commands
        .describe(&call, "coral.functions.answer", &["Answer".to_string()])
        .await
        .unwrap();
    commands.delete_function(&call, "answer").await.unwrap();
    publish_http(&f, "https://evidence.example").await;
    let provider = "evidence";
    let searched = commands
        .search(
            &call,
            coral_search::SearchInput {
                query: Some("  private missing café  ".into()),
                provider: Some(provider.into()),
                limit: Some(7),
                cursor: None,
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
                        call_path: "coral.functions.answer".into(),
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
                        cursor: None,
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
    let f = Fixture::new().await;
    let server = MockServer::start().await;
    publish_http(&f, &server.uri()).await;
    let actor = AuthenticatedPrincipal::user(f.member.clone());
    let goal = f.task(&actor).await;
    let commands = coral_task::TaskCommands::new(
        f.tasks.clone(),
        coral_functions::SavedFunctionModule::new(
            f.database.database(),
            f.workspaces.clone(),
            coral_functions::IsolateLimits::default(),
        )
        .unwrap(),
        coral_search::UniversalSearchModule::new(f.database.database(), f.workspaces.clone()),
        f.search_index.clone(),
        f.runs.clone(),
    );
    let call = coral_task::AttributedCommand::new(
        actor,
        f.workspace.id().clone(),
        goal.id().clone(),
        coral_task::TaskIntent::parse("Follow up on the review").unwrap(),
        coral_workspace::TraceId::current_or_new(),
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
        "identical Operations are separate exact decisions"
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
                    comment: Some("This exact call only".into()),
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
        coral_functions::SavedFunctionModule::new(
            f.database.database(),
            f.workspaces.clone(),
            coral_functions::IsolateLimits::default(),
        )
        .unwrap(),
        coral_search::UniversalSearchModule::new(f.database.database(), f.workspaces.clone()),
        f.search_index.clone(),
        f.runs.clone(),
    );
    let call = coral_task::AttributedCommand::new(
        actor,
        f.workspace.id().clone(),
        goal.id().clone(),
        coral_task::TaskIntent::parse("Locate the original submission").unwrap(),
        coral_workspace::TraceId::current_or_new(),
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

async fn start(f: &Fixture, source: &str) -> coral_run::RunId {
    f.runs
        .start(
            &AuthenticatedPrincipal::user(f.member.clone()),
            f.workspace.id(),
            coral_run::RunSubmission::new(
                coral_run::ProgramSource::parse(source).unwrap(),
                coral_run::RunTimeout::from_millis(60_000).unwrap(),
            ),
        )
        .await
        .unwrap()
        .run_id()
        .clone()
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
    let providers = coral_provider::ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default()
            .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        f.database.database(),
        f.workspaces.clone(),
    );
    let context = super::super::change(&f.owner);
    let setup = providers.prepare_openapi_fixture(&context, f.workspace.id(),
        coral_catalog::OpenApiDocument::parse(&json!({
            "openapi": "3.1.0", "info": {"title": "Evidence", "version": "1"},
            "servers": [{"url": url}], "paths": {"/result": {"post": {
                "operationId": "read", "requestBody": {"content": {"application/json": {"schema": {"type": "object"}}}},
                "responses": {"200": {"description": "Native result", "content": {"application/json": {"schema": {"type": "object"}}}}}
            }}}
        }).to_string()).unwrap()
    ).await.unwrap();
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
        .published_provider()
        .unwrap()
        .id()
        .clone()
}

async fn policy(
    f: &Fixture,
    provider: &coral_provider::ProviderId,
    state: coral_policy::OperationAccessState,
) {
    coral_policy::OperationPolicyModule::new(f.database.database(), f.workspaces.clone())
        .set_access_state(
            &super::super::change(&f.owner),
            coral_policy::OperationPolicyTarget::new(
                f.workspace.id().clone(),
                provider.clone(),
                coral_catalog::OperationKey::parse("read").unwrap(),
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
    policy(&f, &provider, coral_policy::OperationAccessState::Enabled).await;
    let source = "return await coral.providers.evidence.read({body: {private: 'argument'}});";
    let run = start(&f, source).await;
    let detail = terminal(&f, &run).await;
    let Some(wire::inspect_run_response::Terminal::Failed(failure)) = &detail.terminal else {
        panic!("an uncaught upstream error fails the Run")
    };
    let error = failure.error.as_ref().expect("Program error");
    assert_eq!(error.operation_code, "error_response");
    assert_eq!(error.operation_status, 418);
    assert_eq!(error.operation_body, r#"{"private":[null,42]}"#);
    assert!(!error.operation_body_truncated);
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

    policy(&f, &provider, coral_policy::OperationAccessState::Disabled).await;
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
    assert_eq!(failure.code, "operation_disabled");
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
    policy(&f, &provider, coral_policy::OperationAccessState::Enabled).await;
    f.saved_functions
        .save(
            &coral_functions::SavedFunctionChangeContext::new(
                AuthenticatedPrincipal::user(f.member.clone()),
                1_700_000_000_000_000_007,
                coral_workspace::TraceId::current_or_new(),
            ),
            f.workspace.id(),
            coral_functions::SavedFunctionSave::new(
                coral_functions::SavedFunctionPath::parse("report.forecast").unwrap(),
                coral_functions::SavedFunctionDescription::parse("Forecast one city").unwrap(),
                coral_functions::SavedFunctionSource::parse(
                    "export default async function forecast(city: string) {\n    console.info(city);\n    return coral.providers.evidence.read({ body: { city } });\n}",
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    let run = start(
        &f,
        "console.warn(\"w\");\nawait coral.emit({ step: 1 });\nawait coral.functions.report.forecast(\"Oslo\");\nawait coral.providers.evidence.read({ body: {} });",
    )
    .await;
    let detail = terminal(&f, &run).await;
    let [snapshot] = detail.linked_functions.as_slice() else {
        panic!("one linked snapshot")
    };
    let program = |line, column| located(Document::Program(wire::ActivityEmpty {}), line, column);
    let in_snapshot = |line, column| {
        located(
            Document::SavedFunction(snapshot.saved_function.clone()),
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
    let mut expected_rows = outputs;
    expected_rows.extend([in_snapshot(3, 37), program(4, 32)]);
    assert_eq!(row_locations, expected_rows);
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
    policy(&f, &provider, coral_policy::OperationAccessState::Enabled).await;
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
    let [operation] = summary.operations.as_slice() else {
        panic!("one Operation: {:?}", summary.operations)
    };
    assert_eq!(operation.call_path, "coral.providers.evidence.read");
    let [actor] = summary.actors.as_slice() else {
        panic!("one actor: {:?}", summary.actors)
    };
    assert_eq!(actor.label, "You");
    assert_eq!(
        actor.key,
        AuthenticatedPrincipal::user(f.member.clone())
            .principal()
            .id()
            .to_string()
    );
}
