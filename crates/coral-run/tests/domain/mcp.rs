//! Real MCP -> Program -> durable Run observation, shared by `SQLite` and `Postgres`.
#![expect(clippy::indexing_slicing, reason = "assert known fixture JSON members")]

use super::*;
use serde_json::{Value, json};
use wiremock::matchers::body_partial_json;

#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one real Program scenario covers publication, dispatch, resource rejection and durable observation"
)]
async fn an_mcp_is_error_rejects_the_call_and_provider_bound_reads_survive_run_observation() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(|request: &wiremock::Request| {
            let request: Value = request.body_json().expect("MCP request");
            match request["method"].as_str().expect("method") {
                "initialize" => ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0", "id": request["id"], "result": {
                        "protocolVersion": "2025-11-25", "serverInfo": {"name": "fixture", "version": "1"},
                        "capabilities": {"tools": {}, "resources": {}}
                    }
                })),
                "notifications/initialized" => ResponseTemplate::new(202),
                unexpected => panic!("unexpected MCP method: {unexpected}"),
            }
        }).with_priority(10).mount(&server).await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method": "tools/call"})))
        .respond_with(|request: &wiremock::Request| {
            let request: Value = request.body_json().expect("tool request");
            assert_eq!(request["params"]["name"], "mixed");
            assert!(request["params"].get("task").is_none());
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0", "id": request["id"], "result": {
                    "content": [
                        {"type": "text", "text": "first"},
                        {"type": "resource_link", "uri": "private://report?token=secret", "name": "report", "annotations": {"priority": 0.7}, "_meta": {"vendor/part": 2}},
                        {"type": "text", "text": "last"}
                    ],
                    "structuredContent": {"count": "schema disagreement is still data"},
                    "isError": true, "_meta": {"vendor/request": "a-123"}
                }
            }))
        }).expect(1).mount(&server).await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method": "resources/read"})))
        .respond_with(|request: &wiremock::Request| {
            let request: Value = request.body_json().expect("resource request");
            assert_eq!(request["params"]["uri"], "private://report?token=secret");
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0", "id": request["id"], "result": {
                    "contents": [{"uri": "private://report?token=secret", "text": "report contents", "mimeType": "text/plain"}]
                }
            }))
        }).expect(1).mount(&server).await;
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let (workspace, principal) = fixture
        .user_scope("user:mcp-runner", "Preserve MCP outcomes")
        .await;
    for (namespace, request_id) in [
        ("first", "00000000-0000-0000-0000-000000000001"),
        ("second", "00000000-0000-0000-0000-000000000002"),
    ] {
        let snapshot = coral_catalog::McpDiscoverySnapshot::parse(&json!({
            "protocolVersion": "2025-11-25", "serverInfo": {"name": "fixture", "version": "1"},
            "server": {"transport": "streamable_http", "url": server.uri()},
            "capabilities": {"tools": {}, "resources": {}},
            "tools": [{"name": "mixed", "inputSchema": {"type": "object"}, "outputSchema": {"type": "object", "properties": {"count": {"type": "integer"}}}, "annotations": {"readOnlyHint": true}}]
        }).to_string()).expect("snapshot");
        let context = change(
            principal.authenticated_user().expect("User").clone(),
            30,
            '7',
        );
        let setup = fixture
            .providers
            .prepare_mcp_fixture(&context, &workspace, snapshot)
            .await
            .expect("MCP Setup");
        fixture
            .providers
            .publish_workspace_provider(
                &context,
                WorkspaceProviderPublicationRequest::new(
                    ManagementRequestId::parse(request_id).expect("request id"),
                    workspace.clone(),
                    setup.id().clone(),
                    ProviderNamespace::parse(namespace).expect("namespace"),
                    CredentialRoute::None,
                ),
            )
            .await
            .expect("publish")
            .published_provider()
            .expect("published MCP Provider");
    }
    let runs = fixture.live_runs(None);
    let accepted = runs.start(&principal, &workspace, submission(r#"
        let result;
        try {
          await coral.providers.first.mixed({});
          throw new Error("isError should reject the call");
        } catch (error) {
          if (!(error instanceof coral.ToolError)) throw error;
          result = {code: error.code, status: error.status ?? null, data: error.data, metadata: error.metadata};
        }
        const reference = result.data.content[1].resource;
        let rejected = 0;
        try { await coral.providers.second.resources.read({reference}); } catch (_) { rejected++; }
        try { await coral.providers.first.resources.read({reference: "private://forged"}); } catch (_) { rejected++; }
        if (rejected !== 2) throw new Error("resource authority escaped");
        const resource = await coral.providers.first.resources.read({reference});
        return {result, resource};
    "#)).await.expect("accept MCP Program");
    let terminal = wait_for_terminal(&runs, &principal, &workspace, accepted.run_id()).await;
    let Some(RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value),
    }) = terminal.terminal_outcome()
    else {
        panic!("MCP Program failed: {:?}", terminal.terminal_outcome());
    };
    let thrown = &value.as_json()["result"];
    assert_eq!(thrown["code"], "error_response");
    assert_eq!(
        thrown["status"],
        Value::Null,
        "an MCP outcome has no HTTP status"
    );
    let result = &json!({"data": thrown["data"], "metadata": thrown["metadata"]});
    assert_eq!(result["data"]["isError"], true);
    let history = runs
        .activity_entries(
            &principal,
            &workspace,
            accepted.run_id(),
            &crate::RunEntryWindow::Start,
            100,
            None,
        )
        .await
        .unwrap();
    let summary = history
        .entries
        .iter()
        .find_map(|entry| match entry {
            crate::RunEntry::Invocation(row) if row.call_path == "coral.providers.first.mixed" => {
                Some(row)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(
        summary.outcome,
        Some(crate::InvocationResultKind::UpstreamOutcome)
    );
    let detail = runs
        .inspect_invocation(&principal, &workspace, &summary.id)
        .await
        .unwrap();
    assert_eq!(detail.arguments, json!({}));
    assert!(
        matches!(detail.outcome, Some(crate::InvocationOutcome::UpstreamOutcome(ref value)) if value == result)
    );
    assert_eq!(
        result["data"]["structuredContent"],
        json!({"count": "schema disagreement is still data"})
    );
    assert_eq!(
        result["data"]["content"][0],
        json!({"type": "text", "text": "first"})
    );
    assert_eq!(
        result["data"]["content"][1]["annotations"],
        json!({"priority": 0.7})
    );
    assert_eq!(
        result["data"]["content"][1]["_meta"],
        json!({"vendor/part": 2})
    );
    assert_eq!(
        result["data"]["content"][2],
        json!({"type": "text", "text": "last"})
    );
    assert_eq!(
        result["metadata"],
        json!({"kind": "mcp", "_meta": {"vendor/request": "a-123"}})
    );
    assert_eq!(
        value.as_json()["resource"]["data"]["contents"][0]["text"],
        "report contents"
    );
    assert!(!value.as_json().to_string().contains("private://"));
    assert_eq!(
        super::retained_events(&runs, &principal, &workspace, terminal.run_id())
            .await
            .iter()
            .filter(|event| matches!(
                event.kind(),
                RunEventKind::InvocationEnded {
                    result_kind: crate::InvocationResultKind::UpstreamOutcome,
                    ..
                }
            ))
            .count(),
        2
    );
    assert_eq!(
        super::retained_events(&runs, &principal, &workspace, terminal.run_id())
            .await
            .iter()
            .filter(|event| matches!(
                event.kind(),
                RunEventKind::InvocationEnded {
                    result_kind: crate::InvocationResultKind::CoralFailure,
                    ..
                }
            ))
            .count(),
        2
    );
    let reopened = fixture.live_runs(None);
    let observed = reopened
        .observe(
            &principal,
            &workspace,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .expect("durable observation");
    assert_eq!(observed, terminal);
    server.verify().await;
}
