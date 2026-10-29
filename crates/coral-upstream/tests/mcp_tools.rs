//! MCP wire evidence through the upstream Adapter's public interface.
#![allow(
    unused_crate_dependencies,
    reason = "integration targets inherit library dependencies"
)]
#![expect(clippy::indexing_slicing, reason = "assert known fixture JSON members")]

use coral_catalog::AuthenticationConfiguration;
use coral_catalog::{MaterializationOutcome, McpDiscoverySnapshot, ProviderSetupInput};
use coral_outbound::DestinationPolicy;
use coral_upstream::{McpRejection, McpResources, McpUpstream, ReadyMcpDispatch};
use serde_json::{Value, json};
use std::sync::Arc;
use wiremock::matchers::{body_partial_json, method};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn catalog(url: &str) -> coral_catalog::PreparedProviderCatalog {
    catalog_with_contract(url, None)
}

fn catalog_with_contract(
    url: &str,
    credential_contract: Option<Value>,
) -> coral_catalog::PreparedProviderCatalog {
    let mut snapshot = json!({
        "protocolVersion": "2025-11-25",
        "serverInfo": {"name": "fixture", "version": "1"},
        "server": {"transport": "streamable_http", "url": url},
        "capabilities": {"tools": {}, "resources": {}, "tasks": {"requests": {"tools": {"call": {}}}}},
        "tools": [{
            "name": "mixed", "inputSchema": {"type": "object"},
            "outputSchema": {"type": "object", "properties": {"count": {"type": "integer"}}},
            "execution": {"taskSupport": "optional"},
            "annotations": {"readOnlyHint": true}
        }]
    });
    if let Some(contract) = credential_contract {
        snapshot["credentialContract"] = contract;
    }
    let snapshot = McpDiscoverySnapshot::parse(&snapshot.to_string()).expect("snapshot");
    match coral_catalog::materialize(
        DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        &ProviderSetupInput::from(snapshot),
        &[],
    ) {
        MaterializationOutcome::Ready(catalog) => catalog,
        MaterializationOutcome::Rejected(reason) => panic!("{}", reason.message()),
    }
}

async fn server(result: Value) -> MockServer {
    server_with_response(
        move |request: &Request| {
            let request: Value = request.body_json().expect("tool call");
            assert_eq!(request["params"]["name"], "mixed");
            assert_eq!(request["params"]["arguments"], json!({"page": 2}));
            assert!(request["params"].get("task").is_none());
            assert!(request["params"].get("inputResponses").is_none());
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0", "id": request["id"], "result": result
            }))
        },
        false,
    )
    .await
}

async fn server_with_response(
    response: impl wiremock::Respond + 'static,
    session: bool,
) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method": "initialize"})))
        .respond_with(move |request: &Request| {
            let request: Value = request.body_json().expect("initialize");
            assert_eq!(request["params"]["capabilities"], json!({}));
            let response = ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0", "id": request["id"],
                "result": {"protocolVersion": "2025-11-25", "capabilities": {"tools": {}, "resources": {}},
                           "serverInfo": {"name": "fixture", "version": "1"}}
            }));
            if session { response.insert_header("mcp-session-id", "private-session") } else { response }
        })
        .mount(&server).await;
    Mock::given(method("POST"))
        .and(body_partial_json(
            json!({"method": "notifications/initialized"}),
        ))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method": "tools/call"})))
        .respond_with(response)
        .expect(1)
        .mount(&server)
        .await;
    server
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one resource-link scenario asserts projection, provenance and explicit read"
)]
async fn resource_references_preserve_position_and_only_read_with_the_issuing_provider() {
    let server = server(json!({"content": [
        {"type": "text", "text": "before"},
        {"type": "resource_link", "uri": "private://bucket/secret?token=hidden", "name": "report", "title": "Report", "description": "Read separately", "mimeType": "text/plain", "size": 123,
         "annotations": {"priority": 0.7}, "_meta": {"vendor/version": 2}},
        {"type": "resource", "resource": {"uri": "private://inline/secret", "text": "inline", "mimeType": "text/plain", "_meta": {"vendor/encoding": "utf-8"}}},
        {"type": "text", "text": "after"}
    ]})).await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method": "resources/read"})))
        .respond_with(|request: &Request| {
            let request: Value = request.body_json().expect("read request");
            assert_eq!(request["params"]["uri"], "private://bucket/secret?token=hidden");
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0", "id": request["id"], "result": {
                    "contents": [{"uri": "private://bucket/secret?token=hidden", "text": "report body", "mimeType": "text/plain", "_meta": {"vendor/page": 1}}],
                    "_meta": {"vendor/read": true}, "content": ["read extension, not a tool block"], "ttlMs": "provider extension",
                    "isError": true
                }
            }))
        }).expect(1).mount(&server).await;
    let catalog = catalog(&server.uri());
    let tool = catalog
        .tools()
        .iter()
        .find(|op| op.key().as_str() == "mixed")
        .expect("tool");
    let read = catalog
        .tools()
        .iter()
        .find(|op| op.key().as_str() == "resources/read")
        .expect("resource Tool");
    let resources = Arc::new(McpResources::default());
    let upstream = McpUpstream::new(
        DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
    )
    .expect("client");
    let dispatch = ReadyMcpDispatch::plan(
        tool,
        &json!({"page": 2}),
        "provider-a",
        resources.clone(),
        coral_value::ValueLimits::default(),
    )
    .expect("plan");
    let outcome = upstream.send(dispatch).await.expect("links");
    let content = &outcome.tool_result()["data"]["content"];
    assert_eq!(content[0], json!({"type": "text", "text": "before"}));
    assert_eq!(content[1]["name"], "report");
    assert_eq!(content[1]["title"], "Report");
    assert_eq!(content[1]["description"], "Read separately");
    assert_eq!(content[1]["mimeType"], "text/plain");
    assert_eq!(content[1]["size"], 123);
    assert_eq!(content[1]["annotations"], json!({"priority": 0.7}));
    assert_eq!(content[1]["_meta"], json!({"vendor/version": 2}));
    assert_eq!(content[2]["resource"]["text"], "inline");
    assert_eq!(content[3], json!({"type": "text", "text": "after"}));
    assert!(!outcome.tool_result().to_string().contains("private://"));
    assert_eq!(
        server.received_requests().await.expect("requests").len(),
        3,
        "links are never auto-read"
    );
    let args = json!({"reference": content[1]["resource"]});
    for invalid in [
        json!({"reference": "private://bucket/secret?token=hidden"}),
        json!({"uri": "private://bucket/secret"}),
        json!({"reference": {"$coralMcpResource": "forged"}}),
    ] {
        assert!(
            ReadyMcpDispatch::plan(
                read,
                &invalid,
                "provider-a",
                resources.clone(),
                coral_value::ValueLimits::default()
            )
            .is_err()
        );
    }
    assert!(
        ReadyMcpDispatch::plan(
            read,
            &args,
            "provider-b",
            resources.clone(),
            coral_value::ValueLimits::default()
        )
        .is_err(),
        "same server URL does not mean same Provider"
    );
    assert!(
        ReadyMcpDispatch::plan(
            read,
            &args,
            "provider-a",
            Arc::new(McpResources::default()),
            coral_value::ValueLimits::default()
        )
        .is_err(),
        "another Run has no authority to use the reference"
    );
    let dispatch = ReadyMcpDispatch::plan(
        read,
        &args,
        "provider-a",
        resources,
        coral_value::ValueLimits::default(),
    )
    .expect("exact Provider reference");
    let outcome = upstream.send(dispatch).await.expect("resource read");
    // `isError` belongs to tool results; on a read it is an extension field.
    assert_eq!(outcome.class(), coral_upstream::OutcomeClass::Success);
    assert_eq!(
        outcome.tool_result()["data"]["contents"][0]["text"],
        "report body"
    );
    assert_eq!(
        outcome.tool_result()["data"]["contents"][0]["_meta"],
        json!({"vendor/page": 1})
    );
    assert_eq!(
        outcome.tool_result()["metadata"],
        json!({"kind": "mcp", "_meta": {"vendor/read": true}})
    );
    assert_eq!(
        outcome.tool_result()["data"]["content"],
        json!(["read extension, not a tool block"])
    );
    assert_eq!(outcome.tool_result()["data"]["ttlMs"], "provider extension");
    assert!(!outcome.tool_result().to_string().contains("private://"));
    assert_eq!(server.received_requests().await.expect("requests").len(), 6);
}

async fn call_once(
    server: &MockServer,
    limit: usize,
) -> Result<coral_upstream::McpOutcome, coral_upstream::Error> {
    let catalog = catalog(&server.uri());
    let tool = catalog
        .tools()
        .iter()
        .find(|op| op.key().as_str() == "mixed")
        .expect("tool");
    let dispatch = ReadyMcpDispatch::plan(
        tool,
        &json!({"page": 2}),
        "provider-a",
        Arc::new(McpResources::default()),
        coral_value::ValueLimits::new(8 << 20, 16 << 20, limit, 64 << 20).expect("limits"),
    )
    .expect("plan");
    McpUpstream::new(
        DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
    )
    .expect("client")
    .send(dispatch)
    .await
}

#[tokio::test]
async fn request_associated_sse_preserves_an_empty_complete_result_and_absent_fields() {
    let server = server_with_response(
        |request: &Request| {
            let request: Value = request.body_json().expect("call");
            let result = json!({"jsonrpc": "2.0", "id": request["id"], "result": {"content": []}});
            ResponseTemplate::new(200).set_body_raw(
                format!(": heartbeat\r\n\r\nevent: message\r\ndata: {result}\r\n\r\n"),
                "text/event-stream",
            )
        },
        true,
    )
    .await;
    let outcome = call_once(&server, 4096)
        .await
        .expect("SSE complete response");
    assert_eq!(
        outcome.tool_result(),
        &json!({"data": {"content": []}, "metadata": {"kind": "mcp"}})
    );
    server.verify().await;
    let requests = server.received_requests().await.expect("requests");
    assert!(
        requests.iter().all(|request| request.method != "GET"),
        "no unsolicited resource stream"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request
                .body_json::<Value>()
                .is_ok_and(|value| value["method"] == "tools/call"))
            .count(),
        1
    );
}

#[test]
fn mcp_request_limit_includes_the_wire_envelope_before_dispatch() {
    let catalog = catalog("http://127.0.0.1:1/mcp");
    let tool = catalog
        .tools()
        .iter()
        .find(|op| op.key().as_str() == "mixed")
        .expect("Tool");
    let arguments = json!({"text":"x".repeat(950)});
    assert!(arguments.to_string().len() < 1024);
    assert!(matches!(
        ReadyMcpDispatch::plan(
            tool,
            &arguments,
            "provider-a",
            Arc::new(McpResources::default()),
            coral_value::ValueLimits::new(512, 2048, 1024, 4096).expect("limits")
        ),
        Err(coral_upstream::Error::InputTooLarge)
    ));
}

#[tokio::test]
async fn mcp_request_at_the_wire_byte_limit_is_sent_once() {
    let server = server_with_response(
        |request: &Request| {
            let request: Value = request.body_json().expect("request");
            ResponseTemplate::new(200).set_body_json(
                json!({"jsonrpc":"2.0", "id":request["id"], "result":{"content":[]}}),
            )
        },
        true,
    )
    .await;
    let catalog = catalog(&server.uri());
    let tool = catalog
        .tools()
        .iter()
        .find(|op| op.key().as_str() == "mixed")
        .expect("Tool");
    let empty_wire = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"mixed","arguments":{"text":""},"_meta":{"progressToken":0}}}"#;
    let arguments = json!({"text":"x".repeat(1024 - empty_wire.len())});
    let limits = coral_value::ValueLimits::new(512, 2048, 1024, 4096).expect("limits");
    let dispatch = ReadyMcpDispatch::plan(
        tool,
        &arguments,
        "provider-a",
        Arc::new(McpResources::default()),
        limits,
    )
    .expect("exact boundary accepted");
    McpUpstream::new(
        DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
    )
    .expect("client")
    .send(dispatch)
    .await
    .expect("complete outcome");
    let requests = server.received_requests().await.expect("requests");
    let calls = requests
        .iter()
        .filter(|request| {
            request
                .body_json::<Value>()
                .is_ok_and(|value| value["method"] == "tools/call")
        })
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls.first().expect("call").body.len(), 1024);
    let oversized = json!({"text":"x".repeat(1025 - empty_wire.len())});
    assert!(matches!(
        ReadyMcpDispatch::plan(
            tool,
            &oversized,
            "provider-a",
            Arc::new(McpResources::default()),
            limits
        ),
        Err(coral_upstream::Error::InputTooLarge)
    ));
}

#[tokio::test]
async fn oversized_json_and_sse_results_keep_the_size_failure_category() {
    for media in ["application/json", "text/event-stream"] {
        let server = server_with_response(
            ResponseTemplate::new(200).set_body_raw("x".repeat(5000), media),
            true,
        )
        .await;
        assert!(matches!(
            call_once(&server, 4096).await,
            Err(coral_upstream::Error::ResultTooLarge)
        ));
        server.verify().await;
    }
}

#[tokio::test]
async fn expired_sessions_protocol_errors_and_oversized_results_never_repeat_the_call() {
    for response in [
        ResponseTemplate::new(404),
        ResponseTemplate::new(503),
        ResponseTemplate::new(200).set_body_raw("not JSON", "application/json"),
        ResponseTemplate::new(200).set_body_raw("x".repeat(5000), "application/json"),
        ResponseTemplate::new(200)
            .set_body_raw(format!("data: {}", "x".repeat(5000)), "text/event-stream"),
    ] {
        let server = server_with_response(response, true).await;
        call_once(&server, 4096)
            .await
            .expect_err("no complete result");
        server.verify().await;
        let requests = server.received_requests().await.expect("requests");
        assert_eq!(
            requests
                .iter()
                .filter(|request| request
                    .body_json::<Value>()
                    .is_ok_and(|value| value["method"] == "initialize"))
                .count(),
            1,
            "expired session must not cause reinitialization and replay"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request
                    .body_json::<Value>()
                    .is_ok_and(|value| value["method"] == "tools/call"))
                .count(),
            1
        );
    }
}

/// What the peer's `tools/list` says about the called tool once `tools/call`
/// is refused with JSON-RPC `-32602`.
#[derive(Clone, Copy, Debug)]
enum ToolListing {
    Lists,
    ListsOnSecondPage,
    Omits,
    Fails,
}

async fn mount_tool_listing(server: &MockServer, listing: ToolListing) {
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method": "tools/list"})))
        .respond_with(move |request: &Request| {
            let request: Value = request.body_json().expect("tools/list");
            let tools = |names: &[&str]| -> Vec<Value> {
                names
                    .iter()
                    .map(|name| json!({"name": name, "inputSchema": {"type": "object"}}))
                    .collect()
            };
            let result = match listing {
                ToolListing::Fails => return ResponseTemplate::new(500),
                ToolListing::Lists => json!({"tools": tools(&["other", "mixed"])}),
                ToolListing::ListsOnSecondPage
                    if request["params"]["cursor"] == json!("page-2") =>
                {
                    json!({"tools": tools(&["mixed"])})
                }
                ToolListing::ListsOnSecondPage => {
                    json!({"tools": tools(&["other"]), "nextCursor": "page-2"})
                }
                ToolListing::Omits => json!({"tools": tools(&["other"])}),
            };
            ResponseTemplate::new(200)
                .set_body_json(json!({"jsonrpc": "2.0", "id": request["id"], "result": result}))
        })
        .mount(server)
        .await;
}

async fn requests_for(server: &MockServer, method: &str) -> usize {
    server
        .received_requests()
        .await
        .expect("requests")
        .iter()
        .filter(|request| {
            request
                .body_json::<Value>()
                .is_ok_and(|value| value["method"] == method)
        })
        .count()
}

#[tokio::test]
async fn json_rpc_errors_are_typed_rejections_that_never_repeat_the_call() {
    for (code, sse, listing, expected, listings) in [
        (
            -32602,
            false,
            ToolListing::Lists,
            McpRejection::InvalidParams,
            1,
        ),
        (
            -32602,
            true,
            ToolListing::Lists,
            McpRejection::InvalidParams,
            1,
        ),
        (
            -32602,
            false,
            ToolListing::ListsOnSecondPage,
            McpRejection::InvalidParams,
            2,
        ),
        (
            -32602,
            false,
            ToolListing::Omits,
            McpRejection::UnknownTool,
            1,
        ),
        // An unreadable listing keeps the peer's own answer.
        (
            -32602,
            false,
            ToolListing::Fails,
            McpRejection::InvalidParams,
            1,
        ),
        (
            -32603,
            false,
            ToolListing::Lists,
            McpRejection::Other(-32603),
            0,
        ),
    ] {
        let server = server_with_response(
            move |request: &Request| {
                let request: Value = request.body_json().expect("call");
                let envelope = json!({"jsonrpc": "2.0", "id": request["id"], "error": {
                    "code": code, "message": "Invalid params: message-canary",
                    "data": {"field": "zzzUnknown", "secret": "data-canary"}
                }});
                if sse {
                    ResponseTemplate::new(200)
                        .set_body_raw(format!("data: {envelope}\n\n"), "text/event-stream")
                } else {
                    ResponseTemplate::new(200).set_body_json(envelope)
                }
            },
            true,
        )
        .await;
        mount_tool_listing(&server, listing).await;
        let error = call_once(&server, 4096)
            .await
            .expect_err("a JSON-RPC error is an answer, not a result");
        assert!(
            matches!(error, coral_upstream::Error::McpRejected(rejection) if rejection == expected),
            "{listing:?}: {error:?}"
        );
        assert!(
            !error.to_string().contains("canary"),
            "the peer's error payload never leaves the Adapter: {error}"
        );
        server.verify().await;
        assert_eq!(
            requests_for(&server, "tools/call").await,
            1,
            "{listing:?}: a rejection is never retried"
        );
        assert_eq!(
            requests_for(&server, "tools/list").await,
            listings,
            "{listing:?}: only a -32602 on tools/call reads the listing"
        );
    }
}

#[tokio::test]
async fn a_json_rpc_error_on_resources_read_is_a_typed_rejection_without_a_listing_read() {
    for (code, expected) in [
        (-32602, McpRejection::InvalidParams),
        (-32603, McpRejection::Other(-32603)),
    ] {
        let server = server(json!({"content": [
            {"type": "resource_link", "uri": "private://bucket/report", "name": "report"}
        ]}))
        .await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({"method": "resources/read"})))
            .respond_with(move |request: &Request| {
                let request: Value = request.body_json().expect("read request");
                ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0", "id": request["id"], "error": {
                        "code": code, "message": "Resource not found: message-canary",
                        "data": {"uri": "private://bucket/report", "secret": "data-canary"}
                    }
                }))
            })
            .expect(1)
            .mount(&server)
            .await;
        let catalog = catalog(&server.uri());
        let tool = catalog
            .tools()
            .iter()
            .find(|op| op.key().as_str() == "mixed")
            .expect("tool");
        let read = catalog
            .tools()
            .iter()
            .find(|op| op.key().as_str() == "resources/read")
            .expect("resource Tool");
        let resources = Arc::new(McpResources::default());
        let upstream = McpUpstream::new(
            DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        )
        .expect("client");
        let dispatch = ReadyMcpDispatch::plan(
            tool,
            &json!({"page": 2}),
            "provider-a",
            resources.clone(),
            coral_value::ValueLimits::default(),
        )
        .expect("plan");
        let outcome = upstream.send(dispatch).await.expect("link");
        let args = json!({"reference": outcome.tool_result()["data"]["content"][0]["resource"]});
        let dispatch = ReadyMcpDispatch::plan(
            read,
            &args,
            "provider-a",
            resources,
            coral_value::ValueLimits::default(),
        )
        .expect("reference");
        let error = upstream
            .send(dispatch)
            .await
            .expect_err("a JSON-RPC error is an answer, not a result");
        assert!(
            matches!(error, coral_upstream::Error::McpRejected(rejection) if rejection == expected),
            "{code}: {error:?}"
        );
        assert!(
            !error.to_string().contains("canary"),
            "the peer's error payload never leaves the Adapter: {error}"
        );
        server.verify().await;
        assert_eq!(
            requests_for(&server, "resources/read").await,
            1,
            "{code}: a rejection is never retried"
        );
        assert_eq!(
            requests_for(&server, "tools/list").await,
            0,
            "{code}: only a tools/call rejection reads the listing"
        );
    }
}

#[tokio::test]
async fn a_malformed_json_rpc_error_is_not_a_rejection() {
    for error in [
        json!({"code": -32602}),
        json!({"message": "no code"}),
        json!({"code": "-32602", "message": "code must be an integer"}),
    ] {
        let server = server_with_response(
            move |request: &Request| {
                let request: Value = request.body_json().expect("call");
                ResponseTemplate::new(200)
                    .set_body_json(json!({"jsonrpc": "2.0", "id": request["id"], "error": error}))
            },
            false,
        )
        .await;
        let error = call_once(&server, 4096)
            .await
            .expect_err("a malformed error envelope is no answer");
        assert!(
            matches!(error, coral_upstream::Error::McpRequest),
            "{error:?}"
        );
        server.verify().await;
    }
}

#[tokio::test]
async fn a_refused_initialize_is_an_incomplete_request_not_a_rejection() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(|request: &Request| {
            let request: Value = request.body_json().expect("json-rpc");
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0", "id": request["id"],
                "error": {"code": -32602, "message": "unsupported protocol version"}
            }))
        })
        .mount(&server)
        .await;
    let error = call_once(&server, 4096)
        .await
        .expect_err("no session was established");
    assert!(
        matches!(error, coral_upstream::Error::McpRequest),
        "{error:?}"
    );
    assert!(
        !server
            .received_requests()
            .await
            .expect("requests")
            .iter()
            .any(|request| request
                .body_json::<Value>()
                .is_ok_and(|value| value["method"] == "tools/call")),
        "the Tool request is never sent without a session"
    );
}

#[tokio::test]
async fn tool_extension_fields_do_not_change_the_result_method() {
    let expected = json!({"content": [], "isError": true, "tools": [], "resources": [], "contents": ["extra"], "resultType": "provider extension"});
    for sse in [false, true] {
        let response = expected.clone();
        let server = server_with_response(
            move |request: &Request| {
                let request: Value = request.body_json().expect("call");
                let envelope = json!({"jsonrpc": "2.0", "id": request["id"], "result": response});
                if sse {
                    ResponseTemplate::new(200)
                        .set_body_raw(format!("data: {envelope}\n\n"), "text/event-stream")
                } else {
                    ResponseTemplate::new(200).set_body_json(envelope)
                }
            },
            false,
        )
        .await;
        let outcome = call_once(&server, 4096)
            .await
            .expect("extensions cannot reclassify a complete tool result");
        assert_eq!(
            outcome.tool_result(),
            &json!({"data": expected, "metadata": {"kind": "mcp"}})
        );
        server.verify().await;
    }
}

#[tokio::test]
async fn incomplete_tool_response_is_not_a_complete_outcome() {
    let server = server(json!({"structuredContent": {"only": "partial"}})).await;
    call_once(&server, 16 * 1024 * 1024)
        .await
        .expect_err("a missing required content field cannot be silently manufactured");
    server.verify().await;
}

#[tokio::test]
async fn complete_tool_outcomes_preserve_content_and_only_is_error_is_an_error_response() {
    for is_error in [None, Some(false), Some(true)] {
        let mut result = json!({
            "content": [
                {"type": "text", "text": "before", "annotations": {"audience": ["assistant"], "priority": 0.7}, "_meta": {"vendor/part": 1}},
                {"type": "image", "data": "AAE=", "mimeType": "image/png"},
                {"type": "audio", "data": "AgM=", "mimeType": "audio/wav"},
                {"type": "text", "text": "after"}
            ],
            "structuredContent": {"count": "not the declared integer", "cursor": "next-page"},
            "_meta": {"vendor/request": "r-123"}
        });
        if let Some(value) = is_error {
            result["isError"] = json!(value);
        }
        let server = server(result.clone()).await;
        let outcome = call_once(&server, 16 * 1024 * 1024)
            .await
            .expect("complete outcome, even isError");
        let meta = result
            .as_object_mut()
            .expect("result")
            .remove("_meta")
            .expect("metadata");
        assert_eq!(
            outcome.tool_result(),
            &json!({"data": result, "metadata": {"kind": "mcp", "_meta": meta}})
        );
        assert_eq!(
            outcome.class(),
            if is_error == Some(true) {
                coral_upstream::OutcomeClass::ErrorResponse
            } else {
                coral_upstream::OutcomeClass::Success
            },
            "isError {is_error:?}"
        );
        server.verify().await;
        // Count JSON-RPC posts only: a session close from an earlier test can land
        // on a reused loopback port before this server's handshake.
        let posts = server
            .received_requests()
            .await
            .expect("requests")
            .iter()
            .filter(|request| request.method == "POST")
            .count();
        assert_eq!(
            posts, 3,
            "handshake and one call only; no discovery, pagination or task requests"
        );
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one fixture proves header injection, missing-credential refusal and contract rejection"
)]
async fn bearer_contract_sends_the_host_credential_and_credential_free_dispatch_refuses_one() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(wiremock::matchers::header(
            "authorization",
            "Bearer dispatch-canary",
        ))
        .and(body_partial_json(json!({"method": "initialize"})))
        .respond_with(|request: &Request| {
            let request: Value = request.body_json().expect("initialize");
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0", "id": request["id"],
                "result": {"protocolVersion": "2025-11-25", "capabilities": {"tools": {}},
                           "serverInfo": {"name": "fixture", "version": "1"}}
            }))
        })
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(wiremock::matchers::header(
            "authorization",
            "Bearer dispatch-canary",
        ))
        .and(body_partial_json(
            json!({"method": "notifications/initialized"}),
        ))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(wiremock::matchers::header(
            "authorization",
            "Bearer dispatch-canary",
        ))
        .and(body_partial_json(json!({"method": "tools/call"})))
        .respond_with(|request: &Request| {
            let request: Value = request.body_json().expect("call");
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0", "id": request["id"], "result": {"content": []}
            }))
        })
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401).insert_header("www-authenticate", "Bearer"))
        .mount(&server)
        .await;
    let url = format!("{}/mcp", server.uri());
    let authenticated = catalog_with_contract(
        &url,
        Some(json!(coral_catalog::ProviderCredentialContract::from(
            AuthenticationConfiguration::Bearer
        ))),
    );
    let tool = authenticated
        .tools()
        .iter()
        .find(|tool| tool.relative_path() == "mixed")
        .expect("tool");
    let resources = Arc::new(McpResources::default());
    let mut ready = ReadyMcpDispatch::plan(
        tool,
        &json!({"page": 2}),
        "provider",
        resources.clone(),
        coral_value::ValueLimits::default(),
    )
    .expect("plan");
    assert!(matches!(
        McpUpstream::new(
            DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        )
        .unwrap()
        .send(
            ReadyMcpDispatch::plan(
                tool,
                &json!({"page": 2}),
                "provider",
                resources.clone(),
                coral_value::ValueLimits::default()
            )
            .unwrap()
        )
        .await,
        Err(coral_upstream::Error::CredentialRequired)
    ));
    ready
        .authorize(&coral_credentials::CredentialValues::token(
            coral_credentials::Secret::new("dispatch-canary".into()).unwrap(),
        ))
        .expect("authorize bearer dispatch");
    let outcome = McpUpstream::new(
        DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
    )
    .unwrap()
    .send(ready)
    .await
    .expect("authenticated call");
    assert_eq!(outcome.tool_result()["data"]["content"], json!([]));

    let credential_free = catalog(&url);
    let tool = credential_free
        .tools()
        .iter()
        .find(|tool| tool.relative_path() == "mixed")
        .expect("tool");
    let mut ready = ReadyMcpDispatch::plan(
        tool,
        &json!({"page": 2}),
        "provider",
        resources,
        coral_value::ValueLimits::default(),
    )
    .expect("plan");
    assert!(matches!(
        ready.authorize(&coral_credentials::CredentialValues::token(
            coral_credentials::Secret::new("dispatch-canary".into()).unwrap()
        )),
        Err(coral_upstream::Error::CredentialRequired)
    ));
}

#[tokio::test]
async fn mcp_tools_send_confirmed_service_headers_without_credentials() {
    let server = server(json!({"content": []})).await;
    let snapshot = McpDiscoverySnapshot::parse(&json!({
        "protocolVersion": "2025-11-25", "serverInfo": {"name": "fixture", "version": "1"},
        "server": {"transport": "streamable_http", "url": server.uri()},
        "capabilities": {"tools": {}}, "tools": [{"name": "mixed", "inputSchema": {"type": "object"}}]
    }).to_string()).unwrap().restore_configuration(&json!({
        "authentication": {"method": "none"}, "display_name": "Fixture", "servers": {},
        "request_headers": {"x-tenant": "example"}
    }).to_string(), 1).unwrap();
    let policy =
        DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps);
    let MaterializationOutcome::Ready(catalog) =
        coral_catalog::materialize(policy, &snapshot.into(), &[])
    else {
        panic!("catalog");
    };
    let dispatch = ReadyMcpDispatch::plan(
        catalog.tools().first().unwrap(),
        &json!({"page": 2}),
        "provider-a",
        Arc::new(McpResources::default()),
        coral_value::ValueLimits::default(),
    )
    .unwrap();
    McpUpstream::new(policy)
        .unwrap()
        .send(dispatch)
        .await
        .unwrap();
    for request in server.received_requests().await.unwrap() {
        assert_eq!(request.headers.get("x-tenant").unwrap(), "example");
        assert!(!request.headers.contains_key("authorization"));
    }
}

/// An MCP dispatch is one client span of the Invocation that sends it, and
/// every request of its session carries that span's `traceparent` and no
/// other trace context. A tool result with `isError: true` marks only the
/// client span as an error.
#[tokio::test]
async fn an_mcp_dispatch_carries_only_its_client_span_trace_context() {
    use coral_telemetry::testing::attribute;
    use opentelemetry::trace::{SpanKind, Status};
    use tracing::Instrument as _;

    let capture = capture();
    let server = server(json!({"content": [], "isError": true})).await;
    let invocation = tracing::info_span!("coral.invocation");
    let trace = {
        use opentelemetry::trace::TraceContextExt as _;
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;
        invocation.context().span().span_context().trace_id()
    };
    call_once(&server, 16 * 1024 * 1024)
        .instrument(invocation.clone())
        .await
        .expect("a complete isError outcome");
    drop(invocation);

    // The SDK's session task ends, and drops its spans, after the call returns.
    let mut spans = capture.finished();
    for _ in 0..200 {
        if spans
            .iter()
            .any(|span| span.span_context.trace_id() == trace)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        spans = capture.finished();
    }
    let parent = spans
        .iter()
        .find(|span| span.span_context.trace_id() == trace && span.name == "coral.invocation")
        .expect("Invocation span");
    let client = spans
        .iter()
        .find(|span| span.span_context.trace_id() == trace && span.span_kind == SpanKind::Client)
        .expect("MCP client span");
    assert_eq!(client.name, "tools/call mixed");
    assert_eq!(client.parent_span_id, parent.span_context.span_id());
    for (key, value) in [
        ("mcp.method.name", "tools/call"),
        ("gen_ai.tool.name", "mixed"),
        ("server.address", "127.0.0.1"),
        ("error.type", "tool_error"),
    ] {
        assert_eq!(attribute(client, key).as_deref(), Some(value), "{key}");
    }
    assert!(attribute(client, "server.port").is_some());
    assert!(matches!(client.status, Status::Error { .. }));

    let expected = format!(
        "00-{}-{}-01",
        client.span_context.trace_id(),
        client.span_context.span_id()
    );
    let posts = server.received_requests().await.expect("requests");
    let posts = posts
        .iter()
        .filter(|request| request.method == "POST")
        .collect::<Vec<_>>();
    assert_eq!(posts.len(), 3);
    for request in posts {
        assert_eq!(
            request
                .headers
                .get_all("traceparent")
                .iter()
                .map(|value| value.to_str().expect("ASCII"))
                .collect::<Vec<_>>(),
            vec![expected.as_str()]
        );
        assert!(request.headers.get("tracestate").is_none());
        assert!(request.headers.get("baggage").is_none());
    }
}

/// The process-wide span capture: a subscriber set for one test thread
/// misses callsites another test's thread registered first.
fn capture() -> &'static coral_telemetry::testing::SpanCapture {
    static CAPTURE: std::sync::OnceLock<coral_telemetry::testing::SpanCapture> =
        std::sync::OnceLock::new();
    CAPTURE.get_or_init(|| {
        let capture = coral_telemetry::testing::SpanCapture::default();
        tracing::subscriber::set_global_default(capture.subscriber())
            .expect("install the span capture");
        capture
    })
}

/// An MCP dispatch records one exchange: its whole session's request and
/// response body bytes, and a complete outcome, even one with `isError`.
#[tokio::test]
async fn an_mcp_dispatch_records_its_exchange() {
    // Installed before the scoped capture, so no thread keeps none.
    capture();
    let metrics = coral_telemetry::testing::MetricCapture::default();
    let _metrics = tracing::subscriber::set_default(metrics.subscriber());
    let server = server(json!({"content": [], "isError": true})).await;
    call_once(&server, 16 * 1024 * 1024)
        .await
        .expect("a complete isError outcome");

    let sent = server
        .received_requests()
        .await
        .expect("requests")
        .iter()
        .filter(|request| request.method == "POST")
        .map(|request| request.body.len())
        .sum::<usize>();
    let points = |name: &str| {
        metrics
            .named(name)
            .into_iter()
            .map(|point| (point.attributes, point.count, point.value))
            .collect::<Vec<_>>()
    };
    let protocol = |extra: &[(&str, &str)]| {
        [("provider.protocol", "mcp")]
            .iter()
            .chain(extra)
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    let durations = points("coral.upstream.duration");
    let [(attributes, 1, _)] = durations.as_slice() else {
        panic!("one exchange");
    };
    assert_eq!(*attributes, protocol(&[("outcome", "response")]));
    assert_eq!(
        points("coral.upstream.request.size"),
        [(protocol(&[]), 1, f64::from(u32::try_from(sent).expect("small")))]
    );
    let responses = points("coral.upstream.response.size");
    let [(attributes, 1, received)] = responses.as_slice() else {
        panic!("one response size");
    };
    assert_eq!(*attributes, protocol(&[]));
    assert!(*received > 0.0);
}
