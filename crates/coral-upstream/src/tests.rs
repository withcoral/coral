use std::collections::BTreeMap;
use std::time::Duration;

use reqwest::header::CONTENT_TYPE;
use serde_json::{Map, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use url::Url;
use wiremock::matchers::{body_partial_json, header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::http::{DEFAULT_USER_AGENT, MAX_PROVIDER_RESPONSE_BYTES, upstream_http_client};
use crate::mcp::{
    MCP_PROTOCOL_VERSION, MCP_PROTOCOL_VERSION_HEADER, MCP_SESSION_ID_HEADER, McpHttpSession,
};
use crate::{
    GraphqlUpstreamResponse, HttpRequestPlan, McpConnectionTarget, McpContentBlock,
    McpToolCallPlan, McpUpstreamResponse, ProviderErrorKind, RedactableString, UpstreamError,
    UpstreamInvocationPlan, UpstreamResponseEnvelope, execute_plan, list_mcp_tools,
};

#[test]
fn redactable_string_does_not_debug_secret() {
    let secret = RedactableString::new("super-secret");
    assert_eq!(format!("{secret:?}"), "[REDACTED]");
    assert_eq!(secret.expose_secret(), "super-secret");
}

#[test]
fn graphql_media_type_classifies_errors_by_body() {
    let error = GraphqlUpstreamResponse::from_http_json(
        400,
        Some("application/graphql-response+json"),
        BTreeMap::new(),
        br#"{"data":{"viewer":null},"errors":[{"message":"bad"}]}"#,
    )
    .expect_err("graphql errors fail");
    match error {
        UpstreamError::Provider { kind, detail } => {
            assert_eq!(kind, ProviderErrorKind::GraphqlError);
            assert!(detail.contains("partial_data"));
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn graphql_success_status_preserves_data_with_errors() {
    let response = GraphqlUpstreamResponse::from_http_json(
            200,
            Some("application/json"),
            BTreeMap::new(),
            br#"{"data":{"issue":null},"errors":[{"message":"Could not find issue","path":["issue"]}]}"#,
        )
        .expect("2xx GraphQL data with errors should stay classifyable by app policy");

    assert_eq!(response.data, Some(serde_json::json!({ "issue": null })));
    assert_eq!(
        response.partial_data,
        Some(serde_json::json!({ "issue": null }))
    );
    assert_eq!(response.errors.len(), 1);
}

#[test]
fn graphql_http_error_without_graphql_body_is_http_error() {
    let error = GraphqlUpstreamResponse::from_http_json(
        500,
        Some("text/plain"),
        BTreeMap::new(),
        b"server error",
    )
    .expect_err("http error");
    assert!(matches!(
        error,
        UpstreamError::Provider {
            kind: ProviderErrorKind::HttpError,
            ..
        }
    ));
}

#[test]
fn mcp_is_error_preserves_tool_result_payload_in_provider_detail() {
    let error = McpUpstreamResponse {
        structured_content: Some(serde_json::json!({ "code": "invalid_input" })),
        content: vec![McpContentBlock::Text {
            text: "bad input".to_string(),
        }],
        is_error: true,
        meta: Some(serde_json::json!({ "trace": "provider-trace-id" })),
        response_trust: coral_capabilities::ResponseTrust::UntrustedProviderData,
    }
    .into_success()
    .expect_err("isError=true should fail closed");

    let UpstreamError::Provider { kind, detail } = error else {
        panic!("unexpected error: {error}");
    };
    assert_eq!(kind, ProviderErrorKind::ToolError);
    let detail: Value = serde_json::from_str(&detail).expect("structured provider detail");
    assert_eq!(
        detail.pointer("/message").and_then(Value::as_str),
        Some("upstream MCP tool returned isError=true")
    );
    assert_eq!(
        detail
            .pointer("/mcp_tool_result/structuredContent/code")
            .and_then(Value::as_str),
        Some("invalid_input")
    );
    assert_eq!(
        detail
            .pointer("/mcp_tool_result/content/0/text")
            .and_then(Value::as_str),
        Some("bad input")
    );
    assert_eq!(
        detail
            .pointer("/mcp_tool_result/_meta/trace")
            .and_then(Value::as_str),
        Some("provider-trace-id")
    );
}

#[tokio::test]
async fn http_requests_include_default_user_agent_when_missing() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(header("user-agent", DEFAULT_USER_AGENT))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": true
        })))
        .mount(&server)
        .await;

    execute_plan(&UpstreamInvocationPlan::Http(HttpRequestPlan {
        method: coral_capabilities::HttpMethod::Get,
        url: server.uri().parse().expect("mock server URL"),
        headers: Vec::new(),
        body: None,
        timeout: None,
        trace_labels: BTreeMap::new(),
    }))
    .await
    .expect("HTTP request should include default User-Agent");
}

#[tokio::test]
async fn http_requests_preserve_explicit_user_agent() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(header("user-agent", "custom-coral-agent"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": true
        })))
        .mount(&server)
        .await;

    execute_plan(&UpstreamInvocationPlan::Http(HttpRequestPlan {
        method: coral_capabilities::HttpMethod::Get,
        url: server.uri().parse().expect("mock server URL"),
        headers: vec![(
            "User-Agent".to_string(),
            RedactableString::new("custom-coral-agent"),
        )],
        body: None,
        timeout: None,
        trace_labels: BTreeMap::new(),
    }))
    .await
    .expect("HTTP request should preserve explicit User-Agent");
}

#[tokio::test]
async fn executes_streamable_http_mcp_tools_call() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_partial_json(serde_json::json!({
            "method": "initialize",
        })))
        .and(header(MCP_PROTOCOL_VERSION_HEADER, MCP_PROTOCOL_VERSION))
        .respond_with(
            ResponseTemplate::new(200)
                .append_header(CONTENT_TYPE.as_str(), "application/json")
                .append_header(MCP_SESSION_ID_HEADER, "session-123")
                .set_body_json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": {},
                        "serverInfo": {
                            "name": "fixture",
                            "version": "0.1.0",
                        },
                    },
                })),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header(MCP_SESSION_ID_HEADER, "session-123"))
        .and(header(MCP_PROTOCOL_VERSION_HEADER, MCP_PROTOCOL_VERSION))
        .and(body_partial_json(serde_json::json!({
            "method": "notifications/initialized",
        })))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header(MCP_SESSION_ID_HEADER, "session-123"))
        .and(header(MCP_PROTOCOL_VERSION_HEADER, MCP_PROTOCOL_VERSION))
        .and(body_partial_json(serde_json::json!({
            "method": "tools/call",
            "params": {
                "name": "echo",
                "arguments": { "value": "hello" },
            },
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "result": {
                "structuredContent": { "ok": true },
                "content": [{ "type": "text", "text": "done" }],
                "isError": false
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let response = execute_plan(&UpstreamInvocationPlan::McpToolCall(McpToolCallPlan {
        server: McpConnectionTarget::StreamableHttp {
            url: server.uri().parse().expect("mock server URL"),
            headers: Vec::new(),
        },
        tool_name: "echo".to_string(),
        arguments: Map::from_iter([("value".to_string(), serde_json::json!("hello"))]),
        timeout: None,
        trace_labels: BTreeMap::new(),
    }))
    .await
    .expect("MCP tool call should succeed");

    let UpstreamResponseEnvelope::Mcp(response) = response else {
        panic!("expected MCP response envelope");
    };
    assert_eq!(
        response.structured_content,
        Some(serde_json::json!({ "ok": true }))
    );
    assert_eq!(
        response.content,
        vec![McpContentBlock::Text {
            text: "done".to_string()
        }]
    );
}

#[tokio::test]
async fn streamable_http_mcp_tools_list_treats_null_next_cursor_as_terminal() {
    let server = MockServer::start().await;
    mount_streamable_http_mcp_initialize(&server).await;
    Mock::given(method("POST"))
        .and(header(MCP_SESSION_ID_HEADER, "session-123"))
        .and(header(MCP_PROTOCOL_VERSION_HEADER, MCP_PROTOCOL_VERSION))
        .and(body_partial_json(serde_json::json!({
            "method": "tools/list",
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "result": {
                "tools": [
                    {
                        "name": "echo",
                        "description": "Echo",
                        "inputSchema": { "type": "object" }
                    }
                ],
                "nextCursor": null
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let tools = list_mcp_tools(
        &McpConnectionTarget::StreamableHttp {
            url: server.uri().parse().expect("mock server URL"),
            headers: Vec::new(),
        },
        None,
    )
    .await
    .expect("tools/list should treat null nextCursor as terminal");

    assert_eq!(
        tools.pointer("/tools/0/name").and_then(Value::as_str),
        Some("echo")
    );
    assert_eq!(
        tools
            .pointer("/tools")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(1)
    );
}

#[tokio::test]
async fn streamable_http_mcp_tools_list_rejects_non_string_next_cursor() {
    let server = MockServer::start().await;
    mount_streamable_http_mcp_initialize(&server).await;
    Mock::given(method("POST"))
        .and(header(MCP_SESSION_ID_HEADER, "session-123"))
        .and(header(MCP_PROTOCOL_VERSION_HEADER, MCP_PROTOCOL_VERSION))
        .and(body_partial_json(serde_json::json!({
            "method": "tools/list",
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "result": {
                "tools": [],
                "nextCursor": 7
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let error = list_mcp_tools(
        &McpConnectionTarget::StreamableHttp {
            url: server.uri().parse().expect("mock server URL"),
            headers: Vec::new(),
        },
        None,
    )
    .await
    .expect_err("numeric nextCursor should fail");

    assert!(
        error.to_string().contains("nextCursor must be a string"),
        "{error}"
    );
}

async fn mount_streamable_http_mcp_initialize(server: &MockServer) {
    Mock::given(method("POST"))
        .and(body_partial_json(serde_json::json!({
            "method": "initialize",
        })))
        .and(header(MCP_PROTOCOL_VERSION_HEADER, MCP_PROTOCOL_VERSION))
        .respond_with(
            ResponseTemplate::new(200)
                .append_header(CONTENT_TYPE.as_str(), "application/json")
                .append_header(MCP_SESSION_ID_HEADER, "session-123")
                .set_body_json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": {},
                        "serverInfo": {
                            "name": "fixture",
                            "version": "0.1.0",
                        },
                    },
                })),
        )
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(header(MCP_SESSION_ID_HEADER, "session-123"))
        .and(header(MCP_PROTOCOL_VERSION_HEADER, MCP_PROTOCOL_VERSION))
        .and(body_partial_json(serde_json::json!({
            "method": "notifications/initialized",
        })))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn streamable_http_mcp_sse_returns_before_response_eof() {
    let event = concat!(
        "event: message\n",
        "data: {\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"ok\":true}}\n\n"
    );
    let url = open_sse_response_server(event).await;
    let headers = Vec::new();
    let session = McpHttpSession {
        client: upstream_http_client().expect("client"),
        url: &url,
        headers: &headers,
        timeout: Some(Duration::from_secs(5)),
        session_id: None,
        next_id: 1,
    };

    let response = tokio::time::timeout(
        Duration::from_millis(500),
        session.post_json(
            &serde_json::json!({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/list",
            }),
            true,
        ),
    )
    .await
    .expect("SSE response should not wait for EOF")
    .expect("SSE response should decode");

    assert_eq!(
        response
            .value
            .and_then(|value| value.pointer("/result/ok").cloned()),
        Some(serde_json::json!(true))
    );
}

#[tokio::test]
async fn lists_stdio_mcp_tools() {
    let tools = list_mcp_tools(&stdio_mcp_fixture_server(), Some(Duration::from_secs(5)))
        .await
        .expect("stdio tools/list should succeed");

    assert_eq!(
        tools.pointer("/tools/0/name").and_then(Value::as_str),
        Some("echo")
    );
    assert_eq!(
        tools
            .pointer("/tools/0/inputSchema/type")
            .and_then(Value::as_str),
        Some("object")
    );
}

#[tokio::test]
async fn stdio_mcp_child_gets_declared_env_without_inheriting_ambient_env() {
    let script = r#"
while IFS= read -r line; do
  id=${line#*\"id\":}
  id=${id%%,*}
  case "$line" in
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"fixture","version":"0.1.0"}}}\n' "$id"
      ;;
    *'"method":"tools/list"'*)
      if [ -n "${HOME:-}" ]; then
        tool_name="leaked_home"
      elif [ "${DECLARED_ONLY:-}" != "ok" ]; then
        tool_name="missing_declared"
      else
        tool_name="echo"
      fi
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"%s","description":"Env","inputSchema":{"type":"object"}}]}}\n' "$id" "$tool_name"
      ;;
  esac
done
"#;
    let server = McpConnectionTarget::Stdio {
        command: "/bin/sh".to_string(),
        args: vec!["-c".to_string(), script.to_string()],
        env: vec![("DECLARED_ONLY".to_string(), RedactableString::new("ok"))],
    };

    let tools = list_mcp_tools(&server, Some(Duration::from_secs(5)))
        .await
        .expect("stdio tools/list should succeed");

    assert_eq!(
        tools.pointer("/tools/0/name").and_then(Value::as_str),
        Some("echo")
    );
}

#[tokio::test]
async fn executes_stdio_mcp_tools_call() {
    let response = execute_plan(&UpstreamInvocationPlan::McpToolCall(McpToolCallPlan {
        server: stdio_mcp_fixture_server(),
        tool_name: "echo".to_string(),
        arguments: Map::from_iter([("value".to_string(), serde_json::json!("hello"))]),
        timeout: Some(Duration::from_secs(5)),
        trace_labels: BTreeMap::new(),
    }))
    .await
    .expect("stdio MCP tool call should succeed");

    let UpstreamResponseEnvelope::Mcp(response) = response else {
        panic!("expected MCP response envelope");
    };
    assert_eq!(
        response.structured_content,
        Some(serde_json::json!({ "ok": true }))
    );
    assert_eq!(
        response.content,
        vec![McpContentBlock::Text {
            text: "done".to_string()
        }]
    );
}

async fn open_sse_response_server(event: &'static str) -> Url {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind SSE fixture server");
    let addr = listener.local_addr().expect("fixture server address");
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = Vec::new();
        let mut buffer = [0; 1024];
        loop {
            let read = stream.read(&mut buffer).await.expect("read request");
            if read == 0 {
                return;
            }
            request.extend_from_slice(
                buffer
                    .get(..read)
                    .expect("read byte count must be within the fixture buffer"),
            );
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n",
                )
                .await
                .expect("write response headers");
        let chunk = format!("{:x}\r\n{event}\r\n", event.len());
        stream
            .write_all(chunk.as_bytes())
            .await
            .expect("write SSE chunk");
        stream.flush().await.expect("flush SSE chunk");
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    format!("http://{addr}").parse().expect("fixture URL")
}

fn stdio_mcp_fixture_server() -> McpConnectionTarget {
    let script = r#"
while IFS= read -r line; do
  id=${line#*\"id\":}
  id=${id%%,*}
  case "$line" in
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"fixture","version":"0.1.0"}}}\n' "$id"
      ;;
    *'"method":"tools/list"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"echo","description":"Echo","inputSchema":{"type":"object"}}]}}\n' "$id"
      ;;
    *'"method":"tools/call"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"structuredContent":{"ok":true},"content":[{"type":"text","text":"done"}],"isError":false}}\n' "$id"
      ;;
  esac
done
"#;
    McpConnectionTarget::Stdio {
        command: "/bin/sh".to_string(),
        args: vec!["-c".to_string(), script.to_string()],
        env: Vec::new(),
    }
}

#[tokio::test]
async fn rejects_http_provider_responses_above_size_limit() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .append_header("Content-Type", "text/plain")
                .set_body_bytes(vec![b'x'; MAX_PROVIDER_RESPONSE_BYTES + 1]),
        )
        .mount(&server)
        .await;

    let error = execute_plan(&UpstreamInvocationPlan::Http(HttpRequestPlan {
        method: coral_capabilities::HttpMethod::Get,
        url: server.uri().parse().expect("mock server URL"),
        headers: Vec::new(),
        body: None,
        timeout: None,
        trace_labels: BTreeMap::new(),
    }))
    .await
    .expect_err("oversized response should fail");

    match error {
        UpstreamError::Provider { kind, detail } => {
            assert_eq!(kind, ProviderErrorKind::InvalidResponse);
            assert!(detail.contains("exceeds"));
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn does_not_follow_provider_redirects_with_request_headers() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .append_header("Location", "https://evil.example/steal")
                .set_body_string("redirect"),
        )
        .mount(&server)
        .await;

    let error = execute_plan(&UpstreamInvocationPlan::Http(HttpRequestPlan {
        method: coral_capabilities::HttpMethod::Get,
        url: server.uri().parse().expect("mock server URL"),
        headers: vec![(
            "Authorization".to_string(),
            RedactableString::new("Bearer secret"),
        )],
        body: None,
        timeout: None,
        trace_labels: BTreeMap::new(),
    }))
    .await
    .expect_err("redirect should not be followed");

    match error {
        UpstreamError::Provider { kind, detail } => {
            assert_eq!(kind, ProviderErrorKind::HttpError);
            assert!(detail.contains("302"));
        }
        other => panic!("unexpected error: {other}"),
    }
}
