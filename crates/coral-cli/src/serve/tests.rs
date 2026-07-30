use std::net::TcpListener;
use std::time::Duration;

use ring::rand::SystemRandom;
use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
use rmcp::ServiceExt as _;
use rmcp::model::CallToolRequestParams;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use tempfile::TempDir;

use super::*;

const INITIALIZE: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#;

fn write_config(temp: &TempDir, config: &str) {
    std::fs::write(temp.path().join("config.toml"), config).expect("write config");
}

fn grpc_addr(server: &RunningServer) -> SocketAddr {
    server
        .endpoint_uri()
        .strip_prefix("http://")
        .expect("HTTP endpoint")
        .parse()
        .expect("socket address")
}

async fn assert_catalog_tool(endpoint: String, bearer: Option<&str>) {
    const INTENT: &str = "Exercise the composite server";

    let mut config = StreamableHttpClientTransportConfig::with_uri(endpoint);
    if let Some(bearer) = bearer {
        config = config.auth_header(bearer);
    }
    let client =
        ().serve(StreamableHttpClientTransport::from_config(config))
            .await
            .expect("initialize MCP client");
    let task = client
        .call_tool(CallToolRequestParams::new("start_task").with_arguments(
            serde_json::Map::from_iter([("intent".to_string(), serde_json::json!(INTENT))]),
        ))
        .await
        .expect("start task");
    let task = task.structured_content.expect("structured task");
    let task_id = task
        .get("task_id")
        .and_then(serde_json::Value::as_str)
        .expect("task ID");
    let result = client
        .call_tool(CallToolRequestParams::new("list_catalog").with_arguments(
            serde_json::Map::from_iter([
                ("task_id".to_string(), serde_json::json!(task_id)),
                ("intent".to_string(), serde_json::json!(INTENT)),
            ]),
        ))
        .await
        .expect("call list_catalog");
    assert_eq!(result.is_error, Some(false));
    client.cancel().await.expect("stop MCP client");
}

async fn assert_feedback_tool(endpoint: String) {
    let config = StreamableHttpClientTransportConfig::with_uri(endpoint);
    let client =
        ().serve(StreamableHttpClientTransport::from_config(config))
            .await
            .expect("initialize MCP client");
    let tools = client.list_all_tools().await.expect("list MCP tools");
    assert!(tools.iter().any(|tool| tool.name == "feedback"));
    client.cancel().await.expect("stop MCP client");
}

async fn assert_unauthorized(base: &str, authorization: &str) {
    let rejected = reqwest::Client::new()
        .post(format!("{base}/mcp"))
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("authorization", authorization)
        .body(INITIALIZE)
        .send()
        .await
        .expect("MCP response");
    assert_eq!(rejected.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[test]
fn loopback_grpc_endpoint_maps_wildcards_and_rejects_public_addresses() {
    assert_eq!(
        loopback_grpc_endpoint_uri(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 14555)))
            .expect("IPv4 wildcard"),
        "http://127.0.0.1:14555"
    );
    assert_eq!(
        loopback_grpc_endpoint_uri(SocketAddr::from((Ipv6Addr::UNSPECIFIED, 14555)))
            .expect("IPv6 wildcard"),
        "http://[::1]:14555"
    );
    loopback_grpc_endpoint_uri(SocketAddr::from(([192, 0, 2, 1], 14555)))
        .expect_err("public address must be rejected");
    loopback_grpc_endpoint_uri(SocketAddr::new(
        Ipv4Addr::LOCALHOST.to_ipv6_mapped().into(),
        14555,
    ))
    .expect_err("IPv4-mapped IPv6 address must be rejected");
}

#[tokio::test]
async fn auth_disabled_companion_serves_and_shuts_down() {
    let temp = TempDir::new().expect("temp dir");
    write_config(
        &temp,
        "[trace_history]\nenabled = false\n\n[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:0'\n",
    );
    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions::default(),
    )
    .await
    .expect("start composite server");
    let grpc_addr = grpc_addr(&server);
    assert!(!server.grpc_authentication_enabled());
    let mcp_addr = server.mcp_http_addr().expect("MCP HTTP endpoint");
    assert_catalog_tool(format!("http://{mcp_addr}/mcp"), None).await;
    server.shutdown().await.expect("shutdown composite server");
    let grpc_rebound = TcpListener::bind(grpc_addr).expect("gRPC port must be released");
    let mcp_rebound = TcpListener::bind(mcp_addr).expect("MCP port must be released");
    drop((grpc_rebound, mcp_rebound));
}

#[tokio::test]
async fn companion_uses_supplied_mcp_options() {
    let temp = TempDir::new().expect("temp dir");
    write_config(
        &temp,
        "[trace_history]\nenabled = false\n\n[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:0'\n",
    );
    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions {
            feedback_enabled: true,
            ..McpOptions::default()
        },
    )
    .await
    .expect("start composite server");
    let mcp_addr = server.mcp_http_addr().expect("MCP HTTP endpoint");
    assert_feedback_tool(format!("http://{mcp_addr}/mcp")).await;
    server.shutdown().await.expect("shutdown composite server");
}

/// The advertised protected-resource identifier and the minted token audience
/// must be the same string, so this configures a `public_url` that
/// canonicalization changes (an uppercase host) and mints against whatever the
/// server advertises.
#[tokio::test]
async fn session_authenticated_companion_forwards_bearer() {
    let temp = TempDir::new().expect("temp dir");
    let signing_key =
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .expect("P-256 signing key");
    std::fs::write(temp.path().join("session.key"), signing_key.as_ref()).expect("session key");
    write_config(
        &temp,
        r"
[trace_history]
enabled = false

[server.mcp_http]
enabled = true
bind = '127.0.0.1:0'
public_url = 'https://CORAL.example/mcp'

[auth.session]
signing_key_file = 'session.key'

[auth.authorization_server]
issuer = 'https://auth.example'

[auth.provider]
type = 'oidc'
issuer = 'https://accounts.example'
client_id = 'upstream-client'
client_secret = 'test-secret'
redirect_uri = 'https://auth.example/auth/oidc/callback'
",
    );
    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions::default(),
    )
    .await
    .expect("start authenticated composite server");
    assert!(server.grpc_authentication_enabled());
    let mcp_addr = server.mcp_http_addr().expect("MCP HTTP endpoint");
    let base = format!("http://{mcp_addr}");

    let ready = reqwest::get(format!("{base}/readyz"))
        .await
        .expect("readiness response");
    assert_eq!(ready.status(), reqwest::StatusCode::NO_CONTENT);
    let advertised = reqwest::get(format!("{base}/.well-known/oauth-protected-resource/mcp"))
        .await
        .expect("metadata response")
        .json::<serde_json::Value>()
        .await
        .expect("metadata document");
    let resource = advertised
        .get("resource")
        .and_then(serde_json::Value::as_str)
        .expect("advertised resource")
        .to_string();
    assert_eq!(resource, "https://coral.example/mcp");

    assert_unauthorized(&base, "Bearer wrong-token").await;

    let token = |audience: &str| {
        coral_app::test_session_tokens::issue_access_token(
            "https://auth.example",
            signing_key.as_ref(),
            Duration::from_mins(5),
            "alice",
            "https://client.example/client.json",
            audience,
        )
        .expect("session token")
    };
    let wrong_audience = token("https://other.example/mcp");
    assert_unauthorized(&base, &format!("Bearer {wrong_audience}")).await;

    let token = token(&resource);
    assert_catalog_tool(format!("{base}/mcp"), Some(&token)).await;

    // Readiness observes the backend, not just the port: stopping gRPC while MCP
    // HTTP keeps serving must turn the authenticated probe unhealthy.
    let RunningServer {
        grpc,
        mcp_http,
        grpc_authentication_enabled: _,
    } = server;
    grpc.shutdown().await.expect("shutdown gRPC server");
    let unready = reqwest::get(format!("{base}/readyz"))
        .await
        .expect("readiness response");
    assert_eq!(
        unready.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "an unreachable engine must not report ready"
    );
    mcp_http
        .expect("MCP HTTP server")
        .shutdown()
        .await
        .expect("shutdown MCP HTTP server");
}

#[tokio::test]
async fn mcp_start_failure_releases_the_started_grpc_listener() {
    let grpc_probe = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve gRPC port");
    let grpc_addr = grpc_probe.local_addr().expect("gRPC address");
    drop(grpc_probe);
    let occupied_mcp = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("occupy MCP port");
    let mcp_addr = occupied_mcp.local_addr().expect("MCP address");
    let temp = TempDir::new().expect("temp dir");
    write_config(
        &temp,
        &format!(
            "[trace_history]\nenabled = false\n\n[server.mcp_http]\nenabled = true\nbind = '{mcp_addr}'\n"
        ),
    );

    let result = start(
        ServerBuilder::standalone_grpc(grpc_addr)
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions::default(),
    )
    .await;
    let Err(error) = result else {
        panic!("occupied MCP address must fail startup");
    };
    assert!(error.to_string().contains("failed to bind MCP HTTP server"));
    let rebound = TcpListener::bind(grpc_addr).expect("gRPC listener must be released");
    drop(rebound);
}
