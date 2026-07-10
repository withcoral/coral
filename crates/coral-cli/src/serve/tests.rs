use std::net::TcpListener;
use std::time::{SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rmcp::ServiceExt as _;
use rmcp::model::CallToolRequestParams;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use tempfile::TempDir;

use super::*;

const INITIALIZE: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#;
const OAUTH_ISSUER: &str = "http://localhost:9080";
const OAUTH_RESOURCE: &str = "http://localhost:1457";

fn write_config(temp: &TempDir, config: &str) {
    std::fs::write(temp.path().join("config.toml"), config).expect("write config");
}

fn available_addr() -> SocketAddr {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve address");
    listener.local_addr().expect("reserved address")
}

fn write_oauth_config(temp: &TempDir, oauth_bind: SocketAddr, mcp_bind: Option<SocketAddr>) {
    std::fs::write(temp.path().join("session.key"), [b'k'; 32]).expect("session key");
    let mcp = mcp_bind.map_or_else(String::new, |bind| {
        format!(
            "[server.mcp_http]\nenabled = true\nbind = '{bind}'\nresource_url = '{OAUTH_RESOURCE}'\nauthorization_server = '{OAUTH_ISSUER}'\n\n"
        )
    });
    write_config(
        temp,
        &format!(
            "[trace_history]\nenabled = false\n\n{mcp}[auth]\nhttp_bind_addr = '{oauth_bind}'\n\n[auth.session]\nissuer = '{OAUTH_ISSUER}'\naudience = '{OAUTH_RESOURCE}'\nsigning_key_file = 'session.key'\n\n[auth.oauth]\nissuer = '{OAUTH_ISSUER}'\nresource = '{OAUTH_RESOURCE}'\n\n[auth.providers.test]\nissuer = 'https://accounts.example.test'\nclient_id = 'upstream-client'\nclient_secret = 'test-secret'\nredirect_uri = '{OAUTH_ISSUER}/auth/oidc/test/callback'\n"
        ),
    );
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
    let mut config = StreamableHttpClientTransportConfig::with_uri(endpoint);
    if let Some(bearer) = bearer {
        config = config.auth_header(bearer);
    }
    let client =
        ().serve(StreamableHttpClientTransport::from_config(config))
            .await
            .expect("initialize MCP client");
    let result = client
        .call_tool(CallToolRequestParams::new("list_catalog"))
        .await
        .expect("call list_catalog");
    assert_eq!(result.is_error, Some(false));
    client.cancel().await.expect("stop MCP client");
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

#[test]
fn shutdown_failures_retain_every_component_in_order() {
    let failures = ShutdownFailures::from_results(
        Err(McpHttpError::ShutdownTimedOut),
        Err(OAuthLifecycleError("OAuth test failure".to_string())),
        Err(LocalServerError::Unavailable(
            "gRPC test failure".to_string(),
        )),
    )
    .expect_err("all shutdown failures");

    assert!(failures.mcp.is_some());
    assert!(failures.oauth.is_some());
    assert!(failures.grpc.is_some());
    assert_eq!(
        failures.to_string(),
        "MCP HTTP: MCP HTTP server shutdown timed out; OAuth: OAuth test failure; gRPC: unavailable: gRPC test failure"
    );
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
    )
    .await
    .expect("start composite server");
    let grpc_addr = grpc_addr(&server);
    let mcp_addr = server.mcp_http_addr().expect("MCP HTTP endpoint");
    assert!(server.oauth_addr().is_none());
    assert_catalog_tool(format!("http://{mcp_addr}/mcp"), None).await;
    server.shutdown().await.expect("shutdown composite server");
    let grpc_rebound = TcpListener::bind(grpc_addr).expect("gRPC port must be released");
    let mcp_rebound = TcpListener::bind(mcp_addr).expect("MCP port must be released");
    drop((grpc_rebound, mcp_rebound));
}

#[tokio::test]
async fn oauth_and_mcp_companions_serve_and_release_all_listeners() {
    let temp = TempDir::new().expect("temp dir");
    write_oauth_config(
        &temp,
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))),
    );
    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
    )
    .await
    .expect("start composite server");
    let grpc_addr = grpc_addr(&server);
    let oauth_addr = server.oauth_addr().expect("OAuth endpoint");
    let mcp_addr = server.mcp_http_addr().expect("MCP HTTP endpoint");

    let response = reqwest::get(format!(
        "http://{oauth_addr}/.well-known/oauth-authorization-server"
    ))
    .await
    .expect("OAuth metadata response");
    assert!(response.status().is_success());
    let metadata = response.text().await.expect("OAuth metadata body");
    assert!(metadata.contains(&format!(r#""issuer":"{OAUTH_ISSUER}""#)));

    server.shutdown().await.expect("shutdown composite server");
    let grpc_rebound = TcpListener::bind(grpc_addr).expect("gRPC port must be released");
    let oauth_rebound = TcpListener::bind(oauth_addr).expect("OAuth port must be released");
    let mcp_rebound = TcpListener::bind(mcp_addr).expect("MCP port must be released");
    drop((grpc_rebound, oauth_rebound, mcp_rebound));
}

#[tokio::test]
async fn oauth_start_failure_releases_the_started_grpc_listener() {
    let grpc_addr = available_addr();
    let occupied_oauth = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("occupy OAuth port");
    let oauth_addr = occupied_oauth.local_addr().expect("OAuth address");
    let temp = TempDir::new().expect("temp dir");
    write_oauth_config(&temp, oauth_addr, None);

    let result = start(
        ServerBuilder::standalone_grpc(grpc_addr)
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
    )
    .await;
    let Err(error) = result else {
        panic!("occupied OAuth address must fail startup");
    };
    assert!(error.to_string().contains("failed to bind OAuth server"));
    let rebound = TcpListener::bind(grpc_addr).expect("gRPC listener must be released");
    drop((rebound, occupied_oauth));
}

#[tokio::test]
async fn session_authenticated_companion_forwards_bearer() {
    let temp = TempDir::new().expect("temp dir");
    std::fs::write(temp.path().join("session.key"), [b'k'; 32]).expect("session key");
    write_config(
        &temp,
        r"
[trace_history]
enabled = false

[server.mcp_http]
enabled = true
bind = '127.0.0.1:0'
resource_url = 'https://coral.example/mcp'
authorization_server = 'https://auth.example/'
scope = 'coral:mcp'

[auth.session]
issuer = 'https://auth.example'
audience = 'https://coral.example/mcp'
signing_key_file = 'session.key'
",
    );
    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
    )
    .await
    .expect("start authenticated composite server");
    let mcp_addr = server.mcp_http_addr().expect("MCP HTTP endpoint");
    let base = format!("http://{mcp_addr}");

    let ready = reqwest::get(format!("{base}/readyz"))
        .await
        .expect("readiness response");
    assert_eq!(ready.status(), reqwest::StatusCode::NO_CONTENT);
    let rejected = reqwest::Client::new()
        .post(format!("{base}/mcp"))
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("authorization", "Bearer wrong-token")
        .body(INITIALIZE)
        .send()
        .await
        .expect("rejected request");
    assert_eq!(rejected.status(), reqwest::StatusCode::UNAUTHORIZED);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time")
        .as_secs();
    let token = encode(
        &Header::new(Algorithm::HS256),
        &serde_json::json!({
            "iss": "https://auth.example",
            "aud": "https://coral.example/mcp",
            "sub": "alice",
            "provider": "oidc",
            "iat": now,
            "nbf": now,
            "exp": now + 300,
        }),
        &EncodingKey::from_secret(&[b'k'; 32]),
    )
    .expect("session token");
    assert_catalog_tool(format!("{base}/mcp"), Some(&token)).await;
    server.shutdown().await.expect("shutdown composite server");
}

#[tokio::test]
async fn mcp_start_failure_releases_started_oauth_and_grpc_listeners() {
    let grpc_probe = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve gRPC port");
    let oauth_probe = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve OAuth port");
    let grpc_addr = grpc_probe.local_addr().expect("gRPC address");
    let oauth_addr = oauth_probe.local_addr().expect("OAuth address");
    drop((grpc_probe, oauth_probe));
    let occupied_mcp = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("occupy MCP port");
    let mcp_addr = occupied_mcp.local_addr().expect("MCP address");
    let temp = TempDir::new().expect("temp dir");
    write_oauth_config(&temp, oauth_addr, Some(mcp_addr));

    let result = start(
        ServerBuilder::standalone_grpc(grpc_addr)
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
    )
    .await;
    let Err(error) = result else {
        panic!("occupied MCP address must fail startup");
    };
    assert!(error.to_string().contains("failed to bind MCP HTTP server"));
    let grpc_rebound = TcpListener::bind(grpc_addr).expect("gRPC listener must be released");
    let oauth_rebound = TcpListener::bind(oauth_addr).expect("OAuth listener must be released");
    drop((grpc_rebound, oauth_rebound, occupied_mcp));
}
