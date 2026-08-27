use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use coral_api::v1::{
    CatalogItemKind, CreateWorkspaceRequest, ListCatalogRequest, PaginationRequest, Workspace,
};
use coral_client::workspace;
use coral_mcp::{McpSurface, McpSurfaceProvider, McpSurfaceProviderError, McpToolRoute};
use jsonwebtoken::jwk::{Jwk, ThumbprintHash};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::header::WWW_AUTHENTICATE;
use ring::rand::SystemRandom;
use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
use rmcp::ServiceExt as _;
use rmcp::model::CallToolRequestParams;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use tempfile::TempDir;
use tonic::Request;

use super::*;

const INITIALIZE: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#;
const OAUTH_ISSUER: &str = "http://localhost:9080";
const OAUTH_RESOURCE: &str = "http://localhost:1457";
const SESSION_ISSUER: &str = "https://auth.example";
/// The canonical base of the per-workspace MCP resources the session fixtures
/// serve; [`SESSION_RESOURCE`] is [`TEST_WORKSPACE_NAME`]'s member of that
/// family — the audience a token must carry to be valid at its URL.
const SESSION_BASE: &str = "https://coral.example/mcp";
const SESSION_RESOURCE: &str = "https://coral.example/mcp/workspace/analytics";
const CORAL_UI_RESOURCE: &str = "https://coral-ui.example";
const TEST_WORKSPACE_NAME: &str = "analytics";
const MCP_SESSION_ID: &str = "mcp-session-id";

/// The listener-relative MCP URL of one workspace.
fn ws_path(name: &str) -> String {
    format!("/mcp/workspace/{name}")
}

/// The authenticated MCP surface every session fixture below serves.
///
/// The bind and the `public_url` whose per-workspace family decides the
/// accepted audiences are this constant; which workspace a request is about is
/// its URL's to say.
const AUTHENTICATED_MCP_HTTP: &str = r"
[server.mcp_http]
enabled = true
bind = '127.0.0.1:0'
public_url = 'https://CORAL.example/mcp'
";

struct TestMcpProvider;

impl McpSurfaceProvider for TestMcpProvider {
    fn surface(&self) -> Result<McpSurface, McpSurfaceProviderError> {
        Ok(McpSurface::replace(
            std::iter::empty::<McpToolRoute>(),
            ["start_task", "list_catalog", "end_task"],
            Some("Use the available Coral catalog tools.".to_string()),
        )?)
    }
}

struct UnexpectedMcpProvider;

impl McpSurfaceProvider for UnexpectedMcpProvider {
    fn surface(&self) -> Result<McpSurface, McpSurfaceProviderError> {
        Err(std::io::Error::other("MCP provider ran while MCP HTTP was disabled").into())
    }
}

fn write_config(temp: &TempDir, config: &str) {
    std::fs::write(temp.path().join("config.toml"), config).expect("write config");
}

/// Writes an auth-disabled companion config.
///
/// No workspace is named anywhere: the surface serves every existing workspace
/// at its own URL, so the fixtures scope a session by the URL they dial.
fn write_auth_disabled_config(temp: &TempDir) {
    write_config(
        temp,
        "[trace_history]\nenabled = false\n\n[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:0'\n",
    );
}

fn write_oauth_config(temp: &TempDir, oauth_bind: SocketAddr, mcp_bind: Option<SocketAddr>) {
    let signing_key =
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .expect("P-256 signing key");
    std::fs::write(temp.path().join("session.key"), signing_key.as_ref()).expect("session key");
    let mcp_bind = mcp_bind.unwrap_or_else(|| SocketAddr::from((Ipv4Addr::LOCALHOST, 0)));
    write_config(
        temp,
        &format!(
            "[trace_history]\nenabled = false\n\n[server.mcp_http]\nenabled = true\nbind = '{mcp_bind}'\npublic_url = '{OAUTH_RESOURCE}'\n\n[auth]\nhttp_bind_addr = '{oauth_bind}'\n\n[auth.session]\nsigning_key_file = 'session.key'\n\n[auth.authorization_server]\nissuer = '{OAUTH_ISSUER}'\n\n[auth.provider]\nissuer = 'https://accounts.example.test'\nclient_id = 'upstream-client'\nclient_secret = 'test-secret'\nredirect_uri = '{OAUTH_ISSUER}/auth/oidc/callback'\n"
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

async fn assert_catalog_tool(endpoint: String) {
    const INTENT: &str = "Exercise the composite server";

    let config = StreamableHttpClientTransportConfig::with_uri(endpoint);
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

async fn assert_cli_extension_filter(endpoint: String) {
    let client =
        ().serve(StreamableHttpClientTransport::from_uri(endpoint))
            .await
            .expect("initialize MCP client");
    let tools = client.list_all_tools().await.expect("list MCP tools");
    assert_eq!(tools.len(), 3);
    assert!(tools.iter().all(|tool| matches!(
        tool.name.as_ref(),
        "start_task" | "list_catalog" | "end_task"
    )));
    client.cancel().await.expect("stop MCP client");
}

async fn initialize_mcp(endpoint: &str, authorization: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(endpoint)
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("authorization", authorization)
        .body(INITIALIZE)
        .send()
        .await
        .expect("MCP response")
}

/// Asserts the MCP surface admits a bearer token past authentication.
///
/// The mirror of [`assert_unauthorized`], and it stops at the handshake because
/// that is where authentication is decided: everything after it is
/// workspace-scoped — even `tools/list`, which enumerates a workspace's table
/// functions — so `coral-app`'s workspace authorization tests own the boundary
/// behind it.
///
/// An accepted audience is then refused *membership*: nothing in these
/// fixtures provisions a directory row for the caller (only a completed login
/// writes one), so admission's concealed not-found for the URL's workspace is
/// exactly what proves the token passed authentication and the URL's name
/// reached admission. The absent challenge is what tells that refusal apart
/// from a refused audience, which is answered `401` and carries one.
async fn assert_mcp_authenticated(endpoint: &str, token: &str, workspace: &str) {
    let guidance = mcp_refusal_guidance(endpoint, token).await;
    assert!(
        guidance.contains(&format!("Workspace `{workspace}` was not found")),
        "an accepted audience must be refused only its membership: {guidance}"
    );
}

/// Reads the guidance an accepted audience's refused handshake carries.
///
/// Every property that separates this refusal from an authentication one is
/// asserted here rather than at the call sites, which then differ only in the
/// guidance they expect: the audience passed authentication, so the answer is
/// `200` with no challenge, and no session was opened for a caller who was not
/// admitted to one.
async fn mcp_refusal_guidance(endpoint: &str, token: &str) -> String {
    let accepted = initialize_mcp(endpoint, &format!("Bearer {token}")).await;
    assert_eq!(
        accepted.status(),
        reqwest::StatusCode::OK,
        "an accepted audience must pass authentication and be answered its handshake"
    );
    assert!(accepted.headers().get(WWW_AUTHENTICATE).is_none());
    assert!(
        accepted.headers().get(MCP_SESSION_ID).is_none(),
        "a refused handshake must leave no session behind"
    );
    let refusal = accepted
        .json::<serde_json::Value>()
        .await
        .expect("JSON-RPC handshake answer");
    refusal
        .pointer("/error/message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("a refused handshake carries guidance: {refusal}"))
        .to_string()
}

async fn assert_unauthorized(base: &str, authorization: &str) {
    let rejected = initialize_mcp(
        &format!("{base}{}", ws_path(TEST_WORKSPACE_NAME)),
        authorization,
    )
    .await;
    assert_eq!(rejected.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(
        rejected.headers().get_all(WWW_AUTHENTICATE).iter().count(),
        1
    );
    assert!(
        rejected
            .headers()
            .get(WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("Bearer ")),
        "MCP rejection must include one bearer challenge"
    );
}

/// Asserts the private gRPC data plane refuses a call carrying no credentials.
///
/// This probes the listener rather than `grpc_authentication_enabled`, which is
/// derived from configuration and so reports true even if nobody installed the
/// session provider. It is the assertion that fails if composition stops gating
/// gRPC and the listener quietly falls back to the local principal.
async fn assert_grpc_rejects_unauthenticated(endpoint: &str) {
    let unauthenticated = AppClient::connect(endpoint)
        .await
        .expect("unauthenticated gRPC client");
    let denied = unauthenticated
        .catalog_client()
        .list_catalog(Request::new(catalog_request()))
        .await
        .expect_err("the gRPC data plane must refuse an unauthenticated call");
    assert_eq!(denied.code(), Code::Unauthenticated);
}

/// The ordinary workspace these fixtures name.
///
/// Nothing provisions a workspace any more, and no helper reconstructs one, so
/// these fixtures name the workspace they mean. Most call sites never create it:
/// the probes below assert on authentication, which is decided before the
/// workspace is ever looked up.
fn test_workspace() -> Workspace {
    workspace(TEST_WORKSPACE_NAME)
}

/// Creates [`test_workspace`] over the unauthenticated loopback gRPC endpoint.
async fn create_test_workspace(endpoint: &str) {
    AppClient::connect(endpoint)
        .await
        .expect("local gRPC client")
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(test_workspace()),
        }))
        .await
        .expect("create workspace");
}

fn catalog_request() -> ListCatalogRequest {
    ListCatalogRequest {
        workspace: Some(test_workspace()),
        catalog_name: String::new(),
        schema_name: String::new(),
        kind: CatalogItemKind::Unspecified as i32,
        pagination: Some(PaginationRequest {
            limit: 1,
            offset: 0,
        }),
    }
}

/// Mints a session token through the real issuer.
///
/// Anything the issuer can legitimately emit goes through it, so these tests
/// track the issuer's wire format instead of re-implementing the JWT the
/// verifier expects. Fixtures the issuer can never emit — an already-expired
/// token, or one whose `kid` disclaims the key that signed it — still have to be
/// assembled by hand wherever they are needed.
fn session_token(signing_key: &[u8], audience: &str) -> String {
    coral_app::test_session_tokens::issue_access_token(
        SESSION_ISSUER,
        signing_key,
        Duration::from_mins(5),
        "alice",
        "https://client.example/client.json",
        audience,
        coral_app::PrincipalKind::User,
    )
    .expect("session token")
}

/// Assembles a session token by hand, for fixtures the issuer cannot emit.
///
/// Reserved for tokens the real issuer will never produce: one that already
/// expired (its TTL is a `Duration`, and a zero TTL is rejected outright) or one
/// whose `kid` disclaims the key that signed it. Claims mirror
/// `SessionTokenClaims` exactly so these fixtures fail on the property under
/// test rather than on a shape the verifier never sees. Anything legitimate goes
/// through [`session_token`].
fn signed_session_token(
    signing_key: &EncodingKey,
    key_id: &str,
    audience: &str,
    issued_at: u64,
    expires_at: u64,
    token_id: &str,
) -> String {
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(key_id.to_string());
    header.typ = Some("at+jwt".to_string());
    encode(
        &header,
        &serde_json::json!({
            "iss": SESSION_ISSUER,
            "aud": audience,
            "sub": "alice",
            "jti": token_id,
            "client_id": "https://client.example/client.json",
            "iat": issued_at,
            "nbf": issued_at,
            "exp": expires_at,
        }),
        signing_key,
    )
    .expect("session token")
}

fn write_session_config(temp: &TempDir, signing_key: &[u8]) {
    write_session_config_with_mcp(temp, signing_key, AUTHENTICATED_MCP_HTTP);
}

fn write_coral_ui_only_session_config(temp: &TempDir, signing_key: &[u8]) {
    write_session_config_with_mcp(temp, signing_key, "");
}

fn write_session_config_with_mcp(temp: &TempDir, signing_key: &[u8], mcp_http: &str) {
    std::fs::write(temp.path().join("session.key"), signing_key).expect("session key");
    // The uppercase hosts are deliberate: the assertions only prove
    // canonicalization if the configured URLs actually need it.
    write_config(
        temp,
        &format!(
            r"
[trace_history]
enabled = false

{mcp_http}

[auth]
allowed_audiences = ['https://CORAL-UI.example/']

[auth.session]
signing_key_file = 'session.key'

[auth.authorization_server]
issuer = 'https://auth.example'

[auth.provider]
issuer = 'https://accounts.example'
client_id = 'upstream-client'
client_secret = 'test-secret'
redirect_uri = 'https://auth.example/auth/oidc/callback'
",
        ),
    );
}

/// Asserts the private gRPC data plane admits a token past authentication.
///
/// The probe call is workspace-scoped, so a caller who holds no membership is
/// legitimately refused *after* authentication has already succeeded, and that
/// refusal still proves the audience was accepted. Only `Unauthenticated`, or a
/// status meaning the call never reached the service, falsifies what these tests
/// own. `coral-app`'s workspace authorization tests own the boundary itself.
async fn assert_grpc_authenticated(server: &RunningServer, token: &str) {
    let authenticated = connect_with_loopback_bearer(
        server.endpoint_uri(),
        BearerToken::new(token).expect("bearer token"),
    )
    .await
    .expect("authenticated gRPC client");
    if let Err(refused) = authenticated
        .catalog_client()
        .list_catalog(Request::new(catalog_request()))
        .await
    {
        assert!(
            !matches!(
                refused.code(),
                Code::Unauthenticated | Code::Unavailable | Code::Unknown
            ),
            "an accepted audience must reach the service: {refused:?}"
        );
    }
}

async fn assert_authenticated_surfaces(server: &RunningServer, mcp_endpoint: &str, token: &str) {
    assert_grpc_authenticated(server, token).await;
    assert_mcp_authenticated(mcp_endpoint, token, TEST_WORKSPACE_NAME).await;
}

#[derive(Debug, Eq, PartialEq)]
struct BearerRejection {
    grpc_code: Code,
    grpc_message: String,
    grpc_details: Vec<u8>,
    status: reqwest::StatusCode,
    challenge: String,
    body: String,
}

async fn assert_bearer_rejected(
    server: &RunningServer,
    mcp_endpoint: &str,
    token: &str,
) -> BearerRejection {
    let client = connect_with_loopback_bearer(
        server.endpoint_uri(),
        BearerToken::new(token).expect("bearer token"),
    )
    .await
    .expect("gRPC client");
    let denied = client
        .catalog_client()
        .list_catalog(Request::new(catalog_request()))
        .await
        .expect_err("invalid session token must fail gRPC");
    let grpc_code = denied.code();
    let grpc_message = denied.message().to_string();
    let grpc_details = denied.details().to_vec();
    assert_eq!(grpc_code, Code::Unauthenticated);
    assert_eq!(grpc_message, "unauthenticated: authentication required");
    assert!(grpc_details.is_empty());
    assert!(!format!("{denied:?}").contains(token));

    let denied = reqwest::Client::new()
        .post(mcp_endpoint)
        .bearer_auth(token)
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .body(INITIALIZE)
        .send()
        .await
        .expect("invalid session MCP response");
    let status = denied.status();
    let challenge = denied
        .headers()
        .get(WWW_AUTHENTICATE)
        .and_then(|value| value.to_str().ok())
        .expect("ASCII MCP authentication challenge")
        .to_string();
    assert_eq!(denied.headers().get_all(WWW_AUTHENTICATE).iter().count(), 1);
    let body = denied.text().await.expect("invalid session MCP body");
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    assert!(body.is_empty());
    BearerRejection {
        grpc_code,
        grpc_message,
        grpc_details,
        status,
        challenge,
        body,
    }
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

/// The printed per-workspace URL template follows the served base path, so it
/// never names a route that would 404 on the listener.
///
/// The auth-disabled surface has no public URL and always mounts under `/mcp`;
/// the authenticated surface mounts under its `public_url`'s path, so a base
/// with a non-`/mcp` path — the shape older docs recommended — must print that
/// path rather than a hardcoded one.
#[test]
fn mcp_workspace_url_path_follows_the_served_base_path() {
    let auth_disabled = McpHttpServeConfig::AuthDisabled {
        bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        expose_non_loopback: false,
        allowed_hosts: Vec::new(),
    };
    assert_eq!(
        mcp_http_workspace_url_path(&auth_disabled),
        "/mcp/workspace/{workspace}"
    );

    let authenticated = |public_url: &str| McpHttpServeConfig::Authenticated {
        bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        public_url: public_url.to_string(),
        authorization_server: "https://auth.example".to_string(),
    };
    assert_eq!(
        mcp_http_workspace_url_path(&authenticated("https://mcp.example.com/mcp")),
        "/mcp/workspace/{workspace}"
    );
    assert_eq!(
        mcp_http_workspace_url_path(&authenticated("https://mcp.example.com")),
        "/workspace/{workspace}",
        "a root public_url mounts the family at the origin root"
    );
    assert_eq!(
        mcp_http_workspace_url_path(&authenticated("https://mcp.example.com/base/mcp")),
        "/base/mcp/workspace/{workspace}"
    );
}

#[test]
fn shutdown_failures_retain_every_component_in_order() {
    let failures = ShutdownFailures::from_results(
        Err(McpHttpError::ShutdownTimedOut),
        Err(AuthServerError::Config("OAuth test failure".to_string())),
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

/// Also proves the URL's workspace reaches the running MCP surface: the tools
/// asserted here are workspace-scoped, and nothing but the dialed URL names
/// the workspace they resolve against.
#[tokio::test]
async fn auth_disabled_companion_serves_and_shuts_down() {
    let temp = TempDir::new().expect("temp dir");
    write_auth_disabled_config(&temp);
    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions::default(),
        Some(Arc::new(TestMcpProvider)),
    )
    .await
    .expect("start composite server");
    let grpc_addr = grpc_addr(&server);
    assert!(!server.grpc_authentication_enabled());
    assert!(!server.mcp_http_authentication_enabled());
    let mcp_addr = server.mcp_http_addr().expect("MCP HTTP endpoint");
    assert!(server.oauth_addr().is_none());
    // The tools this asserts are workspace-scoped, and nothing provisions a
    // workspace any more, so the fixture creates the one the dialed URL names.
    // Nothing in config selects it: the workspace is named only by the URL path.
    create_test_workspace(server.endpoint_uri()).await;
    assert_catalog_tool(format!("http://{mcp_addr}{}", ws_path(TEST_WORKSPACE_NAME))).await;
    assert_cli_extension_filter(format!("http://{mcp_addr}{}", ws_path(TEST_WORKSPACE_NAME))).await;
    Box::pin(server.shutdown())
        .await
        .expect("shutdown composite server");
    let grpc_rebound = TcpListener::bind(grpc_addr).expect("gRPC port must be released");
    let mcp_rebound = TcpListener::bind(mcp_addr).expect("MCP port must be released");
    drop((grpc_rebound, mcp_rebound));
}

/// Configuration resolution already rejects an unconsented non-loopback bind,
/// so no config file can reach this arm with one. This hand-built settings
/// value pins the second fail-closed layer's wiring anyway: without consent,
/// serve must route through the constructor that enforces loopback.
#[tokio::test]
async fn unconsented_non_loopback_settings_fail_closed_in_serve() {
    let settings = McpHttpServeConfig::AuthDisabled {
        bind_addr: SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 0)),
        expose_non_loopback: false,
        allowed_hosts: Vec::new(),
    };
    let result = start_mcp_http(
        Some(settings),
        None,
        SocketAddr::from((Ipv4Addr::LOCALHOST, 1)),
        McpOptions::default(),
    )
    .await;
    match result {
        Err(McpStartError::Http(McpHttpError::NonLoopbackBind(_))) => {}
        Err(other) => panic!("expected the loopback rejection, got: {other}"),
        Ok(_) => panic!("an unconsented non-loopback bind must not start"),
    }
}

#[tokio::test]
async fn opted_in_auth_disabled_companion_serves_off_loopback() {
    let temp = TempDir::new().expect("temp dir");
    write_config(
        &temp,
        "[trace_history]\nenabled = false\n\n[server.mcp_http]\nenabled = true\n\
         bind = '0.0.0.0:0'\nallow_unauthenticated_non_loopback = true\n",
    );
    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions::default(),
        None,
    )
    .await
    .expect("start composite server with the exposure opt-in");
    assert!(!server.mcp_http_authentication_enabled());
    let mcp_addr = server.mcp_http_addr().expect("MCP HTTP endpoint");
    assert!(mcp_addr.ip().is_unspecified(), "bind must leave loopback");
    // Dial through loopback: the point here is that the listener started and
    // serves; reachability from other interfaces is the operator's affair.
    let dial = SocketAddr::from((Ipv4Addr::LOCALHOST, mcp_addr.port()));
    // The tools this asserts are workspace-scoped, and nothing provisions a
    // workspace any more, so the fixture creates the one it scopes MCP to.
    create_test_workspace(server.endpoint_uri()).await;
    assert_catalog_tool(format!("http://{dial}{}", ws_path(TEST_WORKSPACE_NAME))).await;
    Box::pin(server.shutdown())
        .await
        .expect("shutdown composite server");
}

#[tokio::test]
async fn companion_uses_supplied_mcp_options() {
    let temp = TempDir::new().expect("temp dir");
    write_auth_disabled_config(&temp);
    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions {
            feedback_enabled: true,
            ..McpOptions::default()
        },
        None,
    )
    .await
    .expect("start composite server");
    let mcp_addr = server.mcp_http_addr().expect("MCP HTTP endpoint");
    // `tools/list` enumerates the workspace's table functions, so it needs the
    // workspace MCP is scoped to actually exist.
    create_test_workspace(server.endpoint_uri()).await;
    assert_feedback_tool(format!("http://{mcp_addr}{}", ws_path(TEST_WORKSPACE_NAME))).await;
    Box::pin(server.shutdown())
        .await
        .expect("shutdown composite server");
}

/// The advertised protected-resource identifier and the minted token audience
/// must be the same string, so this configures a `public_url` that
/// canonicalization changes (an uppercase host) and mints against whatever the
/// server advertises.
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
        McpOptions::default(),
        None,
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

    Box::pin(server.shutdown())
        .await
        .expect("shutdown composite server");
    let grpc_rebound = TcpListener::bind(grpc_addr).expect("gRPC port must be released");
    let oauth_rebound = TcpListener::bind(oauth_addr).expect("OAuth port must be released");
    let mcp_rebound = TcpListener::bind(mcp_addr).expect("MCP port must be released");
    drop((grpc_rebound, oauth_rebound, mcp_rebound));
}

#[tokio::test]
async fn oauth_start_failure_releases_the_started_grpc_listener() {
    let grpc_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve gRPC port");
    let grpc_addr = grpc_listener.local_addr().expect("gRPC address");
    let occupied_oauth = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("occupy OAuth port");
    let oauth_addr = occupied_oauth.local_addr().expect("OAuth address");
    let temp = TempDir::new().expect("temp dir");
    write_oauth_config(&temp, oauth_addr, None);

    // Hand the live gRPC listener to the server so the reserved port never
    // lapses between selection and bind: a parallel process cannot claim it, so
    // startup fails on the occupied OAuth port and must release the gRPC
    // listener afterward.
    let result = start(
        ServerBuilder::standalone_grpc(grpc_addr)
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads()
            .with_prebound_grpc_listener(grpc_listener),
        McpOptions::default(),
        None,
    )
    .await;
    let Err(error) = result else {
        panic!("occupied OAuth address must fail startup");
    };
    assert!(
        error
            .to_string()
            .contains("failed to bind authorization server")
    );
    let rebound = TcpListener::bind(grpc_addr).expect("gRPC listener must be released");
    drop((rebound, occupied_oauth));
}

#[tokio::test]
async fn session_authenticated_companion_gates_grpc_and_mcp() {
    let temp = TempDir::new().expect("temp dir");
    let signing_key =
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .expect("P-256 signing key");
    write_session_config(&temp, signing_key.as_ref());
    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions::default(),
        None,
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
    // Metadata is served per workspace, at the path-inserted well-known URL of
    // the workspace's own resource; the base's metadata path names no
    // workspace and is gone.
    let advertised = reqwest::get(format!(
        "{base}/.well-known/oauth-protected-resource/mcp/workspace/{TEST_WORKSPACE_NAME}"
    ))
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
    assert_eq!(resource, SESSION_RESOURCE);
    let retired_base = reqwest::get(format!("{base}/.well-known/oauth-protected-resource/mcp"))
        .await
        .expect("base metadata response");
    assert_eq!(retired_base.status(), reqwest::StatusCode::NOT_FOUND);

    assert_unauthorized(&base, "Bearer wrong-token").await;

    let wrong_audience = session_token(signing_key.as_ref(), "https://other.example/mcp");
    assert_unauthorized(&base, &format!("Bearer {wrong_audience}")).await;

    // The base itself is no longer an audience: a token minted for it dies at
    // every workspace URL, exactly like any other wrong audience.
    let base_audience = session_token(signing_key.as_ref(), SESSION_BASE);
    assert_unauthorized(&base, &format!("Bearer {base_audience}")).await;

    assert_grpc_rejects_unauthenticated(server.endpoint_uri()).await;

    let token = session_token(signing_key.as_ref(), &resource);
    assert_authenticated_surfaces(
        &server,
        &format!("{base}{}", ws_path(TEST_WORKSPACE_NAME)),
        &token,
    )
    .await;

    let coral_ui_token = session_token(signing_key.as_ref(), CORAL_UI_RESOURCE);
    assert_grpc_authenticated(&server, &coral_ui_token).await;
    assert_unauthorized(&base, &format!("Bearer {coral_ui_token}")).await;

    // Readiness observes the backend, not just the port: stopping gRPC while MCP
    // HTTP keeps serving must turn the authenticated probe unhealthy.
    let RunningServer {
        grpc,
        oauth,
        mcp_http,
        grpc_authentication_enabled: _,
        mcp_http_authentication_enabled: _,
        mcp_http_workspace_url_path: _,
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
    oauth
        .expect("OAuth server")
        .shutdown()
        .await
        .expect("shutdown OAuth server");
}

/// The URL's workspace reaches the authenticated MCP surface, and the refusal
/// is the concealed not-found *for that URL's own name*.
///
/// Two handshakes with valid per-workspace tokens differ only in the workspace
/// their URL (and audience) names; neither is among the caller's memberships
/// (only a completed login writes a membership row), and the two answers are
/// identical once the names are substituted — through the running server, not
/// a mocked directory. Admission never asks whether a name exists globally, so
/// nothing else about the names could change the answer.
///
/// The refusal also says which client decided it. Admission lists memberships
/// on the caller's own bearer-bound connection; the unauthenticated loopback
/// client this surface also holds serves `/readyz` only, which is asserted
/// working right beside the refusals it must have played no part in.
#[tokio::test]
async fn session_authenticated_companion_scopes_mcp_to_the_urls_workspace() {
    let temp = TempDir::new().expect("temp dir");
    let signing_key =
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .expect("P-256 signing key");
    write_session_config(&temp, signing_key.as_ref());
    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions::default(),
        None,
    )
    .await
    .expect("start authenticated composite server");
    let mcp_addr = server.mcp_http_addr().expect("MCP HTTP endpoint");
    let base = format!("http://{mcp_addr}");

    let ready = reqwest::get(format!("{base}/readyz"))
        .await
        .expect("readiness response");
    assert_eq!(
        ready.status(),
        reqwest::StatusCode::NO_CONTENT,
        "the unauthenticated client this surface holds serves readiness"
    );

    let token = session_token(signing_key.as_ref(), SESSION_RESOURCE);
    // The same token is accepted on both surfaces, so what MCP answers below is
    // an admission answer about a workspace rather than one about the token.
    assert_grpc_authenticated(&server, &token).await;

    let existing =
        mcp_refusal_guidance(&format!("{base}{}", ws_path(TEST_WORKSPACE_NAME)), &token).await;
    assert!(
        existing.contains(&format!("Workspace `{TEST_WORKSPACE_NAME}` was not found")),
        "the URL's name must reach admission and be the one it answers about: {existing}"
    );

    let ghost_token = session_token(
        signing_key.as_ref(),
        "https://coral.example/mcp/workspace/ghost",
    );
    let ghost = mcp_refusal_guidance(&format!("{base}{}", ws_path("ghost")), &ghost_token).await;
    assert!(
        ghost.contains("Workspace `ghost` was not found"),
        "guidance: {ghost}"
    );
    assert_eq!(
        existing.replace(TEST_WORKSPACE_NAME, "{ws}"),
        ghost.replace("ghost", "{ws}"),
        "an existing workspace the caller does not hold and one never created \
         must be refused with the same sentence"
    );

    Box::pin(server.shutdown())
        .await
        .expect("shutdown composite server");
    let mcp_rebound = TcpListener::bind(mcp_addr).expect("MCP port must be released");
    drop(mcp_rebound);
}

/// MCP HTTP admits only the exact audience of the workspace URL a request
/// arrives at: a token minted for a sibling surface, or for a *different
/// workspace's* URL, must not be replayable there.
///
/// This asserts on the authenticator the running MCP surface is handed, composed
/// from an on-disk config by the same function `start` calls, and starts the
/// server that composition configured. The regression it guards is `coral serve`
/// handing MCP the private API's full audience allowlist, which would let a
/// token minted for the BFF — or any workspace — be presented anywhere.
#[tokio::test]
async fn session_auth_composes_a_route_exact_audience_for_mcp() {
    let temp = TempDir::new().expect("temp dir");
    let signing_key =
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .expect("P-256 signing key");
    write_session_config(&temp, signing_key.as_ref());
    let builder = ServerBuilder::configured_standalone_grpc()
        .with_config_dir(temp.path())
        .with_noop_feedback_uploads();
    let mut settings = builder.serve_settings().expect("resolve serve settings");
    let mcp_config = settings.mcp_http().cloned();
    let (builder, mcp_authenticator) =
        compose_session_policies(builder, settings.take_session_auth(), mcp_config.as_ref());
    let mcp_authenticator = mcp_authenticator.expect("composed MCP authenticator");

    let mut grpc = builder.start().await.expect("start composed gRPC server");
    assert!(
        grpc.take_authorization_server().is_some(),
        "app startup owns the authorization server this composition runs"
    );

    mcp_authenticator
        .principal_for_bearer_with_audience(
            &session_token(signing_key.as_ref(), SESSION_RESOURCE),
            SESSION_RESOURCE,
        )
        .expect("token minted for exactly this workspace's URL");
    mcp_authenticator
        .principal_for_bearer_with_audience(
            &session_token(signing_key.as_ref(), CORAL_UI_RESOURCE),
            SESSION_RESOURCE,
        )
        .expect_err("MCP must refuse a token minted for a sibling surface");
    mcp_authenticator
        .principal_for_bearer_with_audience(
            &session_token(
                signing_key.as_ref(),
                "https://coral.example/mcp/workspace/other",
            ),
            SESSION_RESOURCE,
        )
        .expect_err("a bearer for one workspace must die at another's URL");

    grpc.shutdown()
        .await
        .expect("shutdown composed gRPC server");
}

#[tokio::test]
async fn coral_ui_only_audience_authenticates_private_grpc_without_mcp_http() {
    let temp = TempDir::new().expect("temp dir");
    let signing_key =
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .expect("P-256 signing key");
    write_coral_ui_only_session_config(&temp, signing_key.as_ref());

    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions::default(),
        Some(Arc::new(UnexpectedMcpProvider)),
    )
    .await
    .expect("start Coral UI-only authenticated server");

    assert!(server.grpc_authentication_enabled());
    assert!(server.mcp_http_addr().is_none());
    assert!(server.oauth_addr().is_some());
    assert_grpc_rejects_unauthenticated(server.endpoint_uri()).await;

    let coral_ui_token = session_token(signing_key.as_ref(), CORAL_UI_RESOURCE);
    assert_grpc_authenticated(&server, &coral_ui_token).await;

    Box::pin(server.shutdown())
        .await
        .expect("shutdown Coral UI-only server");
}

#[tokio::test]
async fn session_failures_and_restart_are_fail_closed() {
    let temp = TempDir::new().expect("temp dir");
    let signing_key =
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .expect("P-256 signing key");
    write_session_config(&temp, signing_key.as_ref());

    let encoding_key = EncodingKey::from_ec_der(signing_key.as_ref());
    let key_id = Jwk::from_encoding_key(&encoding_key, Algorithm::ES256)
        .expect("signing JWK")
        .thumbprint(ThumbprintHash::SHA256);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time")
        .as_secs();
    let valid = session_token(signing_key.as_ref(), SESSION_RESOURCE);
    let expired = signed_session_token(
        &encoding_key,
        &key_id,
        SESSION_RESOURCE,
        now.saturating_sub(300),
        now.saturating_sub(120),
        "expired-token",
    );
    let wrong_audience = session_token(signing_key.as_ref(), "https://coral.example/not-mcp");
    let forged_key =
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .expect("forged P-256 signing key");
    let forged = signed_session_token(
        &EncodingKey::from_ec_der(forged_key.as_ref()),
        &key_id,
        SESSION_RESOURCE,
        now.saturating_sub(1),
        now + 300,
        "forged-token",
    );

    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions::default(),
        None,
    )
    .await
    .expect("start authenticated composite server");
    let mcp_endpoint = format!(
        "http://{}{}",
        server.mcp_http_addr().expect("MCP HTTP endpoint"),
        ws_path(TEST_WORKSPACE_NAME)
    );

    let expired_rejection = assert_bearer_rejected(&server, &mcp_endpoint, &expired).await;
    let wrong_audience_rejection =
        assert_bearer_rejected(&server, &mcp_endpoint, &wrong_audience).await;
    let malformed_rejection =
        assert_bearer_rejected(&server, &mcp_endpoint, "not-a-session-token").await;
    let forged_rejection = assert_bearer_rejected(&server, &mcp_endpoint, &forged).await;
    assert_eq!(expired_rejection, wrong_audience_rejection);
    assert_eq!(expired_rejection, malformed_rejection);
    assert_eq!(expired_rejection, forged_rejection);
    assert_authenticated_surfaces(&server, &mcp_endpoint, &valid).await;
    Box::pin(server.shutdown()).await.expect("first shutdown");

    let restarted = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions::default(),
        None,
    )
    .await
    .expect("restart authenticated composite server");
    let restarted_mcp = format!(
        "http://{}{}",
        restarted.mcp_http_addr().expect("restarted MCP endpoint"),
        ws_path(TEST_WORKSPACE_NAME)
    );
    assert_authenticated_surfaces(&restarted, &restarted_mcp, &valid).await;
    Box::pin(restarted.shutdown())
        .await
        .expect("restarted shutdown");
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
        McpOptions::default(),
        None,
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
