use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{HeaderValue, Method, Request, StatusCode, header};
use axum::{Router, response::Response};
use coral_api::v1::user_service_server::{UserService, UserServiceServer};
use coral_api::v1::workspace_service_server::{WorkspaceService, WorkspaceServiceServer};
use coral_api::v1::{
    AddWorkspaceMemberRequest, AddWorkspaceMemberResponse, CreateWorkspaceRequest,
    CreateWorkspaceResponse, DeleteWorkspaceRequest, DeleteWorkspaceResponse,
    GetCurrentUserRequest, GetCurrentUserResponse, ListUsersRequest, ListUsersResponse,
    ListWorkspaceMembersRequest, ListWorkspaceMembersResponse, ListWorkspacesRequest,
    ListWorkspacesResponse, RemoveWorkspaceMemberRequest, RemoveWorkspaceMemberResponse, User,
    WorkspaceMembership, WorkspaceRole,
};
use coral_client::local::{ServerBuilder, connect_with_loopback_bearer};
use coral_client::{AppClient, BearerToken, workspace};
use futures::{future::BoxFuture, poll};
use rmcp::handler::server::router::tool::IntoToolRoute;
use rmcp::model::{CallToolRequestParams, ClientJsonRpcMessage};
use rmcp::transport::{
    StreamableHttpClientTransport,
    streamable_http_client::StreamableHttpClientTransportConfig,
    streamable_http_server::session::{
        SessionManager as _,
        local::{SessionConfig, create_local_session},
    },
};
use rmcp::{ErrorData, Json, ServiceExt as _};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataMap;
use tonic::transport::Server;
use tonic::{Request as GrpcRequest, Response as GrpcResponse, Status};
use tower::ServiceExt as _;

use crate::{McpOptions, McpSurface, McpToolContext};

use super::{
    AUTHENTICATED_SESSION_IDLE_TIMEOUT, AuthenticatedMcpHttpConfig, AuthenticatedMcpHttpRuntime,
    AuthenticatedSession, AuthenticatedSessionManager, AuthenticatedSessions,
    MAX_AUTHENTICATED_SESSIONS, MAX_MCP_REQUEST_BODY_SIZE, McpHttpConfig, McpHttpError,
    McpWorkspaceSegment, ReadinessProbe, RunningMcpHttpServer, SESSION_ID_HEADER,
    SHUTDOWN_GRACE_PERIOD, SessionOwner, WORKSPACE_URL_HINT, auth_disabled_router,
    authenticated_router, authenticated_router_with_sessions, binding_fingerprint,
    readiness_status, start_auth_disabled, start_authenticated,
};

const INITIALIZE: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#;
const PING: &str = r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#;
/// The first exchange a caller would reach for after a handshake — and the
/// first that is workspace-scoped, so the first that could only fail opaquely.
const TOOLS_LIST: &str = r#"{"jsonrpc":"2.0","id":3,"method":"tools/list"}"#;

#[rmcp::tool(description = "Return the workspace bound to this MCP session")]
fn extension_workspace(
    context: &McpToolContext,
) -> BoxFuture<'_, Result<Json<serde_json::Value>, ErrorData>> {
    Box::pin(async move {
        Ok(Json(serde_json::json!({
            "workspace": context.workspace().name,
            "core_tool_count": context.core_tools().definitions().await?.len(),
        })))
    })
}

fn extension_options() -> McpOptions {
    McpOptions {
        surface: McpSurface::extend([
            (extension_workspace_tool_attr(), extension_workspace).into_tool_route()
        ])
        .expect("build MCP surface"),
        ..McpOptions::default()
    }
}

/// The listener-relative MCP URL of one workspace, on either surface.
fn ws_path(name: &str) -> String {
    format!("/mcp/workspace/{name}")
}

fn segment(name: &str) -> McpWorkspaceSegment {
    McpWorkspaceSegment::parse(name).expect("valid workspace segment")
}

fn raw_mcp_request(body: impl Into<Body>) -> Request<Body> {
    raw_mcp_request_at(&ws_path(TEST_WORKSPACE), body)
}

fn raw_mcp_request_at(path: &str, body: impl Into<Body>) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri(path)
        .header(header::HOST, "127.0.0.1")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.into())
        .expect("request")
}

/// Counts live auth-disabled sessions across every served workspace.
async fn local_session_count(state: &Arc<super::HttpState>) -> usize {
    let mut count = 0;
    for entry in state.workspaces.read().await.values() {
        count += entry.sessions.sessions.read().await.len();
    }
    count
}

async fn assert_bad_request_without_session(
    router: &Router,
    state: &Arc<super::HttpState>,
    request: Request<Body>,
) {
    let response = send(router, request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(local_session_count(state).await, 0);
}

fn authenticated_config() -> AuthenticatedMcpHttpConfig {
    authenticated_config_at("0.0.0.0:0".parse().unwrap())
}

fn authenticated_config_at(bind_addr: SocketAddr) -> AuthenticatedMcpHttpConfig {
    AuthenticatedMcpHttpConfig::new(
        bind_addr,
        "https://mcp.example.com/mcp",
        "https://login.example.com/",
    )
    .unwrap()
}

fn auth_request(authorization: &str, session: Option<&str>, body: &str) -> Request<Body> {
    auth_request_at(TEST_WORKSPACE, authorization, session, body)
}

/// One authenticated request at the named workspace's URL.
fn auth_request_at(
    workspace: &str,
    authorization: &str,
    session: Option<&str>,
    body: &str,
) -> Request<Body> {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(ws_path(workspace))
        .header(header::HOST, "mcp.example.com")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, authorization);
    if let Some(value) = session {
        request = request.header(SESSION_ID_HEADER, value);
    }
    request.body(Body::from(body.to_string())).expect("request")
}

async fn send(router: &Router, request: Request<Body>) -> Response {
    router.clone().oneshot(request).await.expect("response")
}

async fn assert_sessionless_non_post_requests_rejected(router: &Router) {
    for (method, expected) in [
        (Method::GET, StatusCode::BAD_REQUEST),
        (Method::DELETE, StatusCode::BAD_REQUEST),
        (Method::PUT, StatusCode::METHOD_NOT_ALLOWED),
    ] {
        let mut request = auth_request("Bearer token-a", None, "");
        *request.method_mut() = method;
        assert_eq!(send(router, request).await.status(), expected);
    }
}

async fn assert_invalid_initialize_protocols_rejected(router: &Router) {
    let invalid_protocols = [
        vec![HeaderValue::from_static("2025-06-18")],
        vec![
            HeaderValue::from_static("2025-03-26"),
            HeaderValue::from_static("2025-03-26"),
        ],
        vec![
            HeaderValue::from_static("2025-03-26"),
            HeaderValue::from_static("2025-06-18"),
        ],
        vec![HeaderValue::from_bytes(b"bad\x80version").unwrap()],
        vec![HeaderValue::from_static("")],
    ];
    for protocols in invalid_protocols {
        let mut invalid = auth_request("Bearer token-a", None, INITIALIZE);
        for protocol in protocols {
            invalid
                .headers_mut()
                .append("mcp-protocol-version", protocol);
        }
        assert_eq!(
            send(router, invalid).await.status(),
            StatusCode::BAD_REQUEST
        );
    }
}

/// The one ordinary workspace the HTTP fixtures work in. A fresh app owns no
/// workspace, so a fixture whose handshake must be admitted creates it first —
/// the URL names it, and admission answers existence from a live listing.
const TEST_WORKSPACE: &str = "analytics";

/// Creates [`TEST_WORKSPACE`] and returns the surface's template options.
///
/// The options name no workspace: each request's URL does, so the only thing a
/// fixture must do for a handshake at [`TEST_WORKSPACE`]'s URL to be admitted
/// is make the workspace exist.
async fn workspace_scoped_options(app: &AppClient) -> McpOptions {
    create_workspace(app, TEST_WORKSPACE).await;
    McpOptions::default()
}

/// Creates one workspace through the same public RPC any client would use.
async fn create_workspace(app: &AppClient, name: &str) {
    app.workspace_client()
        .create_workspace(GrpcRequest::new(CreateWorkspaceRequest {
            workspace: Some(workspace(name)),
        }))
        .await
        .expect("create test workspace");
}

async fn local_app() -> (TempDir, coral_client::local::RunningServer, AppClient) {
    let temp = TempDir::new().expect("temp dir");
    let server = ServerBuilder::new()
        .with_config_dir(temp.path().join("coral-config"))
        .with_noop_feedback_uploads()
        .start()
        .await
        .expect("start app server");
    let app = AppClient::connect(server.endpoint_uri())
        .await
        .expect("connect app client");
    (temp, server, app)
}

async fn open_stalled_request(server: &RunningMcpHttpServer) -> TcpStream {
    let mut stalled = TcpStream::connect(server.local_addr())
        .await
        .expect("connect stalled request");
    stalled
        .write_all(
            format!(
                "POST {} HTTP/1.1\r\nHost: {}\r\nAccept: application/json, text/event-stream\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n1\r\n{{\r\n",
                ws_path(TEST_WORKSPACE),
                server.local_addr()
            )
            .as_bytes(),
        )
        .await
        .expect("write partial request");
    stalled.flush().await.expect("flush partial request");
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.state.requests.try_write().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("stalled request was accepted");
    stalled
}

type WeakLocalWorkspaces = std::sync::Weak<
    tokio::sync::RwLock<std::collections::HashMap<McpWorkspaceSegment, super::WorkspaceSessions>>,
>;

fn local_workspaces(server: &RunningMcpHttpServer) -> WeakLocalWorkspaces {
    match &server.state.sessions {
        SessionOwner::Local(workspaces) => Arc::downgrade(workspaces),
        SessionOwner::Authenticated(_) => panic!("expected local sessions"),
    }
}

/// Counts live sessions across the map a running auth-disabled server owns.
async fn running_local_session_count(workspaces: &WeakLocalWorkspaces) -> usize {
    let workspaces = workspaces.upgrade().expect("workspaces");
    let mut count = 0;
    for entry in workspaces.read().await.values() {
        count += entry.sessions.sessions.read().await.len();
    }
    count
}

/// The workspace names the auth-disabled server currently caches an entry for.
async fn cached_workspace_names(workspaces: &WeakLocalWorkspaces) -> Vec<String> {
    let workspaces = workspaces.upgrade().expect("workspaces");
    let mut names: Vec<String> = workspaces
        .read()
        .await
        .keys()
        .map(|name| name.as_str().to_string())
        .collect();
    names.sort();
    names
}

fn authenticated_sessions(
    server: &RunningMcpHttpServer,
) -> std::sync::Weak<super::AuthenticatedSessions> {
    match &server.state.sessions {
        SessionOwner::Authenticated(sessions) => Arc::downgrade(sessions),
        SessionOwner::Local(_) => panic!("expected authenticated sessions"),
    }
}

#[test]
fn auth_disabled_config_accepts_only_real_loopback_binds() {
    for ip in ["127.0.0.1", "127.42.3.9", "::1", "::ffff:127.0.0.1"] {
        let address = SocketAddr::new(ip.parse().expect("IP"), 8080);
        McpHttpConfig::new(address).expect("loopback");
    }
    for ip in ["0.0.0.0", "192.0.2.1", "::", "::ffff:192.0.2.1"] {
        let address = SocketAddr::new(ip.parse().expect("IP"), 8080);
        assert!(matches!(
            McpHttpConfig::new(address),
            Err(McpHttpError::NonLoopbackBind(_))
        ));
    }
}

#[test]
fn exposure_consent_constructor_accepts_non_loopback_binds() {
    for ip in ["0.0.0.0", "192.0.2.1", "::"] {
        let address = SocketAddr::new(ip.parse().expect("IP"), 8080);
        let config = McpHttpConfig::allow_unauthenticated_non_loopback(address);
        assert_eq!(config.bind_addr(), address);
    }
}

#[test]
fn allowed_hosts_must_be_valid_header_values() {
    let config = McpHttpConfig::new(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080))
        .expect("loopback");
    assert!(matches!(
        config.with_allowed_hosts(["bad\nhost".to_string()]),
        Err(McpHttpError::InvalidAuthConfig(_))
    ));
}

#[tokio::test]
async fn auth_disabled_router_accepts_loopback_names_and_configured_hosts() {
    let (_temp, app_server, app) = local_app().await;
    // Host acceptance is what this asserts, but a handshake is only admitted
    // for a workspace that exists, so the fixture creates the one its URL names.
    let options = workspace_scoped_options(&app).await;
    let (router, state) = auth_disabled_router(
        app.clone(),
        options,
        ReadinessProbe::from_app(app),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        &["coral".to_string()],
    );

    // The baseline loopback names and the operator-listed host all initialize;
    // anything else keeps hitting the DNS-rebinding 403.
    for host in [
        "localhost:14556",
        "127.0.0.1:14556",
        "[::1]:14556",
        "coral:14556",
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(ws_path(TEST_WORKSPACE))
                    .header(header::HOST, host)
                    .header(header::ACCEPT, "application/json, text/event-stream")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(INITIALIZE))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK, "Host {host}");
    }

    let response = router
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(ws_path(TEST_WORKSPACE))
                .header(header::HOST, "coral.example")
                .header(header::ACCEPT, "application/json, text/event-stream")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(INITIALIZE))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    state.server.config.cancellation_token.cancel();
    state.server.sessions.close_all().await;
    app_server.shutdown().await.expect("shutdown app server");
}

#[tokio::test]
async fn raw_routes_enforce_health_and_host_contracts() {
    let (_temp, app_server, app) = local_app().await;
    let advertised_ip = IpAddr::V4(Ipv4Addr::new(127, 42, 3, 9));
    let options = workspace_scoped_options(&app).await;
    let (router, state) = auth_disabled_router(
        app.clone(),
        options,
        ReadinessProbe::from_app(app),
        advertised_ip,
        &[],
    );

    for path in ["/livez", "/readyz"] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(ws_path(TEST_WORKSPACE))
                .header(header::HOST, "attacker.example")
                .header(header::ACCEPT, "application/json, text/event-stream")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(INITIALIZE))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(ws_path(TEST_WORKSPACE))
                .header(header::HOST, "127.42.3.9:8080")
                .header(header::ORIGIN, "https://attacker.example")
                .header(header::ACCEPT, "application/json, text/event-stream")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(INITIALIZE))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(local_session_count(&state).await, 0);

    let response = router
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(ws_path(TEST_WORKSPACE))
                .header(header::HOST, "127.42.3.9:8080")
                .header(header::ACCEPT, "application/json, text/event-stream")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(INITIALIZE))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    state.server.config.cancellation_token.cancel();
    state.server.sessions.close_all().await;
    app_server.shutdown().await.expect("shutdown app server");
}

#[tokio::test]
async fn mcp_rejects_uninitialized_and_oversized_requests_without_leaking_sessions() {
    let (_temp, app_server, app) = local_app().await;
    let options = workspace_scoped_options(&app).await;
    let (router, state) = auth_disabled_router(
        app.clone(),
        options,
        ReadinessProbe::from_app(app),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        &[],
    );

    for body in [
        PING,
        r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
    ] {
        assert_bad_request_without_session(&router, &state, raw_mcp_request(body)).await;
    }

    let mut invalid_session_request = raw_mcp_request(PING);
    invalid_session_request.headers_mut().insert(
        SESSION_ID_HEADER,
        HeaderValue::from_bytes(b"invalid\x80session").expect("opaque header"),
    );
    assert_bad_request_without_session(&router, &state, invalid_session_request).await;

    let invalid_protocols = [
        vec![HeaderValue::from_static("2025-06-18")],
        vec![
            HeaderValue::from_static("2025-03-26"),
            HeaderValue::from_static("2025-03-26"),
        ],
        vec![
            HeaderValue::from_static("2025-03-26"),
            HeaderValue::from_static("2025-06-18"),
        ],
        vec![HeaderValue::from_bytes(b"bad\x80version").expect("opaque header")],
        vec![HeaderValue::from_static("")],
    ];
    for protocols in invalid_protocols {
        let mut request = raw_mcp_request(INITIALIZE);
        for protocol in protocols {
            request
                .headers_mut()
                .append("mcp-protocol-version", protocol);
        }
        assert_bad_request_without_session(&router, &state, request).await;
    }

    let mut initialize = raw_mcp_request(INITIALIZE);
    initialize.headers_mut().insert(
        "mcp-protocol-version",
        HeaderValue::from_static("2025-03-26"),
    );
    let initialized = router.clone().oneshot(initialize).await.expect("response");
    assert_eq!(initialized.status(), StatusCode::OK);
    let session_id = initialized
        .headers()
        .get(SESSION_ID_HEADER)
        .expect("session ID")
        .clone();

    let mut oversized = PING.as_bytes().to_vec();
    oversized.resize(MAX_MCP_REQUEST_BODY_SIZE + 1, b' ');
    let mut oversized_request = raw_mcp_request(oversized);
    oversized_request
        .headers_mut()
        .insert(SESSION_ID_HEADER, session_id.clone());
    oversized_request.headers_mut().insert(
        "mcp-protocol-version",
        HeaderValue::from_static("2025-03-26"),
    );
    let response = router
        .clone()
        .oneshot(oversized_request)
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(local_session_count(&state).await, 1);

    let mut ping = raw_mcp_request(PING);
    ping.headers_mut().insert(SESSION_ID_HEADER, session_id);
    ping.headers_mut().insert(
        "mcp-protocol-version",
        HeaderValue::from_static("2025-03-26"),
    );
    let response = send(&router, ping).await;
    assert!(response.status().is_success());

    state.server.config.cancellation_token.cancel();
    state.server.sessions.close_all().await;
    app_server.shutdown().await.expect("shutdown app server");
}

#[tokio::test]
async fn readyz_classifies_auth_transport_and_timeout_results() {
    for (code, expected) in [
        (tonic::Code::Unauthenticated, StatusCode::NO_CONTENT),
        (tonic::Code::PermissionDenied, StatusCode::NO_CONTENT),
        (tonic::Code::Unavailable, StatusCode::SERVICE_UNAVAILABLE),
    ] {
        let probe = ReadinessProbe(Arc::new(move || Box::pin(async move { Err(code) })));
        assert_eq!(
            readiness_status(&probe, Duration::from_millis(10)).await,
            expected
        );
    }

    let pending = ReadinessProbe(Arc::new(|| {
        Box::pin(std::future::pending::<Result<(), tonic::Code>>())
    }));
    assert_eq!(
        readiness_status(&pending, Duration::from_millis(10)).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

/// Reads the workspace the session at one workspace URL was scoped to.
async fn session_workspace_line(server: &RunningMcpHttpServer, workspace: &str) -> String {
    let transport = StreamableHttpClientTransport::from_uri(format!(
        "http://{}{}",
        server.local_addr(),
        ws_path(workspace)
    ));
    let client = ().serve(transport).await.expect("initialize MCP client");
    let line = client
        .peer_info()
        .expect("initialize result")
        .instructions
        .as_deref()
        .expect("initialize instructions")
        .lines()
        .find(|line| line.starts_with("Current Coral workspace:"))
        .expect("workspace line")
        .to_string();
    let _cancel_result = client.cancel().await;
    line
}

/// Every existing workspace is served at its own URL, each session scoped to
/// exactly the workspace its URL names — with several workspaces existing, so
/// any single-workspace selection rule would have to pick wrong somewhere.
#[tokio::test]
async fn auth_disabled_routes_each_workspace_at_its_own_url() {
    let (_temp, app_server, app) = local_app().await;
    let options = workspace_scoped_options(&app).await;
    create_workspace(&app, "reporting").await;
    let config =
        McpHttpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("loopback config");
    let server = start_auth_disabled(config, app, options)
        .await
        .expect("start MCP HTTP server");

    assert_eq!(
        session_workspace_line(&server, TEST_WORKSPACE).await,
        format!("Current Coral workspace: {TEST_WORKSPACE}.")
    );
    assert_eq!(
        session_workspace_line(&server, "reporting").await,
        "Current Coral workspace: reporting."
    );

    server.shutdown().await.expect("shutdown MCP HTTP server");
    app_server.shutdown().await.expect("shutdown app server");
}

/// The server starts with no workspaces at all, and existence is answered per
/// handshake with negatives never cached: a URL that is not-found now becomes
/// servable the moment its workspace is created, with nothing restarted.
#[tokio::test]
async fn auth_disabled_serves_with_no_workspaces_and_admits_one_created_later() {
    let (_temp, app_server, app) = local_app().await;
    let config =
        McpHttpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("loopback config");
    let server = start_auth_disabled(config, app.clone(), McpOptions::default())
        .await
        .expect("a server with nothing to serve still starts");

    let response = initialize_raw(&server, TEST_WORKSPACE).await;
    assert_eq!(
        response.0,
        StatusCode::NOT_FOUND,
        "before the workspace exists its URL is a plain not-found"
    );
    assert!(
        response.1.is_empty(),
        "an unknown workspace says nothing at all: {:?}",
        response.1
    );

    create_workspace(&app, TEST_WORKSPACE).await;
    assert_eq!(
        session_workspace_line(&server, TEST_WORKSPACE).await,
        format!("Current Coral workspace: {TEST_WORKSPACE}."),
        "the probe that found nothing must not have been cached"
    );

    server.shutdown().await.expect("shutdown MCP HTTP server");
    app_server.shutdown().await.expect("shutdown app server");
}

/// Deleting a workspace makes its URL refuse new handshakes immediately.
///
/// The already-open session is the stated boundary: admission is decided per
/// handshake, so the session keeps answering transport-level exchanges while
/// every workspace-scoped call fails against the backend that no longer has
/// the workspace.
#[tokio::test]
async fn auth_disabled_delete_makes_the_url_not_found_for_new_handshakes() {
    let (_temp, app_server, app) = local_app().await;
    let options = workspace_scoped_options(&app).await;
    let config =
        McpHttpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("loopback config");
    let server = start_auth_disabled(config, app.clone(), options)
        .await
        .expect("start MCP HTTP server");

    let transport = StreamableHttpClientTransport::from_uri(format!(
        "http://{}{}",
        server.local_addr(),
        ws_path(TEST_WORKSPACE)
    ));
    let client = ().serve(transport).await.expect("initialize MCP client");

    app.workspace_client()
        .delete_workspace(GrpcRequest::new(DeleteWorkspaceRequest {
            workspace: Some(workspace(TEST_WORKSPACE)),
        }))
        .await
        .expect("delete the workspace");

    let (status, body) = initialize_raw(&server, TEST_WORKSPACE).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a deleted workspace's URL refuses the next handshake like one that never existed"
    );
    assert!(body.is_empty(), "and says nothing at all: {body:?}");

    let refused = client
        .call_tool(CallToolRequestParams::new("start_task").with_arguments(
            serde_json::Map::from_iter([(
                "intent".to_string(),
                serde_json::json!("Reach the deleted workspace"),
            )]),
        ))
        .await
        .expect("a workspace rejection is an in-band tool result");
    assert_eq!(
        refused.is_error,
        Some(true),
        "the surviving session's workspace-scoped calls fail against the backend"
    );

    let _cancel_result = client.cancel().await;
    server.shutdown().await.expect("shutdown MCP HTTP server");
    app_server.shutdown().await.expect("shutdown app server");
}

/// A deleted workspace's dead cache entry is evicted, not kept forever.
///
/// The map would otherwise grow for the process's lifetime across create/delete
/// cycles. Eviction runs on the next handshake and only drops entries whose
/// workspace is gone AND holds no live session, so the surface stays bounded
/// without disturbing a session that is still draining.
#[tokio::test]
async fn auth_disabled_evicts_a_deleted_workspaces_dead_cache_entry() {
    let (_temp, app_server, app) = local_app().await;
    let options = workspace_scoped_options(&app).await;
    create_workspace(&app, "reporting").await;
    let config =
        McpHttpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("loopback config");
    let server = start_auth_disabled(config, app.clone(), options)
        .await
        .expect("start MCP HTTP server");
    let workspaces = local_workspaces(&server);

    // A completed session at TEST_WORKSPACE leaves a cached entry with no live
    // session behind it.
    let opened = StreamableHttpClientTransport::from_uri(format!(
        "http://{}{}",
        server.local_addr(),
        ws_path(TEST_WORKSPACE)
    ));
    let client = ().serve(opened).await.expect("initialize MCP client");
    client.cancel().await.expect("close the session");
    assert_eq!(running_local_session_count(&workspaces).await, 0);
    assert_eq!(
        cached_workspace_names(&workspaces).await,
        vec![TEST_WORKSPACE.to_string()],
        "the completed handshake cached its workspace"
    );

    app.workspace_client()
        .delete_workspace(GrpcRequest::new(DeleteWorkspaceRequest {
            workspace: Some(workspace(TEST_WORKSPACE)),
        }))
        .await
        .expect("delete the workspace");

    // A handshake for a workspace that still exists carries the current
    // inventory, so it evicts the deleted one's dead entry.
    let survivor = StreamableHttpClientTransport::from_uri(format!(
        "http://{}{}",
        server.local_addr(),
        ws_path("reporting")
    ));
    let survivor = ().serve(survivor).await.expect("initialize survivor client");
    assert_eq!(
        cached_workspace_names(&workspaces).await,
        vec!["reporting".to_string()],
        "the deleted workspace's dead entry is gone, the live one remains"
    );

    survivor.cancel().await.expect("close survivor session");
    server.shutdown().await.expect("shutdown MCP HTTP server");
    app_server.shutdown().await.expect("shutdown app server");
}

/// One raw initialize at a workspace URL, as (status, body bytes).
async fn initialize_raw(server: &RunningMcpHttpServer, name: &str) -> (StatusCode, Vec<u8>) {
    let mut stream = TcpStream::connect(server.local_addr())
        .await
        .expect("connect");
    stream
        .write_all(
            format!(
                "POST {} HTTP/1.1\r\nHost: {}\r\nAccept: application/json, text/event-stream\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{INITIALIZE}",
                ws_path(name),
                server.local_addr(),
                INITIALIZE.len(),
            )
            .as_bytes(),
        )
        .await
        .expect("write initialize");
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .expect("response ends")
        .expect("read response");
    let text = String::from_utf8_lossy(&response);
    let status_line = text.lines().next().expect("status line");
    let code = status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse::<u16>()
        .expect("numeric status");
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_headers, body)| body.as_bytes().to_vec())
        .unwrap_or_default();
    (StatusCode::from_u16(code).expect("status"), body)
}

#[tokio::test]
async fn streamable_http_executes_tools_and_shutdown_is_bounded() {
    let (_temp, app_server, app) = local_app().await;
    let config =
        McpHttpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("loopback config");
    let server = start_auth_disabled(
        config,
        app.clone(),
        McpOptions {
            feedback_enabled: true,
            ..workspace_scoped_options(&app).await
        },
    )
    .await
    .expect("start MCP HTTP server");
    let transport = StreamableHttpClientTransport::from_uri(format!(
        "http://{}{}",
        server.local_addr(),
        ws_path(TEST_WORKSPACE)
    ));
    let client = ().serve(transport).await.expect("initialize MCP client");
    let task = client
        .call_tool(CallToolRequestParams::new("start_task").with_arguments(
            serde_json::Map::from_iter([(
                "intent".to_string(),
                serde_json::json!("Exercise the Streamable HTTP transport"),
            )]),
        ))
        .await
        .expect("start task");
    let task = task.structured_content.expect("structured task");
    let task_id = task
        .get("task_id")
        .and_then(serde_json::Value::as_str)
        .expect("task ID")
        .to_string();
    let list_catalog = || {
        CallToolRequestParams::new("list_catalog").with_arguments(serde_json::Map::from_iter([
            ("task_id".to_string(), serde_json::json!(task_id)),
            (
                "intent".to_string(),
                serde_json::json!("Exercise the Streamable HTTP transport"),
            ),
        ]))
    };

    let catalog = client
        .call_tool(list_catalog())
        .await
        .expect("call list_catalog");
    assert_eq!(catalog.is_error, Some(false));

    app_server.shutdown().await.expect("shutdown app server");
    let rejected = client
        .call_tool(list_catalog())
        .await
        .expect("gRPC rejection is an in-band tool result");
    assert_eq!(rejected.is_error, Some(true));

    let sessions = local_workspaces(&server);
    let state = Arc::downgrade(&server.state);
    assert_eq!(running_local_session_count(&sessions).await, 1);
    drop(
        server
            .state
            .requests
            .try_write()
            .expect("request gate idle"),
    );
    let mut stalled = open_stalled_request(&server).await;
    tokio::time::timeout(Duration::from_secs(3), server.shutdown())
        .await
        .expect("shutdown must be bounded")
        .expect("shutdown MCP HTTP server");
    let mut response = Vec::new();
    let eof_or_reset =
        tokio::time::timeout(Duration::from_secs(1), stalled.read_to_end(&mut response))
            .await
            .expect("stalled connection must close");
    if eof_or_reset.is_ok() {
        assert!(response.starts_with(b"HTTP/1.1 503"));
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while sessions.strong_count() != 0 || state.strong_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server-owned state must be released");

    let _cancel_result = client.cancel().await;
}

#[tokio::test]
async fn shutdown_timeout_matches_one_second_contract() {
    assert_eq!(SHUTDOWN_GRACE_PERIOD, Duration::from_secs(1));

    let (_temp, app_server, app) = local_app().await;
    let config =
        McpHttpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("loopback config");
    let server = start_auth_disabled(config, app, McpOptions::default())
        .await
        .expect("start MCP HTTP server");
    let state = server.state.clone();
    let request = state.requests.read().await;

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        server.shutdown_with_grace_period(Duration::ZERO),
    )
    .await
    .expect("zero-grace shutdown must be bounded");
    assert!(matches!(result, Err(McpHttpError::ShutdownTimedOut)));

    drop(request);
    drop(state);
    app_server.shutdown().await.expect("shutdown app server");
}

#[tokio::test]
async fn dropping_server_cancels_requests_and_releases_state() {
    let (_temp, app_server, app) = local_app().await;
    let options = workspace_scoped_options(&app).await;
    let config =
        McpHttpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("loopback config");
    let server = start_auth_disabled(config, app, options)
        .await
        .expect("start MCP HTTP server");
    let transport = StreamableHttpClientTransport::from_uri(format!(
        "http://{}{}",
        server.local_addr(),
        ws_path(TEST_WORKSPACE)
    ));
    let client = ().serve(transport).await.expect("initialize MCP client");

    let sessions = local_workspaces(&server);
    let state = Arc::downgrade(&server.state);
    assert_eq!(running_local_session_count(&sessions).await, 1);
    let mut stalled = open_stalled_request(&server).await;

    drop(server);

    let mut response = Vec::new();
    let eof_or_reset =
        tokio::time::timeout(Duration::from_secs(1), stalled.read_to_end(&mut response))
            .await
            .expect("stalled connection must close");
    if eof_or_reset.is_ok() {
        assert!(response.starts_with(b"HTTP/1.1 503"));
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while sessions.strong_count() != 0 || state.strong_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server-owned state must be released");

    let _cancel_result = client.cancel().await;
    app_server.shutdown().await.expect("shutdown app server");
}

#[tokio::test]
async fn authenticated_session_lifecycle_is_coherent() {
    let config = authenticated_config();
    let (_temp, app_server, app) = local_app().await;
    let options = workspace_scoped_options(&app).await;
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let counted_factory_calls = factory_calls.clone();
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_, _| async { Ok::<_, ()>(()) },
        move |_| {
            counted_factory_calls.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok::<_, ()>(app.clone()))
        },
        options,
        || async { Ok::<_, tonic::Code>(()) },
    );
    let (router, state) = authenticated_router(config, runtime);

    let mut browser_request = auth_request("Bearer token-a", None, INITIALIZE);
    browser_request
        .headers_mut()
        .insert(header::ORIGIN, "https://attacker.example".parse().unwrap());
    assert_eq!(
        send(&router, browser_request).await.status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send(&router, auth_request("Bearer token-a", None, PING))
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_invalid_initialize_protocols_rejected(&router).await;
    assert_eq!(state.sessions.len().await, 0);
    assert_eq!(factory_calls.load(Ordering::Relaxed), 0);

    assert_sessionless_non_post_requests_rejected(&router).await;
    assert_eq!(state.sessions.len().await, 0);
    assert_eq!(factory_calls.load(Ordering::Relaxed), 0);

    let mut initialize = auth_request("Bearer token-a", None, INITIALIZE);
    initialize.headers_mut().insert(
        "mcp-protocol-version",
        HeaderValue::from_static("2025-03-26"),
    );
    let response = send(&router, initialize).await;
    assert_eq!(response.status(), StatusCode::OK);
    let session = response.headers()[SESSION_ID_HEADER]
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(factory_calls.load(Ordering::Relaxed), 1);
    assert_eq!(state.sessions.len().await, 1);

    for invalid in [&b"bad id"[..], &b"bad\tid"[..], &b"bad\x80id"[..]] {
        let invalid = HeaderValue::from_bytes(invalid).unwrap();
        let mut request = auth_request("Bearer token-a", None, PING);
        request.headers_mut().insert(SESSION_ID_HEADER, invalid);
        let status = send(&router, request).await.status();
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    let mismatched = send(
        &router,
        auth_request("Bearer token-b", Some(&session), PING),
    )
    .await;
    assert_eq!(mismatched.status(), StatusCode::FORBIDDEN);
    assert_eq!(state.sessions.len().await, 1);

    let mut ping = auth_request("Bearer token-a", Some(&session), PING);
    ping.headers_mut()
        .insert("mcp-protocol-version", "2025-03-26".parse().unwrap());
    assert!(send(&router, ping).await.status().is_success());
    assert_eq!(factory_calls.load(Ordering::Relaxed), 1);
    let mut delete = auth_request("Bearer token-a", Some(&session), "");
    *delete.method_mut() = Method::DELETE;
    assert!(send(&router, delete).await.status().is_success());
    assert_eq!(state.sessions.len().await, 0);

    let missing = send(
        &router,
        auth_request("Bearer token-a", Some(&session), "{}"),
    )
    .await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    app_server.shutdown().await.unwrap();
}

#[tokio::test]
async fn authenticated_requests_are_bounded_before_and_after_initialization() {
    let (_temp, app_server, app) = local_app().await;
    let options = workspace_scoped_options(&app).await;
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let counted_factory_calls = factory_calls.clone();
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_, _| async { Ok::<_, ()>(()) },
        move |_| {
            counted_factory_calls.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok::<_, ()>(app.clone()))
        },
        options,
        || async { Ok::<_, tonic::Code>(()) },
    );
    let (router, state) = authenticated_router(authenticated_config(), runtime);

    let mut oversized_initialize = INITIALIZE.to_string();
    oversized_initialize.extend(std::iter::repeat_n(
        ' ',
        MAX_MCP_REQUEST_BODY_SIZE + 1 - oversized_initialize.len(),
    ));
    let response = send(
        &router,
        auth_request("Bearer token-a", None, &oversized_initialize),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(factory_calls.load(Ordering::Relaxed), 0);
    assert_eq!(state.sessions.len().await, 0);

    let initialized = send(&router, auth_request("Bearer token-a", None, INITIALIZE)).await;
    let session = initialized.headers()[SESSION_ID_HEADER]
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(factory_calls.load(Ordering::Relaxed), 1);
    assert_eq!(state.sessions.len().await, 1);

    let mut oversized_ping = PING.to_string();
    oversized_ping.extend(std::iter::repeat_n(
        ' ',
        MAX_MCP_REQUEST_BODY_SIZE + 1 - oversized_ping.len(),
    ));
    let mut request = auth_request("Bearer token-a", Some(&session), &oversized_ping);
    request
        .headers_mut()
        .insert("mcp-protocol-version", "2025-03-26".parse().unwrap());
    let response = send(&router, request).await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(state.sessions.len().await, 1);

    let mut ping = auth_request("Bearer token-a", Some(&session), PING);
    ping.headers_mut()
        .insert("mcp-protocol-version", "2025-03-26".parse().unwrap());
    assert!(send(&router, ping).await.status().is_success());

    state.sessions.close_all().await;
    app_server.shutdown().await.unwrap();
}

#[tokio::test]
async fn authenticated_sessions_remain_isolated() {
    let (_temp, app_server, app) = local_app().await;
    let options = workspace_scoped_options(&app).await;
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_, _| async { Ok::<_, ()>(()) },
        move |_| std::future::ready(Ok::<_, ()>(app.clone())),
        options,
        || async { Ok::<_, tonic::Code>(()) },
    );
    let (router, state) = authenticated_router(authenticated_config(), runtime);

    let first = send(&router, auth_request("Bearer token-a", None, INITIALIZE)).await;
    assert_eq!(first.status(), StatusCode::OK);
    let first_session = first.headers()[SESSION_ID_HEADER]
        .to_str()
        .unwrap()
        .to_string();
    let second = send(&router, auth_request("Bearer token-b", None, INITIALIZE)).await;
    assert_eq!(second.status(), StatusCode::OK);
    let second_session = second.headers()[SESSION_ID_HEADER]
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(state.sessions.len().await, 2);

    let response = send(
        &router,
        auth_request("Bearer token-b", Some(&first_session), PING),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(state.sessions.len().await, 2);

    for (token, session) in [
        ("Bearer token-a", &first_session),
        ("Bearer token-b", &second_session),
    ] {
        let mut ping = auth_request(token, Some(session), PING);
        ping.headers_mut()
            .insert("mcp-protocol-version", "2025-03-26".parse().unwrap());
        assert!(send(&router, ping).await.status().is_success());
    }

    state.sessions.close_all().await;
    app_server.shutdown().await.unwrap();
}

#[tokio::test]
async fn authenticated_session_admission_rejects_before_client_creation() {
    let (_temp, app_server, app) = local_app().await;
    let options = workspace_scoped_options(&app).await;
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let counted_factory_calls = factory_calls.clone();
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_, _| async { Ok::<_, ()>(()) },
        move |_| {
            counted_factory_calls.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok::<_, ()>(app.clone()))
        },
        options,
        || async { Ok::<_, tonic::Code>(()) },
    );
    let sessions = Arc::new(AuthenticatedSessions::new(1));
    let (router, state) =
        authenticated_router_with_sessions(authenticated_config(), runtime, sessions);

    let first = send(&router, auth_request("Bearer token-a", None, INITIALIZE)).await;
    assert_eq!(first.status(), StatusCode::OK);
    let first_session = first.headers()[SESSION_ID_HEADER]
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(factory_calls.load(Ordering::Relaxed), 1);
    assert_eq!(state.sessions.len().await, 1);
    assert_eq!(state.sessions.available_permits(), 0);

    let rejected = send(&router, auth_request("Bearer token-b", None, INITIALIZE)).await;
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(factory_calls.load(Ordering::Relaxed), 1);
    assert_eq!(state.sessions.len().await, 1);

    let mut ping = auth_request("Bearer token-a", Some(&first_session), PING);
    ping.headers_mut()
        .insert("mcp-protocol-version", "2025-03-26".parse().unwrap());
    assert!(send(&router, ping).await.status().is_success());

    state.sessions.close_all().await;
    assert_eq!(state.sessions.available_permits(), 1);
    app_server.shutdown().await.unwrap();
}

/// Reads the JSON-RPC error a refused `initialize` is answered with.
///
/// A refusal has to arrive as the initialize response itself, addressed to that
/// request: every later exchange on this surface is workspace-scoped, so a
/// caller refused any other way would learn only that something failed.
async fn refusal_error(response: Response) -> serde_json::Value {
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers().get(SESSION_ID_HEADER).is_none(),
        "a refused initialize must not hand back a session"
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let message: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        message.get("id"),
        Some(&serde_json::json!(1)),
        "the refusal must answer the initialize request: {message}"
    );
    message
        .get("error")
        .expect("a refusal carries a JSON-RPC error")
        .clone()
}

fn refusal_guidance(error: &serde_json::Value) -> &str {
    error
        .get("message")
        .and_then(serde_json::Value::as_str)
        .expect("a refusal carries guidance text")
}

/// The session an admitted `initialize` opened, and the result it answered with.
///
/// A handshake answered as one JSON object and one answered as an event stream
/// carry the same result, so this reads whichever framing arrived rather than
/// pinning one — an event stream opens with an empty priming frame, so the
/// payload is the last one carrying anything. The read is bounded, because a
/// body that never ends is a failure to report rather than a test that hangs
/// until something else gives up on it.
async fn admitted_result(response: Response) -> (String, serde_json::Value) {
    assert_eq!(response.status(), StatusCode::OK);
    let session = response
        .headers()
        .get(SESSION_ID_HEADER)
        .expect("an admitted handshake hands back a session")
        .to_str()
        .expect("the session id is text")
        .to_string();
    let body = tokio::time::timeout(
        Duration::from_secs(5),
        to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("the handshake response body ends")
    .expect("read the handshake response body");
    let text = String::from_utf8(body.to_vec()).expect("the handshake response is text");
    let payload = text
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .rfind(|payload| !payload.is_empty())
        .unwrap_or_else(|| text.trim());
    let message: serde_json::Value = serde_json::from_str(payload)
        .unwrap_or_else(|error| panic!("the handshake response is JSON ({error}): {text}"));
    let result = message
        .get("result")
        .unwrap_or_else(|| panic!("an admitted handshake carries a result: {message}"))
        .clone();
    (session, result)
}

/// The instructions an admitted session opened with.
///
/// They carry the workspace the session was scoped to, which is the only place
/// a caller is told what their session reaches — so it is where a session bound
/// to a workspace nobody configured would show.
fn admitted_instructions(result: &serde_json::Value) -> &str {
    result
        .get("instructions")
        .and_then(serde_json::Value::as_str)
        .expect("an initialize result carries the session's instructions")
}

/// Admits one caller, and says the session reaches `configured` and not `other`.
///
/// Returns the session, so what the caller goes on to hold is the session the
/// workspace claim was made about.
async fn admitted_session_scoped_to(response: Response, configured: &str, other: &str) -> String {
    let (session, result) = admitted_result(response).await;
    let instructions = admitted_instructions(&result);
    assert!(
        instructions.contains(&format!("Current Coral workspace: {configured}.")),
        "the session opens in the workspace its surface configured: {instructions}"
    );
    assert!(
        !instructions.contains(other),
        "no part of the session mentions the other caller's workspace: {instructions}"
    );
    session
}

/// Refuses one caller the configured workspace without offering the one they
/// hold.
///
/// `held` is the accessible workspace a fallback would reach for, so naming it
/// here is what separates "refused" from "refused, and not quietly rerouted".
fn assert_refused_without_a_substitute(error: &serde_json::Value, configured: &str, held: &str) {
    let guidance = refusal_guidance(error);
    assert!(
        guidance.contains(&format!("Workspace `{configured}` was not found")),
        "guidance: {guidance}"
    );
    assert!(
        !guidance.contains(held),
        "the workspace this caller does hold is never offered in place of the configured one: {guidance}"
    );
}

/// One RPC a workspace directory answered, and the credential it carried.
///
/// Which credential asked is the whole of a membership listing's meaning: the
/// same question put on a shared unauthenticated connection returns one
/// deployment-wide answer, so recording the authorization is what separates a
/// per-caller admission decision from a per-deployment one.
#[derive(Clone, Debug, PartialEq, Eq)]
struct DirectoryCall {
    rpc: &'static str,
    authorization: Option<String>,
}

/// Every RPC one workspace directory answered, in order.
type DirectoryCalls = Arc<std::sync::Mutex<Vec<DirectoryCall>>>;

/// The memberships a directory answers with, editable while it keeps serving.
///
/// A membership that can only change across a restart cannot tell a decision
/// made per handshake from one cached at startup, so the map the running
/// directory reads is the same map a test revokes from.
type DirectoryMemberships = Arc<std::sync::Mutex<HashMap<String, Vec<String>>>>;

/// A workspace directory that reveals only the caller's own memberships.
///
/// Memberships are keyed by the bearer the request carries, as the real service
/// scopes them to the authenticated caller. Every RPC is recorded, not only the
/// listing, so a test can claim admission asked one question and no other: a
/// second listing, or any question that could tell a concealed workspace from
/// an absent one, lands in the same log.
struct WorkspaceDirectory {
    memberships: DirectoryMemberships,
    calls: DirectoryCalls,
}

/// Logs one answered RPC and reports the credential it arrived on.
fn record_call(
    calls: &DirectoryCalls,
    rpc: &'static str,
    metadata: &MetadataMap,
) -> Option<String> {
    let authorization = metadata
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    calls
        .lock()
        .expect("record directory call")
        .push(DirectoryCall {
            rpc,
            authorization: authorization.clone(),
        });
    authorization
}

impl WorkspaceDirectory {
    fn record(&self, rpc: &'static str, metadata: &MetadataMap) -> Option<String> {
        record_call(&self.calls, rpc, metadata)
    }

    /// Records a question admission has no business asking, then refuses it.
    ///
    /// Refusing alone would leave the attempt invisible once the caller
    /// recovers from the error, so the log sees it first.
    fn refuse(&self, rpc: &'static str, metadata: &MetadataMap) -> Status {
        self.record(rpc, metadata);
        Status::unimplemented(rpc)
    }
}

#[tonic::async_trait]
impl WorkspaceService for WorkspaceDirectory {
    async fn list_workspaces(
        &self,
        request: GrpcRequest<ListWorkspacesRequest>,
    ) -> Result<GrpcResponse<ListWorkspacesResponse>, Status> {
        let memberships = self
            .record("ListWorkspaces", request.metadata())
            .and_then(|authorization| {
                self.memberships
                    .lock()
                    .expect("read directory memberships")
                    .get(&authorization)
                    .cloned()
            })
            .into_iter()
            .flatten()
            .map(|name| WorkspaceMembership {
                workspace: Some(workspace(name.as_str())),
                role: WorkspaceRole::Owner as i32,
            })
            .collect();
        Ok(GrpcResponse::new(ListWorkspacesResponse { memberships }))
    }

    async fn create_workspace(
        &self,
        request: GrpcRequest<CreateWorkspaceRequest>,
    ) -> Result<GrpcResponse<CreateWorkspaceResponse>, Status> {
        Err(self.refuse("CreateWorkspace", request.metadata()))
    }

    async fn delete_workspace(
        &self,
        request: GrpcRequest<DeleteWorkspaceRequest>,
    ) -> Result<GrpcResponse<DeleteWorkspaceResponse>, Status> {
        Err(self.refuse("DeleteWorkspace", request.metadata()))
    }

    async fn list_workspace_members(
        &self,
        request: GrpcRequest<ListWorkspaceMembersRequest>,
    ) -> Result<GrpcResponse<ListWorkspaceMembersResponse>, Status> {
        Err(self.refuse("ListWorkspaceMembers", request.metadata()))
    }

    async fn add_workspace_member(
        &self,
        request: GrpcRequest<AddWorkspaceMemberRequest>,
    ) -> Result<GrpcResponse<AddWorkspaceMemberResponse>, Status> {
        Err(self.refuse("AddWorkspaceMember", request.metadata()))
    }

    async fn remove_workspace_member(
        &self,
        request: GrpcRequest<RemoveWorkspaceMemberRequest>,
    ) -> Result<GrpcResponse<RemoveWorkspaceMemberResponse>, Status> {
        Err(self.refuse("RemoveWorkspaceMember", request.metadata()))
    }
}

/// The identity half of the same directory, logging into the same record.
///
/// Admission has no identity question to ask: which workspaces a caller holds
/// is answered by the caller's own connection, and the caller's name adds
/// nothing to it. The half is served anyway — and `GetCurrentUser` is
/// *answered* rather than refused — so that "identity was never asked" is an
/// observation instead of an accident of wiring: an admission that asked would
/// be answered and carry on exactly as before, and only the log would differ.
struct IdentityDirectory {
    calls: DirectoryCalls,
}

#[tonic::async_trait]
impl UserService for IdentityDirectory {
    async fn get_current_user(
        &self,
        request: GrpcRequest<GetCurrentUserRequest>,
    ) -> Result<GrpcResponse<GetCurrentUserResponse>, Status> {
        let authorization = record_call(&self.calls, "GetCurrentUser", request.metadata());
        Ok(GrpcResponse::new(GetCurrentUserResponse {
            user: Some(User {
                user_id: authorization.unwrap_or_default(),
                display_name: String::new(),
            }),
        }))
    }

    async fn list_users(
        &self,
        request: GrpcRequest<ListUsersRequest>,
    ) -> Result<GrpcResponse<ListUsersResponse>, Status> {
        record_call(&self.calls, "ListUsers", request.metadata());
        Err(Status::unimplemented("ListUsers"))
    }
}

/// One workspace directory served over loopback gRPC for the length of a test.
struct RunningDirectory {
    endpoint: String,
    calls: DirectoryCalls,
    memberships: DirectoryMemberships,
    task: tokio::task::JoinHandle<()>,
}

impl RunningDirectory {
    /// The RPCs this directory has answered so far.
    fn calls(&self) -> Vec<DirectoryCall> {
        self.calls.lock().expect("read directory calls").clone()
    }

    /// Takes one workspace away from one caller, on the directory now serving.
    ///
    /// Nothing is restarted, rebuilt or reconnected: the map this edits is the
    /// one the running directory answers listings from, so whatever asks next
    /// asks the changed world. The count assertion is what keeps a revocation
    /// from silently doing nothing — a revocation that removed no membership
    /// would leave a test green for the wrong reason.
    fn revoke(&self, token: &str, name: &str) {
        let mut memberships = self.memberships.lock().expect("revoke a membership");
        let held = memberships
            .get_mut(&format!("Bearer {token}"))
            .expect("revoke from a caller this directory knows");
        let before = held.len();
        held.retain(|workspace| workspace != name);
        assert_eq!(
            held.len() + 1,
            before,
            "revoking `{name}` from `{token}` must remove exactly the named membership"
        );
    }
}

impl Drop for RunningDirectory {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Serves one directory whose memberships are keyed by bearer token.
///
/// A directory is a whole world: the workspaces it contains are exactly the
/// ones somebody in it holds. Two directories are therefore what it takes to
/// pose a workspace that exists but is out of reach against one that was never
/// created at all.
async fn serve_directory(memberships: Vec<(&str, Vec<String>)>) -> RunningDirectory {
    let calls = DirectoryCalls::default();
    let memberships: DirectoryMemberships = Arc::new(std::sync::Mutex::new(
        memberships
            .into_iter()
            .map(|(token, names)| (format!("Bearer {token}"), names))
            .collect(),
    ));
    let directory = WorkspaceDirectory {
        memberships: Arc::clone(&memberships),
        calls: Arc::clone(&calls),
    };
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind workspace directory");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("directory address")
    );
    let identity = IdentityDirectory {
        calls: Arc::clone(&calls),
    };
    let task = tokio::spawn(async move {
        let _served = Server::builder()
            .add_service(WorkspaceServiceServer::new(directory))
            .add_service(UserServiceServer::new(identity))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    RunningDirectory {
        endpoint,
        calls,
        memberships,
        task,
    }
}

/// The one question admission may ask, put on one caller's own bearer.
fn listing_by(token: &str) -> DirectoryCall {
    DirectoryCall {
        rpc: "ListWorkspaces",
        authorization: Some(format!("Bearer {token}")),
    }
}

/// The identity question admission has, and must have, no use for.
fn identity_by(token: &str) -> DirectoryCall {
    DirectoryCall {
        rpc: "GetCurrentUser",
        authorization: Some(format!("Bearer {token}")),
    }
}

/// A control-plane RPC, recorded against the credential that attempted it.
///
/// Creating a workspace is an administrator's act, performed by a person in
/// Reef. It is named here only so a test can say the MCP surface never reached
/// it — and so the log can be shown to notice when something does.
fn control_plane_by(token: &str) -> DirectoryCall {
    DirectoryCall {
        rpc: "CreateWorkspace",
        authorization: Some(format!("Bearer {token}")),
    }
}

/// A runtime that admits against one directory and disowns one bearer.
///
/// The disowned bearer stands for a token the authorization server declines to
/// vouch for. Keeping it in the same runtime as the admitted ones is what lets
/// a test compare an authentication answer against an admission answer without
/// changing anything else about the surface.
fn admission_runtime(endpoint: String, disowned: &'static str) -> AuthenticatedMcpHttpRuntime {
    AuthenticatedMcpHttpRuntime::new(
        move |token: String, _audience: String| {
            let vouched = token != disowned;
            async move { if vouched { Ok(()) } else { Err(()) } }
        },
        move |token: String| {
            let endpoint = endpoint.clone();
            async move { bearer_client(&endpoint, &token).await }
        },
        McpOptions::default(),
        || async { Ok::<_, tonic::Code>(()) },
    )
}

/// Connects one caller's own bearer-bound client, the way serving does.
async fn bearer_client(endpoint: &str, token: &str) -> Result<AppClient, ()> {
    let bearer = BearerToken::new(token).map_err(|_error| ())?;
    connect_with_loopback_bearer(endpoint, bearer)
        .await
        .map_err(|_error| ())
}

/// One authenticated surface over one directory of workspaces.
///
/// The surface serves every workspace at its own URL; which workspace a
/// request is about is the URL's to say, so the fixture configures none.
fn surface(directory: &RunningDirectory) -> (Router, super::AuthState) {
    authenticated_router(
        authenticated_config(),
        admission_runtime(directory.endpoint.clone(), "impostor"),
    )
}

/// A surface that cannot build the caller's client answers 503, not not-found.
///
/// Unavailability is a statement about the server, not about any workspace, so
/// it must not wear the concealed refusal's shape — and it must not challenge,
/// because the caller's credential was fine. The directory is asked nothing:
/// admission never got far enough to have a question.
#[tokio::test]
async fn authenticated_admission_unavailability_is_not_an_answer_about_a_workspace() {
    let directory = serve_directory(vec![("member", vec![TEST_WORKSPACE.to_string()])]).await;
    let readiness_calls = Arc::new(AtomicUsize::new(0));
    let counted_readiness_calls = Arc::clone(&readiness_calls);
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_, _| async { Ok::<_, ()>(()) },
        move |_token: String| std::future::ready(Err::<AppClient, ()>(())),
        McpOptions::default(),
        move || {
            counted_readiness_calls.fetch_add(1, Ordering::Relaxed);
            async { Ok::<_, tonic::Code>(()) }
        },
    );
    let (router, state) = authenticated_router(authenticated_config(), runtime);

    let unavailable = send(&router, auth_request("Bearer member", None, INITIALIZE)).await;
    assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        !unavailable.headers().contains_key(header::WWW_AUTHENTICATE),
        "an availability answer is not an authentication answer"
    );
    assert!(
        unavailable.headers().get(SESSION_ID_HEADER).is_none(),
        "no session opens on an undecided admission"
    );
    assert_eq!(readiness_calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        directory.calls(),
        Vec::new(),
        "admission that could not build a client had no question to ask"
    );
    assert_eq!(state.sessions.len().await, 0);
    assert_eq!(
        state.sessions.available_permits(),
        MAX_AUTHENTICATED_SESSIONS
    );
}

/// Admission binds a session only to the exact workspace the caller holds, and
/// learns that from one question asked on the caller's own connection.
///
/// One configured name meets three callers. The member holds it and is
/// admitted. The outsider is in the same directory, where the workspace exists
/// and they simply do not belong to it — they hold only a name that starts the
/// same way, so admitting anyone's first membership would show up here. The
/// stranger is in a directory where no such workspace was ever created. Both
/// are refused with the identical sentence, so nothing in the answer separates
/// a concealed workspace from an absent one — and the member's admission is
/// what keeps that sameness from being "deny everyone".
///
/// The directories record every RPC, so three claims are observable rather than
/// assumed: each admission listed exactly once, each listing carried its own
/// caller's bearer rather than a shared one, and nothing else was ever asked.
#[tokio::test]
async fn authenticated_admission_binds_only_an_exact_membership() {
    let holders = serve_directory(vec![
        ("member", vec![TEST_WORKSPACE.to_string()]),
        ("outsider", vec![format!("{TEST_WORKSPACE}-staging")]),
    ])
    .await;
    let elsewhere = serve_directory(vec![("stranger", Vec::new())]).await;
    let holders_endpoint = holders.endpoint.clone();
    let elsewhere_endpoint = elsewhere.endpoint.clone();
    let readiness_calls = Arc::new(AtomicUsize::new(0));
    let counted_readiness_calls = Arc::clone(&readiness_calls);
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_, _| async { Ok::<_, ()>(()) },
        move |token: String| {
            let endpoint = if token == "stranger" {
                elsewhere_endpoint.clone()
            } else {
                holders_endpoint.clone()
            };
            async move { bearer_client(&endpoint, &token).await }
        },
        McpOptions::default(),
        move || {
            counted_readiness_calls.fetch_add(1, Ordering::Relaxed);
            async { Ok::<_, tonic::Code>(()) }
        },
    );
    let (router, state) = authenticated_router(authenticated_config(), runtime);

    let admitted = send(&router, auth_request("Bearer member", None, INITIALIZE)).await;
    assert_eq!(admitted.status(), StatusCode::OK);
    let session = admitted.headers()[SESSION_ID_HEADER]
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(state.sessions.len().await, 1);
    assert_eq!(
        holders.calls(),
        vec![listing_by("member")],
        "admission asks the caller's own connection once, and asks nothing else"
    );

    let mut ping = auth_request("Bearer member", Some(&session), PING);
    ping.headers_mut()
        .insert("mcp-protocol-version", "2025-03-26".parse().unwrap());
    assert!(send(&router, ping).await.status().is_success());
    assert_eq!(
        holders.calls().len(),
        1,
        "the decision is made once at admission, not again per request"
    );

    let outsider_refusal =
        refusal_error(send(&router, auth_request("Bearer outsider", None, INITIALIZE)).await).await;
    let stranger_refusal =
        refusal_error(send(&router, auth_request("Bearer stranger", None, INITIALIZE)).await).await;

    assert_eq!(
        outsider_refusal, stranger_refusal,
        "a workspace out of reach and one that does not exist must read alike"
    );
    let guidance = refusal_guidance(&outsider_refusal);
    assert!(
        guidance.contains(&format!("Workspace `{TEST_WORKSPACE}` was not found")),
        "the answer they share must be the not-found contract: {guidance}"
    );
    assert_eq!(
        holders.calls(),
        vec![listing_by("member"), listing_by("outsider")],
        "every admission lists once, on its own caller's bearer"
    );
    assert_eq!(elsewhere.calls(), vec![listing_by("stranger")]);
    assert_eq!(
        readiness_calls.load(Ordering::Relaxed),
        0,
        "the unauthenticated client stays restricted to health readiness"
    );
    assert_eq!(
        state.sessions.len().await,
        1,
        "a refused caller opens no session"
    );
    assert_eq!(
        state.sessions.available_permits(),
        MAX_AUTHENTICATED_SESSIONS - 1,
        "a refusal releases the admission it reserved"
    );

    state.sessions.close_all().await;
}

/// Three ways to be turned away stay three answers for whoever runs the server.
///
/// A caller the authorization server will not vouch for, a workspace the
/// caller does not hold, and a server that cannot decide are three different
/// things to fix — a token, a membership, an outage — and an operator reading
/// one response has to know which. Concealment is owed to the *caller* about
/// which workspaces exist, and this is the boundary of that debt: it never
/// obliged the surface to answer "no" the same way to questions that are not
/// about a workspace at all.
///
/// The rejected bearer never reaches the directory — authentication is settled
/// before the membership question is worth asking — and the unavailable
/// surface asks nothing either, so neither refusal can be the membership
/// refusal in disguise.
#[tokio::test]
async fn authenticated_admission_keeps_its_three_refusals_apart() {
    let directory = serve_directory(vec![("member", vec![TEST_WORKSPACE.to_string()])]).await;
    let (router, state) = surface(&directory);

    let unvouched = send(&router, auth_request("Bearer impostor", None, INITIALIZE)).await;
    assert_eq!(unvouched.status(), StatusCode::UNAUTHORIZED);
    assert!(
        unvouched.headers().contains_key(header::WWW_AUTHENTICATE),
        "an authentication answer sends the caller back to the authorization server"
    );
    assert_eq!(
        directory.calls(),
        Vec::new(),
        "a bearer nobody vouches for is turned away before any membership question"
    );

    let unreachable = send(&router, auth_request("Bearer stranger", None, INITIALIZE)).await;
    assert!(
        !unreachable.headers().contains_key(header::WWW_AUTHENTICATE),
        "an admission answer must not read as an authentication failure"
    );
    let unreachable = refusal_error(unreachable).await;
    assert!(
        refusal_guidance(&unreachable)
            .contains(&format!("Workspace `{TEST_WORKSPACE}` was not found")),
        "guidance: {}",
        refusal_guidance(&unreachable)
    );

    let (broken_router, broken_state) = authenticated_router(
        authenticated_config(),
        AuthenticatedMcpHttpRuntime::new(
            |_, _| async { Ok::<_, ()>(()) },
            |_token: String| std::future::ready(Err::<AppClient, ()>(())),
            McpOptions::default(),
            || async { Ok::<_, tonic::Code>(()) },
        ),
    );
    let unavailable = send(
        &broken_router,
        auth_request("Bearer member", None, INITIALIZE),
    )
    .await;
    assert_eq!(
        unavailable.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "a server that cannot decide says so, in nobody's refusal shape"
    );
    assert!(
        !unavailable.headers().contains_key(header::WWW_AUTHENTICATE),
        "an availability answer must not read as an authentication failure"
    );

    assert_eq!(
        directory.calls(),
        vec![listing_by("stranger")],
        "only the membership refusal had a question to ask"
    );
    assert_eq!(state.sessions.len().await, 0);
    assert_eq!(
        state.sessions.available_permits(),
        MAX_AUTHENTICATED_SESSIONS
    );
    assert_eq!(broken_state.sessions.len().await, 0);
    assert_eq!(
        broken_state.sessions.available_permits(),
        MAX_AUTHENTICATED_SESSIONS
    );
}

/// Admission asks one question, of the directory's membership half only.
///
/// The listing decides the session once and is not repeated per request, and
/// the identity half is never consulted at all: who the caller is cannot
/// broaden what they may reach, so asking would only invite deciding on it.
/// Both claims are made against a log that demonstrably sees what it denies —
/// a second admission lands a second listing in it, and the identity question
/// this directory answers lands there too the moment anyone asks it.
///
/// The neighbour is the fallback's last hiding place: they hold exactly one
/// workspace, which is precisely the shape a "resolve the caller's only
/// workspace" rule was written for. They are refused the configured name and
/// never offered their own.
#[tokio::test]
async fn authenticated_admission_asks_memberships_once_and_identity_never() {
    let directory = serve_directory(vec![
        ("member", vec![TEST_WORKSPACE.to_string()]),
        ("neighbor", vec!["reporting".to_string()]),
    ])
    .await;
    let (router, state) = surface(&directory);

    let admitted = send(&router, auth_request("Bearer member", None, INITIALIZE)).await;
    assert_eq!(admitted.status(), StatusCode::OK);
    let session = admitted.headers()[SESSION_ID_HEADER]
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(directory.calls(), vec![listing_by("member")]);

    let mut ping = auth_request("Bearer member", Some(&session), PING);
    ping.headers_mut()
        .insert("mcp-protocol-version", "2025-03-26".parse().unwrap());
    assert!(send(&router, ping).await.status().is_success());
    assert_eq!(
        directory.calls(),
        vec![listing_by("member")],
        "an admitted session re-asks nothing"
    );

    let neighbor =
        refusal_error(send(&router, auth_request("Bearer neighbor", None, INITIALIZE)).await).await;
    let guidance = refusal_guidance(&neighbor);
    assert!(
        guidance.contains(&format!("Workspace `{TEST_WORKSPACE}` was not found")),
        "guidance: {guidance}"
    );
    assert!(
        !guidance.contains("reporting"),
        "the one workspace a caller does hold is never offered in place of the configured one: {guidance}"
    );
    assert_eq!(
        state.sessions.len().await,
        1,
        "a sole membership is refused, not resolved into a session"
    );

    assert_eq!(
        directory.calls(),
        vec![listing_by("member"), listing_by("neighbor")],
        "a second admission is a second listing, so counting one earlier meant something"
    );
    assert!(
        directory
            .calls()
            .iter()
            .all(|call| call.rpc != "GetCurrentUser"),
        "admission has no identity question: {:?}",
        directory.calls()
    );

    let asker = bearer_client(&directory.endpoint, "member")
        .await
        .expect("bearer client");
    asker
        .user_client()
        .get_current_user(GrpcRequest::new(GetCurrentUserRequest {}))
        .await
        .expect("this directory answers identity");
    assert_eq!(
        directory.calls().last(),
        Some(&identity_by("member")),
        "the identity question is one this log records, so its absence above was observed"
    );

    state.sessions.close_all().await;
}

/// The refusal lands on the handshake, before any session exists to misuse.
///
/// `initialize` is the only exchange on this surface that is not
/// workspace-scoped, so it is the only one that can carry a sentence an
/// operator can act on. Admitting a memberless caller and letting `tools/list`
/// fail instead would spend that one chance: the caller would be left holding a
/// live session and a bare not-found, with nothing saying which workspace was
/// meant or what to change.
///
/// So the handshake is answered with the guidance, and the two ways a caller
/// might still try to reach a tool — asking without a session, and inventing
/// one — are shown to lead nowhere near a workspace.
#[tokio::test]
async fn authenticated_admission_refuses_the_handshake_not_the_first_tool_call() {
    let directory = serve_directory(vec![("orphan", Vec::new())]).await;
    let (router, state) = surface(&directory);

    let refusal =
        refusal_error(send(&router, auth_request("Bearer orphan", None, INITIALIZE)).await).await;
    let guidance = refusal_guidance(&refusal);
    assert!(
        guidance.contains(&format!("Workspace `{TEST_WORKSPACE}` was not found")),
        "guidance: {guidance}"
    );
    assert!(
        guidance.contains("Check the workspace URL"),
        "the handshake is where a way out still fits: {guidance}"
    );
    assert_eq!(state.sessions.len().await, 0);
    assert_eq!(
        state.sessions.available_permits(),
        MAX_AUTHENTICATED_SESSIONS
    );

    let sessionless = send(&router, auth_request("Bearer orphan", None, TOOLS_LIST)).await;
    assert_eq!(
        sessionless.status(),
        StatusCode::BAD_REQUEST,
        "only a handshake opens a session, so no tool call can skip the refusal"
    );

    let invented = send(
        &router,
        auth_request(
            "Bearer orphan",
            Some("00000000-0000-0000-0000-000000000000"),
            TOOLS_LIST,
        ),
    )
    .await;
    assert_eq!(invented.status(), StatusCode::NOT_FOUND);
    let body = to_bytes(invented.into_body(), usize::MAX).await.unwrap();
    assert!(
        body.is_empty(),
        "a session that was never opened says nothing about any workspace: {body:?}"
    );

    assert_eq!(
        directory.calls(),
        vec![listing_by("orphan")],
        "the handshake asked once and nothing after it asked again"
    );
}

/// Two callers in one deployment, each holding a workspace the other does not.
///
/// Neither name contains the other, so "this answer never mentioned the other
/// workspace" cannot be satisfied by the other name hiding inside this one.
const ALPHA_WORKSPACE: &str = "alpha-ledger";
const BETA_WORKSPACE: &str = "beta-ledger";

/// Two callers, two workspace URLs on one surface, and neither is ever served
/// the other's workspace.
///
/// The URL names the workspace, so two people sharing one deployment reach
/// their workspaces through one surface — each at the URL naming the workspace
/// that person holds. What has to hold is that the pairing is the only one
/// that works: each caller is admitted at their own workspace's URL, scoped to
/// exactly that workspace, and turned away at the other's.
///
/// The refusals are where a fallback would surface. Both of these callers hold a
/// workspace, and it is an accessible one — exactly the material a "serve
/// whatever they can reach" rule needs — so being refused the name they do not
/// hold, and never being offered the name they do, is the claim.
///
/// The cross-workspace session replay is the sharp edge: beta's session id is
/// presented at alpha's URL *by beta, whose bearer is valid where the session
/// really lives* — and it is still a plain not-found, because at that URL the
/// session does not exist. Only a wrong bearer for a session that does exist
/// at the URL earns the 403.
///
/// Nothing here asks the directory anything but a membership listing, and the
/// log proves the absence rather than assuming it: one control-plane attempt at
/// the end lands in the same log, so the RPCs an MCP credential must never
/// reach are ones this fixture demonstrably notices.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one scenario walks both workspaces end to end"
)]
async fn authenticated_admission_binds_each_caller_to_the_urls_workspace() {
    let directory = serve_directory(vec![
        ("alpha", vec![ALPHA_WORKSPACE.to_string()]),
        ("beta", vec![BETA_WORKSPACE.to_string()]),
    ])
    .await;
    let (router, state) = surface(&directory);

    let alpha_session = admitted_session_scoped_to(
        send(
            &router,
            auth_request_at(ALPHA_WORKSPACE, "Bearer alpha", None, INITIALIZE),
        )
        .await,
        ALPHA_WORKSPACE,
        BETA_WORKSPACE,
    )
    .await;
    let beta_session = admitted_session_scoped_to(
        send(
            &router,
            auth_request_at(BETA_WORKSPACE, "Bearer beta", None, INITIALIZE),
        )
        .await,
        BETA_WORKSPACE,
        ALPHA_WORKSPACE,
    )
    .await;

    let mut ping = auth_request_at(ALPHA_WORKSPACE, "Bearer alpha", Some(&alpha_session), PING);
    ping.headers_mut()
        .insert("mcp-protocol-version", "2025-03-26".parse().unwrap());
    assert!(send(&router, ping).await.status().is_success());
    assert_eq!(
        send(
            &router,
            auth_request_at(ALPHA_WORKSPACE, "Bearer beta", Some(&alpha_session), PING)
        )
        .await
        .status(),
        StatusCode::FORBIDDEN,
        "one caller's session is not a credential the other can present"
    );
    assert_eq!(
        send(
            &router,
            auth_request_at(ALPHA_WORKSPACE, "Bearer beta", Some(&beta_session), PING)
        )
        .await
        .status(),
        StatusCode::NOT_FOUND,
        "a session opened at one workspace's URL does not exist at another's, \
         even for the bearer that owns it where it lives"
    );
    // The discriminating case for the workspace-before-fingerprint ordering:
    // alpha's own bearer — valid at this URL — presenting beta's foreign
    // session id. Both the workspace and the fingerprint mismatch, so a
    // fingerprint-first check would answer 403 and confirm the foreign id
    // exists to a caller who belongs here; the workspace-first check answers
    // the same 404 an invented id gets, disclosing nothing.
    assert_eq!(
        send(
            &router,
            auth_request_at(ALPHA_WORKSPACE, "Bearer alpha", Some(&beta_session), PING)
        )
        .await
        .status(),
        StatusCode::NOT_FOUND,
        "a foreign workspace's session id is not-found even to a bearer valid at this URL"
    );

    let beta_on_alpha = refusal_error(
        send(
            &router,
            auth_request_at(ALPHA_WORKSPACE, "Bearer beta", None, INITIALIZE),
        )
        .await,
    )
    .await;
    assert_refused_without_a_substitute(&beta_on_alpha, ALPHA_WORKSPACE, BETA_WORKSPACE);

    let alpha_on_beta = refusal_error(
        send(
            &router,
            auth_request_at(BETA_WORKSPACE, "Bearer alpha", None, INITIALIZE),
        )
        .await,
    )
    .await;
    assert_refused_without_a_substitute(&alpha_on_beta, BETA_WORKSPACE, ALPHA_WORKSPACE);

    assert_eq!(state.sessions.len().await, 2);
    assert_eq!(
        state.sessions.available_permits(),
        MAX_AUTHENTICATED_SESSIONS - 2,
        "the refused callers left no admission reserved behind"
    );

    assert_eq!(
        directory.calls(),
        vec![
            listing_by("alpha"),
            listing_by("beta"),
            listing_by("beta"),
            listing_by("alpha"),
        ],
        "every handshake asked once, on its own caller's bearer, and nothing else asked at all"
    );

    let asker = bearer_client(&directory.endpoint, "alpha")
        .await
        .expect("bearer client");
    let attempted = asker
        .workspace_client()
        .create_workspace(GrpcRequest::new(CreateWorkspaceRequest {
            workspace: Some(workspace(BETA_WORKSPACE)),
        }))
        .await;
    assert!(
        attempted.is_err(),
        "an MCP credential is not an administrator"
    );
    assert_eq!(
        directory.calls().last(),
        Some(&control_plane_by("alpha")),
        "a control-plane RPC is one this log records, so its absence above was observed"
    );

    state.sessions.close_all().await;
}

/// One sentence covers every reason the configured workspace is out of reach.
///
/// The debt is concealment, not refusal: a caller must not be able to read an
/// answer and learn whether the name they were configured for exists. So the
/// three situations that could each have grown their own wording are compared
/// against one another whole — code, message and structured data together —
/// rather than each being checked to be *some* error, which is a check that
/// survives exactly the leak it is supposed to catch.
///
/// The three differ in everything the surface could key a leak on. One caller
/// holds a workspace and the configured one exists beside it, held by somebody
/// else. One holds a workspace in a world where the configured name was never
/// created. One holds nothing at all. And the caller who does hold the
/// configured name is admitted from the same surface, so the sameness of the
/// other three is concealment rather than a door that is simply shut.
#[tokio::test]
async fn authenticated_admission_conceals_why_a_workspace_is_out_of_reach() {
    let deployment = serve_directory(vec![
        ("alpha", vec![ALPHA_WORKSPACE.to_string()]),
        ("beta", vec![BETA_WORKSPACE.to_string()]),
    ])
    .await;
    let elsewhere = serve_directory(vec![
        ("nomad", vec!["nomad-ledger".to_string()]),
        ("hermit", Vec::new()),
    ])
    .await;
    let deployment_endpoint = deployment.endpoint.clone();
    let elsewhere_endpoint = elsewhere.endpoint.clone();
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_, _| async { Ok::<_, ()>(()) },
        move |token: String| {
            let endpoint = if token == "alpha" || token == "beta" {
                deployment_endpoint.clone()
            } else {
                elsewhere_endpoint.clone()
            };
            async move { bearer_client(&endpoint, &token).await }
        },
        McpOptions::default(),
        || async { Ok::<_, tonic::Code>(()) },
    );
    let (router, state) = authenticated_router(authenticated_config(), runtime);

    let held_by_another = refusal_error(
        send(
            &router,
            auth_request_at(BETA_WORKSPACE, "Bearer alpha", None, INITIALIZE),
        )
        .await,
    )
    .await;
    let never_created = refusal_error(
        send(
            &router,
            auth_request_at(BETA_WORKSPACE, "Bearer nomad", None, INITIALIZE),
        )
        .await,
    )
    .await;
    let holds_nothing = refusal_error(
        send(
            &router,
            auth_request_at(BETA_WORKSPACE, "Bearer hermit", None, INITIALIZE),
        )
        .await,
    )
    .await;

    assert_eq!(
        held_by_another, never_created,
        "a workspace somebody else holds must read exactly as one that was never created"
    );
    assert_eq!(
        never_created, holds_nothing,
        "holding a workspace elsewhere must not change the answer either"
    );

    let guidance = refusal_guidance(&held_by_another);
    assert!(
        guidance.contains(&format!("Workspace `{BETA_WORKSPACE}` was not found")),
        "the answer they share is the not-found contract: {guidance}"
    );
    assert!(
        !guidance.contains(ALPHA_WORKSPACE) && !guidance.contains("nomad-ledger"),
        "the shared answer names only the workspace that was asked for: {guidance}"
    );

    let (_session, admitted) = admitted_result(
        send(
            &router,
            auth_request_at(BETA_WORKSPACE, "Bearer beta", None, INITIALIZE),
        )
        .await,
    )
    .await;
    let instructions = admitted_instructions(&admitted);
    assert!(
        instructions.contains(&format!("Current Coral workspace: {BETA_WORKSPACE}.")),
        "one sentence for everyone else is concealment only because the holder gets in: {instructions}"
    );

    assert_eq!(
        deployment.calls(),
        vec![listing_by("alpha"), listing_by("beta")],
        "each caller was answered from their own connection"
    );
    assert_eq!(
        elsewhere.calls(),
        vec![listing_by("nomad"), listing_by("hermit")],
        "and the surface never asked the other world whether the name exists there"
    );
    assert_eq!(state.sessions.len().await, 1);
    assert_eq!(
        state.sessions.available_permits(),
        MAX_AUTHENTICATED_SESSIONS - 1,
        "three refusals released the admissions they reserved"
    );

    state.sessions.close_all().await;
}

/// A membership taken away lands on the next handshake, with nothing restarted.
///
/// The server, its session store, its runtime and the directory it asks are the
/// same objects throughout: the only thing that changes is one entry in the
/// directory the running surface reads. So a decision cached anywhere — at
/// startup, per token, per process — would keep admitting the revoked caller,
/// and this is where that shows.
///
/// Two things bound the claim. A colleague who still holds the workspace is
/// still admitted afterwards, so what changed was one membership and not the
/// world. And the session already open goes on working: admission is decided
/// once, at the handshake, and this fixes what "affects the next session"
/// actually buys — a revoked caller cannot open another session, while the one
/// they hold lasts until it is closed or idles out. That is the boundary, stated
/// rather than implied.
#[tokio::test]
async fn authenticated_admission_follows_a_revocation_without_a_restart() {
    let directory = serve_directory(vec![
        ("alpha", vec![ALPHA_WORKSPACE.to_string()]),
        ("colleague", vec![ALPHA_WORKSPACE.to_string()]),
        ("beta", vec![BETA_WORKSPACE.to_string()]),
    ])
    .await;
    let (router, state) = surface(&directory);

    let (session, admitted) = admitted_result(
        send(
            &router,
            auth_request_at(ALPHA_WORKSPACE, "Bearer alpha", None, INITIALIZE),
        )
        .await,
    )
    .await;
    let instructions = admitted_instructions(&admitted);
    assert!(
        instructions.contains(&format!("Current Coral workspace: {ALPHA_WORKSPACE}.")),
        "instructions: {instructions}"
    );

    directory.revoke("alpha", ALPHA_WORKSPACE);

    let mut ping = auth_request_at(ALPHA_WORKSPACE, "Bearer alpha", Some(&session), PING);
    ping.headers_mut()
        .insert("mcp-protocol-version", "2025-03-26".parse().unwrap());
    assert!(
        send(&router, ping).await.status().is_success(),
        "the session already admitted is not re-decided per request"
    );

    let after_revocation = refusal_error(
        send(
            &router,
            auth_request_at(ALPHA_WORKSPACE, "Bearer alpha", None, INITIALIZE),
        )
        .await,
    )
    .await;
    let guidance = refusal_guidance(&after_revocation);
    assert!(
        guidance.contains(&format!("Workspace `{ALPHA_WORKSPACE}` was not found")),
        "the same server that admitted this caller a moment ago now refuses them: {guidance}"
    );

    let never_held = refusal_error(
        send(
            &router,
            auth_request_at(ALPHA_WORKSPACE, "Bearer beta", None, INITIALIZE),
        )
        .await,
    )
    .await;
    assert_eq!(
        after_revocation, never_held,
        "a membership taken away reads exactly like one never held"
    );

    let (_colleague_session, colleague) = admitted_result(
        send(
            &router,
            auth_request_at(ALPHA_WORKSPACE, "Bearer colleague", None, INITIALIZE),
        )
        .await,
    )
    .await;
    let instructions = admitted_instructions(&colleague);
    assert!(
        instructions.contains(&format!("Current Coral workspace: {ALPHA_WORKSPACE}.")),
        "one membership was revoked, not the workspace: {instructions}"
    );

    assert_eq!(
        directory.calls(),
        vec![
            listing_by("alpha"),
            listing_by("alpha"),
            listing_by("beta"),
            listing_by("colleague"),
        ],
        "the second handshake asked the live directory again rather than reusing the first answer"
    );
    assert_eq!(
        state.sessions.len().await,
        2,
        "the refusals opened no sessions, and the revoked caller's first one was not closed"
    );

    state.sessions.close_all().await;
}

#[tokio::test]
async fn authenticated_session_honors_the_declared_idle_timeout() {
    let (_temp, app_server, app) = local_app().await;
    let options = workspace_scoped_options(&app).await;
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_, _| async { Ok::<_, ()>(()) },
        move |_| std::future::ready(Ok::<_, ()>(app.clone())),
        options,
        || async { Ok::<_, tonic::Code>(()) },
    );
    let sessions = Arc::new(AuthenticatedSessions::new(1));
    let (router, state) =
        authenticated_router_with_sessions(authenticated_config(), runtime, sessions);

    let initialized = send(&router, auth_request("Bearer token-a", None, INITIALIZE)).await;
    assert_eq!(initialized.status(), StatusCode::OK);
    let session = initialized.headers()[SESSION_ID_HEADER]
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(state.sessions.available_permits(), 0);

    // Admission is a live gRPC exchange with the app server, so the clock is
    // taken over only once it has finished: auto-advance can otherwise run a
    // request's own deadline out from under it while it waits on the socket.
    tokio::time::pause();
    tokio::task::yield_now().await;
    tokio::time::advance(SessionConfig::DEFAULT_KEEP_ALIVE + Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(state.sessions.len().await, 1);

    let mut ping = auth_request("Bearer token-a", Some(&session), PING);
    ping.headers_mut()
        .insert("mcp-protocol-version", "2025-03-26".parse().unwrap());
    assert!(send(&router, ping).await.status().is_success());

    tokio::task::yield_now().await;
    tokio::time::advance(AUTHENTICATED_SESSION_IDLE_TIMEOUT + Duration::from_secs(1)).await;
    for _ in 0..100 {
        if state.sessions.len().await == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(state.sessions.len().await, 0);
    assert_eq!(state.sessions.available_permits(), 1);

    let missing = send(
        &router,
        auth_request("Bearer token-a", Some(&session), PING),
    )
    .await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    app_server.shutdown().await.unwrap();
}

#[tokio::test]
async fn authenticated_session_creation_is_cancellation_atomic() {
    let sessions = Arc::new(AuthenticatedSessions::default());
    let admission_permit = sessions.try_admit().expect("reserve session");
    let manager = AuthenticatedSessionManager {
        sessions: sessions.clone(),
        workspace: segment(TEST_WORKSPACE),
        fingerprint: binding_fingerprint("token-a"),
        admission_permit: tokio::sync::Mutex::new(Some(admission_permit)),
    };

    let records = sessions.records.write().await;
    let mut blocked_create = Box::pin(manager.create_session());
    assert!(matches!(poll!(blocked_create.as_mut()), Poll::Pending));
    drop(blocked_create);
    drop(records);
    assert_eq!(sessions.len().await, 0);
    assert_eq!(sessions.available_permits(), MAX_AUTHENTICATED_SESSIONS - 1);

    let (session_id, transport) = manager.create_session().await.unwrap();
    assert_eq!(sessions.len().await, 1);
    drop(transport);
    manager.close_session(&session_id).await.unwrap();
    assert_eq!(sessions.len().await, 0);
    assert_eq!(sessions.available_permits(), MAX_AUTHENTICATED_SESSIONS);
}

#[tokio::test]
async fn cancelled_authenticated_session_close_removes_the_record() {
    let sessions = Arc::new(AuthenticatedSessions::default());
    let admission_permit = sessions.try_admit().expect("reserve session");
    let session_id: Arc<str> = Arc::from("blocked-close");
    let fingerprint = binding_fingerprint("token-a");
    let mut config = SessionConfig::default();
    config.channel_capacity = 1;
    let (handle, _worker) = create_local_session(session_id.clone(), config);
    let message: ClientJsonRpcMessage = serde_json::from_str(PING).unwrap();
    handle.push_message(message, None).await.unwrap();
    sessions.records.write().await.insert(
        session_id.clone(),
        AuthenticatedSession {
            workspace: segment(TEST_WORKSPACE),
            fingerprint,
            handle,
            _admission_permit: admission_permit,
        },
    );
    let manager = AuthenticatedSessionManager {
        sessions: sessions.clone(),
        workspace: segment(TEST_WORKSPACE),
        fingerprint,
        admission_permit: tokio::sync::Mutex::new(None),
    };

    let mut close = Box::pin(manager.close_session(&session_id));
    assert!(matches!(poll!(close.as_mut()), Poll::Pending));
    drop(close);
    assert_eq!(sessions.len().await, 0);
    assert_eq!(sessions.available_permits(), MAX_AUTHENTICATED_SESSIONS);
}

#[test]
fn authenticated_config_accepts_only_https_or_loopback_http_public_urls() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    for public_url in ["http://mcp.example.com/mcp", "ftp://mcp.example.com/mcp"] {
        let error =
            AuthenticatedMcpHttpConfig::new(bind_addr, public_url, "https://login.example.com/")
                .unwrap_err();
        assert!(matches!(
            error,
            McpHttpError::InvalidAuthConfig("OAuth URL must use HTTPS or loopback HTTP")
        ));
    }
    for public_url in [
        "http://localhost/mcp",
        "http://127.0.0.1/mcp",
        "http://[::1]/mcp",
        "https://mcp.example.com/mcp",
    ] {
        AuthenticatedMcpHttpConfig::new(bind_addr, public_url, "http://localhost:8080/")
            .expect("HTTPS and loopback HTTP remain valid");
    }
}

#[test]
fn authenticated_config_canonicalizes_oauth_identifiers_and_derives_the_route() {
    let config = AuthenticatedMcpHttpConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "https://mcp.example.com/",
        "https://login.example.com/",
    )
    .unwrap();
    assert_eq!(config.urls.base().identifier(), "https://mcp.example.com");
    assert_eq!(config.authorization_server, "https://login.example.com");
    // A root public URL mounts the workspace family at the origin root; the
    // usual `/mcp` base mounts it under `/mcp`.
    assert_eq!(config.mcp_route(), "/workspace/{workspace}");
    assert_eq!(
        authenticated_config().mcp_route(),
        "/mcp/workspace/{workspace}"
    );
}

/// Discovery is per workspace, uniform across existence, and scope-free.
///
/// The challenge names exactly the metadata URL the wildcard route serves for
/// that workspace, the document's `resource` is exactly that workspace's URL,
/// and none of it depends on whether the workspace exists — challenge and
/// document for a name nobody created are byte-identical to a real one's after
/// substituting the name, so an anonymous probe learns nothing. The base URL's
/// own metadata path names no workspace and is a plain not-found.
#[tokio::test]
async fn authenticated_discovery_is_per_workspace_and_existence_blind() {
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_, _| async { Ok::<_, ()>(()) },
        |_| std::future::ready(Err::<AppClient, ()>(())),
        McpOptions::default(),
        || async { Ok::<_, tonic::Code>(()) },
    );
    let (router, _state) = authenticated_router(authenticated_config(), runtime);

    let mut challenges = Vec::new();
    let mut documents = Vec::new();
    for name in [TEST_WORKSPACE, "never-created"] {
        let unauthorized = send(&router, raw_mcp_request_at(&ws_path(name), INITIALIZE)).await;
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED, "{name}");
        let challenge = unauthorized
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(
            challenge,
            format!(
                "Bearer resource_metadata=\"https://mcp.example.com/.well-known/oauth-protected-resource/mcp/workspace/{name}\""
            ),
            "the challenge names the route's own metadata URL"
        );
        assert!(!challenge.contains("scope="));

        let metadata = send(
            &router,
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/.well-known/oauth-protected-resource/mcp/workspace/{name}"
                ))
                .header(header::HOST, "mcp.example.com")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(metadata.status(), StatusCode::OK, "{name}");
        let body = to_bytes(metadata.into_body(), usize::MAX).await.unwrap();
        let document: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            document.get("resource").expect("resource"),
            &serde_json::json!(format!("https://mcp.example.com/mcp/workspace/{name}"))
        );
        assert_eq!(
            document.get("authorization_servers").expect("servers"),
            &serde_json::json!(["https://login.example.com"])
        );
        assert!(document.get("scopes_supported").is_none());

        challenges.push(challenge.replace(name, "{ws}"));
        documents.push(
            String::from_utf8(body.to_vec())
                .expect("metadata is text")
                .replace(name, "{ws}"),
        );
    }
    assert_eq!(
        challenges.first(),
        challenges.last(),
        "existence must not shape the challenge"
    );
    assert_eq!(
        documents.first(),
        documents.last(),
        "existence must not shape the metadata document"
    );

    // The base metadata path names no workspace: not-found, no document.
    let base = send(
        &router,
        Request::builder()
            .method(Method::GET)
            .uri("/.well-known/oauth-protected-resource/mcp")
            .header(header::HOST, "mcp.example.com")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(base.status(), StatusCode::NOT_FOUND);
}

/// Malformed segments and retired paths are plain not-founds with no challenge
/// and no metadata; the fallback's static hint names only the URL shape.
#[tokio::test]
async fn authenticated_routes_refuse_malformed_and_legacy_paths_plainly() {
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_, _| async { Ok::<_, ()>(()) },
        |_| std::future::ready(Err::<AppClient, ()>(())),
        McpOptions::default(),
        || async { Ok::<_, tonic::Code>(()) },
    );
    let (router, _state) = authenticated_router(authenticated_config(), runtime);

    // A well-formed route with a malformed segment: matched, then refused with
    // nothing at all — no challenge that would make it look protected.
    for path in ["/mcp/workspace/te%61m", "/mcp/workspace/te%2Fam"] {
        let response = send(&router, raw_mcp_request_at(path, INITIALIZE)).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        assert!(
            !response.headers().contains_key(header::WWW_AUTHENTICATE),
            "a non-canonical spelling earns no challenge: {path}"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty(), "{path}: {body:?}");
    }

    // The retired base endpoint and other unmatched paths fall back to the
    // static hint, which names the URL shape and never a workspace.
    for path in ["/mcp", "/mcp/workspace", &format!("{}/", ws_path("team"))] {
        let response = send(&router, raw_mcp_request_at(path, INITIALIZE)).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        assert!(
            !response.headers().contains_key(header::WWW_AUTHENTICATE),
            "{path}"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let text = String::from_utf8(body.to_vec()).expect("hint is text");
        assert!(
            text.contains(WORKSPACE_URL_HINT),
            "the fallback carries the shape hint: {path} -> {text}"
        );
    }
}

/// A bearer minted for one workspace's resource dies at another's URL.
///
/// The runtime's validator here admits a token only for the exact audience it
/// was minted for, the way the real composition does — so what this pins is
/// that the surface hands the validator the route's own resource, making the
/// route the audience boundary. The refusal is an authentication answer with
/// the refusing route's own challenge, never a workspace answer.
#[tokio::test]
async fn a_bearer_for_one_workspace_is_rejected_at_anothers_url() {
    let directory = serve_directory(vec![
        ("alpha", vec![ALPHA_WORKSPACE.to_string()]),
        ("beta", vec![BETA_WORKSPACE.to_string()]),
    ])
    .await;
    let endpoint = directory.endpoint.clone();
    let minted_for = |token: &str| match token {
        "alpha" => format!("https://mcp.example.com/mcp/workspace/{ALPHA_WORKSPACE}"),
        "beta" => format!("https://mcp.example.com/mcp/workspace/{BETA_WORKSPACE}"),
        _ => String::new(),
    };
    let runtime = AuthenticatedMcpHttpRuntime::new(
        move |token: String, audience: String| {
            let matches = minted_for(&token) == audience;
            async move { if matches { Ok(()) } else { Err(()) } }
        },
        move |token: String| {
            let endpoint = endpoint.clone();
            async move { bearer_client(&endpoint, &token).await }
        },
        McpOptions::default(),
        || async { Ok::<_, tonic::Code>(()) },
    );
    let (router, state) = authenticated_router(authenticated_config(), runtime);

    let admitted = send(
        &router,
        auth_request_at(ALPHA_WORKSPACE, "Bearer alpha", None, INITIALIZE),
    )
    .await;
    assert_eq!(
        admitted.status(),
        StatusCode::OK,
        "the bearer works at exactly the URL it was minted for"
    );

    let rejected = send(
        &router,
        auth_request_at(BETA_WORKSPACE, "Bearer alpha", None, INITIALIZE),
    )
    .await;
    assert_eq!(
        rejected.status(),
        StatusCode::UNAUTHORIZED,
        "at any other workspace's URL the same bearer is an invalid token"
    );
    let challenge = rejected
        .headers()
        .get(header::WWW_AUTHENTICATE)
        .expect("an authentication refusal challenges")
        .to_str()
        .unwrap();
    assert!(
        challenge.contains(&format!("/mcp/workspace/{BETA_WORKSPACE}")),
        "the challenge belongs to the refusing route: {challenge}"
    );
    assert_eq!(
        directory.calls(),
        vec![listing_by("alpha")],
        "a wrong-audience bearer never reaches the membership question"
    );

    state.sessions.close_all().await;
}

#[tokio::test]
async fn dropping_authenticated_server_closes_sessions_and_releases_state() {
    let (_temp, app_server, app) = local_app().await;
    // Nothing provisions a workspace any more, so the fixture creates the one
    // its session serves and scopes the options to it.
    let options = workspace_scoped_options(&app).await;
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_, _| async { Ok::<_, ()>(()) },
        move |_| std::future::ready(Ok::<_, ()>(app.clone())),
        McpOptions {
            surface: extension_options().surface,
            ..options
        },
        || async { Ok::<_, tonic::Code>(()) },
    );
    let server = start_authenticated(
        authenticated_config_at("127.0.0.1:0".parse().unwrap()),
        runtime,
    )
    .await;
    let server = server.expect("start authenticated MCP HTTP server");
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!(
            "http://{}{}",
            server.local_addr(),
            ws_path(TEST_WORKSPACE)
        ))
        .auth_header("token-a"),
    );
    let client = ().serve(transport).await.expect("initialize MCP client");
    let extension = client
        .call_tool(CallToolRequestParams::new("extension_workspace"))
        .await
        .expect("call authenticated extension tool")
        .structured_content
        .expect("extension structured content");
    assert_eq!(
        extension.get("workspace"),
        Some(&serde_json::json!(TEST_WORKSPACE)),
        "the session's workspace is the URL's, not a configured default"
    );
    let sessions = authenticated_sessions(&server);
    let state = Arc::downgrade(&server.state);
    assert_eq!(sessions.upgrade().expect("sessions").len().await, 1);

    drop(server);

    tokio::time::timeout(Duration::from_secs(2), async {
        while sessions.strong_count() != 0 || state.strong_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("authenticated server-owned state must be released");

    let _cancel_result = client.cancel().await;
    app_server.shutdown().await.unwrap();
}

#[tokio::test]
async fn authenticated_extension_uses_its_session_app_client() {
    let (_live_temp, live_server, live_app) = local_app().await;
    let (_stopped_temp, stopped_server, stopped_app) = local_app().await;
    create_workspace(&live_app, TEST_WORKSPACE).await;
    stopped_server
        .shutdown()
        .await
        .expect("stop second app server");

    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_, _| async { Ok::<_, ()>(()) },
        move |token| {
            let app = if token == "token-a" {
                live_app.clone()
            } else {
                stopped_app.clone()
            };
            std::future::ready(Ok::<_, ()>(app))
        },
        extension_options(),
        || async { Ok::<_, tonic::Code>(()) },
    );
    let server = start_authenticated(
        authenticated_config_at("127.0.0.1:0".parse().unwrap()),
        runtime,
    )
    .await
    .expect("start authenticated MCP HTTP server");
    let endpoint = format!("http://{}{}", server.local_addr(), ws_path(TEST_WORKSPACE));

    let live_client = ()
        .serve(StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(endpoint.clone()).auth_header("token-a"),
        ))
        .await
        .expect("initialize live MCP session");
    // Admission lists the caller's workspaces on the session's own client, so
    // the stopped app refuses this session at the handshake — which is itself
    // the claim: the session reaches its own client and nobody else's.
    let stopped_session = ()
        .serve(StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(endpoint).auth_header("token-b"),
        ))
        .await;

    let live_result = live_client
        .call_tool(CallToolRequestParams::new("extension_workspace"))
        .await
        .expect("live session extension call")
        .structured_content
        .expect("live session structured result");
    assert!(
        live_result
            .get("core_tool_count")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|count| count > 0)
    );
    assert!(
        stopped_session.is_err(),
        "the stopped session must be refused through its own unavailable client"
    );

    live_client.cancel().await.expect("cancel live MCP client");
    server.shutdown().await.expect("shutdown MCP HTTP server");
    live_server
        .shutdown()
        .await
        .expect("shutdown live app server");
}
