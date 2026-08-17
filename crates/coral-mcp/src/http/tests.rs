use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{HeaderValue, Method, Request, StatusCode, header};
use axum::{Router, response::Response};
use coral_api::v1::workspace_service_server::{WorkspaceService, WorkspaceServiceServer};
use coral_api::v1::{
    AddWorkspaceMemberRequest, AddWorkspaceMemberResponse, CreateWorkspaceRequest,
    CreateWorkspaceResponse, DeleteWorkspaceRequest, DeleteWorkspaceResponse,
    ListWorkspaceMembersRequest, ListWorkspaceMembersResponse, ListWorkspacesRequest,
    ListWorkspacesResponse, RemoveWorkspaceMemberRequest, RemoveWorkspaceMemberResponse,
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
    ReadinessProbe, RunningMcpHttpServer, SESSION_ID_HEADER, SHUTDOWN_GRACE_PERIOD, SessionOwner,
    auth_disabled_router, authenticated_router, authenticated_router_with_sessions,
    binding_fingerprint, readiness_status, start_auth_disabled, start_authenticated,
};

const INITIALIZE: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#;
const PING: &str = r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#;

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

fn raw_mcp_request(body: impl Into<Body>) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri("/mcp")
        .header(header::HOST, "127.0.0.1")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.into())
        .expect("request")
}

async fn assert_bad_request_without_session(
    router: &Router,
    state: &Arc<super::HttpState>,
    request: Request<Body>,
) {
    let response = send(router, request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(state.sessions.sessions.read().await.is_empty());
}

fn authenticated_config() -> AuthenticatedMcpHttpConfig {
    authenticated_config_at("0.0.0.0:0".parse().unwrap())
}

fn authenticated_config_at(bind_addr: SocketAddr) -> AuthenticatedMcpHttpConfig {
    AuthenticatedMcpHttpConfig::new(
        bind_addr,
        "https://mcp.example.com/custom%20mcp",
        "https://login.example.com/",
    )
    .unwrap()
}

fn auth_request(authorization: &str, session: Option<&str>, body: &str) -> Request<Body> {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri("/mcp")
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
/// workspace, so only the tests that reach a workspace-scoped tool create it,
/// and they name it in their [`McpOptions`] rather than leaving the choice to
/// the server.
const TEST_WORKSPACE: &str = "analytics";

/// Creates [`TEST_WORKSPACE`] and returns options scoped to it.
async fn workspace_scoped_options(app: &AppClient) -> McpOptions {
    create_workspace(app, TEST_WORKSPACE).await;
    workspace_named_options()
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

/// Names [`TEST_WORKSPACE`] without creating it.
///
/// The auth-disabled session factory demands a *resolved* workspace, not an
/// existing one: whether the name resolves is answered by the request that
/// needs it. Loopback fixtures that never reach a workspace-scoped tool
/// therefore only have to name one, and naming it is what keeps them from
/// exercising a fallback. Authenticated fixtures need
/// [`workspace_scoped_options`] instead, because admission there checks the
/// name against the caller's memberships before opening a session.
fn workspace_named_options() -> McpOptions {
    McpOptions {
        workspace: Some(workspace(TEST_WORKSPACE)),
        ..McpOptions::default()
    }
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
                "POST /mcp HTTP/1.1\r\nHost: {}\r\nAccept: application/json, text/event-stream\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n1\r\n{{\r\n",
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

fn local_sessions(
    server: &RunningMcpHttpServer,
) -> std::sync::Weak<rmcp::transport::streamable_http_server::session::local::LocalSessionManager> {
    match &server.state.sessions {
        SessionOwner::Local(sessions) => Arc::downgrade(sessions),
        SessionOwner::Authenticated(_) => panic!("expected local sessions"),
    }
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
    let (router, state) = auth_disabled_router(
        app.clone(),
        McpOptions::default(),
        ReadinessProbe::from_app(app),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        &["coral".to_string()],
    )
    .expect("router scoped to a workspace");

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
                    .uri("/mcp")
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
                .uri("/mcp")
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
    let (router, state) = auth_disabled_router(
        app.clone(),
        workspace_named_options(),
        ReadinessProbe::from_app(app),
        advertised_ip,
        &[],
    )
    .expect("router scoped to a workspace");

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
                .uri("/mcp")
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
                .uri("/mcp")
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
    assert!(state.sessions.sessions.read().await.is_empty());

    let response = router
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/mcp")
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
    let (router, state) = auth_disabled_router(
        app.clone(),
        workspace_named_options(),
        ReadinessProbe::from_app(app),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        &[],
    )
    .expect("router scoped to a workspace");

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
    assert_eq!(state.sessions.sessions.read().await.len(), 1);

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

/// Readiness must observe the engine, not a workspace nothing provisions.
///
/// This is the mirror of coral-app's gRPC discriminator: an instance that owns
/// a workspace and cannot resolve its catalog is an unready engine. The probe
/// this replaces asked `ListCatalog` for the literal `default`; nothing creates
/// that workspace any more, so it read `NotFound` — a code
/// `catalog_rejection_is_reachable` deliberately treats as reachable — and
/// answered ready here, which is exactly what this asserts it no longer does.
///
/// The unparseable config file is that engine: catalog resolution reads it
/// after the workspace check and fails with an infrastructure code. The config
/// is corrupted before the first probe so no cached answer predates it.
#[tokio::test]
async fn readyz_reports_unready_when_the_engine_cannot_resolve_a_real_catalog() {
    let (temp, app_server, app) = local_app().await;
    let options = workspace_scoped_options(&app).await;
    std::fs::write(
        temp.path().join("coral-config").join("config.toml"),
        "this is not toml",
    )
    .expect("write unparseable config");
    let (router, state) = auth_disabled_router(
        app.clone(),
        options,
        ReadinessProbe::from_app(app),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        &[],
    )
    .expect("router scoped to a workspace");

    let response = send(
        &router,
        Request::builder()
            .uri("/readyz")
            .body(Body::empty())
            .expect("request"),
    )
    .await;

    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "an engine that cannot resolve a catalog it owns must not report ready"
    );
    state.server.config.cancellation_token.cancel();
    state.server.sessions.close_all().await;
    app_server.shutdown().await.expect("shutdown app server");
}

/// Reads the workspace an initialized session was scoped to.
async fn session_workspace_line(server: &RunningMcpHttpServer) -> String {
    let transport =
        StreamableHttpClientTransport::from_uri(format!("http://{}/mcp", server.local_addr()));
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

/// A configured name is used exactly, including one no workspace answers to.
///
/// Composition validated the name's shape already, so the adapter wraps it
/// rather than re-deriving it, and it never consults memberships: a surface
/// configured today has to start for a workspace created tomorrow. The unnamed
/// second workspace here would make any membership-based choice ambiguous, so
/// starting at all proves the configured name was taken as authoritative.
#[tokio::test]
async fn auth_disabled_workspace_selection_uses_the_configured_name() {
    let (_temp, app_server, app) = local_app().await;
    let options = workspace_scoped_options(&app).await;
    create_workspace(&app, "reporting").await;
    let config =
        McpHttpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("loopback config");
    let server = start_auth_disabled(config, app, options)
        .await
        .expect("start MCP HTTP server");

    assert_eq!(
        session_workspace_line(&server).await,
        format!("Current Coral workspace: {TEST_WORKSPACE}.")
    );

    server.shutdown().await.expect("shutdown MCP HTTP server");
    app_server.shutdown().await.expect("shutdown app server");
}

/// Naming none is answered by the local user's one membership.
#[tokio::test]
async fn auth_disabled_workspace_selection_uses_the_sole_membership() {
    let (_temp, app_server, app) = local_app().await;
    create_workspace(&app, "reporting").await;
    let config =
        McpHttpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("loopback config");
    let server = start_auth_disabled(config, app, McpOptions::default())
        .await
        .expect("start MCP HTTP server");

    assert_eq!(
        session_workspace_line(&server).await,
        "Current Coral workspace: reporting."
    );

    server.shutdown().await.expect("shutdown MCP HTTP server");
    app_server.shutdown().await.expect("shutdown app server");
}

/// Owning nothing and owning several are different problems for the operator,
/// so they get different guidance instead of one workspace picked for them.
#[tokio::test]
async fn auth_disabled_workspace_selection_reports_zero_and_several_distinctly() {
    let (_temp, app_server, app) = local_app().await;
    let config =
        McpHttpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("loopback config");

    let Err(McpHttpError::NoLocalWorkspace) =
        start_auth_disabled(config.clone(), app.clone(), McpOptions::default()).await
    else {
        panic!("a local user with no workspace must be told to create one");
    };

    create_workspace(&app, TEST_WORKSPACE).await;
    create_workspace(&app, "reporting").await;
    let Err(McpHttpError::AmbiguousLocalWorkspace(available)) =
        start_auth_disabled(config, app, McpOptions::default()).await
    else {
        panic!("a local user with several workspaces must be told to name one");
    };
    for name in [TEST_WORKSPACE, "reporting"] {
        assert!(
            available.contains(name),
            "the guidance must name the workspaces to choose between, got {available}"
        );
    }

    app_server.shutdown().await.expect("shutdown app server");
}

/// A configured name nothing answers to is a request-time not-found, not a
/// startup failure: the surface serves, and the tool that needs the workspace
/// reports the ordinary contract rather than reaching another workspace.
#[tokio::test]
async fn auth_disabled_workspace_selection_defers_a_missing_name_to_the_request() {
    let (_temp, app_server, app) = local_app().await;
    create_workspace(&app, "reporting").await;
    let config =
        McpHttpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("loopback config");
    let server = start_auth_disabled(
        config,
        app,
        McpOptions {
            workspace: Some(workspace("never-created")),
            ..McpOptions::default()
        },
    )
    .await
    .expect("a configured name is not checked for existence at startup");

    let transport =
        StreamableHttpClientTransport::from_uri(format!("http://{}/mcp", server.local_addr()));
    let client = ().serve(transport).await.expect("initialize MCP client");
    let refused = client
        .call_tool(CallToolRequestParams::new("start_task").with_arguments(
            serde_json::Map::from_iter([(
                "intent".to_string(),
                serde_json::json!("Reach the workspace that was configured"),
            )]),
        ))
        .await
        .expect("a workspace rejection is an in-band tool result");

    assert_eq!(
        refused.is_error,
        Some(true),
        "a configured workspace that does not exist must be reported, not replaced"
    );
    let reported = format!("{:?}", refused.content);
    assert!(
        reported.contains("never-created"),
        "the refusal must name the workspace that was asked for, got {reported}"
    );

    let _cancel_result = client.cancel().await;
    server.shutdown().await.expect("shutdown MCP HTTP server");
    app_server.shutdown().await.expect("shutdown app server");
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
    let transport =
        StreamableHttpClientTransport::from_uri(format!("http://{}/mcp", server.local_addr()));
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

    let sessions = local_sessions(&server);
    let state = Arc::downgrade(&server.state);
    assert_eq!(
        sessions
            .upgrade()
            .expect("sessions")
            .sessions
            .read()
            .await
            .len(),
        1
    );
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
    let server = start_auth_disabled(config, app, workspace_named_options())
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
    let config =
        McpHttpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("loopback config");
    let server = start_auth_disabled(config, app, workspace_named_options())
        .await
        .expect("start MCP HTTP server");
    let transport =
        StreamableHttpClientTransport::from_uri(format!("http://{}/mcp", server.local_addr()));
    let client = ().serve(transport).await.expect("initialize MCP client");

    let sessions = local_sessions(&server);
    let state = Arc::downgrade(&server.state);
    assert_eq!(
        sessions
            .upgrade()
            .expect("sessions")
            .sessions
            .read()
            .await
            .len(),
        1
    );
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
        |_| async { Ok::<_, ()>(()) },
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
        |_| async { Ok::<_, ()>(()) },
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
        |_| async { Ok::<_, ()>(()) },
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
        |_| async { Ok::<_, ()>(()) },
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

/// A workspace directory that reveals only the caller's own memberships.
///
/// Memberships are keyed by the bearer the request carries, as the real service
/// scopes them to the authenticated caller. Every RPC is recorded, not only the
/// listing, so a test can claim admission asked one question and no other: a
/// second listing, or any question that could tell a concealed workspace from
/// an absent one, lands in the same log.
struct WorkspaceDirectory {
    memberships: HashMap<String, Vec<String>>,
    calls: DirectoryCalls,
}

impl WorkspaceDirectory {
    fn record(&self, rpc: &'static str, metadata: &MetadataMap) -> Option<String> {
        let authorization = metadata
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        self.calls
            .lock()
            .expect("record directory call")
            .push(DirectoryCall {
                rpc,
                authorization: authorization.clone(),
            });
        authorization
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
            .and_then(|authorization| self.memberships.get(&authorization))
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

/// One workspace directory served over loopback gRPC for the length of a test.
struct RunningDirectory {
    endpoint: String,
    calls: DirectoryCalls,
    task: tokio::task::JoinHandle<()>,
}

impl RunningDirectory {
    /// The RPCs this directory has answered so far.
    fn calls(&self) -> Vec<DirectoryCall> {
        self.calls.lock().expect("read directory calls").clone()
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
    let directory = WorkspaceDirectory {
        memberships: memberships
            .into_iter()
            .map(|(token, names)| (format!("Bearer {token}"), names))
            .collect(),
        calls: Arc::clone(&calls),
    };
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind workspace directory");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("directory address")
    );
    let task = tokio::spawn(async move {
        let _served = Server::builder()
            .add_service(WorkspaceServiceServer::new(directory))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    RunningDirectory {
        endpoint,
        calls,
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

/// Connects one caller's own bearer-bound client, the way serving does.
async fn bearer_client(endpoint: &str, token: &str) -> Result<AppClient, ()> {
    let bearer = BearerToken::new(token).map_err(|_error| ())?;
    connect_with_loopback_bearer(endpoint, bearer)
        .await
        .map_err(|_error| ())
}

/// A surface that names no workspace admits nobody, and says what to do.
///
/// The caller holds a workspace here, and it is still not substituted for the
/// one nothing named: an unnamed workspace is a configuration answer, so no
/// bearer-bound client is built and the directory is asked nothing at all —
/// admission cannot pick what it never read. The unauthenticated readiness
/// probe stays untouched too; it is not a way to find a workspace.
#[tokio::test]
async fn authenticated_admission_requires_a_configured_workspace() {
    let directory = serve_directory(vec![("member", vec![TEST_WORKSPACE.to_string()])]).await;
    let endpoint = directory.endpoint.clone();
    let client_calls = Arc::new(AtomicUsize::new(0));
    let counted_client_calls = Arc::clone(&client_calls);
    let readiness_calls = Arc::new(AtomicUsize::new(0));
    let counted_readiness_calls = Arc::clone(&readiness_calls);
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_| async { Ok::<_, ()>(()) },
        move |token: String| {
            counted_client_calls.fetch_add(1, Ordering::Relaxed);
            let endpoint = endpoint.clone();
            async move { bearer_client(&endpoint, &token).await }
        },
        McpOptions::default(),
        move || {
            counted_readiness_calls.fetch_add(1, Ordering::Relaxed);
            async { Ok::<_, tonic::Code>(()) }
        },
    );
    let (router, state) = authenticated_router(authenticated_config(), runtime);

    let refused = send(&router, auth_request("Bearer member", None, INITIALIZE)).await;
    let error = refusal_error(refused).await;

    let guidance = refusal_guidance(&error);
    assert!(
        guidance.contains("no workspace configured"),
        "guidance: {guidance}"
    );
    assert!(
        guidance.contains("Reef") && guidance.contains("server.mcp_http.workspace"),
        "guidance must name both ways out: {guidance}"
    );
    assert_eq!(client_calls.load(Ordering::Relaxed), 0);
    assert_eq!(readiness_calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        directory.calls(),
        Vec::new(),
        "with nothing configured there is no membership question to ask"
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
        |_| async { Ok::<_, ()>(()) },
        move |token: String| {
            let endpoint = if token == "stranger" {
                elsewhere_endpoint.clone()
            } else {
                holders_endpoint.clone()
            };
            async move { bearer_client(&endpoint, &token).await }
        },
        workspace_named_options(),
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

#[tokio::test]
async fn authenticated_session_honors_the_declared_idle_timeout() {
    let (_temp, app_server, app) = local_app().await;
    let options = workspace_scoped_options(&app).await;
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_| async { Ok::<_, ()>(()) },
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
            fingerprint,
            handle,
            _admission_permit: admission_permit,
        },
    );
    let manager = AuthenticatedSessionManager {
        sessions: sessions.clone(),
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
fn authenticated_config_omits_scope_and_canonicalizes_root_oauth_identifiers() {
    let config = AuthenticatedMcpHttpConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "https://mcp.example.com/",
        "https://login.example.com/",
    )
    .unwrap();

    assert_eq!(config.public_url, "https://mcp.example.com");
    assert_eq!(config.authorization_server, "https://login.example.com");
    assert_eq!(
        config.challenge,
        HeaderValue::from_static(
            "Bearer resource_metadata=\"https://mcp.example.com/.well-known/oauth-protected-resource\""
        )
    );
}

#[tokio::test]
async fn authenticated_discovery_and_challenge_do_not_advertise_scopes() {
    let config = authenticated_config();
    let metadata_path = config.metadata_path.clone();
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_| async { Ok::<_, ()>(()) },
        |_| std::future::ready(Err::<AppClient, ()>(())),
        workspace_named_options(),
        || async { Ok::<_, tonic::Code>(()) },
    );
    let (router, _state) = authenticated_router(config, runtime);

    let unauthorized = send(&router, raw_mcp_request(INITIALIZE)).await;
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    let challenge = unauthorized
        .headers()
        .get(header::WWW_AUTHENTICATE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(challenge.contains("resource_metadata="));
    assert!(!challenge.contains("scope="));

    let metadata = send(
        &router,
        Request::builder()
            .method(Method::GET)
            .uri(metadata_path)
            .header(header::HOST, "mcp.example.com")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(metadata.status(), StatusCode::OK);
    let body = to_bytes(metadata.into_body(), usize::MAX).await.unwrap();
    let document: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        document
            .get("resource")
            .expect("protected-resource metadata must contain resource"),
        &serde_json::json!("https://mcp.example.com/custom%20mcp")
    );
    assert_eq!(
        document
            .get("authorization_servers")
            .expect("protected-resource metadata must contain authorization_servers"),
        &serde_json::json!(["https://login.example.com"])
    );
    assert!(document.get("scopes_supported").is_none());
}

#[tokio::test]
async fn dropping_authenticated_server_closes_sessions_and_releases_state() {
    let (_temp, app_server, app) = local_app().await;
    // Nothing provisions a workspace any more, so the fixture creates the one
    // its session serves and scopes the options to it.
    let options = workspace_scoped_options(&app).await;
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_| async { Ok::<_, ()>(()) },
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
            "http://{}/mcp",
            server.local_addr()
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
        "the session serves the workspace its options name"
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
    // Nothing provisions a workspace any more, so the fixture creates the one
    // the sessions serve and scopes the options to it.
    let options = workspace_scoped_options(&live_app).await;
    stopped_server
        .shutdown()
        .await
        .expect("stop second app server");

    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_| async { Ok::<_, ()>(()) },
        move |token| {
            let app = if token == "token-a" {
                live_app.clone()
            } else {
                stopped_app.clone()
            };
            std::future::ready(Ok::<_, ()>(app))
        },
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
    .await
    .expect("start authenticated MCP HTTP server");
    let endpoint = format!("http://{}/mcp", server.local_addr());

    let live_client = ()
        .serve(StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(endpoint.clone()).auth_header("token-a"),
        ))
        .await
        .expect("initialize live MCP session");
    let stopped_client = ()
        .serve(StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(endpoint).auth_header("token-b"),
        ))
        .await
        .expect("initialize stopped MCP session");

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
    stopped_client
        .call_tool(CallToolRequestParams::new("extension_workspace"))
        .await
        .expect_err("stopped session must use its unavailable app client");

    live_client.cancel().await.expect("cancel live MCP client");
    stopped_client
        .cancel()
        .await
        .expect("cancel stopped MCP client");
    server.shutdown().await.expect("shutdown MCP HTTP server");
    live_server
        .shutdown()
        .await
        .expect("shutdown live app server");
}
