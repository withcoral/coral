use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{HeaderValue, Method, Request, StatusCode, header};
use axum::{Router, response::Response};
use coral_client::{AppClient, local::ServerBuilder};
use futures::poll;
use rmcp::ServiceExt as _;
use rmcp::model::{CallToolRequestParams, ClientJsonRpcMessage};
use rmcp::transport::{
    StreamableHttpClientTransport,
    streamable_http_client::StreamableHttpClientTransportConfig,
    streamable_http_server::session::{
        SessionManager as _,
        local::{SessionConfig, create_local_session},
    },
};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tower::ServiceExt as _;

use crate::McpOptions;

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

#[tokio::test]
async fn raw_routes_enforce_health_and_host_contracts() {
    let (_temp, app_server, app) = local_app().await;
    let advertised_ip = IpAddr::V4(Ipv4Addr::new(127, 42, 3, 9));
    let (router, state) = auth_disabled_router(
        app.clone(),
        McpOptions::default(),
        ReadinessProbe::from_app(app),
        advertised_ip,
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
        McpOptions::default(),
        ReadinessProbe::from_app(app),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
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

#[tokio::test]
async fn streamable_http_executes_tools_and_shutdown_is_bounded() {
    let (_temp, app_server, app) = local_app().await;
    let config =
        McpHttpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("loopback config");
    let server = start_auth_disabled(
        config,
        app,
        McpOptions {
            feedback_enabled: true,
            ..McpOptions::default()
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
    let config =
        McpHttpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("loopback config");
    let server = start_auth_disabled(config, app, McpOptions::default())
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
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let counted_factory_calls = factory_calls.clone();
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_| async { Ok::<_, ()>(()) },
        move |_| {
            counted_factory_calls.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok::<_, ()>(app.clone()))
        },
        McpOptions::default(),
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
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let counted_factory_calls = factory_calls.clone();
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_| async { Ok::<_, ()>(()) },
        move |_| {
            counted_factory_calls.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok::<_, ()>(app.clone()))
        },
        McpOptions::default(),
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
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_| async { Ok::<_, ()>(()) },
        move |_| std::future::ready(Ok::<_, ()>(app.clone())),
        McpOptions::default(),
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
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let counted_factory_calls = factory_calls.clone();
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_| async { Ok::<_, ()>(()) },
        move |_| {
            counted_factory_calls.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok::<_, ()>(app.clone()))
        },
        McpOptions::default(),
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

#[tokio::test]
async fn authenticated_session_honors_the_declared_idle_timeout() {
    let (_temp, app_server, app) = local_app().await;
    tokio::time::pause();
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_| async { Ok::<_, ()>(()) },
        move |_| std::future::ready(Ok::<_, ()>(app.clone())),
        McpOptions::default(),
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
        McpOptions::default(),
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
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_| async { Ok::<_, ()>(()) },
        move |_| std::future::ready(Ok::<_, ()>(app.clone())),
        McpOptions::default(),
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
