use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use axum::{Router, response::Response};
use coral_client::{AppClient, local::ServerBuilder};
use coral_mcp::McpOptions;
use rmcp::ServiceExt as _;
use rmcp::model::CallToolRequestParams;
use rmcp::transport::StreamableHttpClientTransport;
use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tower::ServiceExt as _;

use super::{
    AuthenticatedMcpHttpConfig, AuthenticatedMcpHttpRuntime, McpHttpConfig, McpHttpError,
    ReadinessProbe, RunningMcpHttpServer, SESSION_ID_HEADER, SHUTDOWN_GRACE_PERIOD,
    SessionBindingFingerprint, SessionBindingStatus, SessionBindingStore, SessionBindingStoreError,
    auth_disabled_router, authenticated_router, readiness_status, start_auth_disabled,
};

const INITIALIZE: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#;
const PING: &str = r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#;

fn authenticated_config() -> AuthenticatedMcpHttpConfig {
    AuthenticatedMcpHttpConfig::new(
        "0.0.0.0:0".parse().unwrap(),
        "https://mcp.example.com/custom%20mcp",
        "https://login.example.com/",
        "coral:mcp",
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

    let initialize = json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-03-26", "capabilities": {},
            "clientInfo": {"name": "raw-test", "version": "1"}}
    });
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/mcp")
                .header(header::HOST, "attacker.example")
                .header(header::ACCEPT, "application/json, text/event-stream")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(initialize.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = router
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/mcp")
                .header(header::HOST, "127.42.3.9:8080")
                .header(header::ACCEPT, "application/json, text/event-stream")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(initialize.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    state.config.cancellation_token.cancel();
    super::close_sessions(state.sessions.as_ref()).await;
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

    let catalog = client
        .call_tool(CallToolRequestParams::new("list_catalog"))
        .await
        .expect("call list_catalog");
    assert_eq!(catalog.is_error, Some(false));

    app_server.shutdown().await.expect("shutdown app server");
    let rejected = client
        .call_tool(CallToolRequestParams::new("list_catalog"))
        .await
        .expect("gRPC rejection is an in-band tool result");
    assert_eq!(rejected.is_error, Some(true));

    let sessions = Arc::downgrade(&server.state.sessions);
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
    let requests = server.state.requests.clone();
    let request = requests.read().await;

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        server.shutdown_with_grace_period(Duration::ZERO),
    )
    .await
    .expect("zero-grace shutdown must be bounded");
    assert!(matches!(result, Err(McpHttpError::ShutdownTimedOut)));

    drop(request);
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

    let sessions = Arc::downgrade(&server.state.sessions);
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
async fn authenticated_sessions_do_not_leak_after_invalid_or_deleted_requests() {
    let config = authenticated_config();
    let (_temp, app_server, app) = local_app().await;
    let factory = coral_mcp::CoralMcpServerFactory::new(app, McpOptions::default());
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let counted_factory_calls = factory_calls.clone();
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_| async { Ok::<_, ()>(()) },
        move |_| {
            counted_factory_calls.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok::<_, ()>(factory.clone()))
        },
        || async { Ok::<_, tonic::Code>(()) },
    );
    let (router, state) = authenticated_router(config, runtime);
    let _invalid = send(&router, auth_request("Bearer token-a", None, "{}")).await;
    let unsupported = INITIALIZE.replace("2025-03-26", "2099-01-01");
    let _unsupported = send(&router, auth_request("Bearer token-a", None, &unsupported)).await;
    for protocols in [&["2025-06-18"][..], &["2025-03-26", "2025-06-18"][..]] {
        let mut invalid = auth_request("Bearer token-a", None, INITIALIZE);
        for protocol in protocols {
            invalid
                .headers_mut()
                .append("mcp-protocol-version", protocol.parse().unwrap());
        }
        let _rejected = send(&router, invalid).await;
    }
    assert!(state.sessions.sessions.read().await.is_empty());
    assert_eq!(factory_calls.load(Ordering::Relaxed), 0);
    let response = send(&router, auth_request("Bearer token-a", None, INITIALIZE)).await;
    let session = response.headers()[SESSION_ID_HEADER]
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(factory_calls.load(Ordering::Relaxed), 1);
    for invalid in [&b"bad id"[..], &b"bad\tid"[..], &b"bad\x80id"[..]] {
        let invalid = axum::http::HeaderValue::from_bytes(invalid).unwrap();
        let mut request = auth_request("Bearer token-a", None, "{}");
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
    let mut ping = auth_request("Bearer token-a", Some(&session), PING);
    ping.headers_mut()
        .insert("mcp-protocol-version", "2025-03-26".parse().unwrap());
    assert!(send(&router, ping).await.status().is_success());
    assert_eq!(factory_calls.load(Ordering::Relaxed), 1);
    let mut delete = auth_request("Bearer token-a", Some(&session), "");
    *delete.method_mut() = Method::DELETE;
    assert!(send(&router, delete).await.status().is_success());
    let missing = send(
        &router,
        auth_request("Bearer token-a", Some(&session), "{}"),
    )
    .await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    app_server.shutdown().await.unwrap();
}

struct FailingSessionBindingStore {
    remove_calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl SessionBindingStore for FailingSessionBindingStore {
    async fn bind(
        &self,
        _session_id: &str,
        _fingerprint: SessionBindingFingerprint,
    ) -> Result<(), SessionBindingStoreError> {
        Err(io::Error::other("binding store unavailable").into())
    }

    async fn authorize_and_touch(
        &self,
        _session_id: &str,
        _fingerprint: &SessionBindingFingerprint,
    ) -> Result<SessionBindingStatus, SessionBindingStoreError> {
        Ok(SessionBindingStatus::Missing)
    }

    async fn remove(&self, _session_id: &str) -> Result<(), SessionBindingStoreError> {
        self.remove_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn binding_store_failure_closes_new_session() {
    let (_temp, app_server, app) = local_app().await;
    let factory = coral_mcp::CoralMcpServerFactory::new(app, McpOptions::default());
    let remove_calls = Arc::new(AtomicUsize::new(0));
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_| async { Ok::<_, ()>(()) },
        move |_| std::future::ready(Ok::<_, ()>(factory.clone())),
        || async { Ok::<_, tonic::Code>(()) },
    )
    .with_session_binding_store(Arc::new(FailingSessionBindingStore {
        remove_calls: remove_calls.clone(),
    }));
    let (router, state) = authenticated_router(authenticated_config(), runtime);

    let response = send(&router, auth_request("Bearer token-a", None, INITIALIZE)).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(state.sessions.sessions.read().await.is_empty());
    assert_eq!(remove_calls.load(Ordering::Relaxed), 1);
    app_server.shutdown().await.unwrap();
}

struct MissingSessionBindingStore;

#[async_trait::async_trait]
impl SessionBindingStore for MissingSessionBindingStore {
    async fn bind(
        &self,
        _session_id: &str,
        _fingerprint: SessionBindingFingerprint,
    ) -> Result<(), SessionBindingStoreError> {
        Ok(())
    }

    async fn authorize_and_touch(
        &self,
        _session_id: &str,
        _fingerprint: &SessionBindingFingerprint,
    ) -> Result<SessionBindingStatus, SessionBindingStoreError> {
        Ok(SessionBindingStatus::Missing)
    }

    async fn remove(&self, _session_id: &str) -> Result<(), SessionBindingStoreError> {
        Ok(())
    }
}

#[tokio::test]
async fn missing_binding_closes_local_session() {
    let (_temp, app_server, app) = local_app().await;
    let factory = coral_mcp::CoralMcpServerFactory::new(app, McpOptions::default());
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_| async { Ok::<_, ()>(()) },
        move |_| std::future::ready(Ok::<_, ()>(factory.clone())),
        || async { Ok::<_, tonic::Code>(()) },
    )
    .with_session_binding_store(Arc::new(MissingSessionBindingStore));
    let (router, state) = authenticated_router(authenticated_config(), runtime);
    let initialized = send(&router, auth_request("Bearer token-a", None, INITIALIZE)).await;
    let session = initialized.headers()[SESSION_ID_HEADER]
        .to_str()
        .unwrap()
        .to_string();

    let response = send(
        &router,
        auth_request("Bearer token-a", Some(&session), PING),
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(state.sessions.sessions.read().await.is_empty());
    app_server.shutdown().await.unwrap();
}

struct AuthorizedSessionBindingStore {
    remove_calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl SessionBindingStore for AuthorizedSessionBindingStore {
    async fn bind(
        &self,
        _session_id: &str,
        _fingerprint: SessionBindingFingerprint,
    ) -> Result<(), SessionBindingStoreError> {
        Ok(())
    }

    async fn authorize_and_touch(
        &self,
        _session_id: &str,
        _fingerprint: &SessionBindingFingerprint,
    ) -> Result<SessionBindingStatus, SessionBindingStoreError> {
        Ok(SessionBindingStatus::Authorized)
    }

    async fn remove(&self, _session_id: &str) -> Result<(), SessionBindingStoreError> {
        self.remove_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn missing_rmcp_session_removes_stale_binding() {
    let (_temp, app_server, app) = local_app().await;
    let factory = coral_mcp::CoralMcpServerFactory::new(app, McpOptions::default());
    let remove_calls = Arc::new(AtomicUsize::new(0));
    let runtime = AuthenticatedMcpHttpRuntime::new(
        |_| async { Ok::<_, ()>(()) },
        move |_| std::future::ready(Ok::<_, ()>(factory.clone())),
        || async { Ok::<_, tonic::Code>(()) },
    )
    .with_session_binding_store(Arc::new(AuthorizedSessionBindingStore {
        remove_calls: remove_calls.clone(),
    }));
    let (router, _state) = authenticated_router(authenticated_config(), runtime);

    let response = send(
        &router,
        auth_request("Bearer token-a", Some("missing-session"), PING),
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(remove_calls.load(Ordering::Relaxed), 1);
    app_server.shutdown().await.unwrap();
}
