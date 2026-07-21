//! Streamable HTTP transport for Coral's MCP surface.
//!
//! [`start_auth_disabled`] is intentionally limited to loopback. It shares an
//! unauthenticated local [`coral_client::AppClient`] across sessions and is not
//! a safe construction path for a long-running, non-loopback server.

use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use coral_api::v1::{CatalogItemKind, ListCatalogRequest, PaginationRequest};
use coral_client::{AppClient, default_workspace};
use coral_mcp::{CoralMcpServerFactory, McpOptions};
use rmcp::model::{ClientJsonRpcMessage, ClientRequest, ProtocolVersion};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService,
    session::{SessionManager, local::LocalSessionManager},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock, oneshot};
use tokio::task::JoinHandle;
use tonic::Request as GrpcRequest;
use tower::ServiceExt;
use url::{Host, Position, Url};

const READINESS_TIMEOUT: Duration = Duration::from_secs(2);
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(1);
const SESSION_ID_HEADER: &str = "mcp-session-id";
const METADATA_ROOT: &str = "/.well-known/oauth-protected-resource";
const METADATA_ROUTE: &str = "/.well-known/oauth-protected-resource/{*resource_path}";
const MAX_BOUND_SESSIONS: usize = 4096;
const BOUND_SESSION_IDLE_TIMEOUT: Duration = Duration::from_hours(1);

type ProbeFuture = Pin<Box<dyn Future<Output = Result<(), tonic::Code>> + Send>>;
type Fut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type TokenValidator = Arc<dyn Fn(String) -> Fut<Result<(), ()>> + Send + Sync>;
type SessionFactory = Arc<dyn Fn(String) -> Fut<Result<CoralMcpServerFactory, ()>> + Send + Sync>;
type AuthState = Arc<AuthenticatedHttpState>;

/// Error returned by an authenticated MCP session-binding store.
pub type SessionBindingStoreError = Box<dyn Error + Send + Sync + 'static>;

/// Bearer-token fingerprint used to bind an MCP session to its authorization context.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SessionBindingFingerprint([u8; 32]);

impl SessionBindingFingerprint {
    /// Returns the fingerprint bytes for storage.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Result of authorizing a request against a stored session binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SessionBindingStatus {
    /// The supplied authorization context owns the session.
    Authorized,
    /// No active binding exists for the session.
    Missing,
    /// The session belongs to a different authorization context.
    Mismatch,
}

/// Storage seam for binding MCP session IDs to authorization contexts.
///
/// Implementations must atomically compare and refresh a binding in
/// [`SessionBindingStore::authorize_and_touch`]. Live MCP handlers remain
/// process-local and outside this store.
#[async_trait]
pub trait SessionBindingStore: Send + Sync + 'static {
    /// Creates or replaces a session binding.
    async fn bind(
        &self,
        session_id: &str,
        fingerprint: SessionBindingFingerprint,
    ) -> Result<(), SessionBindingStoreError>;

    /// Atomically authorizes and refreshes a session binding.
    async fn authorize_and_touch(
        &self,
        session_id: &str,
        fingerprint: &SessionBindingFingerprint,
    ) -> Result<SessionBindingStatus, SessionBindingStoreError>;

    /// Removes a session binding.
    async fn remove(&self, session_id: &str) -> Result<(), SessionBindingStoreError>;
}

#[derive(Default)]
struct InMemorySessionBindingStore {
    bindings: Mutex<HashMap<String, InMemorySessionBinding>>,
}

struct InMemorySessionBinding {
    fingerprint: SessionBindingFingerprint,
    last_seen: Instant,
}

/// Configuration for the auth-disabled loopback MCP HTTP server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct McpHttpConfig {
    bind_addr: SocketAddr,
}

impl McpHttpConfig {
    /// Creates loopback-only server configuration.
    ///
    /// # Errors
    ///
    /// Returns [`McpHttpError::NonLoopbackBind`] for any non-loopback address.
    pub fn new(bind_addr: SocketAddr) -> Result<Self, McpHttpError> {
        if !is_loopback(bind_addr.ip()) {
            return Err(McpHttpError::NonLoopbackBind(bind_addr));
        }
        Ok(Self { bind_addr })
    }

    /// Returns the configured bind address.
    #[must_use]
    pub fn bind_addr(self) -> SocketAddr {
        self.bind_addr
    }
}

/// Validated OAuth protected-resource configuration.
#[derive(Clone, Debug)]
pub struct AuthenticatedMcpHttpConfig {
    bind_addr: SocketAddr,
    resource_url: String,
    authorization_server: String,
    scope: String,
    metadata_path: String,
    challenge: HeaderValue,
    allowed_hosts: Vec<String>,
}

impl AuthenticatedMcpHttpConfig {
    /// # Errors
    /// Validates OAuth configuration, returning an error for unsafe URLs, scopes, or headers.
    pub fn new(
        bind_addr: SocketAddr,
        resource_url: impl Into<String>,
        authorization_server: impl Into<String>,
        scope: impl Into<String>,
    ) -> Result<Self, McpHttpError> {
        let resource = validated_oauth_url(&resource_url.into())?;
        let authorization_server = validated_oauth_url(&authorization_server.into())?;
        let scope = scope.into();
        let valid = scope.bytes().all(|b| matches!(b, 33 | 35..=91 | 93..=126));
        if scope.is_empty() || !valid {
            return Err(McpHttpError::InvalidAuthConfig("invalid OAuth scope"));
        }
        let (metadata_url, metadata_path) = protected_resource_metadata_url(&resource);
        let challenge = format!("Bearer resource_metadata=\"{metadata_url}\", scope=\"{scope}\"");
        let challenge = HeaderValue::from_str(&challenge)
            .map_err(|_error| McpHttpError::InvalidAuthConfig("invalid challenge header"))?;
        let mut allowed_hosts = vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
            "::1".to_string(),
            bind_addr.ip().to_string(),
        ];
        if let Some(host) = resource.host_str() {
            allowed_hosts.push(host.to_string());
        }
        if allowed_hosts
            .iter()
            .any(|host| HeaderValue::from_str(host).is_err())
        {
            return Err(McpHttpError::InvalidAuthConfig("invalid allowed Host"));
        }
        Ok(Self {
            bind_addr,
            resource_url: resource.to_string(),
            authorization_server: authorization_server.to_string(),
            scope,
            metadata_path,
            challenge,
            allowed_hosts,
        })
    }
}

/// Authenticated callbacks supplied by the composition root.
#[derive(Clone)]
pub struct AuthenticatedMcpHttpRuntime {
    validator: TokenValidator,
    session_factory: SessionFactory,
    readiness: ReadinessProbe,
    session_bindings: Arc<dyn SessionBindingStore>,
}

impl AuthenticatedMcpHttpRuntime {
    /// Creates a runtime whose factory MUST forward its bearer, never an unauthenticated client.
    pub fn new<V, VF, F, FF, R, RF>(validator: V, session_factory: F, readiness: R) -> Self
    where
        V: Fn(String) -> VF + Send + Sync + 'static,
        VF: Future<Output = Result<(), ()>> + Send + 'static,
        F: Fn(String) -> FF + Send + Sync + 'static,
        FF: Future<Output = Result<CoralMcpServerFactory, ()>> + Send + 'static,
        R: Fn() -> RF + Send + Sync + 'static,
        RF: Future<Output = Result<(), tonic::Code>> + Send + 'static,
    {
        Self {
            validator: Arc::new(move |token| Box::pin(validator(token))),
            session_factory: Arc::new(move |token| Box::pin(session_factory(token))),
            readiness: ReadinessProbe(Arc::new(move || Box::pin(readiness()))),
            session_bindings: Arc::new(InMemorySessionBindingStore::default()),
        }
    }

    /// Configures storage for binding MCP sessions to authorization contexts.
    ///
    /// This store protects the authorization association for each MCP session.
    /// Live MCP handlers remain process-local, so deployments must use
    /// process-sticky routing.
    #[must_use]
    pub fn with_session_binding_store(mut self, store: Arc<dyn SessionBindingStore>) -> Self {
        self.session_bindings = store;
        self
    }
}

fn validated_oauth_url(value: &str) -> Result<Url, McpHttpError> {
    let url =
        Url::parse(value).map_err(|_error| McpHttpError::InvalidAuthConfig("invalid OAuth URL"))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.query().is_some()
    {
        return Err(McpHttpError::InvalidAuthConfig("unsafe OAuth URL"));
    }
    let loopback = match url.host() {
        Some(Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => is_loopback(IpAddr::V6(ip)),
        None => false,
    };
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err(McpHttpError::InvalidAuthConfig(
            "OAuth URL must use HTTPS or loopback HTTP",
        ));
    }
    Ok(url)
}

fn protected_resource_metadata_url(resource: &Url) -> (String, String) {
    let resource_path = match resource.path() {
        "/" => "",
        path => path,
    };
    let path = format!("{METADATA_ROOT}{resource_path}");
    (format!("{}{path}", &resource[..Position::BeforePath]), path)
}

fn is_loopback(ip: IpAddr) -> bool {
    ip.is_loopback()
        || matches!(ip, IpAddr::V6(ip) if ip.to_ipv4_mapped().is_some_and(|ip| ip.is_loopback()))
}

/// MCP HTTP startup or shutdown failure.
#[derive(Debug, thiserror::Error)]
pub enum McpHttpError {
    /// Auth-disabled serving is restricted to the local machine.
    #[error("auth-disabled MCP HTTP bind must be loopback, got {0}")]
    NonLoopbackBind(SocketAddr),
    /// Authenticated serving configuration is invalid.
    #[error("invalid authenticated MCP HTTP configuration: {0}")]
    InvalidAuthConfig(&'static str),
    /// The TCP listener could not bind.
    #[error("failed to bind MCP HTTP server to {address}")]
    Bind {
        /// Requested bind address.
        address: SocketAddr,
        /// Listener error.
        #[source]
        source: io::Error,
    },
    /// The HTTP server exited with an I/O error.
    #[error("MCP HTTP server failed")]
    Server(#[source] io::Error),
    /// The HTTP server task could not be joined.
    #[error("MCP HTTP server task failed")]
    Join(#[source] tokio::task::JoinError),
    /// Coordinated request and connection draining exceeded its deadline.
    #[error("MCP HTTP server shutdown timed out")]
    ShutdownTimedOut,
}

/// Handle for a running MCP HTTP server.
///
/// Call [`RunningMcpHttpServer::shutdown`] for deterministic teardown. Dropping
/// this handle cancels active HTTP work and closes sessions asynchronously.
pub struct RunningMcpHttpServer {
    local_addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), io::Error>>,
    state: Arc<ServerState>,
}

impl RunningMcpHttpServer {
    /// Returns the listener address, including an OS-assigned port.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    fn begin_shutdown(&mut self) {
        self.state.config.cancellation_token.cancel();
        if let Some(shutdown) = self.shutdown.take() {
            let _send_result = shutdown.send(());
        }
    }

    /// Stops accepting requests, terminates MCP sessions, and joins the server.
    ///
    /// # Errors
    ///
    /// Returns [`McpHttpError`] if the HTTP task fails or coordinated draining
    /// cannot complete within one second.
    pub async fn shutdown(self) -> Result<(), McpHttpError> {
        self.shutdown_with_grace_period(SHUTDOWN_GRACE_PERIOD).await
    }

    async fn shutdown_with_grace_period(
        mut self,
        grace_period: Duration,
    ) -> Result<(), McpHttpError> {
        self.begin_shutdown();
        let state = self.state.clone();
        let drain = async {
            let quiescence = state.requests.write().await;
            close_sessions(state.sessions.as_ref()).await;
            drop(quiescence);
            (&mut self.task).await
        };
        if let Ok(result) = tokio::time::timeout(grace_period, drain).await {
            return join_server(result);
        }
        self.task.abort();
        let _join_result = (&mut self.task).await;
        Err(McpHttpError::ShutdownTimedOut)
    }
}

impl Drop for RunningMcpHttpServer {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

fn join_server(
    result: Result<Result<(), io::Error>, tokio::task::JoinError>,
) -> Result<(), McpHttpError> {
    result
        .map_err(McpHttpError::Join)?
        .map_err(McpHttpError::Server)
}

async fn close_sessions(sessions: &LocalSessionManager) {
    let ids: Vec<_> = sessions.sessions.read().await.keys().cloned().collect();
    for id in ids {
        let _close_result = sessions.close_session(&id).await;
    }
}

/// Starts the loopback-only, authentication-disabled MCP HTTP server.
///
/// The supplied unauthenticated client is shared across independent MCP
/// sessions. Auth-required serving must use a distinct construction path that
/// creates a bearer-bound client after validating each incoming session.
///
/// # Errors
///
/// Returns [`McpHttpError`] if the listener cannot bind.
pub async fn start_auth_disabled(
    config: McpHttpConfig,
    app: AppClient,
    options: McpOptions,
) -> Result<RunningMcpHttpServer, McpHttpError> {
    let listener = TcpListener::bind(config.bind_addr())
        .await
        .map_err(|source| McpHttpError::Bind {
            address: config.bind_addr(),
            source,
        })?;
    let local_addr = listener.local_addr().map_err(McpHttpError::Server)?;
    let readiness = ReadinessProbe::from_app(app.clone());
    let (router, state) = auth_disabled_router(app, options, readiness, local_addr.ip());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task_state = state.clone();
    let task = tokio::spawn(async move {
        let result = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _shutdown_result = shutdown_rx.await;
            })
            .await;
        close_sessions(task_state.sessions.as_ref()).await;
        result
    });
    Ok(RunningMcpHttpServer {
        local_addr,
        shutdown: Some(shutdown_tx),
        task,
        state,
    })
}

/// Starts authenticated serving; fails when the listener cannot bind.
/// # Errors
/// Returns an error when the listener cannot bind.
pub async fn start_authenticated(
    config: AuthenticatedMcpHttpConfig,
    runtime: AuthenticatedMcpHttpRuntime,
) -> Result<RunningMcpHttpServer, McpHttpError> {
    let listener = TcpListener::bind(config.bind_addr)
        .await
        .map_err(|source| McpHttpError::Bind {
            address: config.bind_addr,
            source,
        })?;
    let local_addr = listener.local_addr().map_err(McpHttpError::Server)?;
    let (router, state) = authenticated_router(config, runtime);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _shutdown_result = shutdown_rx.await;
            })
            .await
    });
    Ok(RunningMcpHttpServer {
        local_addr,
        shutdown: Some(shutdown_tx),
        task,
        state,
    })
}

#[derive(Clone)]
struct ReadinessProbe(Arc<dyn Fn() -> ProbeFuture + Send + Sync>);

impl ReadinessProbe {
    fn from_app(app: AppClient) -> Self {
        Self(Arc::new(move || {
            let mut catalog = app.catalog_client();
            Box::pin(async move {
                catalog
                    .list_catalog(GrpcRequest::new(ListCatalogRequest {
                        workspace: Some(default_workspace()),
                        schema_name: String::new(),
                        kind: CatalogItemKind::Unspecified as i32,
                        pagination: Some(PaginationRequest {
                            limit: 1,
                            offset: 0,
                        }),
                    }))
                    .await
                    .map(|_response| ())
                    .map_err(|status| status.code())
            })
        }))
    }
}

fn auth_disabled_router(
    app: AppClient,
    options: McpOptions,
    readiness: ReadinessProbe,
    advertised_ip: IpAddr,
) -> (Router, Arc<ServerState>) {
    let factory = CoralMcpServerFactory::new(app, options);
    let config =
        StreamableHttpServerConfig::default().with_allowed_hosts([advertised_ip.to_string()]);
    let sessions = Arc::new(LocalSessionManager::default());
    let server = Arc::new(ServerState {
        sessions,
        config,
        requests: Arc::new(RwLock::new(())),
    });
    let state = Arc::new(HttpState {
        factory,
        readiness,
        server: server.clone(),
    });
    let router = Router::new()
        .route("/mcp", any(mcp))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .with_state(state.clone());
    (router, server)
}

#[derive(Clone)]
struct HttpState {
    factory: CoralMcpServerFactory,
    readiness: ReadinessProbe,
    server: Arc<ServerState>,
}

struct ServerState {
    sessions: Arc<LocalSessionManager>,
    config: StreamableHttpServerConfig,
    requests: Arc<RwLock<()>>,
}

async fn mcp(State(state): State<Arc<HttpState>>, request: Request<Body>) -> Response {
    let _request = state.server.requests.read().await;
    serve_mcp_request(
        request,
        Some(state.factory.clone()),
        state.server.sessions.clone(),
        state.server.config.clone(),
    )
    .await
}

fn authenticated_router(
    config: AuthenticatedMcpHttpConfig,
    runtime: AuthenticatedMcpHttpRuntime,
) -> (Router, Arc<ServerState>) {
    let streamable_config =
        StreamableHttpServerConfig::default().with_allowed_hosts(config.allowed_hosts.clone());
    let server = Arc::new(ServerState {
        sessions: Arc::new(LocalSessionManager::default()),
        config: streamable_config,
        requests: Arc::new(RwLock::new(())),
    });
    let state = Arc::new(AuthenticatedHttpState {
        config,
        runtime,
        server: server.clone(),
    });
    let router = Router::new()
        .route("/mcp", any(authenticated_mcp))
        .route("/livez", get(livez))
        .route("/readyz", get(authenticated_readyz))
        .route(METADATA_ROOT, get(metadata))
        .route(METADATA_ROUTE, get(metadata))
        .with_state(state.clone());
    (router, server)
}

struct AuthenticatedHttpState {
    config: AuthenticatedMcpHttpConfig,
    runtime: AuthenticatedMcpHttpRuntime,
    server: Arc<ServerState>,
}

async fn authenticated_mcp(State(state): State<AuthState>, request: Request<Body>) -> Response {
    let _request = state.server.requests.read().await;
    if request.method() == Method::OPTIONS {
        return StatusCode::NO_CONTENT.into_response();
    }
    let Some(token) = bearer_token(request.headers()) else {
        return unauthorized_response(&state.config);
    };
    let validation = tokio::select! {
        biased;
        () = state.server.config.cancellation_token.cancelled() => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        validation = (state.runtime.validator)(token.clone()) => validation,
    };
    if validation.is_err() {
        return unauthorized_response(&state.config);
    }
    let binding_fingerprint = binding_fingerprint(&token);
    let request_session = match session_id(request.headers()) {
        Ok(session) => session.map(str::to_string),
        Err(()) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let closes_session = request.method() == Method::DELETE;
    let request = if request_session.is_none() {
        tokio::select! {
            biased;
            () = state.server.config.cancellation_token.cancelled() => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
            request = initialize_request(request) => match request {
                Ok(request) => request,
                Err(response) => return response,
            },
        }
    } else {
        request
    };
    let factory = if let Some(session_id) = request_session.as_deref() {
        if let Err(status) = authorize_bound_session(&state, session_id, &binding_fingerprint).await
        {
            return status.into_response();
        }
        None
    } else {
        match create_session_factory(&state, token).await {
            Ok(factory) => Some(factory),
            Err(status) => return status.into_response(),
        }
    };
    let response = serve_mcp_request(
        request,
        factory,
        state.server.sessions.clone(),
        state.server.config.clone(),
    )
    .await;
    finalize_authenticated_response(
        &state,
        request_session.as_deref(),
        binding_fingerprint,
        closes_session,
        response,
    )
    .await
}

async fn finalize_authenticated_response(
    state: &AuthenticatedHttpState,
    request_session: Option<&str>,
    binding_fingerprint: SessionBindingFingerprint,
    closes_session: bool,
    response: Response,
) -> Response {
    if request_session.is_none()
        && response.status().is_success()
        && let Ok(Some(session_id)) = session_id(response.headers())
    {
        if let Err(_error) = state
            .runtime
            .session_bindings
            .bind(session_id, binding_fingerprint)
            .await
        {
            remove_managed_session(state.server.as_ref(), session_id).await;
            let _remove_result = state.runtime.session_bindings.remove(session_id).await;
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    } else if (closes_session && response.status().is_success()
        || response.status() == StatusCode::NOT_FOUND)
        && let Some(session_id) = request_session
        && state
            .runtime
            .session_bindings
            .remove(session_id)
            .await
            .is_err()
    {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    response
}

async fn create_session_factory(
    state: &AuthenticatedHttpState,
    token: String,
) -> Result<CoralMcpServerFactory, StatusCode> {
    tokio::select! {
        biased;
        () = state.server.config.cancellation_token.cancelled() => Err(StatusCode::SERVICE_UNAVAILABLE),
        factory = (state.runtime.session_factory)(token) => factory.map_err(|()| StatusCode::SERVICE_UNAVAILABLE),
    }
}

async fn authorize_bound_session(
    state: &AuthenticatedHttpState,
    session_id: &str,
    fingerprint: &SessionBindingFingerprint,
) -> Result<(), StatusCode> {
    let status = state
        .runtime
        .session_bindings
        .authorize_and_touch(session_id, fingerprint)
        .await
        .map_err(|_error| StatusCode::SERVICE_UNAVAILABLE)?;
    if status == SessionBindingStatus::Missing {
        remove_managed_session(state.server.as_ref(), session_id).await;
    }
    match status {
        SessionBindingStatus::Authorized => Ok(()),
        SessionBindingStatus::Missing => Err(StatusCode::NOT_FOUND),
        SessionBindingStatus::Mismatch => Err(StatusCode::FORBIDDEN),
    }
}

async fn initialize_request(request: Request<Body>) -> Result<Request<Body>, Response> {
    let (parts, body) = request.into_parts();
    let mut protocols = parts.headers.get_all("mcp-protocol-version").iter();
    let protocol = protocols.next();
    let versions = ProtocolVersion::KNOWN_VERSIONS;
    if protocols.next().is_some()
        || protocol.is_some_and(|header| !versions.iter().any(|v| header == v.as_str()))
    {
        return Err(StatusCode::BAD_REQUEST.into_response());
    }
    let bytes = axum::body::to_bytes(body, 1_048_576)
        .await
        .map_err(|_error| StatusCode::PAYLOAD_TOO_LARGE.into_response())?;
    let Ok(ClientJsonRpcMessage::Request(request)) =
        serde_json::from_slice::<ClientJsonRpcMessage>(&bytes)
    else {
        return Err(StatusCode::UNPROCESSABLE_ENTITY.into_response());
    };
    let ClientRequest::InitializeRequest(initialize) = request.request else {
        return Err(StatusCode::UNPROCESSABLE_ENTITY.into_response());
    };
    let body_protocol = initialize.params.protocol_version.as_str();
    let supported = versions.iter().any(|v| v.as_str() == body_protocol);
    if !supported || protocol.is_some_and(|header| header != body_protocol) {
        return Err(StatusCode::BAD_REQUEST.into_response());
    }
    Ok(Request::from_parts(parts, Body::from(bytes)))
}

async fn remove_managed_session(server: &ServerState, id: &str) {
    let _close_result = server.sessions.close_session(&Arc::from(id)).await;
}

async fn authenticated_readyz(State(state): State<Arc<AuthenticatedHttpState>>) -> StatusCode {
    tokio::select! {
        biased;
        () = state.server.config.cancellation_token.cancelled() => StatusCode::SERVICE_UNAVAILABLE,
        status = readiness_status(&state.runtime.readiness, READINESS_TIMEOUT) => status,
    }
}

async fn metadata(State(state): State<AuthState>, uri: Uri) -> Response {
    if uri.path() != state.config.metadata_path {
        return StatusCode::NOT_FOUND.into_response();
    }
    (
        [(header::CONTENT_TYPE, "application/json")],
        json!({
            "resource": state.config.resource_url,
            "authorization_servers": [state.config.authorization_server],
            "scopes_supported": [state.config.scope],
            "bearer_methods_supported": ["header"],
        })
        .to_string(),
    )
        .into_response()
}

fn unauthorized_response(config: &AuthenticatedMcpHttpConfig) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, config.challenge.clone())],
    )
        .into_response()
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some() || value.trim() != value {
        return None;
    }
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Bearer")
        || token.is_empty()
        || token.contains(char::is_whitespace)
    {
        return None;
    }
    Some(token.to_string())
}

fn session_id(headers: &HeaderMap) -> Result<Option<&str>, ()> {
    let mut values = headers.get_all(SESSION_ID_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_error| ())?;
    let valid = !value.is_empty() && value.as_bytes().iter().all(u8::is_ascii_graphic);
    if values.next().is_some() || !valid {
        return Err(());
    }
    Ok(Some(value))
}

fn binding_fingerprint(token: &str) -> SessionBindingFingerprint {
    SessionBindingFingerprint(Sha256::digest(token.as_bytes()).into())
}

#[async_trait]
impl SessionBindingStore for InMemorySessionBindingStore {
    async fn bind(
        &self,
        session_id: &str,
        fingerprint: SessionBindingFingerprint,
    ) -> Result<(), SessionBindingStoreError> {
        let mut bindings = self.bindings.lock().await;
        insert_session_binding(&mut bindings, session_id, fingerprint, Instant::now());
        Ok(())
    }

    async fn authorize_and_touch(
        &self,
        session_id: &str,
        fingerprint: &SessionBindingFingerprint,
    ) -> Result<SessionBindingStatus, SessionBindingStoreError> {
        let mut bindings = self.bindings.lock().await;
        Ok(authorize_session_binding(
            &mut bindings,
            session_id,
            fingerprint,
            Instant::now(),
        ))
    }

    async fn remove(&self, session_id: &str) -> Result<(), SessionBindingStoreError> {
        self.bindings.lock().await.remove(session_id);
        Ok(())
    }
}

fn insert_session_binding(
    bindings: &mut HashMap<String, InMemorySessionBinding>,
    session_id: &str,
    fingerprint: SessionBindingFingerprint,
    now: Instant,
) {
    prune_expired_session_bindings(bindings, now);
    if !bindings.contains_key(session_id)
        && bindings.len() >= MAX_BOUND_SESSIONS
        && let Some(oldest) = bindings
            .iter()
            .min_by_key(|(_, binding)| binding.last_seen)
            .map(|(session_id, _)| session_id.clone())
    {
        bindings.remove(&oldest);
    }
    let binding = InMemorySessionBinding {
        fingerprint,
        last_seen: now,
    };
    bindings.insert(session_id.to_string(), binding);
}

fn authorize_session_binding(
    bindings: &mut HashMap<String, InMemorySessionBinding>,
    session_id: &str,
    fingerprint: &SessionBindingFingerprint,
    now: Instant,
) -> SessionBindingStatus {
    let Some(binding) = bindings.get(session_id) else {
        return SessionBindingStatus::Missing;
    };
    if now.saturating_duration_since(binding.last_seen) > BOUND_SESSION_IDLE_TIMEOUT {
        bindings.remove(session_id);
        return SessionBindingStatus::Missing;
    }
    if binding.fingerprint != *fingerprint {
        return SessionBindingStatus::Mismatch;
    }
    if let Some(binding) = bindings.get_mut(session_id) {
        binding.last_seen = now;
    }
    SessionBindingStatus::Authorized
}

fn prune_expired_session_bindings(
    bindings: &mut HashMap<String, InMemorySessionBinding>,
    now: Instant,
) {
    bindings.retain(|_session_id, binding| {
        now.saturating_duration_since(binding.last_seen) <= BOUND_SESSION_IDLE_TIMEOUT
    });
}

// The service wrapper is intentionally request-scoped. Auth-required routing
// can validate an initialize request and pass its bearer-bound factory here,
// while later requests reuse the handler already held by `sessions`.
async fn serve_mcp_request(
    request: Request<Body>,
    factory: Option<CoralMcpServerFactory>,
    sessions: Arc<LocalSessionManager>,
    config: StreamableHttpServerConfig,
) -> Response {
    let cancellation = config.cancellation_token.clone();
    let service = StreamableHttpService::new(
        move || {
            factory
                .as_ref()
                .map(CoralMcpServerFactory::create)
                .ok_or_else(|| io::Error::other("MCP session handler factory unavailable"))
        },
        sessions,
        config,
    );
    tokio::select! {
        biased;
        () = cancellation.cancelled() => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        result = service.oneshot(request) => match result {
            Ok(response) => response.map(Body::new),
            Err(never) => match never {},
        },
    }
}

async fn livez() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn readyz(State(state): State<Arc<HttpState>>) -> StatusCode {
    tokio::select! {
        biased;
        () = state.server.config.cancellation_token.cancelled() => StatusCode::SERVICE_UNAVAILABLE,
        status = readiness_status(&state.readiness, READINESS_TIMEOUT) => status,
    }
}

async fn readiness_status(probe: &ReadinessProbe, timeout: Duration) -> StatusCode {
    match tokio::time::timeout(timeout, (probe.0)()).await {
        Ok(Ok(())) => StatusCode::NO_CONTENT,
        Ok(Err(code)) if catalog_rejection_is_reachable(code) => StatusCode::NO_CONTENT,
        Ok(Err(_)) | Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

fn catalog_rejection_is_reachable(code: tonic::Code) -> bool {
    !matches!(
        code,
        tonic::Code::Cancelled
            | tonic::Code::Unknown
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Unimplemented
            | tonic::Code::Internal
            | tonic::Code::Unavailable
            | tonic::Code::DataLoss
    )
}

#[cfg(test)]
mod tests;
