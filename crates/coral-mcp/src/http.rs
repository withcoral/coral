//! Streamable HTTP transport for Coral's MCP surface.
//!
//! [`start_auth_disabled`] is intentionally limited to loopback. It shares an
//! unauthenticated local [`coral_client::AppClient`] across sessions and is not
//! a safe construction path for a long-running, non-loopback server.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use coral_api::v1::{CatalogItemKind, ListCatalogRequest, PaginationRequest};
use coral_app::{CanonicalOauthUrl, OauthUrlError};
use coral_client::{AppClient, default_workspace};
use futures::{Stream, StreamExt as _};
use rmcp::model::{ClientJsonRpcMessage, ClientRequest, ServerJsonRpcMessage};
use rmcp::transport::{
    WorkerTransport,
    common::{
        http_header::HEADER_MCP_PROTOCOL_VERSION, server_side_http::session_id as new_session_id,
    },
    streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService,
        session::{
            ServerSseMessage, SessionId, SessionManager,
            local::{
                EventIdParseError, LocalSessionHandle, LocalSessionManager,
                LocalSessionManagerError, LocalSessionWorker, SessionConfig, SessionError,
                create_local_session,
            },
        },
    },
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request as GrpcRequest;
use tower::ServiceExt;
use url::{Position, Url};

use crate::{CoralMcpServerFactory, McpOptions};

/// How long `/readyz` waits on the gRPC readiness probe before answering itself.
///
/// On the authenticated route this bounds the health RPC that coral-app already
/// bounds server-side with `READINESS_PROBE_TIMEOUT`, and it has to stay above
/// it. Were this the smaller of the two, `/readyz` would give up before the
/// server got to answer, reporting an engine unready on a deadline that says
/// nothing about it. Changing either constant means revisiting both.
const READINESS_TIMEOUT: Duration = Duration::from_secs(2);
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(1);
const SESSION_ID_HEADER: &str = "mcp-session-id";
const METADATA_ROOT: &str = "/.well-known/oauth-protected-resource";
const METADATA_ROUTE: &str = "/.well-known/oauth-protected-resource/{*resource_path}";
const MAX_MCP_REQUEST_BODY_SIZE: usize = 1_048_576;
const MAX_AUTHENTICATED_SESSIONS: usize = 4096;
const AUTHENTICATED_SESSION_IDLE_TIMEOUT: Duration = Duration::from_hours(1);

type ProbeFuture = Pin<Box<dyn Future<Output = Result<(), tonic::Code>> + Send>>;
type Fut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type TokenValidator = Arc<dyn Fn(String) -> Fut<Result<(), ()>> + Send + Sync>;
type SessionClientFactory = Arc<dyn Fn(String) -> Fut<Result<AppClient, ()>> + Send + Sync>;
type AuthState = Arc<AuthenticatedHttpState>;

#[derive(Clone, Copy, Eq, PartialEq)]
struct BearerFingerprint([u8; 32]);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionAuthorization {
    Authorized,
    Missing,
    Mismatch,
}

struct AuthenticatedSessions {
    records: RwLock<HashMap<SessionId, AuthenticatedSession>>,
    config: SessionConfig,
    admission: Arc<Semaphore>,
}

struct AuthenticatedSession {
    fingerprint: BearerFingerprint,
    handle: LocalSessionHandle,
    _admission_permit: OwnedSemaphorePermit,
}

struct AuthenticatedSessionManager {
    sessions: Arc<AuthenticatedSessions>,
    fingerprint: BearerFingerprint,
    admission_permit: Mutex<Option<OwnedSemaphorePermit>>,
}

#[derive(Debug, thiserror::Error)]
enum AuthenticatedSessionManagerError {
    #[error(transparent)]
    Local(#[from] LocalSessionManagerError),
    #[error(transparent)]
    Session(#[from] SessionError),
    #[error(transparent)]
    EventId(#[from] EventIdParseError),
    #[error("authenticated MCP session admission was not reserved")]
    AdmissionNotReserved,
}

impl AuthenticatedSessions {
    fn new(max_sessions: usize) -> Self {
        let mut config = SessionConfig::default();
        config.keep_alive = Some(AUTHENTICATED_SESSION_IDLE_TIMEOUT);
        Self {
            records: RwLock::new(HashMap::new()),
            config,
            admission: Arc::new(Semaphore::new(max_sessions)),
        }
    }

    fn try_admit(&self) -> Option<OwnedSemaphorePermit> {
        self.admission.clone().try_acquire_owned().ok()
    }

    async fn authorize(
        &self,
        session_id: &SessionId,
        fingerprint: &BearerFingerprint,
    ) -> SessionAuthorization {
        let records = self.records.read().await;
        let Some(session) = records.get(session_id) else {
            return SessionAuthorization::Missing;
        };
        if session.fingerprint == *fingerprint {
            SessionAuthorization::Authorized
        } else {
            SessionAuthorization::Mismatch
        }
    }

    async fn close_all(&self) {
        let sessions: Vec<_> = self
            .records
            .write()
            .await
            .drain()
            .map(|(_session_id, session)| session)
            .collect();
        for AuthenticatedSession {
            handle,
            _admission_permit,
            ..
        } in sessions
        {
            let _close_result = close_session_handle(handle).await;
        }
    }

    #[cfg(test)]
    async fn len(&self) -> usize {
        self.records.read().await.len()
    }

    #[cfg(test)]
    fn available_permits(&self) -> usize {
        self.admission.available_permits()
    }
}

impl Default for AuthenticatedSessions {
    fn default() -> Self {
        Self::new(MAX_AUTHENTICATED_SESSIONS)
    }
}

async fn close_session_handle(handle: LocalSessionHandle) -> Result<(), LocalSessionManagerError> {
    match handle.close().await {
        Ok(()) | Err(SessionError::SessionServiceTerminated) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

impl AuthenticatedSessionManager {
    async fn session_handle(
        &self,
        session_id: &SessionId,
    ) -> Result<LocalSessionHandle, AuthenticatedSessionManagerError> {
        self.sessions
            .records
            .read()
            .await
            .get(session_id)
            .filter(|session| session.fingerprint == self.fingerprint)
            .map(|session| session.handle.clone())
            .ok_or_else(|| LocalSessionManagerError::SessionNotFound(session_id.clone()).into())
    }
}

impl SessionManager for AuthenticatedSessionManager {
    type Error = AuthenticatedSessionManagerError;
    type Transport = WorkerTransport<LocalSessionWorker>;

    async fn create_session(&self) -> Result<(SessionId, Self::Transport), Self::Error> {
        let mut records = self.sessions.records.write().await;
        let admission_permit = self
            .admission_permit
            .try_lock()
            .map_err(|_error| AuthenticatedSessionManagerError::AdmissionNotReserved)?
            .take()
            .ok_or(AuthenticatedSessionManagerError::AdmissionNotReserved)?;
        let session_id = new_session_id();
        let (handle, worker) =
            create_local_session(session_id.clone(), self.sessions.config.clone());
        records.insert(
            session_id.clone(),
            AuthenticatedSession {
                fingerprint: self.fingerprint,
                handle,
                _admission_permit: admission_permit,
            },
        );
        drop(records);
        Ok((session_id, WorkerTransport::spawn(worker)))
    }

    async fn initialize_session(
        &self,
        session_id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<ServerJsonRpcMessage, Self::Error> {
        let handle = self.session_handle(session_id).await?;
        Ok(handle.initialize(message).await?)
    }

    async fn has_session(&self, session_id: &SessionId) -> Result<bool, Self::Error> {
        Ok(self
            .sessions
            .records
            .read()
            .await
            .get(session_id)
            .is_some_and(|session| session.fingerprint == self.fingerprint))
    }

    async fn close_session(&self, session_id: &SessionId) -> Result<(), Self::Error> {
        let session = {
            let mut records = self.sessions.records.write().await;
            match records.get(session_id) {
                None => None,
                Some(session) if session.fingerprint != self.fingerprint => {
                    return Err(
                        LocalSessionManagerError::SessionNotFound(session_id.clone()).into(),
                    );
                }
                Some(_) => records.remove(session_id),
            }
        };
        match session {
            Some(AuthenticatedSession {
                handle,
                _admission_permit,
                ..
            }) => Ok(close_session_handle(handle).await?),
            None => Ok(()),
        }
    }

    async fn create_stream(
        &self,
        session_id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        let handle = self.session_handle(session_id).await?;
        let receiver = handle.establish_request_wise_channel().await?;
        let request_id = receiver.http_request_id;
        handle.push_message(message, request_id).await?;
        let priming = self.sessions.config.sse_retry.map(|retry| {
            let event_id = request_id.map_or_else(|| "0".to_string(), |id| format!("0/{id}"));
            ServerSseMessage::priming(event_id, retry)
        });
        Ok(futures::stream::iter(priming).chain(ReceiverStream::new(receiver.inner)))
    }

    async fn accept_message(
        &self,
        session_id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<(), Self::Error> {
        self.session_handle(session_id)
            .await?
            .push_message(message, None)
            .await?;
        Ok(())
    }

    async fn create_standalone_stream(
        &self,
        session_id: &SessionId,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        let receiver = self
            .session_handle(session_id)
            .await?
            .establish_common_channel()
            .await?;
        Ok(ReceiverStream::new(receiver.inner))
    }

    async fn resume(
        &self,
        session_id: &SessionId,
        last_event_id: String,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        let receiver = self
            .session_handle(session_id)
            .await?
            .resume(last_event_id.parse()?)
            .await?;
        Ok(ReceiverStream::new(receiver.inner))
    }
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
    public_url: String,
    authorization_server: String,
    metadata_path: String,
    challenge: HeaderValue,
    allowed_hosts: Vec<String>,
}

impl AuthenticatedMcpHttpConfig {
    /// # Errors
    /// Validates OAuth configuration, returning an error for unsafe URLs or headers.
    pub fn new(
        bind_addr: SocketAddr,
        public_url: impl Into<String>,
        authorization_server: impl Into<String>,
    ) -> Result<Self, McpHttpError> {
        let resource = validated_oauth_url(&public_url.into())?;
        let authorization_server = validated_oauth_url(&authorization_server.into())?;
        let (metadata_url, metadata_path) = protected_resource_metadata_url(resource.url());
        let challenge = format!("Bearer resource_metadata=\"{metadata_url}\"");
        let challenge = HeaderValue::from_str(&challenge)
            .map_err(|_error| McpHttpError::InvalidAuthConfig("invalid challenge header"))?;
        let mut allowed_hosts = vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
            "::1".to_string(),
            bind_addr.ip().to_string(),
        ];
        if let Some(host) = resource.url().host_str() {
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
            public_url: resource.into_identifier(),
            authorization_server: authorization_server.into_identifier(),
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
    session_client_factory: SessionClientFactory,
    options: McpOptions,
    readiness: ReadinessProbe,
}

impl AuthenticatedMcpHttpRuntime {
    /// Creates a runtime whose client factory must forward the caller's bearer
    /// token on outbound requests.
    pub fn new<V, VF, F, FF, R, RF>(
        validator: V,
        session_client_factory: F,
        options: McpOptions,
        readiness: R,
    ) -> Self
    where
        V: Fn(String) -> VF + Send + Sync + 'static,
        VF: Future<Output = Result<(), ()>> + Send + 'static,
        F: Fn(String) -> FF + Send + Sync + 'static,
        FF: Future<Output = Result<AppClient, ()>> + Send + 'static,
        R: Fn() -> RF + Send + Sync + 'static,
        RF: Future<Output = Result<(), tonic::Code>> + Send + 'static,
    {
        Self {
            validator: Arc::new(move |token| Box::pin(validator(token))),
            session_client_factory: Arc::new(move |token| Box::pin(session_client_factory(token))),
            options,
            readiness: ReadinessProbe(Arc::new(move || Box::pin(readiness()))),
        }
    }
}

/// Validates an OAuth URL with the canonicalizer `coral-app` owns.
///
/// The advertised protected-resource identifier must match the audience minted
/// into access tokens byte for byte, so this crate must not canonicalize on its
/// own.
fn validated_oauth_url(value: &str) -> Result<CanonicalOauthUrl, McpHttpError> {
    CanonicalOauthUrl::parse(value).map_err(|error| {
        McpHttpError::InvalidAuthConfig(match error {
            OauthUrlError::Transport => "OAuth URL must use HTTPS or loopback HTTP",
            OauthUrlError::Query => "unsafe OAuth URL",
            OauthUrlError::Shape => "invalid OAuth URL",
        })
    })
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
            state.sessions.close_all().await;
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
    Ok(spawn_http_server(
        listener,
        local_addr,
        router,
        state.server.clone(),
    ))
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
    Ok(spawn_http_server(
        listener,
        local_addr,
        router,
        state.server.clone(),
    ))
}

fn spawn_http_server(
    listener: TcpListener,
    local_addr: SocketAddr,
    router: Router,
    state: Arc<ServerState>,
) -> RunningMcpHttpServer {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task_state = state.clone();
    let task = tokio::spawn(async move {
        let result = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _shutdown_result = shutdown_rx.await;
            })
            .await;
        task_state.sessions.close_all().await;
        result
    });
    RunningMcpHttpServer {
        local_addr,
        shutdown: Some(shutdown_tx),
        task,
        state,
    }
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
                        catalog_name: String::new(),
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
) -> (Router, Arc<HttpState>) {
    let factory = CoralMcpServerFactory::new(app, options);
    let config =
        StreamableHttpServerConfig::default().with_allowed_hosts([advertised_ip.to_string()]);
    let sessions = Arc::new(LocalSessionManager::default());
    let server = Arc::new(ServerState {
        sessions: SessionOwner::Local(sessions.clone()),
        config,
        requests: RwLock::new(()),
    });
    let state = Arc::new(HttpState {
        factory,
        readiness,
        sessions,
        server: server.clone(),
    });
    let router = Router::new()
        .route("/mcp", any(mcp))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .with_state(state.clone());
    (router, state)
}

struct HttpState {
    factory: CoralMcpServerFactory,
    readiness: ReadinessProbe,
    sessions: Arc<LocalSessionManager>,
    server: Arc<ServerState>,
}

enum SessionOwner {
    Local(Arc<LocalSessionManager>),
    Authenticated(Arc<AuthenticatedSessions>),
}

impl SessionOwner {
    async fn close_all(&self) {
        match self {
            Self::Local(sessions) => {
                let session_ids: Vec<_> = sessions.sessions.read().await.keys().cloned().collect();
                for session_id in session_ids {
                    let _close_result = sessions.close_session(&session_id).await;
                }
            }
            Self::Authenticated(sessions) => sessions.close_all().await,
        }
    }
}

struct ServerState {
    sessions: SessionOwner,
    config: StreamableHttpServerConfig,
    requests: RwLock<()>,
}

async fn mcp(State(state): State<Arc<HttpState>>, request: Request<Body>) -> Response {
    let _request = state.server.requests.read().await;
    if request.headers().contains_key(header::ORIGIN) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let request = tokio::select! {
        biased;
        () = state.server.config.cancellation_token.cancelled() => {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        request = validate_mcp_request(request) => match request {
            Ok(request) => request,
            Err(response) => return response,
        },
    };
    serve_mcp_request(
        request,
        Some(state.factory.clone()),
        state.sessions.clone(),
        state.server.config.clone(),
    )
    .await
}

async fn validate_mcp_request(request: Request<Body>) -> Result<Request<Body>, Response> {
    if request.method() != Method::POST {
        return Ok(request);
    }

    let has_session = request
        .headers()
        .get(SESSION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some();
    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, MAX_MCP_REQUEST_BODY_SIZE)
        .await
        .map_err(|_error| StatusCode::PAYLOAD_TOO_LARGE.into_response())?;
    if !has_session {
        let message = serde_json::from_slice::<ClientJsonRpcMessage>(&bytes)
            .map_err(|_error| StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response())?;
        let ClientJsonRpcMessage::Request(request) = message else {
            return Err(StatusCode::BAD_REQUEST.into_response());
        };
        let ClientRequest::InitializeRequest(initialize) = request.request else {
            return Err(StatusCode::BAD_REQUEST.into_response());
        };
        if !initialize_protocol_header_matches(
            &parts.headers,
            initialize.params.protocol_version.as_str(),
        ) {
            return Err(StatusCode::BAD_REQUEST.into_response());
        }
    }
    Ok(Request::from_parts(parts, Body::from(bytes)))
}

fn initialize_protocol_header_matches(headers: &HeaderMap, body_protocol: &str) -> bool {
    let mut protocols = headers.get_all(HEADER_MCP_PROTOCOL_VERSION).iter();
    let protocol = protocols.next();
    protocols.next().is_none()
        && protocol.is_none_or(|header| {
            header
                .to_str()
                .is_ok_and(|protocol| protocol == body_protocol)
        })
}

fn authenticated_router(
    config: AuthenticatedMcpHttpConfig,
    runtime: AuthenticatedMcpHttpRuntime,
) -> (Router, AuthState) {
    authenticated_router_with_sessions(config, runtime, Arc::new(AuthenticatedSessions::default()))
}

fn authenticated_router_with_sessions(
    config: AuthenticatedMcpHttpConfig,
    runtime: AuthenticatedMcpHttpRuntime,
    sessions: Arc<AuthenticatedSessions>,
) -> (Router, AuthState) {
    let streamable_config =
        StreamableHttpServerConfig::default().with_allowed_hosts(config.allowed_hosts.clone());
    let server = Arc::new(ServerState {
        sessions: SessionOwner::Authenticated(sessions.clone()),
        config: streamable_config,
        requests: RwLock::new(()),
    });
    let state = Arc::new(AuthenticatedHttpState {
        config,
        runtime,
        sessions,
        server: server.clone(),
    });
    let router = Router::new()
        .route("/mcp", any(authenticated_mcp))
        .route("/livez", get(livez))
        .route("/readyz", get(authenticated_readyz))
        .route(METADATA_ROOT, get(metadata))
        .route(METADATA_ROUTE, get(metadata))
        .with_state(state.clone());
    (router, state)
}

struct AuthenticatedHttpState {
    config: AuthenticatedMcpHttpConfig,
    runtime: AuthenticatedMcpHttpRuntime,
    sessions: Arc<AuthenticatedSessions>,
    server: Arc<ServerState>,
}

async fn authenticated_mcp(State(state): State<AuthState>, request: Request<Body>) -> Response {
    let _request = state.server.requests.read().await;
    if request.headers().contains_key(header::ORIGIN) {
        return StatusCode::FORBIDDEN.into_response();
    }
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
    let request = tokio::select! {
        biased;
        () = state.server.config.cancellation_token.cancelled() => {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        request = validate_mcp_request(request) => match request {
            Ok(request) => request,
            Err(response) => return response,
        },
    };
    let binding_fingerprint = binding_fingerprint(&token);
    let request_session = match session_id(request.headers()) {
        Ok(session) => session.map(str::to_string),
        Err(()) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let (factory, admission_permit) = if let Some(session_id) = request_session.as_deref() {
        if let Err(status) = authorize_bound_session(&state, session_id, &binding_fingerprint).await
        {
            return status.into_response();
        }
        (None, None)
    } else if request.method() == Method::POST {
        let Some(admission_permit) = state.sessions.try_admit() else {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        };
        match create_session_factory(&state, token).await {
            Ok(factory) => (Some(factory), Some(admission_permit)),
            Err(status) => return status.into_response(),
        }
    } else {
        (None, None)
    };
    let sessions = Arc::new(AuthenticatedSessionManager {
        sessions: state.sessions.clone(),
        fingerprint: binding_fingerprint,
        admission_permit: Mutex::new(admission_permit),
    });
    serve_mcp_request(request, factory, sessions, state.server.config.clone()).await
}

async fn create_session_factory(
    state: &AuthenticatedHttpState,
    token: String,
) -> Result<CoralMcpServerFactory, StatusCode> {
    let client = tokio::select! {
        biased;
        () = state.server.config.cancellation_token.cancelled() => Err(StatusCode::SERVICE_UNAVAILABLE),
        client = (state.runtime.session_client_factory)(token) => client.map_err(|()| StatusCode::SERVICE_UNAVAILABLE),
    }?;
    Ok(CoralMcpServerFactory::new(
        client,
        state.runtime.options.clone(),
    ))
}

async fn authorize_bound_session(
    state: &AuthenticatedHttpState,
    session_id: &str,
    fingerprint: &BearerFingerprint,
) -> Result<(), StatusCode> {
    let session_id = Arc::from(session_id);
    let status = state.sessions.authorize(&session_id, fingerprint).await;
    match status {
        SessionAuthorization::Authorized => Ok(()),
        SessionAuthorization::Missing => Err(StatusCode::NOT_FOUND),
        SessionAuthorization::Mismatch => Err(StatusCode::FORBIDDEN),
    }
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
            "resource": state.config.public_url,
            "authorization_servers": [state.config.authorization_server],
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

fn binding_fingerprint(token: &str) -> BearerFingerprint {
    BearerFingerprint(Sha256::digest(token.as_bytes()).into())
}

// The service wrapper is intentionally request-scoped. Auth-required routing
// can validate an initialize request and pass its bearer-bound factory here,
// while later requests reuse the handler already held by `sessions`.
async fn serve_mcp_request<M>(
    request: Request<Body>,
    factory: Option<CoralMcpServerFactory>,
    sessions: Arc<M>,
    config: StreamableHttpServerConfig,
) -> Response
where
    M: SessionManager,
{
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
