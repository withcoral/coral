//! Streamable HTTP transport for Coral's MCP surface.
//!
//! [`start_auth_disabled`] is limited to loopback by default. It shares an
//! unauthenticated local [`coral_client::AppClient`] across sessions, so
//! network reachability is its entire access control. The only way off
//! loopback without authentication is
//! [`McpHttpConfig::allow_unauthenticated_non_loopback`], the constructor
//! that configuration deliberately routes operator consent through; any
//! other non-loopback serving must use the authenticated construction path.

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
use coral_api::v1::ListWorkspacesRequest;
use coral_app::{CanonicalOauthUrl, McpWorkspaceSegment, OauthUrlError, WorkspaceMcpUrls};
use coral_client::{AppClient, workspace as workspace_proto};
use futures::{Stream, StreamExt as _};
use rmcp::model::{
    ClientJsonRpcMessage, ClientRequest, ErrorData, RequestId, ServerJsonRpcMessage,
};
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

use crate::{CoralMcpServerFactory, McpOptions, server::WorkspaceRequired};

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
/// The auth-disabled listener has no public URL to derive a mount from, so its
/// workspace routes live under the fixed `/mcp` prefix.
const AUTH_DISABLED_MCP_PREFIX: &str = "/mcp";
const AUTH_DISABLED_MCP_WORKSPACE_ROUTE: &str = "/mcp/workspace/{workspace}";
/// What an unmatched path is told, in place of a bare not-found.
///
/// The sentence is static on purpose: it names the URL shape — public
/// information every startup summary and document states — and never a
/// workspace, so it discloses nothing a concealed refusal hides. The legacy
/// `/mcp` endpoint lands here too; this is its entire deprecation notice.
const WORKSPACE_URL_HINT: &str = "Each workspace is served at its own MCP URL ending in /workspace/{workspace}. Check the URL with your workspace owner.";
const MAX_MCP_REQUEST_BODY_SIZE: usize = 1_048_576;
const MAX_AUTHENTICATED_SESSIONS: usize = 4096;
const AUTHENTICATED_SESSION_IDLE_TIMEOUT: Duration = Duration::from_hours(1);

type ProbeFuture = Pin<Box<dyn Future<Output = Result<(), tonic::Code>> + Send>>;
type Fut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
/// Validates one bearer token for one exact audience — the route's resource.
type TokenValidator = Arc<dyn Fn(String, String) -> Fut<Result<(), ()>> + Send + Sync>;
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
    /// The session-admission budget, deliberately **server-global** rather than
    /// per workspace: adding workspace URLs must not multiply the listener's
    /// effective session-capacity limit, so every workspace on this listener
    /// draws from the one pool. A single member can therefore hold the whole
    /// budget — that is the intended cap, not an isolation gap, and partitioning
    /// it per workspace would let N workspaces multiply capacity N-fold. The
    /// permit is reserved before the bearer-bound membership listing so the same
    /// budget also bounds concurrent admission work, and a refused handshake
    /// releases it immediately.
    admission: Arc<Semaphore>,
}

struct AuthenticatedSession {
    /// The workspace whose URL opened this session.
    ///
    /// Checked before the bearer binding: a session presented at another
    /// workspace's URL does not exist there, whatever credential arrives with
    /// it, so the answer is the same not-found an unknown session gets.
    workspace: McpWorkspaceSegment,
    fingerprint: BearerFingerprint,
    handle: LocalSessionHandle,
    _admission_permit: OwnedSemaphorePermit,
}

struct AuthenticatedSessionManager {
    sessions: Arc<AuthenticatedSessions>,
    workspace: McpWorkspaceSegment,
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
        workspace: &McpWorkspaceSegment,
        fingerprint: &BearerFingerprint,
    ) -> SessionAuthorization {
        let records = self.records.read().await;
        let Some(session) = records.get(session_id) else {
            return SessionAuthorization::Missing;
        };
        // Workspace first, deliberately: at the wrong workspace's URL the
        // session does not exist, even for a bearer that is valid there, so a
        // cross-workspace replay is indistinguishable from an unknown session.
        if session.workspace != *workspace {
            return SessionAuthorization::Missing;
        }
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
    fn owns(&self, session: &AuthenticatedSession) -> bool {
        session.workspace == self.workspace && session.fingerprint == self.fingerprint
    }

    async fn session_handle(
        &self,
        session_id: &SessionId,
    ) -> Result<LocalSessionHandle, AuthenticatedSessionManagerError> {
        self.sessions
            .records
            .read()
            .await
            .get(session_id)
            .filter(|session| self.owns(session))
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
                workspace: self.workspace.clone(),
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
            .is_some_and(|session| self.owns(session)))
    }

    async fn close_session(&self, session_id: &SessionId) -> Result<(), Self::Error> {
        let session = {
            let mut records = self.sessions.records.write().await;
            match records.get(session_id) {
                None => None,
                Some(session) if !self.owns(session) => {
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

/// Configuration for the auth-disabled MCP HTTP server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpHttpConfig {
    bind_addr: SocketAddr,
    extra_allowed_hosts: Vec<String>,
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
        Ok(Self {
            bind_addr,
            extra_allowed_hosts: Vec::new(),
        })
    }

    /// Creates server configuration that may bind off loopback.
    ///
    /// Skipping the loopback check is this constructor's entire purpose, and
    /// what its name exists to make callers say: every session still shares
    /// one unauthenticated client, so whoever can reach the listener holds
    /// the local user's full authority. Call this only to honor an explicit,
    /// fail-closed operator opt-in (`allow_unauthenticated_non_loopback` in
    /// the server configuration) — never as a convenience.
    #[must_use]
    pub fn allow_unauthenticated_non_loopback(bind_addr: SocketAddr) -> Self {
        Self {
            bind_addr,
            extra_allowed_hosts: Vec::new(),
        }
    }

    /// Accepts additional Host header values beside the loopback defaults.
    ///
    /// The Host allowlist is the DNS-rebinding defense, so entries must name
    /// only hosts the operator expects legitimate clients to use — e.g. a
    /// Docker Compose service name.
    ///
    /// # Errors
    ///
    /// Returns [`McpHttpError::InvalidAuthConfig`] when an entry is not a
    /// valid header value.
    pub fn with_allowed_hosts(
        mut self,
        hosts: impl IntoIterator<Item = String>,
    ) -> Result<Self, McpHttpError> {
        let hosts: Vec<String> = hosts.into_iter().collect();
        if hosts
            .iter()
            .any(|host| HeaderValue::from_str(host).is_err())
        {
            return Err(McpHttpError::InvalidAuthConfig("invalid allowed Host"));
        }
        self.extra_allowed_hosts = hosts;
        Ok(self)
    }

    /// Returns the configured bind address.
    #[must_use]
    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }
}

/// Validated OAuth protected-resource configuration.
///
/// The public URL is the base of the per-workspace resource family: every
/// workspace is served at `<public_url>/workspace/<name>`, each such URL its
/// own protected resource with its own metadata document and token audience.
/// The base itself is not an MCP endpoint.
#[derive(Clone, Debug)]
pub struct AuthenticatedMcpHttpConfig {
    bind_addr: SocketAddr,
    urls: WorkspaceMcpUrls,
    authorization_server: String,
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
            urls: WorkspaceMcpUrls::new(resource),
            authorization_server: authorization_server.into_identifier(),
            allowed_hosts,
        })
    }

    /// The axum route template the workspace family mounts at, derived from
    /// the public URL's path so the served paths match the advertised URLs
    /// without any ingress rewriting.
    fn mcp_route(&self) -> String {
        format!("{}/workspace/{{workspace}}", self.urls.base_path())
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
        V: Fn(String, String) -> VF + Send + Sync + 'static,
        VF: Future<Output = Result<(), ()>> + Send + 'static,
        F: Fn(String) -> FF + Send + Sync + 'static,
        FF: Future<Output = Result<AppClient, ()>> + Send + 'static,
        R: Fn() -> RF + Send + Sync + 'static,
        RF: Future<Output = Result<(), tonic::Code>> + Send + 'static,
    {
        Self {
            validator: Arc::new(move |token, audience| Box::pin(validator(token, audience))),
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

fn is_loopback(ip: IpAddr) -> bool {
    ip.is_loopback()
        || matches!(ip, IpAddr::V6(ip) if ip.to_ipv4_mapped().is_some_and(|ip| ip.is_loopback()))
}

/// MCP HTTP startup or shutdown failure.
#[derive(Debug, thiserror::Error)]
pub enum McpHttpError {
    /// Static MCP extension composition was invalid.
    #[error(transparent)]
    Surface(#[from] crate::McpSurfaceError),
    /// Auth-disabled serving is restricted to the local machine unless the
    /// operator consented via [`McpHttpConfig::allow_unauthenticated_non_loopback`].
    #[error("auth-disabled MCP HTTP bind must be loopback, got {0}")]
    NonLoopbackBind(SocketAddr),
    /// MCP HTTP serving configuration is invalid.
    #[error("invalid MCP HTTP configuration: {0}")]
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

/// Starts the authentication-disabled MCP HTTP server.
///
/// The supplied unauthenticated client is shared across independent MCP
/// sessions, so the listener is loopback-only unless the config was built
/// with [`McpHttpConfig::allow_unauthenticated_non_loopback`]. Auth-required
/// serving must use a distinct construction path that creates a bearer-bound
/// client after validating each incoming session.
///
/// Every existing workspace is served at `/mcp/workspace/<name>` — the URL
/// names the workspace, so no workspace is resolved at startup and any value
/// in `options.workspace` (stdio's carrier for its own resolved selection) is
/// ignored here. Existence is checked per handshake against the local
/// client's own workspace listing, so a workspace created after startup is
/// reachable and a deleted one refuses new sessions immediately.
///
/// # Errors
///
/// Returns [`McpHttpError`] if the listener cannot bind.
pub async fn start_auth_disabled(
    config: McpHttpConfig,
    app: AppClient,
    options: McpOptions,
) -> Result<RunningMcpHttpServer, McpHttpError> {
    options.surface.validate(options.feedback_enabled)?;
    let listener = TcpListener::bind(config.bind_addr())
        .await
        .map_err(|source| McpHttpError::Bind {
            address: config.bind_addr(),
            source,
        })?;
    let local_addr = listener.local_addr().map_err(McpHttpError::Server)?;
    let readiness = ReadinessProbe::from_app(app.clone());
    let (router, state) = auth_disabled_router(
        app,
        McpOptions {
            workspace: None,
            ..options
        },
        readiness,
        local_addr.ip(),
        &config.extra_allowed_hosts,
    );
    Ok(spawn_http_server(
        listener,
        local_addr,
        router,
        state.server.clone(),
    ))
}

/// Names the workspaces the client's own caller belongs to, in listing order.
///
/// The listing is scoped to whoever the client authenticates as, so which
/// client this is asked with is the whole of the answer's meaning.
async fn membership_workspace_names(app: &AppClient) -> Result<Vec<String>, tonic::Status> {
    Ok(app
        .workspace_client()
        .list_workspaces(GrpcRequest::new(ListWorkspacesRequest {}))
        .await?
        .into_inner()
        .memberships
        .into_iter()
        .map(|membership| membership.workspace.unwrap_or_default().name)
        .collect())
}

/// Starts authenticated serving; fails when the listener cannot bind.
/// # Errors
/// Returns an error when the listener cannot bind.
pub async fn start_authenticated(
    config: AuthenticatedMcpHttpConfig,
    runtime: AuthenticatedMcpHttpRuntime,
) -> Result<RunningMcpHttpServer, McpHttpError> {
    runtime
        .options
        .surface
        .validate(runtime.options.feedback_enabled)?;
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
    /// Probes the server over the unauthenticated gRPC health service.
    ///
    /// A data-plane call cannot serve here. It would have to name a workspace,
    /// and the only name available without reading state is one nothing
    /// provisions — whose `NotFound` [`rejection_is_reachable`] reads as
    /// reachable, so `/readyz` would answer ready for a server that can reach
    /// nothing at all. The health service asks about the instance itself,
    /// server-side, and names no workspace.
    ///
    /// Twin of `probe_serving_health` in `coral-cli/src/serve.rs`, which
    /// supplies this probe for the authenticated surface. The mapping — ready,
    /// not-ready as `Unavailable`, the status code otherwise — is duplicated on
    /// purpose: only the surface-specific reasoning around it differs. Change
    /// one mapping and the other needs the same change, or the two `/readyz`
    /// surfaces drift apart.
    fn from_app(app: AppClient) -> Self {
        Self(Arc::new(move || {
            let app = app.clone();
            Box::pin(async move {
                match app.check_engine_ready().await {
                    Ok(true) => Ok(()),
                    Ok(false) => Err(tonic::Code::Unavailable),
                    Err(status) => Err(status.code()),
                }
            })
        }))
    }
}

/// Builds the loopback router serving every workspace at its own URL.
///
/// Session factories are created per workspace, on first admitted handshake,
/// and cached: sessions within one workspace share guide-block state while
/// sessions in different workspaces share nothing, and each workspace's
/// sessions live in their own manager so a session id minted at one
/// workspace's URL structurally does not exist at another's.
fn auth_disabled_router(
    app: AppClient,
    options: McpOptions,
    readiness: ReadinessProbe,
    advertised_ip: IpAddr,
    extra_allowed_hosts: &[String],
) -> (Router, Arc<HttpState>) {
    // These are the same loopback names the authenticated listener accepts, so
    // a client dialing `localhost` is not rejected for the bind being
    // `127.0.0.1`. The allowlist stays exact beyond that baseline: it is the
    // DNS-rebinding defense for originless requests, so only operator-listed
    // hosts join it.
    let mut allowed_hosts = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
        advertised_ip.to_string(),
    ];
    allowed_hosts.extend(extra_allowed_hosts.iter().cloned());
    let config = StreamableHttpServerConfig::default().with_allowed_hosts(allowed_hosts);
    let workspaces = Arc::new(RwLock::new(HashMap::new()));
    let server = Arc::new(ServerState {
        sessions: SessionOwner::Local(workspaces.clone()),
        config,
        requests: RwLock::new(()),
    });
    let state = Arc::new(HttpState {
        app,
        options,
        workspaces,
        readiness,
        server: server.clone(),
    });
    let router = Router::new()
        .route(AUTH_DISABLED_MCP_WORKSPACE_ROUTE, any(mcp))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .fallback(not_an_mcp_endpoint)
        .with_state(state.clone());
    (router, state)
}

/// One served workspace on the auth-disabled surface.
struct WorkspaceSessions {
    factory: CoralMcpServerFactory,
    sessions: Arc<LocalSessionManager>,
}

type LocalWorkspaces = Arc<RwLock<HashMap<McpWorkspaceSegment, WorkspaceSessions>>>;

struct HttpState {
    app: AppClient,
    options: McpOptions,
    workspaces: LocalWorkspaces,
    readiness: ReadinessProbe,
    server: Arc<ServerState>,
}

enum SessionOwner {
    Local(LocalWorkspaces),
    Authenticated(Arc<AuthenticatedSessions>),
}

impl SessionOwner {
    async fn close_all(&self) {
        match self {
            Self::Local(workspaces) => {
                let managers: Vec<_> = workspaces
                    .read()
                    .await
                    .values()
                    .map(|workspace| workspace.sessions.clone())
                    .collect();
                for sessions in managers {
                    let session_ids: Vec<_> =
                        sessions.sessions.read().await.keys().cloned().collect();
                    for session_id in session_ids {
                        let _close_result = sessions.close_session(&session_id).await;
                    }
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
    // The raw request path is parsed rather than the router's decoded capture:
    // the charset admits no percent-encoding, so an encoded spelling of a
    // valid name is refused instead of normalized into it.
    let Some(workspace) = parse_workspace_route(request.uri().path(), AUTH_DISABLED_MCP_PREFIX)
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let (request, initialize_id) = tokio::select! {
        biased;
        () = state.server.config.cancellation_token.cancelled() => {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        request = validate_mcp_request(request) => match request {
            Ok(validated) => validated,
            Err(response) => return response,
        },
    };
    let entry = if initialize_id.is_some() {
        // Existence is answered per handshake, and absence is never cached:
        // the local caller's own listing is the whole inventory here, so a
        // workspace created a moment ago is admitted and a deleted one is the
        // same plain not-found a name that never existed gets.
        match admit_local_workspace(&state, &workspace).await {
            Ok(entry) => entry,
            Err(response) => return response,
        }
    } else {
        let workspaces = state.workspaces.read().await;
        let Some(entry) = workspaces.get(&workspace) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        (entry.factory.clone(), entry.sessions.clone())
    };
    let (factory, sessions) = entry;
    serve_mcp_request(
        request,
        Some(factory),
        sessions,
        state.server.config.clone(),
    )
    .await
}

/// Admits one auth-disabled handshake for the workspace its URL names.
///
/// Lists the local caller's workspaces to answer existence, then returns the
/// workspace's cached factory and session manager, creating them on first
/// admission. The listing failing is a server problem, not an answer about
/// any workspace, so it maps to service-unavailable rather than not-found.
async fn admit_local_workspace(
    state: &HttpState,
    workspace: &McpWorkspaceSegment,
) -> Result<(CoralMcpServerFactory, Arc<LocalSessionManager>), Response> {
    let names = tokio::select! {
        biased;
        () = state.server.config.cancellation_token.cancelled() => {
            return Err(StatusCode::SERVICE_UNAVAILABLE.into_response());
        }
        names = membership_workspace_names(&state.app) => names.map_err(|status| {
            tracing::warn!(%status, "listing local workspaces for MCP admission failed");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        })?,
    };
    if !names.iter().any(|name| name == workspace.as_str()) {
        return Err(StatusCode::NOT_FOUND.into_response());
    }
    let mut workspaces = state.workspaces.write().await;
    if let Some(entry) = workspaces.get(workspace) {
        return Ok((entry.factory.clone(), entry.sessions.clone()));
    }
    let options = McpOptions {
        workspace: Some(workspace_proto(workspace.as_str().to_string())),
        ..state.options.clone()
    };
    let factory = CoralMcpServerFactory::new(state.app.clone(), options)
        .map_err(|WorkspaceRequired| StatusCode::INTERNAL_SERVER_ERROR.into_response())?;
    let sessions = Arc::new(LocalSessionManager::default());
    workspaces.insert(
        workspace.clone(),
        WorkspaceSessions {
            factory: factory.clone(),
            sessions: sessions.clone(),
        },
    );
    Ok((factory, sessions))
}

/// Parses `<prefix>/workspace/<segment>` from a raw request path.
fn parse_workspace_route(path: &str, prefix: &str) -> Option<McpWorkspaceSegment> {
    McpWorkspaceSegment::parse(path.strip_prefix(prefix)?.strip_prefix("/workspace/")?)
}

/// Answers every path that is not an MCP endpoint on this listener.
///
/// The body is one static sentence naming the URL shape; see
/// [`WORKSPACE_URL_HINT`] for why that discloses nothing.
async fn not_an_mcp_endpoint() -> Response {
    (
        StatusCode::NOT_FOUND,
        [(header::CONTENT_TYPE, "application/json")],
        json!({ "error": "not_found", "hint": WORKSPACE_URL_HINT }).to_string(),
    )
        .into_response()
}

/// Validates one MCP request, reporting the id of the `initialize` it carries.
///
/// The id is `Some` exactly for the sessionless POST that opens a session, so a
/// caller that refuses admission can answer that request in its own terms
/// instead of re-parsing the body it just handed on.
async fn validate_mcp_request(
    request: Request<Body>,
) -> Result<(Request<Body>, Option<RequestId>), Response> {
    if request.method() != Method::POST {
        return Ok((request, None));
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
    let mut initialize_id = None;
    if !has_session {
        let message = serde_json::from_slice::<ClientJsonRpcMessage>(&bytes)
            .map_err(|_error| StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response())?;
        let ClientJsonRpcMessage::Request(request) = message else {
            return Err(StatusCode::BAD_REQUEST.into_response());
        };
        let request_id = request.id;
        let ClientRequest::InitializeRequest(initialize) = request.request else {
            return Err(StatusCode::BAD_REQUEST.into_response());
        };
        if !initialize_protocol_header_matches(
            &parts.headers,
            initialize.params.protocol_version.as_str(),
        ) {
            return Err(StatusCode::BAD_REQUEST.into_response());
        }
        initialize_id = Some(request_id);
    }
    Ok((Request::from_parts(parts, Body::from(bytes)), initialize_id))
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
        .route(&state.config.mcp_route(), any(authenticated_mcp))
        .route("/livez", get(livez))
        .route("/readyz", get(authenticated_readyz))
        .route(METADATA_ROOT, get(metadata))
        .route(METADATA_ROUTE, get(metadata))
        .fallback(not_an_mcp_endpoint)
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
    // The raw request path is parsed rather than the router's decoded capture:
    // the charset admits no percent-encoding, so an encoded spelling of a
    // valid name is refused instead of normalized into it. A malformed segment
    // is a plain not-found with no challenge and no metadata; every
    // well-formed one gets the identical challenge whether or not a workspace
    // by that name exists, so an anonymous probe learns nothing.
    let Some(workspace) = state.config.urls.parse_route_path(request.uri().path()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if request.method() == Method::OPTIONS {
        return StatusCode::NO_CONTENT.into_response();
    }
    let Some(token) = bearer_token(request.headers()) else {
        return unauthorized_response(&state.config, &workspace);
    };
    // The route's resource is the exact audience this request must have been
    // minted for: a bearer for one workspace is invalid at another's URL, with
    // that URL's own challenge.
    let resource = state.config.urls.resource(&workspace);
    let validation = tokio::select! {
        biased;
        () = state.server.config.cancellation_token.cancelled() => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        validation = (state.runtime.validator)(token.clone(), resource) => validation,
    };
    if validation.is_err() {
        return unauthorized_response(&state.config, &workspace);
    }
    let (request, initialize_id) = tokio::select! {
        biased;
        () = state.server.config.cancellation_token.cancelled() => {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        request = validate_mcp_request(request) => match request {
            Ok(validated) => validated,
            Err(response) => return response,
        },
    };
    let binding_fingerprint = binding_fingerprint(&token);
    let request_session = match session_id(request.headers()) {
        Ok(session) => session.map(str::to_string),
        Err(()) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let (factory, admission_permit) = if let Some(session_id) = request_session.as_deref() {
        if let Err(status) =
            authorize_bound_session(&state, session_id, &workspace, &binding_fingerprint).await
        {
            return status.into_response();
        }
        (None, None)
    } else if request.method() == Method::POST {
        let Some(admission_permit) = state.sessions.try_admit() else {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        };
        match create_session_factory(&state, token, &workspace).await {
            Ok(factory) => (Some(factory), Some(admission_permit)),
            Err(refusal) => return refusal.into_response(initialize_id),
        }
    } else {
        (None, None)
    };
    let sessions = Arc::new(AuthenticatedSessionManager {
        sessions: state.sessions.clone(),
        workspace,
        fingerprint: binding_fingerprint,
        admission_permit: Mutex::new(admission_permit),
    });
    serve_mcp_request(request, factory, sessions, state.server.config.clone()).await
}

/// Builds the bearer-bound session factory admitted for one initialize request.
///
/// The membership listing that decides admission runs on the caller's own
/// bearer-bound client and on nothing else. The unauthenticated client this
/// surface holds serves health readiness only: listing with it would hand every
/// caller one deployment-wide answer and collapse the per-caller concealment
/// this admission exists to provide.
///
/// The workspace is the one the request's URL names — nothing else selects
/// it, and no other workspace is ever substituted.
async fn create_session_factory(
    state: &AuthenticatedHttpState,
    token: String,
    workspace: &McpWorkspaceSegment,
) -> Result<CoralMcpServerFactory, SessionRefusal> {
    let client = tokio::select! {
        biased;
        () = state.server.config.cancellation_token.cancelled() => Err(SessionRefusal::Unavailable),
        client = (state.runtime.session_client_factory)(token) => client.map_err(|()| SessionRefusal::Unavailable),
    }?;
    require_membership(&client, workspace.as_str()).await?;
    let options = McpOptions {
        workspace: Some(workspace_proto(workspace.as_str().to_string())),
        ..state.runtime.options.clone()
    };
    CoralMcpServerFactory::new(client, options)
        .map_err(|WorkspaceRequired| SessionRefusal::Unavailable)
}

/// Admits the session only when the caller already holds the named workspace.
///
/// The single listing this makes is the entire authorization decision: an exact
/// name match admits, and every other outcome is the one concealed refusal. No
/// membership is picked on the caller's behalf and no other workspace is
/// substituted, so nothing here needs to know who the caller is — a listing
/// scoped to the caller answers the only question admission has.
async fn require_membership(client: &AppClient, workspace: &str) -> Result<(), SessionRefusal> {
    let memberships = membership_workspace_names(client).await.map_err(|status| {
        tracing::warn!(%status, "listing memberships for MCP session admission failed");
        SessionRefusal::Unavailable
    })?;
    if memberships.iter().any(|name| name == workspace) {
        return Ok(());
    }
    Err(SessionRefusal::WorkspaceNotFound(workspace.to_string()))
}

/// Why an authenticated MCP session was not admitted.
///
/// [`Self::WorkspaceNotFound`] is deliberately the only membership answer.
/// Admission reads the caller's own memberships and never asks whether the
/// URL's name exists, so a workspace the caller may not reach and one that
/// was never created are indistinguishable from here — and from the caller.
enum SessionRefusal {
    /// The URL's workspace is not one of the caller's memberships.
    WorkspaceNotFound(String),
    /// Admission could not be decided; not an answer about any workspace.
    Unavailable,
}

impl SessionRefusal {
    /// Answers the `initialize` request that asked to be admitted.
    ///
    /// The handshake is the only exchange on this surface that is not
    /// workspace-scoped, so it is the one place guidance can reach a caller who
    /// has no workspace to reach. A transport-level status would surface as a
    /// bare connection failure instead.
    fn into_response(self, initialize_id: Option<RequestId>) -> Response {
        let guidance = match self {
            Self::Unavailable => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
            Self::WorkspaceNotFound(name) => workspace_not_found(&name),
        };
        let refusal =
            ServerJsonRpcMessage::error(ErrorData::invalid_request(guidance, None), initialize_id);
        match serde_json::to_string(&refusal) {
            Ok(body) => ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
            Err(_error) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
}

/// The one answer for a workspace URL the caller does not hold.
///
/// It must stay identical for a name that does not exist and a name the caller
/// may not reach; the two are the same sentence on purpose.
fn workspace_not_found(name: &str) -> String {
    format!(
        "Workspace `{name}` was not found. Check the workspace URL, or ask a workspace owner to add you."
    )
}

async fn authorize_bound_session(
    state: &AuthenticatedHttpState,
    session_id: &str,
    workspace: &McpWorkspaceSegment,
    fingerprint: &BearerFingerprint,
) -> Result<(), StatusCode> {
    let session_id = Arc::from(session_id);
    let status = state
        .sessions
        .authorize(&session_id, workspace, fingerprint)
        .await;
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

/// Serves one workspace resource's RFC 9728 metadata document.
///
/// The document is a pure derivation of the URL — its `resource` value is
/// exactly the identifier whose path-inserted well-known URL was fetched —
/// and it is served for every well-formed workspace name, existent or not.
/// Uniformity is the concealment: gating it on existence would let an
/// anonymous probe enumerate workspaces. The base URL's own metadata path
/// names no workspace and is a plain not-found, like every other path.
async fn metadata(State(state): State<AuthState>, uri: Uri) -> Response {
    let Some(workspace) = state.config.urls.parse_metadata_path(uri.path()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    (
        [(header::CONTENT_TYPE, "application/json")],
        json!({
            "resource": state.config.urls.resource(&workspace),
            "authorization_servers": [state.config.authorization_server],
            "bearer_methods_supported": ["header"],
        })
        .to_string(),
    )
        .into_response()
}

/// Challenges with the route's own metadata URL.
///
/// Clients follow the challenged URL exclusively, so it must agree byte for
/// byte with the path [`metadata`] serves. Built per request: the workspace
/// segment's charset guarantees a valid header value, and the fallback arm
/// exists only because `HeaderValue::from_str` returns a `Result`.
fn unauthorized_response(
    config: &AuthenticatedMcpHttpConfig,
    workspace: &McpWorkspaceSegment,
) -> Response {
    let challenge = format!(
        "Bearer resource_metadata=\"{}\"",
        config.urls.metadata_url(workspace)
    );
    match HeaderValue::from_str(&challenge) {
        Ok(challenge) => (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, challenge)],
        )
            .into_response(),
        Err(_error) => StatusCode::UNAUTHORIZED.into_response(),
    }
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
        Ok(Err(code)) if rejection_is_reachable(code) => StatusCode::NO_CONTENT,
        Ok(Err(_)) | Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// Whether a rejection still proves the server on the other end is reachable.
///
/// The probe's own transport failures — and the codes a server emits when it
/// cannot serve — mean unready. Everything else arrived *from* a server that
/// answered, so it reports reachability even though the call itself failed.
fn rejection_is_reachable(code: tonic::Code) -> bool {
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
