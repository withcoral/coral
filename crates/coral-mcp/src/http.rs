use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use coral_workspace::{Principal, WorkspaceId};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::session::{SessionId, SessionManager as _};
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinHandle;
use tower::ServiceExt as _;

use crate::authentication::{LocalAuthentication, now};
use crate::server::McpServer;
use crate::{
    AuthenticationError, McpModules, McpRuntime, WORKSPACE_MCP_ROUTE, WorkspaceAuthenticator,
    parse_workspace_mcp_path,
};

/// Application-owned listener readiness. The callback must bound and coalesce probes.
/// It must not inspect a Workspace or contact an Upstream Service.
pub type ReadinessProbe = Arc<
    dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>> + Send + Sync,
>;

const MAX_MCP_REQUEST_BODY_SIZE: usize = 1024 * 1024;
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(1);

/// Loopback listener configuration for the credential-free local Adapter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct McpHttpConfig {
    bind_addr: SocketAddr,
}

impl McpHttpConfig {
    /// Creates loopback-only local serving configuration.
    ///
    /// # Errors
    ///
    /// Returns [`McpHttpError::NonLoopbackBind`] for a non-loopback address.
    pub fn new(bind_addr: SocketAddr) -> Result<Self, McpHttpError> {
        if !is_loopback(bind_addr.ip()) {
            return Err(McpHttpError::NonLoopbackBind(bind_addr));
        }
        Ok(Self { bind_addr })
    }

    /// Returns the configured listener address.
    #[must_use]
    pub const fn bind_addr(self) -> SocketAddr {
        self.bind_addr
    }
}

/// Local MCP listener startup or shutdown failure.
#[derive(Debug, thiserror::Error)]
pub enum McpHttpError {
    /// Credential-free serving is restricted to loopback.
    #[error("credential-free MCP HTTP bind must be loopback, got {0}")]
    NonLoopbackBind(SocketAddr),
    /// The listener could not bind.
    #[error("failed to bind MCP HTTP server to {address}")]
    Bind {
        /// Requested address.
        address: SocketAddr,
        /// Listener failure.
        #[source]
        source: io::Error,
    },
    /// The HTTP server exited unexpectedly.
    #[error("MCP HTTP server failed")]
    Server(#[source] io::Error),
    /// The HTTP task could not be joined.
    #[error("MCP HTTP server task failed")]
    Join(#[source] tokio::task::JoinError),
    /// Coordinated shutdown exceeded its bounded grace period.
    #[error("MCP HTTP server shutdown timed out")]
    ShutdownTimedOut,
}

/// Handle for one running local Streamable HTTP listener.
pub struct RunningMcpHttpServer {
    local_addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), io::Error>>,
    state: Arc<HttpState>,
}

impl RunningMcpHttpServer {
    /// Returns the bound loopback address, including an OS-assigned port.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops accepting requests, closes sessions, and joins the listener.
    ///
    /// # Errors
    ///
    /// Returns a server, join, or bounded-shutdown error.
    pub async fn shutdown(mut self) -> Result<(), McpHttpError> {
        self.begin_shutdown();
        match tokio::time::timeout(SHUTDOWN_GRACE_PERIOD, &mut self.task).await {
            Ok(result) => result
                .map_err(McpHttpError::Join)?
                .map_err(McpHttpError::Server),
            Err(_elapsed) => {
                self.task.abort();
                let _join_result = (&mut self.task).await;
                Err(McpHttpError::ShutdownTimedOut)
            }
        }
    }

    fn begin_shutdown(&mut self) {
        self.state.config.cancellation_token.cancel();
        if let Some(shutdown) = self.shutdown.take() {
            let _send_result = shutdown.send(());
        }
    }
}

impl Drop for RunningMcpHttpServer {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

/// Starts the credential-free Workspace-scoped Streamable HTTP Adapter.
///
/// Missing credentials map to the implicit local User; a presented credential
/// must authenticate an Agent Registration. Each path fixes one Workspace, and normal authority is
/// checked before transport dispatch. Domain Modules recheck exact authority,
/// attribution, and resource visibility for each tool operation.
///
/// # Errors
///
/// Returns [`McpHttpError`] if the listener cannot bind.
pub async fn start_local(
    config: McpHttpConfig,
    modules: McpModules,
    readiness: ReadinessProbe,
) -> Result<RunningMcpHttpServer, McpHttpError> {
    let authentication = Arc::new(LocalAuthentication(modules.workspaces.clone()));
    start_server(config, modules, authentication, None, readiness).await
}

/// Serves authenticated User OAuth and Agent requests on the Workspace surface.
/// The host supplies public hosts and a credential-verifying Adapter, never a
/// preauthenticated session or a caller-selected Principal.
/// # Errors
/// Returns a listener startup failure.
pub async fn start_authenticated(
    bind_addr: SocketAddr,
    allowed_hosts: Vec<String>,
    modules: McpModules,
    authentication: Arc<dyn WorkspaceAuthenticator>,
    readiness: ReadinessProbe,
) -> Result<RunningMcpHttpServer, McpHttpError> {
    start_server(
        McpHttpConfig { bind_addr },
        modules,
        authentication,
        Some(allowed_hosts),
        readiness,
    )
    .await
}

async fn start_server(
    config: McpHttpConfig,
    modules: McpModules,
    authentication: Arc<dyn WorkspaceAuthenticator>,
    allowed_hosts: Option<Vec<String>>,
    readiness: ReadinessProbe,
) -> Result<RunningMcpHttpServer, McpHttpError> {
    let listener = TcpListener::bind(config.bind_addr())
        .await
        .map_err(|source| McpHttpError::Bind {
            address: config.bind_addr(),
            source,
        })?;
    let local_addr = listener.local_addr().map_err(McpHttpError::Server)?;
    let allowed_hosts = allowed_hosts.unwrap_or_else(|| {
        vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
            "::1".to_string(),
            local_addr.ip().to_string(),
        ]
    });
    let streamable_config = StreamableHttpServerConfig::default()
        .with_allowed_hosts(allowed_hosts)
        .with_max_request_body_bytes(MAX_MCP_REQUEST_BODY_SIZE);
    let state = Arc::new(HttpState {
        modules,
        authentication,
        sessions: RwLock::new(HashMap::new()),
        config: streamable_config,
    });
    let health_state = state.clone();
    let router = Router::new()
        .route("/healthz", axum::routing::get(|| async { StatusCode::OK }))
        .route(
            "/readyz",
            axum::routing::get(move || {
                let readiness = readiness.clone();
                let state = health_state.clone();
                async move {
                    if !state.config.cancellation_token.is_cancelled() && readiness().await {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            }),
        )
        .route(WORKSPACE_MCP_ROUTE, any(mcp))
        .route(
            "/.well-known/oauth-protected-resource/workspaces/{workspace_id}/mcp",
            axum::routing::get(metadata),
        )
        .with_state(state.clone());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task_state = state.clone();
    let task = tokio::spawn(async move {
        let result = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _shutdown_result = shutdown_rx.await;
            })
            .await;
        task_state.close_sessions().await;
        result
    });
    Ok(RunningMcpHttpServer {
        local_addr,
        shutdown: Some(shutdown_tx),
        task,
        state,
    })
}

struct HttpState {
    modules: McpModules,
    authentication: Arc<dyn WorkspaceAuthenticator>,
    sessions: RwLock<HashMap<(WorkspaceId, Principal), Arc<LocalSessionManager>>>,
    config: StreamableHttpServerConfig,
}

impl HttpState {
    async fn session_manager(
        &self,
        workspace_id: &WorkspaceId,
        principal: &Principal,
    ) -> Arc<LocalSessionManager> {
        let key = (workspace_id.clone(), principal.clone());
        if let Some(manager) = self.sessions.read().await.get(&key) {
            return manager.clone();
        }
        self.sessions
            .write()
            .await
            .entry(key)
            .or_insert_with(|| Arc::new(LocalSessionManager::default()))
            .clone()
    }

    async fn close_sessions(&self) {
        let managers = self
            .sessions
            .write()
            .await
            .drain()
            .map(|(_workspace_id, manager)| manager)
            .collect::<Vec<_>>();
        for manager in managers {
            let session_ids = manager
                .sessions
                .read()
                .await
                .keys()
                .cloned()
                .collect::<Vec<SessionId>>();
            for session_id in session_ids {
                let _close_result = manager.close_session(&session_id).await;
            }
        }
    }
}

async fn mcp(State(state): State<Arc<HttpState>>, mut request: Request<Body>) -> Response {
    if state.config.cancellation_token.is_cancelled() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if request.headers().contains_key(header::ORIGIN) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(workspace_id) = parse_workspace_mcp_path(request.uri().path()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if request.uri().query().is_some() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Ok(bearer) = bearer_credential(request.headers()) else {
        return authentication_error(&state, &workspace_id, AuthenticationError::Unauthenticated);
    };
    let principal = match state
        .authentication
        .authenticate(&workspace_id, bearer)
        .await
    {
        Ok(principal) => principal,
        Err(error) => return authentication_error(&state, &workspace_id, error),
    };
    let Ok(now) = now() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match state
        .modules
        .workspaces
        .check_principal_inspection(&principal, &workspace_id, now)
        .await
    {
        Ok(_evidence) => {}
        Err(
            coral_workspace::Error::WorkspaceNotFound | coral_workspace::Error::PermissionDenied(_),
        ) => return StatusCode::NOT_FOUND.into_response(),
        Err(_error) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
    // Authentication is complete. RMCP/session extensions need no credential
    // material; only the separately constructed canonical context reaches tools.
    request.headers_mut().remove(header::AUTHORIZATION);
    let sessions = state
        .session_manager(&workspace_id, principal.principal())
        .await;
    let handler = McpServer::new(
        McpRuntime {
            modules: state.modules.clone(),
            principal,
        },
        workspace_id,
    );
    let config = state.config.clone();
    let service = StreamableHttpService::new(
        move || Ok::<_, io::Error>(handler.clone()),
        sessions,
        config,
    );
    match service.oneshot(request).await {
        Ok(response) => response.map(Body::new),
        Err(never) => match never {},
    }
}

fn bearer_credential(headers: &axum::http::HeaderMap) -> Result<Option<&str>, ()> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(());
    }
    let (scheme, token) = value
        .to_str()
        .map_err(|_error| ())?
        .split_once(' ')
        .ok_or(())?;
    if !scheme.eq_ignore_ascii_case("Bearer")
        || token.is_empty()
        || token.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return Err(());
    }
    Ok(Some(token))
}

fn authentication_error(
    state: &HttpState,
    workspace: &WorkspaceId,
    error: AuthenticationError,
) -> Response {
    if matches!(error, AuthenticationError::Unavailable) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let challenge = state.authentication.oauth_resource(workspace).map_or_else(
        || "Bearer".to_owned(),
        |resource| format!("Bearer resource_metadata=\"{}\"", resource.metadata_url),
    );
    let mut response = (StatusCode::UNAUTHORIZED, "authentication required").into_response();
    if let Ok(value) = axum::http::HeaderValue::from_str(&challenge) {
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, value);
    }
    response
}

async fn metadata(State(state): State<Arc<HttpState>>, request: Request<Body>) -> Response {
    let Some(path) = request
        .uri()
        .path()
        .strip_prefix("/.well-known/oauth-protected-resource")
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(workspace) = parse_workspace_mcp_path(path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(resource) = state.authentication.oauth_resource(&workspace) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    ([(header::CONTENT_TYPE, "application/json")], serde_json::json!({"resource": resource.resource, "authorization_servers": [resource.authorization_server], "bearer_methods_supported": ["header"]}).to_string()).into_response()
}

fn is_loopback(ip: IpAddr) -> bool {
    ip.is_loopback()
        || matches!(ip, IpAddr::V6(ip) if ip.to_ipv4_mapped().is_some_and(|ip| ip.is_loopback()))
}
