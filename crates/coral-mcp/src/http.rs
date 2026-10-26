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
use coral_workspace::{AuthenticatedPrincipal, Principal, WorkspaceId, WorkspaceState};
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
    AuthenticationError, McpModules, McpRuntime, ORGANIZATION_MCP_ROUTE, OauthProtectedResource,
    WORKSPACE_MCP_ROUTE, WorkspaceAuthenticator, parse_workspace_mcp_path,
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
        .route(ORGANIZATION_MCP_ROUTE, any(organization_mcp))
        .route(
            "/.well-known/oauth-protected-resource/workspaces/{workspace_id}/mcp",
            axum::routing::get(metadata),
        )
        .route(
            ORGANIZATION_METADATA_ROUTE,
            axum::routing::get(organization_metadata),
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

/// Protected-resource metadata of the Organization MCP Endpoint (RFC 9728).
const ORGANIZATION_METADATA_ROUTE: &str = "/.well-known/oauth-protected-resource/mcp";

/// Which MCP endpoint a request reached. It decides how a lost Workspace answers.
#[derive(Clone, Copy)]
enum Endpoint {
    /// `/workspaces/{workspace_id}/mcp`: the URL fixes the Workspace.
    Workspace,
    /// `/mcp`: the credential fixes the Workspace.
    Organization,
}

async fn mcp(State(state): State<Arc<HttpState>>, request: Request<Body>) -> Response {
    if let Some(refusal) = refuse_transport(&state, &request) {
        return refusal;
    }
    let Some(workspace_id) = parse_workspace_mcp_path(request.uri().path()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let resource = state.authentication.oauth_resource(&workspace_id);
    let Ok(bearer) = bearer_credential(request.headers()) else {
        return unauthorized(resource, AuthenticationError::Unauthenticated, false);
    };
    let principal = match state
        .authentication
        .authenticate(&workspace_id, bearer)
        .await
    {
        Ok(principal) => principal,
        Err(error) => return unauthorized(resource, error, bearer.is_some()),
    };
    serve(
        &state,
        request,
        workspace_id,
        principal,
        Endpoint::Workspace,
    )
    .await
}

async fn organization_mcp(State(state): State<Arc<HttpState>>, request: Request<Body>) -> Response {
    if let Some(refusal) = refuse_transport(&state, &request) {
        return refusal;
    }
    let resource = state.authentication.organization_oauth_resource();
    let Ok(bearer) = bearer_credential(request.headers()) else {
        return unauthorized(resource, AuthenticationError::Unauthenticated, false);
    };
    let (principal, workspace_id) = match state.authentication.authenticate_credential(bearer).await
    {
        Ok(authenticated) => authenticated,
        Err(error) => return unauthorized(resource, error, bearer.is_some()),
    };
    serve(
        &state,
        request,
        workspace_id,
        principal,
        Endpoint::Organization,
    )
    .await
}

/// Refuses requests no MCP endpoint serves, before any credential is read.
fn refuse_transport(state: &HttpState, request: &Request<Body>) -> Option<Response> {
    if state.config.cancellation_token.is_cancelled() {
        return Some(StatusCode::SERVICE_UNAVAILABLE.into_response());
    }
    if request.headers().contains_key(header::ORIGIN) {
        return Some(StatusCode::FORBIDDEN.into_response());
    }
    if request.uri().query().is_some() {
        return Some(StatusCode::NOT_FOUND.into_response());
    }
    None
}

/// Checks current authority in the Workspace an endpoint fixed and hands the
/// request to that Workspace's and Principal's MCP sessions.
async fn serve(
    state: &HttpState,
    mut request: Request<Body>,
    workspace_id: WorkspaceId,
    principal: AuthenticatedPrincipal,
    endpoint: Endpoint,
) -> Response {
    let Ok(now) = now() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    // At `/mcp` a lost Workspace answers 401 `invalid_token`, so the client
    // signs in again and its User picks another Workspace (ADR 0025).
    let lost = || match endpoint {
        Endpoint::Workspace => StatusCode::NOT_FOUND.into_response(),
        Endpoint::Organization => unauthorized(
            state.authentication.organization_oauth_resource(),
            AuthenticationError::Unauthenticated,
            true,
        ),
    };
    let evidence = match state
        .modules
        .workspaces
        .check_principal_inspection(&principal, &workspace_id, now)
        .await
    {
        Ok(evidence) => evidence,
        Err(
            coral_workspace::Error::WorkspaceNotFound | coral_workspace::Error::PermissionDenied(_),
        ) => return lost(),
        Err(_error) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    let workspace = evidence.workspace();
    if matches!(endpoint, Endpoint::Organization) && workspace.state() == WorkspaceState::Archived {
        return lost();
    }
    let mut handler = McpServer::new(
        McpRuntime {
            modules: state.modules.clone(),
            principal: principal.clone(),
        },
        workspace_id.clone(),
    );
    if matches!(endpoint, Endpoint::Organization) {
        handler = handler.naming_workspace(workspace.display_name().as_str());
    }
    // Authentication is complete. RMCP/session extensions need no credential
    // material; only the separately constructed canonical context reaches tools.
    request.headers_mut().remove(header::AUTHORIZATION);
    let sessions = state
        .session_manager(&workspace_id, principal.principal())
        .await;
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

/// A 401 whose challenge names the endpoint's protected-resource metadata.
/// `invalid_token` tells an OAuth client that its token no longer works there.
fn unauthorized(
    resource: Option<OauthProtectedResource>,
    error: AuthenticationError,
    invalid_token: bool,
) -> Response {
    if matches!(error, AuthenticationError::Unavailable) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let mut parameters = Vec::new();
    if invalid_token {
        parameters.push("error=\"invalid_token\"".to_owned());
    }
    if let Some(resource) = resource {
        parameters.push(format!("resource_metadata=\"{}\"", resource.metadata_url));
    }
    let challenge = if parameters.is_empty() {
        "Bearer".to_owned()
    } else {
        format!("Bearer {}", parameters.join(", "))
    };
    let mut response = (StatusCode::UNAUTHORIZED, error.to_string()).into_response();
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
    metadata_response(&resource)
}

async fn organization_metadata(State(state): State<Arc<HttpState>>) -> Response {
    state
        .authentication
        .organization_oauth_resource()
        .as_ref()
        .map_or_else(|| StatusCode::NOT_FOUND.into_response(), metadata_response)
}

fn metadata_response(resource: &OauthProtectedResource) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], serde_json::json!({"resource": resource.resource, "authorization_servers": [resource.authorization_server], "bearer_methods_supported": ["header"]}).to_string()).into_response()
}

fn is_loopback(ip: IpAddr) -> bool {
    ip.is_loopback()
        || matches!(ip, IpAddr::V6(ip) if ip.to_ipv4_mapped().is_some_and(|ip| ip.is_loopback()))
}
