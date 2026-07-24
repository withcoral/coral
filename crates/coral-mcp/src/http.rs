//! Loopback Streamable HTTP transport for Coral's MCP surface.
//!
//! [`start_auth_disabled`] is intentionally limited to loopback. It shares an
//! unauthenticated local [`coral_client::AppClient`] across sessions and is not
//! a safe construction path for a long-running, non-loopback server.

use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use coral_api::v1::{CatalogItemKind, ListCatalogRequest, PaginationRequest};
use coral_client::{AppClient, default_workspace};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::SessionManager,
    session::local::LocalSessionManager,
};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinHandle;
use tonic::Request as GrpcRequest;
use tower::ServiceExt;

use crate::{CoralMcpServerFactory, McpOptions};

const READINESS_TIMEOUT: Duration = Duration::from_secs(2);
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(1);

type ProbeFuture = Pin<Box<dyn Future<Output = Result<(), tonic::Code>> + Send>>;

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

/// Handle for a running auth-disabled MCP HTTP server.
///
/// Call [`RunningMcpHttpServer::shutdown`] for deterministic teardown. Dropping
/// this handle cancels active HTTP work and closes sessions asynchronously.
pub struct RunningMcpHttpServer {
    local_addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), io::Error>>,
    state: Arc<HttpState>,
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
) -> (Router, Arc<HttpState>) {
    let factory = CoralMcpServerFactory::new(app, options);
    let config =
        StreamableHttpServerConfig::default().with_allowed_hosts([advertised_ip.to_string()]);
    let sessions = Arc::new(LocalSessionManager::default());
    let state = Arc::new(HttpState {
        factory,
        sessions: sessions.clone(),
        config,
        readiness,
        requests: RwLock::new(()),
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
    sessions: Arc<LocalSessionManager>,
    config: StreamableHttpServerConfig,
    readiness: ReadinessProbe,
    requests: RwLock<()>,
}

async fn mcp(State(state): State<Arc<HttpState>>, request: Request<Body>) -> Response {
    let _request = state.requests.read().await;
    serve_mcp_request(
        request,
        state.factory.clone(),
        state.sessions.clone(),
        state.config.clone(),
    )
    .await
}

// The service wrapper is intentionally request-scoped. Auth-required routing
// can validate an initialize request and pass its bearer-bound factory here,
// while later requests reuse the handler already held by `sessions`.
async fn serve_mcp_request(
    request: Request<Body>,
    factory: CoralMcpServerFactory,
    sessions: Arc<LocalSessionManager>,
    config: StreamableHttpServerConfig,
) -> Response {
    let cancellation = config.cancellation_token.clone();
    let service = StreamableHttpService::new(move || Ok(factory.create()), sessions, config);
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
        () = state.config.cancellation_token.cancelled() => StatusCode::SERVICE_UNAVAILABLE,
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
