//! Builds and runs the Coral gRPC server.

use std::borrow::Cow;
use std::convert::Infallible;
use std::future::{Future, Ready};
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};

use axum::body::Body as AxumBody;
use axum::extract::Request as AxumRequest;
use axum::response::Response as AxumResponse;
use coral_api::v1::catalog_service_server::CatalogServiceServer;
use coral_api::v1::feedback_service_server::FeedbackServiceServer;
use coral_api::v1::function_service_server::FunctionServiceServer;
use coral_api::v1::query_service_server::QueryServiceServer;
use coral_api::v1::search_service_server::SearchServiceServer;
use coral_api::v1::source_service_server::SourceServiceServer;
use coral_api::v1::task_service_server::TaskServiceServer;
use coral_api::v1::trace_service_server::TraceServiceServer;
use coral_api::v1::workspace_service_server::WorkspaceServiceServer;
use coral_api::{
    CATALOG_RESPONSE_MAX_MESSAGE_SIZE, HTTP2_MAX_HEADER_LIST_SIZE, QUERY_RESPONSE_MAX_MESSAGE_SIZE,
    SEARCH_RESPONSE_MAX_MESSAGE_SIZE, SOURCE_RESPONSE_MAX_MESSAGE_SIZE,
    TRACE_RESPONSE_MAX_MESSAGE_SIZE,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{self, JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codegen::http::header::CONTENT_TYPE;
use tonic::codegen::http::{HeaderValue, Method, Request, Response, StatusCode};
use tonic::service::Routes;
use tonic::transport::Server;
use tonic_web::GrpcWebLayer;
use tower::{Layer, Service};

use super::env::AppEnvironment;
use super::error::AppError;
use super::health::AggregateHealthService;
use crate::EngineExtensionsProvider;
use crate::catalog::discovery::CatalogDiscovery;
use crate::catalog::service::CatalogService;
use crate::credentials::config::CredentialStorageConfig;
use crate::credentials::{CredentialManager, CredentialStore};
use crate::features::{Feature, FeatureOverrides, FeatureStore};
use crate::feedback::manager::FeedbackManager;
use crate::feedback::publisher::{
    FeedbackPublisher, HostedFeedbackPublisher, NoopFeedbackPublisher,
};
use crate::feedback::service::FeedbackService;
use crate::functions::service::FunctionService;
use crate::identity::{SingleUserPrincipalProvider, UserPrincipalProvider};
use crate::query::manager::QueryManager;
use crate::query::service::QueryService;
use crate::search::manager::SearchManager;
use crate::search::observed::SearchObservationHandle;
use crate::search::service::SearchService;
use crate::sources::manager::SourceManager;
use crate::sources::materialization::SourceDiagnosticReporter;
use crate::sources::service::SourceService;
use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig, run_state_migrations};
use crate::state::{AppStateLayout, ConfigStore};
use crate::task::manager::TaskManager;
use crate::task::service::TaskService;
use crate::task::store::JsonlTaskEventStore;
use crate::telemetry::TelemetryConfig;
use crate::telemetry::service::TraceService;
use crate::transport::GrpcRequestContextLayer;
use crate::workspaces::{WorkspaceLifecycleLock, WorkspaceManager, WorkspaceService};

/// A static asset (e.g., a built SPA file) served on the same port as
/// gRPC-Web.
pub struct StaticAsset {
    /// Raw bytes of the asset.
    pub bytes: Cow<'static, [u8]>,
    /// MIME type to surface as `Content-Type`.
    pub content_type: Cow<'static, str>,
}

/// Source of static assets served alongside gRPC-Web on a single port.
///
/// Coral itself is asset-agnostic: `coral-cli`'s `embedded-ui` feature
/// supplies an implementation backed by the built UI bundle.
pub trait StaticAssetsProvider: Send + Sync + 'static {
    /// Returns the asset stored at `path` (relative, no leading slash), or
    /// `None` if the asset does not exist.
    fn get(&self, path: &str) -> Option<StaticAsset>;
}

/// Server-side bootstrap configuration for the Coral server.
#[derive(Clone)]
pub(crate) struct ServerConfig {
    config_dir: Option<PathBuf>,
    mode: ServerMode,
    engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    user_principal_provider: Arc<dyn UserPrincipalProvider>,
    feedback_publisher: Arc<dyn FeedbackPublisher>,
    feature_overrides: FeatureOverrides,
    enable_stderr_logs: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerConfig {
    pub(crate) fn new() -> Self {
        Self {
            config_dir: None,
            mode: ServerMode::EphemeralGrpc,
            engine_extensions_providers: Vec::new(),
            user_principal_provider: Arc::new(SingleUserPrincipalProvider),
            feedback_publisher: Arc::new(HostedFeedbackPublisher::new()),
            feature_overrides: FeatureOverrides::default(),
            enable_stderr_logs: false,
        }
    }

    pub(crate) fn with_config_dir(mut self, config_dir: impl Into<PathBuf>) -> Self {
        self.config_dir = Some(config_dir.into());
        self
    }

    pub(crate) fn with_mode(mut self, mode: ServerMode) -> Self {
        self.mode = mode;
        self
    }

    pub(crate) fn add_engine_extensions_provider(
        mut self,
        engine_extensions_provider: Arc<dyn EngineExtensionsProvider>,
    ) -> Self {
        self.engine_extensions_providers
            .push(engine_extensions_provider);
        self
    }

    #[must_use]
    pub(crate) fn with_stderr_logs(mut self, enable_stderr_logs: bool) -> Self {
        self.enable_stderr_logs = enable_stderr_logs;
        self
    }

    pub(crate) fn with_feature_overrides(mut self, feature_overrides: FeatureOverrides) -> Self {
        self.feature_overrides = feature_overrides;
        self
    }
}

/// Concrete local server mode.
///
/// Each variant is a supported product mode instead of an independent
/// transport or asset-serving knob.
#[derive(Clone)]
pub enum ServerMode {
    /// Ephemeral native gRPC for CLI, MCP, and local client callers.
    EphemeralGrpc,
    /// Native gRPC bound to an explicit address for a standalone server.
    StandaloneGrpc {
        /// Address to bind.
        bind: SocketAddr,
    },
    /// Loopback gRPC-Web server that also serves embedded UI assets.
    EmbeddedUi {
        /// Port to bind on `127.0.0.1`.
        port: u16,
        /// Static UI assets served on the same origin as gRPC-Web.
        assets: Arc<dyn StaticAssetsProvider>,
    },
}

impl ServerMode {
    fn bind_addr(&self) -> SocketAddr {
        match self {
            Self::EphemeralGrpc => SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            Self::StandaloneGrpc { bind } => *bind,
            Self::EmbeddedUi { port, .. } => SocketAddr::from((Ipv4Addr::LOCALHOST, *port)),
        }
    }
}

/// Builder for the Coral server runtime.
#[derive(Clone, Default)]
pub struct ServerBuilder {
    config: ServerConfig,
}

impl ServerBuilder {
    #[must_use]
    /// Creates a builder for the default ephemeral native gRPC local server.
    pub fn new() -> Self {
        Self {
            config: ServerConfig::new(),
        }
    }

    #[must_use]
    /// Creates a builder for an ephemeral native gRPC local server.
    pub fn ephemeral_grpc() -> Self {
        Self::new().with_mode(ServerMode::EphemeralGrpc)
    }

    #[must_use]
    /// Creates a standalone native gRPC server bound to an explicit address.
    pub fn standalone_grpc(bind: SocketAddr) -> Self {
        Self::new().with_mode(ServerMode::StandaloneGrpc { bind })
    }

    #[must_use]
    /// Creates a builder for loopback gRPC-Web with embedded UI assets.
    ///
    /// Requests with native `application/grpc` content-types are rejected with
    /// HTTP 415. Requests for paths under registered gRPC services route to
    /// gRPC-Web; every other path is dispatched to the supplied
    /// [`StaticAssetsProvider`], with SPA fallback to `index.html` for
    /// unknown paths.
    pub fn embedded_ui_loopback(port: u16, assets: Arc<dyn StaticAssetsProvider>) -> Self {
        Self::new().with_mode(ServerMode::EmbeddedUi { port, assets })
    }

    #[must_use]
    /// Selects the local server mode.
    pub fn with_mode(mut self, mode: ServerMode) -> Self {
        self.config = self.config.with_mode(mode);
        self
    }

    #[must_use]
    /// Overrides the Coral config directory used by the local server.
    pub fn with_config_dir(mut self, config_dir: impl Into<PathBuf>) -> Self {
        self.config = self.config.with_config_dir(config_dir);
        self
    }

    #[must_use]
    /// Adds an engine extensions provider used for query runtime builds.
    ///
    /// Providers are evaluated in call order, so later providers can add or
    /// override engine extensions produced by earlier providers.
    pub fn add_engine_extensions_provider(
        mut self,
        engine_extensions_provider: Arc<dyn EngineExtensionsProvider>,
    ) -> Self {
        self.config = self
            .config
            .add_engine_extensions_provider(engine_extensions_provider);
        self
    }

    #[must_use]
    /// Sets the server-side user principal provider.
    ///
    /// The default provider returns the local single-user principal for every
    /// request. Product runtimes can authenticate inbound metadata and select a
    /// user by installing their own provider.
    pub fn with_user_principal_provider(
        mut self,
        user_principal_provider: Arc<dyn UserPrincipalProvider>,
    ) -> Self {
        self.config.user_principal_provider = user_principal_provider;
        self
    }

    #[must_use]
    /// Enables or disables local stderr log rendering for this server.
    ///
    /// `MCP` stdio adapters can enable this for diagnostics while keeping
    /// stdout reserved for protocol messages. Other command surfaces should
    /// leave it disabled and rely on OTEL export for logs.
    pub fn with_stderr_logs(mut self, enable_stderr_logs: bool) -> Self {
        self.config = self.config.with_stderr_logs(enable_stderr_logs);
        self
    }

    #[must_use]
    /// Applies process-local runtime feature overrides to this server instance.
    pub fn with_feature_overrides(mut self, feature_overrides: FeatureOverrides) -> Self {
        self.config = self.config.with_feature_overrides(feature_overrides);
        self
    }

    /// Disables hosted feedback upload for tests and controlled local harnesses.
    #[doc(hidden)]
    #[must_use]
    pub fn with_noop_feedback_uploads(mut self) -> Self {
        self.config.feedback_publisher = Arc::new(NoopFeedbackPublisher);
        self
    }

    /// Starts the Coral gRPC server on TCP.
    ///
    /// By default, Coral keeps a real local gRPC boundary here so the public
    /// client talks to the same typed transport contract the server exposes.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the config directory cannot be determined,
    /// required directories cannot be created, the config or credential backends
    /// fail to initialize, or the gRPC server cannot be started.
    pub async fn start(self) -> Result<RunningServer, AppError> {
        let env = AppEnvironment::discover();
        let layout = env.app_state_layout(self.config.config_dir)?;
        layout.ensure()?;
        let features = FeatureStore::from_layout(layout.clone())
            .load_with_overrides(&self.config.feature_overrides)?;
        let coral_db = init_database(&layout).await?;
        let config_store = ConfigStore::new(layout.clone());
        run_state_migrations(&coral_db, &config_store).await?;
        let coral_db = Arc::new(coral_db);
        let telemetry_config = TelemetryConfig::load(&layout)?;
        let internal_trace_store_dir = telemetry_config
            .trace_history
            .enabled
            .then(|| layout.local_trace_store_dir());
        let installed_trace_store = crate::telemetry::init_tracing(
            &telemetry_config,
            self.config.enable_stderr_logs,
            internal_trace_store_dir.clone(),
        )?;
        let active_trace_store = telemetry_config
            .trace_history
            .enabled
            .then_some(installed_trace_store)
            .flatten();
        let active_trace_store_dir = active_trace_store.as_ref().map(|store| store.dir.clone());
        let credential_config = CredentialStorageConfig::load(&layout)?;
        let credential_store =
            CredentialStore::with_preference(layout.clone(), credential_config.storage);
        let credential_manager = CredentialManager::new(credential_store);
        let workspace_lifecycle_lock = WorkspaceLifecycleLock::default();
        let diagnostic_reporter = SourceDiagnosticReporter::default();
        let source_manager = SourceManager::with_diagnostic_reporter(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
            workspace_lifecycle_lock.clone(),
            diagnostic_reporter.clone(),
        );
        let workspace_manager = WorkspaceManager::new(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
            active_trace_store_dir.clone(),
            workspace_lifecycle_lock.clone(),
            Arc::clone(&coral_db),
            diagnostic_reporter.clone(),
        );
        let feedback_manager =
            FeedbackManager::with_publisher(layout.clone(), self.config.feedback_publisher);
        let task_manager = TaskManager::new(Arc::new(JsonlTaskEventStore::new(layout.clone())));
        let body_capture_max_bytes = telemetry_config
            .trace_history
            .http_body_recording_max_bytes();
        let query_runtime_context = env
            .query_runtime_context()
            .with_body_capture_max_bytes(body_capture_max_bytes);

        let query_manager = QueryManager::with_diagnostic_reporter(
            config_store.clone(),
            workspace_manager.clone(),
            credential_manager,
            query_runtime_context,
            layout.clone(),
            workspace_lifecycle_lock.clone(),
            self.config.engine_extensions_providers,
            diagnostic_reporter.clone(),
        );
        let observed_values_search_enabled = features.enabled(Feature::ObservedValuesSearch);
        let search_observations =
            observed_values_search_enabled.then(|| SearchObservationHandle::new(layout.clone()));
        let search_manager = SearchManager::with_diagnostic_reporter(
            layout,
            &config_store,
            workspace_manager.clone(),
            observed_values_search_enabled,
            diagnostic_reporter,
            CatalogDiscovery::new(query_manager.clone()),
            workspace_lifecycle_lock,
        );
        let trace_components = trace_components_for_store(active_trace_store);
        start_server(
            ServerDependencies {
                source: source_manager,
                workspace: workspace_manager,
                query: query_manager,
                search: search_manager,
                search_observations,
                feedback: feedback_manager,
                task: task_manager,
            },
            trace_components,
            self.config.user_principal_provider,
            self.config.mode,
        )
        .await
    }
}

fn trace_components_for_store(
    active_trace_store: Option<crate::telemetry::InstalledLocalTraceStore>,
) -> TraceServerComponents {
    active_trace_store.map_or_else(TraceServerComponents::default, |store| {
        TraceServerComponents {
            local_trace_store_dir: Some(store.dir.clone()),
            service: Some(TraceService::new(store.dir, store.retention)),
        }
    })
}

async fn init_database(layout: &AppStateLayout) -> Result<CoralDb, AppError> {
    let database_config = resolve_database_config(layout)?;
    let coral_db = CoralDb::open(database_config).await?;
    coral_db.migrate().await?;
    Ok(coral_db)
}

fn resolve_database_config(layout: &AppStateLayout) -> Result<ResolvedDatabaseConfig, AppError> {
    match DatabaseConfig::load(layout)? {
        DatabaseConfig::Sqlite { path } => Ok(ResolvedDatabaseConfig::Sqlite { path }),
        DatabaseConfig::Postgres { url_env } => {
            let url = AppEnvironment::env_var(&url_env)
                .map_err(|_error| {
                    AppError::FailedPrecondition(format!(
                        "database backend 'postgres' requires environment variable `{url_env}` to contain valid UTF-8"
                    ))
                })?
                .ok_or_else(|| {
                    AppError::FailedPrecondition(format!(
                        "database backend 'postgres' requires environment variable `{url_env}`"
                    ))
                })?;
            Ok(ResolvedDatabaseConfig::Postgres { url })
        }
    }
}

/// Running Coral server.
///
/// Call [`RunningServer::shutdown`] for deterministic teardown. Dropping this
/// handle sends shutdown to the background task as a best-effort fallback, but
/// does not wait for the task to finish.
pub struct RunningServer {
    endpoint_uri: String,
    local_trace_store_dir: Option<PathBuf>,
    search: SearchManager,
    search_observations: Mutex<Option<SearchObservationHandle>>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    task: Mutex<Option<JoinHandle<Result<(), tonic::transport::Error>>>>,
}

impl RunningServer {
    #[must_use]
    /// Returns the endpoint URI for this server.
    ///
    /// This is part of the narrow sibling-facing bootstrap seam used by the
    /// thin local client and by integration tests that need explicit control
    /// over server configuration.
    pub fn endpoint_uri(&self) -> &str {
        &self.endpoint_uri
    }

    #[must_use]
    /// Returns the process-installed local trace store directory, when local
    /// trace history is enabled for this process.
    pub fn local_trace_store_dir(&self) -> Option<&std::path::Path> {
        self.local_trace_store_dir.as_deref()
    }

    /// Shuts the server down and waits for the background task to finish.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the server task fails while shutting down.
    pub async fn shutdown(self) -> Result<(), AppError> {
        self.shutdown_inner().await
    }

    async fn shutdown_inner(&self) -> Result<(), AppError> {
        if let Some(shutdown_tx) = self
            .shutdown_tx
            .lock()
            .expect("shutdown mutex poisoned")
            .take()
        {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "send error means the receiver is already dropped, which is fine during shutdown"
            )]
            let _ = shutdown_tx.send(());
        }

        let task = self.task.lock().expect("task mutex poisoned").take();
        let task_result = match task {
            Some(task) => match task.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(AppError::from(error)),
                Err(error) => Err(AppError::from(error)),
            },
            None => Ok(()),
        };
        let search_observations_result = self.shutdown_search_observations().await;
        task_result?;
        search_observations_result?;
        Ok(())
    }

    async fn shutdown_search_observations(&self) -> Result<(), AppError> {
        let search_observations = self
            .search_observations
            .lock()
            .expect("search observation mutex poisoned")
            .take();
        if let Some(search_observations) = search_observations {
            let shutdown_result =
                task::spawn_blocking(move || search_observations.shutdown()).await;
            drain_search_before_shutdown(self.search.clone()).await;
            shutdown_result??;
        }
        Ok(())
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self
            .shutdown_tx
            .lock()
            .expect("shutdown mutex poisoned")
            .take()
        {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "send error means the receiver is already dropped, which is fine during shutdown"
            )]
            let _ = shutdown_tx.send(());
        }
    }
}

#[derive(Default)]
struct TraceServerComponents {
    service: Option<TraceService>,
    local_trace_store_dir: Option<PathBuf>,
}

struct ServerDependencies {
    source: SourceManager,
    workspace: WorkspaceManager,
    query: QueryManager,
    search: SearchManager,
    search_observations: Option<SearchObservationHandle>,
    feedback: FeedbackManager,
    task: TaskManager,
}

async fn start_server(
    dependencies: ServerDependencies,
    trace_components: TraceServerComponents,
    user_principal_provider: Arc<dyn UserPrincipalProvider>,
    mode: ServerMode,
) -> Result<RunningServer, AppError> {
    let TraceServerComponents {
        service: trace_service,
        local_trace_store_dir,
    } = trace_components;
    let ServerDependencies {
        source,
        workspace,
        query,
        search,
        search_observations,
        feedback,
        task,
    } = dependencies;
    let (source, query) = match search_observations.as_ref() {
        Some(search_observations) => (
            source.with_search_observation_handle(search_observations.clone()),
            query.with_search_observation_handle(search_observations.clone()),
        ),
        None => (source, query),
    };
    let source_service = SourceService::new(source, query.clone(), workspace.clone());
    let workspace_service = WorkspaceService::new(workspace);
    let catalog_service = CatalogService::new(query.clone());
    let function_service = FunctionService::new(query.clone());
    let query_service = QueryService::new(query);
    let search_service = SearchService::new(search.clone());
    let feedback_service = FeedbackService::new(feedback);
    let task_service = TaskService::new(task);
    let mut application_routes = Routes::default()
        .add_service(
            SourceServiceServer::new(source_service)
                .max_encoding_message_size(SOURCE_RESPONSE_MAX_MESSAGE_SIZE),
        )
        .add_service(WorkspaceServiceServer::new(workspace_service))
        .add_service(
            CatalogServiceServer::new(catalog_service)
                .max_encoding_message_size(CATALOG_RESPONSE_MAX_MESSAGE_SIZE),
        )
        .add_service(FeedbackServiceServer::new(feedback_service))
        .add_service(FunctionServiceServer::new(function_service))
        .add_service(TaskServiceServer::new(task_service))
        .add_service(
            QueryServiceServer::new(query_service)
                .max_encoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE),
        )
        .add_service(
            SearchServiceServer::new(search_service)
                .max_encoding_message_size(SEARCH_RESPONSE_MAX_MESSAGE_SIZE),
        );
    if let Some(trace_service) = trace_service {
        application_routes = application_routes.add_service(
            TraceServiceServer::new(trace_service)
                .max_encoding_message_size(TRACE_RESPONSE_MAX_MESSAGE_SIZE),
        );
    }
    let routes = Routes::from(
        application_routes
            .into_axum_router()
            .layer(GrpcRequestContextLayer::new(user_principal_provider)),
    )
    // Process liveness must not depend on principal selection.
    .add_service(tonic_health::pb::health_server::HealthServer::new(
        AggregateHealthService,
    ));

    let listener = TcpListener::bind(mode.bind_addr()).await?;
    let endpoint_uri = format!("http://{}", listener.local_addr()?);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let task = match mode {
        ServerMode::EphemeralGrpc | ServerMode::StandaloneGrpc { .. } => {
            start_grpc_server(listener, shutdown_rx, routes)
        }
        ServerMode::EmbeddedUi { assets, .. } => {
            start_grpc_web_server(listener, shutdown_rx, routes, assets)
        }
    };

    Ok(RunningServer {
        endpoint_uri,
        local_trace_store_dir,
        search,
        search_observations: Mutex::new(search_observations),
        shutdown_tx: Mutex::new(Some(shutdown_tx)),
        task: Mutex::new(Some(task)),
    })
}

fn start_grpc_server(
    listener: TcpListener,
    shutdown_rx: oneshot::Receiver<()>,
    routes: Routes,
) -> JoinHandle<Result<(), tonic::transport::Error>> {
    tokio::spawn(async move {
        Server::builder()
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE)
            .add_routes(routes)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                drop(shutdown_rx.await);
            })
            .await
    })
}

fn start_grpc_web_server(
    listener: TcpListener,
    shutdown_rx: oneshot::Receiver<()>,
    routes: Routes,
    static_assets: Arc<dyn StaticAssetsProvider>,
) -> JoinHandle<Result<(), tonic::transport::Error>> {
    let grpc = routes
        .into_axum_router()
        .layer(GrpcWebLayer::new())
        .layer(GrpcWebOnlyLayer);

    let app = grpc.fallback_service(StaticAssetService {
        provider: static_assets,
    });

    let combined: Routes = app.into();

    tokio::spawn(async move {
        Server::builder()
            .accept_http1(true)
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE)
            .add_routes(combined)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                drop(shutdown_rx.await);
            })
            .await
    })
}

async fn drain_search_before_shutdown(search: SearchManager) {
    if let Err(error) = search.drain_before_shutdown().await {
        tracing::debug!(
            error = ?error,
            "failed to prepare search state before shutdown"
        );
    }
}

#[derive(Clone, Copy)]
struct GrpcWebOnlyLayer;

impl<S> Layer<S> for GrpcWebOnlyLayer {
    type Service = GrpcWebOnlyService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcWebOnlyService { inner }
    }
}

#[derive(Clone)]
struct GrpcWebOnlyService<S> {
    inner: S,
}

impl<S, ReqB, ResB> Service<Request<ReqB>> for GrpcWebOnlyService<S>
where
    S: Service<Request<ReqB>, Response = Response<ResB>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ResB: Default,
{
    type Response = Response<ResB>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqB>) -> Self::Future {
        if is_native_grpc_content_type(request.headers().get(CONTENT_TYPE)) {
            return Box::pin(async {
                Ok(Response::builder()
                    .status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
                    .body(ResB::default())
                    .expect("static response is valid"))
            });
        }

        let future = self.inner.call(request);
        Box::pin(future)
    }
}

fn normalized_content_type(content_type: Option<&HeaderValue>) -> Option<String> {
    Some(
        content_type?
            .to_str()
            .ok()?
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase(),
    )
}

fn is_native_grpc_content_type(content_type: Option<&HeaderValue>) -> bool {
    let Some(content_type) = normalized_content_type(content_type) else {
        return false;
    };
    content_type == "application/grpc" || content_type.starts_with("application/grpc+")
}

fn is_grpc_web_content_type(content_type: Option<&HeaderValue>) -> bool {
    let Some(content_type) = normalized_content_type(content_type) else {
        return false;
    };
    content_type == "application/grpc-web" || content_type.starts_with("application/grpc-web+")
}

fn is_grpc_content_type(content_type: Option<&HeaderValue>) -> bool {
    is_native_grpc_content_type(content_type) || is_grpc_web_content_type(content_type)
}

#[derive(Clone)]
struct StaticAssetService {
    provider: Arc<dyn StaticAssetsProvider>,
}

impl Service<AxumRequest> for StaticAssetService {
    type Response = AxumResponse;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: AxumRequest) -> Self::Future {
        if is_grpc_content_type(request.headers().get(CONTENT_TYPE)) {
            return std::future::ready(Ok(static_fallback_error_response(
                StatusCode::NOT_FOUND,
                "Not Found",
            )));
        }
        if request.method() != Method::GET && request.method() != Method::HEAD {
            return std::future::ready(Ok(static_fallback_error_response(
                StatusCode::METHOD_NOT_ALLOWED,
                "Method Not Allowed",
            )));
        }

        let path = request.uri().path();
        let key = path.trim_start_matches('/');
        let asset = self
            .provider
            .get(key)
            .or_else(|| self.provider.get("index.html"));
        let response = match asset {
            Some(asset) => {
                let content_type = HeaderValue::from_str(&asset.content_type)
                    .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));
                let mut builder = AxumResponse::builder().status(StatusCode::OK);
                builder
                    .headers_mut()
                    .expect("fresh response builder")
                    .insert(CONTENT_TYPE, content_type);
                builder
                    .body(AxumBody::from(asset.bytes.into_owned()))
                    .expect("static response is valid")
            }
            None => static_fallback_error_response(StatusCode::NOT_FOUND, "Not Found"),
        };
        std::future::ready(Ok(response))
    }
}

fn static_fallback_error_response(status: StatusCode, body: &'static str) -> AxumResponse {
    AxumResponse::builder()
        .status(status)
        .header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        )
        .body(AxumBody::from(body))
        .expect("static response is valid")
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "JSON row assertions intentionally fail loudly in tests"
    )]

    use std::borrow::Cow;
    use std::net::{Ipv4Addr, SocketAddr, TcpListener};
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use coral_api::v1::query_service_client::QueryServiceClient;
    use coral_api::v1::source_service_client::SourceServiceClient;
    use coral_api::v1::task_service_client::TaskServiceClient;
    use coral_api::v1::trace_service_client::TraceServiceClient;
    use coral_api::v1::{
        EndTaskRequest, ExecuteSqlRequest, ImportSourceRequest, ImportSourceResponse,
        ListSourcesRequest, ListTracesRequest, StartTaskRequest, TaskStatus, Workspace,
        import_source_response,
    };
    use coral_api::{HTTP2_MAX_HEADER_LIST_SIZE, QUERY_RESPONSE_MAX_MESSAGE_SIZE};
    use coral_engine::QueryRuntimeContext;
    use tempfile::TempDir;
    use tonic::transport::Endpoint;
    use tonic::{Code, Request};

    use super::{
        RunningServer, ServerBuilder, ServerDependencies, ServerMode, StaticAsset,
        StaticAssetsProvider, TraceServerComponents, is_grpc_web_content_type,
        is_native_grpc_content_type, start_server,
    };
    use crate::bootstrap::AppError;
    use crate::catalog::discovery::CatalogDiscovery;
    use crate::credentials::{CredentialManager, CredentialStore};
    use crate::features::{Feature, FeatureOverrides};
    use crate::feedback::manager::FeedbackManager;
    use crate::query::manager::QueryManager;
    use crate::search::manager::SearchManager;
    use crate::search::observed::{
        ObservedValuesQueueJob, ObservedValuesSurfaceKind, SearchObservationHandle,
        SqliteObservedValuesStore,
    };
    use crate::sources::manager::SourceManager;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig, run_state_migrations};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::task::manager::TaskManager;
    use crate::task::store::JsonlTaskEventStore;
    use crate::telemetry::service::TraceService;
    use crate::transport::workspace_to_proto;
    use crate::workspaces::{WorkspaceManager, WorkspaceName};
    use crate::{
        AwsEngineExtensionsProvider, NoopEngineExtensionsProvider, SingleUserPrincipalProvider,
        UserPrincipal, UserPrincipalProvider, UserPrincipalProviderError,
    };

    fn default_workspace() -> Workspace {
        workspace_to_proto(&WorkspaceName::default())
    }

    fn disable_internal_tracing(config_dir: &Path) {
        std::fs::create_dir_all(config_dir).expect("create config dir");
        std::fs::write(
            config_dir.join("config.toml"),
            r"
version = 1

[trace_history]
enabled = false
",
        )
        .expect("write telemetry config");
    }

    fn configure_observed_values_search(config_dir: &Path, enabled: bool) {
        std::fs::create_dir_all(config_dir).expect("create config dir");
        std::fs::write(
            config_dir.join("config.toml"),
            format!(
                r"
version = 1

[features]
observed_values_search = {enabled}

[trace_history]
enabled = false
"
            ),
        )
        .expect("write feature config");
    }

    fn has_search_observation_handle(server: &RunningServer) -> bool {
        server
            .search_observations
            .lock()
            .expect("search observation mutex")
            .is_some()
    }

    #[derive(Debug)]
    struct RejectingUserPrincipalProvider;

    #[tonic::async_trait]
    impl UserPrincipalProvider for RejectingUserPrincipalProvider {
        async fn principal_for_metadata(
            &self,
            _metadata: &tonic::metadata::MetadataMap,
        ) -> Result<UserPrincipal, UserPrincipalProviderError> {
            Err(UserPrincipalProviderError::unauthenticated(
                "rejected user principal",
            ))
        }
    }

    async fn test_db(layout: &AppStateLayout, config_store: &ConfigStore) -> Arc<CoralDb> {
        let config = DatabaseConfig::load(layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        run_state_migrations(&db, config_store)
            .await
            .expect("run state migrations");
        Arc::new(db)
    }

    #[tokio::test]
    async fn shutdown_attempts_observed_values_shutdown_after_server_task_error() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let db = test_db(&layout, &config_store).await;
        let workspace_manager = WorkspaceManager::new_for_tests(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
            None,
            db,
        );
        let query_manager = QueryManager::new_for_tests(
            config_store.clone(),
            workspace_manager.clone(),
            credential_manager,
            QueryRuntimeContext::default(),
            layout.clone(),
            vec![Arc::new(NoopEngineExtensionsProvider)],
        );
        let lifecycle_lock = workspace_manager.lifecycle_lock();
        let search = SearchManager::new(
            layout.clone(),
            &config_store,
            workspace_manager,
            true,
            CatalogDiscovery::new(query_manager),
            lifecycle_lock,
        );
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let generation = store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        store
            .enqueue_if_current(
                &workspace,
                &ObservedValuesQueueJob {
                    owner_source_name: "github".to_string(),
                    source_name: "github".to_string(),
                    source_scope_id: "scope".to_string(),
                    surface_kind: ObservedValuesSurfaceKind::Table,
                    surface_name: "issues".to_string(),
                    payload_json: r#"{"values":[{"column_name":"title","display_value":"Payment outage","search_text":"payment outage","value_key":"payment-outage"}]}"#
                        .to_string(),
                },
                generation,
            )
            .expect("enqueue observed value");
        let search_observations = SearchObservationHandle::new(layout);
        let task = tokio::spawn(async {
            let should_panic = true;
            assert!(!should_panic, "server task panicked");
            Ok::<(), tonic::transport::Error>(())
        });
        let server = RunningServer {
            endpoint_uri: "http://127.0.0.1:0".to_string(),
            local_trace_store_dir: None,
            search,
            search_observations: Mutex::new(Some(search_observations)),
            shutdown_tx: Mutex::new(None),
            task: Mutex::new(Some(task)),
        };

        let result = server.shutdown_inner().await;

        assert!(matches!(result, Err(AppError::TaskJoin(_))));
        assert!(
            server
                .search_observations
                .lock()
                .expect("search observation mutex")
                .is_none()
        );
        assert_eq!(
            store
                .projected_value_count(&workspace)
                .expect("projected value count"),
            1
        );
        assert_eq!(
            store
                .pending_queue_job_count(&workspace)
                .expect("pending queue depth"),
            0
        );
    }

    #[tokio::test]
    async fn standalone_grpc_binds_the_requested_loopback_address() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        disable_internal_tracing(&config_dir);
        let server = ServerBuilder::standalone_grpc(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .with_config_dir(config_dir)
            .start()
            .await
            .expect("start explicit loopback server");

        assert!(server.endpoint_uri().starts_with("http://127.0.0.1:"));
        server.shutdown().await.expect("shutdown server");
    }

    #[tokio::test]
    async fn standalone_grpc_binds_the_requested_non_loopback_address() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        disable_internal_tracing(&config_dir);
        let server = ServerBuilder::standalone_grpc(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
            .with_config_dir(config_dir)
            .start()
            .await
            .expect("start explicit non-loopback server");

        assert!(server.endpoint_uri().starts_with("http://0.0.0.0:"));
        server.shutdown().await.expect("shutdown server");
    }

    #[tokio::test]
    async fn server_builder_leaves_observation_handle_detached_by_default() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        disable_internal_tracing(&config_dir);

        let server = ServerBuilder::new()
            .with_config_dir(config_dir)
            .start()
            .await
            .expect("start server");

        assert!(!has_search_observation_handle(&server));
        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn server_builder_attaches_observation_handle_when_config_enabled() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        configure_observed_values_search(&config_dir, true);

        let server = ServerBuilder::new()
            .with_config_dir(config_dir)
            .start()
            .await
            .expect("start server");

        assert!(has_search_observation_handle(&server));
        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn server_builder_process_overrides_control_observation_handle() {
        let temp = TempDir::new().expect("temp dir");
        let disabled_config_dir = temp.path().join("disabled-config");
        configure_observed_values_search(&disabled_config_dir, false);
        let mut enable_override = FeatureOverrides::default();
        enable_override.set(Feature::ObservedValuesSearch, true);

        let enabled_server = ServerBuilder::new()
            .with_config_dir(disabled_config_dir)
            .with_feature_overrides(enable_override)
            .start()
            .await
            .expect("start process-enabled server");

        assert!(has_search_observation_handle(&enabled_server));
        enabled_server.shutdown().await.expect("shutdown");

        let enabled_config_dir = temp.path().join("enabled-config");
        configure_observed_values_search(&enabled_config_dir, true);
        let mut disable_override = FeatureOverrides::default();
        disable_override.set(Feature::ObservedValuesSearch, false);

        let disabled_server = ServerBuilder::new()
            .with_config_dir(enabled_config_dir)
            .with_feature_overrides(disable_override)
            .start()
            .await
            .expect("start process-disabled server");

        assert!(!has_search_observation_handle(&disabled_server));
        disabled_server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn trace_service_is_unregistered_when_local_store_is_disabled() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        disable_internal_tracing(&config_dir);
        let server = ServerBuilder::new()
            .with_config_dir(config_dir)
            .start()
            .await
            .expect("start server");
        let channel = Endpoint::from_shared(server.endpoint_uri().to_string())
            .expect("endpoint")
            .connect()
            .await
            .expect("connect");
        let mut trace_client = TraceServiceClient::new(channel);

        let status = trace_client
            .list_traces(Request::new(ListTracesRequest {
                page_size: 10,
                page_token: String::new(),
                workspace: None,
            }))
            .await
            .expect_err("trace service should be disabled");

        assert_eq!(status.code(), Code::Unimplemented);
        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn database_config_failure_aborts_startup_after_cutover() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        std::fs::write(
            config_dir.join("config.toml"),
            r#"
version = 1

[trace_history]
enabled = false

[database]
backend = "unsupported"
"#,
        )
        .expect("write config");

        let result = ServerBuilder::new()
            .with_config_dir(config_dir)
            .start()
            .await;

        assert!(
            result.is_err(),
            "unsupported database config should abort startup after database cutover"
        );
    }

    #[tokio::test]
    async fn task_lifecycle_through_server_persists() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        disable_internal_tracing(&config_dir);
        let server = ServerBuilder::new()
            .with_config_dir(config_dir.clone())
            .start()
            .await
            .expect("start server");
        let channel = Endpoint::from_shared(server.endpoint_uri().to_string())
            .expect("endpoint")
            .connect()
            .await
            .expect("connect");
        let mut task_client = TaskServiceClient::new(channel);

        let task = task_client
            .start_task(Request::new(StartTaskRequest {
                workspace: Some(default_workspace()),
                intent: "find the HR onboarding form".to_string(),
            }))
            .await
            .expect("start task")
            .into_inner()
            .task
            .expect("task");
        uuid::Uuid::parse_str(&task.task_id).expect("task id is a UUID");

        let task_end = task_client
            .end_task(Request::new(EndTaskRequest {
                workspace: Some(default_workspace()),
                task_id: task.task_id.clone(),
                task_status: TaskStatus::Success as i32,
            }))
            .await
            .expect("end task")
            .into_inner()
            .task_end
            .expect("task end");
        assert_eq!(task_end.task_id, task.task_id);
        assert_eq!(task_end.task_status, TaskStatus::Success as i32);

        let layout = AppStateLayout::discover(Some(config_dir)).expect("layout");
        let workspace = WorkspaceName::default();
        let tasks =
            std::fs::read_to_string(layout.task_events_file(&workspace)).expect("task events file");
        assert!(tasks.contains(&task.task_id));
        assert!(
            tasks.contains("find the HR onboarding form"),
            "task events should contain start intent, got: {tasks}"
        );
        assert!(
            tasks.contains("success"),
            "task events should contain end status, got: {tasks}"
        );
        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn shutdown_drains_observed_values_queue() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        configure_observed_values_search(&config_dir, true);
        let layout = AppStateLayout::discover(Some(config_dir.clone())).expect("layout");
        layout.ensure().expect("layout dirs");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout);
        let generation = store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        store
            .enqueue_if_current(
                &workspace,
                &ObservedValuesQueueJob {
                    owner_source_name: "github".to_string(),
                    source_name: "github".to_string(),
                    source_scope_id: "scope".to_string(),
                    surface_kind: ObservedValuesSurfaceKind::Table,
                    surface_name: "issues".to_string(),
                    payload_json: r#"{"values":[{"column_name":"title","display_value":"Payment outage","search_text":"payment outage","value_key":"payment-outage"}]}"#
                        .to_string(),
                },
                generation,
            )
            .expect("enqueue observed value");

        let server = ServerBuilder::new()
            .with_config_dir(config_dir)
            .start()
            .await
            .expect("start server");
        server.shutdown().await.expect("shutdown");

        assert_eq!(
            store
                .projected_value_count(&workspace)
                .expect("projected value count"),
            1
        );
        assert_eq!(
            store
                .pending_queue_job_count(&workspace)
                .expect("pending queue depth"),
            0
        );
    }

    #[tokio::test]
    async fn shutdown_leaves_observed_values_queue_untouched_when_feature_is_disabled() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        disable_internal_tracing(&config_dir);
        let layout = AppStateLayout::discover(Some(config_dir.clone())).expect("layout");
        layout.ensure().expect("layout dirs");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout);
        let generation = store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        store
            .enqueue_if_current(
                &workspace,
                &ObservedValuesQueueJob {
                    owner_source_name: "github".to_string(),
                    source_name: "github".to_string(),
                    source_scope_id: "scope".to_string(),
                    surface_kind: ObservedValuesSurfaceKind::Table,
                    surface_name: "issues".to_string(),
                    payload_json: r#"{"values":[{"column_name":"title","display_value":"Payment outage","search_text":"payment outage","value_key":"payment-outage"}]}"#
                        .to_string(),
                },
                generation,
            )
            .expect("enqueue observed value");

        let server = ServerBuilder::new()
            .with_config_dir(config_dir)
            .start()
            .await
            .expect("start server");
        server.shutdown().await.expect("shutdown");

        assert_eq!(
            store
                .projected_value_count(&workspace)
                .expect("projected value count"),
            0
        );
        assert_eq!(
            store
                .pending_queue_job_count(&workspace)
                .expect("pending queue depth"),
            1
        );
    }

    #[tokio::test]
    async fn trace_service_lists_empty_store() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        let layout = AppStateLayout::discover(Some(config_dir)).expect("layout");
        layout.ensure().expect("layout dirs");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let db = test_db(&layout, &config_store).await;
        let source_manager = SourceManager::new_for_tests(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
        );
        let feedback_manager = FeedbackManager::new(layout.clone());
        let task_manager = TaskManager::new(Arc::new(JsonlTaskEventStore::new(layout.clone())));
        let workspace_manager = WorkspaceManager::new_for_tests(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
            None,
            Arc::clone(&db),
        );
        let query_manager = QueryManager::new_for_tests(
            config_store.clone(),
            workspace_manager.clone(),
            credential_manager,
            QueryRuntimeContext::default(),
            layout.clone(),
            vec![Arc::new(NoopEngineExtensionsProvider)],
        );
        let search_observations = SearchObservationHandle::new(layout.clone());
        let lifecycle_lock = workspace_manager.lifecycle_lock();
        let search_manager = SearchManager::new(
            layout.clone(),
            &config_store,
            workspace_manager.clone(),
            true,
            CatalogDiscovery::new(query_manager.clone()),
            lifecycle_lock,
        );
        let trace_service =
            TraceService::new(temp.path().join("trace-store"), Duration::from_mins(1));
        let server = start_server(
            ServerDependencies {
                source: source_manager,
                workspace: workspace_manager,
                query: query_manager,
                search: search_manager,
                search_observations: Some(search_observations),
                feedback: feedback_manager,
                task: task_manager,
            },
            TraceServerComponents {
                service: Some(trace_service),
                local_trace_store_dir: None,
            },
            Arc::new(SingleUserPrincipalProvider),
            ServerMode::EphemeralGrpc,
        )
        .await
        .expect("start server");
        let channel = Endpoint::from_shared(server.endpoint_uri().to_string())
            .expect("endpoint")
            .connect()
            .await
            .expect("connect");
        let mut trace_client = TraceServiceClient::new(channel);

        let response = trace_client
            .list_traces(Request::new(ListTracesRequest {
                page_size: 10,
                page_token: String::new(),
                workspace: None,
            }))
            .await
            .expect("list traces")
            .into_inner();

        assert!(response.traces.is_empty());
        assert!(response.next_page_token.is_empty());
        server.shutdown().await.expect("shutdown");
    }

    fn grpc_web_body(message: &impl prost::Message) -> Vec<u8> {
        let mut encoded = Vec::new();
        prost::Message::encode(message, &mut encoded).expect("encode protobuf");

        let mut body = Vec::with_capacity(5 + encoded.len());
        body.push(0);
        body.extend_from_slice(
            &u32::try_from(encoded.len())
                .expect("fixture protobuf length fits u32")
                .to_be_bytes(),
        );
        body.extend_from_slice(&encoded);
        body
    }

    struct StubAssets;

    impl StaticAssetsProvider for StubAssets {
        fn get(&self, path: &str) -> Option<StaticAsset> {
            if path.is_empty() || path == "index.html" {
                Some(StaticAsset {
                    bytes: Cow::Borrowed(b"<html><body>Coral UI</body></html>"),
                    content_type: Cow::Borrowed("text/html; charset=utf-8"),
                })
            } else if path == "assets/app.js" {
                Some(StaticAsset {
                    bytes: Cow::Borrowed(b"console.log('coral')"),
                    content_type: Cow::Borrowed("application/javascript"),
                })
            } else {
                None
            }
        }
    }

    #[test]
    fn server_builder_accepts_engine_extensions_providers() {
        let _builder = ServerBuilder::new()
            .add_engine_extensions_provider(Arc::new(AwsEngineExtensionsProvider))
            .add_engine_extensions_provider(Arc::new(NoopEngineExtensionsProvider));
    }

    #[tokio::test]
    async fn server_builder_applies_injected_provider_to_ephemeral_grpc() {
        let temp = TempDir::new().expect("temp dir");
        let server = ServerBuilder::new()
            .with_config_dir(temp.path().join("coral-config"))
            .with_user_principal_provider(Arc::new(RejectingUserPrincipalProvider))
            .start()
            .await
            .expect("start server");
        let channel = Endpoint::from_shared(server.endpoint_uri().to_string())
            .expect("endpoint")
            .connect()
            .await
            .expect("connect");

        let status = SourceServiceClient::new(channel)
            .list_sources(Request::new(ListSourcesRequest {
                workspace: Some(default_workspace()),
            }))
            .await
            .expect_err("request should be rejected");

        assert_eq!(status.code(), Code::Unauthenticated);
        server.shutdown().await.expect("shutdown");
    }

    #[test]
    fn native_grpc_content_type_detection_excludes_grpc_web() {
        assert!(is_native_grpc_content_type(Some(
            &"application/grpc".parse().expect("header")
        )));
        assert!(is_native_grpc_content_type(Some(
            &"application/grpc+proto; charset=utf-8"
                .parse()
                .expect("header")
        )));
        assert!(!is_native_grpc_content_type(Some(
            &"application/grpc-web+proto".parse().expect("header")
        )));
    }

    #[test]
    fn grpc_web_content_type_detection_accepts_grpc_web() {
        assert!(is_grpc_web_content_type(Some(
            &"application/grpc-web".parse().expect("header")
        )));
        assert!(is_grpc_web_content_type(Some(
            &"application/grpc-web+proto; charset=utf-8"
                .parse()
                .expect("header")
        )));
        assert!(!is_grpc_web_content_type(Some(
            &"application/grpc+proto".parse().expect("header")
        )));
    }

    #[tokio::test]
    async fn embedded_ui_server_accepts_browser_requests_and_rejects_native_grpc() {
        let temp = TempDir::new().expect("temp dir");
        let running = ServerBuilder::embedded_ui_loopback(0, Arc::new(StubAssets))
            .with_config_dir(temp.path().join("coral-config"))
            .start()
            .await
            .expect("start embedded UI server");
        let endpoint = running.endpoint_uri();
        let path = format!("{endpoint}/coral.v1.SourceService/ListSources");
        let client = reqwest::Client::new();

        let response = client
            .post(&path)
            .header("content-type", "application/grpc-web+proto")
            .header("x-grpc-web", "1")
            .body(grpc_web_body(&ListSourcesRequest {
                workspace: Some(default_workspace()),
            }))
            .send()
            .await
            .expect("gRPC-Web request");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert!(
            !response
                .bytes()
                .await
                .expect("gRPC-Web response")
                .is_empty(),
            "expected framed gRPC-Web response body"
        );

        let native_grpc = client
            .post(&path)
            .header("content-type", "application/grpc")
            .body(Vec::new())
            .send()
            .await
            .expect("native gRPC request");
        assert_eq!(
            native_grpc.status(),
            reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE
        );

        running.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn embedded_ui_server_streams_import_source_over_grpc_web() {
        let temp = TempDir::new().expect("temp dir");
        let running = ServerBuilder::embedded_ui_loopback(0, Arc::new(StubAssets))
            .with_config_dir(temp.path().join("coral-config"))
            .start()
            .await
            .expect("start embedded UI server");
        let endpoint = running.endpoint_uri();
        let path = format!("{endpoint}/coral.v1.SourceService/ImportSource");
        let client = reqwest::Client::new();

        let response = client
            .post(&path)
            .header("content-type", "application/grpc-web+proto")
            .header("x-grpc-web", "1")
            .body(grpc_web_body(&ImportSourceRequest {
                workspace: Some(default_workspace()),
                manifest_yaml: r#"
name: stream_test
version: 0.1.0
dsl_version: 3
backend: http
base_url: "https://example.com"
tables:
  - name: messages
    description: Messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#
                .to_string(),
                variables: Vec::new(),
                secrets: Vec::new(),
                oauth_credential_retrievals: Vec::new(),
            }))
            .send()
            .await
            .expect("gRPC-Web streaming request");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body = response.bytes().await.expect("gRPC-Web streaming body");
        let body = body.as_ref();
        assert!(body.len() >= 5, "expected framed gRPC-Web response body");
        assert_eq!(body[0], 0, "expected first frame to be a data frame");
        let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
        let frame = body.get(5..5 + len).expect("complete gRPC-Web data frame");
        let trailer_offset = 5 + len;
        assert!(
            body.len() >= trailer_offset + 5,
            "expected final gRPC-Web trailer frame"
        );
        assert_eq!(
            body[trailer_offset], 0x80,
            "expected final frame to be uncompressed trailers"
        );
        let trailer_len = u32::from_be_bytes([
            body[trailer_offset + 1],
            body[trailer_offset + 2],
            body[trailer_offset + 3],
            body[trailer_offset + 4],
        ]) as usize;
        let trailer_end = trailer_offset + 5 + trailer_len;
        let trailers = body
            .get(trailer_offset + 5..trailer_end)
            .expect("complete gRPC-Web trailer frame");
        assert_eq!(
            body.len(),
            trailer_end,
            "expected trailers to be the final gRPC-Web frame"
        );
        let trailers = std::str::from_utf8(trailers).expect("trailers are UTF-8");
        assert!(
            trailers.lines().any(|line| {
                line.strip_prefix("grpc-status:")
                    .is_some_and(|status| status.trim() == "0")
            }),
            "expected successful gRPC-Web trailer status, got {trailers:?}"
        );
        let event = <ImportSourceResponse as prost::Message>::decode(frame)
            .expect("decode import source response")
            .event
            .expect("stream event");
        match event {
            import_source_response::Event::Source(source) => {
                assert_eq!(source.name, "stream_test");
            }
            other => panic!("unexpected stream event: {other:?}"),
        }

        running.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn embedded_ui_authenticates_grpc_web_without_gating_static_assets() {
        let temp = TempDir::new().expect("temp dir");
        let running = ServerBuilder::embedded_ui_loopback(0, Arc::new(StubAssets))
            .with_config_dir(temp.path().join("coral-config"))
            .with_user_principal_provider(Arc::new(RejectingUserPrincipalProvider))
            .start()
            .await
            .expect("start embedded UI server");
        let endpoint = running.endpoint_uri().to_string();
        let client = reqwest::Client::new();

        // Root serves index.html
        let root = client.get(&endpoint).send().await.expect("root request");
        assert_eq!(root.status(), reqwest::StatusCode::OK);
        assert_eq!(
            root.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("text/html; charset=utf-8")
        );
        let body = root.text().await.expect("root body");
        assert!(body.contains("Coral UI"), "unexpected body: {body}");

        // Asset path serves the asset
        let asset = client
            .get(format!("{endpoint}/assets/app.js"))
            .send()
            .await
            .expect("asset request");
        assert_eq!(asset.status(), reqwest::StatusCode::OK);
        assert_eq!(
            asset
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/javascript")
        );

        // Unknown path falls back to index.html (SPA fallback).
        let route = client
            .get(format!("{endpoint}/some/spa/route"))
            .send()
            .await
            .expect("spa route request");
        assert_eq!(route.status(), reqwest::StatusCode::OK);
        assert_eq!(
            route
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("text/html; charset=utf-8")
        );

        // Registered gRPC-Web routes still pass through the principal gate.
        let grpc_path = format!("{endpoint}/coral.v1.SourceService/ListSources");
        let response = client
            .post(&grpc_path)
            .header("content-type", "application/grpc-web+proto")
            .header("x-grpc-web", "1")
            .body(grpc_web_body(&ListSourcesRequest {
                workspace: Some(default_workspace()),
            }))
            .send()
            .await
            .expect("gRPC-Web request");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let grpc_status = response
            .headers()
            .get("grpc-status")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        let body = String::from_utf8_lossy(&response.bytes().await.expect("gRPC-Web response"))
            .into_owned();
        assert!(
            grpc_status.as_deref() == Some("16")
                || body.contains("grpc-status: 16")
                || body.contains("grpc-status:16"),
            "expected unauthenticated gRPC-Web status, got header {grpc_status:?} and body {body:?}"
        );

        let unknown_grpc = client
            .post(format!("{endpoint}/unknown.Service/Method"))
            .header("content-type", "application/grpc-web+proto")
            .header("x-grpc-web", "1")
            .body(grpc_web_body(&ListSourcesRequest {
                workspace: Some(default_workspace()),
            }))
            .send()
            .await
            .expect("unknown gRPC-Web request");
        assert_eq!(unknown_grpc.status(), reqwest::StatusCode::NOT_FOUND);
        assert_eq!(
            unknown_grpc
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("text/plain; charset=utf-8")
        );
        let unknown_body = unknown_grpc.text().await.expect("unknown body");
        assert_eq!(unknown_body, "Not Found");

        running.shutdown().await.expect("shutdown");
    }

    fn loopback_sockets_available() -> bool {
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).is_ok()
    }

    #[tokio::test]
    async fn file_tilde_sources_resolve_from_app_owned_runtime_context() {
        if !loopback_sockets_available() {
            return;
        }

        let temp = TempDir::new().expect("temp dir");
        let fake_home = temp.path().join("fake-home");
        let config_dir = temp.path().join("coral-config");
        let data_dir = fake_home.join("fixture-data");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        std::fs::write(
            data_dir.join("messages.jsonl"),
            r#"{"type":"user","text":"hello"}
{"type":"assistant","text":"world"}
"#,
        )
        .expect("write fixture");

        let layout = AppStateLayout::discover(Some(config_dir.clone())).expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let db = test_db(&layout, &config_store).await;
        let source_manager = SourceManager::new_for_tests(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
        );
        let feedback_manager = FeedbackManager::new(layout.clone());
        let task_manager = TaskManager::new(Arc::new(JsonlTaskEventStore::new(layout.clone())));
        let workspace_manager = WorkspaceManager::new_for_tests(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
            None,
            Arc::clone(&db),
        );
        let query_manager = QueryManager::new_for_tests(
            config_store.clone(),
            workspace_manager.clone(),
            credential_manager,
            QueryRuntimeContext {
                home_dir: Some(fake_home.clone()),
                ..QueryRuntimeContext::default()
            },
            layout.clone(),
            vec![Arc::new(NoopEngineExtensionsProvider)],
        );
        let search_observations = SearchObservationHandle::new(layout.clone());
        let lifecycle_lock = workspace_manager.lifecycle_lock();
        let search_manager = SearchManager::new(
            layout.clone(),
            &config_store,
            workspace_manager.clone(),
            true,
            CatalogDiscovery::new(query_manager.clone()),
            lifecycle_lock,
        );
        let running = start_server(
            ServerDependencies {
                source: source_manager,
                workspace: workspace_manager,
                query: query_manager,
                search: search_manager,
                search_observations: Some(search_observations),
                feedback: feedback_manager,
                task: task_manager,
            },
            TraceServerComponents::default(),
            Arc::new(SingleUserPrincipalProvider),
            ServerMode::EphemeralGrpc,
        )
        .await
        .expect("start server");
        let channel = Endpoint::from_shared(running.endpoint_uri().to_string())
            .expect("endpoint")
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE)
            .connect()
            .await
            .expect("connect");
        let mut source_client = SourceServiceClient::new(channel.clone());
        let mut query_client = QueryServiceClient::new(channel)
            .max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);

        let mut import_stream = source_client
            .import_source(Request::new(ImportSourceRequest {
                workspace: Some(default_workspace()),
                manifest_yaml: r#"
name: tilde_demo
version: 0.1.0
dsl_version: 3
backend: file
tables:
  - name: messages
    description: Fixture messages
    format: jsonl
    source:
      location: file://~/fixture-data/
      glob: "**/*.jsonl"
    columns:
      - name: type
        type: Utf8
      - name: text
        type: Utf8
"#
                .to_string(),
                variables: Vec::new(),
                secrets: Vec::new(),
                oauth_credential_retrievals: Vec::new(),
            }))
            .await
            .expect("create source")
            .into_inner();
        let imported = import_stream
            .message()
            .await
            .expect("import source stream")
            .and_then(|response| match response.event {
                Some(import_source_response::Event::Source(source)) => Some(source),
                _ => None,
            })
            .expect("import source response");
        assert_eq!(imported.name, "tilde_demo");

        let response = query_client
            .execute_sql(Request::new(ExecuteSqlRequest {
                workspace: Some(default_workspace()),
                sql: "SELECT text FROM tilde_demo.messages ORDER BY text".to_string(),
            }))
            .await
            .expect("execute sql")
            .into_inner();
        let result = coral_client::decode_execute_sql_response(&response).expect("decode");
        let rows = coral_client::batches_to_json_rows(result.batches()).expect("rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["text"], "hello");
        assert_eq!(rows[1]["text"], "world");
    }

    /// an `ExecuteSql` response larger than
    /// the previous tonic 4 MB default must round-trip cleanly. Before the
    /// fix, this query failed with `h2 protocol error … PROTOCOL_ERROR`.
    #[tokio::test]
    async fn execute_sql_response_above_default_4mb_limit_round_trips() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");

        let layout = AppStateLayout::discover(Some(config_dir.clone())).expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let db = test_db(&layout, &config_store).await;
        let source_manager = SourceManager::new_for_tests(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
        );
        let feedback_manager = FeedbackManager::new(layout.clone());
        let task_manager = TaskManager::new(Arc::new(JsonlTaskEventStore::new(layout.clone())));
        let workspace_manager = WorkspaceManager::new_for_tests(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
            None,
            Arc::clone(&db),
        );
        let query_manager = QueryManager::new_for_tests(
            config_store.clone(),
            workspace_manager.clone(),
            credential_manager,
            QueryRuntimeContext::default(),
            layout.clone(),
            vec![Arc::new(NoopEngineExtensionsProvider)],
        );
        let search_observations = SearchObservationHandle::new(layout.clone());
        let lifecycle_lock = workspace_manager.lifecycle_lock();
        let search_manager = SearchManager::new(
            layout.clone(),
            &config_store,
            workspace_manager.clone(),
            true,
            CatalogDiscovery::new(query_manager.clone()),
            lifecycle_lock,
        );
        let running = start_server(
            ServerDependencies {
                source: source_manager,
                workspace: workspace_manager,
                query: query_manager,
                search: search_manager,
                search_observations: Some(search_observations),
                feedback: feedback_manager,
                task: task_manager,
            },
            TraceServerComponents::default(),
            Arc::new(SingleUserPrincipalProvider),
            ServerMode::EphemeralGrpc,
        )
        .await
        .expect("start server");
        let channel = Endpoint::from_shared(running.endpoint_uri().to_string())
            .expect("endpoint")
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE)
            .connect()
            .await
            .expect("connect");
        let mut query_client = QueryServiceClient::new(channel)
            .max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);

        // No underscore separator — DataFusion's SQL parser is conservative
        // about numeric literal formats.
        let sql = "SELECT repeat('x', 5000000) AS pad";
        let response = query_client
            .execute_sql(Request::new(ExecuteSqlRequest {
                workspace: Some(default_workspace()),
                sql: sql.to_string(),
            }))
            .await
            .expect("execute_sql >4MB response")
            .into_inner();

        // Prove the payload actually crossed the old 4 MB ceiling — without
        // this check the test could silently start passing for the wrong
        // reason if `repeat` ever returned a smaller value.
        assert!(
            response.arrow_ipc_stream.len() > 4 * 1024 * 1024,
            "regression payload was {} bytes; expected >4MB",
            response.arrow_ipc_stream.len()
        );

        let result = coral_client::decode_execute_sql_response(&response).expect("decode");
        assert_eq!(result.row_count(), 1);
    }

    /// an invalid column against a wide manifest must surface as a clean `tonic::Status`,
    /// not a transport-level `h2 protocol error`. Pre-fix, `DataFusion`'s
    /// "Valid fields are …" error enumerating ~600 field names
    /// overflowed HTTP/2 trailers; the CLI saw `PROTOCOL_ERROR` instead
    /// of the intended status.
    ///
    /// Also verifies the behavior change: wrapped `SchemaError` now maps
    /// to `Code::InvalidArgument` (via `find_root()`), not `Code::Internal`.
    #[tokio::test]
    async fn invalid_column_on_wide_manifest_returns_clean_status() {
        use std::fmt::Write as _;

        use crate::bootstrap::MAX_STATUS_DETAIL_BYTES;

        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        let data_dir = temp.path().join("wide-data");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        // No rows needed — the test cares only about schema width.
        let location = format!("file://{}/", data_dir.display());

        let mut manifest = String::new();
        manifest.push_str("name: wide_demo\n");
        manifest.push_str("version: 0.1.0\n");
        manifest.push_str("dsl_version: 3\n");
        manifest.push_str("backend: file\n");
        manifest.push_str("tables:\n");
        manifest.push_str("  - name: wide\n");
        manifest.push_str("    description: Wide fixture\n");
        manifest.push_str("    format: jsonl\n");
        manifest.push_str("    source:\n");
        writeln!(manifest, "      location: {location}").expect("write to String");
        manifest.push_str("      glob: \"**/*.jsonl\"\n");
        manifest.push_str("    columns:\n");
        for i in 0..600 {
            writeln!(manifest, "      - name: col_{i:04}\n        type: Utf8")
                .expect("write to String");
        }

        let layout = AppStateLayout::discover(Some(config_dir.clone())).expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let db = test_db(&layout, &config_store).await;
        let source_manager = SourceManager::new_for_tests(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
        );
        let feedback_manager = FeedbackManager::new(layout.clone());
        let task_manager = TaskManager::new(Arc::new(JsonlTaskEventStore::new(layout.clone())));
        let workspace_manager = WorkspaceManager::new_for_tests(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
            None,
            Arc::clone(&db),
        );
        let query_manager = QueryManager::new_for_tests(
            config_store.clone(),
            workspace_manager.clone(),
            credential_manager,
            QueryRuntimeContext::default(),
            layout.clone(),
            vec![Arc::new(NoopEngineExtensionsProvider)],
        );
        let search_observations = SearchObservationHandle::new(layout.clone());
        let lifecycle_lock = workspace_manager.lifecycle_lock();
        let search_manager = SearchManager::new(
            layout.clone(),
            &config_store,
            workspace_manager.clone(),
            true,
            CatalogDiscovery::new(query_manager.clone()),
            lifecycle_lock,
        );
        let running = start_server(
            ServerDependencies {
                source: source_manager,
                workspace: workspace_manager,
                query: query_manager,
                search: search_manager,
                search_observations: Some(search_observations),
                feedback: feedback_manager,
                task: task_manager,
            },
            TraceServerComponents::default(),
            Arc::new(SingleUserPrincipalProvider),
            ServerMode::EphemeralGrpc,
        )
        .await
        .expect("start server");
        let channel = Endpoint::from_shared(running.endpoint_uri().to_string())
            .expect("endpoint")
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE)
            .connect()
            .await
            .expect("connect");
        let mut source_client = SourceServiceClient::new(channel.clone());
        let mut query_client = QueryServiceClient::new(channel)
            .max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);

        let mut import_stream = source_client
            .import_source(Request::new(ImportSourceRequest {
                workspace: Some(default_workspace()),
                manifest_yaml: manifest,
                variables: Vec::new(),
                secrets: Vec::new(),
                oauth_credential_retrievals: Vec::new(),
            }))
            .await
            .expect("import wide source")
            .into_inner();
        let imported = import_stream
            .message()
            .await
            .expect("import wide source stream")
            .and_then(|response| match response.event {
                Some(import_source_response::Event::Source(source)) => Some(source),
                _ => None,
            })
            .expect("import wide source response");
        assert_eq!(imported.name, "wide_demo");

        let status = query_client
            .execute_sql(Request::new(ExecuteSqlRequest {
                workspace: Some(default_workspace()),
                sql: "SELECT bogus_column FROM wide_demo.wide LIMIT 0".to_string(),
            }))
            .await
            .expect_err("expected gRPC Status, not a transport-level PROTOCOL_ERROR");

        assert_eq!(
            status.code(),
            tonic::Code::InvalidArgument,
            "wrapped schema error should map to InvalidArgument via find_root(); message = {:?}",
            status.message()
        );
        assert!(
            status.message().len() <= MAX_STATUS_DETAIL_BYTES,
            "status message was {} bytes; truncator should have clipped it to <= {MAX_STATUS_DETAIL_BYTES}",
            status.message().len(),
        );
        assert!(
            status.message().contains("No column named"),
            "missing expected schema-error head in: {:?}",
            status.message()
        );
    }
}
