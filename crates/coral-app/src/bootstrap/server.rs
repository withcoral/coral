//! Builds and runs the Coral gRPC server.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use coral_api::v1::catalog_service_server::CatalogServiceServer;
use coral_api::v1::feature_service_server::FeatureServiceServer;
use coral_api::v1::feedback_service_server::FeedbackServiceServer;
use coral_api::v1::function_service_server::FunctionServiceServer;
use coral_api::v1::gui_onboarding_service_server::GuiOnboardingServiceServer;
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
use tokio::sync::{oneshot, watch};
use tokio::task::{self, JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::Routes;
use tonic::transport::Server;

use super::env::AppEnvironment;
use super::error::AppError;
use super::health::{AggregateHealthService, EngineReadiness};
use super::server_config::{LoadedServerConfig, ServeSettings};
use crate::EngineExtensionsProvider;
use crate::catalog::discovery::CatalogDiscovery;
use crate::catalog::service::CatalogService;
use crate::credentials::config::CredentialStorageConfig;
use crate::credentials::{CredentialManager, CredentialStore};
use crate::features::service::FeatureService;
use crate::features::{Feature, FeatureOverrides, FeatureStore, Features};
use crate::feedback::manager::FeedbackManager;
use crate::feedback::publisher::{
    FeedbackPublisher, HostedFeedbackPublisher, NoopFeedbackPublisher,
};
use crate::feedback::service::FeedbackService;
use crate::functions::service::FunctionService;
use crate::gui_onboarding::manager::GuiOnboardingManager;
use crate::gui_onboarding::service::GuiOnboardingService;
use crate::identity::{LocalPrincipalProvider, PrincipalProvider};
use crate::query::manager::QueryManager;
use crate::query::service::QueryService;
use crate::search::manager::SearchManager;
use crate::search::observed::SearchObservationHandle;
use crate::search::response_history::{SearchResponseHistory, SearchResponseHistoryWorker};
use crate::search::service::SearchService;
use crate::sources::manager::SourceManager;
use crate::sources::materialization::SourceDiagnosticReporter;
use crate::sources::service::SourceService;
use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig, run_state_migrations};
use crate::state::{AppStateLayout, ConfigStore};
use crate::task::manager::TaskManager;
use crate::task::service::TaskService;
use crate::task::store::TaskStore;
use crate::telemetry::service::TraceService;
use crate::telemetry::{TelemetryConfig, TraceManager};
use crate::transport::GrpcRequestContextLayer;
use crate::workspaces::{
    WorkspaceLifecycleLock, WorkspaceManager, WorkspacePoolRegistry, WorkspaceService,
};

#[derive(Clone)]
enum ServerModeSelection {
    Explicit(ServerMode),
    ConfiguredStandaloneGrpc,
}

/// Server-side bootstrap configuration for the Coral server.
#[derive(Clone)]
pub(crate) struct ServerConfig {
    config_dir: Option<PathBuf>,
    mode: ServerModeSelection,
    engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    /// Provider every request's principal is resolved through.
    ///
    /// Always present, defaulting to [`LocalPrincipalProvider`], so nothing
    /// downstream of the builder has to handle a server that might have no way
    /// to name its caller.
    principal_provider: Arc<dyn PrincipalProvider>,
    feedback_publisher: Arc<dyn FeedbackPublisher>,
    feature_overrides: FeatureOverrides,
    enable_stderr_logs: bool,
    // Test seam: a listener the gRPC server adopts instead of binding
    // `mode.bind_addr()` itself. Lets startup-failure tests hold the reserved
    // port continuously rather than selecting and releasing it, closing the
    // race where another process claims the port before the server binds.
    grpc_listener: Option<Arc<std::net::TcpListener>>,
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
            mode: ServerModeSelection::Explicit(ServerMode::EphemeralGrpc),
            engine_extensions_providers: Vec::new(),
            principal_provider: Arc::new(LocalPrincipalProvider),
            feedback_publisher: Arc::new(HostedFeedbackPublisher::new()),
            feature_overrides: FeatureOverrides::default(),
            enable_stderr_logs: false,
            grpc_listener: None,
        }
    }

    pub(crate) fn with_config_dir(mut self, config_dir: impl Into<PathBuf>) -> Self {
        self.config_dir = Some(config_dir.into());
        self
    }

    pub(crate) fn with_mode(mut self, mode: ServerMode) -> Self {
        self.mode = ServerModeSelection::Explicit(mode);
        self
    }

    pub(crate) fn with_configured_standalone_grpc(mut self) -> Self {
        self.mode = ServerModeSelection::ConfiguredStandaloneGrpc;
        self
    }

    fn resolved_mode(&self, layout: &AppStateLayout) -> Result<ServerMode, AppError> {
        match &self.mode {
            ServerModeSelection::Explicit(mode @ ServerMode::StandaloneGrpc { .. }) => {
                let config = LoadedServerConfig::load(layout)?;
                config.reject_removed_auth()?;
                Ok(mode.clone())
            }
            ServerModeSelection::Explicit(mode) => Ok(mode.clone()),
            ServerModeSelection::ConfiguredStandaloneGrpc => {
                let config = LoadedServerConfig::load(layout)?;
                let bind = config.grpc_settings()?.bind_addr;
                Ok(ServerMode::StandaloneGrpc { bind })
            }
        }
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
}

impl ServerMode {
    fn bind_addr(&self) -> SocketAddr {
        match self {
            Self::EphemeralGrpc => SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            Self::StandaloneGrpc { bind } => *bind,
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
    /// Creates a standalone gRPC server using `[server].bind_addr`.
    pub fn configured_standalone_grpc() -> Self {
        Self {
            config: ServerConfig::new().with_configured_standalone_grpc(),
        }
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

    /// Resolves the settings for companions served beside this gRPC server.
    ///
    /// Only settings are returned. Constructing the session providers and the
    /// authorization server, and running the transports they belong to, is the
    /// caller's job: this builder starts a gRPC server and nothing else.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the configuration cannot be loaded or validated.
    pub fn serve_settings(&self) -> Result<ServeSettings, AppError> {
        let layout = AppEnvironment::discover().app_state_layout(self.config.config_dir.clone())?;
        LoadedServerConfig::load(&layout)?.companion_settings()
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
    /// Sets the server-side principal provider.
    ///
    /// The default provider returns the local principal for every
    /// request. Product runtimes can authenticate inbound metadata and select
    /// any canonical principal by installing their own provider.
    ///
    /// Whoever resolves `[auth]` composes the provider it asks for and installs
    /// it here — `coral-cli`'s `serve::compose_session_policies` does this for
    /// `coral server`. Without this call a standalone listener serves the local
    /// principal to every caller its address is reachable from.
    pub fn with_principal_provider(
        mut self,
        principal_provider: Arc<dyn PrincipalProvider>,
    ) -> Self {
        self.config.principal_provider = principal_provider;
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

    /// Adopts an already-bound listener for the gRPC server instead of binding
    /// the mode's address.
    ///
    /// Startup-failure tests use this to reserve a port and hand the live
    /// listener straight to the server, so the port never lapses between
    /// selection and bind and a parallel process cannot steal it.
    #[doc(hidden)]
    #[must_use]
    pub fn with_prebound_grpc_listener(mut self, listener: std::net::TcpListener) -> Self {
        self.config.grpc_listener = Some(Arc::new(listener));
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
        let layout = env.app_state_layout(self.config.config_dir.clone())?;
        let mode = self.config.resolved_mode(&layout)?;
        let principal = self.config.principal_provider.clone();
        let listener = self.config.grpc_listener.clone();
        layout.ensure()?;
        let feature_store = FeatureStore::from_layout(layout.clone());
        let features = feature_store.load_with_overrides(&self.config.feature_overrides)?;
        let coral_db = init_database(&layout).await?;
        let config_store = ConfigStore::new(layout.clone());
        run_state_migrations(&coral_db, &config_store, &layout).await?;
        let coral_db = Arc::new(coral_db);
        let telemetry = init_server_telemetry(&layout, self.config.enable_stderr_logs)?;
        let active_trace_store_dir = telemetry
            .active_trace_store
            .as_ref()
            .map(|store| store.dir.clone());
        let credential_config = CredentialStorageConfig::load(&layout)?;
        let credential_store =
            CredentialStore::with_preference(layout.clone(), credential_config.storage);
        let credential_manager = CredentialManager::new(credential_store);
        let workspace_lifecycle_lock = WorkspaceLifecycleLock::default();
        let workspace_pool_registry = Arc::new(WorkspacePoolRegistry::default());
        let diagnostic_reporter = SourceDiagnosticReporter::default();
        let database_sources_enabled = features.enabled(Feature::DatabaseSources);
        let source_manager = SourceManager::with_diagnostic_reporter(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
            workspace_lifecycle_lock.clone(),
            diagnostic_reporter.clone(),
        )
        .with_pool_registry(Arc::clone(&workspace_pool_registry))
        .with_database_sources_enabled(database_sources_enabled);
        let workspace_manager = WorkspaceManager::new(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
            active_trace_store_dir.clone(),
            workspace_lifecycle_lock.clone(),
            Arc::clone(&coral_db),
            diagnostic_reporter.clone(),
        )
        .with_pool_registry(Arc::clone(&workspace_pool_registry));
        let feedback_manager =
            FeedbackManager::with_publisher(layout.clone(), self.config.feedback_publisher);
        let task_manager = TaskManager::new(TaskStore::new(Arc::clone(&coral_db)));
        let task_activity = crate::task::activity::TaskActivityRecorder::new(Arc::clone(&coral_db));
        let query_runtime_context = env.query_runtime_context().with_body_capture_max_bytes(
            telemetry
                .config
                .trace_history
                .http_body_recording_max_bytes(),
        );
        let query_manager = QueryManager::with_diagnostic_reporter(
            config_store.clone(),
            workspace_manager.clone(),
            credential_manager,
            query_runtime_context,
            layout.clone(),
            workspace_lifecycle_lock.clone(),
            self.config.engine_extensions_providers,
            diagnostic_reporter.clone(),
            workspace_pool_registry,
        )
        .with_database_sources_enabled(database_sources_enabled)
        .with_task_activity_recorder(task_activity);
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
            workspace_lifecycle_lock.clone(),
        );
        let trace_components =
            init_trace_components(telemetry, Arc::clone(&coral_db), workspace_lifecycle_lock);
        let dependencies = ServerDependencies {
            gui_onboarding: GuiOnboardingManager::new(Arc::clone(&coral_db)),
            source: source_manager,
            workspace: workspace_manager,
            query: query_manager,
            search: search_manager,
            search_observations,
            feedback: feedback_manager,
            task: task_manager,
            feature_store,
            active_features: features,
        };
        start_server(dependencies, trace_components, principal, mode, listener).await
    }
}

struct InitializedServerTelemetry {
    config: TelemetryConfig,
    active_trace_store: Option<crate::telemetry::InstalledLocalTraceStore>,
    search_response_history_binding: SearchResponseHistoryBinding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchResponseHistoryBinding {
    Disabled,
    CaptureOnly,
    Attached,
    Incompatible,
}

impl SearchResponseHistoryBinding {
    fn capture_enabled(self) -> bool {
        matches!(self, Self::CaptureOnly | Self::Attached)
    }

    fn reader_enabled(self) -> bool {
        self == Self::Attached
    }
}

fn init_server_telemetry(
    layout: &AppStateLayout,
    enable_stderr_logs: bool,
) -> Result<InitializedServerTelemetry, AppError> {
    let config = TelemetryConfig::load(layout)?;
    let local_trace_store_dir = config
        .trace_history
        .enabled
        .then(|| layout.local_trace_store_dir());
    let installed_trace_store =
        crate::telemetry::init_tracing(&config, enable_stderr_logs, local_trace_store_dir.clone())?;
    let active_trace_store = config
        .trace_history
        .enabled
        .then_some(installed_trace_store)
        .flatten();
    let configured_retention = config.trace_history.retention();
    let search_response_history_binding = search_response_history_binding(
        local_trace_store_dir.as_deref(),
        configured_retention,
        active_trace_store.as_ref(),
    );
    if let (Some(configured_dir), Some(active_store)) = (
        local_trace_store_dir.as_deref(),
        active_trace_store.as_ref(),
    ) && search_response_history_binding == SearchResponseHistoryBinding::Incompatible
    {
        tracing::warn!(
            configured_trace_store_dir = %configured_dir.display(),
            active_trace_store_dir = %active_store.dir.display(),
            ?configured_retention,
            active_retention = ?active_store.retention,
            "Search response history is disabled because this server does not match the process-owned trace store"
        );
    }
    Ok(InitializedServerTelemetry {
        config,
        active_trace_store,
        search_response_history_binding,
    })
}

fn search_response_history_binding(
    configured_trace_store_dir: Option<&Path>,
    configured_retention: Duration,
    active_trace_store: Option<&crate::telemetry::InstalledLocalTraceStore>,
) -> SearchResponseHistoryBinding {
    match (configured_trace_store_dir, active_trace_store) {
        (None, _) => SearchResponseHistoryBinding::Disabled,
        (Some(_configured_dir), None) => SearchResponseHistoryBinding::CaptureOnly,
        (Some(configured_dir), Some(active_store))
            if configured_dir == active_store.dir
                && configured_retention == active_store.retention =>
        {
            SearchResponseHistoryBinding::Attached
        }
        (Some(_configured_dir), Some(_active_store)) => SearchResponseHistoryBinding::Incompatible,
    }
}

fn trace_components_for_store(
    active_trace_store: Option<crate::telemetry::InstalledLocalTraceStore>,
    coral_db: Arc<CoralDb>,
    search_response_history_retention: Duration,
    search_response_history: SearchResponseHistory,
    search_response_history_worker: SearchResponseHistoryWorker,
    attach_search_response_history_reader: bool,
) -> TraceServerComponents {
    match active_trace_store {
        None => TraceServerComponents {
            search_response_history: Some(search_response_history.clone()),
            search_response_history_worker: Some(search_response_history_worker),
            ..TraceServerComponents::default()
        },
        Some(store) => {
            let trace_manager = TraceManager::new(store.dir.clone(), store.retention);
            let trace_manager = if attach_search_response_history_reader {
                trace_manager
                    .with_search_response_history(coral_db, search_response_history_retention)
            } else {
                trace_manager
            };
            TraceServerComponents {
                local_trace_store_dir: Some(store.dir),
                service: Some(TraceService::new(trace_manager)),
                search_response_history: Some(search_response_history),
                search_response_history_worker: Some(search_response_history_worker),
            }
        }
    }
}

fn init_trace_components(
    telemetry: InitializedServerTelemetry,
    coral_db: Arc<CoralDb>,
    workspace_lifecycle_lock: WorkspaceLifecycleLock,
) -> TraceServerComponents {
    let InitializedServerTelemetry {
        config,
        active_trace_store,
        search_response_history_binding,
    } = telemetry;
    let trace_history = &config.trace_history;
    let search_response_history_retention = trace_history.retention();
    let (history, worker) = SearchResponseHistory::start(
        Arc::clone(&coral_db),
        workspace_lifecycle_lock,
        search_response_history_binding.capture_enabled(),
        search_response_history_retention,
    );
    trace_components_for_store(
        active_trace_store,
        coral_db,
        search_response_history_retention,
        history,
        worker,
        search_response_history_binding.reader_enabled(),
    )
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
    local_addr: SocketAddr,
    local_trace_store_dir: Option<PathBuf>,
    search: SearchManager,
    search_observations: Mutex<Option<SearchObservationHandle>>,
    search_response_history_worker: Mutex<Option<SearchResponseHistoryWorker>>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    task_finished: watch::Receiver<bool>,
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

    /// Returns the address bound by this server.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    #[must_use]
    /// Returns the process-installed local trace store directory, when local
    /// trace history is enabled for this process.
    pub fn local_trace_store_dir(&self) -> Option<&std::path::Path> {
        self.local_trace_store_dir.as_deref()
    }

    /// Waits until the background server task exits.
    ///
    /// This method is cancellation-safe and does not initiate shutdown,
    /// consume the task result, or release server resources. Call
    /// [`RunningServer::shutdown`] afterward to join the task, surface errors,
    /// and complete cleanup.
    pub async fn wait_for_exit(&self) {
        let mut task_finished = self.task_finished.clone();
        let _finished = task_finished.wait_for(|finished| *finished).await;
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
        self.shutdown_search_response_history().await;
        task_result?;
        search_observations_result?;
        Ok(())
    }

    async fn shutdown_search_response_history(&self) {
        let worker = self
            .search_response_history_worker
            .lock()
            .expect("Search response history worker mutex poisoned")
            .take();
        if let Some(worker) = worker {
            worker.shutdown().await;
        }
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
    search_response_history: Option<SearchResponseHistory>,
    search_response_history_worker: Option<SearchResponseHistoryWorker>,
}

struct ServerDependencies {
    gui_onboarding: GuiOnboardingManager,
    source: SourceManager,
    workspace: WorkspaceManager,
    query: QueryManager,
    search: SearchManager,
    search_observations: Option<SearchObservationHandle>,
    feedback: FeedbackManager,
    task: TaskManager,
    feature_store: FeatureStore,
    // The resolution `start` performed, carried forward so the feature service
    // can report what this server is running rather than only what config says.
    active_features: Features,
}

/// Builds the gRPC routes for every application service, and returns the query
/// manager the health service reads readiness from.
fn application_routes(
    dependencies: ServerDependencies,
    trace_service: Option<TraceService>,
    search_response_history: Option<SearchResponseHistory>,
) -> (Routes, QueryManager) {
    let ServerDependencies {
        gui_onboarding,
        source,
        workspace,
        query,
        search,
        search_observations,
        feedback,
        task,
        feature_store,
        active_features,
    } = dependencies;
    let (source, query) = match search_observations.as_ref() {
        Some(search_observations) => (
            source.with_search_observation_handle(search_observations.clone()),
            query.with_search_observation_handle(search_observations.clone()),
        ),
        None => (source, query),
    };
    let health_queries = query.clone();
    let source_service = SourceService::new(source, query.clone(), workspace.clone());
    let workspace_service = WorkspaceService::new(workspace);
    let catalog_service = CatalogService::new(query.clone(), task.clone());
    let function_service = FunctionService::new(query.clone());
    let query_service = QueryService::new(query, task.clone());
    let search_service = configured_search_service(search, task.clone(), search_response_history);
    let feedback_service = FeedbackService::new(feedback, task.clone());
    let feature_service = FeatureService::new(feature_store, active_features);
    let task_service = TaskService::new(task);
    let gui_onboarding_service = GuiOnboardingService::new(gui_onboarding);
    let mut routes = Routes::default()
        .add_service(GuiOnboardingServiceServer::new(gui_onboarding_service))
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
        .add_service(FeatureServiceServer::new(feature_service))
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
        routes = routes.add_service(
            TraceServiceServer::new(trace_service)
                .max_encoding_message_size(TRACE_RESPONSE_MAX_MESSAGE_SIZE),
        );
    }
    (routes, health_queries)
}

async fn start_server(
    dependencies: ServerDependencies,
    trace_components: TraceServerComponents,
    principal_provider: Arc<dyn PrincipalProvider>,
    mode: ServerMode,
    grpc_listener: Option<Arc<std::net::TcpListener>>,
) -> Result<RunningServer, AppError> {
    let TraceServerComponents {
        service: trace_service,
        local_trace_store_dir,
        search_response_history,
        search_response_history_worker,
    } = trace_components;
    // `RunningServer` owns both for shutdown; the routes only borrow them.
    let search = dependencies.search.clone();
    let search_observations = dependencies.search_observations.clone();
    let (application_routes, health_queries) =
        application_routes(dependencies, trace_service, search_response_history);
    let routes = Routes::from(
        application_routes
            .into_axum_router()
            .layer(GrpcRequestContextLayer::new(principal_provider)),
    )
    // Health must not depend on principal selection: it is the readiness signal
    // an orchestrator reaches without a credential.
    .add_service(tonic_health::pb::health_server::HealthServer::new(
        AggregateHealthService::new(EngineReadiness::from_query_manager(health_queries)),
    ));

    let listener = match grpc_listener {
        // A test handed us a live listener; adopt it so the reserved port never
        // lapses. `from_std` requires the socket be non-blocking.
        Some(prebound) => {
            let prebound = prebound.try_clone()?;
            prebound.set_nonblocking(true)?;
            TcpListener::from_std(prebound)?
        }
        None => TcpListener::bind(mode.bind_addr()).await?,
    };
    let local_addr = listener.local_addr()?;
    let endpoint_uri = format!("http://{local_addr}");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (task_finished_tx, task_finished) = watch::channel(false);

    let task = match mode {
        ServerMode::EphemeralGrpc | ServerMode::StandaloneGrpc { .. } => {
            start_grpc_server(listener, shutdown_rx, routes, task_finished_tx)
        }
    };

    Ok(RunningServer {
        endpoint_uri,
        local_addr,
        local_trace_store_dir,
        search,
        search_observations: Mutex::new(search_observations),
        search_response_history_worker: Mutex::new(search_response_history_worker),
        shutdown_tx: Mutex::new(Some(shutdown_tx)),
        task_finished,
        task: Mutex::new(Some(task)),
    })
}

fn configured_search_service(
    search: SearchManager,
    task: TaskManager,
    response_history: Option<SearchResponseHistory>,
) -> SearchService {
    let service = SearchService::new(search, task);
    match response_history {
        Some(history) => service.with_response_history(history),
        None => service,
    }
}

fn start_grpc_server(
    listener: TcpListener,
    shutdown_rx: oneshot::Receiver<()>,
    routes: Routes,
    task_finished: watch::Sender<bool>,
) -> JoinHandle<Result<(), tonic::transport::Error>> {
    tokio::spawn(async move {
        let result = Server::builder()
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE)
            .add_routes(routes)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                drop(shutdown_rx.await);
            })
            .await;
        task_finished.send_replace(true);
        result
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

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "JSON row assertions intentionally fail loudly in tests"
    )]

    use std::future::Future as _;
    use std::net::{Ipv4Addr, SocketAddr, TcpListener};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::task::Poll;
    use std::time::Duration;

    use coral_api::v1::gui_onboarding_service_client::GuiOnboardingServiceClient;
    use coral_api::v1::query_service_client::QueryServiceClient;
    use coral_api::v1::source_service_client::SourceServiceClient;
    use coral_api::v1::task_service_client::TaskServiceClient;
    use coral_api::v1::trace_service_client::TraceServiceClient;
    use coral_api::v1::{
        EndTaskRequest, ExecuteSqlRequest, GetGuiOnboardingStateRequest, ImportSourceRequest,
        ListSourcesRequest, ListTracesRequest, StartTaskRequest, TaskStatus, TraceView, Workspace,
        import_source_response,
    };
    use coral_api::{HTTP2_MAX_HEADER_LIST_SIZE, QUERY_RESPONSE_MAX_MESSAGE_SIZE};
    use coral_engine::QueryRuntimeContext;
    use tempfile::TempDir;
    use tokio::sync::{oneshot, watch};
    use tonic::transport::Endpoint;
    use tonic::{Code, Request};

    use super::{
        RunningServer, SearchResponseHistoryBinding, ServerBuilder, ServerDependencies, ServerMode,
        TraceServerComponents, search_response_history_binding, start_server,
    };
    use crate::bootstrap::AppError;
    use crate::catalog::discovery::CatalogDiscovery;
    use crate::credentials::{CredentialManager, CredentialStore};
    use crate::features::{Feature, FeatureOverrides, FeatureStore, Features};
    use crate::feedback::manager::FeedbackManager;
    use crate::gui_onboarding::manager::GuiOnboardingManager;
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
    use crate::task::store::TaskStore;
    use crate::telemetry::{TraceManager, service::TraceService};
    use crate::transport::workspace_to_proto;
    use crate::workspaces::{WorkspaceManager, WorkspaceName};
    use crate::{
        AwsEngineExtensionsProvider, LocalPrincipalProvider, NoopEngineExtensionsProvider,
        Principal, PrincipalProvider, PrincipalProviderError,
    };

    fn default_workspace() -> Workspace {
        workspace_to_proto(&WorkspaceName::default())
    }

    #[test]
    fn search_response_history_uses_the_matching_process_trace_store() {
        let configured_dir = PathBuf::from("matching-trace-store");
        let configured_retention = Duration::from_mins(1);
        let active_store = crate::telemetry::InstalledLocalTraceStore {
            dir: configured_dir.clone(),
            retention: configured_retention,
        };

        let binding = search_response_history_binding(
            Some(configured_dir.as_path()),
            configured_retention,
            Some(&active_store),
        );

        assert_eq!(binding, SearchResponseHistoryBinding::Attached);
        assert!(binding.capture_enabled());
        assert!(binding.reader_enabled());
    }

    #[test]
    fn search_response_history_rejects_a_different_process_trace_store() {
        let configured_dir = PathBuf::from("configured-trace-store");
        let configured_retention = Duration::from_mins(1);
        let active_store = crate::telemetry::InstalledLocalTraceStore {
            dir: PathBuf::from("process-trace-store"),
            retention: configured_retention,
        };

        let binding = search_response_history_binding(
            Some(configured_dir.as_path()),
            configured_retention,
            Some(&active_store),
        );

        assert_eq!(binding, SearchResponseHistoryBinding::Incompatible);
        assert!(!binding.capture_enabled());
        assert!(!binding.reader_enabled());
    }

    #[test]
    fn search_response_history_still_captures_without_a_coral_trace_store() {
        let binding = search_response_history_binding(
            Some(Path::new("configured-trace-store")),
            Duration::from_mins(1),
            None,
        );

        assert_eq!(binding, SearchResponseHistoryBinding::CaptureOnly);
        assert!(binding.capture_enabled());
        assert!(!binding.reader_enabled());
    }

    #[test]
    fn search_response_history_rejects_different_process_retention() {
        let configured_dir = PathBuf::from("matching-trace-store");
        let active_store = crate::telemetry::InstalledLocalTraceStore {
            dir: configured_dir.clone(),
            retention: Duration::from_mins(1),
        };

        let binding = search_response_history_binding(
            Some(configured_dir.as_path()),
            Duration::from_mins(2),
            Some(&active_store),
        );

        assert_eq!(binding, SearchResponseHistoryBinding::Incompatible);
        assert!(!binding.capture_enabled());
        assert!(!binding.reader_enabled());
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
    struct RejectingPrincipalProvider;

    #[tonic::async_trait]
    impl PrincipalProvider for RejectingPrincipalProvider {
        async fn principal_for_metadata(
            &self,
            _metadata: &tonic::metadata::MetadataMap,
        ) -> Result<Principal, PrincipalProviderError> {
            Err(PrincipalProviderError::unauthenticated(
                "rejected principal",
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
        run_state_migrations(&db, config_store, layout)
            .await
            .expect("run state migrations");
        Arc::new(db)
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "server lifecycle test requires full runtime assembly"
    )]
    async fn wait_for_exit_preserves_task_error_and_shutdown_cleanup() {
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
        let (task_gate_tx, task_gate_rx) = oneshot::channel();
        let (task_finished_tx, task_finished) = watch::channel(false);
        let task = tokio::spawn(async move {
            drop(task_gate_rx.await);
            let should_panic = true;
            assert!(!should_panic, "server task panicked");
            task_finished_tx.send_replace(true);
            Ok::<(), tonic::transport::Error>(())
        });
        let server = RunningServer {
            endpoint_uri: "http://127.0.0.1:0".to_string(),
            local_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            local_trace_store_dir: None,
            search,
            search_observations: Mutex::new(Some(search_observations)),
            search_response_history_worker: Mutex::new(None),
            shutdown_tx: Mutex::new(None),
            task_finished,
            task: Mutex::new(Some(task)),
        };

        let mut canceled_wait = Box::pin(server.wait_for_exit());
        std::future::poll_fn(|cx| {
            assert!(canceled_wait.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(canceled_wait);
        task_gate_tx.send(()).expect("release server task");

        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(1), server.wait_for_exit())
                .await
                .expect("server task completion should wake waiters");
        }
        let result = server.shutdown_inner().await;

        assert!(matches!(
            result,
            Err(AppError::TaskJoin(error)) if error.is_panic()
        ));
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

    #[test]
    fn configured_standalone_grpc_resolves_configured_bind() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        std::fs::write(
            config_dir.join("config.toml"),
            "[server]\nbind_addr = '127.0.0.2:14555'\n",
        )
        .expect("write config");
        let layout = AppStateLayout::discover(Some(config_dir)).expect("layout");
        let builder = ServerBuilder::configured_standalone_grpc();

        let ServerMode::StandaloneGrpc { bind } = builder
            .config
            .resolved_mode(&layout)
            .expect("resolve configured bind")
        else {
            panic!("configured server must use standalone gRPC mode");
        };
        assert_eq!(bind, SocketAddr::from(([127, 0, 0, 2], 14555)));
    }

    #[test]
    fn explicit_standalone_grpc_rejects_removed_static_auth_config() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        std::fs::create_dir_all(&config_dir).expect("config dir");
        std::fs::write(
            config_dir.join("config.toml"),
            "[server.auth]\ntoken_env = 'CORAL_SERVER_AUTH_TOKEN'\n",
        )
        .expect("config file");
        let layout = AppStateLayout::discover(Some(config_dir)).expect("layout");
        let builder =
            ServerBuilder::standalone_grpc(SocketAddr::from((Ipv4Addr::LOCALHOST, 14555)));

        assert!(
            builder.config.resolved_mode(&layout).is_err(),
            "removed standalone auth config must fail closed"
        );
    }

    /// Writes a config whose `[auth]` marks the instance authenticated.
    ///
    /// The signing key the section names is never created: startup only asks
    /// whether authentication is configured, so nothing here resolves.
    fn configure_session_auth(config_dir: &Path) {
        std::fs::create_dir_all(config_dir).expect("create config dir");
        std::fs::write(
            config_dir.join("config.toml"),
            r"
[trace_history]
enabled = false

[auth.authorization_server]
issuer = 'https://auth.example.test'

[auth.session]
signing_key_file = 'session.key'

[auth.provider]
issuer = 'https://accounts.example.test'
client_id = 'upstream-client'
client_secret = 'test-secret'
redirect_uri = 'https://auth.example.test/auth/oidc/callback'
",
        )
        .expect("write auth config");
    }

    /// Both standalone entry points bind a real address, so neither may serve
    /// the local principal to it while the configuration asks for session
    /// authentication.
    /// The regression guard for the CLI: `bootstrap()` starts this server on a
    /// host whose config may well be a `coral server` config.
    #[tokio::test]
    async fn ephemeral_grpc_starts_with_configured_auth() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        configure_session_auth(&config_dir);

        let server = ServerBuilder::new()
            .with_config_dir(config_dir)
            .start()
            .await
            .expect("configured auth must not block the local ephemeral server");

        server.shutdown().await.expect("shutdown server");
    }

    #[test]
    fn explicit_standalone_grpc_does_not_parse_the_configured_bind() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        std::fs::create_dir_all(&config_dir).expect("config dir");
        std::fs::write(
            config_dir.join("config.toml"),
            "[server]\nbind_addr = 'not-an-address'\n",
        )
        .expect("config file");
        let layout = AppStateLayout::discover(Some(config_dir)).expect("layout");
        let explicit_bind = SocketAddr::from((Ipv4Addr::LOCALHOST, 14555));
        let builder = ServerBuilder::standalone_grpc(explicit_bind);

        let ServerMode::StandaloneGrpc { bind } = builder
            .config
            .resolved_mode(&layout)
            .expect("explicit bind overrides configured bind")
        else {
            panic!("explicit standalone mode must remain selected");
        };
        assert_eq!(bind, explicit_bind);
    }

    #[test]
    fn resolves_mcp_http_config_without_starting_grpc() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        std::fs::create_dir_all(&config_dir).expect("config dir");
        std::fs::write(
            config_dir.join("config.toml"),
            "[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:14556'\n",
        )
        .expect("config file");

        let settings = ServerBuilder::configured_standalone_grpc()
            .with_config_dir(config_dir)
            .serve_settings()
            .expect("resolve MCP HTTP config");
        let config = settings.mcp_http().expect("enabled MCP HTTP config");

        assert_eq!(
            config.bind_addr(),
            SocketAddr::from((Ipv4Addr::LOCALHOST, 14556))
        );
    }

    #[tokio::test]
    async fn configured_standalone_grpc_starts_with_configured_bind() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        // Keep this server-start test from populating process-global tracing
        // state and making telemetry unit tests depend on execution order.
        std::fs::write(
            config_dir.join("config.toml"),
            "[server]\nbind_addr = '127.0.0.1:0'\n\n[trace_history]\nenabled = false\n",
        )
        .expect("write config");

        let server = ServerBuilder::configured_standalone_grpc()
            .with_config_dir(config_dir)
            .start()
            .await
            .expect("start configured standalone server");

        let endpoint = server
            .endpoint_uri()
            .strip_prefix("http://")
            .expect("endpoint scheme")
            .parse::<SocketAddr>()
            .expect("socket address endpoint");
        assert_eq!(server.local_addr(), endpoint);
        assert!(endpoint.ip().is_loopback());
        assert_ne!(endpoint.port(), 0);
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
                view: TraceView::Unspecified as i32,
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
            .with_config_dir(config_dir)
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
        let workspace_manager = WorkspaceManager::new_for_tests(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
            None,
            Arc::clone(&db),
        );
        let task_manager = TaskManager::new(TaskStore::new(Arc::clone(&db)));
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
        let trace_service = TraceService::new(TraceManager::new(
            temp.path().join("trace-store"),
            Duration::from_mins(1),
        ));
        let server = start_server(
            ServerDependencies {
                gui_onboarding: GuiOnboardingManager::new(Arc::clone(&db)),
                source: source_manager,
                workspace: workspace_manager,
                query: query_manager,
                search: search_manager,
                search_observations: Some(search_observations),
                feedback: feedback_manager,
                task: task_manager,
                feature_store: FeatureStore::from_layout(layout.clone()),
                active_features: Features::default(),
            },
            TraceServerComponents {
                service: Some(trace_service),
                local_trace_store_dir: None,
                search_response_history: None,
                search_response_history_worker: None,
            },
            Arc::new(LocalPrincipalProvider),
            ServerMode::EphemeralGrpc,
            None,
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
                view: TraceView::Unspecified as i32,
            }))
            .await
            .expect("list traces")
            .into_inner();

        assert!(response.traces.is_empty());
        assert!(response.next_page_token.is_empty());
        server.shutdown().await.expect("shutdown");
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
            .with_principal_provider(Arc::new(RejectingPrincipalProvider))
            .start()
            .await
            .expect("start server");
        let channel = Endpoint::from_shared(server.endpoint_uri().to_string())
            .expect("endpoint")
            .connect()
            .await
            .expect("connect");

        let status = SourceServiceClient::new(channel.clone())
            .list_sources(Request::new(ListSourcesRequest {
                workspace: Some(default_workspace()),
            }))
            .await
            .expect_err("request should be rejected");

        assert_eq!(status.code(), Code::Unauthenticated);

        let status = GuiOnboardingServiceClient::new(channel)
            .get_gui_onboarding_state(Request::new(GetGuiOnboardingStateRequest {}))
            .await
            .expect_err("onboarding request should be rejected");
        assert_eq!(status.code(), Code::Unauthenticated);
        server.shutdown().await.expect("shutdown");
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
        let workspace_manager = WorkspaceManager::new_for_tests(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
            None,
            Arc::clone(&db),
        );
        let task_manager = TaskManager::new(TaskStore::new(Arc::clone(&db)));
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
                gui_onboarding: GuiOnboardingManager::new(Arc::clone(&db)),
                source: source_manager,
                workspace: workspace_manager,
                query: query_manager,
                search: search_manager,
                search_observations: Some(search_observations),
                feedback: feedback_manager,
                task: task_manager,
                feature_store: FeatureStore::from_layout(layout.clone()),
                active_features: Features::default(),
            },
            TraceServerComponents::default(),
            Arc::new(LocalPrincipalProvider),
            ServerMode::EphemeralGrpc,
            None,
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
                guide_read_context: None,
                task_attribution: None,
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
        let workspace_manager = WorkspaceManager::new_for_tests(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
            None,
            Arc::clone(&db),
        );
        let task_manager = TaskManager::new(TaskStore::new(Arc::clone(&db)));
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
                gui_onboarding: GuiOnboardingManager::new(Arc::clone(&db)),
                source: source_manager,
                workspace: workspace_manager,
                query: query_manager,
                search: search_manager,
                search_observations: Some(search_observations),
                feedback: feedback_manager,
                task: task_manager,
                feature_store: FeatureStore::from_layout(layout.clone()),
                active_features: Features::default(),
            },
            TraceServerComponents::default(),
            Arc::new(LocalPrincipalProvider),
            ServerMode::EphemeralGrpc,
            None,
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
                guide_read_context: None,
                task_attribution: None,
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
        let workspace_manager = WorkspaceManager::new_for_tests(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
            None,
            Arc::clone(&db),
        );
        let task_manager = TaskManager::new(TaskStore::new(Arc::clone(&db)));
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
                gui_onboarding: GuiOnboardingManager::new(Arc::clone(&db)),
                source: source_manager,
                workspace: workspace_manager,
                query: query_manager,
                search: search_manager,
                search_observations: Some(search_observations),
                feedback: feedback_manager,
                task: task_manager,
                feature_store: FeatureStore::from_layout(layout.clone()),
                active_features: Features::default(),
            },
            TraceServerComponents::default(),
            Arc::new(LocalPrincipalProvider),
            ServerMode::EphemeralGrpc,
            None,
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
                guide_read_context: None,
                task_attribution: None,
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
