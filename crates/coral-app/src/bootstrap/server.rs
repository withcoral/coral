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
use axum::response::{IntoResponse as _, Response as AxumResponse};
use coral_api::v1::catalog_service_server::CatalogServiceServer;
use coral_api::v1::episode_service_server::EpisodeServiceServer;
use coral_api::v1::feedback_service_server::FeedbackServiceServer;
use coral_api::v1::identity_service_server::IdentityServiceServer;
use coral_api::v1::identity_spec_service_server::IdentitySpecServiceServer;
use coral_api::v1::query_service_server::QueryServiceServer;
use coral_api::v1::source_service_server::SourceServiceServer;
use coral_api::v1::trace_service_server::TraceServiceServer;
use coral_api::{
    CATALOG_RESPONSE_MAX_MESSAGE_SIZE, HTTP2_MAX_HEADER_LIST_SIZE, QUERY_RESPONSE_MAX_MESSAGE_SIZE,
    TRACE_RESPONSE_MAX_MESSAGE_SIZE,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codegen::http::header::CONTENT_TYPE;
use tonic::codegen::http::{HeaderValue, Method, Request, Response, StatusCode};
use tonic::server::NamedService;
use tonic::service::Routes;
use tonic::transport::Server;
use tonic_web::GrpcWebLayer;
use tower::{Layer, Service};

use super::env::AppEnvironment;
use super::error::AppError;
use crate::authorization::{
    AllowAllManagementAuthorizer, AllowAllWorkspaceAuthorizer, ManagementAuthorizer,
    WorkspaceAuthorizer,
};
use crate::catalog::service::CatalogService;
use crate::credentials::config::CredentialStorageConfig;
use crate::credentials::{CredentialManager, CredentialStore};
use crate::episode::service::EpisodeService;
use crate::episode::store::EpisodeStore;
use crate::features::{FeatureOverrides, FeatureStore, Features};
use crate::feedback::manager::FeedbackManager;
use crate::feedback::publisher::{
    FeedbackPublisher, HostedFeedbackPublisher, NoopFeedbackPublisher,
};
use crate::feedback::service::FeedbackService;
use crate::identities::{
    IdentityManagementHandle, IdentityService, UserOwnedIdentityManager, UserOwnedIdentityStore,
};
use crate::identity_specs::{IdentitySpecManager, IdentitySpecRegistry, IdentitySpecService};
use crate::query::manager::QueryManager;
use crate::query::service::QueryService;
use crate::source_registry::SourceRegistry;
use crate::sources::manager::SourceManager;
use crate::sources::service::SourceService;
use crate::state::{AppStateLayout, ConfigStore};
use crate::telemetry::TelemetryConfig;
use crate::telemetry::service::TraceService;
use crate::transport::GrpcMethodAnnotatedService;
use crate::{
    EngineExtensionsProvider, IdentitySpecUsageProvider, SingleUserPrincipalProvider,
    SourceIdentityProvider, UserPrincipalError, UserPrincipalProvider,
};

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

type GrpcRouteExtender = Arc<
    dyn Fn(&ServerExtensionContext, &Arc<dyn UserPrincipalProvider>, Routes) -> Routes
        + Send
        + Sync,
>;
type SourceIdentityProviderFactory =
    Arc<dyn Fn(&ServerExtensionContext) -> Arc<dyn SourceIdentityProvider> + Send + Sync>;

/// Runtime context supplied to product-specific server extensions.
#[derive(Clone)]
pub struct ServerExtensionContext {
    identity_management: IdentityManagementHandle,
}

impl ServerExtensionContext {
    fn new(identity_management: IdentityManagementHandle) -> Self {
        Self {
            identity_management,
        }
    }

    /// Returns the shared identity management handle for this server.
    #[must_use]
    pub fn identity_management(&self) -> &IdentityManagementHandle {
        &self.identity_management
    }
}

/// Concrete local server mode.
///
/// Each variant is a supported product mode instead of an independent
/// transport or asset-serving knob.
#[derive(Clone)]
pub enum ServerMode {
    /// Native gRPC for CLI, MCP, and local client callers.
    NativeGrpc,
    /// Loopback gRPC-Web server that also serves embedded UI assets.
    EmbeddedUi {
        /// Port to bind on `127.0.0.1`.
        port: u16,
        /// Static UI assets served on the same origin as gRPC-Web.
        assets: Arc<dyn StaticAssetsProvider>,
    },
}

impl ServerMode {
    fn bind_addr(&self, native_grpc_bind_addr: Option<SocketAddr>) -> SocketAddr {
        match self {
            Self::NativeGrpc => {
                native_grpc_bind_addr.unwrap_or_else(|| SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            }
            Self::EmbeddedUi { port, .. } => SocketAddr::from((Ipv4Addr::LOCALHOST, *port)),
        }
    }
}

/// Builder for the Coral server runtime.
#[derive(Clone)]
pub struct ServerBuilder {
    // Defaults preserve single-user local-first behavior; see `Self::new`.
    config_dir: Option<PathBuf>,
    mode: ServerMode,
    native_grpc_bind_addr: Option<SocketAddr>,
    engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    source_identity_providers: Vec<Arc<dyn SourceIdentityProvider>>,
    source_identity_provider_factories: Vec<SourceIdentityProviderFactory>,
    identity_spec_usage_providers: Vec<Arc<dyn IdentitySpecUsageProvider>>,
    source_registry: Option<Arc<dyn SourceRegistry>>,
    identity_spec_registry: Option<Arc<dyn IdentitySpecRegistry>>,
    user_owned_identity_store: Option<Arc<dyn UserOwnedIdentityStore>>,
    user_principal_provider: Arc<dyn UserPrincipalProvider>,
    management_authorizer: Arc<dyn ManagementAuthorizer>,
    workspace_authorizer: Arc<dyn WorkspaceAuthorizer>,
    unsafe_public_native_grpc_bind: bool,
    feedback_publisher: Arc<dyn FeedbackPublisher>,
    grpc_route_extenders: Vec<GrpcRouteExtender>,
    enable_stderr_logs: bool,
    feature_overrides: FeatureOverrides,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerBuilder {
    #[must_use]
    /// Creates a builder for the default native gRPC local server.
    pub fn new() -> Self {
        Self {
            config_dir: None,
            mode: ServerMode::NativeGrpc,
            native_grpc_bind_addr: None,
            engine_extensions_providers: Vec::new(),
            source_identity_providers: Vec::new(),
            source_identity_provider_factories: Vec::new(),
            identity_spec_usage_providers: Vec::new(),
            source_registry: None,
            identity_spec_registry: None,
            user_owned_identity_store: None,
            user_principal_provider: Arc::new(SingleUserPrincipalProvider),
            management_authorizer: Arc::new(AllowAllManagementAuthorizer),
            workspace_authorizer: Arc::new(AllowAllWorkspaceAuthorizer),
            unsafe_public_native_grpc_bind: false,
            feedback_publisher: Arc::new(HostedFeedbackPublisher::new()),
            grpc_route_extenders: Vec::new(),
            enable_stderr_logs: false,
            feature_overrides: FeatureOverrides::default(),
        }
    }

    #[must_use]
    /// Creates a builder for a native gRPC local server.
    pub fn native_grpc() -> Self {
        Self::new().with_mode(ServerMode::NativeGrpc)
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
        self.mode = mode;
        self
    }

    #[must_use]
    /// Selects the bind address for native gRPC mode.
    ///
    /// The default native gRPC address remains `127.0.0.1:0` for local-first
    /// OSS callers. Product runtimes that install their own authentication and
    /// management authorization can bind a public address such as `0.0.0.0:0`
    /// only after explicitly calling [`Self::allow_unsafe_public_native_grpc_bind`].
    pub fn with_native_grpc_bind_addr(mut self, bind_addr: SocketAddr) -> Self {
        self.native_grpc_bind_addr = Some(bind_addr);
        self
    }

    #[must_use]
    /// Allows native gRPC to bind a non-loopback address.
    ///
    /// This opt-in exists to keep OSS Coral's allow-all local defaults from
    /// accidentally becoming a network service. Only product runtimes that have
    /// installed request authentication and management authorization should use
    /// it.
    pub fn allow_unsafe_public_native_grpc_bind(mut self) -> Self {
        self.unsafe_public_native_grpc_bind = true;
        self
    }

    #[must_use]
    /// Overrides the Coral config directory used by the local server.
    pub fn with_config_dir(mut self, config_dir: impl Into<PathBuf>) -> Self {
        self.config_dir = Some(config_dir.into());
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
        self.engine_extensions_providers
            .push(engine_extensions_provider);
        self
    }

    #[must_use]
    /// Sets the server-side user principal provider.
    ///
    /// The default provider returns the local single-user principal for every
    /// request. Product-specific siblings can install a provider that
    /// authenticates inbound metadata and selects the querying user for
    /// multi-user servers.
    pub fn with_user_principal_provider(
        mut self,
        user_principal_provider: Arc<dyn UserPrincipalProvider>,
    ) -> Self {
        self.user_principal_provider = user_principal_provider;
        self
    }

    #[must_use]
    /// Sets the product-specific management-plane authorizer.
    ///
    /// The default authorizer allows all source and identity-spec mutations to
    /// preserve OSS single-user behavior. Product runtimes can install a
    /// stricter policy for multi-user control planes.
    pub fn with_management_authorizer(
        mut self,
        management_authorizer: Arc<dyn ManagementAuthorizer>,
    ) -> Self {
        self.management_authorizer = management_authorizer;
        self
    }

    #[must_use]
    /// Sets the product-specific workspace data-access authorizer.
    ///
    /// The default authorizer allows all workspace reads and observational writes
    /// to preserve OSS single-user behavior. Product runtimes can install a
    /// stricter policy for multi-user data planes.
    pub fn with_workspace_authorizer(
        mut self,
        workspace_authorizer: Arc<dyn WorkspaceAuthorizer>,
    ) -> Self {
        self.workspace_authorizer = workspace_authorizer;
        self
    }

    #[must_use]
    /// Adds a provider that can resolve configured source identity bindings.
    pub fn add_source_identity_provider(
        mut self,
        source_identity_provider: Arc<dyn SourceIdentityProvider>,
    ) -> Self {
        self.source_identity_providers
            .push(source_identity_provider);
        self
    }

    #[must_use]
    /// Adds a provider factory that receives server runtime extension context.
    ///
    /// Use this when a product-specific provider needs access to shared
    /// managers created during server startup, such as identity management.
    pub fn add_source_identity_provider_factory(
        mut self,
        source_identity_provider_factory: impl Fn(
            &ServerExtensionContext,
        ) -> Arc<dyn SourceIdentityProvider>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.source_identity_provider_factories
            .push(Arc::new(source_identity_provider_factory));
        self
    }

    #[must_use]
    /// Adds a provider that reports stored identities using installed identity specs.
    ///
    /// OSS Coral stores only user-owned identities, but products built on Coral
    /// can store workspace-owned identities too. Those products should install
    /// a usage provider so identity-spec deletion can reject or report
    /// orphaning for every stored identity owner and type.
    pub fn add_identity_spec_usage_provider(
        mut self,
        identity_spec_usage_provider: Arc<dyn IdentitySpecUsageProvider>,
    ) -> Self {
        self.identity_spec_usage_providers
            .push(identity_spec_usage_provider);
        self
    }

    #[must_use]
    /// Sets the durable registry used for workspace-installed sources.
    ///
    /// The default registry persists source records in local `config.toml`.
    /// Product-specific siblings can install a registry backed by their own
    /// durable control-plane store.
    pub fn with_source_registry(mut self, source_registry: Arc<dyn SourceRegistry>) -> Self {
        self.source_registry = Some(source_registry);
        self
    }

    #[must_use]
    /// Sets the durable registry used for global identity specs.
    ///
    /// The default registry persists identity specs in local app state.
    /// Product-specific siblings can install a registry backed by their own
    /// control-plane store.
    pub fn with_identity_spec_registry(
        mut self,
        identity_spec_registry: Arc<dyn IdentitySpecRegistry>,
    ) -> Self {
        self.identity_spec_registry = Some(identity_spec_registry);
        self
    }

    #[must_use]
    /// Sets the durable store used for user-owned identities and source
    /// identity selections.
    ///
    /// The default store persists identities in local app state.
    /// Product-specific siblings can install a store backed by their own
    /// credential and identity persistence layer. Custom stores must also add
    /// an [`IdentitySpecUsageProvider`] so identity-spec replacement and
    /// deletion cannot orphan externally stored identities.
    pub fn with_user_owned_identity_store(
        mut self,
        user_owned_identity_store: Arc<dyn UserOwnedIdentityStore>,
    ) -> Self {
        self.user_owned_identity_store = Some(user_owned_identity_store);
        self
    }

    #[must_use]
    /// Adds a product-specific gRPC service to the Coral server listener.
    ///
    /// OSS Coral owns the core gRPC service set. Product-specific sibling
    /// crates can use this seam to mount their own generated tonic services onto
    /// the same authenticated gRPC endpoint without adding product concepts to
    /// OSS APIs.
    pub fn add_grpc_service<S>(mut self, service: S) -> Self
    where
        S: Service<Request<tonic::body::Body>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Response: axum::response::IntoResponse,
        S::Future: Send + 'static,
    {
        let service = Arc::new(service);
        self.grpc_route_extenders.push(Arc::new(
            move |_context, user_principal_provider, routes| {
                routes.add_service(AuthenticatedGrpcService::new(
                    (*service).clone(),
                    Arc::clone(user_principal_provider),
                ))
            },
        ));
        self
    }

    #[must_use]
    /// Adds a product-specific gRPC service factory to the Coral listener.
    ///
    /// The factory runs after core server managers are initialized and receives
    /// [`ServerExtensionContext`], allowing product services to reuse shared
    /// Coral managers instead of rebuilding parallel logic.
    pub fn add_grpc_service_factory<F, S>(mut self, factory: F) -> Self
    where
        F: Fn(&ServerExtensionContext) -> S + Send + Sync + 'static,
        S: Service<Request<tonic::body::Body>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Response: axum::response::IntoResponse,
        S::Future: Send + 'static,
    {
        self.grpc_route_extenders.push(Arc::new(
            move |context, user_principal_provider, routes| {
                routes.add_service(AuthenticatedGrpcService::new(
                    factory(context),
                    Arc::clone(user_principal_provider),
                ))
            },
        ));
        self
    }

    #[must_use]
    /// Enables or disables local stderr log rendering for this server.
    ///
    /// `MCP` stdio adapters can enable this for diagnostics while keeping
    /// stdout reserved for protocol messages. Other command surfaces should
    /// leave it disabled and rely on OTEL export for logs.
    pub fn with_stderr_logs(mut self, enable_stderr_logs: bool) -> Self {
        self.enable_stderr_logs = enable_stderr_logs;
        self
    }

    #[must_use]
    /// Applies process-local runtime feature overrides to this local server.
    pub fn with_feature_overrides(mut self, feature_overrides: FeatureOverrides) -> Self {
        self.feature_overrides = feature_overrides;
        self
    }

    /// Disables hosted feedback upload for tests and controlled local harnesses.
    #[doc(hidden)]
    #[must_use]
    pub fn with_noop_feedback_uploads(mut self) -> Self {
        self.feedback_publisher = Arc::new(NoopFeedbackPublisher);
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
        self.ensure_compatible_extension_storage()?;
        let env = AppEnvironment::discover();
        let layout = env.app_state_layout(self.config_dir)?;
        layout.ensure()?;
        let telemetry_config = TelemetryConfig::load(&layout)?;
        let internal_trace_store_dir = telemetry_config
            .trace_history
            .enabled
            .then(|| layout.local_trace_store_dir());
        let installed_trace_store = crate::telemetry::init_tracing(
            &telemetry_config,
            self.enable_stderr_logs,
            internal_trace_store_dir.clone(),
        )?;
        let config_store = ConfigStore::new(layout.clone());
        let source_registry = self
            .source_registry
            .unwrap_or_else(|| Arc::new(config_store.clone()));
        let features =
            FeatureStore::new(layout.clone()).load_with_overrides(&self.feature_overrides)?;
        let credential_config = CredentialStorageConfig::load(&layout)?;
        let credential_store =
            CredentialStore::with_preference(layout.clone(), credential_config.storage);
        let credential_manager = CredentialManager::new(credential_store.clone());
        let source_manager = SourceManager::new_with_features_and_source_registry(
            Arc::clone(&source_registry),
            credential_manager.clone(),
            layout.clone(),
            features.clone(),
        );
        let feedback_manager =
            FeedbackManager::with_publisher(layout.clone(), self.feedback_publisher);
        let episode_store = EpisodeStore::new(layout.clone());
        let body_capture_max_bytes = telemetry_config
            .trace_history
            .http_body_recording_max_bytes();
        let query_runtime_context = env
            .query_runtime_context()
            .with_body_capture_max_bytes(body_capture_max_bytes);
        let identity_spec_manager = identity_spec_manager_for_server(
            &layout,
            &credential_store,
            &features,
            self.identity_spec_registry,
            self.identity_spec_usage_providers,
        );
        let user_owned_identity_manager = if let Some(store) = self.user_owned_identity_store {
            UserOwnedIdentityManager::new_with_store(identity_spec_manager.clone(), store)
        } else {
            UserOwnedIdentityManager::new(
                layout.clone(),
                identity_spec_manager.clone(),
                credential_store,
            )
        };
        let extension_context = ServerExtensionContext::new(user_owned_identity_manager.handle());
        let mut source_identity_providers = self.source_identity_providers;
        for source_identity_provider_factory in self.source_identity_provider_factories {
            source_identity_providers.push(source_identity_provider_factory(&extension_context));
        }
        source_identity_providers.push(Arc::new(user_owned_identity_manager.clone()));

        let query_manager = QueryManager::new_with_features_and_source_registry(
            config_store,
            source_registry,
            credential_manager,
            query_runtime_context,
            layout.clone(),
            self.engine_extensions_providers,
            source_identity_providers,
            features,
        );
        let trace_service = if telemetry_config.trace_history.enabled {
            installed_trace_store.map(|store| TraceService::new(store.dir, store.retention))
        } else {
            None
        };
        start_server(ServerServices {
            source_manager,
            query_manager,
            identity_spec_manager,
            user_owned_identity_manager,
            user_principal_provider: self.user_principal_provider,
            management_authorizer: self.management_authorizer,
            workspace_authorizer: self.workspace_authorizer,
            feedback_manager,
            episode_store,
            trace_service,
            mode: self.mode,
            native_grpc_bind_addr: self.native_grpc_bind_addr,
            grpc_route_extenders: self.grpc_route_extenders,
            extension_context,
        })
        .await
    }

    fn ensure_compatible_extension_storage(&self) -> Result<(), AppError> {
        if let ServerMode::NativeGrpc = self.mode
            && let Some(bind_addr) = self.native_grpc_bind_addr
            && !bind_addr.ip().is_loopback()
            && (!self.unsafe_public_native_grpc_bind
                || self
                    .user_principal_provider
                    .is_default_single_user_provider()
                || self.management_authorizer.is_default_allow_all_authorizer()
                || self.workspace_authorizer.is_default_allow_all_authorizer())
        {
            return Err(AppError::FailedPrecondition(format!(
                "non-loopback native gRPC bind address '{bind_addr}' requires allow_unsafe_public_native_grpc_bind(), a non-default user principal provider, a non-default management authorizer, and a non-default workspace authorizer"
            )));
        }
        if self.identity_spec_registry.is_some() && self.user_owned_identity_store.is_none() {
            return Err(AppError::FailedPrecondition(
                "custom identity spec registries require a custom user-owned identity store"
                    .to_string(),
            ));
        }
        if self.user_owned_identity_store.is_some() && self.identity_spec_usage_providers.is_empty()
        {
            return Err(AppError::FailedPrecondition(
                "custom user-owned identity stores require an identity spec usage provider"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

fn identity_spec_manager_for_server(
    layout: &AppStateLayout,
    credential_store: &CredentialStore,
    features: &Features,
    identity_spec_registry: Option<Arc<dyn IdentitySpecRegistry>>,
    identity_spec_usage_providers: Vec<Arc<dyn IdentitySpecUsageProvider>>,
) -> IdentitySpecManager {
    if let Some(identity_spec_registry) = identity_spec_registry {
        IdentitySpecManager::new_with_registry(
            layout.clone(),
            identity_spec_registry,
            features.clone(),
            identity_spec_usage_providers,
        )
    } else {
        IdentitySpecManager::new_with_credential_store(
            layout.clone(),
            credential_store.clone(),
            features.clone(),
            identity_spec_usage_providers,
        )
    }
}

/// Running Coral server.
///
/// Call [`RunningServer::shutdown`] for deterministic teardown. Dropping this
/// handle sends shutdown to the background task as a best-effort fallback, but
/// does not wait for the task to finish.
pub struct RunningServer {
    endpoint_uri: String,
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
        if let Some(task) = task {
            task.await??;
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

struct ServerServices {
    source_manager: SourceManager,
    query_manager: QueryManager,
    identity_spec_manager: IdentitySpecManager,
    user_owned_identity_manager: UserOwnedIdentityManager,
    user_principal_provider: Arc<dyn UserPrincipalProvider>,
    management_authorizer: Arc<dyn ManagementAuthorizer>,
    workspace_authorizer: Arc<dyn WorkspaceAuthorizer>,
    feedback_manager: FeedbackManager,
    episode_store: EpisodeStore,
    trace_service: Option<TraceService>,
    mode: ServerMode,
    native_grpc_bind_addr: Option<SocketAddr>,
    grpc_route_extenders: Vec<GrpcRouteExtender>,
    extension_context: ServerExtensionContext,
}

async fn start_server(services: ServerServices) -> Result<RunningServer, AppError> {
    let (routes, mode, native_grpc_bind_addr) = build_server_routes(services);

    let listener = TcpListener::bind(mode.bind_addr(native_grpc_bind_addr)).await?;
    let endpoint_uri = format!("http://{}", listener.local_addr()?);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let task = match mode {
        ServerMode::NativeGrpc => start_grpc_server(listener, shutdown_rx, routes),
        ServerMode::EmbeddedUi { assets, .. } => {
            start_grpc_web_server(listener, shutdown_rx, routes, assets)
        }
    };

    Ok(RunningServer {
        endpoint_uri,
        shutdown_tx: Mutex::new(Some(shutdown_tx)),
        task: Mutex::new(Some(task)),
    })
}

fn build_server_routes(services: ServerServices) -> (Routes, ServerMode, Option<SocketAddr>) {
    let ServerServices {
        source_manager,
        query_manager,
        identity_spec_manager,
        user_owned_identity_manager,
        user_principal_provider,
        management_authorizer,
        workspace_authorizer,
        feedback_manager,
        episode_store,
        trace_service,
        mode,
        native_grpc_bind_addr,
        grpc_route_extenders,
        extension_context,
    } = services;
    let source_service = SourceService::new(
        source_manager,
        query_manager.clone(),
        identity_spec_manager.clone(),
        user_owned_identity_manager.clone(),
        Arc::clone(&user_principal_provider),
        Arc::clone(&management_authorizer),
        Arc::clone(&workspace_authorizer),
    );
    let catalog_service = CatalogService::new(
        query_manager.clone(),
        Arc::clone(&user_principal_provider),
        Arc::clone(&workspace_authorizer),
    );
    let identity_service = IdentityService::new(
        user_owned_identity_manager,
        Arc::clone(&user_principal_provider),
    );
    let query_service = QueryService::new(
        query_manager,
        Arc::clone(&user_principal_provider),
        Arc::clone(&workspace_authorizer),
    );
    let identity_spec_service = IdentitySpecService::new(
        identity_spec_manager,
        Arc::clone(&user_principal_provider),
        Arc::clone(&management_authorizer),
    );
    let feedback_service = FeedbackService::new(
        feedback_manager,
        Arc::clone(&user_principal_provider),
        Arc::clone(&workspace_authorizer),
    );
    let episode_service = EpisodeService::new(
        episode_store,
        Arc::clone(&user_principal_provider),
        Arc::clone(&workspace_authorizer),
    );
    let mut routes = Routes::default()
        .add_service(GrpcMethodAnnotatedService::new(SourceServiceServer::new(
            source_service,
        )))
        .add_service(GrpcMethodAnnotatedService::new(
            IdentitySpecServiceServer::new(identity_spec_service),
        ))
        .add_service(GrpcMethodAnnotatedService::new(IdentityServiceServer::new(
            identity_service,
        )))
        .add_service(GrpcMethodAnnotatedService::new(
            CatalogServiceServer::new(catalog_service)
                .max_encoding_message_size(CATALOG_RESPONSE_MAX_MESSAGE_SIZE),
        ))
        .add_service(GrpcMethodAnnotatedService::new(FeedbackServiceServer::new(
            feedback_service,
        )))
        // Registered unconditionally, like `FeedbackService` above: the local
        // transport is feature-agnostic by design (effective features are resolved
        // in `coral-cli`, which gates the *consumers*, not the routes). The
        // `episodes` feature gates the only caller — the `coral-mcp` capture path —
        // so on a default/disabled install this endpoint is reachable but inert:
        // nothing opens an episode, so no intent is ever written. See the
        // `EpisodeService` module docs and `open_episode_*` server tests below.
        .add_service(GrpcMethodAnnotatedService::new(EpisodeServiceServer::new(
            episode_service,
        )))
        .add_service(GrpcMethodAnnotatedService::new(
            QueryServiceServer::new(query_service)
                .max_encoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE),
        ));
    if let Some(trace_service) = trace_service {
        let trace_service =
            trace_service.with_user_principal_provider(Arc::clone(&user_principal_provider));
        routes = routes.add_service(GrpcMethodAnnotatedService::new(
            TraceServiceServer::new(trace_service)
                .max_encoding_message_size(TRACE_RESPONSE_MAX_MESSAGE_SIZE),
        ));
    }
    for extend_routes in grpc_route_extenders {
        routes = extend_routes(&extension_context, &user_principal_provider, routes);
    }

    (routes, mode, native_grpc_bind_addr)
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

#[derive(Clone)]
struct AuthenticatedGrpcService<S> {
    inner: S,
    user_principal_provider: Arc<dyn UserPrincipalProvider>,
}

impl<S> AuthenticatedGrpcService<S> {
    fn new(inner: S, user_principal_provider: Arc<dyn UserPrincipalProvider>) -> Self {
        Self {
            inner,
            user_principal_provider,
        }
    }
}

impl<S> NamedService for AuthenticatedGrpcService<S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
}

impl<S> Service<Request<tonic::body::Body>> for AuthenticatedGrpcService<S>
where
    S: Service<Request<tonic::body::Body>, Error = Infallible> + Clone + Send + 'static,
    S::Response: axum::response::IntoResponse,
    S::Future: Send + 'static,
{
    type Response = AxumResponse;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<tonic::body::Body>) -> Self::Future {
        let metadata = tonic::metadata::MetadataMap::from_headers(request.headers().clone());
        let user_principal_provider = Arc::clone(&self.user_principal_provider);
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            if let Err(error) = user_principal_provider
                .principal_for_metadata(&metadata)
                .await
            {
                return Ok(user_principal_status(error).into_http::<AxumBody>());
            }
            let response = inner.call(request).await?;
            Ok(response.into_response())
        })
    }
}

fn user_principal_status(error: UserPrincipalError) -> tonic::Status {
    match error {
        UserPrincipalError::Unauthenticated(message) => tonic::Status::unauthenticated(message),
        UserPrincipalError::InvalidInput(message) => {
            tonic::Status::invalid_argument(format!("invalid user principal metadata: {message}"))
        }
        UserPrincipalError::Internal(message) => tonic::Status::internal(message),
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
    use std::collections::BTreeMap;
    use std::convert::Infallible;
    use std::future::Future;
    use std::net::{Ipv4Addr, SocketAddr, TcpListener};
    use std::path::Path;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use coral_api::v1::catalog_service_client::CatalogServiceClient;
    use coral_api::v1::episode_service_client::EpisodeServiceClient;
    use coral_api::v1::feedback_service_client::FeedbackServiceClient;
    use coral_api::v1::identity_service_client::IdentityServiceClient;
    use coral_api::v1::identity_spec_service_client::IdentitySpecServiceClient;
    use coral_api::v1::query_service_client::QueryServiceClient;
    use coral_api::v1::source_service_client::SourceServiceClient;
    use coral_api::v1::trace_service_client::TraceServiceClient;
    use coral_api::v1::{
        AddIdentitySpecRequest, CreateBundledSourceRequest, DeleteIdentitySpecRequest,
        ExecuteSqlRequest, ExecuteSqlResponse, ImportSourceRequest, ImportSourceResponse,
        ListCatalogRequest, ListIdentitySpecsRequest, ListSourcesRequest, ListTracesRequest,
        ListTracesResponse, ListUserOwnedIdentitiesRequest, OAuthCredentialRetrieval,
        OpenEpisodeRequest, SubmitFeedbackRequest, Workspace, import_source_response,
    };
    use coral_api::{HTTP2_MAX_HEADER_LIST_SIZE, QUERY_RESPONSE_MAX_MESSAGE_SIZE};
    use coral_engine::QueryRuntimeContext;

    use crate::authorization::{
        AllowAllManagementAuthorizer, AllowAllWorkspaceAuthorizer, AuthorizationError,
        ManagementAuthorizer, SourceMutationKind, WorkspaceAccessKind, WorkspaceAuthorizer,
    };
    use crate::identities::{
        IdentityOwnerKey, UserOwnedIdentityManager, UserOwnedIdentityMaterialGuard,
        UserOwnedIdentityRecord, UserOwnedIdentityStore,
    };
    use crate::identity_specs::{
        IdentitySpecManager, IdentitySpecRegistry, IdentitySpecRegistryRecord,
        IdentitySpecUsageProvider,
    };
    use tempfile::TempDir;
    use tonic::body::Body as TonicBody;
    use tonic::codegen::http::{Request as HttpRequest, Response as HttpResponse};
    use tonic::server::NamedService;
    use tonic::transport::{Channel, Endpoint};
    use tonic::{Code, Request};
    use tower::{Service, ServiceExt as _};

    use super::{
        RunningServer, ServerBuilder, ServerExtensionContext, ServerMode, ServerServices,
        StaticAsset, StaticAssetsProvider, is_grpc_web_content_type, is_native_grpc_content_type,
        start_server,
    };
    use crate::bootstrap::AppError;
    use crate::credentials::{CredentialManager, CredentialStore};
    use crate::episode::store::EpisodeStore;
    use crate::features::{Feature, FeatureOverrides};
    use crate::feedback::manager::FeedbackManager;
    use crate::query::manager::QueryManager;
    use crate::sources::manager::SourceManager;
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::telemetry::service::TraceService;
    use crate::transport::workspace_to_proto;
    use crate::workspaces::WorkspaceName;
    use crate::{
        AwsEngineExtensionsProvider, NoopEngineExtensionsProvider, SingleUserPrincipalProvider,
        UserPrincipal, UserPrincipalError, UserPrincipalProvider,
    };

    fn default_workspace() -> Workspace {
        workspace_to_proto(&WorkspaceName::default())
    }

    #[derive(Debug)]
    struct RejectingUserPrincipalProvider;

    #[tonic::async_trait]
    impl UserPrincipalProvider for RejectingUserPrincipalProvider {
        async fn principal_for_metadata(
            &self,
            _metadata: &tonic::metadata::MetadataMap,
        ) -> Result<UserPrincipal, UserPrincipalError> {
            Err(UserPrincipalError::unauthenticated(
                "rejected user principal",
            ))
        }
    }

    #[derive(Clone)]
    struct ExtensionProbeService;

    impl NamedService for ExtensionProbeService {
        const NAME: &'static str = "coral.test.ExtensionProbe";
    }

    impl Service<HttpRequest<TonicBody>> for ExtensionProbeService {
        type Response = HttpResponse<TonicBody>;
        type Error = Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: HttpRequest<TonicBody>) -> Self::Future {
            std::future::ready(Ok(
                tonic::Status::unimplemented("extension reached").into_http()
            ))
        }
    }

    #[derive(Debug)]
    struct EmptyIdentitySpecRegistry;

    impl IdentitySpecRegistry for EmptyIdentitySpecRegistry {
        fn list_identity_specs(&self) -> Result<Vec<IdentitySpecRegistryRecord>, AppError> {
            Ok(Vec::new())
        }

        fn get_identity_spec(
            &self,
            _name: &str,
        ) -> Result<Option<IdentitySpecRegistryRecord>, AppError> {
            Ok(None)
        }

        fn upsert_identity_spec(
            &self,
            _name: &str,
            _record: IdentitySpecRegistryRecord,
        ) -> Result<(), AppError> {
            Ok(())
        }

        fn remove_identity_spec(&self, _name: &str) -> Result<(), AppError> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct EmptyUserOwnedIdentityStore;

    #[tonic::async_trait]
    impl UserOwnedIdentityStore for EmptyUserOwnedIdentityStore {
        async fn list_identities(
            &self,
            _owner: &IdentityOwnerKey,
        ) -> Result<Vec<UserOwnedIdentityRecord>, AppError> {
            Ok(Vec::new())
        }

        async fn load_identity(
            &self,
            _owner: &IdentityOwnerKey,
            _identity_name: &str,
        ) -> Result<Option<UserOwnedIdentityRecord>, AppError> {
            Ok(None)
        }

        async fn replace_identity(
            &self,
            _owner: &IdentityOwnerKey,
            _record: &UserOwnedIdentityRecord,
            _material: &BTreeMap<String, String>,
        ) -> Result<(), AppError> {
            Ok(())
        }

        async fn delete_identity(
            &self,
            _owner: &IdentityOwnerKey,
            _identity_name: &str,
        ) -> Result<bool, AppError> {
            Ok(false)
        }

        async fn material_guard(
            &self,
            _owner: &IdentityOwnerKey,
            _identity_name: &str,
        ) -> Result<Box<dyn UserOwnedIdentityMaterialGuard>, AppError> {
            Ok(Box::new(EmptyUserOwnedIdentityMaterialGuard))
        }

        async fn delete_source_identity_bindings(
            &self,
            _workspace_name: &str,
            _source_name: &str,
            _surface_ids: &[String],
            _preserved_user_id: Option<&str>,
        ) -> Result<(), AppError> {
            Ok(())
        }
    }

    struct EmptyUserOwnedIdentityMaterialGuard;

    #[tonic::async_trait]
    impl UserOwnedIdentityMaterialGuard for EmptyUserOwnedIdentityMaterialGuard {
        async fn read_material(&self) -> Result<BTreeMap<String, String>, AppError> {
            Ok(BTreeMap::new())
        }

        async fn write_material(
            &self,
            _material: &BTreeMap<String, String>,
        ) -> Result<(), AppError> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct DenyingManagementAuthorizer;

    #[tonic::async_trait]
    impl ManagementAuthorizer for DenyingManagementAuthorizer {
        async fn authorize_identity_spec_mutation(
            &self,
            _principal: &UserPrincipal,
        ) -> Result<(), AuthorizationError> {
            Err(AuthorizationError::forbidden("identity specs are blocked"))
        }

        async fn authorize_source_mutation(
            &self,
            _principal: &UserPrincipal,
            _workspace_id: &str,
            _kind: SourceMutationKind,
        ) -> Result<(), AuthorizationError> {
            Err(AuthorizationError::forbidden("sources are blocked"))
        }
    }

    #[derive(Debug)]
    struct DenyIdentitySpecAllowSourcesAuthorizer;

    #[tonic::async_trait]
    impl ManagementAuthorizer for DenyIdentitySpecAllowSourcesAuthorizer {
        async fn authorize_identity_spec_mutation(
            &self,
            _principal: &UserPrincipal,
        ) -> Result<(), AuthorizationError> {
            Err(AuthorizationError::forbidden("identity specs are blocked"))
        }

        async fn authorize_source_mutation(
            &self,
            _principal: &UserPrincipal,
            _workspace_id: &str,
            _kind: SourceMutationKind,
        ) -> Result<(), AuthorizationError> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct DenyImportWithOAuthAuthorizer;

    #[tonic::async_trait]
    impl ManagementAuthorizer for DenyImportWithOAuthAuthorizer {
        async fn authorize_identity_spec_mutation(
            &self,
            _principal: &UserPrincipal,
        ) -> Result<(), AuthorizationError> {
            Ok(())
        }

        async fn authorize_source_mutation(
            &self,
            _principal: &UserPrincipal,
            _workspace_id: &str,
            kind: SourceMutationKind,
        ) -> Result<(), AuthorizationError> {
            if kind == SourceMutationKind::ImportWithOAuth {
                return Err(AuthorizationError::forbidden("OAuth imports are blocked"));
            }
            Ok(())
        }
    }

    #[derive(Debug)]
    struct DenyingWorkspaceAuthorizer;

    #[tonic::async_trait]
    impl WorkspaceAuthorizer for DenyingWorkspaceAuthorizer {
        async fn authorize_workspace_access(
            &self,
            _principal: &UserPrincipal,
            _workspace_id: &str,
            _kind: WorkspaceAccessKind,
        ) -> Result<(), AuthorizationError> {
            Err(AuthorizationError::forbidden("workspace access is blocked"))
        }
    }

    #[derive(Debug)]
    struct AllowAllNonDefaultWorkspaceAuthorizer;

    #[tonic::async_trait]
    impl WorkspaceAuthorizer for AllowAllNonDefaultWorkspaceAuthorizer {
        async fn authorize_workspace_access(
            &self,
            _principal: &UserPrincipal,
            _workspace_id: &str,
            _kind: WorkspaceAccessKind,
        ) -> Result<(), AuthorizationError> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct StaticIdentitySpecUsageProvider {
        identity_spec_name: String,
        count: u32,
    }

    impl IdentitySpecUsageProvider for StaticIdentitySpecUsageProvider {
        fn count_identities_for_spec(&self, identity_spec_name: &str) -> Result<u32, AppError> {
            if identity_spec_name == self.identity_spec_name {
                Ok(self.count)
            } else {
                Ok(0)
            }
        }
    }

    fn fixed_token_identity_spec_yaml(name: &str) -> String {
        format!(
            r"
kind: identity
spec_version: 1
name: {name}
version: 0.1.0
description: Demo identity.
issuer: github
type: fixed_token
audience:
  host: github.com
"
        )
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

    /// Starts a native-gRPC server whose services are all built from a fresh
    /// layout under `temp`, returning it with a connected channel.
    async fn start_test_server(
        temp: &TempDir,
        runtime_context: QueryRuntimeContext,
        trace_service: Option<TraceService>,
    ) -> (RunningServer, Channel) {
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store.clone());
        let identity_spec_manager = IdentitySpecManager::new(layout.clone());
        let user_owned_identity_manager = UserOwnedIdentityManager::new(
            layout.clone(),
            identity_spec_manager.clone(),
            credential_store,
        );
        let extension_context = ServerExtensionContext::new(user_owned_identity_manager.handle());
        let server = start_server(ServerServices {
            source_manager: SourceManager::new(
                config_store.clone(),
                credential_manager.clone(),
                layout.clone(),
            ),
            query_manager: QueryManager::new(
                config_store,
                credential_manager,
                runtime_context,
                layout.clone(),
                vec![Arc::new(NoopEngineExtensionsProvider)],
            ),
            identity_spec_manager: identity_spec_manager.clone(),
            user_owned_identity_manager,
            user_principal_provider: Arc::new(SingleUserPrincipalProvider),
            management_authorizer: Arc::new(AllowAllManagementAuthorizer),
            workspace_authorizer: Arc::new(AllowAllWorkspaceAuthorizer),
            feedback_manager: FeedbackManager::new(layout.clone()),
            episode_store: EpisodeStore::new(layout.clone()),
            trace_service,
            mode: ServerMode::NativeGrpc,
            native_grpc_bind_addr: None,
            grpc_route_extenders: Vec::new(),
            extension_context,
        })
        .await
        .expect("start server");
        let channel = connect(&server).await;
        (server, channel)
    }

    /// Connects with the client-side header budget raised to the app's limit.
    async fn connect(server: &RunningServer) -> Channel {
        Endpoint::from_shared(server.endpoint_uri().to_string())
            .expect("endpoint")
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE)
            .connect()
            .await
            .expect("connect")
    }

    /// An `ImportSourceRequest` for the default workspace with no bindings.
    fn import_source_request(manifest_yaml: String) -> ImportSourceRequest {
        ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml,
            ..ImportSourceRequest::default()
        }
    }

    /// Imports a manifest over gRPC and returns the imported source's name.
    async fn import_source_name_over_grpc(
        source_client: &mut SourceServiceClient<Channel>,
        manifest_yaml: String,
    ) -> String {
        let mut import_stream = source_client
            .import_source(Request::new(import_source_request(manifest_yaml)))
            .await
            .expect("create source")
            .into_inner();
        import_stream
            .message()
            .await
            .expect("import source stream")
            .and_then(|response| match response.event {
                Some(import_source_response::Event::Source(source)) => Some(source),
                _ => None,
            })
            .expect("import source response")
            .name
    }

    fn list_sources_request() -> ListSourcesRequest {
        ListSourcesRequest {
            workspace: Some(default_workspace()),
        }
    }

    /// Issues `ExecuteSql` for `sql` against the default workspace.
    async fn execute_sql(
        client: &mut QueryServiceClient<Channel>,
        sql: &str,
    ) -> Result<tonic::Response<ExecuteSqlResponse>, tonic::Status> {
        client
            .execute_sql(Request::new(ExecuteSqlRequest {
                workspace: Some(default_workspace()),
                sql: sql.to_string(),
            }))
            .await
    }

    /// Issues a default first-page `ListTraces` request.
    async fn list_traces(
        client: &mut TraceServiceClient<Channel>,
    ) -> Result<tonic::Response<ListTracesResponse>, tonic::Status> {
        client
            .list_traces(Request::new(ListTracesRequest {
                page_size: 10,
                page_token: String::new(),
            }))
            .await
    }

    /// Asserts that `call` is rejected with `Unauthenticated`.
    async fn expect_unauthenticated<T: std::fmt::Debug>(
        label: &str,
        call: impl Future<Output = Result<T, tonic::Status>>,
    ) {
        let status = call
            .await
            .expect_err(&format!("{label} should require a request principal"));
        assert_eq!(status.code(), Code::Unauthenticated, "{label}");
    }

    /// Asserts that `call` is rejected with `PermissionDenied`.
    async fn expect_permission_denied<T: std::fmt::Debug>(
        label: &str,
        call: impl Future<Output = Result<T, tonic::Status>>,
    ) {
        let status = call
            .await
            .expect_err(&format!("{label} should require workspace access"));
        assert_eq!(status.code(), Code::PermissionDenied, "{label}");
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
        let mut trace_client = TraceServiceClient::new(connect(&server).await);

        let status = list_traces(&mut trace_client)
            .await
            .expect_err("trace service should be disabled");

        assert_eq!(status.code(), Code::Unimplemented);
        server.shutdown().await.expect("shutdown");
    }

    /// The `OpenEpisode` route is registered unconditionally — the transport is
    /// feature-agnostic, and the `episodes` feature gates the `coral-mcp` consumer
    /// rather than the route. Drive the full path end-to-end through a real
    /// `EpisodeServiceClient` on a default install (episodes disabled) and confirm
    /// the call is served and the intent is persisted. Guards against a dropped or
    /// miswired `EpisodeServiceServer` route, which the direct-`EpisodeService`
    /// unit tests cannot catch.
    #[tokio::test]
    async fn open_episode_through_server_persists() {
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
        let mut episode_client = EpisodeServiceClient::new(channel);

        episode_client
            .open_episode(Request::new(OpenEpisodeRequest {
                workspace: Some(default_workspace()),
                episode_id: "ep_smoke".to_string(),
                intent: "find the HR onboarding form".to_string(),
                parent_episode_id: String::new(),
            }))
            .await
            .expect("OpenEpisode is served regardless of the episodes feature");

        // The handler ran the full path through to the per-workspace episode log.
        let layout = AppStateLayout::discover(Some(config_dir)).expect("layout");
        let raw = std::fs::read_to_string(layout.episodes_file(&WorkspaceName::default()))
            .expect("episode file should exist");
        assert!(raw.contains("ep_smoke"), "episode id should be persisted");
        assert!(
            raw.contains("find the HR onboarding form"),
            "intent should be persisted"
        );
        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn trace_service_lists_empty_store() {
        let temp = TempDir::new().expect("temp dir");
        let trace_service =
            TraceService::new(temp.path().join("trace-store"), Duration::from_mins(1));
        let (server, channel) =
            start_test_server(&temp, QueryRuntimeContext::default(), Some(trace_service)).await;
        let mut trace_client = TraceServiceClient::new(channel);

        let response = list_traces(&mut trace_client)
            .await
            .expect("list traces")
            .into_inner();

        assert!(response.traces.is_empty());
        assert!(response.next_page_token.is_empty());
        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn server_builder_installs_identity_spec_management_authorizer() {
        let temp = TempDir::new().expect("temp dir");
        let server = ServerBuilder::new()
            .with_config_dir(temp.path().join("coral-config"))
            .with_management_authorizer(Arc::new(DenyingManagementAuthorizer))
            .start()
            .await
            .expect("start server");
        let mut client = IdentitySpecServiceClient::new(connect(&server).await);

        let status = client
            .add_identity_spec(Request::new(AddIdentitySpecRequest {
                manifest_yaml: String::new(),
                inputs: Vec::new(),
            }))
            .await
            .expect_err("identity spec mutation should be denied");

        assert_eq!(status.code(), Code::PermissionDenied);
        assert!(status.message().contains("identity specs are blocked"));
        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn server_builder_installs_source_management_authorizer() {
        let temp = TempDir::new().expect("temp dir");
        let server = ServerBuilder::new()
            .with_config_dir(temp.path().join("coral-config"))
            .with_management_authorizer(Arc::new(DenyingManagementAuthorizer))
            .start()
            .await
            .expect("start server");
        let mut client = SourceServiceClient::new(connect(&server).await);

        let status = client
            .create_bundled_source(Request::new(CreateBundledSourceRequest {
                workspace: Some(default_workspace()),
                name: "github".to_string(),
                variables: Vec::new(),
                secrets: Vec::new(),
            }))
            .await
            .expect_err("source mutation should be denied");

        assert_eq!(status.code(), Code::PermissionDenied);
        assert!(status.message().contains("sources are blocked"));
        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn source_import_with_identity_specs_requires_identity_spec_authorization() {
        let temp = TempDir::new().expect("temp dir");
        let server = ServerBuilder::new()
            .with_config_dir(temp.path().join("coral-config"))
            .with_management_authorizer(Arc::new(DenyIdentitySpecAllowSourcesAuthorizer))
            .start()
            .await
            .expect("start server");
        let mut client = SourceServiceClient::new(connect(&server).await);
        let mut request = import_source_request("not valid yaml".to_string());
        request
            .identity_spec_manifest_yamls
            .push(fixed_token_identity_spec_yaml("github_oauth"));

        let status = client
            .import_source(Request::new(request))
            .await
            .expect_err("identity spec mutation should be denied before import starts");

        assert_eq!(status.code(), Code::PermissionDenied);
        assert!(status.message().contains("identity specs are blocked"));
        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn source_import_with_oauth_uses_distinct_authorization_kind() {
        let temp = TempDir::new().expect("temp dir");
        let server = ServerBuilder::new()
            .with_config_dir(temp.path().join("coral-config"))
            .with_management_authorizer(Arc::new(DenyImportWithOAuthAuthorizer))
            .start()
            .await
            .expect("start server");
        let mut client = SourceServiceClient::new(connect(&server).await);
        let mut request = import_source_request("not valid yaml".to_string());
        request
            .oauth_credential_retrievals
            .push(OAuthCredentialRetrieval {
                input_key: "API_TOKEN".to_string(),
                method_index: Some(0),
                credential_inputs: Vec::new(),
            });

        let status = client
            .import_source(Request::new(request))
            .await
            .expect_err("OAuth import should be denied before import starts");

        assert_eq!(status.code(), Code::PermissionDenied);
        assert!(status.message().contains("OAuth imports are blocked"));
        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn server_builder_installs_identity_spec_usage_provider() {
        let temp = TempDir::new().expect("temp dir");
        let mut feature_overrides = FeatureOverrides::default();
        feature_overrides.set(Feature::DslV4, true);
        let server = ServerBuilder::new()
            .with_config_dir(temp.path().join("coral-config"))
            .with_feature_overrides(feature_overrides)
            .add_identity_spec_usage_provider(Arc::new(StaticIdentitySpecUsageProvider {
                identity_spec_name: "github_oauth".to_string(),
                count: 4,
            }))
            .start()
            .await
            .expect("start server");
        let mut client = IdentitySpecServiceClient::new(connect(&server).await);
        client
            .add_identity_spec(Request::new(AddIdentitySpecRequest {
                manifest_yaml: fixed_token_identity_spec_yaml("github_oauth"),
                inputs: Vec::new(),
            }))
            .await
            .expect("add identity spec");

        let status = client
            .delete_identity_spec(Request::new(DeleteIdentitySpecRequest {
                name: "github_oauth".to_string(),
                force: false,
            }))
            .await
            .expect_err("usage provider should block non-forced deletion");

        assert_eq!(status.code(), Code::FailedPrecondition);
        assert!(status.message().contains("4 stored identities"));
        let deleted = client
            .delete_identity_spec(Request::new(DeleteIdentitySpecRequest {
                name: "github_oauth".to_string(),
                force: true,
            }))
            .await
            .expect("force delete identity spec")
            .into_inner();
        assert_eq!(deleted.orphaned_identities, 4);
        server.shutdown().await.expect("shutdown");
    }

    fn grpc_web_body(message: &impl prost::Message) -> Vec<u8> {
        let encoded = message.encode_to_vec();
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
            let (bytes, content_type): (&'static [u8], &'static str) = match path {
                "" | "index.html" => (
                    b"<html><body>Coral UI</body></html>",
                    "text/html; charset=utf-8",
                ),
                "assets/app.js" => (b"console.log('coral')", "application/javascript"),
                _ => return None,
            };
            Some(StaticAsset {
                bytes: Cow::Borrowed(bytes),
                content_type: Cow::Borrowed(content_type),
            })
        }
    }

    /// Starts an embedded-UI loopback server that serves [`StubAssets`].
    async fn start_embedded_ui_server() -> (TempDir, RunningServer, reqwest::Client) {
        let temp = TempDir::new().expect("temp dir");
        let running = ServerBuilder::embedded_ui_loopback(0, Arc::new(StubAssets))
            .with_config_dir(temp.path().join("coral-config"))
            .start()
            .await
            .expect("start embedded UI server");
        (temp, running, reqwest::Client::new())
    }

    /// POSTs a gRPC-Web framed `message` to `path`.
    async fn post_grpc_web(
        client: &reqwest::Client,
        path: &str,
        message: &impl prost::Message,
    ) -> reqwest::Response {
        client
            .post(path)
            .header("content-type", "application/grpc-web+proto")
            .header("x-grpc-web", "1")
            .body(grpc_web_body(message))
            .send()
            .await
            .expect("gRPC-Web request")
    }

    #[test]
    fn server_builder_accepts_engine_extensions_providers() {
        let _builder = ServerBuilder::new()
            .add_engine_extensions_provider(Arc::new(AwsEngineExtensionsProvider))
            .add_engine_extensions_provider(Arc::new(NoopEngineExtensionsProvider));
    }

    #[tokio::test]
    async fn server_builder_rejects_public_native_grpc_bind_without_auth_overrides() {
        let temp = TempDir::new().expect("temp dir");
        let result = ServerBuilder::new()
            .with_config_dir(temp.path().join("coral-config"))
            .with_native_grpc_bind_addr(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
            .start()
            .await;

        let Err(error) = result else {
            panic!("server should reject public native gRPC bind with default auth");
        };
        assert!(
            error.to_string().contains(
                "non-loopback native gRPC bind address '0.0.0.0:0' requires allow_unsafe_public_native_grpc_bind(), a non-default user principal provider, a non-default management authorizer, and a non-default workspace authorizer"
            ),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn server_builder_rejects_public_native_grpc_bind_with_unsafe_opt_in_and_local_defaults()
    {
        let temp = TempDir::new().expect("temp dir");
        let result = ServerBuilder::new()
            .with_config_dir(temp.path().join("coral-config"))
            .with_native_grpc_bind_addr(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
            .allow_unsafe_public_native_grpc_bind()
            .with_user_principal_provider(Arc::new(SingleUserPrincipalProvider))
            .with_management_authorizer(Arc::new(AllowAllManagementAuthorizer))
            .with_workspace_authorizer(Arc::new(AllowAllWorkspaceAuthorizer))
            .start()
            .await;

        let Err(error) = result else {
            panic!("server should reject public native gRPC bind with local default auth");
        };
        assert!(
            error.to_string().contains(
                "non-loopback native gRPC bind address '0.0.0.0:0' requires allow_unsafe_public_native_grpc_bind(), a non-default user principal provider, a non-default management authorizer, and a non-default workspace authorizer"
            ),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn server_builder_accepts_public_native_grpc_bind_with_explicit_opt_in() {
        let temp = TempDir::new().expect("temp dir");
        let server = ServerBuilder::new()
            .with_config_dir(temp.path().join("coral-config"))
            .with_native_grpc_bind_addr(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
            .allow_unsafe_public_native_grpc_bind()
            .with_user_principal_provider(Arc::new(RejectingUserPrincipalProvider))
            .with_management_authorizer(Arc::new(DenyingManagementAuthorizer))
            .with_workspace_authorizer(Arc::new(AllowAllNonDefaultWorkspaceAuthorizer))
            .start()
            .await
            .expect("public bind should start once auth is explicit");

        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn server_builder_rejects_custom_user_owned_identity_store_without_usage_provider() {
        let temp = TempDir::new().expect("temp dir");
        let result = ServerBuilder::new()
            .with_config_dir(temp.path().join("coral-config"))
            .with_user_owned_identity_store(Arc::new(EmptyUserOwnedIdentityStore))
            .start()
            .await;

        let Err(error) = result else {
            panic!("server should reject custom identity store without usage provider");
        };
        assert!(
            error.to_string().contains(
                "custom user-owned identity stores require an identity spec usage provider"
            ),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn server_builder_accepts_custom_user_owned_identity_store_with_usage_provider() {
        let temp = TempDir::new().expect("temp dir");
        let server = ServerBuilder::new()
            .with_config_dir(temp.path().join("coral-config"))
            .with_user_owned_identity_store(Arc::new(EmptyUserOwnedIdentityStore))
            .add_identity_spec_usage_provider(Arc::new(StaticIdentitySpecUsageProvider {
                identity_spec_name: "demo_oauth".to_string(),
                count: 0,
            }))
            .start()
            .await
            .expect("custom identity store should start with usage provider");

        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn grpc_services_reject_unauthenticated_requests() {
        let temp = TempDir::new().expect("temp dir");
        let server = ServerBuilder::new()
            .with_config_dir(temp.path().join("coral-config"))
            .with_user_principal_provider(Arc::new(RejectingUserPrincipalProvider))
            .start()
            .await
            .expect("start server");
        let channel = connect(&server).await;
        let mut source_client = SourceServiceClient::new(channel.clone());
        let mut query_client = QueryServiceClient::new(channel.clone());
        let mut catalog_client = CatalogServiceClient::new(channel.clone());
        let mut identity_spec_client = IdentitySpecServiceClient::new(channel.clone());
        let mut identity_client = IdentityServiceClient::new(channel.clone());
        let mut feedback_client = FeedbackServiceClient::new(channel.clone());
        let mut episode_client = EpisodeServiceClient::new(channel.clone());
        let mut trace_client = TraceServiceClient::new(channel);

        expect_unauthenticated(
            "source list",
            source_client.list_sources(list_sources_request()),
        )
        .await;
        expect_unauthenticated("query", execute_sql(&mut query_client, "SELECT 1")).await;
        expect_unauthenticated(
            "catalog",
            catalog_client.list_catalog(ListCatalogRequest {
                workspace: Some(default_workspace()),
                ..ListCatalogRequest::default()
            }),
        )
        .await;
        expect_unauthenticated(
            "identity specs",
            identity_spec_client.list_identity_specs(ListIdentitySpecsRequest {}),
        )
        .await;
        expect_unauthenticated(
            "identity list",
            identity_client.list_user_owned_identities(ListUserOwnedIdentitiesRequest {}),
        )
        .await;
        expect_unauthenticated(
            "feedback",
            feedback_client.submit_feedback(SubmitFeedbackRequest {
                workspace: Some(default_workspace()),
                trying_to_do: "test".to_string(),
                tried: "test".to_string(),
                stuck: "test".to_string(),
            }),
        )
        .await;
        expect_unauthenticated(
            "episodes",
            episode_client.open_episode(OpenEpisodeRequest {
                workspace: Some(default_workspace()),
                episode_id: "ep_auth".to_string(),
                intent: "test auth".to_string(),
                parent_episode_id: String::new(),
            }),
        )
        .await;
        expect_unauthenticated("traces", list_traces(&mut trace_client)).await;

        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn workspace_scoped_services_reject_unauthorized_workspace_access() {
        let temp = TempDir::new().expect("temp dir");
        let server = ServerBuilder::new()
            .with_config_dir(temp.path().join("coral-config"))
            .with_workspace_authorizer(Arc::new(DenyingWorkspaceAuthorizer))
            .start()
            .await
            .expect("start server");
        let channel = connect(&server).await;
        let mut source_client = SourceServiceClient::new(channel.clone());
        let mut query_client = QueryServiceClient::new(channel.clone());
        let mut catalog_client = CatalogServiceClient::new(channel.clone());
        let mut feedback_client = FeedbackServiceClient::new(channel.clone());
        let mut episode_client = EpisodeServiceClient::new(channel);

        expect_permission_denied(
            "source list",
            source_client.list_sources(list_sources_request()),
        )
        .await;
        expect_permission_denied("query", execute_sql(&mut query_client, "SELECT 1")).await;
        expect_permission_denied(
            "catalog",
            catalog_client.list_catalog(ListCatalogRequest {
                workspace: Some(default_workspace()),
                ..ListCatalogRequest::default()
            }),
        )
        .await;
        expect_permission_denied(
            "feedback",
            feedback_client.submit_feedback(SubmitFeedbackRequest {
                workspace: Some(default_workspace()),
                trying_to_do: "test".to_string(),
                tried: "test".to_string(),
                stuck: "test".to_string(),
            }),
        )
        .await;
        expect_permission_denied(
            "episodes",
            episode_client.open_episode(OpenEpisodeRequest {
                workspace: Some(default_workspace()),
                episode_id: "ep_authz".to_string(),
                intent: "test authz".to_string(),
                parent_episode_id: String::new(),
            }),
        )
        .await;

        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn extension_grpc_services_reject_unauthenticated_requests() {
        let temp = TempDir::new().expect("temp dir");
        let server = ServerBuilder::new()
            .with_config_dir(temp.path().join("coral-config"))
            .with_user_principal_provider(Arc::new(RejectingUserPrincipalProvider))
            .add_grpc_service(ExtensionProbeService)
            .start()
            .await
            .expect("start server");
        let mut channel = connect(&server).await;
        let request = HttpRequest::builder()
            .method("POST")
            .uri("/coral.test.ExtensionProbe/Check")
            .header("content-type", "application/grpc")
            .body(TonicBody::empty())
            .expect("extension request");

        let response = channel
            .ready()
            .await
            .expect("extension channel ready")
            .call(request)
            .await
            .expect("extension auth response");

        assert_eq!(
            response
                .headers()
                .get("grpc-status")
                .and_then(|value| value.to_str().ok()),
            Some("16")
        );
        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn server_builder_rejects_custom_identity_spec_registry_without_custom_identity_store() {
        let temp = TempDir::new().expect("temp dir");
        let result = ServerBuilder::new()
            .with_config_dir(temp.path().join("coral-config"))
            .with_identity_spec_registry(Arc::new(EmptyIdentitySpecRegistry))
            .start()
            .await;

        let Err(error) = result else {
            panic!("server should reject incompatible identity spec registry/store configuration");
        };
        assert!(
            error.to_string().contains(
                "custom identity spec registries require a custom user-owned identity store"
            ),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn native_grpc_mode_uses_configured_bind_addr() {
        let bind_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0));

        assert_eq!(ServerMode::NativeGrpc.bind_addr(Some(bind_addr)), bind_addr);
        assert_eq!(
            ServerMode::NativeGrpc.bind_addr(None),
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
        );
    }

    #[test]
    fn native_grpc_content_type_detection_excludes_grpc_web() {
        for (header, native) in [
            ("application/grpc", true),
            ("application/grpc+proto; charset=utf-8", true),
            ("application/grpc-web+proto", false),
        ] {
            let v = header.parse().expect("header");
            assert_eq!(is_native_grpc_content_type(Some(&v)), native, "{header}");
        }
    }

    #[test]
    fn grpc_web_content_type_detection_accepts_grpc_web() {
        for (header, web) in [
            ("application/grpc-web", true),
            ("application/grpc-web+proto; charset=utf-8", true),
            ("application/grpc+proto", false),
        ] {
            let v = header.parse().expect("header");
            assert_eq!(is_grpc_web_content_type(Some(&v)), web, "{header}");
        }
    }

    #[tokio::test]
    async fn embedded_ui_server_accepts_browser_requests_and_rejects_native_grpc() {
        let (_temp, running, client) = start_embedded_ui_server().await;
        let endpoint = running.endpoint_uri();
        let path = format!("{endpoint}/coral.v1.SourceService/ListSources");

        let response = post_grpc_web(&client, &path, &list_sources_request()).await;
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
        let (_temp, running, client) = start_embedded_ui_server().await;
        let endpoint = running.endpoint_uri();
        let path = format!("{endpoint}/coral.v1.SourceService/ImportSource");
        let yaml = r#"
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
"#;

        let response =
            post_grpc_web(&client, &path, &import_source_request(yaml.to_string())).await;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body = response.bytes().await.expect("gRPC-Web streaming body");
        let body = body.as_ref();
        let frame_len = |at: usize| {
            u32::from_be_bytes([body[at], body[at + 1], body[at + 2], body[at + 3]]) as usize
        };
        assert!(body.len() >= 5, "expected framed gRPC-Web response body");
        assert_eq!(body[0], 0, "expected first frame to be a data frame");
        let len = frame_len(1);
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
        let trailer_end = trailer_offset + 5 + frame_len(trailer_offset + 1);
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
    async fn embedded_ui_server_serves_static_assets_alongside_grpc_web() {
        let (_temp, running, client) = start_embedded_ui_server().await;
        let endpoint = running.endpoint_uri().to_string();

        // Root serves index.html, assets serve their own content type, and
        // unknown paths fall back to index.html (SPA fallback).
        for (path, content_type) in [
            ("", "text/html; charset=utf-8"),
            ("/assets/app.js", "application/javascript"),
            ("/some/spa/route", "text/html; charset=utf-8"),
        ] {
            let url = format!("{endpoint}{path}");
            let response = client.get(&url).send().await.expect("GET request");
            assert_eq!(response.status(), reqwest::StatusCode::OK, "{url}");
            assert_eq!(
                response
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok()),
                Some(content_type),
                "{url}"
            );
            if path.is_empty() {
                let body = response.text().await.expect("root body");
                assert!(body.contains("Coral UI"), "unexpected body: {body}");
            }
        }

        // gRPC-Web still works on the same port
        let grpc_path = format!("{endpoint}/coral.v1.SourceService/ListSources");
        let response = post_grpc_web(&client, &grpc_path, &list_sources_request()).await;
        assert_eq!(response.status(), reqwest::StatusCode::OK);

        // Unknown gRPC-Web services report a plain not-found response.
        let unknown_path = format!("{endpoint}/unknown.Service/Method");
        let unknown_grpc = post_grpc_web(&client, &unknown_path, &list_sources_request()).await;
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
        let data_dir = fake_home.join("fixture-data");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        std::fs::write(
            data_dir.join("messages.jsonl"),
            r#"{"type":"user","text":"hello"}
{"type":"assistant","text":"world"}
"#,
        )
        .expect("write fixture");

        let (_running, channel) = start_test_server(
            &temp,
            QueryRuntimeContext {
                home_dir: Some(fake_home.clone()),
                ..QueryRuntimeContext::default()
            },
            None,
        )
        .await;
        let mut source_client = SourceServiceClient::new(channel.clone());
        let mut query_client = QueryServiceClient::new(channel)
            .max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);

        let imported = import_source_name_over_grpc(
            &mut source_client,
            r#"
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
        )
        .await;
        assert_eq!(imported, "tilde_demo");

        let sql = "SELECT text FROM tilde_demo.messages ORDER BY text";
        let response = execute_sql(&mut query_client, sql)
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
        let (_running, channel) =
            start_test_server(&temp, QueryRuntimeContext::default(), None).await;
        let mut query_client = QueryServiceClient::new(channel)
            .max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);

        // No underscore separator — DataFusion's SQL parser is conservative
        // about numeric literal formats.
        let sql = "SELECT repeat('x', 5000000) AS pad";
        let response = execute_sql(&mut query_client, sql)
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

        let (_running, channel) =
            start_test_server(&temp, QueryRuntimeContext::default(), None).await;
        let mut source_client = SourceServiceClient::new(channel.clone());
        let mut query_client = QueryServiceClient::new(channel)
            .max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);

        let imported = import_source_name_over_grpc(&mut source_client, manifest).await;
        assert_eq!(imported, "wide_demo");

        let sql = "SELECT bogus_column FROM wide_demo.wide LIMIT 0";
        let status = execute_sql(&mut query_client, sql)
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
