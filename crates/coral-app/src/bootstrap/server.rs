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
use coral_api::v1::episode_service_server::EpisodeServiceServer;
use coral_api::v1::feedback_service_server::FeedbackServiceServer;
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
use tonic::service::Routes;
use tonic::transport::Server;
use tonic_web::GrpcWebLayer;
use tower::{Layer, Service};

use super::env::AppEnvironment;
use super::error::AppError;
use crate::authorization::{AllowAllManagementAuthorizer, ManagementAuthorizer};
use crate::catalog::service::CatalogService;
use crate::credentials::config::CredentialStorageConfig;
use crate::credentials::{CredentialManager, CredentialStore};
use crate::episode::service::EpisodeService;
use crate::episode::store::EpisodeStore;
use crate::features::{FeatureOverrides, FeatureStore};
use crate::feedback::manager::FeedbackManager;
use crate::feedback::publisher::{
    FeedbackPublisher, HostedFeedbackPublisher, NoopFeedbackPublisher,
};
use crate::feedback::service::FeedbackService;
use crate::identity_specs::{IdentitySpecManager, IdentitySpecService};
use crate::query::manager::QueryManager;
use crate::query::service::QueryService;
use crate::sources::manager::SourceManager;
use crate::sources::service::SourceService;
use crate::state::ConfigStore;
use crate::telemetry::TelemetryConfig;
use crate::telemetry::service::TraceService;
use crate::transport::GrpcMethodAnnotatedService;
use crate::{EngineExtensionsProvider, SingleUserPrincipalProvider, UserPrincipalProvider};

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
    fn bind_addr(&self) -> SocketAddr {
        match self {
            Self::NativeGrpc => SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
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
    engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    user_principal_provider: Arc<dyn UserPrincipalProvider>,
    feedback_publisher: Arc<dyn FeedbackPublisher>,
    enable_stderr_logs: bool,
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
            engine_extensions_providers: Vec::new(),
            user_principal_provider: Arc::new(SingleUserPrincipalProvider),
            feedback_publisher: Arc::new(HostedFeedbackPublisher::new()),
            enable_stderr_logs: false,
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
    /// Enables or disables local stderr log rendering for this server.
    ///
    /// `MCP` stdio adapters can enable this for diagnostics while keeping
    /// stdout reserved for protocol messages. Other command surfaces should
    /// leave it disabled and rely on OTEL export for logs.
    pub fn with_stderr_logs(mut self, enable_stderr_logs: bool) -> Self {
        self.enable_stderr_logs = enable_stderr_logs;
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
        let features =
            FeatureStore::new(layout.clone()).load_with_overrides(&FeatureOverrides::default())?;
        let credential_config = CredentialStorageConfig::load(&layout)?;
        let credential_store =
            CredentialStore::with_preference(layout.clone(), credential_config.storage);
        let credential_manager = CredentialManager::new(credential_store.clone());
        let source_manager = SourceManager::new_with_features(
            config_store.clone(),
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
        let identity_spec_manager = IdentitySpecManager::new_with_credential_store(
            layout.clone(),
            credential_store,
            features,
            Vec::new(),
        );

        let query_manager = QueryManager::new(
            config_store,
            credential_manager,
            query_runtime_context,
            layout,
            self.engine_extensions_providers,
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
            user_principal_provider: self.user_principal_provider,
            management_authorizer: Arc::new(AllowAllManagementAuthorizer),
            feedback_manager,
            episode_store,
            trace_service,
            mode: self.mode,
        })
        .await
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
    user_principal_provider: Arc<dyn UserPrincipalProvider>,
    management_authorizer: Arc<dyn ManagementAuthorizer>,
    feedback_manager: FeedbackManager,
    episode_store: EpisodeStore,
    trace_service: Option<TraceService>,
    mode: ServerMode,
}

async fn start_server(services: ServerServices) -> Result<RunningServer, AppError> {
    let ServerServices {
        source_manager,
        query_manager,
        identity_spec_manager,
        user_principal_provider,
        management_authorizer,
        feedback_manager,
        episode_store,
        trace_service,
        mode,
    } = services;
    let source_service = SourceService::new(
        source_manager,
        query_manager.clone(),
        Arc::clone(&user_principal_provider),
    );
    let catalog_service =
        CatalogService::new(query_manager.clone(), Arc::clone(&user_principal_provider));
    let query_service = QueryService::new(query_manager, Arc::clone(&user_principal_provider));
    let identity_spec_service = IdentitySpecService::new(
        identity_spec_manager,
        Arc::clone(&user_principal_provider),
        Arc::clone(&management_authorizer),
    );
    let feedback_service =
        FeedbackService::new(feedback_manager, Arc::clone(&user_principal_provider));
    let episode_service = EpisodeService::new(episode_store);
    let mut routes = Routes::default()
        .add_service(GrpcMethodAnnotatedService::new(SourceServiceServer::new(
            source_service,
        )))
        .add_service(GrpcMethodAnnotatedService::new(
            IdentitySpecServiceServer::new(identity_spec_service),
        ))
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

    let listener = TcpListener::bind(mode.bind_addr()).await?;
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
    use std::future::Future;
    use std::net::{Ipv4Addr, TcpListener};
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use coral_api::v1::catalog_service_client::CatalogServiceClient;
    use coral_api::v1::episode_service_client::EpisodeServiceClient;
    use coral_api::v1::feedback_service_client::FeedbackServiceClient;
    use coral_api::v1::query_service_client::QueryServiceClient;
    use coral_api::v1::source_service_client::SourceServiceClient;
    use coral_api::v1::trace_service_client::TraceServiceClient;
    use coral_api::v1::{
        ExecuteSqlRequest, ExecuteSqlResponse, ImportSourceRequest, ImportSourceResponse,
        ListCatalogRequest, ListSourcesRequest, ListTracesRequest, ListTracesResponse,
        OpenEpisodeRequest, SubmitFeedbackRequest, Workspace, import_source_response,
    };
    use coral_api::{HTTP2_MAX_HEADER_LIST_SIZE, QUERY_RESPONSE_MAX_MESSAGE_SIZE};
    use coral_engine::QueryRuntimeContext;

    use tempfile::TempDir;
    use tonic::transport::{Channel, Endpoint};
    use tonic::{Code, Request};

    use super::{
        RunningServer, ServerBuilder, ServerMode, ServerServices, StaticAsset,
        StaticAssetsProvider, is_grpc_web_content_type, is_native_grpc_content_type, start_server,
    };
    use crate::authorization::AllowAllManagementAuthorizer;
    use crate::credentials::{CredentialManager, CredentialStore};
    use crate::episode::store::EpisodeStore;
    use crate::feedback::manager::FeedbackManager;
    use crate::identity_specs::IdentitySpecManager;
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
        let credential_manager = CredentialManager::new(CredentialStore::new(layout.clone()));
        let identity_spec_manager = IdentitySpecManager::new(layout.clone());
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
            identity_spec_manager,
            user_principal_provider: Arc::new(SingleUserPrincipalProvider),
            management_authorizer: Arc::new(AllowAllManagementAuthorizer),
            feedback_manager: FeedbackManager::new(layout.clone()),
            episode_store: EpisodeStore::new(layout.clone()),
            trace_service,
            mode: ServerMode::NativeGrpc,
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
        let mut feedback_client = FeedbackServiceClient::new(channel.clone());
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
            "feedback",
            feedback_client.submit_feedback(SubmitFeedbackRequest {
                workspace: Some(default_workspace()),
                trying_to_do: "test".to_string(),
                tried: "test".to_string(),
                stuck: "test".to_string(),
            }),
        )
        .await;
        expect_unauthenticated("traces", list_traces(&mut trace_client)).await;

        server.shutdown().await.expect("shutdown");
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
