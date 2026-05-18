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
use coral_api::v1::feedback_service_server::FeedbackServiceServer;
use coral_api::v1::query_service_server::QueryServiceServer;
use coral_api::v1::source_service_server::SourceServiceServer;
use coral_api::v1::trace_service_server::TraceServiceServer;
use coral_api::{
    HTTP2_MAX_HEADER_LIST_SIZE, QUERY_RESPONSE_MAX_MESSAGE_SIZE, TRACE_RESPONSE_MAX_MESSAGE_SIZE,
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
use crate::EngineExtensionsProvider;
use crate::feedback::manager::FeedbackManager;
use crate::feedback::publisher::{
    FeedbackPublisher, HostedFeedbackPublisher, NoopFeedbackPublisher,
};
use crate::feedback::service::FeedbackService;
use crate::query::manager::QueryManager;
use crate::query::service::QueryService;
use crate::sources::manager::SourceManager;
use crate::sources::service::SourceService;
use crate::state::{AppStateLayout, ConfigStore, SecretStore};
use crate::telemetry::TelemetryConfig;
use crate::telemetry::service::TraceService;
use crate::transport::GrpcMethodAnnotatedService;

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
    feedback_publisher: Arc<dyn FeedbackPublisher>,
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
            mode: ServerMode::NativeGrpc,
            engine_extensions_providers: Vec::new(),
            feedback_publisher: Arc::new(HostedFeedbackPublisher::new()),
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
#[derive(Clone, Default)]
pub struct ServerBuilder {
    config: ServerConfig,
}

impl ServerBuilder {
    #[must_use]
    /// Creates a builder for the default native gRPC local server.
    pub fn new() -> Self {
        Self {
            config: ServerConfig::new(),
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
    /// Enables or disables local stderr log rendering for this server.
    ///
    /// `MCP` stdio adapters can enable this for diagnostics while keeping
    /// stdout reserved for protocol messages. Other command surfaces should
    /// leave it disabled and rely on OTEL export for logs.
    pub fn with_stderr_logs(mut self, enable_stderr_logs: bool) -> Self {
        self.config = self.config.with_stderr_logs(enable_stderr_logs);
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
    /// required directories cannot be created, the config or secrets backends
    /// fail to initialize, or the gRPC server cannot be started.
    pub async fn start(self) -> Result<RunningServer, AppError> {
        let env = AppEnvironment::discover();
        let layout = AppStateLayout::discover(
            self.config
                .config_dir
                .or_else(|| env.coral_config_dir_override()),
        )?;
        layout.ensure()?;
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
        let config_store = ConfigStore::new(layout.clone());
        let secret_store = SecretStore::new(layout.clone());
        let source_manager =
            SourceManager::new(config_store.clone(), secret_store.clone(), layout.clone());
        let feedback_manager =
            FeedbackManager::with_publisher(layout.clone(), self.config.feedback_publisher);
        let query_manager = QueryManager::new(
            config_store,
            secret_store,
            env.query_runtime_context(),
            layout,
            self.config.engine_extensions_providers,
        );
        let trace_service = if telemetry_config.trace_history.enabled {
            installed_trace_store.map(|store| TraceService::new(store.dir, store.retention))
        } else {
            None
        };
        start_server(
            source_manager,
            query_manager,
            feedback_manager,
            trace_service,
            self.config.mode,
        )
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

async fn start_server(
    source_manager: SourceManager,
    query_manager: QueryManager,
    feedback_manager: FeedbackManager,
    trace_service: Option<TraceService>,
    mode: ServerMode,
) -> Result<RunningServer, AppError> {
    let source_service = SourceService::new(source_manager, query_manager.clone());
    let query_service = QueryService::new(query_manager);
    let feedback_service = FeedbackService::new(feedback_manager);
    let mut routes = Routes::default()
        .add_service(GrpcMethodAnnotatedService::new(SourceServiceServer::new(
            source_service,
        )))
        .add_service(GrpcMethodAnnotatedService::new(FeedbackServiceServer::new(
            feedback_service,
        )))
        .add_service(GrpcMethodAnnotatedService::new(
            QueryServiceServer::new(query_service)
                .max_encoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE),
        ));
    if let Some(trace_service) = trace_service {
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
    use std::net::{Ipv4Addr, TcpListener};
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use coral_api::v1::query_service_client::QueryServiceClient;
    use coral_api::v1::source_service_client::SourceServiceClient;
    use coral_api::v1::trace_service_client::TraceServiceClient;
    use coral_api::v1::{
        ExecuteSqlRequest, ImportSourceRequest, ListSourcesRequest, ListTracesRequest, Workspace,
    };
    use coral_api::{HTTP2_MAX_HEADER_LIST_SIZE, QUERY_RESPONSE_MAX_MESSAGE_SIZE};
    use coral_engine::QueryRuntimeContext;
    use tempfile::TempDir;
    use tonic::transport::Endpoint;
    use tonic::{Code, Request};

    use super::{
        ServerBuilder, ServerMode, StaticAsset, StaticAssetsProvider, is_grpc_web_content_type,
        is_native_grpc_content_type, start_server,
    };
    use crate::feedback::manager::FeedbackManager;
    use crate::query::manager::QueryManager;
    use crate::sources::manager::SourceManager;
    use crate::state::{AppStateLayout, ConfigStore, SecretStore};
    use crate::telemetry::service::TraceService;
    use crate::transport::workspace_to_proto;
    use crate::workspaces::WorkspaceName;
    use crate::{AwsEngineExtensionsProvider, NoopEngineExtensionsProvider};

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
            }))
            .await
            .expect_err("trace service should be disabled");

        assert_eq!(status.code(), Code::Unimplemented);
        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn trace_service_lists_empty_store() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        let layout = AppStateLayout::discover(Some(config_dir)).expect("layout");
        layout.ensure().expect("layout dirs");
        let config_store = ConfigStore::new(layout.clone());
        let secret_store = SecretStore::new(layout.clone());
        let source_manager =
            SourceManager::new(config_store.clone(), secret_store.clone(), layout.clone());
        let feedback_manager = FeedbackManager::new(layout.clone());
        let query_manager = QueryManager::new(
            config_store,
            secret_store,
            QueryRuntimeContext::default(),
            layout,
            vec![Arc::new(NoopEngineExtensionsProvider)],
        );
        let trace_service =
            TraceService::new(temp.path().join("trace-store"), Duration::from_mins(1));
        let server = start_server(
            source_manager,
            query_manager,
            feedback_manager,
            Some(trace_service),
            ServerMode::NativeGrpc,
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
    async fn embedded_ui_server_serves_static_assets_alongside_grpc_web() {
        let temp = TempDir::new().expect("temp dir");
        let running = ServerBuilder::embedded_ui_loopback(0, Arc::new(StubAssets))
            .with_config_dir(temp.path().join("coral-config"))
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

        // gRPC-Web still works on the same port
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
        let source_manager = SourceManager::new(
            ConfigStore::new(layout.clone()),
            SecretStore::new(layout.clone()),
            layout.clone(),
        );
        let feedback_manager = FeedbackManager::new(layout.clone());
        let query_manager = QueryManager::new(
            ConfigStore::new(layout.clone()),
            SecretStore::new(layout.clone()),
            QueryRuntimeContext {
                home_dir: Some(fake_home.clone()),
            },
            layout,
            vec![Arc::new(NoopEngineExtensionsProvider)],
        );
        let running = start_server(
            source_manager,
            query_manager,
            feedback_manager,
            None,
            ServerMode::NativeGrpc,
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

        source_client
            .import_source(Request::new(ImportSourceRequest {
                workspace: Some(default_workspace()),
                manifest_yaml: r#"
name: tilde_demo
version: 0.1.0
dsl_version: 3
backend: jsonl
tables:
  - name: messages
    description: Fixture messages
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
            }))
            .await
            .expect("create source");

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
        let source_manager = SourceManager::new(
            ConfigStore::new(layout.clone()),
            SecretStore::new(layout.clone()),
            layout.clone(),
        );
        let feedback_manager = FeedbackManager::new(layout.clone());
        let query_manager = QueryManager::new(
            ConfigStore::new(layout.clone()),
            SecretStore::new(layout.clone()),
            QueryRuntimeContext::default(),
            layout,
            vec![Arc::new(NoopEngineExtensionsProvider)],
        );
        let running = start_server(
            source_manager,
            query_manager,
            feedback_manager,
            None,
            ServerMode::NativeGrpc,
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
        manifest.push_str("backend: jsonl\n");
        manifest.push_str("tables:\n");
        manifest.push_str("  - name: wide\n");
        manifest.push_str("    description: Wide fixture\n");
        manifest.push_str("    source:\n");
        writeln!(manifest, "      location: {location}").expect("write to String");
        manifest.push_str("      glob: \"**/*.jsonl\"\n");
        manifest.push_str("    columns:\n");
        for i in 0..600 {
            writeln!(manifest, "      - name: col_{i:04}\n        type: Utf8")
                .expect("write to String");
        }

        let layout = AppStateLayout::discover(Some(config_dir.clone())).expect("layout");
        let source_manager = SourceManager::new(
            ConfigStore::new(layout.clone()),
            SecretStore::new(layout.clone()),
            layout.clone(),
        );
        let feedback_manager = FeedbackManager::new(layout.clone());
        let query_manager = QueryManager::new(
            ConfigStore::new(layout.clone()),
            SecretStore::new(layout.clone()),
            QueryRuntimeContext::default(),
            layout,
            vec![Arc::new(NoopEngineExtensionsProvider)],
        );
        let running = start_server(
            source_manager,
            query_manager,
            feedback_manager,
            None,
            ServerMode::NativeGrpc,
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

        source_client
            .import_source(Request::new(ImportSourceRequest {
                workspace: Some(default_workspace()),
                manifest_yaml: manifest,
                variables: Vec::new(),
                secrets: Vec::new(),
            }))
            .await
            .expect("import wide source");

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
