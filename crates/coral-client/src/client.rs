//! Client-side bootstrap for local Coral clients.

use coral_api::v1::Workspace;
use coral_api::v1::catalog_service_client::CatalogServiceClient;
use coral_api::v1::feedback_service_client::FeedbackServiceClient;
use coral_api::v1::function_service_client::FunctionServiceClient;
use coral_api::v1::query_service_client::QueryServiceClient;
use coral_api::v1::search_service_client::SearchServiceClient;
use coral_api::v1::source_service_client::SourceServiceClient;
use coral_api::v1::task_service_client::TaskServiceClient;
use coral_api::v1::workspace_service_client::WorkspaceServiceClient;
use coral_api::{
    CATALOG_RESPONSE_MAX_MESSAGE_SIZE, HTTP2_MAX_HEADER_LIST_SIZE, QUERY_RESPONSE_MAX_MESSAGE_SIZE,
    SEARCH_RESPONSE_MAX_MESSAGE_SIZE, SOURCE_RESPONSE_MAX_MESSAGE_SIZE,
};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use url::{Host, Url};

use crate::error::ClientError;
use crate::grpc::{GrpcClientEndpoint, InstrumentedGrpcService};
use crate::propagation::{BearerToken, ClientMetadataInterceptor, StaticClientMetadata};

/// Default workspace used by local Coral clients.
pub use coral_api::DEFAULT_WORKSPACE_ID;

#[must_use]
/// Returns the default workspace used by local Coral clients.
pub fn default_workspace() -> Workspace {
    Workspace {
        name: DEFAULT_WORKSPACE_ID.to_string(),
    }
}

#[must_use]
/// Returns a workspace resource with the provided name.
pub fn workspace(name: impl Into<String>) -> Workspace {
    Workspace { name: name.into() }
}

type RawGrpcService = InterceptedService<Channel, ClientMetadataInterceptor>;
type GrpcService = InstrumentedGrpcService<RawGrpcService>;

/// Public source-management gRPC client.
pub type SourceClient = SourceServiceClient<GrpcService>;

/// Public workspace-management gRPC client.
pub type WorkspaceClient = WorkspaceServiceClient<GrpcService>;

/// Public catalog-discovery gRPC client.
pub type CatalogClient = CatalogServiceClient<GrpcService>;

/// Public SQL query gRPC client.
pub type QueryClient = QueryServiceClient<GrpcService>;

/// Public Universal Search gRPC client.
pub type SearchClient = SearchServiceClient<GrpcService>;

/// Public function management gRPC client.
pub type FunctionClient = FunctionServiceClient<GrpcService>;

/// Public feedback-submission gRPC client.
pub type FeedbackClient = FeedbackServiceClient<GrpcService>;

/// Public task-lifecycle gRPC client.
pub type TaskClient = TaskServiceClient<GrpcService>;

/// Public Coral client handle.
///
/// Wraps the generated gRPC clients for a Coral endpoint.
#[derive(Clone)]
pub struct AppClient {
    source: SourceClient,
    workspace: WorkspaceClient,
    catalog: CatalogClient,
    query: QueryClient,
    search: SearchClient,
    function: FunctionClient,
    feedback: FeedbackClient,
    task: TaskClient,
}

impl AppClient {
    /// Connects to a Coral endpoint.
    ///
    /// This is intentionally pure transport: callers that start a local server
    /// must keep the returned `RunningServer` alive themselves.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the gRPC clients cannot connect.
    pub async fn connect(endpoint_uri: &str) -> Result<Self, ClientError> {
        Self::connect_with_static_metadata(endpoint_uri, StaticClientMetadata::default()).await
    }

    /// Connects to a Coral endpoint and attaches static metadata to every
    /// outgoing request.
    ///
    /// Trace-context, task-attribution, and gRPC transport keys are reserved. Authorization
    /// metadata is sent only over HTTPS, except that plaintext HTTP is allowed
    /// for loopback endpoints used during local development.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the gRPC clients cannot connect, if supplied
    /// metadata is invalid, or if authorization would cross plaintext HTTP.
    pub async fn connect_with_metadata<K, V, I>(
        endpoint_uri: &str,
        metadata: I,
    ) -> Result<Self, ClientError>
    where
        K: AsRef<str>,
        V: AsRef<str>,
        I: IntoIterator<Item = (K, V)>,
    {
        let static_metadata = StaticClientMetadata::try_from_pairs(metadata)?;
        Self::connect_with_static_metadata(endpoint_uri, static_metadata).await
    }

    /// Connects to a Coral endpoint and attaches bearer authorization metadata
    /// to every outgoing request.
    ///
    /// Bearer credentials are sent only over HTTPS, except that plaintext HTTP
    /// is allowed for loopback endpoints used during local development.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::InsecureAuthorizationEndpoint`] before attempting a
    /// connection when the endpoint could expose the token over plaintext.
    /// Returns [`ClientError`] if the gRPC clients otherwise cannot connect.
    pub async fn connect_with_bearer(
        endpoint_uri: &str,
        bearer: BearerToken,
    ) -> Result<Self, ClientError> {
        Self::connect_with_metadata(endpoint_uri, [("authorization", bearer.authorization())]).await
    }

    async fn connect_with_static_metadata(
        endpoint_uri: &str,
        static_metadata: StaticClientMetadata,
    ) -> Result<Self, ClientError> {
        let endpoint = configured_endpoint(endpoint_uri)?;
        if static_metadata.contains_authorization() && !endpoint_allows_authorization(endpoint_uri)
        {
            return Err(ClientError::InsecureAuthorizationEndpoint);
        }
        let grpc_endpoint = GrpcClientEndpoint::from_endpoint_uri(endpoint_uri);
        let channel = endpoint.connect().await?;
        let source_client = SourceClient::new(grpc_service(
            channel.clone(),
            &grpc_endpoint,
            static_metadata.clone(),
        ))
        .max_decoding_message_size(SOURCE_RESPONSE_MAX_MESSAGE_SIZE);
        let workspace_client = WorkspaceClient::new(grpc_service(
            channel.clone(),
            &grpc_endpoint,
            static_metadata.clone(),
        ));
        let catalog_client = CatalogClient::new(grpc_service(
            channel.clone(),
            &grpc_endpoint,
            static_metadata.clone(),
        ))
        .max_decoding_message_size(CATALOG_RESPONSE_MAX_MESSAGE_SIZE);
        let query_client = QueryClient::new(grpc_service(
            channel.clone(),
            &grpc_endpoint,
            static_metadata.clone(),
        ))
        .max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);
        let search_client = SearchClient::new(grpc_service(
            channel.clone(),
            &grpc_endpoint,
            static_metadata.clone(),
        ))
        .max_decoding_message_size(SEARCH_RESPONSE_MAX_MESSAGE_SIZE);
        let function_client = FunctionClient::new(grpc_service(
            channel.clone(),
            &grpc_endpoint,
            static_metadata.clone(),
        ))
        .max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);
        let feedback_client = FeedbackClient::new(grpc_service(
            channel.clone(),
            &grpc_endpoint,
            static_metadata.clone(),
        ));
        let task_client = TaskClient::new(grpc_service(channel, &grpc_endpoint, static_metadata));
        Ok(Self {
            source: source_client,
            workspace: workspace_client,
            catalog: catalog_client,
            query: query_client,
            search: search_client,
            function: function_client,
            feedback: feedback_client,
            task: task_client,
        })
    }

    #[must_use]
    /// Returns a cloned source-management client.
    pub fn source_client(&self) -> SourceClient {
        self.source.clone()
    }

    #[must_use]
    /// Returns a cloned workspace-management client.
    pub fn workspace_client(&self) -> WorkspaceClient {
        self.workspace.clone()
    }

    #[must_use]
    /// Returns a cloned catalog-discovery client.
    pub fn catalog_client(&self) -> CatalogClient {
        self.catalog.clone()
    }

    #[must_use]
    /// Returns a cloned query client.
    pub fn query_client(&self) -> QueryClient {
        self.query.clone()
    }

    #[must_use]
    /// Returns a cloned Universal Search client.
    pub fn search_client(&self) -> SearchClient {
        self.search.clone()
    }

    #[must_use]
    /// Returns a cloned function management client.
    pub fn function_client(&self) -> FunctionClient {
        self.function.clone()
    }

    #[must_use]
    /// Returns a cloned feedback-submission client.
    pub fn feedback_client(&self) -> FeedbackClient {
        self.feedback.clone()
    }

    #[must_use]
    /// Returns a cloned task-lifecycle client.
    pub fn task_client(&self) -> TaskClient {
        self.task.clone()
    }
}

fn configured_endpoint(endpoint_uri: &str) -> Result<Endpoint, ClientError> {
    if Url::parse(endpoint_uri).is_ok_and(|endpoint| endpoint_has_credentials(&endpoint)) {
        return Err(ClientError::EndpointCredentials);
    }
    let endpoint = Endpoint::from_shared(endpoint_uri.to_string())?
        .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE);
    if endpoint.uri().scheme_str() == Some("https") {
        return endpoint
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(Into::into);
    }
    Ok(endpoint)
}

fn endpoint_allows_authorization(endpoint_uri: &str) -> bool {
    let Ok(endpoint) = Url::parse(endpoint_uri) else {
        return false;
    };
    if endpoint_has_credentials(&endpoint) {
        return false;
    }
    match endpoint.scheme() {
        "https" => endpoint.host().is_some(),
        "http" => endpoint.host().as_ref().is_some_and(host_is_loopback),
        _ => false,
    }
}

fn endpoint_has_credentials(endpoint: &Url) -> bool {
    !endpoint.username().is_empty() || endpoint.password().is_some()
}

fn host_is_loopback(host: &Host<&str>) -> bool {
    match host {
        Host::Domain(host) => host.eq_ignore_ascii_case("localhost"),
        Host::Ipv4(address) => address.is_loopback(),
        Host::Ipv6(address) => address.is_loopback(),
    }
}

fn grpc_service(
    channel: Channel,
    endpoint: &GrpcClientEndpoint,
    static_metadata: StaticClientMetadata,
) -> GrpcService {
    InstrumentedGrpcService::new(
        InterceptedService::new(channel, ClientMetadataInterceptor::new(static_metadata)),
        endpoint.clone(),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use coral_api::v1::feedback_service_server::{FeedbackService, FeedbackServiceServer};
    use coral_api::v1::{SubmitFeedbackRequest, SubmitFeedbackResponse};
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::TcpListenerStream;
    use tracing::Instrument as _;
    use tracing_subscriber::prelude::*;

    use super::*;

    #[derive(Clone, Default)]
    struct MetadataCapture(Arc<Mutex<Option<(String, String)>>>);

    #[derive(Clone)]
    struct CapturingFeedbackService {
        capture: MetadataCapture,
    }

    #[tonic::async_trait]
    impl FeedbackService for CapturingFeedbackService {
        async fn submit_feedback(
            &self,
            request: tonic::Request<SubmitFeedbackRequest>,
        ) -> Result<tonic::Response<SubmitFeedbackResponse>, tonic::Status> {
            let authorization = request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let traceparent = request
                .metadata()
                .get("traceparent")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            *self.capture.0.lock().expect("capture lock") = Some((authorization, traceparent));

            Ok(tonic::Response::new(SubmitFeedbackResponse::default()))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn static_authorization_and_active_traceparent_reach_generated_rpc() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind capture server");
        let address = listener.local_addr().expect("capture server address");
        let capture = MetadataCapture::default();
        let service = CapturingFeedbackService {
            capture: capture.clone(),
        };
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(FeedbackServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    drop(shutdown_rx.await);
                })
                .await
        });

        let endpoint = format!("http://{address}");
        let client = AppClient::connect_with_metadata(
            &endpoint,
            [("authorization", "Bearer session-token")],
        )
        .await
        .expect("connect client");
        let provider = SdkTracerProvider::builder().build();
        let tracer = provider.tracer("coral-client-metadata-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);
        let span = tracing::info_span!("metadata_rpc_traversal");

        client
            .feedback_client()
            .submit_feedback(SubmitFeedbackRequest::default())
            .instrument(span)
            .await
            .expect("feedback RPC");

        let (authorization, traceparent) = capture
            .0
            .lock()
            .expect("capture lock")
            .clone()
            .expect("captured metadata");
        assert_eq!(authorization, "Bearer session-token");
        assert!(traceparent.starts_with("00-"), "{traceparent}");
        assert_eq!(traceparent.len(), 55);

        shutdown_tx.send(()).expect("shutdown capture server");
        server.await.expect("join capture server").expect("server");
    }

    #[test]
    fn authorization_endpoints_allow_https_and_loopback_http() {
        for endpoint in [
            "https://api.example.com",
            "https://api.example.com:50051",
            "http://localhost:50051",
            "http://LOCALHOST",
            "http://127.0.0.1:50051",
            "http://127.255.255.254",
            "http://127.1:50051",
            "http://2130706433:50051",
            "http://0x7f000001:50051",
            "http://[::1]:50051",
        ] {
            assert!(
                endpoint_allows_authorization(endpoint),
                "rejected {endpoint}"
            );
        }
    }

    #[test]
    fn authorization_endpoints_reject_remote_or_ambiguous_plaintext_http() {
        for endpoint in [
            "http://api.example.com",
            "http://api.example.com:50051",
            "http://10.0.0.1:50051",
            "http://[2001:db8::1]:50051",
            "http://localhost.example.com:50051",
            "http://localhost@api.example.com:50051",
            "http://user:password@localhost:50051",
            "ftp://localhost:50051",
            "localhost:50051",
        ] {
            assert!(
                !endpoint_allows_authorization(endpoint),
                "accepted {endpoint}"
            );
        }
    }

    #[test]
    fn configured_endpoints_reject_url_credentials() {
        for endpoint in [
            "http://user@localhost:50051",
            "http://user:password@localhost:50051",
            "https://%75ser@example.com:50051",
        ] {
            assert!(matches!(
                configured_endpoint(endpoint),
                Err(ClientError::EndpointCredentials)
            ));
        }
    }

    #[tokio::test]
    async fn authenticated_connect_rejects_remote_http_before_dialing() {
        let Err(error) = AppClient::connect_with_metadata(
            "http://192.0.2.1:1",
            [("authorization", "Basic secret")],
        )
        .await
        else {
            panic!("authorization metadata over remote HTTP must be rejected");
        };
        assert!(matches!(error, ClientError::InsecureAuthorizationEndpoint));

        let bearer = BearerToken::new("secret").expect("valid bearer");
        let Err(error) = AppClient::connect_with_bearer("http://192.0.2.1:1", bearer).await else {
            panic!("remote HTTP must be rejected");
        };

        assert!(matches!(error, ClientError::InsecureAuthorizationEndpoint));
    }

    #[test]
    fn https_endpoint_builds_with_native_root_tls() {
        configured_endpoint("https://api.example.com:50051").expect("HTTPS endpoint");
    }
}
