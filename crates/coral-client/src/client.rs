//! Client-side bootstrap for local Coral clients.

use std::net::IpAddr;

use coral_api::v1::Workspace;
use coral_api::v1::catalog_service_client::CatalogServiceClient;
use coral_api::v1::feedback_service_client::FeedbackServiceClient;
use coral_api::v1::function_service_client::FunctionServiceClient;
use coral_api::v1::identity_service_client::IdentityServiceClient;
use coral_api::v1::identity_spec_service_client::IdentitySpecServiceClient;
use coral_api::v1::query_service_client::QueryServiceClient;
use coral_api::v1::search_service_client::SearchServiceClient;
use coral_api::v1::source_service_client::SourceServiceClient;
use coral_api::v1::task_service_client::TaskServiceClient;
use coral_api::v1::workspace_service_client::WorkspaceServiceClient;
use coral_api::{
    CATALOG_RESPONSE_MAX_MESSAGE_SIZE, HTTP2_MAX_HEADER_LIST_SIZE,
    IDENTITY_RESPONSE_MAX_MESSAGE_SIZE, IDENTITY_SPEC_RESPONSE_MAX_MESSAGE_SIZE,
    QUERY_RESPONSE_MAX_MESSAGE_SIZE, SEARCH_RESPONSE_MAX_MESSAGE_SIZE,
    SOURCE_RESPONSE_MAX_MESSAGE_SIZE,
};
use coral_app::READINESS_SERVICE_NAME;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use url::{Host, Url};

use crate::error::ClientError;
use crate::grpc::{GrpcClientEndpoint, InstrumentedGrpcService};
use crate::propagation::{
    AUTHORIZATION_METADATA_KEY, BearerToken, ClientMetadataInterceptor, StaticClientMetadata,
};

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

/// Public identity-spec management gRPC client.
pub type IdentitySpecClient = IdentitySpecServiceClient<GrpcService>;

/// Public current-user stored-identity gRPC client.
pub type IdentityClient = IdentityServiceClient<GrpcService>;

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

/// Public `grpc.health.v1.Health` client.
pub type HealthCheckClient = HealthClient<GrpcService>;

/// Public Coral client handle.
///
/// Wraps the generated gRPC clients for a Coral endpoint.
#[derive(Clone)]
pub struct AppClient {
    source: SourceClient,
    workspace: WorkspaceClient,
    identity_spec: IdentitySpecClient,
    identity: IdentityClient,
    catalog: CatalogClient,
    query: QueryClient,
    search: SearchClient,
    function: FunctionClient,
    feedback: FeedbackClient,
    task: TaskClient,
    health: HealthCheckClient,
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
    /// Trace-context, task-attribution, and gRPC transport keys are reserved.
    /// Authorization metadata is sent only over HTTPS.
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

    /// Connects to an HTTPS Coral endpoint and attaches bearer authorization
    /// metadata to every outgoing request.
    ///
    /// Bearer credentials are sent only over HTTPS.
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
        Self::connect_with_metadata(
            endpoint_uri,
            [(AUTHORIZATION_METADATA_KEY, bearer.authorization())],
        )
        .await
    }

    pub(crate) async fn connect_with_loopback_bearer(
        endpoint_uri: &str,
        bearer: BearerToken,
    ) -> Result<Self, ClientError> {
        let static_metadata =
            StaticClientMetadata::try_from_pairs([("authorization", bearer.authorization())])?;
        Self::connect_with_static_metadata_for(
            endpoint_uri,
            static_metadata,
            endpoint_allows_loopback_authorization,
        )
        .await
    }

    async fn connect_with_static_metadata(
        endpoint_uri: &str,
        static_metadata: StaticClientMetadata,
    ) -> Result<Self, ClientError> {
        Self::connect_with_static_metadata_for(
            endpoint_uri,
            static_metadata,
            endpoint_allows_authorization,
        )
        .await
    }

    async fn connect_with_static_metadata_for(
        endpoint_uri: &str,
        static_metadata: StaticClientMetadata,
        endpoint_allows_authorization: impl FnOnce(&str) -> bool,
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
        let identity_spec_client = IdentitySpecClient::new(grpc_service(
            channel.clone(),
            &grpc_endpoint,
            static_metadata.clone(),
        ))
        .max_decoding_message_size(IDENTITY_SPEC_RESPONSE_MAX_MESSAGE_SIZE);
        let identity_client = IdentityClient::new(grpc_service(
            channel.clone(),
            &grpc_endpoint,
            static_metadata.clone(),
        ))
        .max_decoding_message_size(IDENTITY_RESPONSE_MAX_MESSAGE_SIZE);
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
        let task_client = TaskClient::new(grpc_service(
            channel.clone(),
            &grpc_endpoint,
            static_metadata.clone(),
        ));
        let health_client =
            HealthCheckClient::new(grpc_service(channel, &grpc_endpoint, static_metadata));
        Ok(Self {
            source: source_client,
            workspace: workspace_client,
            identity_spec: identity_spec_client,
            identity: identity_client,
            catalog: catalog_client,
            query: query_client,
            search: search_client,
            function: function_client,
            feedback: feedback_client,
            task: task_client,
            health: health_client,
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
    /// Returns a cloned identity-spec management client.
    pub fn identity_spec_client(&self) -> IdentitySpecClient {
        self.identity_spec.clone()
    }

    #[must_use]
    /// Returns a cloned current-user stored-identity client.
    pub fn identity_client(&self) -> IdentityClient {
        self.identity.clone()
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

    /// Reports whether the server's engine is ready to answer for its catalog.
    ///
    /// This asks the health service for [`READINESS_SERVICE_NAME`] rather than
    /// the empty aggregate name, which reports process liveness only. The health
    /// service is unauthenticated, so this reaches a server that requires bearer
    /// tokens on every other RPC, and it reuses this client's channel, which is
    /// what makes it usable as a repeated readiness probe.
    ///
    /// # Errors
    ///
    /// Returns the gRPC [`tonic::Status`] when the health RPC cannot complete.
    pub async fn check_engine_ready(&self) -> Result<bool, tonic::Status> {
        let status = self
            .health
            .clone()
            .check(HealthCheckRequest {
                service: READINESS_SERVICE_NAME.to_string(),
            })
            .await?
            .into_inner()
            .status;
        Ok(status == ServingStatus::Serving as i32)
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
    endpoint.scheme() == "https" && endpoint.host().is_some()
}

/// Whether a cleartext endpoint is loopback enough to carry a bearer token.
///
/// The host must be a canonical IP literal *as written*, because this check and
/// the transport do not read the endpoint the same way. `Url::parse` applies
/// WHATWG normalization, so both its `host()` and its `host_str()` report
/// `http://2130706433` and `http://0x7f000001` as a loopback `127.0.0.1`, while
/// the channel is built from the original authority — which a resolver is free to
/// treat as a name and send off-host. So the authority is re-read from the raw
/// input and must round-trip through `IpAddr` unchanged.
fn endpoint_allows_loopback_authorization(endpoint_uri: &str) -> bool {
    let Ok(endpoint) = Url::parse(endpoint_uri) else {
        return false;
    };
    if endpoint_has_credentials(&endpoint) || endpoint.scheme() != "http" {
        return false;
    }
    if !endpoint.host().as_ref().is_some_and(host_is_loopback) {
        return false;
    }
    raw_authority_host(endpoint_uri).is_some_and(is_canonical_loopback_literal)
}

/// Extracts the host from the endpoint as written, before URL normalization.
///
/// Only the host is read raw: the scheme is matched case-insensitively so this
/// agrees with the parsed-`Url` checks, which see an already-lowercased scheme.
/// IPv6 hosts are returned without their brackets.
fn raw_authority_host(endpoint_uri: &str) -> Option<&str> {
    let rest = endpoint_uri
        .split_once("://")
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("http"))
        .map(|(_, rest)| rest)?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    if let Some(host) = authority.strip_prefix('[') {
        return host.split_once(']').map(|(host, _)| host);
    }
    Some(
        authority
            .split_once(':')
            .map_or(authority, |(host, _)| host),
    )
}

/// Whether `host` is a loopback address written in its one canonical spelling.
///
/// The round-trip comparison is what rejects the alternate encodings a resolver
/// might read differently: `0177.0.0.1`, `127.1`, and `2130706433` all either
/// fail to parse or do not print back identically.
fn is_canonical_loopback_literal(host: &str) -> bool {
    host.parse::<IpAddr>()
        .is_ok_and(|address| address.is_loopback() && address.to_string() == host)
}

fn endpoint_has_credentials(endpoint: &Url) -> bool {
    !endpoint.username().is_empty() || endpoint.password().is_some()
}

fn host_is_loopback(host: &Host<&str>) -> bool {
    match host {
        Host::Domain(_) => false,
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
    use coral_api::v1::identity_service_server::{IdentityService, IdentityServiceServer};
    use coral_api::v1::identity_spec_service_server::{
        IdentitySpecService, IdentitySpecServiceServer,
    };
    use coral_api::v1::{
        AddIdentitySpecRequest, AddIdentitySpecResponse, CreateUserOwnedFixedTokenIdentityRequest,
        CreateUserOwnedFixedTokenIdentityResponse, DeleteIdentitySpecRequest,
        DeleteIdentitySpecResponse, DeleteUserOwnedIdentityRequest,
        DeleteUserOwnedIdentityResponse, GetIdentitySpecRequest, GetIdentitySpecResponse,
        GetUserOwnedIdentityRequest, GetUserOwnedIdentityResponse, Identity, IdentitySpecReference,
        IdentitySpecSummary, ListIdentitySpecsRequest, ListIdentitySpecsResponse,
        ListUserOwnedIdentitiesRequest, ListUserOwnedIdentitiesResponse,
    };
    use coral_api::v1::{SubmitFeedbackRequest, SubmitFeedbackResponse};
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};
    use tracing::Instrument as _;
    use tracing_subscriber::prelude::*;

    use super::*;

    const TONIC_DEFAULT_MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;
    const DESCRIPTION_SIZE: usize = 1024 * 1024;
    const SPEC_COUNT: usize = 5;

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
            let route = request
                .metadata()
                .get("x-coral-route")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let traceparent = request
                .metadata()
                .get("traceparent")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            *self.capture.0.lock().expect("capture lock") = Some((route, traceparent));

            Ok(tonic::Response::new(SubmitFeedbackResponse::default()))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn static_metadata_and_active_traceparent_reach_generated_rpc() {
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
        let client = AppClient::connect_with_metadata(&endpoint, [("x-coral-route", "primary")])
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

        let (route, traceparent) = capture
            .0
            .lock()
            .expect("capture lock")
            .clone()
            .expect("captured metadata");
        assert_eq!(route, "primary");
        assert!(traceparent.starts_with("00-"), "{traceparent}");
        assert_eq!(traceparent.len(), 55);

        shutdown_tx.send(()).expect("shutdown capture server");
        server.await.expect("join capture server").expect("server");
    }

    #[test]
    fn authorization_endpoints_allow_https() {
        for endpoint in ["https://api.example.com", "https://api.example.com:50051"] {
            assert!(
                endpoint_allows_authorization(endpoint),
                "rejected {endpoint}"
            );
        }
    }

    #[test]
    fn authorization_endpoints_reject_plaintext_http() {
        for endpoint in [
            "http://api.example.com",
            "http://api.example.com:50051",
            "http://localhost:50051",
            "http://LOCALHOST",
            "http://127.0.0.1:50051",
            "http://127.255.255.254",
            "http://127.1:50051",
            "http://2130706433:50051",
            "http://0x7f000001:50051",
            "http://[::1]:50051",
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
    fn local_authorization_endpoints_require_plaintext_loopback() {
        for endpoint in [
            "http://127.0.0.1:50051",
            "http://127.255.255.254",
            "http://[::1]:50051",
            "HTTP://127.0.0.1:50051",
        ] {
            assert!(
                endpoint_allows_loopback_authorization(endpoint),
                "rejected {endpoint}"
            );
        }
        for endpoint in [
            "https://localhost:50051",
            "http://localhost:50051",
            "http://api.example.com:50051",
            "http://10.0.0.1:50051",
            "http://localhost.example.com:50051",
            "http://localhost@api.example.com:50051",
            "http://user:password@localhost:50051",
            // Non-canonical spellings `Url` normalizes to loopback but the
            // transport may resolve as a name: the bearer must not ride these.
            "http://2130706433:50051",
            "http://0x7f000001:50051",
            "http://0177.0.0.1:50051",
            "http://127.1:50051",
        ] {
            assert!(
                !endpoint_allows_loopback_authorization(endpoint),
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
    async fn authenticated_connect_rejects_http_before_dialing() {
        let Err(error) = AppClient::connect_with_metadata(
            "http://127.0.0.1:1",
            [("authorization", "Basic secret")],
        )
        .await
        else {
            panic!("authorization metadata over HTTP must be rejected");
        };
        assert!(matches!(error, ClientError::InsecureAuthorizationEndpoint));

        let bearer = BearerToken::new("secret").expect("valid bearer");
        let Err(error) = AppClient::connect_with_bearer("http://127.0.0.1:1", bearer).await else {
            panic!("HTTP must be rejected");
        };

        assert!(matches!(error, ClientError::InsecureAuthorizationEndpoint));
    }

    #[test]
    fn https_endpoint_builds_with_native_root_tls() {
        configured_endpoint("https://api.example.com:50051").expect("HTTPS endpoint");
    }

    #[derive(Debug)]
    struct LargeIdentitySpecFixture;

    #[derive(Debug)]
    struct LargeIdentityFixture;

    #[tonic::async_trait]
    impl IdentitySpecService for LargeIdentitySpecFixture {
        async fn add_identity_spec(
            &self,
            _request: Request<AddIdentitySpecRequest>,
        ) -> Result<Response<AddIdentitySpecResponse>, Status> {
            Err(Status::unimplemented("fixture only supports listing"))
        }

        async fn list_identity_specs(
            &self,
            _request: Request<ListIdentitySpecsRequest>,
        ) -> Result<Response<ListIdentitySpecsResponse>, Status> {
            let identity_specs = (0..SPEC_COUNT)
                .map(|index| IdentitySpecSummary {
                    name: format!("large-{index}"),
                    description: "x".repeat(DESCRIPTION_SIZE),
                    ..IdentitySpecSummary::default()
                })
                .collect();
            Ok(Response::new(ListIdentitySpecsResponse { identity_specs }))
        }

        async fn get_identity_spec(
            &self,
            _request: Request<GetIdentitySpecRequest>,
        ) -> Result<Response<GetIdentitySpecResponse>, Status> {
            Err(Status::unimplemented("fixture only supports listing"))
        }

        async fn delete_identity_spec(
            &self,
            _request: Request<DeleteIdentitySpecRequest>,
        ) -> Result<Response<DeleteIdentitySpecResponse>, Status> {
            Err(Status::unimplemented("fixture only supports listing"))
        }
    }

    fn large_identities() -> Vec<Identity> {
        (0..SPEC_COUNT)
            .map(|index| Identity {
                name: format!("large-{index}"),
                identity_spec: Some(IdentitySpecReference {
                    issuer: "x".repeat(DESCRIPTION_SIZE),
                    ..IdentitySpecReference::default()
                }),
                ..Identity::default()
            })
            .collect()
    }

    #[tonic::async_trait]
    impl IdentityService for LargeIdentityFixture {
        async fn create_user_owned_fixed_token_identity(
            &self,
            _request: Request<CreateUserOwnedFixedTokenIdentityRequest>,
        ) -> Result<Response<CreateUserOwnedFixedTokenIdentityResponse>, Status> {
            Err(Status::unimplemented("fixture only supports listing"))
        }

        async fn list_user_owned_identities(
            &self,
            _request: Request<ListUserOwnedIdentitiesRequest>,
        ) -> Result<Response<ListUserOwnedIdentitiesResponse>, Status> {
            Ok(Response::new(ListUserOwnedIdentitiesResponse {
                identities: large_identities(),
            }))
        }

        async fn get_user_owned_identity(
            &self,
            _request: Request<GetUserOwnedIdentityRequest>,
        ) -> Result<Response<GetUserOwnedIdentityResponse>, Status> {
            Err(Status::unimplemented("fixture only supports listing"))
        }

        async fn delete_user_owned_identity(
            &self,
            _request: Request<DeleteUserOwnedIdentityRequest>,
        ) -> Result<Response<DeleteUserOwnedIdentityResponse>, Status> {
            Err(Status::unimplemented("fixture only supports listing"))
        }
    }

    fn assert_large_identity_aggregate(identities: &[Identity]) {
        assert_eq!(identities.len(), SPEC_COUNT);
        let issuer_bytes = identities
            .iter()
            .map(|identity| {
                identity
                    .identity_spec
                    .as_ref()
                    .expect("fixture identity has a spec reference")
                    .issuer
                    .len()
            })
            .sum::<usize>();
        assert!(issuer_bytes > TONIC_DEFAULT_MAX_MESSAGE_SIZE);
    }

    #[tokio::test]
    async fn app_client_decodes_identity_spec_summary_aggregate_above_tonic_default() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind identity-spec fixture");
        let address = listener.local_addr().expect("read fixture address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(
                    IdentitySpecServiceServer::new(LargeIdentitySpecFixture)
                        .max_encoding_message_size(IDENTITY_SPEC_RESPONSE_MAX_MESSAGE_SIZE),
                )
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    drop(shutdown_rx.await);
                })
                .await
        });
        let client = AppClient::connect(&format!("http://{address}"))
            .await
            .expect("connect AppClient to identity-spec fixture");
        let response = client
            .identity_spec_client()
            .list_identity_specs(ListIdentitySpecsRequest::default())
            .await
            .expect("decode identity-spec response through AppClient")
            .into_inner();

        assert_eq!(response.identity_specs.len(), SPEC_COUNT);
        assert!(
            response
                .identity_specs
                .iter()
                .all(|spec| spec.description.len() < TONIC_DEFAULT_MAX_MESSAGE_SIZE)
        );
        assert!(
            response
                .identity_specs
                .iter()
                .map(|spec| spec.description.len())
                .sum::<usize>()
                > TONIC_DEFAULT_MAX_MESSAGE_SIZE
        );
        drop(client);
        shutdown_tx
            .send(())
            .expect("shutdown identity-spec fixture");
        server
            .await
            .expect("join identity-spec fixture")
            .expect("server");
    }

    #[tokio::test]
    async fn app_client_decodes_large_identity_aggregates() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind identity fixture");
        let address = listener.local_addr().expect("read fixture address");
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(
                    IdentityServiceServer::new(LargeIdentityFixture)
                        .max_encoding_message_size(IDENTITY_RESPONSE_MAX_MESSAGE_SIZE),
                )
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
        });
        let app_client = AppClient::connect(&format!("http://{address}"))
            .await
            .expect("connect AppClient to identity fixture");

        let user = app_client
            .identity_client()
            .list_user_owned_identities(ListUserOwnedIdentitiesRequest {})
            .await
            .expect("decode current-user identity aggregate")
            .into_inner();
        assert_large_identity_aggregate(&user.identities);
        server.abort();
    }
}
