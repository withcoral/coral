//! Client-side bootstrap for local Coral clients.

use coral_api::v1::Workspace;
use coral_api::v1::catalog_service_client::CatalogServiceClient;
use coral_api::v1::episode_service_client::EpisodeServiceClient;
use coral_api::v1::feedback_service_client::FeedbackServiceClient;
use coral_api::v1::identity_service_client::IdentityServiceClient;
use coral_api::v1::identity_spec_service_client::IdentitySpecServiceClient;
use coral_api::v1::query_service_client::QueryServiceClient;
use coral_api::v1::source_service_client::SourceServiceClient;
use coral_api::{
    CATALOG_RESPONSE_MAX_MESSAGE_SIZE, HTTP2_MAX_HEADER_LIST_SIZE, QUERY_RESPONSE_MAX_MESSAGE_SIZE,
};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::error::ClientError;
use crate::grpc::{GrpcClientEndpoint, InstrumentedGrpcService};
use crate::propagation::{ClientMetadataInterceptor, StaticClientMetadata};

/// Default workspace used by local Coral clients.
pub use coral_api::DEFAULT_WORKSPACE_ID;

#[must_use]
/// Returns the default workspace used by local Coral clients.
pub fn default_workspace() -> Workspace {
    Workspace {
        name: DEFAULT_WORKSPACE_ID.to_string(),
    }
}

type RawGrpcService = InterceptedService<Channel, ClientMetadataInterceptor>;
type GrpcService = InstrumentedGrpcService<RawGrpcService>;

/// Public source-management gRPC client.
pub type SourceClient = SourceServiceClient<GrpcService>;

/// Public global identity-spec gRPC client.
pub type IdentitySpecClient = IdentitySpecServiceClient<GrpcService>;

/// Public stored-identity gRPC client.
pub type IdentityClient = IdentityServiceClient<GrpcService>;

/// Public catalog-discovery gRPC client.
pub type CatalogClient = CatalogServiceClient<GrpcService>;

/// Public SQL query gRPC client.
pub type QueryClient = QueryServiceClient<GrpcService>;

/// Public feedback-submission gRPC client.
pub type FeedbackClient = FeedbackServiceClient<GrpcService>;

/// Public episode-registration gRPC client.
pub type EpisodeClient = EpisodeServiceClient<GrpcService>;

/// Public Coral client handle.
///
/// Wraps the generated gRPC clients for a Coral endpoint.
#[derive(Clone)]
pub struct AppClient {
    source: SourceClient,
    identity_spec: IdentitySpecClient,
    identity: IdentityClient,
    catalog: CatalogClient,
    query: QueryClient,
    feedback: FeedbackClient,
    episode: EpisodeClient,
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
        Self::connect_with_metadata(endpoint_uri, std::iter::empty::<(&str, &str)>()).await
    }

    /// Connects to a Coral endpoint and attaches static metadata to every
    /// outgoing request.
    ///
    /// The plain [`AppClient::connect`] path remains metadata-free. This hook is
    /// for sibling products that authenticate or route requests outside the OSS
    /// single-user local process.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the gRPC clients cannot connect, or if the
    /// supplied metadata is not valid gRPC metadata.
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
        let endpoint =
            endpoint_from_uri(endpoint_uri)?.http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE);
        let grpc_endpoint = GrpcClientEndpoint::from_endpoint_uri(endpoint_uri);
        let channel = endpoint.connect().await?;
        let source_client = SourceClient::new(grpc_service(
            channel.clone(),
            &grpc_endpoint,
            static_metadata.clone(),
        ));
        let identity_spec_client = IdentitySpecClient::new(grpc_service(
            channel.clone(),
            &grpc_endpoint,
            static_metadata.clone(),
        ));
        let identity_client = IdentityClient::new(grpc_service(
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
        let feedback_client = FeedbackClient::new(grpc_service(
            channel.clone(),
            &grpc_endpoint,
            static_metadata.clone(),
        ));
        let episode_client =
            EpisodeClient::new(grpc_service(channel, &grpc_endpoint, static_metadata));
        Ok(Self {
            source: source_client,
            identity_spec: identity_spec_client,
            identity: identity_client,
            catalog: catalog_client,
            query: query_client,
            feedback: feedback_client,
            episode: episode_client,
        })
    }

    #[must_use]
    /// Returns a cloned source-management client.
    pub fn source_client(&self) -> SourceClient {
        self.source.clone()
    }

    #[must_use]
    /// Returns a cloned global identity-spec client.
    pub fn identity_spec_client(&self) -> IdentitySpecClient {
        self.identity_spec.clone()
    }

    #[must_use]
    /// Returns a cloned stored-identity client.
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
    /// Returns a cloned feedback-submission client.
    pub fn feedback_client(&self) -> FeedbackClient {
        self.feedback.clone()
    }

    #[must_use]
    /// Returns a cloned episode-registration client.
    pub fn episode_client(&self) -> EpisodeClient {
        self.episode.clone()
    }
}

fn endpoint_from_uri(endpoint_uri: &str) -> Result<Endpoint, tonic::transport::Error> {
    let endpoint = Endpoint::from_shared(endpoint_uri.to_string())?;
    if endpoint_uri_scheme_is(endpoint_uri, "https") {
        endpoint.tls_config(ClientTlsConfig::new().with_enabled_roots())
    } else {
        Ok(endpoint)
    }
}

fn endpoint_uri_scheme_is(endpoint_uri: &str, expected_scheme: &str) -> bool {
    endpoint_uri
        .split_once(':')
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case(expected_scheme))
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
