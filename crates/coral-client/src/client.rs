//! Client-side bootstrap for local Coral clients.

use coral_api::v1::Workspace;
use coral_api::v1::capability_service_client::CapabilityServiceClient;
use coral_api::v1::code_mode_service_client::CodeModeServiceClient;
use coral_api::v1::discovery_service_client::DiscoveryServiceClient;
use coral_api::v1::feedback_service_client::FeedbackServiceClient;
use coral_api::v1::query_service_client::QueryServiceClient;
use coral_api::v1::source_service_client::SourceServiceClient;
use coral_api::{
    CAPABILITY_RESPONSE_MAX_MESSAGE_SIZE, CODE_MODE_RESPONSE_MAX_MESSAGE_SIZE,
    DISCOVERY_RESPONSE_MAX_MESSAGE_SIZE, HTTP2_MAX_HEADER_LIST_SIZE,
    QUERY_RESPONSE_MAX_MESSAGE_SIZE,
};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};

use crate::error::ClientError;
use crate::grpc::{GrpcClientEndpoint, InstrumentedGrpcService};
use crate::propagation::TraceContextInterceptor;

/// Default workspace used by local Coral clients.
pub use coral_api::DEFAULT_WORKSPACE_ID;

#[must_use]
/// Returns the default workspace used by local Coral clients.
pub fn default_workspace() -> Workspace {
    Workspace {
        name: DEFAULT_WORKSPACE_ID.to_string(),
    }
}

type RawGrpcService = InterceptedService<Channel, TraceContextInterceptor>;
type GrpcService = InstrumentedGrpcService<RawGrpcService>;

/// Public source-management gRPC client.
pub type SourceClient = SourceServiceClient<GrpcService>;

/// Public capability-invocation gRPC client.
pub type CapabilityClient = CapabilityServiceClient<GrpcService>;

/// Public Code Mode gRPC client.
pub type CodeModeClient = CodeModeServiceClient<GrpcService>;

/// Public generated-export discovery gRPC client.
pub type DiscoveryClient = DiscoveryServiceClient<GrpcService>;

/// Public SQL query gRPC client.
pub type QueryClient = QueryServiceClient<GrpcService>;

/// Public feedback-submission gRPC client.
pub type FeedbackClient = FeedbackServiceClient<GrpcService>;

/// Public Coral client handle.
///
/// Wraps the generated gRPC clients for a Coral endpoint.
#[derive(Clone)]
pub struct AppClient {
    source: SourceClient,
    capability: CapabilityClient,
    code_mode: CodeModeClient,
    discovery: DiscoveryClient,
    query: QueryClient,
    feedback: FeedbackClient,
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
        crate::propagation::ensure_global_propagator();
        let endpoint = Endpoint::from_shared(endpoint_uri.to_string())?
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE);
        let grpc_endpoint = GrpcClientEndpoint::from_endpoint_uri(endpoint_uri);
        let channel = endpoint.connect().await?;
        let source_client = SourceClient::new(grpc_service(channel.clone(), &grpc_endpoint));
        let capability_client =
            CapabilityClient::new(grpc_service(channel.clone(), &grpc_endpoint))
                .max_decoding_message_size(CAPABILITY_RESPONSE_MAX_MESSAGE_SIZE);
        let code_mode_client = CodeModeClient::new(grpc_service(channel.clone(), &grpc_endpoint))
            .max_decoding_message_size(CODE_MODE_RESPONSE_MAX_MESSAGE_SIZE);
        let discovery_client = DiscoveryClient::new(grpc_service(channel.clone(), &grpc_endpoint))
            .max_decoding_message_size(DISCOVERY_RESPONSE_MAX_MESSAGE_SIZE);
        let query_client = QueryClient::new(grpc_service(channel.clone(), &grpc_endpoint))
            .max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);
        let feedback_client = FeedbackClient::new(grpc_service(channel, &grpc_endpoint));
        Ok(Self {
            source: source_client,
            capability: capability_client,
            code_mode: code_mode_client,
            discovery: discovery_client,
            query: query_client,
            feedback: feedback_client,
        })
    }

    #[must_use]
    /// Returns a cloned source-management client.
    pub fn source_client(&self) -> SourceClient {
        self.source.clone()
    }

    #[must_use]
    /// Returns a cloned capability-invocation client.
    pub fn capability_client(&self) -> CapabilityClient {
        self.capability.clone()
    }

    #[must_use]
    /// Returns a cloned Code Mode client.
    pub fn code_mode_client(&self) -> CodeModeClient {
        self.code_mode.clone()
    }

    #[must_use]
    /// Returns a cloned generated-export discovery client.
    pub fn discovery_client(&self) -> DiscoveryClient {
        self.discovery.clone()
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
}

fn grpc_service(channel: Channel, endpoint: &GrpcClientEndpoint) -> GrpcService {
    InstrumentedGrpcService::new(
        InterceptedService::new(channel, TraceContextInterceptor),
        endpoint.clone(),
    )
}
