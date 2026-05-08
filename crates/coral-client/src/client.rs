//! Client-side bootstrap for local Coral clients.

use coral_api::v1::Workspace;
use coral_api::v1::feedback_service_client::FeedbackServiceClient;
use coral_api::v1::query_service_client::QueryServiceClient;
use coral_api::v1::source_service_client::SourceServiceClient;
use coral_api::{HTTP2_MAX_HEADER_LIST_SIZE, QUERY_RESPONSE_MAX_MESSAGE_SIZE};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};

use crate::error::ClientError;
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

/// Public source-management gRPC client.
pub type SourceClient = SourceServiceClient<InterceptedService<Channel, TraceContextInterceptor>>;

/// Public SQL query gRPC client.
pub type QueryClient = QueryServiceClient<InterceptedService<Channel, TraceContextInterceptor>>;

/// Public feedback-submission gRPC client.
///
/// This stays intentionally thin for now: `coral-client` is a local transport
/// bootstrap, so it exposes the generated typed client directly rather than
/// wrapping it in a higher-level SDK surface.
pub type FeedbackClient =
    FeedbackServiceClient<InterceptedService<Channel, TraceContextInterceptor>>;

/// Public Coral client handle.
///
/// Wraps the generated gRPC clients for a Coral endpoint.
#[derive(Clone)]
pub struct AppClient {
    source: SourceClient,
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
        let channel = endpoint.connect().await?;
        let source_client =
            SourceServiceClient::with_interceptor(channel.clone(), TraceContextInterceptor);
        let query_client =
            QueryServiceClient::with_interceptor(channel.clone(), TraceContextInterceptor)
                .max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);
        let feedback_client =
            FeedbackServiceClient::with_interceptor(channel, TraceContextInterceptor);
        Ok(Self {
            source: source_client,
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
