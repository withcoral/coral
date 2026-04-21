//! Client-side bootstrap for local Coral clients.

use coral_api::v1::Workspace;
use coral_api::v1::query_service_client::QueryServiceClient;
use coral_api::v1::source_service_client::SourceServiceClient;
use coral_api::{HTTP2_MAX_HEADER_LIST_SIZE, QUERY_RESPONSE_MAX_MESSAGE_SIZE};
use tonic::transport::{Channel, Endpoint};

use crate::error::ClientError;

/// Default workspace used by local Coral clients.
pub use coral_app::DEFAULT_WORKSPACE_ID;

#[must_use]
/// Returns the default workspace used by local Coral clients.
pub fn default_workspace() -> Workspace {
    Workspace {
        name: DEFAULT_WORKSPACE_ID.to_string(),
    }
}

/// Public source-management gRPC client.
///
/// This stays intentionally thin for now: `coral-client` is a local transport
/// bootstrap, so it exposes the generated typed client directly rather than
/// wrapping it in a higher-level SDK surface.
pub type SourceClient = SourceServiceClient<Channel>;

/// Public SQL query gRPC client.
///
/// This stays intentionally thin for now: `coral-client` is a local transport
/// bootstrap, so it exposes the generated typed client directly rather than
/// wrapping it in a higher-level SDK surface.
pub type QueryClient = QueryServiceClient<Channel>;

/// Public Coral client handle.
///
/// Wraps the generated gRPC clients for a Coral endpoint.
pub struct AppClient {
    source_client: SourceClient,
    query_client: QueryClient,
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
        let endpoint = Endpoint::from_shared(endpoint_uri.to_string())?
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE);
        let channel = endpoint.connect().await?;
        let source_client = SourceServiceClient::new(channel.clone());
        let query_client = QueryServiceClient::new(channel)
            .max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);
        Ok(Self {
            source_client,
            query_client,
        })
    }

    #[must_use]
    /// Returns a cloned source-management client.
    pub fn source_client(&self) -> SourceClient {
        self.source_client.clone()
    }

    #[must_use]
    /// Returns a cloned query client.
    pub fn query_client(&self) -> QueryClient {
        self.query_client.clone()
    }
}
