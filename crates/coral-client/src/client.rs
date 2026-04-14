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

/// Builder for the public Coral client handle.
#[derive(Debug, Clone, Default)]
pub struct ClientBuilder;

impl ClientBuilder {
    #[must_use]
    /// Creates a builder for the default local Coral client.
    pub fn new() -> Self {
        Self
    }

    /// Builds the public Coral client against an internal local gRPC server.
    ///
    /// This intentionally starts the local server here so callers get the real
    /// typed gRPC boundary through the default thin local bootstrap API.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if local server startup or client connection
    /// fails.
    pub async fn build(self) -> Result<AppClient, ClientError> {
        let running = coral_app::ServerBuilder::new().start().await?;
        AppClient::from_running_server(running).await
    }
}

/// Public Coral client handle.
///
/// This is intentionally a light wrapper around the generated gRPC clients and
/// the local server lifetime. It is not yet a richer domain client. Explicit
/// server-controlled bootstrap lives in [`crate::local`].
pub struct AppClient {
    source_client: SourceClient,
    query_client: QueryClient,
    #[allow(
        dead_code,
        reason = "Keeps the internal local server alive for the client lifetime."
    )]
    running_server: coral_app::RunningServer,
}

impl AppClient {
    async fn connect_clients(
        endpoint_uri: &str,
    ) -> Result<(SourceClient, QueryClient), ClientError> {
        let endpoint = Endpoint::from_shared(endpoint_uri.to_string())?
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE);
        let channel = endpoint.connect().await?;
        let source_client = SourceServiceClient::new(channel.clone());
        let query_client = QueryServiceClient::new(channel)
            .max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);
        Ok((source_client, query_client))
    }

    pub(crate) async fn from_running_server(
        running_server: coral_app::RunningServer,
    ) -> Result<Self, ClientError> {
        let (source_client, query_client) =
            Self::connect_clients(running_server.endpoint_uri()).await?;
        Ok(Self {
            source_client,
            query_client,
            running_server,
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
