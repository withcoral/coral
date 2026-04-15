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
///
/// By default, builds a client backed by a local in-process Coral server.
/// Use [`ClientBuilder::endpoint`] to connect to a remote server instead.
#[derive(Debug, Clone, Default)]
pub struct ClientBuilder {
    endpoint: Option<String>,
}

impl ClientBuilder {
    #[must_use]
    /// Creates a builder that targets a local in-process Coral server.
    pub fn new() -> Self {
        Self::default()
    }

    /// Targets a remote Coral server at the given endpoint URI instead of
    /// starting a local server.
    pub fn endpoint(&mut self, uri: impl Into<String>) -> &mut Self {
        self.endpoint = Some(uri.into());
        self
    }

    /// Builds the [`AppClient`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if server startup or client connection fails.
    pub async fn build(&self) -> Result<AppClient, ClientError> {
        if let Some(uri) = &self.endpoint {
            let (source_client, query_client) = connect_clients(uri).await?;
            Ok(AppClient {
                source_client,
                query_client,
                _backend: Backend::Remote,
            })
        } else {
            let server = coral_app::ServerBuilder::new().start().await?;
            let (source_client, query_client) = connect_clients(server.endpoint_uri()).await?;
            Ok(AppClient {
                source_client,
                query_client,
                _backend: Backend::Local { _server: server },
            })
        }
    }
}

/// Public Coral client handle.
///
/// Wraps the generated gRPC clients and optionally owns the local server
/// lifetime. Callers interact with the same `AppClient` regardless of whether
/// the backend is a local in-process server or a remote endpoint.
///
/// Construct via [`ClientBuilder`].
pub struct AppClient {
    source_client: SourceClient,
    query_client: QueryClient,
    _backend: Backend,
}

enum Backend {
    Local { _server: coral_app::RunningServer },
    Remote,
}

impl AppClient {
    /// Connects to an already-running local Coral server, taking ownership of
    /// its lifetime.
    ///
    /// This is the opt-in escape hatch for callers that need explicit control
    /// over local server configuration. Prefer [`ClientBuilder`] unless a
    /// caller needs to customise the [`coral_app::ServerBuilder`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the gRPC clients cannot connect.
    pub async fn from_running_server(
        server: coral_app::RunningServer,
    ) -> Result<Self, ClientError> {
        let (source_client, query_client) = connect_clients(server.endpoint_uri()).await?;
        Ok(Self {
            source_client,
            query_client,
            _backend: Backend::Local { _server: server },
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

async fn connect_clients(endpoint_uri: &str) -> Result<(SourceClient, QueryClient), ClientError> {
    let endpoint = Endpoint::from_shared(endpoint_uri.to_string())?
        .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE);
    let channel = endpoint.connect().await?;
    let source_client = SourceServiceClient::new(channel.clone());
    let query_client =
        QueryServiceClient::new(channel).max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);
    Ok((source_client, query_client))
}
