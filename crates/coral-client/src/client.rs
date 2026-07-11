//! Client-side bootstrap for local Coral clients.

use coral_api::v1::Workspace;
use coral_api::v1::catalog_service_client::CatalogServiceClient;
use coral_api::v1::feedback_service_client::FeedbackServiceClient;
use coral_api::v1::identity_service_client::IdentityServiceClient;
use coral_api::v1::identity_spec_service_client::IdentitySpecServiceClient;
use coral_api::v1::query_service_client::QueryServiceClient;
use coral_api::v1::search_service_client::SearchServiceClient;
use coral_api::v1::source_service_client::SourceServiceClient;
use coral_api::v1::workspace_identity_service_client::WorkspaceIdentityServiceClient;
use coral_api::v1::workspace_service_client::WorkspaceServiceClient;
use coral_api::{
    CATALOG_RESPONSE_MAX_MESSAGE_SIZE, HTTP2_MAX_HEADER_LIST_SIZE,
    IDENTITY_RESPONSE_MAX_MESSAGE_SIZE, IDENTITY_SPEC_RESPONSE_MAX_MESSAGE_SIZE,
    QUERY_RESPONSE_MAX_MESSAGE_SIZE, SEARCH_RESPONSE_MAX_MESSAGE_SIZE,
    SOURCE_RESPONSE_MAX_MESSAGE_SIZE,
};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};

use crate::error::ClientError;
use crate::grpc::{GrpcClientEndpoint, InstrumentedGrpcService};
use crate::propagation::RequestContextInterceptor;

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

type RawGrpcService = InterceptedService<Channel, RequestContextInterceptor>;
type GrpcService = InstrumentedGrpcService<RawGrpcService>;

/// Public source-management gRPC client.
pub type SourceClient = SourceServiceClient<GrpcService>;

/// Public workspace-management gRPC client.
pub type WorkspaceClient = WorkspaceServiceClient<GrpcService>;

/// Public identity-spec gRPC client.
pub type IdentitySpecClient = IdentitySpecServiceClient<GrpcService>;

/// Public stored-identity gRPC client.
pub type IdentityClient = IdentityServiceClient<GrpcService>;

/// Public workspace-owned stored-identity gRPC client.
pub type WorkspaceIdentityClient = WorkspaceIdentityServiceClient<GrpcService>;

/// Public catalog-discovery gRPC client.
pub type CatalogClient = CatalogServiceClient<GrpcService>;

/// Public SQL query gRPC client.
pub type QueryClient = QueryServiceClient<GrpcService>;

/// Public Universal Search gRPC client.
pub type SearchClient = SearchServiceClient<GrpcService>;

/// Public feedback-submission gRPC client.
pub type FeedbackClient = FeedbackServiceClient<GrpcService>;

/// Public Coral client handle.
///
/// Wraps the generated gRPC clients for a Coral endpoint.
#[derive(Clone)]
pub struct AppClient {
    source: SourceClient,
    workspace: WorkspaceClient,
    identity_spec: IdentitySpecClient,
    identity: IdentityClient,
    workspace_identity: WorkspaceIdentityClient,
    catalog: CatalogClient,
    query: QueryClient,
    search: SearchClient,
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
        let endpoint = Endpoint::from_shared(endpoint_uri.to_string())?
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE);
        let grpc_endpoint = GrpcClientEndpoint::from_endpoint_uri(endpoint_uri);
        let channel = endpoint.connect().await?;
        let source_client = SourceClient::new(grpc_service(channel.clone(), &grpc_endpoint))
            .max_decoding_message_size(SOURCE_RESPONSE_MAX_MESSAGE_SIZE);
        let workspace_client = WorkspaceClient::new(grpc_service(channel.clone(), &grpc_endpoint));
        let identity_spec_client =
            IdentitySpecClient::new(grpc_service(channel.clone(), &grpc_endpoint))
                .max_decoding_message_size(IDENTITY_SPEC_RESPONSE_MAX_MESSAGE_SIZE);
        let identity_client = IdentityClient::new(grpc_service(channel.clone(), &grpc_endpoint))
            .max_decoding_message_size(IDENTITY_RESPONSE_MAX_MESSAGE_SIZE);
        let workspace_identity_client =
            WorkspaceIdentityClient::new(grpc_service(channel.clone(), &grpc_endpoint))
                .max_decoding_message_size(IDENTITY_RESPONSE_MAX_MESSAGE_SIZE);
        let catalog_client = CatalogClient::new(grpc_service(channel.clone(), &grpc_endpoint))
            .max_decoding_message_size(CATALOG_RESPONSE_MAX_MESSAGE_SIZE);
        let query_client = QueryClient::new(grpc_service(channel.clone(), &grpc_endpoint))
            .max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);
        let search_client = SearchClient::new(grpc_service(channel.clone(), &grpc_endpoint))
            .max_decoding_message_size(SEARCH_RESPONSE_MAX_MESSAGE_SIZE);
        let feedback_client = FeedbackClient::new(grpc_service(channel, &grpc_endpoint));
        Ok(Self {
            source: source_client,
            workspace: workspace_client,
            identity_spec: identity_spec_client,
            identity: identity_client,
            workspace_identity: workspace_identity_client,
            catalog: catalog_client,
            query: query_client,
            search: search_client,
            feedback: feedback_client,
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
    /// Returns a cloned identity-spec client.
    pub fn identity_spec_client(&self) -> IdentitySpecClient {
        self.identity_spec.clone()
    }

    #[must_use]
    /// Returns a cloned stored-identity client.
    pub fn identity_client(&self) -> IdentityClient {
        self.identity.clone()
    }

    #[must_use]
    /// Returns a cloned workspace-owned stored-identity client.
    pub fn workspace_identity_client(&self) -> WorkspaceIdentityClient {
        self.workspace_identity.clone()
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
    /// Returns a cloned feedback-submission client.
    pub fn feedback_client(&self) -> FeedbackClient {
        self.feedback.clone()
    }
}

fn grpc_service(channel: Channel, endpoint: &GrpcClientEndpoint) -> GrpcService {
    InstrumentedGrpcService::new(
        InterceptedService::new(channel, RequestContextInterceptor),
        endpoint.clone(),
    )
}

#[cfg(test)]
mod tests {
    use coral_api::v1::identity_service_server::{IdentityService, IdentityServiceServer};
    use coral_api::v1::identity_spec_service_server::{
        IdentitySpecService, IdentitySpecServiceServer,
    };
    use coral_api::v1::workspace_identity_service_server::{
        WorkspaceIdentityService, WorkspaceIdentityServiceServer,
    };
    use coral_api::v1::{
        AddIdentitySpecRequest, AddIdentitySpecResponse, CreateUserOwnedIdentityRequest,
        CreateUserOwnedIdentityResponse, CreateWorkspaceOwnedIdentityRequest,
        CreateWorkspaceOwnedIdentityResponse, CredentialMetadata, DeleteIdentitySpecRequest,
        DeleteIdentitySpecResponse, DeleteUserOwnedIdentityRequest,
        DeleteUserOwnedIdentityResponse, DeleteWorkspaceOwnedIdentityRequest,
        DeleteWorkspaceOwnedIdentityResponse, GetIdentitySpecRequest, GetIdentitySpecResponse,
        GetUserOwnedIdentityRequest, GetUserOwnedIdentityResponse,
        GetWorkspaceOwnedIdentityRequest, GetWorkspaceOwnedIdentityResponse, Identity,
        IdentitySpec, ListIdentitySpecsRequest, ListIdentitySpecsResponse,
        ListUserOwnedIdentitiesRequest, ListUserOwnedIdentitiesResponse,
        ListWorkspaceOwnedIdentitiesRequest, ListWorkspaceOwnedIdentitiesResponse, Workspace,
    };
    use coral_api::{IDENTITY_RESPONSE_MAX_MESSAGE_SIZE, IDENTITY_SPEC_RESPONSE_MAX_MESSAGE_SIZE};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};

    use super::AppClient;

    const TONIC_DEFAULT_MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;
    const MANIFEST_SIZE: usize = 1024 * 1024;
    const SPEC_COUNT: usize = 5;

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
                .map(|index| IdentitySpec {
                    name: format!("large-{index}"),
                    manifest_yaml: "x".repeat(MANIFEST_SIZE),
                    ..IdentitySpec::default()
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
                metadata: vec![CredentialMetadata {
                    key: "provider_metadata".to_string(),
                    value: "x".repeat(MANIFEST_SIZE),
                }],
                ..Identity::default()
            })
            .collect()
    }

    #[tonic::async_trait]
    impl IdentityService for LargeIdentityFixture {
        type CreateUserOwnedIdentityStream =
            tokio_stream::Empty<Result<CreateUserOwnedIdentityResponse, Status>>;

        async fn create_user_owned_identity(
            &self,
            _request: Request<CreateUserOwnedIdentityRequest>,
        ) -> Result<Response<Self::CreateUserOwnedIdentityStream>, Status> {
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

    #[tonic::async_trait]
    impl WorkspaceIdentityService for LargeIdentityFixture {
        type CreateWorkspaceOwnedIdentityStream =
            tokio_stream::Empty<Result<CreateWorkspaceOwnedIdentityResponse, Status>>;

        async fn create_workspace_owned_identity(
            &self,
            _request: Request<CreateWorkspaceOwnedIdentityRequest>,
        ) -> Result<Response<Self::CreateWorkspaceOwnedIdentityStream>, Status> {
            Err(Status::unimplemented("fixture only supports listing"))
        }

        async fn list_workspace_owned_identities(
            &self,
            _request: Request<ListWorkspaceOwnedIdentitiesRequest>,
        ) -> Result<Response<ListWorkspaceOwnedIdentitiesResponse>, Status> {
            Ok(Response::new(ListWorkspaceOwnedIdentitiesResponse {
                identities: large_identities(),
            }))
        }

        async fn get_workspace_owned_identity(
            &self,
            _request: Request<GetWorkspaceOwnedIdentityRequest>,
        ) -> Result<Response<GetWorkspaceOwnedIdentityResponse>, Status> {
            Err(Status::unimplemented("fixture only supports listing"))
        }

        async fn delete_workspace_owned_identity(
            &self,
            _request: Request<DeleteWorkspaceOwnedIdentityRequest>,
        ) -> Result<Response<DeleteWorkspaceOwnedIdentityResponse>, Status> {
            Err(Status::unimplemented("fixture only supports listing"))
        }
    }

    fn assert_large_identity_aggregate(identities: &[Identity]) {
        assert_eq!(identities.len(), SPEC_COUNT);
        let metadata_bytes = identities.iter().fold(0, |bytes, identity| {
            let metadata = identity
                .metadata
                .first()
                .expect("large identity fixture has provider metadata");
            assert!(metadata.value.len() < TONIC_DEFAULT_MAX_MESSAGE_SIZE);
            bytes + metadata.value.len()
        });
        assert!(metadata_bytes > TONIC_DEFAULT_MAX_MESSAGE_SIZE);
    }

    #[tokio::test]
    async fn app_client_decodes_identity_spec_aggregate_above_tonic_default() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind identity-spec fixture");
        let address = listener.local_addr().expect("read fixture address");
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(
                    IdentitySpecServiceServer::new(LargeIdentitySpecFixture)
                        .max_encoding_message_size(IDENTITY_SPEC_RESPONSE_MAX_MESSAGE_SIZE),
                )
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
        });

        let app_client = AppClient::connect(&format!("http://{address}"))
            .await
            .expect("connect AppClient to identity-spec fixture");
        let response = app_client
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
                .all(|spec| spec.manifest_yaml.len() < TONIC_DEFAULT_MAX_MESSAGE_SIZE)
        );
        let manifest_bytes = response
            .identity_specs
            .iter()
            .map(|spec| spec.manifest_yaml.len())
            .sum::<usize>();
        assert!(manifest_bytes > TONIC_DEFAULT_MAX_MESSAGE_SIZE);

        server.abort();
    }

    #[tokio::test]
    async fn app_client_decodes_large_aggregates_for_both_identity_services() {
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
                .add_service(
                    WorkspaceIdentityServiceServer::new(LargeIdentityFixture)
                        .max_encoding_message_size(IDENTITY_RESPONSE_MAX_MESSAGE_SIZE),
                )
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
        });

        let app_client = AppClient::connect(&format!("http://{address}"))
            .await
            .expect("connect AppClient to identity fixture");
        let user_response = app_client
            .identity_client()
            .list_user_owned_identities(ListUserOwnedIdentitiesRequest {})
            .await
            .expect("decode user identity response through AppClient")
            .into_inner();
        assert_large_identity_aggregate(&user_response.identities);

        let workspace_response = app_client
            .workspace_identity_client()
            .list_workspace_owned_identities(ListWorkspaceOwnedIdentitiesRequest {
                workspace: Some(Workspace {
                    name: "work".to_string(),
                }),
            })
            .await
            .expect("decode workspace identity response through AppClient")
            .into_inner();
        assert_large_identity_aggregate(&workspace_response.identities);

        server.abort();
    }
}
