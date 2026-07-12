//! Implements the gRPC `WorkspaceIdentityService` for workspace-owned identities.

use std::pin::Pin;

use coral_api::v1::workspace_identity_service_server::WorkspaceIdentityService as WorkspaceIdentityServiceApi;
use coral_api::v1::{
    CreateWorkspaceOwnedIdentityRequest, CreateWorkspaceOwnedIdentityResponse,
    DeleteWorkspaceOwnedIdentityRequest, DeleteWorkspaceOwnedIdentityResponse,
    FixedTokenWorkspaceOwnedIdentitySetup, GetWorkspaceOwnedIdentityRequest,
    GetWorkspaceOwnedIdentityResponse, ListWorkspaceOwnedIdentitiesRequest,
    ListWorkspaceOwnedIdentitiesResponse, create_workspace_owned_identity_request,
    create_workspace_owned_identity_response,
};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use super::manager::IdentityManager;
use super::model::IdentityOwner;
use super::service::identity_to_proto;
use crate::bootstrap::app_status;
use crate::transport::{grpc_span, instrument_grpc, workspace_name_from_proto};
use crate::workspaces::{WorkspaceManager, WorkspaceName};

#[derive(Clone)]
pub(crate) struct WorkspaceIdentityService {
    identities: IdentityManager,
    workspaces: WorkspaceManager,
}

impl WorkspaceIdentityService {
    pub(crate) fn new(identities: IdentityManager, workspaces: WorkspaceManager) -> Self {
        Self {
            identities,
            workspaces,
        }
    }
}

#[tonic::async_trait]
impl WorkspaceIdentityServiceApi for WorkspaceIdentityService {
    type CreateWorkspaceOwnedIdentityStream = CreateWorkspaceOwnedIdentityResponseStreamBox;

    async fn create_workspace_owned_identity(
        &self,
        request: Request<CreateWorkspaceOwnedIdentityRequest>,
    ) -> Result<Response<Self::CreateWorkspaceOwnedIdentityStream>, Status> {
        let span = grpc_span(&request);
        let identities = self.identities.clone();
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let CreateWorkspaceOwnedIdentityRequest {
                workspace,
                name,
                identity_spec,
                setup,
            } = request.into_inner();
            let workspace = workspace_name_from_proto(workspace.as_ref())?;
            require_workspace(&workspaces, &workspace).await?;
            let Some(create_workspace_owned_identity_request::Setup::FixedToken(
                FixedTokenWorkspaceOwnedIdentitySetup { token },
            )) = setup
            else {
                return Err(Status::unimplemented(
                    "OAuth identity creation is not enabled by this server",
                ));
            };
            let record = identities
                .create_or_replace_workspace_fixed_token(&workspace, &name, &identity_spec, token)
                .await
                .map_err(app_status)?;
            let response = CreateWorkspaceOwnedIdentityResponse {
                event: Some(create_workspace_owned_identity_response::Event::Identity(
                    identity_to_proto(record),
                )),
            };
            let stream = Box::pin(tokio_stream::iter([Ok(response)]));
            Ok(Response::new(
                stream as CreateWorkspaceOwnedIdentityResponseStreamBox,
            ))
        })
        .await
    }

    async fn list_workspace_owned_identities(
        &self,
        request: Request<ListWorkspaceOwnedIdentitiesRequest>,
    ) -> Result<Response<ListWorkspaceOwnedIdentitiesResponse>, Status> {
        let span = grpc_span(&request);
        let identities = self.identities.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace = workspace_name_from_proto(request.workspace.as_ref())?;
            let owner = IdentityOwner::workspace(workspace);
            let records = identities
                .list_for_owner(&owner)
                .await
                .map_err(app_status)?;
            Ok(Response::new(ListWorkspaceOwnedIdentitiesResponse {
                identities: records.into_iter().map(identity_to_proto).collect(),
            }))
        })
        .await
    }

    async fn get_workspace_owned_identity(
        &self,
        request: Request<GetWorkspaceOwnedIdentityRequest>,
    ) -> Result<Response<GetWorkspaceOwnedIdentityResponse>, Status> {
        let span = grpc_span(&request);
        let identities = self.identities.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace = workspace_name_from_proto(request.workspace.as_ref())?;
            let owner = IdentityOwner::workspace(workspace);
            let identity = identities
                .get(&owner, &request.name)
                .await
                .map_err(app_status)?;
            Ok(Response::new(GetWorkspaceOwnedIdentityResponse {
                identity: Some(identity_to_proto(identity)),
            }))
        })
        .await
    }

    async fn delete_workspace_owned_identity(
        &self,
        request: Request<DeleteWorkspaceOwnedIdentityRequest>,
    ) -> Result<Response<DeleteWorkspaceOwnedIdentityResponse>, Status> {
        let span = grpc_span(&request);
        let identities = self.identities.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace = workspace_name_from_proto(request.workspace.as_ref())?;
            let owner = IdentityOwner::workspace(workspace);
            identities
                .delete(&owner, &request.name)
                .await
                .map_err(app_status)?;
            Ok(Response::new(DeleteWorkspaceOwnedIdentityResponse {}))
        })
        .await
    }
}

async fn require_workspace(
    workspaces: &WorkspaceManager,
    workspace: &WorkspaceName,
) -> Result<(), Status> {
    workspaces
        .require_workspace(workspace)
        .await
        .map_err(app_status)
}

type CreateWorkspaceOwnedIdentityResponseStreamBox =
    Pin<Box<dyn Stream<Item = Result<CreateWorkspaceOwnedIdentityResponse, Status>> + Send>>;
