//! Implements the gRPC workspace-owned identity management boundary.

use coral_api::v1::workspace_identity_service_server::WorkspaceIdentityService as WorkspaceIdentityServiceApi;
use coral_api::v1::{
    CreateWorkspaceOwnedFixedTokenIdentityRequest, CreateWorkspaceOwnedFixedTokenIdentityResponse,
    DeleteWorkspaceOwnedIdentityRequest, DeleteWorkspaceOwnedIdentityResponse,
    GetWorkspaceOwnedIdentityRequest, GetWorkspaceOwnedIdentityResponse,
    ListWorkspaceOwnedIdentitiesRequest, ListWorkspaceOwnedIdentitiesResponse,
};
use tonic::{Request, Response, Status};

use super::manager::IdentityManager;
use super::model::IdentityOwner;
use super::service::identity_to_proto;
use crate::bootstrap::{AppError, app_status};
use crate::transport::{grpc_span, instrument_grpc, workspace_name_from_proto};

#[derive(Clone)]
pub(crate) struct WorkspaceIdentityService {
    identities: IdentityManager,
}

impl WorkspaceIdentityService {
    pub(crate) fn new(identities: IdentityManager) -> Self {
        Self { identities }
    }
}

#[tonic::async_trait]
impl WorkspaceIdentityServiceApi for WorkspaceIdentityService {
    async fn create_workspace_owned_fixed_token_identity(
        &self,
        request: Request<CreateWorkspaceOwnedFixedTokenIdentityRequest>,
    ) -> Result<Response<CreateWorkspaceOwnedFixedTokenIdentityResponse>, Status> {
        let span = grpc_span(&request);
        let identities = self.identities.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace = workspace_name_from_proto(request.workspace.as_ref())?;
            let setup = request.setup.ok_or_else(|| {
                app_status(AppError::InvalidInput(
                    "missing fixed-token identity setup".to_string(),
                ))
            })?;
            let identity = identities
                .create_or_replace_workspace_fixed_token(
                    &workspace,
                    &request.name,
                    &request.identity_spec_name,
                    setup.token,
                )
                .await
                .map_err(app_status)?;
            Ok(Response::new(
                CreateWorkspaceOwnedFixedTokenIdentityResponse {
                    identity: Some(identity_to_proto(&identity).map_err(app_status)?),
                },
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
            let identities = identities
                .list_for_owner(&owner)
                .await
                .map_err(app_status)?
                .into_iter()
                .map(|identity| identity_to_proto(&identity))
                .collect::<Result<Vec<_>, _>>()
                .map_err(app_status)?;
            Ok(Response::new(ListWorkspaceOwnedIdentitiesResponse {
                identities,
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
                identity: Some(identity_to_proto(&identity).map_err(app_status)?),
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
