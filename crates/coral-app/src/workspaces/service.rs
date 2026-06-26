//! Implements the gRPC `WorkspaceService` for workspace lifecycle APIs.

use coral_api::v1::workspace_service_server::WorkspaceService as WorkspaceServiceApi;
use coral_api::v1::{
    CreateWorkspaceRequest, CreateWorkspaceResponse, DeleteWorkspaceRequest,
    DeleteWorkspaceResponse, ListWorkspacesRequest, ListWorkspacesResponse, Workspace,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::transport::{grpc_span, instrument_grpc, workspace_name_from_proto, workspace_to_proto};
use crate::workspaces::{WorkspaceManager, WorkspaceRecord};

#[derive(Clone)]
pub(crate) struct WorkspaceService {
    workspaces: WorkspaceManager,
}

impl WorkspaceService {
    pub(crate) fn new(workspace_manager: WorkspaceManager) -> Self {
        Self {
            workspaces: workspace_manager,
        }
    }
}

#[tonic::async_trait]
impl WorkspaceServiceApi for WorkspaceService {
    async fn list_workspaces(
        &self,
        request: Request<ListWorkspacesRequest>,
    ) -> Result<Response<ListWorkspacesResponse>, Status> {
        let span = grpc_span(&request);
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let workspaces = workspaces
                .list_workspaces()
                .map_err(app_status)?
                .iter()
                .map(workspace_record_to_proto)
                .collect();
            Ok(Response::new(ListWorkspacesResponse { workspaces }))
        })
        .await
    }

    async fn create_workspace(
        &self,
        request: Request<CreateWorkspaceRequest>,
    ) -> Result<Response<CreateWorkspaceResponse>, Status> {
        let span = grpc_span(&request);
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let workspace = workspaces
                .create_workspace(&workspace_name)
                .map_err(app_status)?;
            Ok(Response::new(CreateWorkspaceResponse {
                workspace: Some(workspace_record_to_proto(&workspace)),
            }))
        })
        .await
    }

    async fn delete_workspace(
        &self,
        request: Request<DeleteWorkspaceRequest>,
    ) -> Result<Response<DeleteWorkspaceResponse>, Status> {
        let span = grpc_span(&request);
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let workspace = workspaces
                .delete_workspace(&workspace_name)
                .await
                .map_err(app_status)?;
            Ok(Response::new(DeleteWorkspaceResponse {
                workspace: Some(workspace_record_to_proto(&workspace)),
            }))
        })
        .await
    }
}

fn workspace_record_to_proto(record: &WorkspaceRecord) -> Workspace {
    workspace_to_proto(&record.name)
}
