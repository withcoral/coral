//! Implements the gRPC `WorkspaceService` for workspace lifecycle APIs.

use coral_api::v1::workspace_service_server::WorkspaceService as WorkspaceServiceApi;
use coral_api::v1::{
    AddWorkspaceMemberRequest, AddWorkspaceMemberResponse, CreateWorkspaceRequest,
    CreateWorkspaceResponse, DeleteWorkspaceRequest, DeleteWorkspaceResponse,
    ListWorkspaceMembersRequest, ListWorkspaceMembersResponse, ListWorkspacesRequest,
    ListWorkspacesResponse, RemoveWorkspaceMemberRequest, RemoveWorkspaceMemberResponse, Workspace,
    WorkspaceMember, WorkspaceMembership, WorkspaceRole,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::{AppError, app_status};
use crate::identity::Principal;
use crate::transport::{
    grpc_span, instrument_grpc, request_context, workspace_name_from_proto, workspace_to_proto,
};
use crate::workspaces::{
    MemberRole, WorkspaceAction, WorkspaceAuthorizer, WorkspaceManager, WorkspaceRecord,
};

#[derive(Clone)]
pub(crate) struct WorkspaceService {
    workspaces: WorkspaceManager,
    authorizer: Option<WorkspaceAuthorizer>,
}

impl WorkspaceService {
    pub(crate) fn new(workspace_manager: WorkspaceManager) -> Self {
        Self {
            workspaces: workspace_manager,
            authorizer: None,
        }
    }

    #[expect(dead_code, reason = "wired by control-plane composition in t15")]
    pub(crate) fn with_authorizer(mut self, authorizer: WorkspaceAuthorizer) -> Self {
        self.authorizer = Some(authorizer);
        self
    }

    async fn authorize(
        &self,
        principal: &Principal,
        workspace: &crate::workspaces::WorkspaceName,
    ) -> Result<(), AppError> {
        if let Some(authorizer) = &self.authorizer {
            return authorizer
                .authorize(principal, workspace, WorkspaceAction::Manage)
                .await;
        }
        if principal.is_local() {
            Ok(())
        } else {
            Err(AppError::PermissionDenied(
                "workspace authorization is unavailable".to_string(),
            ))
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
        let principal = request_context(&request)?.principal().clone();
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let memberships = workspaces
                .list_workspaces_for(&principal)
                .await
                .map_err(app_status)?
                .into_iter()
                .map(|(workspace, role)| WorkspaceMembership {
                    workspace: Some(workspace_record_to_proto(&workspace)),
                    role: member_role_to_proto(role) as i32,
                })
                .collect();
            Ok(Response::new(ListWorkspacesResponse { memberships }))
        })
        .await
    }

    async fn create_workspace(
        &self,
        request: Request<CreateWorkspaceRequest>,
    ) -> Result<Response<CreateWorkspaceResponse>, Status> {
        let span = grpc_span(&request);
        let principal = request_context(&request)?.principal().clone();
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let creation = workspaces
                .create_workspace_for_user(&workspace_name, &principal)
                .await;
            let workspace = match creation {
                Err(AppError::UserNotFound(_)) if principal.is_local() => {
                    workspaces.create_workspace(&workspace_name).await
                }
                result => result,
            }
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
        let principal = request_context(&request)?.principal().clone();
        let service = self.clone();
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            service
                .authorize(&principal, &workspace_name)
                .await
                .map_err(app_status)?;
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

    async fn list_workspace_members(
        &self,
        request: Request<ListWorkspaceMembersRequest>,
    ) -> Result<Response<ListWorkspaceMembersResponse>, Status> {
        let span = grpc_span(&request);
        let principal = request_context(&request)?.principal().clone();
        let service = self.clone();
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            service
                .authorize(&principal, &workspace_name)
                .await
                .map_err(app_status)?;
            let members = workspaces
                .list_workspace_members(&workspace_name)
                .await
                .map_err(app_status)?
                .into_iter()
                .map(workspace_member_to_proto)
                .collect();
            Ok(Response::new(ListWorkspaceMembersResponse { members }))
        })
        .await
    }

    async fn add_workspace_member(
        &self,
        request: Request<AddWorkspaceMemberRequest>,
    ) -> Result<Response<AddWorkspaceMemberResponse>, Status> {
        let span = grpc_span(&request);
        let principal = request_context(&request)?.principal().clone();
        let service = self.clone();
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            service
                .authorize(&principal, &workspace_name)
                .await
                .map_err(app_status)?;
            let member = request.member.ok_or_else(|| {
                app_status(AppError::InvalidInput(
                    "missing workspace member".to_string(),
                ))
            })?;
            let role = member_role_from_proto(member.role).map_err(app_status)?;
            let member = workspaces
                .add_workspace_member(&workspace_name, &member.user_id, role)
                .await
                .map_err(app_status)?;
            Ok(Response::new(AddWorkspaceMemberResponse {
                member: Some(workspace_member_to_proto(member)),
            }))
        })
        .await
    }

    async fn remove_workspace_member(
        &self,
        request: Request<RemoveWorkspaceMemberRequest>,
    ) -> Result<Response<RemoveWorkspaceMemberResponse>, Status> {
        let span = grpc_span(&request);
        let principal = request_context(&request)?.principal().clone();
        let service = self.clone();
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            service
                .authorize(&principal, &workspace_name)
                .await
                .map_err(app_status)?;
            workspaces
                .remove_workspace_member(&workspace_name, &request.user_id)
                .await
                .map_err(app_status)?;
            Ok(Response::new(RemoveWorkspaceMemberResponse {}))
        })
        .await
    }
}

fn workspace_record_to_proto(record: &WorkspaceRecord) -> Workspace {
    workspace_to_proto(&record.name)
}

fn member_role_from_proto(role: i32) -> Result<MemberRole, AppError> {
    match WorkspaceRole::try_from(role) {
        Ok(WorkspaceRole::Owner) => Ok(MemberRole::Owner),
        Ok(WorkspaceRole::Member) => Ok(MemberRole::Member),
        Ok(WorkspaceRole::Unspecified) | Err(_) => Err(AppError::InvalidInput(
            "workspace member role must be OWNER or MEMBER".to_string(),
        )),
    }
}

fn member_role_to_proto(role: MemberRole) -> WorkspaceRole {
    match role {
        MemberRole::Owner => WorkspaceRole::Owner,
        MemberRole::Member => WorkspaceRole::Member,
    }
}

fn workspace_member_to_proto(member: crate::state::db::WorkspaceMemberView) -> WorkspaceMember {
    WorkspaceMember {
        user_id: member.user_id,
        role: member_role_to_proto(member.role) as i32,
        display_name: member.display_name.unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::{member_role_from_proto, member_role_to_proto};
    use crate::workspaces::MemberRole;
    use coral_api::v1::WorkspaceRole;

    #[test]
    fn workspace_member_roles_are_strict_at_the_transport_edge() {
        assert_eq!(
            member_role_from_proto(WorkspaceRole::Owner as i32).expect("owner role"),
            MemberRole::Owner
        );
        assert_eq!(
            member_role_from_proto(WorkspaceRole::Member as i32).expect("member role"),
            MemberRole::Member
        );
        member_role_from_proto(WorkspaceRole::Unspecified as i32)
            .expect_err("unspecified role must be rejected");
        member_role_from_proto(i32::MAX).expect_err("unknown role must be rejected");
        assert_eq!(
            member_role_to_proto(MemberRole::Owner),
            WorkspaceRole::Owner
        );
        assert_eq!(
            member_role_to_proto(MemberRole::Member),
            WorkspaceRole::Member
        );
    }
}
