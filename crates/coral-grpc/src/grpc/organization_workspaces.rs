use super::organization_memberships::member_id;
use super::workspaces::{change, error, owner, page, state, workspace_id};
use super::{GrpcServices, management as wire};
use coral_workspace::{AuthenticatedUser, OrganizationWorkspace, WorkspaceId};
use tonic::{Request, Response, Status};

fn view(workspace: OrganizationWorkspace) -> wire::OrganizationWorkspace {
    wire::OrganizationWorkspace {
        name: format!("workspaces/{}", workspace.workspace.id()),
        display_name: workspace.workspace.display_name().as_str().into(),
        state: state(workspace.workspace.state()).into(),
        etag: workspace.metadata.etag,
        owners: workspace.owners.into_iter().map(owner).collect(),
    }
}

impl GrpcServices {
    async fn organization_workspace_view(
        &self,
        actor: &AuthenticatedUser,
        id: &WorkspaceId,
    ) -> Result<Response<wire::OrganizationWorkspace>, Status> {
        Ok(Response::new(view(
            self.workspaces
                .inspect_organization_workspace(actor, id)
                .await
                .map_err(error)?,
        )))
    }
}

#[tonic::async_trait]
impl wire::organization_workspace_service_server::OrganizationWorkspaceService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.list_organization_workspaces")]
    async fn list_organization_workspaces(
        &self,
        request: Request<wire::ListOrganizationWorkspacesRequest>,
    ) -> Result<Response<wire::ListOrganizationWorkspacesResponse>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let values = self
            .workspaces
            .list_organization_workspaces(&actor)
            .await
            .map_err(error)?
            .into_iter()
            .map(view)
            .collect();
        let (workspaces, next_page_token) = page(
            values,
            input.page_size,
            &input.page_token,
            &format!("{}:organizationWorkspaces", actor.user_id()),
            |w| w.name.clone(),
        )?;
        Ok(Response::new(wire::ListOrganizationWorkspacesResponse {
            workspaces,
            next_page_token,
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.assign_workspace_owner")]
    async fn assign_workspace_owner(
        &self,
        request: Request<wire::AssignWorkspaceOwnerRequest>,
    ) -> Result<Response<wire::OrganizationWorkspace>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let id = workspace_id(&input.name)?;
        let context = change(actor.clone())?
            .with_request_id(&input.request_id)
            .map_err(error)?;
        self.workspaces
            .assign_owner(&context, &id, &member_id(&input.owner)?)
            .await
            .map_err(error)?;
        self.organization_workspace_view(&actor, &id).await
    }

    #[tracing::instrument(skip_all, name = "coral.private.archive_organization_workspace")]
    async fn archive_organization_workspace(
        &self,
        request: Request<wire::ArchiveOrganizationWorkspaceRequest>,
    ) -> Result<Response<wire::OrganizationWorkspace>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let id = workspace_id(&input.name)?;
        let context = change(actor.clone())?
            .with_etag(&input.etag)
            .map_err(error)?;
        self.workspaces
            .archive_as_organization_owner(&context, &id)
            .await
            .map_err(error)?;
        self.organization_workspace_view(&actor, &id).await
    }

    #[tracing::instrument(skip_all, name = "coral.private.restore_organization_workspace")]
    async fn restore_organization_workspace(
        &self,
        request: Request<wire::RestoreOrganizationWorkspaceRequest>,
    ) -> Result<Response<wire::OrganizationWorkspace>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let id = workspace_id(&input.name)?;
        let context = change(actor.clone())?
            .with_etag(&input.etag)
            .map_err(error)?;
        self.workspaces
            .restore_as_organization_owner(&context, &id)
            .await
            .map_err(error)?;
        self.organization_workspace_view(&actor, &id).await
    }
}
