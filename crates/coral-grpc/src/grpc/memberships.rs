use super::workspaces::{
    change, error, invalid, mask, page, resource, role, timestamp, workspace_id,
};
use super::{GrpcServices, management as wire};
use coral_workspace::{
    AuthenticatedUser, ResourceMetadata, UserId, WorkspaceMembership, WorkspaceRole,
};
use tonic::{Request, Response, Status};

fn member(name: &str) -> Result<(coral_workspace::WorkspaceId, UserId), Status> {
    let (workspace, user) = resource(name, "memberships")?;
    Ok((workspace, UserId::parse(user).map_err(error)?))
}
fn membership(
    (m, meta, enabled): (WorkspaceMembership, ResourceMetadata, bool),
) -> wire::WorkspaceMembership {
    wire::WorkspaceMembership {
        name: format!(
            "workspaces/{}/memberships/{}",
            m.workspace_id(),
            m.user_id()
        ),
        user: format!("users/{}", m.user_id()),
        role: role(m.role()).into(),
        etag: meta.etag,
        create_time: Some(timestamp(meta.created_at)),
        update_time: Some(timestamp(meta.updated_at)),
        profile: None,
        enabled,
    }
}
fn parse_role(value: i32) -> Result<WorkspaceRole, Status> {
    match wire::WorkspaceRole::try_from(value) {
        Ok(wire::WorkspaceRole::Owner) => Ok(WorkspaceRole::Owner),
        Ok(wire::WorkspaceRole::Member) => Ok(WorkspaceRole::Member),
        _ => Err(invalid("invalid Membership role")),
    }
}

impl GrpcServices {
    async fn membership_view(
        &self,
        actor: &AuthenticatedUser,
        workspace: &coral_workspace::WorkspaceId,
        user: &UserId,
    ) -> Result<wire::WorkspaceMembership, Status> {
        let mut view = membership(
            self.workspaces
                .inspect_membership(actor, workspace, user)
                .await
                .map_err(error)?,
        );
        view.profile = Some(super::workspaces::profile(
            self.workspaces
                .inspect_user_profile(actor, workspace, user)
                .await
                .map_err(error)?,
        ));
        Ok(view)
    }
}

#[tonic::async_trait]
impl wire::workspace_membership_service_server::WorkspaceMembershipService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.list_memberships")]
    async fn list_workspace_memberships(
        &self,
        request: Request<wire::ListWorkspaceMembershipsRequest>,
    ) -> Result<Response<wire::ListWorkspaceMembershipsResponse>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let w = workspace_id(&input.parent)?;
        let members = self
            .workspaces
            .inspect_memberships(&actor, &w)
            .await
            .map_err(error)?;
        let mut values = Vec::new();
        for (member, meta, profile, enabled) in members {
            let mut view = membership((member, meta, enabled));
            view.profile = Some(super::workspaces::profile(profile));
            values.push(view);
        }
        let (memberships, next_page_token) = page(
            values,
            input.page_size,
            &input.page_token,
            &format!("{}:{w}:memberships", actor.user_id()),
            |m| m.name.clone(),
        )?;
        Ok(Response::new(wire::ListWorkspaceMembershipsResponse {
            memberships,
            next_page_token,
        }))
    }
    #[tracing::instrument(skip_all, name = "coral.private.get_membership")]
    async fn get_workspace_membership(
        &self,
        request: Request<wire::GetWorkspaceMembershipRequest>,
    ) -> Result<Response<wire::WorkspaceMembership>, Status> {
        let actor = self.management_user(&request).await?;
        let (w, user) = member(&request.get_ref().name)?;
        Ok(Response::new(
            self.membership_view(&actor, &w, &user).await?,
        ))
    }
    #[tracing::instrument(skip_all, name = "coral.private.create_membership")]
    async fn create_workspace_membership(
        &self,
        request: Request<wire::CreateWorkspaceMembershipRequest>,
    ) -> Result<Response<wire::WorkspaceMembership>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let w = workspace_id(&input.parent)?;
        let context = change(actor.clone())?
            .with_request_id(&input.request_id)
            .map_err(error)?;
        let granted = self
            .workspaces
            .grant_membership_by_email(&context, &w, &input.email, parse_role(input.role)?)
            .await
            .map_err(error)?;
        Ok(Response::new(
            self.membership_view(&actor, &w, granted.user_id()).await?,
        ))
    }
    #[tracing::instrument(skip_all, name = "coral.private.update_membership")]
    async fn update_workspace_membership(
        &self,
        request: Request<wire::UpdateWorkspaceMembershipRequest>,
    ) -> Result<Response<wire::WorkspaceMembership>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        mask(input.update_mask, &["role"])?;
        let m = input
            .membership
            .ok_or_else(|| invalid("membership required"))?;
        let (w, user) = member(&m.name)?;
        let context = change(actor.clone())?.with_etag(&m.etag).map_err(error)?;
        self.workspaces
            .manage_membership(
                &context,
                &w,
                &AuthenticatedUser::without_organization_membership(user.clone()),
                coral_workspace::MembershipChange::UpdateRole(parse_role(m.role)?),
            )
            .await
            .map_err(error)?;
        Ok(Response::new(
            self.membership_view(&actor, &w, &user).await?,
        ))
    }
    #[tracing::instrument(skip_all, name = "coral.private.delete_membership")]
    async fn delete_workspace_membership(
        &self,
        request: Request<wire::DeleteWorkspaceMembershipRequest>,
    ) -> Result<Response<()>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let (w, user) = member(&input.name)?;
        self.access_lifecycle
            .remove_workspace_member(
                &change(actor)?.with_etag(&input.etag).map_err(error)?,
                &w,
                &user,
            )
            .await
            .map_err(super::workspaces::administration_error)?;
        Ok(Response::new(()))
    }
    #[tracing::instrument(skip_all, name = "coral.private.list_workspace_owners")]
    async fn list_workspace_owners(
        &self,
        request: Request<wire::ListWorkspaceOwnersRequest>,
    ) -> Result<Response<wire::ListWorkspaceOwnersResponse>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let w = workspace_id(&input.parent)?;
        let owners = self
            .workspaces
            .list_owners(&actor, &w)
            .await
            .map_err(error)?;
        let values = owners.into_iter().map(super::workspaces::owner).collect();
        let (owners, next_page_token) = page(
            values,
            input.page_size,
            &input.page_token,
            &format!("{}:{w}:owners", actor.user_id()),
            |o| o.user.clone(),
        )?;
        Ok(Response::new(wire::ListWorkspaceOwnersResponse {
            owners,
            next_page_token,
        }))
    }
}
