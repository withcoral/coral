use super::workspaces::{change, error, invalid, page, profile};
use super::{GrpcServices, management as wire};
use coral_workspace::{OrganizationRole, UserId};
use tonic::{Request, Response, Status};

pub(super) fn member_id(name: &str) -> Result<UserId, Status> {
    let user = name
        .strip_prefix("organizationMemberships/")
        .filter(|id| !id.contains('/'))
        .ok_or_else(|| invalid("invalid Organization Membership name"))?;
    UserId::parse(user).map_err(error)
}

fn membership(
    (membership, user): (
        coral_workspace::OrganizationMembership,
        coral_workspace::UserProfile,
    ),
) -> wire::OrganizationMembership {
    wire::OrganizationMembership {
        name: format!("organizationMemberships/{}", membership.user_id()),
        profile: Some(profile(user)),
        role: match membership.role() {
            OrganizationRole::Owner => wire::OrganizationRole::Owner,
            OrganizationRole::Member => wire::OrganizationRole::Member,
        }
        .into(),
        enabled: membership.enabled(),
        etag: membership.revision().to_string(),
    }
}

#[tonic::async_trait]
impl wire::organization_membership_service_server::OrganizationMembershipService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.get_current_organization_membership")]
    async fn get_current_organization_membership(
        &self,
        request: Request<wire::GetCurrentOrganizationMembershipRequest>,
    ) -> Result<Response<wire::OrganizationMembership>, Status> {
        let actor = self.management_user(&request).await?;
        Ok(Response::new(membership(
            self.organization
                .inspect_current_membership(&actor)
                .await
                .map_err(error)?,
        )))
    }

    #[tracing::instrument(skip_all, name = "coral.private.list_organization_memberships")]
    async fn list_organization_memberships(
        &self,
        request: Request<wire::ListOrganizationMembershipsRequest>,
    ) -> Result<Response<wire::ListOrganizationMembershipsResponse>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let values = self
            .organization
            .list_memberships(&actor)
            .await
            .map_err(error)?
            .into_iter()
            .map(membership)
            .collect();
        let (memberships, next_page_token) = page(
            values,
            input.page_size,
            &input.page_token,
            &format!("{}:organizationMemberships", actor.user_id()),
            |m| m.name.clone(),
        )?;
        Ok(Response::new(wire::ListOrganizationMembershipsResponse {
            memberships,
            next_page_token,
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.delete_organization_membership")]
    async fn delete_organization_membership(
        &self,
        request: Request<wire::DeleteOrganizationMembershipRequest>,
    ) -> Result<Response<()>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let user = member_id(&input.name)?;
        let transfer_to = if input.transfer_to.is_empty() {
            None
        } else {
            Some(member_id(&input.transfer_to)?)
        };
        let revision = input
            .etag
            .parse::<u64>()
            .ok()
            .filter(|revision| *revision > 0)
            .ok_or_else(|| invalid("current Organization Membership etag required"))?;
        self.access_lifecycle
            .remove_organization_member(&change(actor)?, &user, revision, transfer_to.as_ref())
            .await
            .map_err(super::workspaces::administration_error)?;
        Ok(Response::new(()))
    }
}
