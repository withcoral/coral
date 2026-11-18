use super::resource;
use super::status::invalid;
use super::workspaces::{change, error, mask, page, timestamp, workspace_id};
use super::{GrpcServices, management as wire};
use coral_workspace::{
    Agent, AgentDisplayName, AgentRegistration, AgentState, IfMatch, IssuedAgentCredential,
    PrincipalId, RequestId, ResourceMetadata, WorkspaceId,
};
use tonic::{Request, Response, Status};

fn agent_id(name: &str) -> Result<(WorkspaceId, PrincipalId), Status> {
    let (w, id) = resource(name, "agents")?;
    Ok((w, PrincipalId::parse(id).map_err(error)?))
}
fn registration_id(name: &str) -> Result<(WorkspaceId, PrincipalId), Status> {
    agent_id(name.strip_suffix("/registration").ok_or_else(invalid)?)
}
#[expect(
    clippy::needless_pass_by_value,
    reason = "consumes the coherent domain snapshot together with its metadata"
)]
fn agent(a: Agent, meta: ResourceMetadata) -> wire::Agent {
    wire::Agent {
        name: format!(
            "workspaces/{}/agents/{}",
            a.workspace_id(),
            a.principal().id()
        ),
        display_name: a.display_name().as_str().into(),
        created_by: format!("users/{}", a.created_by()),
        state: match a.state() {
            AgentState::Enabled => wire::AgentState::Enabled,
            AgentState::Disabled => wire::AgentState::Disabled,
        }
        .into(),
        etag: meta.version.to_string(),
        create_time: meta.created_at.map(timestamp),
        update_time: meta.updated_at.map(timestamp),
        creator_profile: None,
        access_blockers: Vec::new(),
    }
}
#[expect(
    clippy::needless_pass_by_value,
    reason = "consumes the coherent domain snapshot together with its metadata"
)]
fn registration(r: AgentRegistration, meta: ResourceMetadata) -> wire::AgentRegistration {
    wire::AgentRegistration {
        name: format!(
            "workspaces/{}/agents/{}/registration",
            r.workspace_id(),
            r.agent_id()
        ),
        uid: r.id().to_string(),
        enabled: r.is_enabled(),
        expire_time: r.expires_at().map(timestamp),
        etag: meta.version.to_string(),
        create_time: meta.created_at.map(timestamp),
        update_time: meta.updated_at.map(timestamp),
    }
}
fn issued(view: IssuedAgentCredential, now: coral_db::UnixNanos) -> wire::IssuedAgentCredential {
    let agent = agent_view(view.inspection(), now);
    wire::IssuedAgentCredential {
        agent: Some(agent),
        registration: Some(registration(view.registration, view.registration_metadata)),
        credential: view.credential.expose().into(),
    }
}
fn expiration(
    value: Option<prost_types::Timestamp>,
) -> Result<Option<coral_db::UnixNanos>, Status> {
    value
        .map(|t| {
            if !(0..1_000_000_000).contains(&t.nanos) {
                return Err(invalid());
            }
            t.seconds
                .checked_mul(1_000_000_000)
                .and_then(|v| v.checked_add(i64::from(t.nanos)))
                .map(coral_db::UnixNanos::new)
                .ok_or_else(invalid)
        })
        .transpose()
}

/// An Agent as the page shows it, with what blocks its access at `now`.
fn agent_view(view: coral_workspace::AgentInspection, now: coral_db::UnixNanos) -> wire::Agent {
    let blockers = view.access_blockers(now);
    let profile = view.creator_profile;
    let mut result = agent(view.agent, view.agent_metadata);
    result.creator_profile = Some(super::workspaces::profile(profile));
    result.access_blockers = blockers
        .into_iter()
        .map(|b| match b {
            coral_workspace::AgentAccessBlocker::WorkspaceArchived => {
                wire::AgentAccessBlocker::WorkspaceArchived
            }
            coral_workspace::AgentAccessBlocker::AgentDisabled => {
                wire::AgentAccessBlocker::AgentDisabled
            }
            coral_workspace::AgentAccessBlocker::ApiSuspended => {
                wire::AgentAccessBlocker::ApiSuspended
            }
            coral_workspace::AgentAccessBlocker::Expired => wire::AgentAccessBlocker::Expired,
        } as i32)
        .collect();
    result
}

#[tonic::async_trait]
impl wire::agent_service_server::AgentService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.create_agent")]
    async fn create_agent(
        &self,
        request: Request<wire::CreateAgentRequest>,
    ) -> Result<Response<wire::IssuedAgentCredential>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let w = workspace_id(&input.parent)?;
        let created = self
            .workspaces
            .create_agent(
                &change(actor.clone()),
                &RequestId::parse(&input.request_id).map_err(error)?,
                &w,
                AgentDisplayName::parse(&input.display_name).map_err(error)?,
                expiration(input.expire_time)?,
            )
            .await
            .map_err(error)?;
        Ok(Response::new(issued(created, actor.now())))
    }
    #[tracing::instrument(skip_all, name = "coral.private.list_agents")]
    async fn list_agents(
        &self,
        request: Request<wire::ListAgentsRequest>,
    ) -> Result<Response<wire::ListAgentsResponse>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let w = workspace_id(&input.parent)?;
        let agents = self
            .workspaces
            .inspect_agents(&actor, &w)
            .await
            .map_err(error)?;
        // The request's time decides every Agent's blockers.
        let now = actor.now();
        let values = agents
            .into_iter()
            .map(|view| agent_view(view, now))
            .collect::<Vec<_>>();
        let (agents, next_page_token) = page(
            values,
            input.page_size,
            &input.page_token,
            &format!("{}:{w}:agents", actor.user_id()),
            |a| a.name.clone(),
        )?;
        Ok(Response::new(wire::ListAgentsResponse {
            agents,
            next_page_token,
        }))
    }
    #[tracing::instrument(skip_all, name = "coral.private.get_agent")]
    async fn get_agent(
        &self,
        request: Request<wire::GetAgentRequest>,
    ) -> Result<Response<wire::Agent>, Status> {
        let actor = self.management_user(&request).await?;
        let (w, id) = agent_id(&request.get_ref().name)?;
        let view = self
            .workspaces
            .inspect_agent(&actor, &w, &id)
            .await
            .map_err(error)?;
        Ok(Response::new(agent_view(view, actor.now())))
    }
    #[tracing::instrument(skip_all, name = "coral.private.update_agent")]
    async fn update_agent(
        &self,
        request: Request<wire::UpdateAgentRequest>,
    ) -> Result<Response<wire::Agent>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        mask(input.update_mask, &["state"])?;
        let a = input.agent.ok_or_else(invalid)?;
        let (w, id) = agent_id(&a.name)?;
        let state = match wire::AgentState::try_from(a.state) {
            Ok(wire::AgentState::Enabled) => AgentState::Enabled,
            Ok(wire::AgentState::Disabled) => AgentState::Disabled,
            _ => return Err(invalid()),
        };
        self.workspaces
            .set_agent_state(
                &change(actor.clone()),
                &w,
                &id,
                &IfMatch::parse(&a.etag).map_err(error)?,
                state,
            )
            .await
            .map_err(error)?;
        let view = self
            .workspaces
            .inspect_agent(&actor, &w, &id)
            .await
            .map_err(error)?;
        Ok(Response::new(agent_view(view, actor.now())))
    }
    #[tracing::instrument(skip_all, name = "coral.private.delete_agent")]
    async fn delete_agent(
        &self,
        request: Request<wire::DeleteAgentRequest>,
    ) -> Result<Response<()>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let (w, id) = agent_id(&input.name)?;
        self.access_lifecycle
            .delete_agent(
                &change(actor),
                &w,
                &id,
                &IfMatch::parse(&input.etag).map_err(error)?,
            )
            .await
            .map_err(super::workspaces::administration_error)?;
        Ok(Response::new(()))
    }
}

#[tonic::async_trait]
impl wire::agent_registration_service_server::AgentRegistrationService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.get_agent_registration")]
    async fn get_agent_registration(
        &self,
        request: Request<wire::GetAgentRegistrationRequest>,
    ) -> Result<Response<wire::AgentRegistration>, Status> {
        let actor = self.management_user(&request).await?;
        let (w, id) = registration_id(&request.get_ref().name)?;
        let view = self
            .workspaces
            .inspect_agent(&actor, &w, &id)
            .await
            .map_err(error)?;
        Ok(Response::new(registration(
            view.registration,
            view.registration_metadata,
        )))
    }
    #[tracing::instrument(skip_all, name = "coral.private.update_agent_registration")]
    async fn update_agent_registration(
        &self,
        request: Request<wire::UpdateAgentRegistrationRequest>,
    ) -> Result<Response<wire::AgentRegistration>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let fields = mask(input.update_mask, &["enabled", "expire_time"])?;
        let r = input.registration.ok_or_else(invalid)?;
        let (w, id) = registration_id(&r.name)?;
        let view = self
            .workspaces
            .inspect_agent(&actor, &w, &id)
            .await
            .map_err(error)?;
        let enabled = if fields.iter().any(|p| p == "enabled") {
            r.enabled
        } else {
            view.registration.is_enabled()
        };
        let expires = if fields.iter().any(|p| p == "expire_time") {
            expiration(r.expire_time)?
        } else {
            view.registration.expires_at()
        };
        self.workspaces
            .update_agent_registration(
                &change(actor.clone()),
                &w,
                view.registration.id(),
                &IfMatch::parse(&r.etag).map_err(error)?,
                enabled,
                expires,
            )
            .await
            .map_err(error)?;
        let view = self
            .workspaces
            .inspect_agent(&actor, &w, &id)
            .await
            .map_err(error)?;
        Ok(Response::new(registration(
            view.registration,
            view.registration_metadata,
        )))
    }
    #[tracing::instrument(skip_all, name = "coral.private.rotate_agent_credential")]
    async fn rotate_agent_credential(
        &self,
        request: Request<wire::RotateAgentCredentialRequest>,
    ) -> Result<Response<wire::IssuedAgentCredential>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let (w, id) = registration_id(&input.name)?;
        let view = self
            .workspaces
            .inspect_agent(&actor, &w, &id)
            .await
            .map_err(error)?;
        // The etag is read before the request id, as it always was.
        let if_match = IfMatch::parse(&input.etag).map_err(error)?;
        let rotation = RequestId::parse(&input.request_id).map_err(error)?;
        let rotated = self
            .workspaces
            .rotate_agent_credential(
                &change(actor.clone()),
                &rotation,
                &w,
                view.registration.id(),
                &if_match,
            )
            .await
            .map_err(error)?;
        Ok(Response::new(issued(rotated, actor.now())))
    }
}
