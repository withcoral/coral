use super::authored_tools::{correctable, diagnostic, error, private_tool_agent};
use super::policies::{access, access_state};
use super::workspaces::{
    annotate, invalid, page_start, page_token, resource, timestamp, workspace_id,
};
use super::{GrpcServices, management as wire, now};
use coral_tools::{
    AuthoredToolDescription, AuthoredToolSource, PrivateToolPath, SharedTool, SharedToolId,
    SharedToolLifecycleAction, SharedToolLifecycleEvent, SharedToolLifecycleEventId,
    SharedToolOrigin, SharedToolPath, SharedToolPublish, SharedToolReview, SharedToolRevision,
    SharedToolRevisionOrigin, SharedToolSummary,
};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, ChangeContext, Principal, PrincipalKind, TraceId,
    WorkspaceId,
};
use tonic::{Request, Response, Status};

fn path(value: &str) -> Result<SharedToolPath, Status> {
    SharedToolPath::parse(value).map_err(|_error| invalid("invalid Shared Tool Path"))
}

fn user_name(user: &coral_workspace::UserId) -> String {
    format!("users/{user}")
}

/// The resource name that names a Shared Tool for good, after a removal too.
fn resource_name(workspace: &WorkspaceId, id: &SharedToolId) -> String {
    format!("workspaces/{workspace}/sharedTools/{id}")
}

/// The Workspace and identity a Shared Tool's resource name holds.
fn parse_resource_name(name: &str) -> Result<(WorkspaceId, SharedToolId), Status> {
    let (workspace, id) = resource(name, "sharedTools")?;
    let id = SharedToolId::parse(id).map_err(|_error| invalid("invalid Shared Tool name"))?;
    Ok((workspace, id))
}

fn summary(workspace: &WorkspaceId, summary: &SharedToolSummary) -> wire::SharedToolSummary {
    wire::SharedToolSummary {
        path: summary.path().as_str().to_owned(),
        description: summary.description().as_str().to_owned(),
        revision: summary.current_revision(),
        update_time: Some(timestamp(summary.published_at_unix_nanos())),
        name: resource_name(workspace, summary.id()),
    }
}

fn event(workspace: &WorkspaceId, event: &SharedToolLifecycleEvent) -> wire::SharedToolEvent {
    let action = match event.action() {
        SharedToolLifecycleAction::Published => wire::SharedToolEventAction::Published,
        SharedToolLifecycleAction::Removed => wire::SharedToolEventAction::Removed,
    };
    wire::SharedToolEvent {
        id: event.id().to_string(),
        action: action.into(),
        shared_tool: resource_name(workspace, event.shared_tool_id()),
        path: event.path().as_str().to_owned(),
        revision: event.revision(),
        content_hash: event.content_hash().to_owned(),
        actor: user_name(event.actor()),
        event_time: Some(timestamp(event.occurred_at_unix_nanos())),
    }
}

/// The earlier revision `revision` restored, or zero when it copied a
/// Private Tool.
const fn restored_from(revision: &SharedToolRevision) -> u64 {
    match revision.origin() {
        SharedToolRevisionOrigin::Revision(restored) => *restored,
        SharedToolRevisionOrigin::PrivateTool(_) => 0,
    }
}

fn revision(revision: &SharedToolRevision) -> wire::SharedToolRevision {
    wire::SharedToolRevision {
        revision: revision.revision(),
        description: revision.description().as_str().to_owned(),
        declaration: revision.declaration().as_str().to_owned(),
        source: revision.source().as_str().to_owned(),
        diagnostics: revision.diagnostics().iter().map(diagnostic).collect(),
        diagnostics_truncated: revision.diagnostics_truncated(),
        content_hash: revision.content_hash().to_owned(),
        default_access_state: access_state(revision.default_access_state()).into(),
        publisher: user_name(revision.published_by()),
        publish_time: Some(timestamp(revision.published_at_unix_nanos())),
        restored_from_revision: restored_from(revision),
    }
}

fn revision_summary(revision: &SharedToolRevision) -> wire::SharedToolRevisionSummary {
    wire::SharedToolRevisionSummary {
        revision: revision.revision(),
        description: revision.description().as_str().to_owned(),
        content_hash: revision.content_hash().to_owned(),
        default_access_state: access_state(revision.default_access_state()).into(),
        publisher: user_name(revision.published_by()),
        publish_time: Some(timestamp(revision.published_at_unix_nanos())),
        restored_from_revision: restored_from(revision),
    }
}

fn shared_tool(tool: &SharedTool) -> wire::SharedTool {
    wire::SharedTool {
        path: tool.path().as_str().to_owned(),
        creator: user_name(tool.created_by()),
        create_time: Some(timestamp(tool.created_at_unix_nanos())),
        current_revision: Some(revision(tool.current())),
        name: resource_name(tool.workspace_id(), tool.id()),
    }
}

fn review(review: &SharedToolReview) -> wire::SharedToolReview {
    wire::SharedToolReview {
        previous_revision: review.previous().map(revision),
        declaration: review.declaration().as_str().to_owned(),
        diagnostics: review.diagnostics().iter().map(diagnostic).collect(),
        diagnostics_truncated: review.diagnostics_truncated(),
        called_tools: review.called_tools().to_vec(),
        shared_tools: review
            .shared_tools()
            .iter()
            .map(|called| wire::SharedToolCall {
                path: called.path().as_str().to_owned(),
                revision: called.revision(),
            })
            .collect(),
        runtime_built_paths: review.runtime_built_paths().to_vec(),
    }
}

/// What the request's publish copies: the caller's own Private Tool, an
/// Agent's of the same Workspace, or an earlier revision.
fn origin(
    origin: Option<wire::publish_shared_tool_request::Origin>,
    workspace: &WorkspaceId,
    user: &AuthenticatedUser,
) -> Result<SharedToolOrigin, Status> {
    use wire::publish_shared_tool_request::Origin;
    match origin {
        Some(Origin::PrivateTool(private_tool)) => {
            let owner = match private_tool_agent(&private_tool.agent, workspace)? {
                Some(agent) => Principal::from_parts(agent, PrincipalKind::Agent),
                None => AuthenticatedPrincipal::user(user.clone())
                    .principal()
                    .clone(),
            };
            Ok(SharedToolOrigin::PrivateTool {
                owner,
                path: PrivateToolPath::parse(&private_tool.path)
                    .map_err(|_error| invalid("invalid Private Tool Path"))?,
            })
        }
        Some(Origin::RestoreRevision(0)) | None => Err(invalid("origin is required")),
        Some(Origin::RestoreRevision(restored)) => Ok(SharedToolOrigin::Revision(restored)),
    }
}

#[tonic::async_trait]
impl wire::shared_tool_service_server::SharedToolService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.list_shared_tools")]
    async fn list_shared_tools(
        &self,
        request: Request<wire::ListSharedToolsRequest>,
    ) -> Result<Response<wire::ListSharedToolsResponse>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let w = workspace_id(&input.parent)?;
        let scope = format!("{}:{w}:sharedTools", user.user_id());
        let (page_size, after) = page_start(input.page_size, &input.page_token, &scope)?;
        let after = if after.is_empty() {
            None
        } else {
            Some(SharedToolPath::parse(&after).map_err(|_error| invalid("invalid page token"))?)
        };
        let shared_provider = self
            .providers
            .inspect_authored_tool_provider(&user, &w, coral_catalog::AuthoredToolKind::Shared)
            .await
            .map_err(super::providers::error)?;
        let page = self
            .shared_tools
            .list(
                &AuthenticatedPrincipal::user(user),
                &w,
                after.as_ref(),
                page_size,
            )
            .await
            .map_err(error)?;
        let next_page_token = page
            .next_after()
            .map(|after| page_token(&scope, after.as_str()))
            .transpose()?
            .unwrap_or_default();
        Ok(Response::new(wire::ListSharedToolsResponse {
            shared_tools: page
                .summaries()
                .iter()
                .map(|listed| summary(&w, listed))
                .collect(),
            next_page_token,
            shared_provider: super::providers::provider_name(&shared_provider),
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.get_shared_tool")]
    async fn get_shared_tool(
        &self,
        request: Request<wire::GetSharedToolRequest>,
    ) -> Result<Response<wire::SharedTool>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let w = workspace_id(&input.parent)?;
        let path = path(&input.path)?;
        let tool = self
            .shared_tools
            .get(&AuthenticatedPrincipal::user(user), &w, &path)
            .await
            .map_err(error)?;
        Ok(Response::new(shared_tool(&tool)))
    }

    #[tracing::instrument(skip_all, name = "coral.private.list_shared_tool_revisions")]
    async fn list_shared_tool_revisions(
        &self,
        request: Request<wire::ListSharedToolRevisionsRequest>,
    ) -> Result<Response<wire::ListSharedToolRevisionsResponse>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let (w, id) = parse_resource_name(&input.shared_tool)?;
        let revisions = self
            .shared_tools
            .list_revisions(&AuthenticatedPrincipal::user(user), &w, &id)
            .await
            .map_err(error)?;
        Ok(Response::new(wire::ListSharedToolRevisionsResponse {
            revisions: revisions.iter().map(revision_summary).collect(),
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.get_shared_tool_revision")]
    async fn get_shared_tool_revision(
        &self,
        request: Request<wire::GetSharedToolRevisionRequest>,
    ) -> Result<Response<wire::SharedToolRevision>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let (w, id) = parse_resource_name(&input.shared_tool)?;
        let found = self
            .shared_tools
            .get_revision(&AuthenticatedPrincipal::user(user), &w, &id, input.revision)
            .await
            .map_err(error)?;
        Ok(Response::new(revision(&found)))
    }

    #[tracing::instrument(skip_all, name = "coral.private.publish_shared_tool")]
    async fn publish_shared_tool(
        &self,
        request: Request<wire::PublishSharedToolRequest>,
    ) -> Result<Response<wire::PublishSharedToolResponse>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let w = workspace_id(&input.parent)?;
        // The Owner names the path of a publish, so its refusal says what a
        // Shared Tool Path is.
        let path = SharedToolPath::parse(&input.path)
            .map_err(|refusal| correctable("SHARED_TOOL_PATH_INVALID", &refusal))?;
        let mut publish = SharedToolPublish::review(
            path,
            origin(input.origin, &w, &user)?,
            AuthoredToolDescription::parse(&input.description)
                .map_err(|_error| invalid("invalid description"))?,
            AuthoredToolSource::parse(&input.source).map_err(|_error| invalid("invalid source"))?,
            access(input.default_access_state)?,
            input.expected_revision,
        );
        if !input.validate_only {
            publish = publish.approve(input.reviewed_declaration);
        }
        let context = ChangeContext::new(user, now().map_err(annotate)?, TraceId::current_or_new());
        let published = self
            .shared_tool_workflow
            .publish(&context, &w, publish)
            .await
            .map_err(error)?;
        Ok(Response::new(wire::PublishSharedToolResponse {
            shared_tool: published.published().map(shared_tool),
            review: Some(review(published.review())),
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.remove_shared_tool")]
    async fn remove_shared_tool(
        &self,
        request: Request<wire::RemoveSharedToolRequest>,
    ) -> Result<Response<wire::RemoveSharedToolResponse>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let w = workspace_id(&input.parent)?;
        let path = path(&input.path)?;
        let context = ChangeContext::new(user, now().map_err(annotate)?, TraceId::current_or_new());
        let removed = self
            .shared_tool_workflow
            .remove(&context, &w, &path, input.expected_revision)
            .await
            .map_err(error)?;
        Ok(Response::new(wire::RemoveSharedToolResponse {
            shared_tool: resource_name(&w, &removed),
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.list_shared_tool_events")]
    async fn list_shared_tool_events(
        &self,
        request: Request<wire::ListSharedToolEventsRequest>,
    ) -> Result<Response<wire::ListSharedToolEventsResponse>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let w = workspace_id(&input.parent)?;
        let shared_tool = if input.shared_tool.is_empty() {
            None
        } else {
            let (workspace, id) = parse_resource_name(&input.shared_tool)?;
            if workspace != w {
                return Err(invalid("invalid Shared Tool name"));
            }
            Some(id)
        };
        // A page continues only the listing that began it.
        let scope = format!(
            "{}:{w}:sharedToolEvents:{}",
            user.user_id(),
            input.shared_tool
        );
        let (page_size, after) = page_start(input.page_size, &input.page_token, &scope)?;
        let after = if after.is_empty() {
            None
        } else {
            Some(
                SharedToolLifecycleEventId::parse(&after)
                    .map_err(|_error| invalid("invalid page token"))?,
            )
        };
        let page = self
            .shared_tools
            .list_lifecycle_events(
                &AuthenticatedPrincipal::user(user),
                &w,
                shared_tool.as_ref(),
                after.as_ref(),
                page_size,
            )
            .await
            .map_err(error)?;
        let next_page_token = page
            .next_after()
            .map(|after| page_token(&scope, &after.to_string()))
            .transpose()?
            .unwrap_or_default();
        Ok(Response::new(wire::ListSharedToolEventsResponse {
            events: page
                .events()
                .iter()
                .map(|listed| event(&w, listed))
                .collect(),
            next_page_token,
        }))
    }
}
