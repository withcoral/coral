use super::authored_tools::{diagnostic, error};
use super::policies::{access, access_state};
use super::workspaces::{
    annotate, invalid, page_start, page_token, resource, timestamp, workspace_id,
};
use super::{GrpcServices, management as wire, now};
use coral_tools::{
    AuthoredToolDescription, AuthoredToolSource, PrivateToolPath, SharedTool, SharedToolOrigin,
    SharedToolPath, SharedToolPublish, SharedToolReview, SharedToolRevision,
    SharedToolRevisionOrigin, SharedToolSummary,
};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, ChangeContext, Principal, PrincipalId,
    PrincipalKind, TraceId, WorkspaceId,
};
use tonic::{Request, Response, Status};

fn path(value: &str) -> Result<SharedToolPath, Status> {
    SharedToolPath::parse(value).map_err(|_error| invalid("invalid Shared Tool Path"))
}

fn user_name(user: &coral_workspace::UserId) -> String {
    format!("users/{user}")
}

fn summary(summary: &SharedToolSummary) -> wire::SharedToolSummary {
    wire::SharedToolSummary {
        path: summary.path().as_str().to_owned(),
        description: summary.description().as_str().to_owned(),
        revision: summary.current_revision(),
        update_time: Some(timestamp(summary.published_at_unix_nanos())),
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
            let owner = if private_tool.agent.is_empty() {
                AuthenticatedPrincipal::user(user.clone())
                    .principal()
                    .clone()
            } else {
                let (agent_workspace, agent) = resource(&private_tool.agent, "agents")?;
                if &agent_workspace != workspace {
                    return Err(invalid("invalid Agent name"));
                }
                Principal::from_parts(
                    PrincipalId::parse(agent).map_err(|_error| invalid("invalid Agent name"))?,
                    PrincipalKind::Agent,
                )
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
            shared_tools: page.summaries().iter().map(summary).collect(),
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
        let w = workspace_id(&input.parent)?;
        let path = path(&input.path)?;
        let revisions = self
            .shared_tools
            .list_revisions(&AuthenticatedPrincipal::user(user), &w, &path)
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
        let w = workspace_id(&input.parent)?;
        let path = path(&input.path)?;
        let found = self
            .shared_tools
            .get_revision(
                &AuthenticatedPrincipal::user(user),
                &w,
                &path,
                input.revision,
            )
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
        let mut publish = SharedToolPublish::new(
            path(&input.path)?,
            origin(input.origin, &w, &user)?,
            AuthoredToolDescription::parse(&input.description)
                .map_err(|_error| invalid("invalid description"))?,
            AuthoredToolSource::parse(&input.source).map_err(|_error| invalid("invalid source"))?,
            access(input.default_access_state)?,
            input.expected_revision,
        );
        if input.validate_only {
            publish = publish.validate_only();
        }
        let context = ChangeContext::new(user, now().map_err(annotate)?, TraceId::current_or_new());
        // The source is typed from the Tools its publishing Owner can
        // discover, as a Private Tool save types it (ADR 0020).
        let author = AuthenticatedPrincipal::user(context.actor().clone());
        let declarations =
            coral_search::CallerToolDeclarations::new(&self.provider_readiness, &author, &w);
        let published = self
            .shared_tools
            .publish(&context, &w, publish, Some(&declarations))
            .await
            .map_err(error)?;
        Ok(Response::new(wire::PublishSharedToolResponse {
            shared_tool: published.published().map(shared_tool),
            review: Some(review(published.review())),
        }))
    }
}
