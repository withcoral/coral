use super::authored_tools::{diagnostic, error, private_tool_agent};
use super::status::invalid;
use super::workspaces::{next_after, page_request, timestamp, workspace_id};
use super::{GrpcServices, management as wire};
use coral_tools::{PrivateTool, PrivateToolChangeContext, PrivateToolPath, PrivateToolSummary};
use coral_workspace::{AuthenticatedPrincipal, TraceId};
use tonic::{Request, Response, Status};

fn path(value: &str) -> Result<PrivateToolPath, Status> {
    PrivateToolPath::parse(value).map_err(|_error| invalid())
}

fn summary(summary: &PrivateToolSummary) -> wire::PrivateToolSummary {
    wire::PrivateToolSummary {
        path: summary.path().as_str().to_owned(),
        description: summary.description().as_str().to_owned(),
        update_time: Some(timestamp(summary.saved_at())),
    }
}

fn private_tool(function: &PrivateTool) -> wire::PrivateTool {
    wire::PrivateTool {
        path: function.path().as_str().to_owned(),
        description: function.description().as_str().to_owned(),
        declaration: function.declaration().as_str().to_owned(),
        source: function.source().as_str().to_owned(),
        diagnostics: function.diagnostics().iter().map(diagnostic).collect(),
        diagnostics_truncated: function.diagnostics_truncated(),
        update_time: Some(timestamp(function.saved_at())),
    }
}

#[tonic::async_trait]
impl wire::private_tool_service_server::PrivateToolService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.list_private_tools")]
    async fn list_private_tools(
        &self,
        request: Request<wire::ListPrivateToolsRequest>,
    ) -> Result<Response<wire::ListPrivateToolsResponse>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let w = workspace_id(&input.parent)?;
        let asked = page_request::<PrivateToolPath>(input.page_size, &input.after)?;
        let private_provider = self
            .providers
            .inspect_authored_tool_provider(&user, &w, coral_catalog::AuthoredToolKind::Private)
            .await
            .map_err(super::providers::error)?;
        let page = self
            .private_tools
            .list(&AuthenticatedPrincipal::user(user.into_user()), &w, &asked)
            .await
            .map_err(error)?;
        Ok(Response::new(wire::ListPrivateToolsResponse {
            private_tools: page.items.iter().map(summary).collect(),
            next_after: next_after(page.next_after.as_ref()),
            private_provider: super::providers::provider_name(&private_provider),
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.get_private_tool")]
    async fn get_private_tool(
        &self,
        request: Request<wire::GetPrivateToolRequest>,
    ) -> Result<Response<wire::PrivateTool>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let w = workspace_id(&input.parent)?;
        let path = path(&input.path)?;
        let actor = AuthenticatedPrincipal::user(user.into_user());
        let tool = match private_tool_agent(&input.agent, &w)? {
            Some(agent) => {
                self.private_tools
                    .get_agent(&actor, &w, &agent, &path)
                    .await
            }
            None => self.private_tools.get(&actor, &w, &path).await,
        }
        .map_err(error)?;
        Ok(Response::new(private_tool(&tool)))
    }

    #[tracing::instrument(skip_all, name = "coral.private.delete_private_tool")]
    async fn delete_private_tool(
        &self,
        request: Request<wire::DeletePrivateToolRequest>,
    ) -> Result<Response<()>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let w = workspace_id(&input.parent)?;
        let path = path(&input.path)?;
        let now = user.now();
        let context = PrivateToolChangeContext::new(
            AuthenticatedPrincipal::user(user.into_user()),
            now,
            TraceId::current_or_new(),
        );
        let agent = private_tool_agent(&input.agent, &w)?;
        self.private_tool_workflow
            .delete(&context, &w, agent.as_ref(), &path)
            .await
            .map_err(error)?;
        Ok(Response::new(()))
    }
}
