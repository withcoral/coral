use super::workspaces::{
    annotate, error as workspace_error, invalid, page_start, page_token, safe_status, timestamp,
    workspace_id,
};
use super::{GrpcServices, management as wire, now};
use coral_tools::{
    AuthoredToolDiagnostic, AuthoredToolDiagnosticCategory, PrivateTool, PrivateToolChangeContext,
    PrivateToolPath, PrivateToolSummary,
};
use coral_workspace::{AuthenticatedPrincipal, TraceId};
use tonic::{Request, Response, Status};

fn error(error: coral_tools::Error) -> Status {
    use coral_tools::Error as E;
    let (code, reason) = match error {
        E::Workspace(error) => return workspace_error(error),
        E::InvalidInput(_) => (tonic::Code::InvalidArgument, "INVALID_ARGUMENT"),
        E::PrivateToolNotFound => (tonic::Code::NotFound, "NOT_FOUND"),
        E::PermissionDenied(_) => (tonic::Code::PermissionDenied, "PERMISSION_DENIED"),
        E::PrivateToolExists => (tonic::Code::AlreadyExists, "ALREADY_EXISTS"),
        E::PrivateToolRevisionConflict { .. } => (tonic::Code::Aborted, "ABORTED"),
        // Reads and deletes never compile, and no Private Tool call reads or
        // writes a Shared Tool. Name every variant so a new one needs a wire
        // decision.
        E::SharedToolNotFound
        | E::SharedToolRevisionConflict { .. }
        | E::SharedToolSourceUnreadable
        | E::SharedToolPathConflict
        | E::SharedToolAdmission(_)
        | E::SharedToolNotData(_)
        | E::SharedToolCompilationTimedOut
        | E::PrivateToolAdmission(_)
        | E::PrivateToolNotData(_)
        | E::PrivateToolPathConflict
        | E::PrivateToolCompilationTimedOut
        | E::CompilerBusy
        | E::PackageTooLarge { .. }
        | E::CompilerUnavailable(_)
        | E::CompilerWorkerStopped
        | E::Clock(_)
        | E::Json(_)
        | E::CorruptData(_)
        | E::Database(_) => (tonic::Code::Unavailable, "UNAVAILABLE"),
    };
    safe_status(code, reason)
}

fn path(value: &str) -> Result<PrivateToolPath, Status> {
    PrivateToolPath::parse(value).map_err(|_error| invalid("invalid Private Tool Path"))
}

fn summary(summary: &PrivateToolSummary) -> wire::PrivateToolSummary {
    wire::PrivateToolSummary {
        path: summary.path().as_str().to_owned(),
        description: summary.description().as_str().to_owned(),
        update_time: Some(timestamp(summary.saved_at_unix_nanos())),
    }
}

pub(super) fn diagnostic(diagnostic: &AuthoredToolDiagnostic) -> wire::PrivateToolDiagnostic {
    let category = match diagnostic.category() {
        AuthoredToolDiagnosticCategory::Message => wire::PrivateToolDiagnosticCategory::Message,
        AuthoredToolDiagnosticCategory::Warning => wire::PrivateToolDiagnosticCategory::Warning,
        AuthoredToolDiagnosticCategory::Error => wire::PrivateToolDiagnosticCategory::Error,
        AuthoredToolDiagnosticCategory::Suggestion => {
            wire::PrivateToolDiagnosticCategory::Suggestion
        }
    };
    wire::PrivateToolDiagnostic {
        category: category.into(),
        code: diagnostic.code(),
        message: diagnostic.message().to_owned(),
        line: diagnostic.line(),
        column: diagnostic.column(),
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
        update_time: Some(timestamp(function.saved_at_unix_nanos())),
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
        let scope = format!("{}:{w}:privateTools", user.user_id());
        let (page_size, after) = page_start(input.page_size, &input.page_token, &scope)?;
        let after = if after.is_empty() {
            None
        } else {
            Some(PrivateToolPath::parse(&after).map_err(|_error| invalid("invalid page token"))?)
        };
        let private_provider = self
            .providers
            .inspect_private_provider(&user, &w)
            .await
            .map_err(super::providers::error)?;
        let page = self
            .private_tools
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
        Ok(Response::new(wire::ListPrivateToolsResponse {
            private_tools: page.summaries().iter().map(summary).collect(),
            next_page_token,
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
        let function = self
            .private_tools
            .get(&AuthenticatedPrincipal::user(user), &w, &path)
            .await
            .map_err(error)?;
        Ok(Response::new(private_tool(&function)))
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
        let context = PrivateToolChangeContext::new(
            AuthenticatedPrincipal::user(user),
            now().map_err(annotate)?,
            TraceId::current_or_new(),
        );
        self.private_tool_deletion
            .delete(&context, &w, &path)
            .await
            .map_err(error)?;
        Ok(Response::new(()))
    }
}
