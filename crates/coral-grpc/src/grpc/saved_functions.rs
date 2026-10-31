use super::workspaces::{
    annotate, error as workspace_error, invalid, page_start, page_token, safe_status, timestamp,
    workspace_id,
};
use super::{GrpcServices, management as wire, now};
use coral_functions::{
    SavedFunction, SavedFunctionChangeContext, SavedFunctionDiagnostic,
    SavedFunctionDiagnosticCategory, SavedFunctionPath, SavedFunctionSummary,
};
use coral_workspace::{AuthenticatedPrincipal, TraceId};
use tonic::{Request, Response, Status};

fn error(error: coral_functions::Error) -> Status {
    use coral_functions::Error as E;
    let (code, reason) = match error {
        E::Workspace(error) => return workspace_error(error),
        E::InvalidInput(_) => (tonic::Code::InvalidArgument, "INVALID_ARGUMENT"),
        E::SavedFunctionNotFound => (tonic::Code::NotFound, "NOT_FOUND"),
        E::PermissionDenied(_) => (tonic::Code::PermissionDenied, "PERMISSION_DENIED"),
        // Reads and deletes never compile. Name every variant so a new one needs a wire decision.
        E::SavedFunctionAdmission(_)
        | E::SavedFunctionCompilationTimedOut
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

fn path(value: &str) -> Result<SavedFunctionPath, Status> {
    SavedFunctionPath::parse(value).map_err(|_error| invalid("invalid Saved Function Path"))
}

fn summary(summary: &SavedFunctionSummary) -> wire::SavedFunctionSummary {
    wire::SavedFunctionSummary {
        path: summary.path().as_str().to_owned(),
        description: summary.description().as_str().to_owned(),
        update_time: Some(timestamp(summary.saved_at_unix_nanos())),
    }
}

fn diagnostic(diagnostic: &SavedFunctionDiagnostic) -> wire::SavedFunctionDiagnostic {
    let category = match diagnostic.category() {
        SavedFunctionDiagnosticCategory::Message => wire::SavedFunctionDiagnosticCategory::Message,
        SavedFunctionDiagnosticCategory::Warning => wire::SavedFunctionDiagnosticCategory::Warning,
        SavedFunctionDiagnosticCategory::Error => wire::SavedFunctionDiagnosticCategory::Error,
        SavedFunctionDiagnosticCategory::Suggestion => {
            wire::SavedFunctionDiagnosticCategory::Suggestion
        }
    };
    wire::SavedFunctionDiagnostic {
        category: category.into(),
        code: diagnostic.code(),
        message: diagnostic.message().to_owned(),
        line: diagnostic.line(),
        column: diagnostic.column(),
    }
}

fn saved_function(function: &SavedFunction) -> wire::SavedFunction {
    wire::SavedFunction {
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
impl wire::saved_function_service_server::SavedFunctionService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.list_saved_functions")]
    async fn list_saved_functions(
        &self,
        request: Request<wire::ListSavedFunctionsRequest>,
    ) -> Result<Response<wire::ListSavedFunctionsResponse>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let w = workspace_id(&input.parent)?;
        let scope = format!("{}:{w}:savedFunctions", user.user_id());
        let (page_size, after) = page_start(input.page_size, &input.page_token, &scope)?;
        let after = if after.is_empty() {
            None
        } else {
            Some(SavedFunctionPath::parse(&after).map_err(|_error| invalid("invalid page token"))?)
        };
        let page = self
            .saved_functions
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
        Ok(Response::new(wire::ListSavedFunctionsResponse {
            saved_functions: page.summaries().iter().map(summary).collect(),
            next_page_token,
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.get_saved_function")]
    async fn get_saved_function(
        &self,
        request: Request<wire::GetSavedFunctionRequest>,
    ) -> Result<Response<wire::SavedFunction>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let w = workspace_id(&input.parent)?;
        let path = path(&input.path)?;
        let function = self
            .saved_functions
            .get(&AuthenticatedPrincipal::user(user), &w, &path)
            .await
            .map_err(error)?;
        Ok(Response::new(saved_function(&function)))
    }

    #[tracing::instrument(skip_all, name = "coral.private.delete_saved_function")]
    async fn delete_saved_function(
        &self,
        request: Request<wire::DeleteSavedFunctionRequest>,
    ) -> Result<Response<()>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let w = workspace_id(&input.parent)?;
        let path = path(&input.path)?;
        let context = SavedFunctionChangeContext::new(
            AuthenticatedPrincipal::user(user),
            now().map_err(annotate)?,
            TraceId::current_or_new(),
        );
        self.saved_function_deletion
            .delete(&context, &w, &path)
            .await
            .map_err(error)?;
        Ok(Response::new(()))
    }
}
