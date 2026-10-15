use super::workspaces::{error as workspace_error, invalid, safe_status, timestamp, workspace_id};
use super::{GrpcServices, management as wire, now, resource};
use coral_middleware::{
    Middleware, MiddlewareChangeContext, MiddlewareDiagnostic, MiddlewareId, MiddlewareScopes,
    MoveDirection,
};
use coral_workspace::{AuthenticatedPrincipal, TraceId, WorkspaceId};
use tonic::{Request, Response, Status};

fn error(error: coral_middleware::Error) -> Status {
    use coral_middleware::Error as E;
    let (code, reason) = match error {
        E::Workspace(error) => return workspace_error(error),
        E::InvalidInput(_) => (tonic::Code::InvalidArgument, "INVALID_ARGUMENT"),
        E::MiddlewareNotFound => (tonic::Code::NotFound, "NOT_FOUND"),
        E::PermissionDenied(_) => (tonic::Code::PermissionDenied, "PERMISSION_DENIED"),
        E::LimitExceeded(_) => (tonic::Code::ResourceExhausted, "LIMIT_EXCEEDED"),
        // Management never authors. Name every variant so a new one needs a wire decision.
        E::Authoring(_)
        | E::CompilationTimedOut
        | E::CompilerUnavailable(_)
        | E::CompilerWorkerStopped
        | E::Clock(_)
        | E::Json(_)
        | E::CorruptData(_)
        | E::Database(_) => (tonic::Code::Unavailable, "UNAVAILABLE"),
    };
    safe_status(code, reason)
}

fn name(value: &str) -> Result<(WorkspaceId, MiddlewareId), Status> {
    let (workspace, id) = resource(value, "middleware")?;
    let id = MiddlewareId::parse(id).map_err(|_error| invalid("invalid Middleware name"))?;
    Ok((workspace, id))
}

fn diagnostic(diagnostic: &MiddlewareDiagnostic) -> wire::MiddlewareDiagnostic {
    let category = match diagnostic.category {
        coral_middleware::MiddlewareDiagnosticCategory::Message => {
            wire::MiddlewareDiagnosticCategory::Message
        }
        coral_middleware::MiddlewareDiagnosticCategory::Warning => {
            wire::MiddlewareDiagnosticCategory::Warning
        }
        coral_middleware::MiddlewareDiagnosticCategory::Error => {
            wire::MiddlewareDiagnosticCategory::Error
        }
        coral_middleware::MiddlewareDiagnosticCategory::Suggestion => {
            wire::MiddlewareDiagnosticCategory::Suggestion
        }
    };
    wire::MiddlewareDiagnostic {
        category: category.into(),
        code: diagnostic.code,
        message: diagnostic.message.clone(),
        line: diagnostic.line,
        column: diagnostic.column,
    }
}

fn view(middleware: &Middleware, detail: bool) -> wire::Middleware {
    let scopes = middleware.scopes();
    wire::Middleware {
        name: format!(
            "workspaces/{}/middleware/{}",
            middleware.workspace_id(),
            middleware.id()
        ),
        display_name: middleware.name().as_str().to_owned(),
        description: middleware.description().as_str().to_owned(),
        source: if detail {
            middleware.source().as_str().to_owned()
        } else {
            String::new()
        },
        enabled: middleware.enabled(),
        scopes: Some(wire::MiddlewareScopes {
            run: scopes.run,
            function: scopes.function,
            operation: scopes.operation,
        }),
        position: middleware.position(),
        revision: middleware.revision(),
        diagnostics: if detail {
            middleware.diagnostics().iter().map(diagnostic).collect()
        } else {
            Vec::new()
        },
        update_time: Some(timestamp(middleware.updated_at_unix_nanos())),
    }
}

impl GrpcServices {
    async fn middleware_change<T>(
        &self,
        request: &Request<T>,
    ) -> Result<MiddlewareChangeContext, Status> {
        let user = self.management_user(request).await?;
        Ok(MiddlewareChangeContext::new(
            AuthenticatedPrincipal::user(user),
            now()?,
            TraceId::current_or_new(),
        ))
    }
}

#[tonic::async_trait]
impl wire::middleware_service_server::MiddlewareService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.list_middleware")]
    async fn list_middleware(
        &self,
        request: Request<wire::ListMiddlewareRequest>,
    ) -> Result<Response<wire::ListMiddlewareResponse>, Status> {
        let user = self.management_user(&request).await?;
        let w = workspace_id(&request.get_ref().parent)?;
        let chain = self.middleware.list(&user, &w).await.map_err(error)?;
        Ok(Response::new(wire::ListMiddlewareResponse {
            middleware: chain.iter().map(|item| view(item, false)).collect(),
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.get_middleware")]
    async fn get_middleware(
        &self,
        request: Request<wire::GetMiddlewareRequest>,
    ) -> Result<Response<wire::Middleware>, Status> {
        let user = self.management_user(&request).await?;
        let (w, id) = name(&request.get_ref().name)?;
        let middleware = self.middleware.get(&user, &w, &id).await.map_err(error)?;
        Ok(Response::new(view(&middleware, true)))
    }

    #[tracing::instrument(skip_all, name = "coral.private.set_middleware_enabled")]
    async fn set_middleware_enabled(
        &self,
        request: Request<wire::SetMiddlewareEnabledRequest>,
    ) -> Result<Response<wire::Middleware>, Status> {
        let context = self.middleware_change(&request).await?;
        let input = request.into_inner();
        let (w, id) = name(&input.name)?;
        let middleware = self
            .middleware
            .set_enabled(&context, &w, &id, input.enabled)
            .await
            .map_err(error)?;
        Ok(Response::new(view(&middleware, false)))
    }

    #[tracing::instrument(skip_all, name = "coral.private.update_middleware_scopes")]
    async fn update_middleware_scopes(
        &self,
        request: Request<wire::UpdateMiddlewareScopesRequest>,
    ) -> Result<Response<wire::Middleware>, Status> {
        let context = self.middleware_change(&request).await?;
        let input = request.into_inner();
        let (w, id) = name(&input.name)?;
        let scopes = input
            .scopes
            .ok_or_else(|| invalid("Middleware scopes are required"))?;
        let middleware = self
            .middleware
            .set_scopes(
                &context,
                &w,
                &id,
                MiddlewareScopes {
                    run: scopes.run,
                    function: scopes.function,
                    operation: scopes.operation,
                },
            )
            .await
            .map_err(error)?;
        Ok(Response::new(view(&middleware, false)))
    }

    #[tracing::instrument(skip_all, name = "coral.private.move_middleware")]
    async fn move_middleware(
        &self,
        request: Request<wire::MoveMiddlewareRequest>,
    ) -> Result<Response<wire::Middleware>, Status> {
        let context = self.middleware_change(&request).await?;
        let input = request.into_inner();
        let (w, id) = name(&input.name)?;
        let direction = match wire::MiddlewareMoveDirection::try_from(input.direction) {
            Ok(wire::MiddlewareMoveDirection::Up) => MoveDirection::Up,
            Ok(wire::MiddlewareMoveDirection::Down) => MoveDirection::Down,
            _ => return Err(invalid("Middleware move direction is required")),
        };
        let middleware = self
            .middleware
            .move_middleware(&context, &w, &id, direction)
            .await
            .map_err(error)?;
        Ok(Response::new(view(&middleware, false)))
    }

    #[tracing::instrument(skip_all, name = "coral.private.delete_middleware")]
    async fn delete_middleware(
        &self,
        request: Request<wire::DeleteMiddlewareRequest>,
    ) -> Result<Response<wire::DeleteMiddlewareResponse>, Status> {
        let context = self.middleware_change(&request).await?;
        let input = request.into_inner();
        let (w, id) = name(&input.name)?;
        self.middleware
            .delete(&context, &w, &id)
            .await
            .map_err(error)?;
        Ok(Response::new(wire::DeleteMiddlewareResponse {
            name: input.name,
        }))
    }
}
