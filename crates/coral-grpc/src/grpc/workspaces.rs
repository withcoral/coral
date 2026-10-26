use super::{GrpcServices, PrivateAudience, management as wire, now};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use coral_workspace::{
    AuthenticatedUser, ChangeContext, ResourceMetadata, TraceId, WorkspaceDisplayName,
    WorkspaceEvidence, WorkspaceId,
};
use prost::Message as _;
use tonic::{Request, Response, Status};

impl GrpcServices {
    pub(super) async fn management_user<T>(
        &self,
        request: &Request<T>,
    ) -> Result<AuthenticatedUser, Status> {
        self.principal(request, None, PrivateAudience::Reef)
            .await
            .map_err(annotate)?
            .authenticated_user()
            .cloned()
            .ok_or_else(|| annotate(Status::unauthenticated("User required")))
    }
    async fn workspace_view(
        &self,
        user: &AuthenticatedUser,
        id: &WorkspaceId,
    ) -> Result<wire::Workspace, Status> {
        let (evidence, meta) = self
            .workspaces
            .inspect_workspace(user, id)
            .await
            .map_err(error)?;
        let mut view = workspace(&evidence, meta);
        view.current_user = format!("users/{}", user.user_id());
        view.current_user_profile = Some(profile(
            self.workspaces
                .inspect_user_profile(user, id, user.user_id())
                .await
                .map_err(error)?,
        ));
        view.mcp_endpoint = self.mcp_origin.get().map_or_else(String::new, |origin| {
            format!("{origin}/workspaces/{id}/mcp")
        });
        // Only a deployment with sign-in can ask which Workspace `/mcp` serves.
        if self.authentication.is_some()
            && let Some(origin) = self.mcp_origin.get()
        {
            view.organization_mcp_endpoint = format!("{origin}/mcp");
        }
        Ok(view)
    }
}

pub(super) fn change(user: AuthenticatedUser) -> Result<ChangeContext, Status> {
    Ok(ChangeContext::new(
        user,
        now().map_err(annotate)?,
        TraceId::current_or_new(),
    ))
}
pub(super) fn workspace_id(name: &str) -> Result<WorkspaceId, Status> {
    let Some(id) = name.strip_prefix("workspaces/") else {
        return Err(invalid("invalid Workspace name"));
    };
    WorkspaceId::parse(id).map_err(error)
}
pub(super) fn timestamp(nanos: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: nanos.div_euclid(1_000_000_000),
        nanos: i32::try_from(nanos.rem_euclid(1_000_000_000))
            .expect("nanosecond remainder fits i32"),
    }
}
fn workspace(evidence: &WorkspaceEvidence, meta: ResourceMetadata) -> wire::Workspace {
    let w = evidence.workspace();
    wire::Workspace {
        name: format!("workspaces/{}", w.id()),
        display_name: w.display_name().as_str().into(),
        state: state(w.state()).into(),
        current_user: String::new(),
        current_user_profile: None,
        mcp_endpoint: String::new(),
        organization_mcp_endpoint: String::new(),
        etag: meta.etag,
        create_time: Some(timestamp(meta.created_at)),
        update_time: Some(timestamp(meta.updated_at)),
        effective_role: role(evidence.effective_role()).into(),
    }
}
pub(super) fn profile(profile: coral_workspace::UserProfile) -> wire::UserProfile {
    wire::UserProfile {
        user: format!("users/{}", profile.user_id),
        display_name: profile.name.unwrap_or_default(),
        email: profile.email.unwrap_or_default(),
        local: profile.local,
    }
}

pub(super) fn role(role: coral_workspace::WorkspaceRole) -> wire::WorkspaceRole {
    match role {
        coral_workspace::WorkspaceRole::Owner => wire::WorkspaceRole::Owner,
        coral_workspace::WorkspaceRole::Member => wire::WorkspaceRole::Member,
    }
}
pub(super) fn state(state: coral_workspace::WorkspaceState) -> wire::WorkspaceState {
    match state {
        coral_workspace::WorkspaceState::Active => wire::WorkspaceState::Active,
        coral_workspace::WorkspaceState::Archived => wire::WorkspaceState::Archived,
    }
}
pub(super) fn owner(owner: coral_workspace::WorkspaceOwner) -> wire::WorkspaceOwner {
    wire::WorkspaceOwner {
        user: format!("users/{}", owner.profile.user_id),
        profile: Some(profile(owner.profile)),
        enabled: owner.enabled,
    }
}
pub(super) fn mask(
    mask: Option<prost_types::FieldMask>,
    allowed: &[&str],
) -> Result<Vec<String>, Status> {
    let paths = mask.ok_or_else(|| invalid("update_mask required"))?.paths;
    if paths.is_empty() || paths.iter().any(|p| !allowed.contains(&p.as_str())) {
        return Err(invalid("unsupported update_mask"));
    }
    Ok(paths)
}

// Standard google.rpc.Status / ErrorInfo wire layout. No domain error text is exported.
#[derive(Clone, PartialEq, prost::Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}
#[derive(Clone, PartialEq, prost::Message)]
struct ErrorInfo {
    #[prost(string, tag = "1")]
    reason: String,
    #[prost(string, tag = "2")]
    domain: String,
    #[prost(map = "string, string", tag = "3")]
    metadata: std::collections::HashMap<String, String>,
}
#[expect(
    clippy::needless_pass_by_value,
    reason = "consumes domain failures in Result::map_err without exporting their text"
)]
pub(super) fn error(error: coral_workspace::Error) -> Status {
    use coral_workspace::Error as E;
    let (code, reason) = match error {
        E::InvalidInput(_) => (tonic::Code::InvalidArgument, "INVALID_ARGUMENT"),
        E::WorkspaceNotFound | E::MembershipNotFound | E::AgentNotFound => {
            (tonic::Code::NotFound, "NOT_FOUND")
        }
        E::PermissionDenied(_) => (tonic::Code::PermissionDenied, "PERMISSION_DENIED"),
        E::EtagMismatch | E::OrganizationAuthorityConflict => {
            (tonic::Code::Aborted, "ETAG_MISMATCH")
        }
        E::AmbiguousEmail => (tonic::Code::FailedPrecondition, "AMBIGUOUS_EMAIL"),
        E::RequestAlreadyApplied => (tonic::Code::FailedPrecondition, "REQUEST_ALREADY_APPLIED"),
        E::MembershipAlreadyExists => (tonic::Code::AlreadyExists, "MEMBERSHIP_ALREADY_EXISTS"),
        E::WorkspaceArchived => (tonic::Code::FailedPrecondition, "WORKSPACE_ARCHIVED"),
        E::LastWorkspaceOwner => (tonic::Code::FailedPrecondition, "LAST_WORKSPACE_OWNER"),
        E::OwnerTransferRequired => (tonic::Code::FailedPrecondition, "OWNER_TRANSFER_REQUIRED"),
        E::OwnerTargetNotEnabled => (tonic::Code::FailedPrecondition, "OWNER_TARGET_NOT_ENABLED"),
        E::WorkspaceAlreadyActive
        | E::WorkspaceAlreadyArchived
        | E::AgentDisabled
        | E::AgentRegistrationExpired => {
            (tonic::Code::FailedPrecondition, "INVALID_LIFECYCLE_STATE")
        }
        E::InvalidAgentCredential => (tonic::Code::Unauthenticated, "UNAUTHENTICATED"),
        // Retrying can succeed. Name every variant so a new one needs a wire decision.
        E::CredentialGeneration | E::CorruptData(_) | E::Database(_) | E::EventFields(_) => {
            (tonic::Code::Unavailable, "UNAVAILABLE")
        }
    };
    safe_status(code, reason)
}

pub(super) fn administration_error(error: coral_administration::Error) -> Status {
    use coral_administration::Error as E;
    match error {
        E::Workspace(error) => self::error(error),
        E::Database(_) | E::Provider(_) | E::Functions(_) => {
            safe_status(tonic::Code::Unavailable, "UNAVAILABLE")
        }
    }
}
pub(super) fn invalid(message: &'static str) -> Status {
    annotate(Status::invalid_argument(message))
}

pub(super) fn resource<'a>(
    name: &'a str,
    collection: &str,
) -> Result<(WorkspaceId, &'a str), Status> {
    super::resource(name, collection).map_err(annotate)
}

pub(super) fn annotate(status: Status) -> Status {
    if !status.details().is_empty() {
        return status;
    }
    let reason = match status.code() {
        tonic::Code::InvalidArgument => "INVALID_ARGUMENT",
        tonic::Code::Unauthenticated => "UNAUTHENTICATED",
        tonic::Code::PermissionDenied => "PERMISSION_DENIED",
        tonic::Code::NotFound => "NOT_FOUND",
        tonic::Code::Internal => "INTERNAL",
        _ => "UNAVAILABLE",
    };
    safe_status(status.code(), reason)
}

pub(super) fn safe_status(code: tonic::Code, reason: &str) -> Status {
    let detail = ErrorInfo {
        reason: reason.into(),
        domain: "coral.management".into(),
        metadata: [(
            "trace_id".into(),
            TraceId::current_or_new().as_str().to_owned(),
        )]
        .into(),
    };
    let status = RpcStatus {
        code: code as i32,
        message: reason.into(),
        details: vec![prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.ErrorInfo".into(),
            value: detail.encode_to_vec(),
        }],
    };
    Status::with_details(code, reason, status.encode_to_vec().into())
}

/// Tokens are opaque continuation positions, never authority. Every page reloads
/// current visibility; binding to User, parent and collection prevents reuse elsewhere.
/// Decodes a non-authorizing continuation before a bounded repository read.
pub(super) fn page_start(size: i32, token: &str, scope: &str) -> Result<(u32, String), Status> {
    if !(0..=100).contains(&size) || token.len() > 4096 {
        return Err(invalid("invalid page request"));
    }
    let size = if size == 0 {
        30
    } else {
        u32::try_from(size).map_err(|_error| invalid("invalid page size"))?
    };
    let after = if token.is_empty() {
        String::new()
    } else {
        let bytes = URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_error| invalid("invalid page token"))?;
        let (bound, after): (String, String) =
            serde_json::from_slice(&bytes).map_err(|_error| invalid("invalid page token"))?;
        if bound != scope {
            return Err(invalid("invalid page token"));
        }
        after
    };
    Ok((size, after))
}

pub(super) fn page<T>(
    mut values: Vec<T>,
    size: i32,
    token: &str,
    scope: &str,
    key: impl Fn(&T) -> String,
) -> Result<(Vec<T>, String), Status> {
    let (size, after) = page_start(size, token, scope)?;
    let size = usize::try_from(size).map_err(|_error| invalid("invalid page size"))?;
    values.sort_by_key(&key);
    values.retain(|v| key(v) > after);
    let more = values.len() > size;
    values.truncate(size);
    let next = match values.last().filter(|_| more) {
        Some(last) => page_token(scope, &key(last))?,
        None => String::new(),
    };
    Ok((values, next))
}

/// Encodes the continuation after `after`, bound to `scope`, for [`page_start`].
pub(super) fn page_token(scope: &str, after: &str) -> Result<String, Status> {
    Ok(URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&(scope, after))
            .map_err(|_error| annotate(Status::internal("page encoding failed")))?,
    ))
}

#[tonic::async_trait]
impl wire::workspace_service_server::WorkspaceService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.create_workspace")]
    async fn create_workspace(
        &self,
        request: Request<wire::CreateWorkspaceRequest>,
    ) -> Result<Response<wire::Workspace>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let context = change(user.clone())?
            .with_request_id(&input.request_id)
            .map_err(error)?;
        let created = self
            .workspace_workflow
            .create_workspace(
                &context,
                WorkspaceDisplayName::parse(&input.display_name).map_err(error)?,
            )
            .await
            .map_err(administration_error)?;
        Ok(Response::new(
            self.workspace_view(&user, created.id()).await?,
        ))
    }
    #[tracing::instrument(skip_all, name = "coral.private.list_workspaces")]
    async fn list_workspaces(
        &self,
        request: Request<wire::ListWorkspacesRequest>,
    ) -> Result<Response<wire::ListWorkspacesResponse>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let values = self.workspaces.list(&user).await.map_err(error)?;
        let (values, next_page_token) = page(
            values,
            input.page_size,
            &input.page_token,
            &format!("{}:workspaces", user.user_id()),
            coral_workspace::Workspace::listing_key,
        )?;
        let mut workspaces = Vec::new();
        for w in values {
            workspaces.push(self.workspace_view(&user, w.id()).await?);
        }
        Ok(Response::new(wire::ListWorkspacesResponse {
            workspaces,
            next_page_token,
        }))
    }
    #[tracing::instrument(skip_all, name = "coral.private.get_workspace")]
    async fn get_workspace(
        &self,
        request: Request<wire::GetWorkspaceRequest>,
    ) -> Result<Response<wire::Workspace>, Status> {
        let user = self.management_user(&request).await?;
        Ok(Response::new(
            self.workspace_view(&user, &workspace_id(&request.get_ref().name)?)
                .await?,
        ))
    }
    #[tracing::instrument(skip_all, name = "coral.private.update_workspace")]
    async fn update_workspace(
        &self,
        request: Request<wire::UpdateWorkspaceRequest>,
    ) -> Result<Response<wire::Workspace>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        mask(input.update_mask, &["display_name"])?;
        let w = input
            .workspace
            .ok_or_else(|| invalid("workspace required"))?;
        let id = workspace_id(&w.name)?;
        self.workspaces
            .update_display_name(
                &change(user.clone())?.with_etag(&w.etag).map_err(error)?,
                &id,
                WorkspaceDisplayName::parse(&w.display_name).map_err(error)?,
            )
            .await
            .map_err(error)?;
        Ok(Response::new(self.workspace_view(&user, &id).await?))
    }
    #[tracing::instrument(skip_all, name = "coral.private.archive_workspace")]
    async fn archive_workspace(
        &self,
        request: Request<wire::ArchiveWorkspaceRequest>,
    ) -> Result<Response<wire::Workspace>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let id = workspace_id(&input.name)?;
        self.workspaces
            .archive(
                &change(user.clone())?
                    .with_etag(&input.etag)
                    .map_err(error)?,
                &id,
            )
            .await
            .map_err(error)?;
        Ok(Response::new(self.workspace_view(&user, &id).await?))
    }
    #[tracing::instrument(skip_all, name = "coral.private.restore_workspace")]
    async fn restore_workspace(
        &self,
        request: Request<wire::RestoreWorkspaceRequest>,
    ) -> Result<Response<wire::Workspace>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let id = workspace_id(&input.name)?;
        self.workspaces
            .restore(
                &change(user.clone())?
                    .with_etag(&input.etag)
                    .map_err(error)?,
                &id,
            )
            .await
            .map_err(error)?;
        Ok(Response::new(self.workspace_view(&user, &id).await?))
    }
}

#[cfg(test)]
mod tests {
    use coral_workspace::Error as E;
    use tonic::Code;

    /// Reef names these reasons in management forms (`apps/reef/app/lib/management.server.ts`).
    #[test]
    fn workspace_errors_keep_the_reasons_reef_explains() {
        let cases = [
            (
                E::AmbiguousEmail,
                Code::FailedPrecondition,
                "AMBIGUOUS_EMAIL",
            ),
            (
                E::WorkspaceArchived,
                Code::FailedPrecondition,
                "WORKSPACE_ARCHIVED",
            ),
            (
                E::RequestAlreadyApplied,
                Code::FailedPrecondition,
                "REQUEST_ALREADY_APPLIED",
            ),
            (
                E::LastWorkspaceOwner,
                Code::FailedPrecondition,
                "LAST_WORKSPACE_OWNER",
            ),
            (
                E::OwnerTransferRequired,
                Code::FailedPrecondition,
                "OWNER_TRANSFER_REQUIRED",
            ),
            (
                E::OwnerTargetNotEnabled,
                Code::FailedPrecondition,
                "OWNER_TARGET_NOT_ENABLED",
            ),
            (E::WorkspaceNotFound, Code::NotFound, "NOT_FOUND"),
            (
                E::PermissionDenied("private detail"),
                Code::PermissionDenied,
                "PERMISSION_DENIED",
            ),
            (E::EtagMismatch, Code::Aborted, "ETAG_MISMATCH"),
            (
                E::InvalidAgentCredential,
                Code::Unauthenticated,
                "UNAUTHENTICATED",
            ),
        ];
        for (failure, code, reason) in cases {
            let label = format!("{failure:?}");
            let status = super::error(failure);
            assert_eq!((status.code(), status.message()), (code, reason), "{label}");
        }
    }
}
