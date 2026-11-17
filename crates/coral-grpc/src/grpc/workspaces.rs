use super::status::{self, invalid, safe_status};
use super::{GrpcServices, PrivateAudience, management as wire};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use coral_db::{PageKey, PageRequest, PageSize, UnixNanos};
use coral_workspace::{
    AuthenticatedUser, ChangeContext, ResourceMetadata, TraceId, WorkspaceDisplayName,
    WorkspaceEvidence, WorkspaceId,
};
use tonic::{Request, Response, Status};

/// The User a management request authenticated as, with the request's time:
/// the instant its authentication read the server's clock.
///
/// That one reading dates everything the handler decides before it waits on
/// outside work: [`change`] dates the change by it, and a handler that needs
/// the time again takes [`Self::now`]. It stands for its User wherever a
/// domain call borrows one.
#[derive(Clone)]
pub(super) struct ManagementUser {
    user: AuthenticatedUser,
    now: UnixNanos,
}

impl ManagementUser {
    /// The request's time.
    pub(super) const fn now(&self) -> UnixNanos {
        self.now
    }

    /// The User alone, for a domain call that takes one by value.
    pub(super) fn into_user(self) -> AuthenticatedUser {
        self.user
    }
}

impl std::ops::Deref for ManagementUser {
    type Target = AuthenticatedUser;

    fn deref(&self) -> &AuthenticatedUser {
        &self.user
    }
}

impl GrpcServices {
    pub(super) async fn management_user<T>(
        &self,
        request: &Request<T>,
    ) -> Result<ManagementUser, Status> {
        let (principal, now) = self
            .principal_at(request, None, PrivateAudience::Reef)
            .await?;
        let user = principal
            .authenticated_user()
            .cloned()
            .ok_or_else(status::unauthenticated)?;
        Ok(ManagementUser { user, now })
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
        let own = self
            .workspaces
            .inspect_user_profile(user, id, user.user_id())
            .await
            .map_err(error)?;
        Ok(self.workspace_projection(user, &evidence, meta, profile(own)))
    }

    /// Projects one Workspace as `user` sees it, with `own`, that User's profile.
    fn workspace_projection(
        &self,
        user: &AuthenticatedUser,
        evidence: &WorkspaceEvidence,
        meta: ResourceMetadata,
        own: wire::UserProfile,
    ) -> wire::Workspace {
        let id = evidence.workspace().id();
        let mut view = workspace(evidence, meta);
        view.current_user = format!("users/{}", user.user_id());
        view.current_user_profile = Some(own);
        view.mcp_endpoint = self.mcp_origin.get().map_or_else(String::new, |origin| {
            format!("{origin}/workspaces/{id}/mcp")
        });
        // Only a deployment with sign-in can ask which Workspace `/mcp` serves.
        if self.authentication.signs_in()
            && let Some(origin) = self.mcp_origin.get()
        {
            view.organization_mcp_endpoint = format!("{origin}/mcp");
        }
        view
    }
}

/// The context of the change `user`'s request makes, dated by the request's time.
pub(super) fn change(user: ManagementUser) -> ChangeContext {
    ChangeContext::new(user.user, user.now, TraceId::current_or_new())
}
pub(super) fn workspace_id(name: &str) -> Result<WorkspaceId, Status> {
    let Some(id) = name.strip_prefix("workspaces/") else {
        return Err(invalid());
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
        create_time: Some(timestamp(meta.created_at.get())),
        update_time: Some(timestamp(meta.updated_at.get())),
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
    let paths = mask.ok_or_else(invalid)?.paths;
    if paths.is_empty() || paths.iter().any(|p| !allowed.contains(&p.as_str())) {
        return Err(invalid());
    }
    Ok(paths)
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "consumes domain failures in Result::map_err without exporting their text"
)]
pub(super) fn error(error: coral_workspace::Error) -> Status {
    use coral_workspace::Error as E;
    let (code, reason) = match error {
        E::InvalidInput(_) => (tonic::Code::InvalidArgument, "INVALID_ARGUMENT"),
        E::WorkspaceNotFound
        | E::MembershipNotFound
        | E::AgentNotFound
        | E::OrganizationMembershipNotFound => (tonic::Code::NotFound, "NOT_FOUND"),
        E::PermissionDenied(_) => (tonic::Code::PermissionDenied, "PERMISSION_DENIED"),
        E::EtagMismatch | E::OrganizationAuthorityConflict => {
            (tonic::Code::Aborted, "ETAG_MISMATCH")
        }
        E::AmbiguousEmail => (tonic::Code::FailedPrecondition, "AMBIGUOUS_EMAIL"),
        E::RequestAlreadyApplied => (tonic::Code::FailedPrecondition, "REQUEST_ALREADY_APPLIED"),
        E::MembershipAlreadyExists => (tonic::Code::AlreadyExists, "MEMBERSHIP_ALREADY_EXISTS"),
        E::WorkspaceArchived => (tonic::Code::FailedPrecondition, "WORKSPACE_ARCHIVED"),
        E::LastWorkspaceOwner => (tonic::Code::FailedPrecondition, "LAST_WORKSPACE_OWNER"),
        E::LastOrganizationOwner => (tonic::Code::FailedPrecondition, "LAST_ORGANIZATION_OWNER"),
        E::MemberTargetNotEnabled => (tonic::Code::FailedPrecondition, "MEMBER_TARGET_NOT_ENABLED"),
        // A defect in Coral, which no retry of the request corrects.
        E::AccessRemovalMismatch => (tonic::Code::Internal, "INTERNAL"),
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

/// The status of one coral-administration failure. An error that wraps
/// another module's reports what that error reports alone.
pub(super) fn administration_error(error: coral_administration::Error) -> Status {
    use coral_administration::Error as E;
    match error {
        E::Workspace(error) => self::error(error),
        E::Provider(error) => super::providers::error(error),
        E::Tools(error) => super::authored_tools::error(error),
        E::Policy(error) => super::policies::error(error),
        E::UndefinedSecretStore => {
            safe_status(tonic::Code::FailedPrecondition, "SECRET_STORE_NOT_DEFINED")
        }
        // An auth error reaches the adapter only here, wrapped, when access
        // removal fails to retire a Refresh Grant. The adapter has no mapper
        // of its own for it.
        E::Database(_) | E::Auth(_) => status::unavailable(),
    }
}
/// The size of the page a list request asks for: zero selects the default of
/// 30, and a size over `limit`, or a continuation too long to be one, is
/// refused.
fn page_size(limit: i32, size: i32, continuation: &str) -> Result<u32, Status> {
    if !(0..=limit).contains(&size) || continuation.len() > 4096 {
        return Err(invalid());
    }
    if size == 0 {
        Ok(30)
    } else {
        u32::try_from(size).map_err(|_error| invalid())
    }
}

/// The page a request asks for, of a list that its owner pages by the key `K`
/// (ADR 0037): at most `size` items after the key that `after` spells, or
/// from the start when it is empty. The cursor binds nothing, so the owner
/// scopes and authorizes every page from the request alone.
pub(super) fn page_request<K: PageKey>(size: i32, after: &str) -> Result<PageRequest<K>, Status> {
    let size = PageSize::new(page_size(100, size, after)?).ok_or_else(invalid)?;
    let after = if after.is_empty() {
        None
    } else {
        Some(K::from_cursor(after).ok_or_else(invalid)?)
    };
    Ok(PageRequest { after, size })
}

/// The cursor of the page after one whose owner returned `next_after`: the
/// key as text, or empty on the last page.
pub(super) fn next_after<K: PageKey>(next_after: Option<&K>) -> String {
    next_after.map_or_default(PageKey::to_cursor)
}

/// Tokens are opaque continuation positions, never authority. Every page reloads
/// current visibility; binding to User, parent and collection prevents reuse elsewhere.
/// Decodes a non-authorizing continuation before a bounded repository read.
pub(super) fn page_start(size: i32, token: &str, scope: &str) -> Result<(u32, String), Status> {
    page_start_within(100, size, token, scope)
}

/// [`page_start`] for a collection whose pages may hold up to `limit` items.
pub(super) fn page_start_within(
    limit: i32,
    size: i32,
    token: &str,
    scope: &str,
) -> Result<(u32, String), Status> {
    let size = page_size(limit, size, token)?;
    let after = if token.is_empty() {
        String::new()
    } else {
        let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_error| invalid())?;
        let (bound, after): (String, String) =
            serde_json::from_slice(&bytes).map_err(|_error| invalid())?;
        if bound != scope {
            return Err(invalid());
        }
        after
    };
    Ok((size, after))
}

pub(super) fn page<T>(
    values: Vec<T>,
    size: i32,
    token: &str,
    scope: &str,
    key: impl Fn(&T) -> String,
) -> Result<(Vec<T>, String), Status> {
    let (size, after) = page_start(size, token, scope)?;
    page_of(values, size, &after, scope, key)
}

/// Cuts `values` to the page after `after`, whose size [`page_start_within`] decoded.
pub(super) fn page_of<T>(
    mut values: Vec<T>,
    size: u32,
    after: &str,
    scope: &str,
    key: impl Fn(&T) -> String,
) -> Result<(Vec<T>, String), Status> {
    let size = usize::try_from(size).map_err(|_error| invalid())?;
    values.sort_by_key(&key);
    values.retain(|v| key(v).as_str() > after);
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
    Ok(URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&(scope, after)).map_err(|_error| status::internal())?))
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
        let context = change(user.clone())
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
        let listed = self
            .workspaces
            .inspect_workspaces(&user)
            .await
            .map_err(error)?;
        let own = profile(listed.profile);
        let (values, next_page_token) = page(
            listed.workspaces,
            input.page_size,
            &input.page_token,
            &format!("{}:workspaces", user.user_id()),
            |(evidence, _)| evidence.workspace().listing_key(),
        )?;
        let workspaces = values
            .into_iter()
            .map(|(evidence, meta)| self.workspace_projection(&user, &evidence, meta, own.clone()))
            .collect();
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
        let w = input.workspace.ok_or_else(invalid)?;
        let id = workspace_id(&w.name)?;
        self.workspaces
            .update_display_name(
                &change(user.clone()).with_etag(&w.etag).map_err(error)?,
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
                &change(user.clone()).with_etag(&input.etag).map_err(error)?,
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
                &change(user.clone()).with_etag(&input.etag).map_err(error)?,
                &id,
            )
            .await
            .map_err(error)?;
        Ok(Response::new(self.workspace_view(&user, &id).await?))
    }
}

#[cfg(test)]
mod tests {
    use coral_db::PageKey;
    use coral_tools::{PrivateToolPath, SharedToolLifecycleEventKey};
    use coral_workspace::Error as E;
    use tonic::Code;

    // Catches: a page size out of bounds or text that is no key of the list
    // being served, a default other than 30, or a cursor that is not the
    // key's own text.
    #[test]
    fn a_page_request_reads_its_size_and_the_key_its_cursor_spells() {
        let path = |size, after: &str| {
            super::page_request::<PrivateToolPath>(size, after)
                .map(|page| (page.size.get(), page.after.as_ref().map(PageKey::to_cursor)))
                .map_err(|status| (status.code(), status.message().to_owned()))
        };
        let invalid = || Err((Code::InvalidArgument, "INVALID_ARGUMENT".to_owned()));
        for (size, after, expected) in [
            (0, "", Ok((30, None))),
            (1, "", Ok((1, None))),
            (100, "", Ok((100, None))),
            (
                2,
                "reports.total",
                Ok((2, Some("reports.total".to_owned()))),
            ),
            (-1, "", invalid()),
            (101, "", invalid()),
            (2, "not a path", invalid()),
            (2, "30/00000000-0000-4000-8000-000000000000", invalid()),
        ] {
            assert_eq!(path(size, after), expected, "{size} after {after:?}");
        }
        // Each list reads its own key: an event position is no path, and a
        // path is no event position.
        let position = "30/00000000-0000-4000-8000-000000000000";
        let event = super::page_request::<SharedToolLifecycleEventKey>(2, position)
            .expect("an event position");
        assert_eq!(super::next_after(event.after.as_ref()), position);
        let no_position =
            super::page_request::<SharedToolLifecycleEventKey>(2, "reports.total").unwrap_err();
        assert_eq!(no_position.code(), Code::InvalidArgument);
        assert_eq!(super::next_after::<PrivateToolPath>(None), "");
    }

    /// Catches: a refusal that reads as retryable because an administration
    /// error wraps the error that refuses.
    #[test]
    fn an_administration_error_reports_what_it_wraps() {
        use coral_administration::Error as Administration;
        let reported = |status: tonic::Status| (status.code(), status.message().to_owned());
        let cases = [
            (
                Administration::Workspace(E::LastWorkspaceOwner),
                super::error(E::LastWorkspaceOwner),
            ),
            (
                Administration::Provider(coral_provider::Error::ProviderNotFound),
                crate::grpc::providers::error(coral_provider::Error::ProviderNotFound),
            ),
            (
                Administration::Tools(coral_tools::Error::PrivateToolExists),
                crate::grpc::authored_tools::error(coral_tools::Error::PrivateToolExists),
            ),
            (
                Administration::Policy(coral_policy::Error::ToolNotFound),
                crate::grpc::policies::error(coral_policy::Error::ToolNotFound),
            ),
        ];
        for (wrapped, alone) in cases {
            let label = format!("{wrapped:?}");
            let status = super::administration_error(wrapped);
            assert_ne!(status.code(), Code::Unavailable, "{label}");
            assert_eq!(reported(status), reported(alone), "{label}");
        }
        assert_eq!(
            super::administration_error(Administration::Database(coral_db::Error::WriterBusy))
                .code(),
            Code::Unavailable
        );
    }

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
                E::OrganizationMembershipNotFound,
                Code::NotFound,
                "NOT_FOUND",
            ),
            (
                E::LastOrganizationOwner,
                Code::FailedPrecondition,
                "LAST_ORGANIZATION_OWNER",
            ),
            (
                E::MemberTargetNotEnabled,
                Code::FailedPrecondition,
                "MEMBER_TARGET_NOT_ENABLED",
            ),
            (E::AccessRemovalMismatch, Code::Internal, "INTERNAL"),
            (
                E::PermissionDenied(coral_workspace::Denial::WorkspaceOwnerRequired),
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
