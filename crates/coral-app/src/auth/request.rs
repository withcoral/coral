//! MCP projection of transport-neutral Principal authentication.
use crate::WorkspaceMcpUrls;
use coral_auth::PrincipalAuthentication;
use coral_mcp::{AuthenticationError, OauthProtectedResource, WorkspaceAuthenticator};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};

/// Authentication Adapter for the exact configured Workspace MCP URL family.
#[derive(Clone)]
pub struct WorkspaceAuthentication {
    authentication: PrincipalAuthentication,
    urls: WorkspaceMcpUrls,
    issuer: String,
}
impl WorkspaceAuthentication {
    pub(crate) const fn new(
        authentication: PrincipalAuthentication,
        urls: WorkspaceMcpUrls,
        issuer: String,
    ) -> Self {
        Self {
            authentication,
            urls,
            issuer,
        }
    }
}
#[async_trait::async_trait]
impl WorkspaceAuthenticator for WorkspaceAuthentication {
    async fn authenticate(
        &self,
        workspace: &WorkspaceId,
        bearer: Option<&str>,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
        self.authentication
            .authenticate(workspace, bearer, &self.urls.resource(workspace))
            .await
            .map_err(into_mcp_error)
    }
    fn oauth_resource(&self, workspace: &WorkspaceId) -> Option<OauthProtectedResource> {
        Some(OauthProtectedResource {
            resource: self.urls.resource(workspace),
            authorization_server: self.issuer.clone(),
            metadata_url: self.urls.metadata_url(workspace),
        })
    }
    async fn authenticate_credential(
        &self,
        bearer: Option<&str>,
    ) -> Result<(AuthenticatedPrincipal, WorkspaceId), AuthenticationError> {
        self.authentication
            .authenticate_credential(bearer, &self.urls.organization_resource())
            .await
            .map(coral_auth::CredentialWorkspace::into_parts)
            .map_err(into_mcp_error)
    }
    fn organization_oauth_resource(&self) -> Option<OauthProtectedResource> {
        Some(OauthProtectedResource {
            resource: self.urls.organization_resource(),
            authorization_server: self.issuer.clone(),
            metadata_url: self.urls.organization_metadata_url(),
        })
    }
}

const fn into_mcp_error(error: coral_auth::AuthenticationError) -> AuthenticationError {
    match error {
        coral_auth::AuthenticationError::Unauthenticated => AuthenticationError::Unauthenticated,
        coral_auth::AuthenticationError::Unavailable => AuthenticationError::Unavailable,
    }
}
