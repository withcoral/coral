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
            .map_err(|error| match error {
                coral_auth::AuthenticationError::Unauthenticated => {
                    AuthenticationError::Unauthenticated
                }
                coral_auth::AuthenticationError::Unavailable => AuthenticationError::Unavailable,
            })
    }
    fn oauth_resource(&self, workspace: &WorkspaceId) -> Option<OauthProtectedResource> {
        Some(OauthProtectedResource {
            resource: self.urls.resource(workspace),
            authorization_server: self.issuer.clone(),
            metadata_url: self.urls.metadata_url(workspace),
        })
    }
}
