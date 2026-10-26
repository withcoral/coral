//! MCP projection of transport-neutral Principal authentication.
use crate::WorkspaceMcpUrls;
use coral_auth::{PrincipalAuthentication, WorkspaceFixedBy};
use coral_mcp::{AuthenticationError, McpEndpoint, OauthProtectedResource, WorkspaceAuthenticator};
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
        endpoint: &McpEndpoint,
        bearer: Option<&str>,
    ) -> Result<(AuthenticatedPrincipal, WorkspaceId), AuthenticationError> {
        let fixed_by = match endpoint {
            McpEndpoint::Workspace(workspace) => WorkspaceFixedBy::Url(workspace),
            McpEndpoint::Organization => WorkspaceFixedBy::Credential,
        };
        self.authentication
            .authenticate(fixed_by, bearer, &self.urls.endpoint_resource(endpoint))
            .await
            .map_err(|error| match error {
                coral_auth::AuthenticationError::Unauthenticated => {
                    AuthenticationError::Unauthenticated
                }
                coral_auth::AuthenticationError::Unavailable => AuthenticationError::Unavailable,
            })
    }
    fn oauth_resource(&self, endpoint: &McpEndpoint) -> Option<OauthProtectedResource> {
        Some(OauthProtectedResource {
            resource: self.urls.endpoint_resource(endpoint),
            authorization_server: self.issuer.clone(),
            metadata_url: self.urls.endpoint_metadata_url(endpoint),
        })
    }
}
