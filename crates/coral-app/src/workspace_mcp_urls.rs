//! OAuth resource and metadata URLs for the canonical Workspace MCP route.
//!
//! The MCP Adapter owns the path and stable-identity parsing. This helper adds
//! the configured public origin; neither display names nor URL prefixes select
//! a Workspace. A matching URL grants no authority.

use coral_mcp::{ORGANIZATION_MCP_ROUTE, parse_workspace_mcp_path, workspace_mcp_path};
use coral_workspace::WorkspaceId;

use crate::oauth_resource::{CanonicalOauthUrl, OauthUrlError};

/// The well-known prefix protected-resource metadata is served under (RFC 9728).
pub const PROTECTED_RESOURCE_METADATA_ROOT: &str = "/.well-known/oauth-protected-resource";

/// Derives and parses Workspace MCP resources under one validated public origin.
#[derive(Clone, Debug)]
pub struct WorkspaceMcpUrls {
    origin: CanonicalOauthUrl,
}

impl WorkspaceMcpUrls {
    /// Builds a URL family from an HTTPS or explicit-loopback HTTP origin.
    ///
    /// # Errors
    ///
    /// Rejects a non-root path: MCP is served only at the canonical
    /// `/workspaces/{workspace_id}/mcp` route, not under configurable prefixes.
    pub fn new(origin: CanonicalOauthUrl) -> Result<Self, OauthUrlError> {
        if origin.url().path() != "/" {
            return Err(OauthUrlError::Origin);
        }
        Ok(Self { origin })
    }

    /// Returns the canonical public origin, without a trailing slash.
    #[must_use]
    pub fn base(&self) -> &CanonicalOauthUrl {
        &self.origin
    }

    /// Whether a canonical audience names the origin rather than a Workspace.
    #[must_use]
    pub fn is_base_audience(&self, candidate: &str) -> bool {
        self.origin.identifier() == candidate
    }

    /// The exact Workspace MCP endpoint and OAuth resource/audience.
    #[must_use]
    pub fn resource(&self, workspace_id: &WorkspaceId) -> String {
        format!(
            "{}{}",
            self.origin.identifier(),
            workspace_mcp_path(workspace_id)
        )
    }

    /// The RFC 9728 protected-resource metadata URL for one Workspace.
    #[must_use]
    pub fn metadata_url(&self, workspace_id: &WorkspaceId) -> String {
        format!(
            "{}{}",
            self.origin.identifier(),
            self.metadata_path(workspace_id)
        )
    }

    /// The listener-relative protected-resource metadata path.
    #[must_use]
    pub fn metadata_path(&self, workspace_id: &WorkspaceId) -> String {
        format!(
            "{PROTECTED_RESOURCE_METADATA_ROOT}{}",
            workspace_mcp_path(workspace_id)
        )
    }

    /// Parses an exact resource under this origin into its stable Workspace ID.
    #[must_use]
    pub fn parse_resource(&self, resource: &str) -> Option<WorkspaceId> {
        parse_workspace_mcp_path(resource.strip_prefix(self.origin.identifier())?)
    }

    /// Parses an exact protected-resource metadata path.
    #[must_use]
    pub fn parse_metadata_path(&self, path: &str) -> Option<WorkspaceId> {
        parse_workspace_mcp_path(path.strip_prefix(PROTECTED_RESOURCE_METADATA_ROOT)?)
    }

    /// The Organization MCP Endpoint and its OAuth resource/audience. Its
    /// tokens also carry the Workspace the User picked at sign-in.
    #[must_use]
    pub fn organization_resource(&self) -> String {
        format!("{}{ORGANIZATION_MCP_ROUTE}", self.origin.identifier())
    }

    /// The RFC 9728 protected-resource metadata URL of the Organization MCP Endpoint.
    #[must_use]
    pub fn organization_metadata_url(&self) -> String {
        format!(
            "{}{PROTECTED_RESOURCE_METADATA_ROOT}{ORGANIZATION_MCP_ROUTE}",
            self.origin.identifier()
        )
    }

    /// Whether a canonical resource is exactly the Organization MCP Endpoint.
    #[must_use]
    pub fn is_organization_resource(&self, resource: &str) -> bool {
        resource == self.organization_resource()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "01234567-89ab-4cde-8fab-0123456789ab";

    #[test]
    fn resources_and_metadata_share_the_served_workspace_path() {
        let id = WorkspaceId::parse(ID).expect("Workspace id");
        for origin in [
            "https://CORAL.example/",
            "https://coral.example:8443",
            "http://127.0.0.1:14556/",
            "http://[::1]:14556",
        ] {
            let urls = WorkspaceMcpUrls::new(CanonicalOauthUrl::parse(origin).expect("origin"))
                .expect("Workspace URLs");
            let path = workspace_mcp_path(&id);
            let resource = urls.resource(&id);
            assert_eq!(resource, format!("{}{path}", urls.base().identifier()));
            assert_eq!(urls.parse_resource(&resource), Some(id.clone()));
            assert_eq!(parse_workspace_mcp_path(&path), Some(id.clone()));
            assert_eq!(
                urls.metadata_path(&id),
                format!("{PROTECTED_RESOURCE_METADATA_ROOT}{path}")
            );
            assert_eq!(
                urls.metadata_url(&id),
                format!("{}{}", urls.base().identifier(), urls.metadata_path(&id))
            );
            assert_eq!(
                urls.parse_metadata_path(&urls.metadata_path(&id)),
                Some(id.clone())
            );
            assert!(urls.parse_resource(urls.base().identifier()).is_none());
            assert!(urls.is_base_audience(urls.base().identifier()));
        }
    }

    #[test]
    fn legacy_bases_and_path_prefixes_are_rejected() {
        for base in [
            "https://coral.example/mcp",
            "https://coral.example/mcp/",
            "https://coral.example/custom",
        ] {
            let error = WorkspaceMcpUrls::new(CanonicalOauthUrl::parse(base).expect("URL"))
                .expect_err("MCP requires an origin");
            assert_eq!(error, OauthUrlError::Origin);
        }
    }

    #[test]
    fn resources_reject_names_aliases_and_other_origins() {
        let urls = WorkspaceMcpUrls::new(
            CanonicalOauthUrl::parse("https://coral.example").expect("origin"),
        )
        .expect("Workspace URLs");
        for resource in [
            "https://coral.example/mcp/workspace/team".to_string(),
            "https://coral.example/workspaces/team/mcp".to_string(),
            format!("https://coral.example/workspaces/{ID}/mcp/"),
            format!("https://coral.example/workspaces/{ID}/mcp?x=1"),
            format!("https://coral.example/workspaces/{ID}/mcp#fragment"),
            format!("https://coral.example/workspaces/{}/mcp", ID.to_uppercase()),
            format!(
                "https://coral.example/workspaces/{}/mcp",
                ID.replacen('0', "%30", 1)
            ),
            format!("https://other.example/workspaces/{ID}/mcp"),
            format!("http://coral.example/workspaces/{ID}/mcp"),
            format!("https://coral.example.attacker/workspaces/{ID}/mcp"),
        ] {
            assert!(urls.parse_resource(&resource).is_none(), "{resource}");
        }
        assert!(
            urls.parse_metadata_path("/.well-known/oauth-protected-resource/mcp/workspace/team")
                .is_none()
        );
    }
}
