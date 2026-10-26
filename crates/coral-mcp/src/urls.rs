//! The canonical MCP routes shared by serving and OAuth configuration.

use coral_workspace::WorkspaceId;

/// The Workspace MCP route, parameterized by a stable Workspace identity.
pub const WORKSPACE_MCP_ROUTE: &str = "/workspaces/{workspace_id}/mcp";

/// The Organization MCP Endpoint. It names no Workspace: the credential fixes one.
pub const ORGANIZATION_MCP_ROUTE: &str = "/mcp";

/// One MCP endpoint, and so what fixes the Workspace a request there acts in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum McpEndpoint {
    /// `/workspaces/{workspace_id}/mcp`: the URL fixes the Workspace.
    Workspace(WorkspaceId),
    /// `/mcp`, the Organization MCP Endpoint: the credential fixes the Workspace.
    Organization,
}

impl McpEndpoint {
    /// Returns this endpoint's canonical path.
    #[must_use]
    pub fn path(&self) -> String {
        match self {
            Self::Workspace(workspace_id) => workspace_mcp_path(workspace_id),
            Self::Organization => ORGANIZATION_MCP_ROUTE.to_owned(),
        }
    }

    /// Parses only an exact MCP path, as [`parse_workspace_mcp_path`] does.
    #[must_use]
    pub fn parse_path(path: &str) -> Option<Self> {
        if path == ORGANIZATION_MCP_ROUTE {
            return Some(Self::Organization);
        }
        parse_workspace_mcp_path(path).map(Self::Workspace)
    }
}

/// Returns the canonical MCP path for one Workspace, independent of its name.
#[must_use]
pub fn workspace_mcp_path(workspace_id: &WorkspaceId) -> String {
    WORKSPACE_MCP_ROUTE.replace("{workspace_id}", &workspace_id.to_string())
}

/// Parses only exact MCP paths containing a canonical Workspace identity.
///
/// Names, encoded identifiers, trailing slashes, and extra segments are rejected.
/// Matching a path grants no authority to the Workspace.
#[must_use]
pub fn parse_workspace_mcp_path(path: &str) -> Option<WorkspaceId> {
    let (prefix, suffix) = WORKSPACE_MCP_ROUTE.split_once("{workspace_id}")?;
    WorkspaceId::parse(path.strip_prefix(prefix)?.strip_suffix(suffix)?).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_routes_use_only_canonical_stable_ids() {
        let id = WorkspaceId::parse("01234567-89ab-4cde-8fab-0123456789ab").expect("Workspace id");
        let path = workspace_mcp_path(&id);
        assert_eq!(path, "/workspaces/01234567-89ab-4cde-8fab-0123456789ab/mcp");
        assert_eq!(parse_workspace_mcp_path(&path), Some(id.clone()));
        for invalid in [
            "/mcp/workspace/team",
            "/workspaces/team/mcp",
            "/workspaces/01234567-89AB-4CDE-8FAB-0123456789AB/mcp",
            "/workspaces/0123456789ab4cde8fab0123456789ab/mcp",
            "/workspaces/%301234567-89ab-4cde-8fab-0123456789ab/mcp",
            "/workspaces/01234567-89ab-4cde-8fab-0123456789ab/mcp/",
            "/workspaces/01234567-89ab-4cde-8fab-0123456789ab/extra/mcp",
            "/workspaces/01234567-89ab-4cde-8fab-0123456789ab/mcp?x=1",
        ] {
            assert!(parse_workspace_mcp_path(invalid).is_none(), "{invalid}");
            assert!(McpEndpoint::parse_path(invalid).is_none(), "{invalid}");
        }
        for endpoint in [McpEndpoint::Workspace(id), McpEndpoint::Organization] {
            assert_eq!(McpEndpoint::parse_path(&endpoint.path()), Some(endpoint));
        }
        for invalid in ["/mcp/", "/MCP", "/mcp?x=1", ""] {
            assert!(McpEndpoint::parse_path(invalid).is_none(), "{invalid}");
        }
    }
}
