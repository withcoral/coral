//! The canonical MCP routes shared by serving and OAuth configuration.

use coral_workspace::WorkspaceId;

/// The sole MCP route, parameterized by a stable Workspace identity.
pub const WORKSPACE_MCP_ROUTE: &str = "/workspaces/{workspace_id}/mcp";

/// The Organization MCP Endpoint. It names no Workspace: the credential fixes one.
pub const ORGANIZATION_MCP_ROUTE: &str = "/mcp";

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
        assert_eq!(parse_workspace_mcp_path(&path), Some(id));
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
        }
    }
}
