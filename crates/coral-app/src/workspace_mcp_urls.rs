//! Per-workspace MCP resource URLs: one template, three roles.
//!
//! Every served workspace has exactly one MCP URL, derived from the configured
//! public base URL as `<base>/workspace/<name>`. That one string is the routing
//! key, the OAuth protected-resource identifier, and the access-token audience,
//! so derivation and parsing live here — a second derivation anywhere else
//! could mint an audience that no longer matches the advertised resource.
//! [`CanonicalOauthUrl`] canonicalizes the base; the workspace segment is held
//! to a charset strict enough that concatenation is itself canonical.

use url::Position;

use crate::oauth_resource::CanonicalOauthUrl;

/// The well-known prefix protected-resource metadata is served under (RFC 9728).
pub const PROTECTED_RESOURCE_METADATA_ROOT: &str = "/.well-known/oauth-protected-resource";

/// The path segment between the base URL and the workspace name.
pub const WORKSPACE_ROUTE_SEGMENT: &str = "workspace";

/// A workspace name in the strict charset per-workspace URLs admit.
///
/// The charset is the RFC 3986 unreserved set (ASCII alphanumerics plus
/// `-`, `.`, `_`, `~`), excluding `.` and `..`. It is deliberately tighter
/// than the app's workspace-name validation: a name outside it cannot appear
/// in a URL, a challenge header, or an audience without escaping, and escaping
/// would create a second spelling of the one canonical string. A workspace
/// whose name falls outside this set is simply unreachable by URL until it is
/// renamed. No percent-encoding is admitted: `%` is not in the charset, so an
/// encoded spelling of a valid name is rejected rather than normalized.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct McpWorkspaceSegment(String);

impl McpWorkspaceSegment {
    /// Parses one raw path segment, refusing anything outside the charset.
    #[must_use]
    pub fn parse(segment: &str) -> Option<Self> {
        let charset_ok = !segment.is_empty()
            && segment.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
            });
        if !charset_ok || segment == "." || segment == ".." {
            return None;
        }
        Some(Self(segment.to_string()))
    }

    /// Borrows the validated workspace name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for McpWorkspaceSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Derives and parses the per-workspace MCP URL family under one base URL.
#[derive(Clone, Debug)]
pub struct WorkspaceMcpUrls {
    base: CanonicalOauthUrl,
}

impl WorkspaceMcpUrls {
    /// Builds the URL family under a canonical base, e.g. `https://host/mcp`.
    #[must_use]
    pub fn new(base: CanonicalOauthUrl) -> Self {
        Self { base }
    }

    /// Returns the canonical base the family hangs under.
    #[must_use]
    pub fn base(&self) -> &CanonicalOauthUrl {
        &self.base
    }

    /// The listener-relative path of the base, with a root path as `""`.
    ///
    /// Routes and metadata paths are derived by appending to this, so the
    /// public URL's path decides where the listener mounts the family.
    #[must_use]
    pub fn base_path(&self) -> &str {
        match self.base.url().path() {
            "/" => "",
            path => path,
        }
    }

    /// The one MCP resource URL for a workspace: routing key, OAuth resource,
    /// and token audience, byte-identical in all three roles.
    #[must_use]
    pub fn resource(&self, segment: &McpWorkspaceSegment) -> String {
        format!(
            "{}/{WORKSPACE_ROUTE_SEGMENT}/{segment}",
            self.base.identifier()
        )
    }

    /// The listener-relative MCP route path for a workspace.
    #[must_use]
    pub fn route_path(&self, segment: &McpWorkspaceSegment) -> String {
        format!("{}/{WORKSPACE_ROUTE_SEGMENT}/{segment}", self.base_path())
    }

    /// The RFC 9728 metadata URL for a workspace's resource: the well-known
    /// suffix is inserted between the host and the resource's path.
    #[must_use]
    pub fn metadata_url(&self, segment: &McpWorkspaceSegment) -> String {
        format!(
            "{}{}",
            &self.base.url()[..Position::BeforePath],
            self.metadata_path(segment)
        )
    }

    /// The listener-relative path [`Self::metadata_url`] is served at.
    #[must_use]
    pub fn metadata_path(&self, segment: &McpWorkspaceSegment) -> String {
        format!(
            "{PROTECTED_RESOURCE_METADATA_ROOT}{}/{WORKSPACE_ROUTE_SEGMENT}/{segment}",
            self.base_path()
        )
    }

    /// Parses a canonical resource identifier back to its workspace segment.
    ///
    /// The exact inverse of [`Self::resource`]: anything that is not the base,
    /// the literal workspace segment, and one charset-valid name is refused.
    #[must_use]
    pub fn parse_resource(&self, resource: &str) -> Option<McpWorkspaceSegment> {
        Self::parse_tail(resource.strip_prefix(self.base.identifier())?)
    }

    /// Parses a listener-relative MCP route path back to its workspace segment.
    ///
    /// The exact inverse of [`Self::route_path`].
    #[must_use]
    pub fn parse_route_path(&self, path: &str) -> Option<McpWorkspaceSegment> {
        Self::parse_tail(path.strip_prefix(self.base_path())?)
    }

    /// Parses a listener-relative metadata path back to its workspace segment.
    ///
    /// The exact inverse of [`Self::metadata_path`].
    #[must_use]
    pub fn parse_metadata_path(&self, path: &str) -> Option<McpWorkspaceSegment> {
        self.parse_route_path(path.strip_prefix(PROTECTED_RESOURCE_METADATA_ROOT)?)
    }

    fn parse_tail(tail: &str) -> Option<McpWorkspaceSegment> {
        McpWorkspaceSegment::parse(
            tail.strip_prefix('/')?
                .strip_prefix(WORKSPACE_ROUTE_SEGMENT)?
                .strip_prefix('/')?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{McpWorkspaceSegment, WorkspaceMcpUrls};
    use crate::oauth_resource::CanonicalOauthUrl;

    fn urls(base: &str) -> WorkspaceMcpUrls {
        WorkspaceMcpUrls::new(CanonicalOauthUrl::parse(base).expect("canonical base"))
    }

    fn segment(name: &str) -> McpWorkspaceSegment {
        McpWorkspaceSegment::parse(name).expect("valid segment")
    }

    #[test]
    fn segments_admit_only_the_unreserved_charset() {
        for valid in ["team", "analytics-staging", "a", "Team_2", "v1.2~x", "..."] {
            assert!(McpWorkspaceSegment::parse(valid).is_some(), "{valid}");
        }
        for invalid in [
            "",
            ".",
            "..",
            "team/other",
            "te am",
            "te%61m",
            "tëam",
            "team?",
            "team#",
            "team\\",
            "team\n",
            "té",
            "🪸",
        ] {
            assert!(McpWorkspaceSegment::parse(invalid).is_none(), "{invalid:?}");
        }
    }

    #[test]
    fn derivation_and_parsing_are_exact_inverses() {
        for (base, name) in [
            ("https://coral.example/mcp", "team"),
            ("https://coral.example/mcp", "analytics-staging"),
            ("https://coral.example:8443/mcp", "team"),
            ("http://127.0.0.1:14556/mcp", "team"),
            ("https://coral.example", "team"),
        ] {
            let urls = urls(base);
            let segment = segment(name);
            let resource = urls.resource(&segment);
            assert_eq!(
                urls.parse_resource(&resource).as_ref(),
                Some(&segment),
                "resource round-trip for {base} / {name}"
            );
            let route = urls.route_path(&segment);
            assert_eq!(
                urls.parse_route_path(&route).as_ref(),
                Some(&segment),
                "route round-trip for {base} / {name}"
            );
            let metadata_path = urls.metadata_path(&segment);
            assert_eq!(
                urls.parse_metadata_path(&metadata_path).as_ref(),
                Some(&segment),
                "metadata round-trip for {base} / {name}"
            );
            assert!(
                urls.metadata_url(&segment).ends_with(&metadata_path),
                "metadata URL must end with its served path"
            );
        }
    }

    #[test]
    fn the_documented_template_shapes_hold_exactly() {
        let urls = urls("https://coral.example/mcp");
        let team = segment("team");
        assert_eq!(
            urls.resource(&team),
            "https://coral.example/mcp/workspace/team"
        );
        assert_eq!(urls.route_path(&team), "/mcp/workspace/team");
        assert_eq!(
            urls.metadata_url(&team),
            "https://coral.example/.well-known/oauth-protected-resource/mcp/workspace/team"
        );
        assert_eq!(
            urls.metadata_path(&team),
            "/.well-known/oauth-protected-resource/mcp/workspace/team"
        );
    }

    #[test]
    fn a_root_base_derives_origin_rooted_workspace_urls() {
        let urls = urls("https://coral.example/");
        let team = segment("team");
        assert_eq!(urls.base_path(), "");
        assert_eq!(urls.resource(&team), "https://coral.example/workspace/team");
        assert_eq!(urls.route_path(&team), "/workspace/team");
        assert_eq!(
            urls.metadata_path(&team),
            "/.well-known/oauth-protected-resource/workspace/team"
        );
    }

    #[test]
    fn non_canonical_spellings_parse_to_nothing() {
        let urls = urls("https://coral.example/mcp");
        for resource in [
            "https://coral.example/mcp",
            "https://coral.example/mcp/workspace",
            "https://coral.example/mcp/workspace/",
            "https://coral.example/mcp/workspace/team/",
            "https://coral.example/mcp/workspace/team/extra",
            "https://coral.example/mcp/workspace/te%61m",
            "https://coral.example/mcp/workspace/te am",
            "https://coral.example/mcp/Workspace/team",
            "https://other.example/mcp/workspace/team",
            "https://coral.example/other/workspace/team",
            "http://coral.example/mcp/workspace/team",
        ] {
            assert!(urls.parse_resource(resource).is_none(), "{resource}");
        }
        for path in [
            "/mcp",
            "/mcp/workspace",
            "/mcp/workspace/",
            "/mcp/workspace/team/",
            "/mcp/workspace/team/extra",
            "/mcp/workspace/te%61m",
            "/workspace/team",
        ] {
            assert!(urls.parse_route_path(path).is_none(), "{path}");
        }
        assert!(
            urls.parse_metadata_path("/.well-known/oauth-protected-resource/mcp")
                .is_none(),
            "the base metadata path names no workspace"
        );
    }
}
