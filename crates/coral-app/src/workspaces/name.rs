use std::fmt;

pub use coral_api::DEFAULT_WORKSPACE_ID;

use crate::bootstrap::AppError;
use crate::identity::parse_path_segment;

/// App-owned identity for one validated workspace name.
///
/// `coral-app` keeps workspace identity as this narrow type throughout app
/// state, managers, and layout code so those layers do not depend on transport
/// message shapes. Strings are normalized into `WorkspaceName` at persistence
/// and service edges before app logic runs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct WorkspaceName(String);

impl WorkspaceName {
    /// Parse and validate a workspace name for app-internal use.
    pub(crate) fn parse(name: &str) -> Result<Self, AppError> {
        parse_path_segment("workspace", name).map(Self)
    }

    /// Borrow the normalized workspace name for filesystem and persistence
    /// boundaries that still operate on strings.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkspaceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Names the legacy `default` workspace, which is now an ordinary name.
///
/// Test-only, and gated so the compiler enforces it: nothing provisions,
/// protects, or resolves this name any more, so no production path may reach
/// for it to stand in for "the caller's workspace" — a caller's workspace comes
/// from their memberships. It exists so fixtures can still spell the one
/// workspace older installs were given.
#[cfg(test)]
impl Default for WorkspaceName {
    fn default() -> Self {
        Self(DEFAULT_WORKSPACE_ID.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_WORKSPACE_ID, WorkspaceName};

    #[test]
    fn parses_default_workspace_name() {
        assert_eq!(WorkspaceName::default().as_str(), DEFAULT_WORKSPACE_ID);
    }

    /// `default` carries no reserved status, so it round-trips through the same
    /// parser every other name does and compares equal to nothing else.
    #[test]
    fn the_legacy_default_name_is_an_ordinary_parsed_name() {
        assert_eq!(
            WorkspaceName::parse(DEFAULT_WORKSPACE_ID).expect("parse legacy default name"),
            WorkspaceName::default()
        );
        for ordinary in ["default-team", "work"] {
            assert_ne!(
                WorkspaceName::parse(ordinary).expect("parse ordinary name"),
                WorkspaceName::default()
            );
        }
    }
}
