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

    #[must_use]
    pub(crate) fn is_default(&self) -> bool {
        self.0 == DEFAULT_WORKSPACE_ID
    }

    #[must_use]
    pub(crate) fn has_reserved_personal_default_prefix(&self) -> bool {
        self.0.starts_with("default-")
    }
}

impl fmt::Display for WorkspaceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

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

    #[test]
    fn parses_reserved_personal_default_names_for_persisted_state() {
        let name = WorkspaceName::parse(" default-user-id ").expect("workspace name");

        assert_eq!(name.as_str(), "default-user-id");
        assert!(name.has_reserved_personal_default_prefix());
        assert!(!WorkspaceName::default().has_reserved_personal_default_prefix());
    }
}
