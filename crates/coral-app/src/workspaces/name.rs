use std::fmt;

use crate::bootstrap::AppError;

/// Canonical default workspace name used across local Coral surfaces.
pub const DEFAULT_WORKSPACE_ID: &str = "default";

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
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err(AppError::InvalidInput("missing workspace name".to_string()));
        }
        if trimmed.contains('/') || trimmed.contains('\\') {
            return Err(AppError::InvalidInput(
                "workspace name must not contain '/' or '\\\\'".to_string(),
            ));
        }
        if trimmed == "." || trimmed == ".." {
            return Err(AppError::InvalidInput(
                "workspace name must not be '.' or '..'".to_string(),
            ));
        }
        Ok(Self(trimmed.to_string()))
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
    fn rejects_forward_and_backward_slashes() {
        let error = WorkspaceName::parse(r"bad\workspace").expect_err("workspace should fail");
        assert!(error.to_string().contains("'/' or '\\\\'"));

        let error = WorkspaceName::parse("bad/workspace").expect_err("workspace should fail");
        assert!(error.to_string().contains("'/' or '\\\\'"));
    }

    #[test]
    fn rejects_path_traversal() {
        let error = WorkspaceName::parse("..").expect_err("'..' should be rejected");
        assert!(error.to_string().contains("'.' or '..'"));

        let error = WorkspaceName::parse(" . ").expect_err("'.' should be rejected");
        assert!(error.to_string().contains("'.' or '..'"));
    }
}
