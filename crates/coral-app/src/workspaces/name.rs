use std::fmt;

use coral_api::v1::Workspace;

/// App-owned identity for one validated workspace name.
///
/// `coral-app` keeps workspace identity as this narrow type throughout app
/// state, managers, and layout code so those layers do not depend on the gRPC
/// `Workspace` message shape. Protobuf workspaces are normalized into
/// `WorkspaceName` at the service edge, and only converted back when preparing
/// transport responses.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct WorkspaceName(String);

impl WorkspaceName {
    /// Wrap an already-validated workspace name for app-internal use.
    #[must_use]
    pub(crate) fn new(name: String) -> Self {
        Self(name)
    }

    /// Borrow the normalized workspace name for filesystem and persistence
    /// boundaries that still operate on strings.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    /// Project the app-owned identity back into the gRPC transport type at the
    /// service boundary.
    #[must_use]
    pub(crate) fn to_proto(&self) -> Workspace {
        Workspace {
            name: self.0.clone(),
        }
    }
}

impl fmt::Display for WorkspaceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
