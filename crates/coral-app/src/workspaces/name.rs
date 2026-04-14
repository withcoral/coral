use std::fmt;

use coral_api::v1::Workspace;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct WorkspaceName(String);

impl WorkspaceName {
    #[must_use]
    pub(crate) fn new(name: String) -> Self {
        Self(name)
    }

    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

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
