use std::fmt;

use serde::{Deserialize, Serialize};

/// App-owned identity for one installed or installable source name.
///
/// `coral-app` uses this instead of raw `String` values in its internal
/// catalog, filesystem layout, and source/query managers so the source-identity
/// seam is explicit in the type system. Stringly source names are kept at the
/// persistence and gRPC transport edges, then normalized into `SourceName`
/// before app logic runs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct SourceName(String);

impl SourceName {
    /// Wrap an already-validated source name for app-internal use.
    #[must_use]
    pub(crate) fn new(name: String) -> Self {
        Self(name)
    }

    /// Borrow the normalized source name at string boundaries such as paths,
    /// config rendering, or protobuf mapping.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SourceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
