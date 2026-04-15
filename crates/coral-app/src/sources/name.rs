use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::bootstrap::AppError;

/// App-owned identity for one installed or installable source name.
///
/// `coral-app` uses this instead of raw `String` values in its internal
/// catalog, filesystem layout, and source/query managers so the source-identity
/// seam is explicit in the type system. Strings are normalized into
/// `SourceName` at persistence, manifest, and transport edges before app logic
/// runs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SourceName(String);

impl SourceName {
    /// Parse and validate a source name for app-internal use.
    pub(crate) fn parse(name: &str) -> Result<Self, AppError> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err(AppError::InvalidInput("missing source name".to_string()));
        }
        if trimmed.contains('/') || trimmed.contains('\\') {
            return Err(AppError::InvalidInput(
                "source name must not contain '/' or '\\\\'".to_string(),
            ));
        }
        if trimmed == "." || trimmed == ".." {
            return Err(AppError::InvalidInput(
                "source name must not be '.' or '..'".to_string(),
            ));
        }
        Ok(Self(trimmed.to_string()))
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

impl Serialize for SourceName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SourceName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

impl FromStr for SourceName {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::SourceName;

    #[test]
    fn rejects_forward_and_backward_slashes() {
        let error = SourceName::parse(r"bad\source").expect_err("source should fail");
        assert!(error.to_string().contains("'/' or '\\\\'"));

        let error = SourceName::parse("bad/source").expect_err("source should fail");
        assert!(error.to_string().contains("'/' or '\\\\'"));
    }

    #[test]
    fn rejects_path_traversal() {
        let error = SourceName::parse("..").expect_err("'..' should be rejected");
        assert!(error.to_string().contains("'.' or '..'"));

        let error = SourceName::parse(" . ").expect_err("'.' should be rejected");
        assert!(error.to_string().contains("'.' or '..'"));
    }
}
