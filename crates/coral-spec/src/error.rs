//! Source-spec-owned error types.

/// Result type used throughout `coral-spec`.
pub type Result<T> = std::result::Result<T, ManifestError>;

/// Errors surfaced while parsing and validating source specs.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// The source-spec YAML could not be parsed.
    #[error("failed to parse manifest yaml: {source}")]
    ParseYaml {
        /// The underlying YAML parse error.
        #[source]
        source: serde_yaml::Error,
    },
    /// A structured source-spec value could not be deserialized.
    #[error("failed to deserialize manifest: {source}")]
    Deserialize {
        /// The underlying structured-data deserialization error.
        #[source]
        source: serde_json::Error,
    },
    /// The source spec violates a semantic validation rule.
    #[error("{0}")]
    Validation(String),
}

impl ManifestError {
    pub(crate) fn parse_yaml(source: serde_yaml::Error) -> Self {
        Self::ParseYaml { source }
    }

    pub(crate) fn deserialize(source: serde_json::Error) -> Self {
        Self::Deserialize { source }
    }

    pub(crate) fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }
}
