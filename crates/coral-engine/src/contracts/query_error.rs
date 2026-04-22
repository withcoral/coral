//! Canonical structured query error contract.

use std::collections::HashMap;

use super::error::StatusCode;

/// Structured query failure with first-class semantic fields.
#[derive(Debug, Clone)]
pub struct StructuredQueryError {
    reason: String,
    summary: String,
    detail: String,
    hint: Option<String>,
    retryable: bool,
    status: StatusCode,
    metadata: HashMap<String, String>,
}

impl StructuredQueryError {
    /// Builds a structured error from its parts.
    pub(crate) fn new(
        reason: impl Into<String>,
        summary: impl Into<String>,
        detail: impl Into<String>,
        hint: Option<String>,
        retryable: bool,
        status: StatusCode,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            reason: reason.into(),
            summary: summary.into(),
            detail: detail.into(),
            hint,
            retryable,
            status,
            metadata,
        }
    }

    /// Machine-readable error reason (e.g. `"MISSING_REQUIRED_FILTER"`).
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }

    /// One-line error summary.
    #[must_use]
    pub fn summary(&self) -> &str {
        &self.summary
    }

    /// Longer explanation (may be empty).
    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }

    /// Actionable recovery guidance.
    #[must_use]
    pub fn hint(&self) -> Option<&str> {
        self.hint.as_deref()
    }

    /// Whether the error is transient.
    #[must_use]
    pub fn retryable(&self) -> bool {
        self.retryable
    }

    /// Transport-neutral status code.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Additional key-value metadata.
    #[must_use]
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }
}

impl std::fmt::Display for StructuredQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.summary)?;
        if !self.detail.is_empty() {
            write!(f, "\n{}", self.detail)?;
        }
        if let Some(hint) = &self.hint {
            write!(f, "\nHint: {hint}")?;
        }
        Ok(())
    }
}
