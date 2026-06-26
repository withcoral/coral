use crate::CoreError;

/// Structured virtual graph diagnostic surfaced by validation and planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    code: &'static str,
    path: String,
    message: String,
}

impl Diagnostic {
    /// Builds one virtual graph diagnostic.
    #[must_use]
    pub fn new(code: &'static str, path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code,
            path: path.into(),
            message: message.into(),
        }
    }

    /// Returns the stable diagnostic code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        self.code
    }

    /// Returns the declaration or plan path associated with this diagnostic.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns the human-readable diagnostic message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn into_core_error(self) -> CoreError {
        CoreError::InvalidInput(format!(
            "virtual graph {} at {}: {}",
            self.code, self.path, self.message
        ))
    }
}
