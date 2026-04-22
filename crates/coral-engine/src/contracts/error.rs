//! Stable transport-neutral error contract for `coral-engine`.

use thiserror::Error;

pub use super::query_error::StructuredQueryError;

/// Errors surfaced by the query layer.
#[derive(Debug, Clone, Error)]
pub enum CoreError {
    /// Caller-supplied input was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// A requested source, schema, or table was not found.
    #[error("resource not found: {0}")]
    NotFound(String),
    /// The request could not be satisfied because required setup is missing.
    #[error("failed precondition: {0}")]
    FailedPrecondition(String),
    /// The service is temporarily unavailable.
    #[error("unavailable: {0}")]
    Unavailable(String),
    /// The requested behavior is not implemented.
    #[error("unimplemented: {0}")]
    Unimplemented(String),
    /// The service failed internally.
    #[error("internal: {0}")]
    Internal(String),
    /// A structured query failure with first-class semantic fields.
    #[error("{_0}")]
    QueryFailure(Box<StructuredQueryError>),
}

impl CoreError {
    /// Creates an internal error with a stable caller-visible message.
    #[must_use]
    pub fn internal(detail: impl Into<String>) -> Self {
        Self::Internal(detail.into())
    }

    #[must_use]
    /// Maps the error to the stable transport-neutral status code.
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidInput(_) => StatusCode::InvalidArgument,
            Self::NotFound(_) => StatusCode::NotFound,
            Self::FailedPrecondition(_) => StatusCode::FailedPrecondition,
            Self::Unavailable(_) => StatusCode::Unavailable,
            Self::Unimplemented(_) => StatusCode::Unimplemented,
            Self::Internal(_) => StatusCode::Internal,
            Self::QueryFailure(sqe) => sqe.status(),
        }
    }
}

impl From<arrow::error::ArrowError> for CoreError {
    fn from(error: arrow::error::ArrowError) -> Self {
        Self::internal(format!("arrow error: {error}"))
    }
}

impl From<serde_json::Error> for CoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::internal(format!("json error: {error}"))
    }
}

impl From<std::string::FromUtf8Error> for CoreError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        Self::internal(format!("utf8 error: {error}"))
    }
}

/// Transport-neutral status codes exposed by the query layer.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StatusCode {
    /// The caller supplied invalid input.
    InvalidArgument,
    /// The requested resource does not exist.
    NotFound,
    /// The request requires additional setup before it can succeed.
    FailedPrecondition,
    /// The service is temporarily unavailable.
    Unavailable,
    /// The requested behavior is not implemented.
    Unimplemented,
    /// The service failed internally.
    Internal,
}
