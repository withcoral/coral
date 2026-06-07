/// SQL runtime result.
pub type SqlResult<T> = std::result::Result<T, SqlError>;

/// Errors surfaced by the SQL layer.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SqlError {
    /// Caller-supplied input was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// A requested schema, table, or binding was not found.
    #[error("resource not found: {0}")]
    NotFound(String),
    /// The request could not be satisfied because required setup is missing.
    #[error("failed precondition: {0}")]
    FailedPrecondition(String),
    /// The requested behavior is not implemented.
    #[error("unimplemented: {0}")]
    Unimplemented(String),
    /// The service failed internally.
    #[error("internal: {0}")]
    Internal(String),
}

impl SqlError {
    /// Maps the error to a stable transport-neutral status code.
    #[must_use]
    pub const fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidInput(_) => StatusCode::InvalidArgument,
            Self::NotFound(_) => StatusCode::NotFound,
            Self::FailedPrecondition(_) => StatusCode::FailedPrecondition,
            Self::Unimplemented(_) => StatusCode::Unimplemented,
            Self::Internal(_) => StatusCode::Internal,
        }
    }
}

impl From<datafusion::error::DataFusionError> for SqlError {
    fn from(error: datafusion::error::DataFusionError) -> Self {
        Self::InvalidInput(error.to_string())
    }
}

impl From<arrow::error::ArrowError> for SqlError {
    fn from(error: arrow::error::ArrowError) -> Self {
        Self::Internal(format!("arrow error: {error}"))
    }
}

/// Transport-neutral status codes exposed by the SQL layer.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StatusCode {
    /// The caller supplied invalid input.
    InvalidArgument,
    /// The requested resource does not exist.
    NotFound,
    /// The request requires additional setup before it can succeed.
    FailedPrecondition,
    /// The requested behavior is not implemented.
    Unimplemented,
    /// The service failed internally.
    Internal,
}
