/// Middleware authoring, management, and persistence failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Caller-supplied domain input was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The Middleware is absent or concealed from this caller.
    #[error("Middleware was not found")]
    MiddlewareNotFound,
    /// Source broke a Middleware authoring rule, so the save was not accepted.
    #[error("Middleware authoring failed: {0}")]
    Authoring(String),
    /// Validation exceeded the fixed authoring deadline.
    #[error("Middleware compilation exceeded the two-second deadline")]
    CompilationTimedOut,
    /// A fixed Workspace bound was reached.
    #[error("Middleware limit exceeded: {0}")]
    LimitExceeded(String),
    /// Only a Workspace Owner can do this.
    #[error("permission denied: {0}")]
    PermissionDenied(&'static str),
    /// The compiler-only isolate could not initialize.
    #[error("Middleware compiler is unavailable: {0}")]
    CompilerUnavailable(String),
    /// A compiler worker stopped before returning its result.
    #[error("Middleware compiler worker stopped unexpectedly")]
    CompilerWorkerStopped,
    /// The clock could not provide safe durable time evidence.
    #[error("Middleware clock failed: {0}")]
    Clock(String),
    /// Evidence could not be encoded.
    #[error("Middleware evidence JSON was invalid: {0}")]
    Json(#[from] serde_json::Error),
    /// Workspace authority or lifecycle rejected the operation.
    #[error(transparent)]
    Workspace(#[from] coral_workspace::Error),
    /// A persisted row does not decode to the domain model.
    #[error("database contains corrupt Middleware data: {0}")]
    CorruptData(String),
    /// The persistence Adapter rejected the operation.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
}

impl Error {
    /// Returns a definite safe rejection; operational uncertainty has none.
    #[must_use]
    pub fn safe_rejection(&self) -> Option<(&'static str, String)> {
        if let Self::Workspace(error) = self {
            return error
                .safe_rejection()
                .map(|(code, message)| (code, message.to_owned()));
        }
        let code = match self {
            Self::InvalidInput(_) => "invalid_input",
            Self::MiddlewareNotFound => "middleware_not_found",
            Self::Authoring(_) => "middleware_admission_failed",
            Self::CompilationTimedOut => "middleware_compilation_timed_out",
            Self::LimitExceeded(_) => "middleware_limit_exceeded",
            Self::PermissionDenied(_) => "permission_denied",
            _ => return None,
        };
        Some((
            code,
            coral_v8::bounded_diagnostic_message(&self.to_string()),
        ))
    }
}
