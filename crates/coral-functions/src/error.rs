/// Current Saved Function authoring, inspection, and persistence failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Caller-supplied domain input was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The Saved Function is absent or concealed from this Principal.
    #[error("Saved Function was not found")]
    SavedFunctionNotFound,
    /// Source could not become one accepted callable definition.
    #[error("Saved Function authoring failed: {0}")]
    SavedFunctionAdmission(String),
    /// Compilation exceeded the fixed authoring deadline.
    #[error("Saved Function compilation exceeded the two-second deadline")]
    SavedFunctionCompilationTimedOut,
    /// Current Principal authority does not permit the operation.
    #[error("permission denied: {0}")]
    PermissionDenied(&'static str),
    /// Compiler output exceeded a fixed authoring limit.
    #[error("Program package exceeds the {component} limit of {maximum_bytes} bytes")]
    PackageTooLarge {
        /// Component that crossed its bound.
        component: &'static str,
        /// Fixed accepted maximum.
        maximum_bytes: usize,
    },
    /// The compiler-only isolate could not initialize.
    #[error("Saved Function compiler is unavailable: {0}")]
    CompilerUnavailable(String),
    /// A compiler worker stopped before returning its result.
    #[error("Saved Function compiler worker stopped unexpectedly")]
    CompilerWorkerStopped,
    /// The clock could not provide safe durable time evidence.
    #[error("Saved Function clock failed: {0}")]
    Clock(String),
    /// Diagnostics or lifecycle fields could not be encoded.
    #[error("Saved Function evidence JSON was invalid: {0}")]
    Json(#[from] serde_json::Error),
    /// Workspace authority or lifecycle rejected the operation.
    #[error(transparent)]
    Workspace(#[from] coral_workspace::Error),
    /// A persisted row does not decode to the domain model.
    #[error("database contains corrupt Saved Function data: {0}")]
    CorruptData(String),
    /// The persistence Adapter rejected the operation.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
}

impl Error {
    /// The error's stable kind, for an event's `error.type`: a bounded
    /// code that never carries the error's text.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidInput { .. } => "invalid_input",
            Self::SavedFunctionNotFound { .. } => "saved_function_not_found",
            Self::SavedFunctionAdmission { .. } => "saved_function_admission",
            Self::SavedFunctionCompilationTimedOut { .. } => "saved_function_compilation_timed_out",
            Self::PermissionDenied { .. } => "permission_denied",
            Self::PackageTooLarge { .. } => "package_too_large",
            Self::CompilerUnavailable { .. } => "compiler_unavailable",
            Self::CompilerWorkerStopped { .. } => "compiler_worker_stopped",
            Self::Clock { .. } => "clock",
            Self::Json { .. } => "json",
            Self::Workspace { .. } => "workspace",
            Self::CorruptData { .. } => "corrupt_data",
            Self::Database { .. } => "database",
        }
    }

    /// Returns a definite safe rejection; operational uncertainty has no invented outcome.
    #[must_use]
    pub fn safe_rejection(&self) -> Option<(&'static str, String)> {
        if let Self::Workspace(error) = self {
            return error
                .safe_rejection()
                .map(|(code, message)| (code, message.to_owned()));
        }
        let code = match self {
            Self::InvalidInput(_) => "invalid_input",
            Self::SavedFunctionNotFound => "saved_function_not_found",
            Self::SavedFunctionAdmission(_) => "saved_function_admission_failed",
            Self::SavedFunctionCompilationTimedOut => "saved_function_compilation_timed_out",
            Self::PackageTooLarge { .. } => "program_package_too_large",
            Self::PermissionDenied(_) => "permission_denied",
            _ => return None,
        };
        Some((
            code,
            coral_v8::bounded_diagnostic_message(&self.to_string()),
        ))
    }
}
