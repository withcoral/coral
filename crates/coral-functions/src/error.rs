/// Current Saved Function authoring, inspection, and persistence failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Caller-supplied domain input was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The Saved Function is absent or concealed from this Principal.
    #[error("Private Tool was not found")]
    SavedFunctionNotFound,
    /// A create-only save found a Private Tool at its path.
    #[error("a Private Tool is already at this path")]
    PrivateToolExists,
    /// A replace-only save expected a revision other than the current one.
    #[error("the Private Tool is at revision {current}")]
    PrivateToolRevisionConflict {
        /// The revision of the function at the path.
        current: u64,
    },
    /// Source could not become one accepted callable definition.
    #[error("Private Tool authoring failed: {0}")]
    SavedFunctionAdmission(String),
    /// The source's arguments or result cannot cross a Tool call as data:
    /// the reason names where the declaration holds a function, a class
    /// instance, a `Date`, `Map` or `Set`, a `bigint` or a `symbol`.
    #[error("a Private Tool takes and returns only data: {0}")]
    PrivateToolNotData(String),
    /// The path names a namespace that holds the caller's other functions,
    /// or lies under one of them.
    #[error(
        "Private Tool authoring failed: Private Tool Path conflicts with an existing virtual namespace"
    )]
    SavedFunctionPathConflict,
    /// Compilation exceeded the fixed authoring deadline.
    #[error("Private Tool compilation exceeded the two-second deadline")]
    SavedFunctionCompilationTimedOut,
    /// The compiler queue was full, so the source was not judged. Retryable.
    #[error("Private Tool compiler is busy; retry shortly")]
    CompilerBusy,
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
    #[error("Private Tool compiler is unavailable: {0}")]
    CompilerUnavailable(String),
    /// A compiler worker stopped before returning its result.
    #[error("Private Tool compiler worker stopped unexpectedly")]
    CompilerWorkerStopped,
    /// The clock could not provide safe durable time evidence.
    #[error("Private Tool clock failed: {0}")]
    Clock(String),
    /// Diagnostics or lifecycle fields could not be encoded.
    #[error("Private Tool evidence JSON was invalid: {0}")]
    Json(#[from] serde_json::Error),
    /// Workspace authority or lifecycle rejected the operation.
    #[error(transparent)]
    Workspace(#[from] coral_workspace::Error),
    /// A persisted row does not decode to the domain model.
    #[error("database contains corrupt Private Tool data: {0}")]
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
            Self::PrivateToolExists => "private_tool_exists",
            Self::PrivateToolRevisionConflict { .. } => "private_tool_revision_conflict",
            Self::SavedFunctionAdmission { .. } => "saved_function_admission",
            Self::PrivateToolNotData(_) => "private_tool_not_data",
            Self::SavedFunctionPathConflict => "saved_function_path_conflict",
            Self::SavedFunctionCompilationTimedOut { .. } => "saved_function_compilation_timed_out",
            Self::CompilerBusy { .. } => "compiler_busy",
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
            Self::PrivateToolExists => "private_tool_exists",
            Self::PrivateToolRevisionConflict { .. } => "private_tool_revision_conflict",
            Self::SavedFunctionAdmission(_) | Self::SavedFunctionPathConflict => {
                "saved_function_admission_failed"
            }
            Self::PrivateToolNotData(_) => "private_tool_not_data",
            Self::SavedFunctionCompilationTimedOut => "saved_function_compilation_timed_out",
            Self::CompilerBusy => "saved_function_compiler_busy",
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
