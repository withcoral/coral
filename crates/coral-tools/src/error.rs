/// Private and Shared Tool authoring, inspection, and persistence failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Caller-supplied domain input was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The Private Tool is absent or concealed from this Principal.
    #[error("Private Tool was not found")]
    PrivateToolNotFound,
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
    PrivateToolAdmission(String),
    /// No live Shared Tool is at the path, or none is visible to this
    /// Principal.
    #[error("Shared Tool was not found")]
    SharedToolNotFound,
    /// A publish expected a current revision other than the one at its
    /// path: zero when none is there.
    #[error("the Shared Tool is at revision {current}")]
    SharedToolRevisionConflict {
        /// The current revision at the path, or zero when none is there.
        current: u64,
    },
    /// The publish names a source the Owner cannot read: another User's
    /// Private Tool, a Private Tool that is gone, or a revision that never
    /// existed.
    #[error("the source of this Shared Tool Publish cannot be read")]
    SharedToolSourceUnreadable,
    /// The path names a namespace that holds other live Shared Tools, or
    /// lies under one of them.
    #[error("Shared Tool Path conflicts with an existing Shared Tool namespace")]
    SharedToolPathConflict,
    /// Source could not become one accepted Shared Tool revision.
    #[error("Shared Tool authoring failed: {0}")]
    SharedToolAdmission(String),
    /// The source's arguments or result cannot cross a Tool call as data.
    #[error("a Shared Tool takes and returns only data: {0}")]
    SharedToolNotData(String),
    /// Compiling a Shared Tool Publish exceeded the fixed authoring deadline.
    #[error("Shared Tool compilation exceeded the two-second deadline")]
    SharedToolCompilationTimedOut,
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
    PrivateToolPathConflict,
    /// Compilation exceeded the fixed authoring deadline.
    #[error("Private Tool compilation exceeded the two-second deadline")]
    PrivateToolCompilationTimedOut,
    /// The compiler queue was full, so the source was not judged. Retryable.
    #[error("the authoring compiler is busy; retry shortly")]
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
    #[error("the authoring compiler is unavailable: {0}")]
    CompilerUnavailable(String),
    /// A compiler worker stopped before returning its result.
    #[error("the authoring compiler worker stopped unexpectedly")]
    CompilerWorkerStopped,
    /// The clock could not provide safe durable time evidence.
    #[error("the authoring clock failed: {0}")]
    Clock(String),
    /// Diagnostics or lifecycle fields could not be encoded.
    #[error("authored Tool evidence JSON was invalid: {0}")]
    Json(#[from] serde_json::Error),
    /// Workspace authority or lifecycle rejected the operation.
    #[error(transparent)]
    Workspace(#[from] coral_workspace::Error),
    /// A persisted row does not decode to the domain model.
    #[error("database contains corrupt authored Tool data: {0}")]
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
            Self::PrivateToolNotFound { .. } => "private_tool_not_found",
            Self::PrivateToolExists => "private_tool_exists",
            Self::PrivateToolRevisionConflict { .. } => "private_tool_revision_conflict",
            Self::PrivateToolAdmission { .. } => "private_tool_admission",
            Self::SharedToolNotFound => "shared_tool_not_found",
            Self::SharedToolRevisionConflict { .. } => "shared_tool_revision_conflict",
            Self::SharedToolSourceUnreadable => "shared_tool_source_unreadable",
            Self::SharedToolPathConflict => "shared_tool_path_conflict",
            Self::SharedToolAdmission(_) => "shared_tool_admission",
            Self::SharedToolNotData(_) => "shared_tool_not_data",
            Self::SharedToolCompilationTimedOut => "shared_tool_compilation_timed_out",
            Self::PrivateToolNotData(_) => "private_tool_not_data",
            Self::PrivateToolPathConflict => "private_tool_path_conflict",
            Self::PrivateToolCompilationTimedOut { .. } => "private_tool_compilation_timed_out",
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
            Self::PrivateToolNotFound => "private_tool_not_found",
            Self::PrivateToolExists => "private_tool_exists",
            Self::PrivateToolRevisionConflict { .. } => "private_tool_revision_conflict",
            Self::PrivateToolAdmission(_) | Self::PrivateToolPathConflict => {
                "private_tool_admission_failed"
            }
            Self::PrivateToolNotData(_) => "private_tool_not_data",
            Self::SharedToolNotFound => "shared_tool_not_found",
            Self::SharedToolRevisionConflict { .. } => "shared_tool_revision_conflict",
            Self::SharedToolSourceUnreadable => "shared_tool_source_unreadable",
            Self::SharedToolPathConflict => "shared_tool_path_conflict",
            Self::SharedToolAdmission(_) => "shared_tool_admission_failed",
            Self::SharedToolNotData(_) => "shared_tool_not_data",
            Self::SharedToolCompilationTimedOut => "shared_tool_compilation_timed_out",
            Self::PrivateToolCompilationTimedOut => "private_tool_compilation_timed_out",
            Self::CompilerBusy => "private_tool_compiler_busy",
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
