use coral_catalog::AuthoredToolKind;

/// Private and Shared Tool authoring, inspection, and persistence failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Caller-supplied domain input was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The Tool is absent or concealed from this Principal: no Private
    /// Tool of the caller's, or no live Shared Tool, is at the path.
    #[error("{} was not found", .0.name())]
    NotFound(AuthoredToolKind),
    /// A create-only save found a Private Tool at its path.
    #[error("a Private Tool is already at this path")]
    PrivateToolExists,
    /// A save or a publish expected a revision other than the current one.
    #[error("the {} is at revision {current}", .kind.name())]
    RevisionConflict {
        /// The kind of Tool at the path.
        kind: AuthoredToolKind,
        /// The current revision at the path: for a publish, zero when no
        /// Shared Tool is there.
        current: u64,
    },
    /// Source could not become one accepted Tool definition or revision.
    #[error("{} authoring failed: {}", .0.name(), .1)]
    Admission(AuthoredToolKind, String),
    /// The publish names a source the Owner cannot read: another User's
    /// Private Tool, a Private Tool that is gone, or a revision that never
    /// existed.
    #[error("the source of this Shared Tool Publish cannot be read")]
    SharedToolSourceUnreadable,
    /// The path names a namespace that holds other Tools of its kind, or
    /// lies under one of them: the caller's other Private Tools, or other
    /// live Shared Tools.
    #[error("{}", path_conflict(*.0))]
    PathConflict(AuthoredToolKind),
    /// The source's arguments or result cannot cross a Tool call as data:
    /// the reason names where the declaration holds a function, a class
    /// instance, a `Date`, `Map` or `Set`, a `bigint` or a `symbol`.
    #[error("a {} takes and returns only data: {}", .0.name(), .1)]
    NotData(AuthoredToolKind, String),
    /// Compilation exceeded the fixed authoring deadline.
    #[error("{} compilation exceeded the two-second deadline", .0.name())]
    CompilationTimedOut(AuthoredToolKind),
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

/// What a path conflict says. A Private Tool's keeps the wording its save
/// has always returned.
const fn path_conflict(kind: AuthoredToolKind) -> &'static str {
    match kind {
        AuthoredToolKind::Private => {
            "Private Tool authoring failed: Private Tool Path conflicts with an existing virtual namespace"
        }
        AuthoredToolKind::Shared => {
            "Shared Tool Path conflicts with an existing Shared Tool namespace"
        }
    }
}

impl Error {
    /// The error's stable kind, for an event's `error.type`: a bounded
    /// code that never carries the error's text.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        use AuthoredToolKind::{Private, Shared};
        match self {
            Self::InvalidInput { .. } => "invalid_input",
            Self::NotFound(Private) => "private_tool_not_found",
            Self::NotFound(Shared) => "shared_tool_not_found",
            Self::PrivateToolExists => "private_tool_exists",
            Self::RevisionConflict { kind: Private, .. } => "private_tool_revision_conflict",
            Self::RevisionConflict { kind: Shared, .. } => "shared_tool_revision_conflict",
            Self::Admission(Private, _) => "private_tool_admission",
            Self::Admission(Shared, _) => "shared_tool_admission",
            Self::SharedToolSourceUnreadable => "shared_tool_source_unreadable",
            Self::PathConflict(Private) => "private_tool_path_conflict",
            Self::PathConflict(Shared) => "shared_tool_path_conflict",
            Self::NotData(Private, _) => "private_tool_not_data",
            Self::NotData(Shared, _) => "shared_tool_not_data",
            Self::CompilationTimedOut(Private) => "private_tool_compilation_timed_out",
            Self::CompilationTimedOut(Shared) => "shared_tool_compilation_timed_out",
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
        use AuthoredToolKind::{Private, Shared};
        if let Self::Workspace(error) = self {
            return error
                .safe_rejection()
                .map(|(code, message)| (code, message.to_owned()));
        }
        let code = match self {
            Self::InvalidInput(_) => "invalid_input",
            Self::PrivateToolExists => "private_tool_exists",
            // A Private Tool's path conflict is refused as its authoring is.
            Self::Admission(Private, _) | Self::PathConflict(Private) => {
                "private_tool_admission_failed"
            }
            Self::Admission(Shared, _) => "shared_tool_admission_failed",
            Self::SharedToolSourceUnreadable => "shared_tool_source_unreadable",
            Self::NotFound(_)
            | Self::RevisionConflict { .. }
            | Self::PathConflict(Shared)
            | Self::NotData(..)
            | Self::CompilationTimedOut(_) => self.code(),
            Self::CompilerBusy => "authoring_compiler_busy",
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
