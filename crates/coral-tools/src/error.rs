use coral_catalog::AuthoredToolKind;

use crate::shared_tool::SharedToolPath;

/// Private and Shared Tool authoring, inspection, and persistence failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Caller-supplied domain input was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The Tool is absent or concealed from this Principal: no Private
    /// Tool of the caller's, or no live Shared Tool, is at the path, or the
    /// Workspace has no Shared Tool with the identity.
    #[error("{} was not found", .0.name())]
    NotFound(AuthoredToolKind),
    /// A create-only save found a Private Tool at its path.
    #[error("a Private Tool is already at this path")]
    PrivateToolExists,
    /// A save, a publish or a removal expected a revision other than the
    /// current one.
    #[error("the {} is at revision {current}", .kind.name())]
    RevisionConflict {
        /// The kind of Tool at the path.
        kind: AuthoredToolKind,
        /// The current revision at the path: for a publish, zero when no
        /// Shared Tool is there.
        current: u64,
    },
    /// Source could not become one accepted Tool definition or revision. The
    /// reason starts with the Tool's Call Path, and holds the authored line
    /// and column of each fault the compiler located.
    #[error("{} authoring failed: {}", .0.name(), .1)]
    Admission(AuthoredToolKind, String),
    /// The publish names a source the Owner cannot read: another User's
    /// Private Tool, a Private Tool that is gone, or a revision that never
    /// existed.
    #[error("the source of this Shared Tool Publish cannot be read")]
    SharedToolSourceUnreadable,
    /// The publish names a revision to restore, and its source or its
    /// description is not that revision's.
    #[error("a restore publishes the exact source and description of the revision it names")]
    SharedToolRestoreDiffers,
    /// The declaration the publish emits is not the one its Owner reviewed:
    /// a Tool the source names changed since the review.
    #[error("the declaration of this Shared Tool changed since its review")]
    SharedToolDeclarationChanged,
    /// The removal names a Shared Tool that the current revision of another
    /// live Shared Tool calls. Removing it would break that one.
    #[error("coral.shared.{} calls this Shared Tool", .dependent.as_str())]
    SharedToolHasDependents {
        /// The path of a Shared Tool that calls it: of several, the first
        /// in path order.
        dependent: SharedToolPath,
    },
    /// The path names a namespace that holds other Tools of its kind, or
    /// lies under one of them: the caller's other Private Tools, or other
    /// live Shared Tools.
    #[error("{}", path_conflict(*.0))]
    PathConflict(AuthoredToolKind),
    /// The source does not take one object, or its argument or result
    /// cannot cross a Tool call as data: the reason names the Tool's Call
    /// Path and the parameter, or where the declaration holds a function, a
    /// class instance, a `Date`, `Map` or `Set`, a `bigint` or a `symbol`.
    #[error("a {} takes one object and returns only data: {}", .0.name(), .1)]
    NotData(AuthoredToolKind, String),
    /// Compilation exceeded the fixed authoring deadline.
    #[error("{} compilation exceeded the two-second deadline", .0.name())]
    CompilationTimedOut(AuthoredToolKind),
    /// The compiler queue was full, so the source was not judged. Retryable.
    #[error("the authoring compiler is busy; retry shortly")]
    CompilerBusy,
    /// No compiler could take the source within the wait for one, so it
    /// was not judged. Retryable.
    #[error("{}", coral_v8::CompileFailure::NO_COMPILER_MESSAGE)]
    NoCompiler,
    /// Current Principal authority does not permit the operation.
    #[error("permission denied: {0}")]
    PermissionDenied(&'static str),
    /// The compiler-only isolate could not initialize.
    #[error("the authoring compiler is unavailable: {0}")]
    CompilerUnavailable(String),
    /// A compiler worker stopped before returning its result.
    #[error("the authoring compiler worker stopped unexpectedly")]
    CompilerWorkerStopped,
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
            Self::SharedToolRestoreDiffers => "shared_tool_restore_differs",
            Self::SharedToolDeclarationChanged => "shared_tool_declaration_changed",
            Self::SharedToolHasDependents { .. } => "shared_tool_has_dependents",
            Self::PathConflict(Private) => "private_tool_path_conflict",
            Self::PathConflict(Shared) => "shared_tool_path_conflict",
            Self::NotData(Private, _) => "private_tool_not_data",
            Self::NotData(Shared, _) => "shared_tool_not_data",
            Self::CompilationTimedOut(Private) => "private_tool_compilation_timed_out",
            Self::CompilationTimedOut(Shared) => "shared_tool_compilation_timed_out",
            Self::CompilerBusy { .. } => "compiler_busy",
            Self::PermissionDenied { .. } => "permission_denied",
            Self::NoCompiler => "no_compiler",
            Self::CompilerUnavailable { .. } => "compiler_unavailable",
            Self::CompilerWorkerStopped { .. } => "compiler_worker_stopped",
            Self::Json { .. } => "json",
            Self::Workspace { .. } => "workspace",
            Self::CorruptData { .. } => "corrupt_data",
            Self::Database { .. } => "database",
        }
    }

    /// The database error behind this one, directly or through the
    /// Workspace error that wraps it. A module that wraps this error asks
    /// here, so it never lists another module's variants.
    #[must_use]
    pub const fn database_error(&self) -> Option<&coral_db::Error> {
        match self {
            Self::Database(error) => Some(error),
            Self::Workspace(error) => error.database_error(),
            // Name every variant so a new one that holds a database error
            // is found here.
            Self::InvalidInput(_)
            | Self::NotFound(_)
            | Self::PrivateToolExists
            | Self::RevisionConflict { .. }
            | Self::Admission(..)
            | Self::SharedToolSourceUnreadable
            | Self::SharedToolRestoreDiffers
            | Self::SharedToolDeclarationChanged
            | Self::SharedToolHasDependents { .. }
            | Self::PathConflict(_)
            | Self::NotData(..)
            | Self::CompilationTimedOut(_)
            | Self::CompilerBusy
            | Self::NoCompiler
            | Self::PermissionDenied(_)
            | Self::CompilerUnavailable(_)
            | Self::CompilerWorkerStopped
            | Self::Json(_)
            | Self::CorruptData(_) => None,
        }
    }

    /// The database error behind this one, by value, or this error back
    /// when it holds none. [`Self::database_error`] says which it is.
    ///
    /// # Errors
    ///
    /// Returns the error itself when no database error is behind it.
    pub fn into_database_error(self) -> Result<coral_db::Error, Self> {
        match self {
            Self::Database(error) => Ok(error),
            Self::Workspace(error) => error.into_database_error().map_err(Self::Workspace),
            other => {
                debug_assert!(other.database_error().is_none());
                Err(other)
            }
        }
    }

    /// The Workspace refusal this error wraps, directly or through another
    /// module's error. A refusal reads the same whichever module wrapped
    /// it, so a caller reports it without naming that module's variants.
    #[must_use]
    pub const fn workspace_refusal(&self) -> Option<coral_workspace::AuthorityRefusal> {
        match self {
            Self::Workspace(error) => error.refusal(),
            // Name every variant so a new one that wraps a Workspace error
            // is found here.
            Self::Database(_)
            | Self::InvalidInput(_)
            | Self::NotFound(_)
            | Self::PrivateToolExists
            | Self::RevisionConflict { .. }
            | Self::Admission(..)
            | Self::SharedToolSourceUnreadable
            | Self::SharedToolRestoreDiffers
            | Self::SharedToolDeclarationChanged
            | Self::SharedToolHasDependents { .. }
            | Self::PathConflict(_)
            | Self::NotData(..)
            | Self::CompilationTimedOut(_)
            | Self::CompilerBusy
            | Self::NoCompiler
            | Self::PermissionDenied(_)
            | Self::CompilerUnavailable(_)
            | Self::CompilerWorkerStopped
            | Self::Json(_)
            | Self::CorruptData(_) => None,
        }
    }

    /// Returns a definite safe rejection; operational uncertainty has no invented outcome.
    #[must_use]
    pub fn safe_rejection(&self) -> Option<(&'static str, String)> {
        use AuthoredToolKind::{Private, Shared};
        if let Some(refusal) = self.workspace_refusal() {
            return Some((refusal.code(), refusal.message().to_owned()));
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
            | Self::SharedToolRestoreDiffers
            | Self::SharedToolDeclarationChanged
            | Self::SharedToolHasDependents { .. }
            | Self::PathConflict(Shared)
            | Self::NotData(..)
            | Self::CompilationTimedOut(_) => self.code(),
            Self::CompilerBusy => "authoring_compiler_busy",
            Self::NoCompiler => "authoring_compiler_unavailable",
            Self::PermissionDenied(_) => "permission_denied",
            _ => return None,
        };
        Some((
            code,
            coral_v8::bounded_diagnostic_message(&self.to_string()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::Error;

    // Catches: a database error that a caller of Tools cannot find, so an
    // unanswered write reads as a definite answer.
    #[test]
    fn a_tools_error_states_the_database_error_behind_it() {
        let unanswered = || coral_db::Error::WriterBusy;
        for (error, label) in [
            (Error::Database(unanswered()), "tools"),
            (
                Error::Workspace(coral_workspace::Error::Database(unanswered())),
                "workspace",
            ),
        ] {
            assert!(
                error
                    .database_error()
                    .is_some_and(coral_db::Error::is_unanswered),
                "{label}"
            );
        }
        for error in [
            Error::PrivateToolExists,
            Error::CorruptData("row".into()),
            Error::Workspace(coral_workspace::Error::WorkspaceArchived),
        ] {
            assert!(error.database_error().is_none(), "{error:?}");
        }
    }
}
