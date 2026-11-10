//! What the Private Tool and Shared Tool services share: one status for
//! each coral-tools failure, and the diagnostic projection.
use super::management as wire;
use super::workspaces::{error as workspace_error, safe_status, safe_status_with};
use coral_catalog::AuthoredToolKind::{Private, Shared};
use coral_tools::{AuthoredToolDiagnostic, AuthoredToolDiagnosticCategory};
use tonic::Status;

/// The status of one coral-tools failure, for both services. Each variant
/// is named, so a new one needs a wire decision.
pub(super) fn error(error: coral_tools::Error) -> Status {
    use coral_tools::Error as E;
    let (code, reason) = match error {
        E::Workspace(error) => return workspace_error(error),
        E::InvalidInput(_) => (tonic::Code::InvalidArgument, "INVALID_ARGUMENT"),
        E::NotFound(_) => (tonic::Code::NotFound, "NOT_FOUND"),
        E::PermissionDenied(_) => (tonic::Code::PermissionDenied, "PERMISSION_DENIED"),
        E::PrivateToolExists => (tonic::Code::AlreadyExists, "ALREADY_EXISTS"),
        E::RevisionConflict { .. } => (tonic::Code::Aborted, "ABORTED"),
        E::SharedToolSourceUnreadable => (
            tonic::Code::FailedPrecondition,
            "SHARED_TOOL_SOURCE_UNREADABLE",
        ),
        E::PathConflict(Shared) => (tonic::Code::FailedPrecondition, "SHARED_TOOL_PATH_TAKEN"),
        // The Owner must first deal with the Shared Tool that calls this one,
        // so the status names it.
        E::SharedToolHasDependents { dependent } => {
            return safe_status_with(
                tonic::Code::FailedPrecondition,
                "SHARED_TOOL_HAS_DEPENDENTS",
                [("dependent", dependent.as_str().to_owned())],
            );
        }
        // Only a publish compiles a source here.
        E::Admission(Shared, _) | E::NotData(Shared, _) | E::PackageTooLarge { .. } => {
            (tonic::Code::InvalidArgument, "SHARED_TOOL_SOURCE_REJECTED")
        }
        // No management call saves a Private Tool, so none meets a Private
        // Tool's authoring refusal. A compiler that did not judge the source
        // in time may on a retry.
        E::Admission(Private, _)
        | E::NotData(Private, _)
        | E::PathConflict(Private)
        | E::CompilationTimedOut(_)
        | E::CompilerBusy
        | E::CompilerUnavailable(_)
        | E::CompilerWorkerStopped
        | E::Clock(_)
        | E::Json(_)
        | E::CorruptData(_)
        | E::Database(_) => (tonic::Code::Unavailable, "UNAVAILABLE"),
    };
    safe_status(code, reason)
}

pub(super) fn diagnostic(diagnostic: &AuthoredToolDiagnostic) -> wire::AuthoredToolDiagnostic {
    let category = match diagnostic.category() {
        AuthoredToolDiagnosticCategory::Message => wire::AuthoredToolDiagnosticCategory::Message,
        AuthoredToolDiagnosticCategory::Warning => wire::AuthoredToolDiagnosticCategory::Warning,
        AuthoredToolDiagnosticCategory::Error => wire::AuthoredToolDiagnosticCategory::Error,
        AuthoredToolDiagnosticCategory::Suggestion => {
            wire::AuthoredToolDiagnosticCategory::Suggestion
        }
    };
    wire::AuthoredToolDiagnostic {
        category: category.into(),
        code: diagnostic.code(),
        message: diagnostic.message().to_owned(),
        line: diagnostic.line(),
        column: diagnostic.column(),
    }
}
