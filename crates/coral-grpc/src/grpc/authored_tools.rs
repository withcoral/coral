//! What the Private Tool and Shared Tool services share: one status for
//! each coral-tools failure, the diagnostic projection, and the Agent a
//! request names as a Private Tool's owner.
use super::management as wire;
use super::workspaces::{
    error as workspace_error, invalid, resource, safe_status, safe_status_with,
};
use coral_catalog::AuthoredToolKind::{Private, Shared};
use coral_tools::{AuthoredToolDiagnostic, AuthoredToolDiagnosticCategory};
use coral_workspace::{PrincipalId, WorkspaceId};
use tonic::Status;

/// The Agent whose Private Tool a request names with
/// `workspaces/{workspace}/agents/{agent}`, or none when `name` is empty and
/// the request means the caller's own. The Agent must be named in `workspace`,
/// the Workspace of the request.
pub(super) fn private_tool_agent(
    name: &str,
    workspace: &WorkspaceId,
) -> Result<Option<PrincipalId>, Status> {
    if name.is_empty() {
        return Ok(None);
    }
    let (agent_workspace, agent) = resource(name, "agents")?;
    if &agent_workspace != workspace {
        return Err(invalid("invalid Agent name"));
    }
    PrincipalId::parse(agent)
        .map(Some)
        .map_err(|_error| invalid("invalid Agent name"))
}

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
        // Only a publish compiles a source here. The Owner corrects the
        // content of a publish, so each refusal of it says what is wrong.
        refusal @ (E::Admission(Shared, _) | E::NotData(Shared, _) | E::PackageTooLarge { .. }) => {
            return content_refused("SHARED_TOOL_SOURCE_REJECTED", &refusal);
        }
        refusal @ E::SharedToolRestoreDiffers => {
            return content_refused("SHARED_TOOL_RESTORE_DIFFERS", &refusal);
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

/// The status of a publish whose content Coral refused: `INVALID_ARGUMENT`
/// with `reason`, and the domain's bounded text of the refusal as the
/// `message` metadata.
fn content_refused(reason: &str, refusal: &coral_tools::Error) -> Status {
    safe_status_with(
        tonic::Code::InvalidArgument,
        reason,
        refusal
            .safe_rejection()
            .map(|(_code, message)| ("message", message)),
    )
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

#[cfg(test)]
mod tests {
    use super::super::workspaces::metadata;
    use coral_catalog::AuthoredToolKind::Shared;
    use coral_tools::Error as E;
    use tonic::Code;

    // Catches: a status that says nothing of a refused package, or that
    // carries the text of a failure the Owner cannot correct. No publish
    // through the service reaches these; the transport tests hold the rest.
    #[test]
    fn only_a_refusal_of_the_content_carries_a_message() {
        // (the failure, its status and reason, whether it says what to correct)
        let cases = [
            (
                E::PackageTooLarge {
                    component: "Tool declaration",
                    maximum_bytes: 1,
                },
                (Code::InvalidArgument, "SHARED_TOOL_SOURCE_REJECTED"),
                true,
            ),
            (E::CompilerBusy, (Code::Unavailable, "UNAVAILABLE"), false),
            (
                E::CompilationTimedOut(Shared),
                (Code::Unavailable, "UNAVAILABLE"),
                false,
            ),
            (
                E::CorruptData("private detail".into()),
                (Code::Unavailable, "UNAVAILABLE"),
                false,
            ),
        ];
        for (failure, expected, says_why) in cases {
            let label = format!("{failure:?}");
            let status = super::error(failure);
            assert_eq!((status.code(), status.message()), expected, "{label}");
            assert_eq!(
                metadata(&status)
                    .get("message")
                    .map(|message| !message.is_empty()),
                says_why.then_some(true),
                "{label}"
            );
        }
    }
}
