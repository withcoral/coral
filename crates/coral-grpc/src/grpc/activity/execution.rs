use super::{
    GrpcServices, PrivateAudience, Request, Response, Status, WorkspaceId, error, limit, resource,
    wire,
};
use coral_run::InvocationStatus;

#[expect(
    clippy::needless_pass_by_value,
    reason = "map_err consumes domain errors to project safe statuses"
)]
pub(super) fn run_error(error: coral_run::Error) -> Status {
    match error {
        coral_run::Error::RunNotFound | coral_run::Error::ApprovalNotFound => {
            Status::not_found("history unavailable")
        }
        coral_run::Error::InvalidInput(_) => Status::invalid_argument("invalid history query"),
        coral_run::Error::PermissionDenied(_)
        | coral_run::Error::Workspace(coral_workspace::Error::PermissionDenied(_)) => {
            Status::permission_denied("history unavailable")
        }
        // The Run Owner stopped before it answered the request: the outcome
        // may be recorded already, and the record read again says.
        coral_run::Error::OwnerLost => {
            Status::aborted("request outcome unknown; read the run again")
        }
        // The request waits in the Run Inbox and its owner has not recorded
        // it yet. Retryable: a retry reads what was recorded meanwhile.
        coral_run::Error::RunRequestNotYetRecorded => super::super::workspaces::safe_status(
            tonic::Code::Unavailable,
            "RUN_REQUEST_NOT_YET_RECORDED",
        ),
        // The process is shutting down and accepts no new Run; another
        // process takes the request.
        coral_run::Error::ShuttingDown => Status::unavailable("server is shutting down; retry"),
        // Name every remaining variant so a new domain error needs an explicit wire decision.
        coral_run::Error::ProgramAdmission(_)
        | coral_run::Error::Functions(_)
        | coral_run::Error::CompilationTimedOut
        | coral_run::Error::PackageTooLarge { .. }
        | coral_run::Error::ProgramRuntimeUnavailable(_)
        | coral_run::Error::ProgramWorkerStopped
        | coral_run::Error::Clock(_)
        | coral_run::Error::IdentifierIssuance(_)
        | coral_run::Error::ProgramValueTooLarge
        | coral_run::Error::InvalidProgramValue
        | coral_run::Error::ProgramValueJson(_)
        | coral_run::Error::InvocationRejected(_)
        | coral_run::Error::InvocationPersistence(_)
        | coral_run::Error::RunEventPersistence(_)
        | coral_run::Error::ClaimMismatch
        | coral_run::Error::HeadMismatch
        | coral_run::Error::RunEnded
        | coral_run::Error::ProgramHostTerminated
        | coral_run::Error::Provider(_)
        | coral_run::Error::Policy(_)
        | coral_run::Error::Upstream(_)
        | coral_run::Error::Workspace(_)
        | coral_run::Error::CorruptData(_)
        | coral_run::Error::Database(_) => Status::unavailable("history unavailable"),
    }
}
pub(super) fn state(value: coral_run::RunState) -> i32 {
    use coral_run::RunState as S;
    use wire::ActivityRunState as W;
    match value {
        S::Running => W::Running,
        S::Succeeded => W::Succeeded,
        S::Failed => W::Failed,
        S::Canceled => W::Canceled,
        S::TimedOut => W::TimedOut,
        S::Interrupted => W::Interrupted,
    }
    .into()
}
fn outcome(value: coral_run::InvocationResultKind) -> i32 {
    use coral_run::InvocationResultKind as K;
    use wire::ActivityInvocationOutcome as W;
    match value {
        K::UpstreamOutcome => W::UpstreamOutcome,
        K::CoralFailure => W::CoralFailure,
        K::RunTerminated => W::RunTerminated,
        K::Indeterminate => W::Indeterminate,
    }
    .into()
}
pub(super) fn agent(workspace: &WorkspaceId, principal: &coral_workspace::Principal) -> String {
    if principal.kind() == coral_workspace::PrincipalKind::Agent {
        format!("workspaces/{workspace}/agents/{}", principal.id())
    } else {
        String::new()
    }
}
pub(super) fn task_reference(submission: Option<&coral_task::TaskSubmission>) -> String {
    submission
        .map(|value| {
            format!(
                "workspaces/{}/tasks/{}",
                value.task.workspace, value.task.task
            )
        })
        .unwrap_or_default()
}
pub(super) fn submission(value: &coral_task::TaskSubmission) -> wire::ActivityInteraction {
    super::interaction(&task_reference(Some(value)), &value.interaction)
}
pub(super) fn run(value: &coral_run::RunSummary) -> wire::ActivityRun {
    wire::ActivityRun {
        intent: String::new(),
        action_page: value
            .action_page
            .as_ref()
            .map(|page| wire::ActivityActionPage {
                actions: page
                    .actions
                    .iter()
                    .map(|action| action_summary(&value.workspace, action))
                    .collect(),
                total: page.total,
                next_action: page
                    .next_action
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
            }),
        actor_label: actor_label(&value.principal, value.agent_label.as_deref()),
        workspace_label: value.workspace_label.clone(),

        context_impact_bytes: value.context_impact_bytes,
        name: format!("workspaces/{}/runs/{}", value.workspace, value.id),
        workspace: format!("workspaces/{}", value.workspace),
        state: state(value.state),
        started_at_unix_nanos: value.started_at,
        ended_at_unix_nanos: value.ended_at,
        duration_nanos: value.duration,
        requires_action: value.requires_action,
        task: String::new(),
        actor: value.principal.id().to_string(),
        agent: agent(&value.workspace, &value.principal),
    }
}
// Labels for removed or concealed attribution. Domain Modules return `None`;
// only this adapter chooses the fallback text.
const UNAVAILABLE_AGENT: &str = "Unavailable agent";
pub(super) const UNAVAILABLE_PROVIDER: &str = "Unavailable provider";
pub(super) const UNAVAILABLE_USER: &str = "Unavailable user";

pub(super) fn actor_label(
    principal: &coral_workspace::Principal,
    agent_label: Option<&str>,
) -> String {
    match principal.kind() {
        coral_workspace::PrincipalKind::User => "You".into(),
        coral_workspace::PrincipalKind::Agent => agent_label.unwrap_or(UNAVAILABLE_AGENT).into(),
    }
}
pub(super) fn summary(value: coral_run::ActivitySummary) -> wire::SummarizeActivityResponse {
    let calls = value.calls;
    wire::SummarizeActivityResponse {
        calls: Some(wire::ActivityCallOutcomes {
            answered: calls.answered,
            blocked: calls.blocked,
            not_allowed: calls.not_allowed,
            needs_setup: calls.needs_setup,
            failed: calls.failed,
            canceled: calls.canceled,
        }),
        stopped: value
            .stopped
            .into_iter()
            .map(|op| wire::ActivityStoppedOperation {
                call_path: op.call_path,
                blocked: op.blocked,
                not_allowed: op.not_allowed,
                needs_setup: op.needs_setup,
            })
            .collect(),
        operations: value
            .operations
            .into_iter()
            .map(|op| wire::ActivityOperationCalls {
                call_path: op.call_path,
                calls: op.calls,
            })
            .collect(),
        approvals: Some(wire::ActivityApprovalDecisions {
            approved: value.approvals.approved,
            declined: value.approvals.declined,
            median_wait_nanos: value.approvals.median_wait_nanos,
        }),
        context_impact_bytes: value.context_impact_bytes,
        context_impact_partial: value.context_impact_partial,
        actors: value
            .actors
            .iter()
            .map(|actor| wire::ActivityContextShare {
                key: actor.principal.id().to_string(),
                label: actor_label(&actor.principal, actor.agent_label.as_deref()),
                bytes: actor.bytes,
            })
            .collect(),
    }
}
pub(super) fn action_summary(
    workspace: &WorkspaceId,
    action: &coral_run::ActionSummary,
) -> wire::ActivityActionSummary {
    wire::ActivityActionSummary {
        request: Some(request_reference(workspace, action.request.clone())),
        call_path: action.call_path.clone(),
        provider_label: action
            .provider_label
            .clone()
            .unwrap_or_else(|| UNAVAILABLE_PROVIDER.into()),
        deadline_at_unix_nanos: action.deadline_at_unix_nanos,
    }
}
pub(super) fn invocation(value: &coral_run::InvocationSummary) -> wire::ActivityInvocation {
    wire::ActivityInvocation {
        context_impact_bytes: value.context_impact_bytes,
        name: format!("workspaces/{}/invocations/{}", value.workspace, value.id),
        workspace: format!("workspaces/{}", value.workspace),
        run: format!("workspaces/{}/runs/{}", value.workspace, value.run),
        provider: format!(
            "workspaces/{}/providers/{}",
            value.workspace, value.provider
        ),
        operation: value.operation.clone(),
        call_path: value.call_path.clone(),
        actor: value.principal.id().to_string(),
        agent: agent(&value.workspace, &value.principal),
        dispatched_at_unix_nanos: value.dispatched_at,
        state: match value.state {
            InvocationStatus::Pending => wire::ActivityInvocationState::Pending,
            InvocationStatus::Dispatching => wire::ActivityInvocationState::Dispatching,
            InvocationStatus::Ended => wire::ActivityInvocationState::Ended,
        }
        .into(),
        outcome: value.outcome.map(outcome).unwrap_or_default(),
        started_at_unix_nanos: value.started_at,
        ended_at_unix_nanos: value.ended_at,
        duration_nanos: value.duration,
        requires_action: value.requires_action,
        task: String::new(),
        source_location: value
            .source_location
            .as_ref()
            .map(|location| source_location(&value.workspace, location)),
    }
}
/// Names a snapshot exactly as `InspectRunResponse.linked_functions` does.
fn saved_function_name(workspace: &WorkspaceId, saved_function_id: &str) -> String {
    format!("workspaces/{workspace}/savedFunctions/{saved_function_id}")
}
pub(super) fn source_location(
    workspace: &WorkspaceId,
    value: &coral_run::SourceLocation,
) -> wire::ActivitySourceLocation {
    use wire::activity_source_location::Document;
    wire::ActivitySourceLocation {
        document: Some(match &value.document {
            coral_run::SourceDocument::Program => Document::Program(wire::ActivityEmpty {}),
            coral_run::SourceDocument::SavedFunction { saved_function_id } => {
                Document::SavedFunction(saved_function_name(workspace, saved_function_id))
            }
        }),
        line: value.line,
        column: value.column,
    }
}
fn program_error(value: &coral_run::ProgramError) -> wire::ActivityProgramError {
    wire::ActivityProgramError {
        name: value.name.clone(),
        message: value.message.clone(),
        frames: value
            .frames
            .iter()
            .map(|f| wire::ActivityErrorFrame {
                source: f.source.clone(),
                line: f.line,
                column: f.column,
            })
            .collect(),
        operation_code: value
            .operation
            .as_ref()
            .map(|o| o.code.clone())
            .unwrap_or_default(),
        operation_trace_id: value
            .operation
            .as_ref()
            .and_then(|o| o.trace_id.clone())
            .unwrap_or_default(),
        operation_status: value
            .operation
            .as_ref()
            .and_then(|o| o.status)
            .map(u32::from)
            .unwrap_or_default(),
        operation_body: value
            .operation
            .as_ref()
            .and_then(|o| o.body.clone())
            .unwrap_or_default(),
        operation_body_truncated: value.operation.as_ref().is_some_and(|o| o.body_truncated),
        cause: value.cause.as_ref().map(|c| Box::new(program_error(c))),
        errors: value.errors.iter().map(program_error).collect(),
        truncated: value.truncated,
        cause_omitted: value.cause_omitted,
        omitted_errors: u32::try_from(value.omitted_errors).unwrap_or(u32::MAX),
        circular: value.circular,
    }
}
pub(super) fn event(
    workspace: &WorkspaceId,
    value: &coral_run::RunEvent,
) -> wire::ActivityRunEvent {
    use coral_run::RunEventKind as K;
    use wire::activity_run_event::Payload as P;
    let payload = match value.kind() {
        K::Started => Some(P::Started(wire::ActivityEmpty {})),
        K::Ended { state: s } => Some(P::Ended(wire::ActivityRunEnded { state: state(*s) })),
        K::InvocationStarted { invocation_id } => {
            Some(P::InvocationStarted(wire::ActivityInvocationStarted {
                invocation: format!("workspaces/{workspace}/invocations/{invocation_id}"),
            }))
        }
        K::InvocationEnded {
            invocation_id,
            result_kind,
        } => Some(P::InvocationEnded(wire::ActivityInvocationEnded {
            invocation: format!("workspaces/{workspace}/invocations/{invocation_id}"),
            outcome: outcome(*result_kind),
        })),
        K::ProgramEmitted { value } => Some(P::ProgramEmitted(wire::ActivityProgramEmitted {
            value_json: value.as_json().to_string(),
        })),
        K::ProgramLogged { level, message } => {
            Some(P::ProgramLogged(wire::ActivityProgramLogged {
                level: match level {
                    coral_run::ProgramLogLevel::Debug => wire::ActivityLogLevel::Debug,
                    coral_run::ProgramLogLevel::Info => wire::ActivityLogLevel::Info,
                    coral_run::ProgramLogLevel::Warn => wire::ActivityLogLevel::Warn,
                    coral_run::ProgramLogLevel::Error => wire::ActivityLogLevel::Error,
                }
                .into(),
                message: message.clone(),
            }))
        }
        K::CancellationRequested { principal } => Some(P::CancellationRequested(
            wire::ActivityCancellationRequested {
                actor: principal.id().to_string(),
                actor_kind: match principal.kind() {
                    coral_workspace::PrincipalKind::User => wire::ActivityPrincipalKind::User,
                    coral_workspace::PrincipalKind::Agent => wire::ActivityPrincipalKind::Agent,
                }
                .into(),
            },
        )),
        _ => None,
    };
    wire::ActivityRunEvent {
        sequence: value.sequence(),
        occurred_at_unix_nanos: value.occurred_at_unix_nanos(),
        payload,
        source_location: value
            .source_location()
            .map(|location| source_location(workspace, location)),
    }
}
fn failure_code(code: coral_run::RunFailureCode) -> i32 {
    use coral_run::RunFailureCode as C;
    use wire::ActivityRunFailureCode as W;
    match code {
        C::ProgramRejected => W::ProgramRejected,
        C::InvalidProgramValue => W::InvalidProgramValue,
        C::ResourceLimitExceeded => W::ResourceLimitExceeded,
        C::MemoryLimitExceeded => W::MemoryLimitExceeded,
        C::ProgramExecutionFailed => W::ProgramExecutionFailed,
        C::RunAuthorityLost => W::RunAuthorityLost,
    }
    .into()
}
pub(super) async fn inspect_run(
    services: &GrpcServices,
    request: Request<wire::InspectRunRequest>,
) -> Result<Response<wire::InspectRunResponse>, Status> {
    let actor = services
        .principal(&request, None, PrivateAudience::Reef)
        .await?;
    let input = request.into_inner();
    let (workspace, id) = resource(&input.name, "runs")?;
    let action_after = if input.action_after.is_empty() {
        None
    } else {
        Some(coral_run::ActionCursor::parse(&input.action_after).map_err(run_error)?)
    };
    let account_connection = if input.account_connection.is_empty() {
        None
    } else {
        let (selected_workspace, id) = resource(&input.account_connection, "accountConnections")?;
        let id = coral_provider::account::ConnectionRequestId::parse(id)
            .map_err(|_invalid| Status::invalid_argument("invalid account connection name"))?;
        (selected_workspace == workspace).then_some(id)
    };
    let history = services
        .tasks
        .inspect_run(
            &services.runs,
            &actor,
            &workspace,
            &coral_run::RunId::parse(id).map_err(run_error)?,
            &coral_run::RunInspection {
                event_after: input.event_after,
                event_limit: limit(input.page_size)?,
                action_after,
                account_connection,
            },
        )
        .await
        .map_err(error)?;
    let detail = history.execution;
    let mut summary = run(&detail.summary);
    summary.task = task_reference(history.submission.as_ref());
    summary.intent = history
        .submission
        .as_ref()
        .map(|submission| submission.interaction.intent.as_str().to_owned())
        .unwrap_or_default();
    Ok(Response::new(wire::InspectRunResponse {
        account_connection: detail
            .account_connection
            .map(|id| format!("workspaces/{workspace}/accountConnections/{id}"))
            .unwrap_or_default(),
        submission: history.submission.as_ref().map(submission),
        run: Some(summary),
        submitted_source: detail.submitted_source,
        linked_functions: detail
            .linked_functions
            .into_iter()
            .map(|s| wire::ActivitySourceSnapshot {
                saved_function: saved_function_name(&workspace, &s.saved_function_id),
                path: s.path,
                source: s.source,
            })
            .collect(),
        next_event_after: detail
            .events
            .last()
            .map_or(input.event_after, coral_run::RunEvent::sequence),
        has_more_events: detail.has_more_events,
        events: detail.events.iter().map(|e| event(&workspace, e)).collect(),
        terminal: detail.terminal.map(|terminal| {
            use wire::inspect_run_response::Terminal as T;
            match terminal {
                coral_run::RunTerminalOutcome::Succeeded { final_value } => {
                    T::Succeeded(wire::ActivityRunSucceeded {
                        final_value_json: match final_value {
                            coral_run::FinalValue::Present(value) => {
                                Some(value.as_json().to_string())
                            }
                            coral_run::FinalValue::Absent => None,
                        },
                    })
                }
                coral_run::RunTerminalOutcome::Failed(failure) => {
                    T::Failed(wire::ActivityRunFailed {
                        code: failure_code(failure.code()),
                        message: failure.message().into(),
                        error: failure.error().map(program_error),
                    })
                }
                coral_run::RunTerminalOutcome::Canceled => T::Canceled(wire::ActivityEmpty {}),
                coral_run::RunTerminalOutcome::TimedOut => T::TimedOut(wire::ActivityEmpty {}),
                coral_run::RunTerminalOutcome::Interrupted => {
                    T::Interrupted(wire::ActivityEmpty {})
                }
            }
        }),
    }))
}
fn request_reference(
    workspace: &WorkspaceId,
    request: coral_run::RequiredActionRequest,
) -> wire::ActivityRequestReference {
    wire::ActivityRequestReference {
        request: Some(match request {
            coral_run::RequiredActionRequest::AccountConnection(id) => {
                wire::activity_request_reference::Request::AccountConnection(format!(
                    "workspaces/{workspace}/accountConnections/{id}"
                ))
            }
            coral_run::RequiredActionRequest::InvocationApproval(id) => {
                wire::activity_request_reference::Request::Approval(format!(
                    "workspaces/{workspace}/approvals/{id}"
                ))
            }
        }),
    }
}
pub(super) async fn inspect_invocation(
    services: &GrpcServices,
    request: Request<wire::InspectInvocationRequest>,
) -> Result<Response<wire::InspectInvocationResponse>, Status> {
    let actor = services
        .principal(&request, None, PrivateAudience::Reef)
        .await?;
    let input = request.into_inner();
    let (workspace, id) = resource(&input.name, "invocations")?;
    let history = services
        .tasks
        .inspect_invocation(
            &services.runs,
            &actor,
            &workspace,
            &coral_run::InvocationId::parse(id).map_err(run_error)?,
        )
        .await
        .map_err(error)?;
    let detail = history.execution;
    let mut summary = invocation(&detail.summary);
    summary.task = task_reference(history.submission.as_ref());
    let outcome = detail.outcome.map(|outcome| {
        use wire::inspect_invocation_response::Outcome as W;
        match outcome {
            coral_run::InvocationOutcome::UpstreamOutcome(value) => {
                W::UpstreamOutcomeJson(value.to_string())
            }
            coral_run::InvocationOutcome::CoralFailure { code, message } => {
                W::CoralFailure(wire::ActivityCoralFailure { code, message })
            }
            coral_run::InvocationOutcome::RunTerminated => W::RunTerminated(wire::ActivityEmpty {}),
            coral_run::InvocationOutcome::Indeterminate => W::Indeterminate(wire::ActivityEmpty {}),
        }
    });
    Ok(Response::new(wire::InspectInvocationResponse {
        invocation: Some(summary),
        arguments_json: detail.arguments.to_string(),
        outcome,
        requests: detail
            .requests
            .into_iter()
            .map(|request| request_reference(&workspace, request))
            .collect(),
    }))
}
