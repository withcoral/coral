//! The Run's spans. Acceptance begins the Run Trace with a short
//! `coral.run.accept` root linked to the accepting request; every later
//! phase is a new child of that stored accept span, on whichever Coral
//! Instance runs it, and no span of an earlier phase is reopened.

use coral_telemetry::RunTrace;
use tracing::field::Empty;

use crate::{RunId, RunState};

/// One phase of a Run under its Run Owner.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Phase {
    /// The start mark: the owner's first chunk.
    Start,
    /// The Program, from start until it returns or throws.
    Execute,
    /// The terminal write after the Program ended, or an interruption.
    Complete,
    /// Settling a recorded termination: a cancellation, the deadline or a
    /// shutdown.
    Terminate,
    /// A recovered owner's one chunk.
    Recover,
}

/// The `coral.run.accept` span, not yet entered. Its context becomes the
/// Run Trace (`RunTrace::begin`).
pub(crate) fn accept_span() -> tracing::Span {
    tracing::info_span!(parent: None, "coral.run.accept", run.id = Empty)
}

/// A new span for `phase` of the Run `run_id`, a child of the stored accept
/// span. Without a stored Run Trace it is a new root.
pub(crate) fn phase_span(trace: Option<&RunTrace>, run_id: &RunId, phase: Phase) -> tracing::Span {
    let span = match phase {
        Phase::Start => tracing::info_span!(
            parent: None,
            "coral.run.start",
            run.id = %run_id,
            run.status = Empty,
            status = Empty,
            error.type = Empty,
            exception.message = Empty,
        ),
        Phase::Execute => {
            tracing::info_span!(parent: None, "coral.run.execute", run.id = %run_id)
        }
        Phase::Complete => tracing::info_span!(
            parent: None,
            "coral.run.complete",
            run.id = %run_id,
            run.status = Empty,
            status = Empty,
            error.type = Empty,
            exception.message = Empty,
        ),
        Phase::Terminate => tracing::info_span!(
            parent: None,
            "coral.run.terminate",
            run.id = %run_id,
            run.status = Empty,
            status = Empty,
            error.type = Empty,
            exception.message = Empty,
        ),
        Phase::Recover => tracing::info_span!(
            parent: None,
            "coral.run.recover",
            run.id = %run_id,
            run.status = Empty,
            status = Empty,
            error.type = Empty,
            exception.message = Empty,
        ),
    };
    if let Some(trace) = trace {
        trace.adopt(&span);
    }
    span
}

/// Records the terminal state the current phase wrote. `failed`,
/// `timed_out` and `interrupted` mark it as an error; `succeeded` and
/// `canceled` do not.
pub(crate) fn record_run_end(state: RunState) {
    let span = tracing::Span::current();
    span.record("run.status", state.as_storage_str());
    let message = match state {
        RunState::Failed => "Run failed",
        RunState::TimedOut => "Run timed out",
        RunState::Interrupted => "Run interrupted",
        RunState::Running | RunState::Succeeded | RunState::Canceled => return,
    };
    coral_telemetry::record_failure(&span, state.as_storage_str(), message);
}

/// The `coral.invocation` span of one admitted Tool Invocation, a child of
/// the current span (the Run's execute phase).
pub(crate) fn invocation_span(
    id: &crate::InvocationId,
    call_path: &str,
    provider_id: &str,
) -> tracing::Span {
    tracing::info_span!(
        "coral.invocation",
        invocation.id = %id,
        tool.call_path = call_path,
        provider.id = provider_id,
        provider.protocol = Empty,
        dispatch.started = Empty,
        invocation.result_kind = Empty,
        failure.code = Empty,
        status = Empty,
        error.type = Empty,
        exception.message = Empty,
    )
}

/// One step of a Tool Invocation on its way to dispatch.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Step {
    Plan,
    Credentials,
    DispatchFence,
    Persist,
}

/// A gate a Tool Invocation passes before its Dispatch Fence.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Gate {
    /// The held recheck of the Workspace, Principal, Provider and policy.
    Authority,
    /// Selecting the Account, including a wait on a connection request.
    Account,
    /// The Approval Request, including the wait for its decision.
    Approval,
}

/// A span for `step` of the Invocation whose span is `invocation`.
pub(crate) fn step_span(invocation: &tracing::Span, step: Step) -> tracing::Span {
    match step {
        Step::Plan => tracing::info_span!(parent: invocation, "coral.invocation.plan"),
        Step::Credentials => {
            tracing::info_span!(parent: invocation, "coral.invocation.credentials")
        }
        Step::DispatchFence => {
            tracing::info_span!(parent: invocation, "coral.invocation.dispatch_fence")
        }
        Step::Persist => tracing::info_span!(parent: invocation, "coral.invocation.persist"),
    }
}

/// A span for `gate` of the Invocation whose span is `invocation`.
pub(crate) fn gate_span(invocation: &tracing::Span, gate: Gate) -> tracing::Span {
    let kind = match gate {
        Gate::Authority => "authority",
        Gate::Account => "account",
        Gate::Approval => "approval",
    };
    tracing::info_span!(parent: invocation, "coral.invocation.gate", gate.kind = kind)
}

/// Records how a Tool Invocation ended: `kind`, or a host defect for
/// `None`. A complete Upstream Outcome is never an error, whatever its
/// status; a Coral Failure, `indeterminate` and a host defect are.
/// `run_terminated` is not.
pub(crate) fn record_invocation_end(
    span: &tracing::Span,
    kind: Option<crate::InvocationResultKind>,
    failure_code: Option<&'static str>,
) {
    use crate::InvocationResultKind;
    if let Some(kind) = kind {
        span.record("invocation.result_kind", kind.as_storage_str());
    }
    match kind {
        Some(InvocationResultKind::UpstreamOutcome | InvocationResultKind::RunTerminated) => {}
        Some(InvocationResultKind::CoralFailure) => {
            let code = failure_code.unwrap_or("coral_failure");
            span.record("failure.code", code);
            coral_telemetry::record_failure(span, code, "Tool Invocation ended in a Coral Failure");
        }
        Some(InvocationResultKind::Indeterminate) => coral_telemetry::record_failure(
            span,
            "indeterminate",
            "Tool Invocation ended without a known result after its dispatch",
        ),
        None => coral_telemetry::record_failure(
            span,
            "host_defect",
            "Tool Invocation ended on a host defect",
        ),
    }
}

/// Links the current span, a request about a Run its caller may see, to the
/// Run's Run Trace. Call it only once the Run is found visible.
pub(crate) fn link_request(trace: Option<&RunTrace>) {
    if let Some(trace) = trace {
        trace.link(&tracing::Span::current());
    }
}
