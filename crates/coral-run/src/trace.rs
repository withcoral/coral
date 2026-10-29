//! The Run's spans. Acceptance begins the Run Trace with a short
//! `coral.run.accept` root linked to the accepting request; every later
//! phase is a new child of that stored accept span, on whichever Coral
//! Instance runs it, and no span of an earlier phase is reopened.

use coral_telemetry::RunTrace;
use coral_telemetry::safe;
use tracing::field::Empty;

use crate::{RunId, RunTerminalOutcome};

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
    tracing::info_span!(parent: None, "coral.run.accept", coral.run.id = Empty)
}

/// A new span for `phase` of the Run `run_id`, a child of the stored accept
/// span. Without a stored Run Trace it is a new root.
pub(crate) fn phase_span(trace: Option<&RunTrace>, run_id: &RunId, phase: Phase) -> tracing::Span {
    let span = match phase {
        Phase::Start => tracing::info_span!(
            parent: None,
            "coral.run.start",
            coral.run.id = safe::id(&run_id),
            coral.run.status = Empty,
            error.type = Empty,
        ),
        Phase::Execute => {
            tracing::info_span!(parent: None, "coral.run.execute", coral.run.id = safe::id(&run_id))
        }
        Phase::Complete => tracing::info_span!(
            parent: None,
            "coral.run.complete",
            coral.run.id = safe::id(&run_id),
            coral.run.status = Empty,
            error.type = Empty,
        ),
        Phase::Terminate => tracing::info_span!(
            parent: None,
            "coral.run.terminate",
            coral.run.id = safe::id(&run_id),
            coral.run.status = Empty,
            error.type = Empty,
        ),
        Phase::Recover => tracing::info_span!(
            parent: None,
            "coral.run.recover",
            coral.run.id = safe::id(&run_id),
            coral.run.status = Empty,
            error.type = Empty,
        ),
    };
    if let Some(trace) = trace {
        trace.adopt(&span);
    }
    span
}

/// A claim ID as a span field. Coral mints each as a UUID; one that does
/// not parse is left off.
pub(crate) fn claim(claim_id: &str) -> Option<uuid::Uuid> {
    uuid::Uuid::parse_str(claim_id).ok()
}

/// Records the terminal outcome the current phase wrote: its state, and for
/// a failure, a timeout or an interruption, its failure code as the error.
/// A success or a cancellation is not an error.
pub(crate) fn record_run_end(outcome: &RunTerminalOutcome) {
    let span = tracing::Span::current();
    span.record(
        "coral.run.status",
        safe::kind(outcome.state().as_storage_str()),
    );
    if let Some(code) = outcome.failure_code() {
        coral_telemetry::record_failure(&span, code);
    }
}

/// The `coral.invocation` span of one admitted Tool Invocation of the Tool
/// at `call_path`, a Tool of `provider`, a child of the current span (the
/// Run's execute phase).
pub(crate) fn invocation_span(
    id: &crate::InvocationId,
    call_path: coral_provider::ResolvedCallPath<'_>,
    provider: &coral_provider::ProviderId,
) -> tracing::Span {
    tracing::info_span!(
        "coral.invocation",
        coral.invocation.id = safe::id(id),
        coral.tool.call_path = safe::id(&call_path),
        coral.provider.id = safe::id(provider),
        coral.provider.protocol = Empty,
        coral.invocation.dispatch_started = Empty,
        coral.invocation.result_kind = Empty,
        coral.invocation.failure.code = Empty,
        error.type = Empty,
    )
}

/// One step of a Tool Invocation on its way to dispatch.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Step {
    Plan,
    /// Readying the Account's credentials, including a token refresh.
    Credentials,
    DispatchFence,
    /// Opening the credentials the Dispatch Fence judged current.
    Authorize,
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
        Step::Authorize => tracing::info_span!(parent: invocation, "coral.invocation.authorize"),
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
    tracing::info_span!(parent: invocation, "coral.invocation.gate", coral.invocation.gate.kind = safe::kind(kind))
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
        span.record(
            "coral.invocation.result_kind",
            safe::kind(kind.as_storage_str()),
        );
    }
    match kind {
        Some(InvocationResultKind::UpstreamOutcome | InvocationResultKind::RunTerminated) => {}
        Some(InvocationResultKind::CoralFailure) => {
            let code = failure_code.unwrap_or("coral_failure");
            span.record("coral.invocation.failure.code", safe::kind(code));
            coral_telemetry::record_failure(span, code);
        }
        Some(InvocationResultKind::Indeterminate) => {
            coral_telemetry::record_failure(span, "indeterminate");
        }
        None => coral_telemetry::record_failure(span, "host_defect"),
    }
}

/// Links the current span, a request about a Run its caller may see, to the
/// Run's Run Trace. Call it only once the Run is found visible.
pub(crate) fn link_request(trace: Option<&RunTrace>) {
    if let Some(trace) = trace {
        trace.link(&tracing::Span::current());
    }
}
