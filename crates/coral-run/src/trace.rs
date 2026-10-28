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
