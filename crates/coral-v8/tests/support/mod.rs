//! What the tests' Program hosts share.

use coral_v8::{PermitFuture, ToolBodyOutcome, ToolCompletion, ToolFuture, ToolHostError};

/// Permits a performed Tool call at once, and completes it with what its body
/// did, as a host that records nothing would: the body's value, or the
/// rejection its caller reads.
pub(crate) fn permit_at_once() -> PermitFuture {
    Box::pin(async { Ok(Box::new(AtOnce) as Box<dyn ToolCompletion>) })
}

struct AtOnce;

impl ToolCompletion for AtOnce {
    fn complete(self: Box<Self>, outcome: ToolBodyOutcome) -> ToolFuture {
        let rejected = |code, message| ToolHostError::Rejected {
            code,
            message,
            trace_id: None,
            suggestions: Vec::new(),
        };
        let completed = match outcome {
            ToolBodyOutcome::Returned(json) => Ok(json),
            ToolBodyOutcome::NotData => Err(rejected(
                "invalid_tool_result",
                "Tool result is not a JSON value".into(),
            )),
            ToolBodyOutcome::Threw { message, .. } => Err(rejected("tool_failed", message.into())),
        };
        Box::pin(async { completed })
    }
}
