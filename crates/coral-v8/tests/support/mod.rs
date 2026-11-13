//! What the tests' Program hosts share.

use std::sync::Arc;

use coral_v8::{
    AuthoredLocation, PermitFuture, ProgramHost, ToolBodyOutcome, ToolCalls, ToolCompletion,
    ToolFuture, ToolHostError,
};

/// Permits a performed Tool call at once, and completes it with what its body
/// did, as a host that records nothing would: the body's value, or the
/// rejection its caller reads. The body's calls reach `body_calls`.
pub(crate) fn permit_at_once(body_calls: Arc<dyn ToolCalls>) -> PermitFuture {
    Box::pin(async { Ok(Box::new(AtOnce(body_calls)) as Box<dyn ToolCompletion>) })
}

/// The calls a body makes, as `host` makes the Program's own.
pub(crate) fn host_calls<H: ProgramHost + 'static>(host: Arc<H>) -> Arc<dyn ToolCalls> {
    Arc::new(HostCalls(host))
}

struct HostCalls<H>(Arc<H>);

impl<H: ProgramHost + 'static> ToolCalls for HostCalls<H> {
    fn invoke(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<AuthoredLocation>,
    ) -> ToolFuture {
        self.0.clone().invoke(call_path, arguments_json, location)
    }

    fn perform(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<AuthoredLocation>,
    ) -> PermitFuture {
        self.0.clone().perform(call_path, arguments_json, location)
    }
}

struct AtOnce(Arc<dyn ToolCalls>);

impl ToolCompletion for AtOnce {
    fn body_calls(&self) -> Arc<dyn ToolCalls> {
        self.0.clone()
    }

    fn complete(self: Box<Self>, outcome: ToolBodyOutcome) -> ToolFuture {
        let rejected = |code: &'static str, message| ToolHostError::Rejected {
            code: code.into(),
            message,
            trace_id: None,
            suggestions: Vec::new(),
        };
        let completed = match outcome {
            ToolBodyOutcome::Returned(json) => Ok(json),
            ToolBodyOutcome::NotData { field, reason } => Err(rejected(
                "invalid_tool_result",
                format!("Tool result is not a JSON value at `{field}`: {reason}").into(),
            )),
            ToolBodyOutcome::Threw { message, .. } => Err(rejected("tool_failed", message.into())),
        };
        Box::pin(async { completed })
    }
}
