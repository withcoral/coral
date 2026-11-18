//! A reopened Run stream's Program host takes back the Invocations its rows
//! record (ADR 0027, Reconnect): each one still open answers its request as
//! it would have on the stream it was made on.

use std::collections::HashMap;
use std::sync::Arc;

use coral_catalog::ExecutionBinding;
use coral_provider::ResolvedInvocationTool;
use tracing::Instrument as _;
use tracing::instrument::WithSubscriber as _;

use super::{RunProgramHost, RunToolCompletion, StopPanickingInvocation};
use crate::InvocationId;
use crate::invocation::ToolBody;
use crate::invocation_lifecycle::InvocationState;
use crate::invocation_repository::StreamInvocation;

/// What the Program gets back for one request whose Invocation is open.
pub(crate) enum Resumed {
    /// A Perform whose body runs: what completes it.
    Performed(u64, Box<dyn coral_v8::ToolCompletion>),
    /// An Invoke that waits for its gates: its answer.
    Answer(u64, coral_v8::ToolFuture),
    /// A Perform that waits for its gates: its permit.
    Permit(u64, coral_v8::PermitFuture),
}

impl RunProgramHost {
    /// Takes back each open Invocation of `invocations` with the Tool it
    /// retained, in admission order, so a body's call follows its body.
    pub(crate) fn resume(
        self: &Arc<Self>,
        invocations: Vec<(StreamInvocation, Option<ResolvedInvocationTool>)>,
    ) -> Vec<Resumed> {
        let mut bodies: HashMap<InvocationId, Arc<ToolBody>> = HashMap::new();
        let mut resumed = Vec::new();
        for (row, target) in invocations {
            let Some(target) = target else {
                continue;
            };
            let number = row.request_number;
            let body = row
                .parent
                .as_ref()
                .and_then(|parent| bodies.get(parent))
                .cloned();
            let performs = matches!(target.tool().execution_binding(), ExecutionBinding::Code(_));
            match (row.state, performs) {
                (InvocationState::Dispatching, true) => {
                    let performed = self.invoker.resumed_perform(&row, target, body.as_ref());
                    bodies.insert(row.id.clone(), performed.body());
                    resumed.push(Resumed::Performed(
                        number,
                        RunToolCompletion::boxed(self, performed),
                    ));
                }
                (InvocationState::Pending, true) => {
                    resumed.push(Resumed::Permit(
                        number,
                        self.resume_perform(row, target, body),
                    ));
                }
                (InvocationState::Pending, false) => {
                    resumed.push(Resumed::Answer(
                        number,
                        self.resume_invoke(row, target, body),
                    ));
                }
                // The reopen settled every other dispatch.
                (InvocationState::Dispatching | InvocationState::Ended, _) => {}
            }
        }
        resumed
    }

    fn resume_invoke(
        self: &Arc<Self>,
        row: StreamInvocation,
        target: ResolvedInvocationTool,
        body: Option<Arc<ToolBody>>,
    ) -> coral_v8::ToolFuture {
        let host = self.clone();
        let invocation = self.runtime.spawn(
            async move {
                let _panic_guard = StopPanickingInvocation(host.stop.clone());
                host.invoker
                    .resume_invoke(&row, target, body.as_ref())
                    .await
                    .map_err(|error| host.host_error(error))
            }
            .instrument(self.span.clone())
            .with_current_subscriber(),
        );
        Box::pin(async move {
            invocation
                .await
                .unwrap_or(Err(coral_v8::ToolHostError::Fatal))
        })
    }

    fn resume_perform(
        self: &Arc<Self>,
        row: StreamInvocation,
        target: ResolvedInvocationTool,
        body: Option<Arc<ToolBody>>,
    ) -> coral_v8::PermitFuture {
        let host = self.clone();
        let permit = self.runtime.spawn(
            async move {
                let _panic_guard = StopPanickingInvocation(host.stop.clone());
                match host
                    .invoker
                    .resume_perform(&row, target, body.as_ref())
                    .await
                {
                    Ok(performed) => Ok(RunToolCompletion::boxed(&host, performed)),
                    Err(error) => Err(host.host_error(error)),
                }
            }
            .instrument(self.span.clone())
            .with_current_subscriber(),
        );
        Box::pin(async move { permit.await.unwrap_or(Err(coral_v8::ToolHostError::Fatal)) })
    }
}
