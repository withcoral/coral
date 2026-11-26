//! The Invocations a Run stream finds on their rows when it reopens on
//! another loop: a `pending` one re-enters its gates, which reread its
//! Approval Request and its account wait, so it parks again or meets the
//! enforcement point; a `dispatching` Perform keeps its body, which still
//! runs on the Run Executor. Nothing is admitted again.

use std::sync::Arc;

use coral_policy::ToolAccessState;
use coral_provider::ResolvedInvocationTool;
use tracing::Instrument as _;

use super::{
    AdmittedInvocation, DispatchSlot, InvocationHostError, InvocationProgress, OpenChild,
    PERFORMED_PROTOCOL, PerformedCall, Permit, ToolBody, ToolInvoker, record_protocol,
};
use crate::invocation_repository::StreamInvocation;
use crate::trace::InvocationTrace;
use crate::{Error, builtin};

/// What the early check of a resumed Invocation reads: its gates and the
/// enforcement point read the Tool's policy again.
const RECHECKED: ToolAccessState = ToolAccessState::Enabled;

impl ToolInvoker {
    /// The Invocation `row` records, with the Tool `target` it retained,
    /// as admission made it: a child of `body` while that body runs.
    fn reopened(
        &self,
        row: &StreamInvocation,
        target: ResolvedInvocationTool,
        body: Option<&Arc<ToolBody>>,
    ) -> (Arc<AdmittedInvocation>, InvocationTrace, Option<OpenChild>) {
        let child = body.and_then(ToolBody::open_child);
        let trace = OpenChild::in_parent_span(child.as_ref(), || {
            InvocationTrace::begin(&row.id, target.call_path(), target.provider_access().id())
        });
        trace.admitted(target.tool());
        let admitted = Arc::new(AdmittedInvocation {
            id: row.id.clone(),
            workspace_id: self.workspace_id.clone(),
            run_id: self.run_id.clone(),
            target,
            call_path: row.call_path.clone(),
            arguments_json: row.arguments_json.clone(),
            // Admission recorded it; nothing after admission reads it.
            source_location: None,
            parent_invocation_id: row.parent.clone(),
            admitted_at_unix_nanos: row.admitted_at_unix_nanos,
            request_number: Some(row.request_number),
            span: trace.span().clone(),
        });
        (admitted, trace, child)
    }

    /// A `dispatching` Perform whose body runs on the Run Executor still:
    /// its `Complete` ends it as one in process would.
    pub(crate) fn resumed_perform(
        &self,
        row: &StreamInvocation,
        target: ResolvedInvocationTool,
        body: Option<&Arc<ToolBody>>,
    ) -> PerformedCall {
        let (admitted, trace, child) = self.reopened(row, target, body);
        record_protocol(&admitted.span, PERFORMED_PROTOCOL);
        trace.sending();
        PerformedCall {
            invoker: self.clone(),
            body: ToolBody::new(admitted.id.clone(), admitted.span.clone()),
            slot: DispatchSlot::new(self.run_loop.clone(), &admitted),
            admitted,
            progress: InvocationProgress {
                dispatch_started: true,
                ..InvocationProgress::default()
            },
            trace,
            open_in_parent: child,
        }
    }

    /// A `pending` Invoke through its gates to its end.
    pub(crate) async fn resume_invoke(
        &self,
        row: &StreamInvocation,
        target: ResolvedInvocationTool,
        body: Option<&Arc<ToolBody>>,
    ) -> Result<String, InvocationHostError> {
        // The child stays open until the call ends.
        let (admitted, trace, _child) = self.reopened(row, target, body);
        let ended = async {
            let arguments = arguments(&admitted)?;
            let builtin = match builtin::BuiltinCall::parse(&admitted.target, &arguments) {
                Some(Ok(call)) => Some(call),
                Some(Err(failure)) => {
                    let progress = InvocationProgress::default();
                    return Err(self.finish_failure(&admitted, &progress, failure).await?);
                }
                None => None,
            };
            self.invoke_admitted(&admitted, &arguments, RECHECKED, builtin, &trace)
                .await
        }
        .instrument(trace.span().clone())
        .await;
        trace.end(ended.as_ref().map(drop));
        ended
    }

    /// A `pending` Perform through its gates to its permit.
    pub(crate) async fn resume_perform(
        &self,
        row: &StreamInvocation,
        target: ResolvedInvocationTool,
        body: Option<&Arc<ToolBody>>,
    ) -> Result<PerformedCall, InvocationHostError> {
        let (admitted, trace, child) = self.reopened(row, target, body);
        let permitted = async {
            let arguments = arguments(&admitted)?;
            let Permit { slot, progress, .. } =
                self.permit(&admitted, &arguments, RECHECKED, None).await?;
            if self.run_loop.is_stopping() {
                self.end_before_send(&admitted).await?;
                return Err(InvocationHostError::Fatal(Error::ProgramHostTerminated));
            }
            trace.sending();
            Ok((slot, progress))
        }
        .instrument(trace.span().clone())
        .await;
        match permitted {
            Ok((slot, progress)) => Ok(PerformedCall {
                invoker: self.clone(),
                body: ToolBody::new(admitted.id.clone(), admitted.span.clone()),
                admitted,
                progress,
                slot,
                trace,
                open_in_parent: child,
            }),
            Err(error) => {
                trace.end(Err(&error));
                Err(error)
            }
        }
    }
}

/// The arguments admission recorded, which it had checked.
fn arguments(admitted: &AdmittedInvocation) -> Result<serde_json::Value, InvocationHostError> {
    serde_json::from_str(&admitted.arguments_json).map_err(|error| {
        InvocationHostError::Fatal(Error::CorruptData(format!(
            "Invocation has invalid arguments JSON: {error}"
        )))
    })
}
