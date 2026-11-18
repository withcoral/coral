//! A reopened stream's Program reader takes the Run back from its rows
//! (ADR 0027, Reconnect): each open Invocation answers its request when it
//! ends, and each request the Run Executor still waits on that the
//! watermark covers is answered again from what Coral recorded, as the call
//! would have answered it. The Run Executor ignores an answer it has.

use std::collections::HashMap;
use std::sync::PoisonError;

use coral_run_executor::wire::v1::{Permitted, invocation_result};
use coral_v8::ToolHostError;

use super::{Performed, Reader, failure, send_outcome, send_result};
use crate::CoralFailure;
use crate::engine::host::Resumed;
use crate::invocation_repository::RecordedOutcome;
use crate::run_stream::Resume;
use crate::types::LinkedTool;

impl Reader {
    /// Starts after `resume`'s watermark, with its open Invocations taken
    /// back and its answered requests answered again.
    pub(super) fn resume(&mut self, resume: Resume, linked: &HashMap<String, LinkedTool>) {
        self.last = resume.watermark;
        self.covered = resume.watermark;
        // Each request's recorded end, `None` while open, and whether it is
        // a Perform, whose permit is never a value.
        let mut ends: HashMap<u64, (Option<RecordedOutcome>, bool)> = HashMap::new();
        let mut open = Vec::new();
        for (mut row, target) in resume.invocations {
            let performs = linked.contains_key(&row.call_path);
            ends.insert(row.request_number, (row.outcome.take(), performs));
            if target.is_some() {
                open.push((row, target));
            }
        }
        for resumed in self.host.resume(open) {
            match resumed {
                Resumed::Performed(number, completion) => {
                    self.performed
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(
                            number,
                            Performed {
                                body: completion.body_calls(),
                                completion,
                            },
                        );
                    send_outcome(
                        &self.link,
                        number,
                        0,
                        invocation_result::Outcome::Permitted(Permitted {}),
                    );
                }
                Resumed::Answer(number, answer) => self.answer(number, answer),
                Resumed::Permit(number, permit) => self.permit(number, permit),
            }
        }
        for awaited in resume.awaited {
            let (end, permits) = if awaited.perform == 0 {
                ends.remove(&awaited.number)
                    .map_or((None, false), |(end, performs)| (Some(end), performs))
            } else {
                (ends.remove(&awaited.perform).map(|(end, _)| end), false)
            };
            let outcome = match end {
                // Open: it answers when it ends.
                Some(None) => continue,
                Some(Some(outcome)) => outcome,
                // Refused before admission: nothing records the answer.
                None => RecordedOutcome::failed(CoralFailure::InvocationNotAdmitted),
            };
            let result = self.replayed(outcome);
            if permits {
                // A permitted Perform's own end answers its `Complete`.
                let outcome = match result {
                    Err(error @ ToolHostError::Rejected { .. }) => failure(error),
                    Ok(_) | Err(ToolHostError::Upstream { .. } | ToolHostError::Fatal) => {
                        failure(ToolHostError::Fatal)
                    }
                };
                send_outcome(&self.link, awaited.number, 0, outcome);
            } else {
                send_result(&self.link, awaited.number, result);
            }
        }
    }

    /// What the call that ended as `outcome` answered the Program.
    fn replayed(&self, outcome: RecordedOutcome) -> Result<String, ToolHostError> {
        let rejected = |failure: CoralFailure, message: String| ToolHostError::Rejected {
            code: failure.code().into(),
            message: message.into(),
            trace_id: coral_v8::ProgramHost::trace_id(&*self.host),
            suggestions: Vec::new(),
        };
        match outcome {
            RecordedOutcome::Returned(json) => Ok(json),
            RecordedOutcome::Rejected {
                rejection,
                outcome_json,
            } => Err(ToolHostError::Upstream {
                code: rejection.code().into(),
                message: rejection.message().into(),
                outcome_json,
            }),
            RecordedOutcome::Failed { failure, message } => Err(rejected(failure, message)),
            RecordedOutcome::Indeterminate => Err(rejected(
                CoralFailure::UpstreamIncomplete,
                CoralFailure::UpstreamIncomplete.message().to_owned(),
            )),
            // A terminated Run stops the Program.
            RecordedOutcome::RunTerminated => Err(ToolHostError::Fatal),
        }
    }
}
