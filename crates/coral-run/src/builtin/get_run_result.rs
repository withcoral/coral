//! Performs `coral.workspace.getRunResult`.

use coral_catalog::{GetRunResultArguments, GetRunResultResult, RunResultFailure, RunToolResult};

use super::{BuiltinHost, get_run_result_repository};
use crate::error::InputProblem;
use crate::{
    Error, FinalValue, Rejection, RunId, RunTerminalOutcome, invocation_repository, run_repository,
    tool_result_shape,
};

/// Built-in calls whose results a later Program has no use for.
const EXCLUDED_CALL_PATHS: [&str; 2] = ["coral.workspace.search", "coral.workspace.getRunResult"];

const RUN_ID_REASON: &str = "names no earlier Run of this Task; pass the run_id that exec returned";

/// `getRunResult`'s arguments in Coral's domain types.
#[derive(Clone)]
pub(crate) struct GetRunResult {
    run_id: RunId,
}

impl GetRunResult {
    /// The call for arguments the contract accepted.
    pub(super) fn new(arguments: &GetRunResultArguments) -> Result<Self, Rejection> {
        let run_id = RunId::parse(arguments.run_id()).map_err(|_error| unknown_run())?;
        Ok(Self { run_id })
    }

    /// Reads the Run, when it is the invoking Principal's, in the invoking
    /// Run's Workspace and, when the invoking Run has one, its Task.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        let mut snapshot = host.database.begin_read_snapshot().await?;
        let Some(run) = run_repository::find_visible_in_transaction(
            &mut snapshot,
            host.workspace_id,
            &self.run_id,
            host.principal,
            false,
        )
        .await?
        else {
            snapshot.rollback().await?;
            return Err(Error::InvocationRejected(unknown_run()));
        };
        let invoking_task =
            get_run_result_repository::task_of_run(&mut snapshot, host.workspace_id, host.run_id)
                .await?;
        if let Some(task) = invoking_task {
            let target_task = get_run_result_repository::task_of_run(
                &mut snapshot,
                host.workspace_id,
                &self.run_id,
            )
            .await?;
            if target_task.as_deref() != Some(task.as_str()) {
                snapshot.rollback().await?;
                return Err(Error::InvocationRejected(unknown_run()));
            }
        }
        let (final_value, failure, final_bytes) = match &run.terminal_outcome {
            Some(RunTerminalOutcome::Succeeded {
                final_value: FinalValue::Present(value),
            }) => (
                Some(value.as_json().clone()),
                None,
                value.encoded_json().len() as u64,
            ),
            Some(RunTerminalOutcome::Failed(failure)) => (
                None,
                Some(RunResultFailure {
                    code: failure.code().code().to_owned(),
                    message: failure.message().to_owned(),
                }),
                failure.message().len() as u64,
            ),
            _ => (None, None, 0),
        };
        let (retained, truncated) = invocation_repository::retained_results(
            &mut snapshot,
            host.workspace_id,
            &self.run_id,
            &EXCLUDED_CALL_PATHS,
            tool_result_shape::RESULTS_READ_MAX_BYTES.saturating_sub(final_bytes),
        )
        .await?;
        snapshot.rollback().await?;
        Ok(GetRunResultResult {
            run_id: self.run_id.as_str().to_owned(),
            state: run.state.as_storage_str().to_owned(),
            final_value,
            failure,
            tool_results: retained
                .into_iter()
                .map(|result| RunToolResult {
                    call_path: result.call_path,
                    arguments: result.arguments,
                    data: result
                        .tool_result
                        .get("data")
                        .cloned()
                        .unwrap_or(result.tool_result),
                })
                .collect(),
            truncated,
        }
        .into_tool_result())
    }
}

/// `invalid_input` naming `runId`, the same for a malformed id and for one
/// another Workspace, Principal or Task holds.
fn unknown_run() -> Rejection {
    Rejection::invalid_inputs(vec![InputProblem::new("runId", RUN_ID_REASON)])
}
