//! Run record row and the lifecycle-evidence checks applied while decoding it.

use coral_workspace::{Principal, PrincipalId, WorkspaceId};
use sqlx::FromRow;

use super::principal_kind_from_storage;
use crate::lifecycle::{RunRecord, RunTermination, RunTerminationKind};
use crate::{
    Error, FinalValue, ProgramValue, RunFailure, RunFailureCode, RunId, RunState,
    RunTerminalOutcome,
};

#[derive(Debug, FromRow)]
pub(super) struct RunRow {
    id: String,
    workspace_id: String,
    principal_id: String,
    principal_kind: String,
    state: String,
    accepted_at_unix_nanos: i64,
    deadline_at_unix_nanos: i64,
    worker_claim_id: Option<String>,
    worker_claim_expires_at_unix_nanos: Option<i64>,
    holder_instance_id: Option<String>,
    trace_parent: Option<String>,
    execution_claim_id: Option<String>,
    execution_started_at_unix_nanos: Option<i64>,
    termination_kind: Option<String>,
    termination_started_at_unix_nanos: Option<i64>,
    termination_deadline_at_unix_nanos: Option<i64>,
    shutdown_grace_nanos: i64,
    cancellation_requested_at_unix_nanos: Option<i64>,
    cancellation_principal_id: Option<String>,
    cancellation_principal_kind: Option<String>,
    terminal_at_unix_nanos: Option<i64>,
    failure_code: Option<String>,
    failure_message: Option<String>,
    failure_error_json: Option<String>,
    final_value_json: Option<String>,
    value_limits_json: String,
    accumulated_output_bytes: i64,
    chunk_sequence: i64,
    /// Read beside the row; absent under retention's hold of a terminal Run.
    #[sqlx(default)]
    last_event_sequence: Option<i64>,
}

impl RunFailureCode {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::ProgramRejected => "program_rejected",
            Self::InvalidProgramValue => "invalid_program_value",
            Self::ResourceLimitExceeded => "resource_limit_exceeded",
            Self::MemoryLimitExceeded => "memory_limit_exceeded",
            Self::ProgramExecutionFailed => "program_execution_failed",
            Self::RunAuthorityLost => "run_authority_lost",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "program_rejected" => Ok(Self::ProgramRejected),
            "invalid_program_value" => Ok(Self::InvalidProgramValue),
            "resource_limit_exceeded" => Ok(Self::ResourceLimitExceeded),
            "memory_limit_exceeded" => Ok(Self::MemoryLimitExceeded),
            "program_execution_failed" => Ok(Self::ProgramExecutionFailed),
            "run_authority_lost" => Ok(Self::RunAuthorityLost),
            _ => Err(Error::CorruptData(format!(
                "Run has unrecognized failure code '{value}'"
            ))),
        }
    }
}

impl RunTerminationKind {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Canceled => "canceled",
            Self::TimedOut => "timed_out",
            Self::Interrupted => "interrupted",
            Self::MemoryLimitExceeded => "memory_limit_exceeded",
        }
    }

    fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "canceled" => Ok(Self::Canceled),
            "timed_out" => Ok(Self::TimedOut),
            "interrupted" => Ok(Self::Interrupted),
            "memory_limit_exceeded" => Ok(Self::MemoryLimitExceeded),
            _ => Err(Error::CorruptData(format!(
                "Run has unrecognized termination kind '{value}'"
            ))),
        }
    }
}

impl RunRow {
    #[expect(
        clippy::too_many_lines,
        reason = "one decode checks every lifecycle column of the Run row together"
    )]
    pub(super) fn decode(self) -> Result<RunRecord, Error> {
        let id = RunId::parse(&self.id)
            .map_err(|_error| Error::CorruptData("Run has invalid identity".to_string()))?;
        let workspace_id = WorkspaceId::parse(&self.workspace_id)
            .map_err(|_error| Error::CorruptData("Run has invalid Workspace".to_string()))?;
        let principal_id = PrincipalId::parse(&self.principal_id)
            .map_err(|_error| Error::CorruptData("Run has invalid Principal".to_string()))?;
        let principal_kind = principal_kind_from_storage(&self.principal_kind)?;
        let principal = Principal::from_parts(principal_id, principal_kind);
        let state = RunState::from_storage_str(&self.state)?;
        let chunk_sequence = u64::try_from(self.chunk_sequence)
            .map_err(|_error| Error::CorruptData("Run has invalid Ledger head".to_string()))?;
        let last_event_sequence = self
            .last_event_sequence
            .map(|last| u64::try_from(last).ok().filter(|last| *last >= 1))
            .map(|last| {
                last.ok_or_else(|| Error::CorruptData("Run has invalid Event sequence".to_string()))
            })
            .transpose()?;

        if self.deadline_at_unix_nanos < self.accepted_at_unix_nanos
            || self.shutdown_grace_nanos <= 0
            || self.worker_claim_id.is_some() != self.worker_claim_expires_at_unix_nanos.is_some()
            || self.execution_claim_id.is_some() != self.execution_started_at_unix_nanos.is_some()
            || self
                .execution_started_at_unix_nanos
                .is_some_and(|started| started < self.accepted_at_unix_nanos)
            || self.cancellation_requested_at_unix_nanos.is_some()
                != self.cancellation_principal_id.is_some()
            || self.cancellation_requested_at_unix_nanos.is_some()
                != self.cancellation_principal_kind.is_some()
        {
            return Err(Error::CorruptData(
                "Run has inconsistent lifecycle evidence".to_string(),
            ));
        }
        if let Some(expires_at) = self.worker_claim_expires_at_unix_nanos
            && expires_at < self.accepted_at_unix_nanos
        {
            return Err(Error::CorruptData(
                "Run has invalid worker claim expiry".to_string(),
            ));
        }
        let termination = decode_termination(&self, state)?;
        // An earlier release released the worker claim when it recorded an
        // interruption; this one keeps it until the terminal chunk.
        let released_on_interruption = self.worker_claim_id.is_none()
            && termination
                .is_some_and(|termination| termination.kind == RunTerminationKind::Interrupted);
        if state == RunState::Running
            && self.execution_claim_id.is_some()
            && self.worker_claim_id != self.execution_claim_id
            && !released_on_interruption
        {
            return Err(Error::CorruptData(
                "running execution has no valid worker claim".to_string(),
            ));
        }
        if state.is_terminal()
            && (self.worker_claim_id.is_some()
                || termination.is_some_and(|value| value.kind.outcome().state() != state))
        {
            return Err(Error::CorruptData(
                "terminal Run has active or mismatched execution evidence".to_string(),
            ));
        }

        let value_limits: coral_value::ValueLimits = serde_json::from_str(&self.value_limits_json)
            .map_err(|error| Error::CorruptData(format!("invalid Run limits: {error}")))?;
        let accumulated_output_bytes = usize::try_from(self.accumulated_output_bytes)
            .ok()
            .filter(|total| *total <= value_limits.run_output_bytes())
            .ok_or_else(|| Error::CorruptData("Run has invalid accumulated output bytes".into()))?;
        let terminal_outcome = decode_terminal(
            state,
            self.terminal_at_unix_nanos,
            self.failure_code.as_deref(),
            self.failure_message,
            self.failure_error_json,
            self.final_value_json,
            value_limits,
        )?;
        Ok(RunRecord {
            id,
            workspace_id,
            principal,
            state,
            accepted_at_unix_nanos: self.accepted_at_unix_nanos,
            deadline_at_unix_nanos: self.deadline_at_unix_nanos,
            worker_claim_id: self.worker_claim_id,
            worker_claim_expires_at_unix_nanos: self.worker_claim_expires_at_unix_nanos,
            holder_instance_id: self
                .holder_instance_id
                .map(crate::CoralInstanceId::from_stored),
            execution_claim_id: self.execution_claim_id,
            execution_started_at_unix_nanos: self.execution_started_at_unix_nanos,
            termination,
            shutdown_grace_nanos: self.shutdown_grace_nanos,
            chunk_sequence,
            last_event_sequence,
            accumulated_output_bytes,
            terminal_outcome,
            terminal_at_unix_nanos: self.terminal_at_unix_nanos,
            value_limits,
            trace: decode_trace(self.trace_parent.as_deref()),
        })
    }
}

/// A stored Run Trace that does not parse is lost telemetry, never a corrupt
/// Run: the Run's phases then start a new trace.
fn decode_trace(stored: Option<&str>) -> Option<coral_telemetry::RunTrace> {
    let stored = stored?;
    coral_telemetry::RunTrace::parse(stored)
        .inspect_err(|error| tracing::warn!(%error, "Run row has an invalid Run Trace"))
        .ok()
}

fn decode_termination(row: &RunRow, state: RunState) -> Result<Option<RunTermination>, Error> {
    if let (Some(id), Some(principal_kind)) = (
        row.cancellation_principal_id.as_deref(),
        row.cancellation_principal_kind.as_deref(),
    ) {
        PrincipalId::parse(id).map_err(|_error| {
            Error::CorruptData("Run has invalid cancellation Principal".to_string())
        })?;
        principal_kind_from_storage(principal_kind)?;
    }
    let fields_present = [
        row.termination_kind.is_some(),
        row.termination_started_at_unix_nanos.is_some(),
        row.termination_deadline_at_unix_nanos.is_some(),
    ];
    if fields_present.iter().all(|present| !present) {
        if state == RunState::Running && row.cancellation_requested_at_unix_nanos.is_some() {
            return Err(Error::CorruptData(
                "running cancellation has no termination evidence".to_string(),
            ));
        }
        return Ok(None);
    }
    if !fields_present.iter().all(|present| *present) {
        return Err(Error::CorruptData(
            "Run has partial termination evidence".to_string(),
        ));
    }
    let kind = RunTerminationKind::from_storage_str(
        row.termination_kind
            .as_deref()
            .ok_or_else(|| Error::CorruptData("Run termination has no kind".to_string()))?,
    )?;
    let started_at_unix_nanos = row
        .termination_started_at_unix_nanos
        .ok_or_else(|| Error::CorruptData("Run termination has no start".to_string()))?;
    let deadline_at_unix_nanos = row
        .termination_deadline_at_unix_nanos
        .ok_or_else(|| Error::CorruptData("Run termination has no deadline".to_string()))?;
    if started_at_unix_nanos < row.accepted_at_unix_nanos
        || deadline_at_unix_nanos < started_at_unix_nanos
        || (kind == RunTerminationKind::Canceled)
            != row.cancellation_requested_at_unix_nanos.is_some()
    {
        return Err(Error::CorruptData(
            "Run has invalid termination evidence".to_string(),
        ));
    }
    Ok(Some(RunTermination {
        kind,
        started_at_unix_nanos,
        deadline_at_unix_nanos,
    }))
}

fn decode_terminal(
    state: RunState,
    terminal_at_unix_nanos: Option<i64>,
    failure_code: Option<&str>,
    failure_message: Option<String>,
    failure_error_json: Option<String>,
    final_value_json: Option<String>,
    limits: coral_value::ValueLimits,
) -> Result<Option<RunTerminalOutcome>, Error> {
    if state == RunState::Running {
        if terminal_at_unix_nanos.is_some()
            || failure_code.is_some()
            || failure_message.is_some()
            || failure_error_json.is_some()
            || final_value_json.is_some()
        {
            return Err(Error::CorruptData(
                "running Run has terminal fields".to_string(),
            ));
        }
        return Ok(None);
    }
    if terminal_at_unix_nanos.is_none() {
        return Err(Error::CorruptData(
            "terminal Run has no terminal timestamp".to_string(),
        ));
    }
    let outcome = match state {
        RunState::Running => unreachable!("handled above"),
        RunState::Succeeded => {
            if failure_code.is_some() || failure_message.is_some() || failure_error_json.is_some() {
                return Err(Error::CorruptData(
                    "successful Run has failure evidence".to_string(),
                ));
            }
            let final_value = match final_value_json {
                None => FinalValue::Absent,
                Some(encoded) => {
                    FinalValue::Present(ProgramValue::from_persisted_json(encoded, limits)?)
                }
            };
            RunTerminalOutcome::Succeeded { final_value }
        }
        RunState::Failed => {
            if final_value_json.is_some() {
                return Err(Error::CorruptData(
                    "failed Run has a Final Value".to_string(),
                ));
            }
            let code = failure_code
                .ok_or_else(|| Error::CorruptData("failed Run has no code".to_string()))
                .and_then(RunFailureCode::from_storage_str)?;
            let message = failure_message
                .ok_or_else(|| Error::CorruptData("failed Run has no message".to_string()))?;
            let mut failure = RunFailure::new(code, message);
            if let Some(json) = failure_error_json {
                let error = serde_json::from_str(&json).map_err(|_error| {
                    Error::CorruptData("Run has invalid Program error".to_string())
                })?;
                failure = failure.with_error(error);
            }
            RunTerminalOutcome::Failed(failure)
        }
        RunState::Canceled => RunTerminalOutcome::Canceled,
        RunState::TimedOut => RunTerminalOutcome::TimedOut,
        RunState::Interrupted => RunTerminalOutcome::Interrupted,
    };
    Ok(Some(outcome))
}
