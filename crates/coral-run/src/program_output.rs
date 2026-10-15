//! Run-bound durable Program Emission and Program Log appends.

use std::sync::Arc;

use crate::run_repository::NewEvent;
use coral_workspace::WorkspaceId;

use crate::engine::Clock;
use crate::{
    Error, PROGRAM_LOG_MESSAGE_MAX_BYTES, ProgramLogLevel, ProgramValue, RunId, SourceLocation,
    run_repository,
};

#[derive(Clone)]
pub(crate) struct ProgramOutputWriter {
    database: Arc<coral_db::Database>,
    run_id: RunId,
    workspace_id: WorkspaceId,
    claim_id: String,
    clock: Arc<dyn Clock>,
    limits: coral_value::ValueLimits,
}

impl ProgramOutputWriter {
    pub(crate) fn new(
        database: Arc<coral_db::Database>,
        run_id: RunId,
        workspace_id: WorkspaceId,
        claim_id: String,
        clock: Arc<dyn Clock>,
        limits: coral_value::ValueLimits,
    ) -> Self {
        Self {
            database,
            run_id,
            workspace_id,
            claim_id,
            clock,
            limits,
        }
    }

    /// Appends one Emission made at `location`. A Middleware's own Emission
    /// names its running execution, which the same transaction rechecks.
    pub(crate) async fn emit(
        &self,
        encoded_json: &str,
        location: Option<SourceLocation>,
        middleware_execution: Option<&str>,
    ) -> Result<(), ProgramOutputWriteError> {
        let value = match ProgramValue::with_limits(encoded_json.to_string(), self.limits) {
            Ok(value) => value,
            Err(Error::ProgramValueTooLarge) => {
                return Err(ProgramOutputWriteError::LimitExceeded);
            }
            Err(Error::ProgramValueJson(_) | Error::InvalidProgramValue) => {
                return Err(ProgramOutputWriteError::InvalidValue);
            }
            Err(error) => return Err(ProgramOutputWriteError::Fatal(error)),
        };
        self.append(
            Output::Emission(value, middleware_execution.map(str::to_owned)),
            location,
            None,
        )
        .await
        .map_err(output_error)
    }

    /// Appends exactly the bytes of an approved Emission request, checking the
    /// approval in the same transaction as the Emission.
    pub(crate) async fn emit_approved(
        &self,
        value: ProgramValue,
        location: Option<SourceLocation>,
        approval: &crate::approval::ApprovalId,
    ) -> Result<(), ProgramOutputWriteError> {
        self.append(Output::Emission(value, None), location, Some(approval))
            .await
            .map_err(output_error)
    }

    pub(crate) async fn log(
        &self,
        level: ProgramLogLevel,
        message: &str,
        location: Option<SourceLocation>,
    ) -> Result<(), ProgramOutputWriteError> {
        if message.len() > PROGRAM_LOG_MESSAGE_MAX_BYTES {
            return Err(ProgramOutputWriteError::LimitExceeded);
        }
        self.append(
            Output::Log {
                level,
                message: message.to_string(),
            },
            location,
            None,
        )
        .await
        .map_err(output_error)
    }

    async fn append(
        &self,
        output: Output,
        location: Option<SourceLocation>,
        approval: Option<&crate::approval::ApprovalId>,
    ) -> Result<(), Error> {
        let mut transaction = self.database.begin().await?;
        let Some(run) =
            run_repository::hold_run(&mut transaction, &self.workspace_id, &self.run_id).await?
        else {
            return Err(Error::RunEventPersistence(
                "Program Run no longer accepts output".to_string(),
            ));
        };
        let occurred_at_unix_nanos = self.clock.now_unix_nanos().map_err(Error::Clock)?;
        if !run.program_work_is_current(&self.claim_id, occurred_at_unix_nanos) {
            transaction.rollback().await?;
            return Err(Error::RunEventPersistence(
                "Program Run no longer accepts output".to_string(),
            ));
        }
        let occurred_at_unix_nanos = occurred_at_unix_nanos.max(run.accepted_at_unix_nanos);
        if let Output::Emission(_, Some(execution)) = &output
            && !crate::middleware_repository::is_running(
                &mut transaction,
                &self.workspace_id,
                &self.run_id,
                execution,
            )
            .await?
        {
            transaction.rollback().await?;
            return Err(Error::RunEventPersistence(
                "Middleware Emission has no running execution".to_string(),
            ));
        }
        if let (Some(approval), Output::Emission(value, _)) = (approval, &output) {
            crate::emission_approval::require_approved(
                &mut transaction,
                &run,
                approval,
                value,
                occurred_at_unix_nanos,
            )
            .await?;
        }
        let (bytes, event) = match &output {
            Output::Emission(value, middleware_execution) => (
                value.encoded_json().len(),
                NewEvent::Emitted(value, location.as_ref(), middleware_execution.as_deref()),
            ),
            Output::Log { level, message } => (
                message.len(),
                NewEvent::Logged(*level, message, location.as_ref()),
            ),
        };
        if !run.output_fits(bytes) {
            return Err(Error::ProgramValueTooLarge);
        }
        run_repository::advance_event_sequence(&mut transaction, &run, &self.claim_id, 1, bytes)
            .await?;
        run_repository::insert_event(
            &mut transaction,
            &run.workspace_id,
            &run.id,
            run.next_event_sequence,
            event,
            occurred_at_unix_nanos,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }
}

fn output_error(error: Error) -> ProgramOutputWriteError {
    match error {
        Error::ProgramValueTooLarge => ProgramOutputWriteError::LimitExceeded,
        Error::InvocationRejected(failure) => ProgramOutputWriteError::Rejected(failure),
        error => ProgramOutputWriteError::Fatal(error),
    }
}

enum Output {
    Emission(ProgramValue, Option<String>),
    Log {
        level: ProgramLogLevel,
        message: String,
    },
}

pub(crate) enum ProgramOutputWriteError {
    InvalidValue,
    LimitExceeded,
    /// A catchable Coral Failure, such as an approval that expired at the fence.
    Rejected(crate::CoralFailure),
    Fatal(Error),
}
