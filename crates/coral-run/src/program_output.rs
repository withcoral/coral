//! Run-bound durable Program Emission and Program Log appends.

use std::sync::Arc;

use coral_workspace::WorkspaceId;

use crate::engine::Clock;
use crate::run_repository::{Chunk, NewEvent};
use crate::{
    Error, PROGRAM_LOG_MESSAGE_MAX_BYTES, ProgramLogLevel, ProgramValue, RunId, run_repository,
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

    pub(crate) async fn emit(&self, encoded_json: &str) -> Result<(), ProgramOutputWriteError> {
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
        self.append(Output::Emission(value))
            .await
            .map_err(output_error)
    }

    pub(crate) async fn log(
        &self,
        level: ProgramLogLevel,
        message: &str,
    ) -> Result<(), ProgramOutputWriteError> {
        if message.len() > PROGRAM_LOG_MESSAGE_MAX_BYTES {
            return Err(ProgramOutputWriteError::LimitExceeded);
        }
        self.append(Output::Log {
            level,
            message: message.to_string(),
        })
        .await
        .map_err(output_error)
    }

    async fn append(&self, output: Output) -> Result<(), Error> {
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
        let (bytes, event) = match &output {
            Output::Emission(value) => (value.encoded_json().len(), NewEvent::Emitted(value)),
            Output::Log { level, message } => (message.len(), NewEvent::Logged(*level, message)),
        };
        if !run.output_fits(bytes) {
            return Err(Error::ProgramValueTooLarge);
        }
        run_repository::commit_chunk(
            &mut transaction,
            Chunk::new(&run, Some(&self.claim_id))
                .event(event, occurred_at_unix_nanos)
                .output_bytes(run.output_total(bytes)?),
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }
}

fn output_error(error: Error) -> ProgramOutputWriteError {
    match error {
        Error::ProgramValueTooLarge => ProgramOutputWriteError::LimitExceeded,
        error => ProgramOutputWriteError::Fatal(error),
    }
}

enum Output {
    Emission(ProgramValue),
    Log {
        level: ProgramLogLevel,
        message: String,
    },
}

pub(crate) enum ProgramOutputWriteError {
    InvalidValue,
    LimitExceeded,
    Fatal(Error),
}
