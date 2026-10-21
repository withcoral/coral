//! Run-bound Program Emission and Program Log requests to the Run Owner.

use crate::owner::OwnerHandle;
use crate::{Error, PROGRAM_LOG_MESSAGE_MAX_BYTES, ProgramLogLevel, ProgramValue, SourceLocation};

/// Validates Program output and asks the Run Owner to apply it. The owner
/// decides the Run Output Budget against its exact total and answers at
/// Applied; above the Applied cap it answers once the chunk is recorded.
pub(crate) struct ProgramOutputWriter {
    owner: OwnerHandle,
    limits: coral_value::ValueLimits,
}

impl ProgramOutputWriter {
    pub(crate) const fn new(owner: OwnerHandle, limits: coral_value::ValueLimits) -> Self {
        Self { owner, limits }
    }

    pub(crate) async fn emit(
        &self,
        encoded_json: &str,
        location: Option<SourceLocation>,
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
        self.owner.emit(value, location).await.map_err(output_error)
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
        self.owner
            .log(level, message.to_string(), location)
            .await
            .map_err(output_error)
    }
}

fn output_error(error: Error) -> ProgramOutputWriteError {
    match error {
        Error::ProgramValueTooLarge => ProgramOutputWriteError::LimitExceeded,
        error => ProgramOutputWriteError::Fatal(error),
    }
}

pub(crate) enum ProgramOutputWriteError {
    InvalidValue,
    LimitExceeded,
    Fatal(Error),
}
