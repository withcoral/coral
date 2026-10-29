//! Run-bound Program Emission and Program Log requests to the Run Owner.

use coral_telemetry::metric::{self, OutputKind};

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
            Err(Error::ProgramValueTooLarge) => Err(ProgramOutputWriteError::LimitExceeded)
                .map_err(measured(OutputKind::Emission))?,
            Err(Error::ProgramValueJson(_) | Error::InvalidProgramValue) => {
                Err(ProgramOutputWriteError::InvalidValue)
                    .map_err(measured(OutputKind::Emission))?
            }
            Err(error) => return Err(ProgramOutputWriteError::Fatal(error)),
        };
        self.owner
            .emit(value, location)
            .await
            .map_err(output_error)
            .map_err(measured(OutputKind::Emission))?;
        metric::output_kept(OutputKind::Emission, byte_count(encoded_json));
        Ok(())
    }

    pub(crate) async fn log(
        &self,
        level: ProgramLogLevel,
        message: &str,
        location: Option<SourceLocation>,
    ) -> Result<(), ProgramOutputWriteError> {
        if message.len() > PROGRAM_LOG_MESSAGE_MAX_BYTES {
            return Err(ProgramOutputWriteError::LimitExceeded).map_err(measured(OutputKind::Log));
        }
        self.owner
            .log(level, message.to_string(), location)
            .await
            .map_err(output_error)
            .map_err(measured(OutputKind::Log))?;
        metric::output_kept(OutputKind::Log, byte_count(message));
        Ok(())
    }
}

/// Counts an output of `kind` the Run could not keep: one too large or
/// invalid. A fatal error keeps nothing, but rejects nothing either.
fn measured(kind: OutputKind) -> impl FnOnce(ProgramOutputWriteError) -> ProgramOutputWriteError {
    move |error| {
        if !matches!(error, ProgramOutputWriteError::Fatal(_)) {
            metric::output_rejected(kind);
        }
        error
    }
}

fn byte_count(text: &str) -> u64 {
    u64::try_from(text.len()).unwrap_or(u64::MAX)
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
