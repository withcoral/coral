//! Run-bound Program Emission and Program Log requests to the loop.

use coral_telemetry::metric::{self, OutputKind};

use crate::run_stream::run_loop::LoopHandle;
use crate::{Error, PROGRAM_LOG_MESSAGE_MAX_BYTES, ProgramLogLevel, ProgramValue, SourceLocation};

/// Validates Program output and asks the loop to apply it. The loop
/// decides the Run Output Budget against its exact total and answers at
/// Applied; above the Applied cap it answers once the chunk is recorded.
pub(crate) struct ProgramOutputWriter {
    run_loop: LoopHandle,
    limits: coral_value::ValueLimits,
}

impl ProgramOutputWriter {
    pub(crate) const fn new(run_loop: LoopHandle, limits: coral_value::ValueLimits) -> Self {
        Self { run_loop, limits }
    }

    pub(crate) async fn emit(
        &self,
        encoded_json: &str,
        location: Option<SourceLocation>,
    ) -> Result<(), ProgramOutputWriteError> {
        let written = self.write_emission(encoded_json, location).await;
        record(OutputKind::Emission, encoded_json.len(), &written);
        written
    }

    pub(crate) async fn log(
        &self,
        level: ProgramLogLevel,
        message: &str,
        location: Option<SourceLocation>,
    ) -> Result<(), ProgramOutputWriteError> {
        let written = self.write_log(level, message, location).await;
        record(OutputKind::Log, message.len(), &written);
        written
    }

    async fn write_emission(
        &self,
        encoded_json: &str,
        location: Option<SourceLocation>,
    ) -> Result<(), ProgramOutputWriteError> {
        let value =
            ProgramValue::with_limits(encoded_json.to_string(), self.limits).map_err(|error| {
                match error {
                    Error::ProgramValueTooLarge => ProgramOutputWriteError::LimitExceeded,
                    Error::ProgramValueJson(_) | Error::InvalidProgramValue => {
                        ProgramOutputWriteError::InvalidValue
                    }
                    error => ProgramOutputWriteError::Fatal(error),
                }
            })?;
        self.run_loop
            .emit(value, location)
            .await
            .map_err(output_error)
    }

    async fn write_log(
        &self,
        level: ProgramLogLevel,
        message: &str,
        location: Option<SourceLocation>,
    ) -> Result<(), ProgramOutputWriteError> {
        if message.len() > PROGRAM_LOG_MESSAGE_MAX_BYTES {
            return Err(ProgramOutputWriteError::LimitExceeded);
        }
        self.run_loop
            .log(level, message.to_string(), location)
            .await
            .map_err(output_error)
    }
}

/// Records an output of `kind` and `size` by whether the Run kept it. One
/// too large or invalid is rejected; a fatal error keeps nothing, but
/// rejects nothing either.
fn record(kind: OutputKind, size: usize, written: &Result<(), ProgramOutputWriteError>) {
    match written {
        Ok(()) => metric::output_kept(kind, size),
        Err(ProgramOutputWriteError::Fatal(_)) => {}
        Err(ProgramOutputWriteError::InvalidValue | ProgramOutputWriteError::LimitExceeded) => {
            metric::output_rejected(kind);
        }
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
