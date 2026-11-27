//! A compile's one span, Coral's (ADR 0027, Telemetry): `coral.compile`,
//! from the wait for a compiler to the answer. Each call of the compile
//! names on it the Run Executor that took the call, so the span names the
//! one that took its last call.

use std::future::Future;

use coral_telemetry::safe;
use tracing::Instrument as _;
use tracing::field::Empty;

use crate::Kind;

/// Runs `compile`, one whole compile from its wait for a compiler to its
/// answer, in a new `coral.compile` span under the current span. An error
/// marks the span failed with the code `code` gives it. A compile that no
/// Run Executor took ends with no Run Executor fields.
///
/// # Errors
///
/// Returns the error of `compile`.
pub async fn traced_compile<T, E>(
    compile: impl Future<Output = Result<T, E>>,
    code: impl FnOnce(&E) -> &'static str,
) -> Result<T, E> {
    let span = tracing::info_span!(
        "coral.compile",
        coral.run_executor.id = Empty,
        coral.run_executor.kind = Empty,
        coral.compile.relayed = Empty,
        error.type = Empty,
    );
    // Boxed, so a caller's future is no larger than without the span.
    let compiled = Box::pin(compile).instrument(span.clone()).await;
    if let Err(error) = &compiled {
        coral_telemetry::record_failure(&span, code(error));
    }
    compiled
}

/// The Run Executor that took a compiler call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ServedBy {
    /// The id it declared in its `Hello`.
    pub(crate) run_executor_id: uuid::Uuid,
    pub(crate) kind: Kind,
}

impl ServedBy {
    /// Names this Run Executor on the current span, the `coral.compile`
    /// span of the compile that made the call, `relayed` when the call went
    /// through the database.
    pub(crate) fn record(&self, relayed: bool) {
        let span = tracing::Span::current();
        span.record("coral.run_executor.id", safe::id(&self.run_executor_id));
        span.record(
            "coral.run_executor.kind",
            safe::kind(self.kind.metric().as_str()),
        );
        span.record("coral.compile.relayed", safe::flag(relayed));
    }
}

#[cfg(test)]
pub(crate) mod tests;
