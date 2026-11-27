//! A compile's one span, Coral's (ADR 0027, Telemetry): `coral.compile`,
//! from the wait for a compiler to the answer. It names how the compile's
//! last compiler call went: whether Coral relayed it, and the Run Executor
//! that answered it. Each call notes where it went when it is sent, and
//! the Run Executor that answered it once it answers. The span records the
//! last call's note once the compile is over, since a field once recorded
//! cannot be taken back.

use std::cell::RefCell;
use std::future::Future;

use coral_telemetry::safe;
use tracing::Instrument as _;
use tracing::field::Empty;

use crate::Kind;

tokio::task_local! {
    /// Where the last compiler call of the compile that this task runs in
    /// [`traced_compile`] went, or none while it went nowhere.
    static LAST_CALL: RefCell<Option<LastCall>>;
}

/// Where one compiler call went, and who answered it.
struct LastCall {
    relayed: bool,
    answered_by: Option<ServedBy>,
}

/// Runs `compile` in a new `coral.compile` span under the current span.
/// `compile` is the whole compile: the caller's wait for a compiler, then
/// its calls. An error marks the span failed with the code `code` gives it.
///
/// The span records how the last call ended. A last call that no Run
/// Executor answered names none, and one that went nowhere records no
/// `coral.compile.relayed` either.
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
    let compile = Box::pin(compile).instrument(span.clone());
    let (compiled, last) = LAST_CALL
        .scope(RefCell::new(None), async {
            let compiled = compile.await;
            (compiled, LAST_CALL.with(RefCell::take))
        })
        .await;
    if let Some(last) = last {
        if let Some(answered_by) = &last.answered_by {
            span.record(
                "coral.run_executor.id",
                safe::id(&answered_by.run_executor_id),
            );
            span.record(
                "coral.run_executor.kind",
                safe::kind(answered_by.kind.metric().as_str()),
            );
        }
        span.record("coral.compile.relayed", safe::flag(last.relayed));
    }
    if let Err(error) = &compiled {
        coral_telemetry::record_failure(&span, code(error));
    }
    compiled
}

/// The Run Executor that answered a compiler call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ServedBy {
    /// The id it declared in its `Hello`.
    pub(crate) run_executor_id: uuid::Uuid,
    pub(crate) kind: Kind,
}

/// Notes that a compiler call of the current compile starts: until it is
/// sent, the compile's last call went nowhere.
pub(crate) fn call_started() {
    note(|last| *last = None);
}

/// Notes that the current call went to a compiler, through the database
/// when `relayed`, before any answer: a call its caller drops while it
/// waits still says where it went.
pub(crate) fn call_sent(relayed: bool) {
    note(|last| {
        *last = Some(LastCall {
            relayed,
            answered_by: None,
        });
    });
}

/// Notes that the Run Executor `answered_by` answered the current call.
pub(crate) fn call_answered(answered_by: ServedBy) {
    note(|last| {
        if let Some(last) = last {
            last.answered_by = Some(answered_by);
        }
    });
}

/// Changes the note of the compile that this task runs in
/// [`traced_compile`]; outside one, it notes nothing.
fn note(change: impl FnOnce(&mut Option<LastCall>)) {
    let _outside_a_compile = LAST_CALL.try_with(|last| change(&mut last.borrow_mut()));
}

#[cfg(test)]
pub(crate) mod tests;
