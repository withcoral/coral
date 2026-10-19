use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

use crate::{Error, RunId};

const CLAIM_LEASE: Duration = Duration::from_secs(30);
/// Leases without a successful commit after which a Run Owner is suspect: it
/// answers Program requests only once their chunk is recorded.
const SUSPECT_AFTER_LEASES: u32 = 1;
/// Leases without a successful commit after which a Run Owner interrupts its
/// Run: it terminates the Program, cancels its dispatches, and records
/// Interrupted as soon as the database answers.
const SELF_INTERRUPT_AFTER_LEASES: u32 = 3;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// The backoff between attempts when the database gives no answer: a reread
/// after an uncertain commit, a recommit, or the Interrupted record. Attempts
/// end on an answer, never on the clock.
pub(crate) const RETRY_BACKOFF_MIN: Duration = Duration::from_millis(50);
pub(crate) const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(2);

/// The wait before retry number `attempt` (from 1): doubling from
/// [`RETRY_BACKOFF_MIN`] up to [`RETRY_BACKOFF_MAX`].
fn retry_backoff(attempt: u32) -> Duration {
    RETRY_BACKOFF_MIN
        .saturating_mul(1 << attempt.saturating_sub(1).min(16))
        .min(RETRY_BACKOFF_MAX)
}

/// The backoff between attempts of one Run write the database did not
/// answer, with one warn line per attempt.
pub(crate) struct Backoff<'a> {
    run_id: &'a RunId,
    write: &'static str,
    attempts: u32,
}

impl<'a> Backoff<'a> {
    pub(crate) const fn new(run_id: &'a RunId, write: &'static str) -> Self {
        Self {
            run_id,
            write,
            attempts: 0,
        }
    }

    /// Logs the unanswered attempt and waits before the next one.
    pub(crate) async fn wait(&mut self, error: &Error) {
        self.attempts += 1;
        tracing::warn!(
            run_id = %self.run_id,
            write = self.write,
            attempt = self.attempts,
            error = %error,
            "Run Owner write did not land; retrying after a backoff"
        );
        tokio::time::sleep(retry_backoff(self.attempts)).await;
    }
}

/// Runs `attempt` until the database answers it: an operational error is
/// followed by another attempt after a bounded backoff, and each attempt
/// reads for itself whether the previous one took effect. Ends on the
/// answer, never on the clock; or with [`Error::OwnerLost`] once
/// `keep_trying` says the owner retired meanwhile and nothing is left to
/// tell it.
pub(crate) async fn until_answered<T, F>(
    run_id: &RunId,
    write: &'static str,
    keep_trying: impl Fn() -> bool,
    mut attempt: impl FnMut() -> F,
) -> Result<T, Error>
where
    F: Future<Output = Result<T, Error>>,
{
    let mut backoff = Backoff::new(run_id, write);
    loop {
        match attempt().await {
            Err(error) if error.is_operational() => {
                backoff.wait(&error).await;
                if !keep_trying() {
                    return Err(Error::OwnerLost);
                }
            }
            answered => return answered,
        }
    }
}

#[cfg(test)]
#[async_trait::async_trait]
pub(crate) trait ExecutionObserver: Send + Sync {
    /// The Run Owner is about to commit its first chunk, the execution-start
    /// mark, under the claim acceptance wrote.
    async fn owner_starting(&self, _run_id: &RunId, _claim_id: &str) {}

    /// The Run Owner is about to commit one chunk; a test may hold it here.
    async fn before_chunk_commit(&self) {}

    /// One chunk commit is about to begin its transaction; `true` fails it as
    /// one the database never answered, before it writes.
    async fn chunk_commit_unanswered(&self) -> bool {
        false
    }

    /// One chunk commit landed; `true` loses its answer, so the owner resolves
    /// the commit as one the database never answered.
    async fn chunk_commit_answer_lost(&self) -> bool {
        false
    }

    /// The owner's task dequeued one message; a test may panic here.
    fn owner_message_dequeued(&self) {}

    fn execution_attempt_finished(&self) {}
}

#[cfg(test)]
pub(crate) struct NoopExecutionObserver;

#[cfg(test)]
#[async_trait::async_trait]
impl ExecutionObserver for NoopExecutionObserver {}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ExecutionPolicy {
    claim_lease: Duration,
    shutdown_grace: Duration,
}

impl ExecutionPolicy {
    pub(crate) const fn production() -> Self {
        Self {
            claim_lease: CLAIM_LEASE,
            shutdown_grace: SHUTDOWN_GRACE,
        }
    }

    #[cfg(test)]
    pub(crate) const fn testing() -> Self {
        Self {
            claim_lease: Duration::from_secs(1),
            shutdown_grace: Duration::from_millis(100),
        }
    }

    /// Keeps fast shutdown while a test advances past a Run deadline without
    /// also expiring the worker claim.
    #[cfg(test)]
    pub(crate) const fn testing_with_claim_lease(claim_lease: Duration) -> Self {
        Self {
            claim_lease,
            ..Self::testing()
        }
    }

    /// Keeps a fast lease while a test proves that work does not wait out the
    /// shutdown grace.
    #[cfg(test)]
    pub(crate) const fn testing_with_shutdown_grace(shutdown_grace: Duration) -> Self {
        Self {
            shutdown_grace,
            ..Self::testing()
        }
    }

    pub(crate) fn claim_lease_nanos(self) -> i64 {
        duration_nanos(self.claim_lease)
    }

    /// When a lease taken at `from_unix_nanos` ends.
    pub(crate) fn lease_until(self, from_unix_nanos: i64) -> Result<i64, crate::Error> {
        from_unix_nanos
            .checked_add(self.claim_lease_nanos())
            .ok_or_else(|| crate::Error::Clock("worker claim deadline overflowed".to_string()))
    }

    /// How long an idle Run Owner goes without a commit before it renews its
    /// lease with an empty chunk: half a lease.
    pub(crate) fn idle_tick(self) -> Duration {
        self.claim_lease / 2
    }

    /// How long after its last successful commit a Run Owner is suspect.
    pub(crate) fn suspect_after_nanos(self) -> i64 {
        self.claim_lease_nanos()
            .saturating_mul(i64::from(SUSPECT_AFTER_LEASES))
    }

    /// How long after its last successful commit a Run Owner interrupts its Run.
    pub(crate) fn self_interrupt_after_nanos(self) -> i64 {
        self.claim_lease_nanos()
            .saturating_mul(i64::from(SELF_INTERRUPT_AFTER_LEASES))
    }

    pub(crate) fn shutdown_grace_nanos(self) -> i64 {
        duration_nanos(self.shutdown_grace)
    }
}

fn duration_nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).expect("fixed execution duration fits durable nanoseconds")
}

/// The stop one Run's owner shares with its Program and its dispatches:
/// uncatchable V8 termination plus a notification every stop-aware wait selects on.
pub(crate) struct ExecutionStop {
    requested: AtomicBool,
    requested_notification: Notify,
    v8: coral_v8::ProgramExecutionControl,
}

impl ExecutionStop {
    pub(crate) fn new() -> Self {
        Self {
            requested: AtomicBool::new(false),
            requested_notification: Notify::new(),
            v8: coral_v8::ProgramExecutionControl::new(),
        }
    }

    pub(crate) fn request(&self) {
        if !self.requested.swap(true, Ordering::SeqCst) {
            let _isolate_was_ready = self.v8.terminate();
            self.requested_notification.notify_waiters();
        }
    }

    pub(crate) fn is_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }

    pub(crate) async fn requested(&self) {
        if self.is_requested() {
            return;
        }
        let notified = self.requested_notification.notified();
        if self.is_requested() {
            return;
        }
        notified.await;
    }

    pub(crate) const fn v8_control(&self) -> &coral_v8::ProgramExecutionControl {
        &self.v8
    }
}
