use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use coral_telemetry::safe;
use tokio::sync::Notify;

use crate::Error;
use crate::types::HeldRunId;

/// How long the end of the Shutdown Drain waits for the loops it moves or
/// interrupts: each one's own end write, retried until the database answers.
const COMMIT_GRACE: Duration = Duration::from_secs(6);
/// How long an Approval Request waits for a decision at most; the Run
/// deadline bounds it further.
const APPROVAL_WAIT: Duration = Duration::from_mins(15);
/// How often a Run stream loop with parked Invocations prechecks their waits in a
/// read snapshot: the ceiling on noticing a wait that ended or became moot
/// without a wake-up.
const PARKED_PRECHECK_INTERVAL: Duration = Duration::from_secs(15);
/// The shortest gap between two reads a Run stream loop makes of its pending
/// connection requests after the change signal rang.
const CHANGE_WAKE_GAP: Duration = Duration::from_secs(1);
#[cfg(test)]
const TEST_PRECHECK_INTERVAL: Duration = Duration::from_millis(200);
#[cfg(test)]
const TEST_WAKE_GAP: Duration = Duration::from_millis(50);
/// The backoff between attempts when the database gives no answer: a reread
/// after an uncertain commit, a recommit, or a terminal write. Attempts
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
    run_id: &'a HeldRunId,
    write: &'static str,
    attempts: u32,
}

impl<'a> Backoff<'a> {
    pub(crate) const fn new(run_id: &'a HeldRunId, write: &'static str) -> Self {
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
            coral.run.id = safe::id(self.run_id),
            coral.run.write = safe::kind(self.write),
            coral.run.write.attempt = safe::number(self.attempts),
            error.type = safe::kind(error.code()),
            "Run write did not land; retrying after a backoff"
        );
        tokio::time::sleep(retry_backoff(self.attempts)).await;
    }
}

/// Runs `attempt` until the database answers it: an operational error is
/// followed by another attempt after a bounded backoff, and each attempt
/// reads for itself whether the previous one took effect. Ends on the
/// answer, never on the clock; or with [`Error::OwnerLost`] once
/// `keep_trying` says the loop retired meanwhile and nothing is left to
/// tell it.
pub(crate) async fn until_answered<T, F>(
    run_id: &HeldRunId,
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
    /// The loop is about to commit one chunk; a test may hold it here.
    async fn before_chunk_commit(&self) {}

    /// One chunk commit is about to begin its transaction; `true` fails it as
    /// one the database never answered, before it writes.
    async fn chunk_commit_unanswered(&self) -> bool {
        false
    }

    /// One chunk commit landed; `true` loses its answer, so the loop resolves
    /// the commit as one the database never answered.
    async fn chunk_commit_answer_lost(&self) -> bool {
        false
    }

    /// The loop's task dequeued one message.
    fn owner_message_dequeued(&self) {}

    /// The wake-up reader woke one Run stream's loop, in a read that `turn`
    /// started.
    fn wake_delivered(&self, _turn: crate::observation::Turn) {}

    /// The wake-up reader is about to read; a test may hold it here.
    async fn before_wake_read(&self) {}

    /// The loop armed `timer` to fire after `after_nanos` on the host clock.
    fn timer_armed(&self, _timer: &crate::owner::Timer, _after_nanos: i64) {}

    /// The loop's `check` of its parked waits read its snapshot and found
    /// `found` waits ended or moot.
    fn precheck_finished(&self, _check: crate::owner::Check, _found: usize) {}

    fn execution_attempt_finished(&self) {}
}

#[cfg(test)]
pub(crate) struct NoopExecutionObserver;

#[cfg(test)]
#[async_trait::async_trait]
impl ExecutionObserver for NoopExecutionObserver {}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ExecutionPolicy {
    commit_grace: Duration,
    approval_wait: Duration,
    precheck_interval: Duration,
    wake_gap: Duration,
}

impl ExecutionPolicy {
    pub(crate) const fn production() -> Self {
        Self {
            commit_grace: COMMIT_GRACE,
            approval_wait: APPROVAL_WAIT,
            precheck_interval: PARKED_PRECHECK_INTERVAL,
            wake_gap: CHANGE_WAKE_GAP,
        }
    }

    #[cfg(test)]
    pub(crate) const fn testing() -> Self {
        Self {
            commit_grace: Duration::from_millis(1100),
            approval_wait: APPROVAL_WAIT,
            precheck_interval: TEST_PRECHECK_INTERVAL,
            wake_gap: TEST_WAKE_GAP,
        }
    }

    /// Keeps everything else while a test chooses how often parked waits
    /// are prechecked and the gap between reads after a change.
    #[cfg(test)]
    pub(crate) const fn with_wakeups(
        self,
        precheck_interval: Duration,
        wake_gap: Duration,
    ) -> Self {
        Self {
            precheck_interval,
            wake_gap,
            ..self
        }
    }

    /// The test wake-ups: a parked wait that became moot is found well
    /// within a test's wait for the Run's end.
    #[cfg(test)]
    pub(crate) const fn with_test_wakeups(self) -> Self {
        self.with_wakeups(TEST_PRECHECK_INTERVAL, TEST_WAKE_GAP)
    }

    /// Expires Approval Requests after `approval_wait`, so a test sees the
    /// loop's timer fire without waiting out the Run.
    #[cfg(test)]
    pub(crate) const fn testing_with_approval_wait(approval_wait: Duration) -> Self {
        Self {
            approval_wait,
            ..Self::testing()
        }
    }

    /// How long the end of the Shutdown Drain waits for the loops it moves
    /// or interrupts to record that.
    pub(crate) const fn commit_grace(self) -> Duration {
        self.commit_grace
    }

    /// How long an Approval Request waits for a decision at most.
    pub(crate) fn approval_wait_nanos(self) -> i64 {
        duration_nanos(self.approval_wait)
    }

    /// How often a Run stream loop with parked Invocations prechecks their waits.
    pub(crate) fn precheck_interval_nanos(self) -> i64 {
        duration_nanos(self.precheck_interval)
    }

    pub(crate) fn wake_gap_nanos(self) -> i64 {
        duration_nanos(self.wake_gap)
    }
}

fn duration_nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).expect("fixed execution duration fits durable nanoseconds")
}

/// The stop one Run stream's loop shares with its dispatches: a
/// notification every stop-aware wait selects on. The Run Executor ends the
/// Program itself, on the stream's `Terminate`.
pub(crate) struct ExecutionStop {
    requested: AtomicBool,
    /// Whether a cancellation of the Run asked for the stop.
    canceled: AtomicBool,
    requested_notification: Notify,
}

impl ExecutionStop {
    pub(crate) fn new() -> Self {
        Self {
            requested: AtomicBool::new(false),
            canceled: AtomicBool::new(false),
            requested_notification: Notify::new(),
        }
    }

    pub(crate) fn request(&self) {
        if !self.requested.swap(true, Ordering::SeqCst) {
            self.requested_notification.notify_waiters();
        }
    }

    /// Requests the stop of a Run that was canceled.
    pub(crate) fn request_canceled(&self) {
        self.canceled.store(true, Ordering::SeqCst);
        self.request();
    }

    pub(crate) fn is_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }

    pub(crate) fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::SeqCst)
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
}
