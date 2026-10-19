use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

#[cfg(test)]
use crate::RunId;

const CLAIM_LEASE: Duration = Duration::from_secs(30);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

#[cfg(test)]
#[async_trait::async_trait]
pub(crate) trait ExecutionObserver: Send + Sync {
    /// The Run Owner is about to commit its first chunk, the execution-start
    /// mark, under the claim acceptance wrote.
    async fn owner_starting(&self, _run_id: &RunId, _claim_id: &str) {}

    /// The Run Owner is about to commit one chunk; a test may hold it here.
    async fn before_chunk_commit(&self) {}

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
