use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use crate::RunId;

const CLAIM_LEASE: Duration = Duration::from_secs(30);
const CLAIM_HEARTBEAT: Duration = Duration::from_secs(1);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

#[cfg(test)]
#[async_trait::async_trait]
pub(crate) trait ExecutionObserver: Send + Sync {
    async fn worker_claimed(&self, _run_id: &RunId, _claim_id: &str) {}

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
    claim_heartbeat: Duration,
    shutdown_grace: Duration,
}

impl ExecutionPolicy {
    pub(crate) const fn production() -> Self {
        Self {
            claim_lease: CLAIM_LEASE,
            claim_heartbeat: CLAIM_HEARTBEAT,
            shutdown_grace: SHUTDOWN_GRACE,
        }
    }

    #[cfg(test)]
    pub(crate) const fn testing() -> Self {
        Self {
            claim_lease: Duration::from_secs(1),
            claim_heartbeat: Duration::from_millis(20),
            shutdown_grace: Duration::from_millis(100),
        }
    }

    /// Keeps fast shutdown/heartbeats while a test advances past a Run deadline
    /// without also expiring the worker claim.
    #[cfg(test)]
    pub(crate) const fn testing_with_claim_lease(claim_lease: Duration) -> Self {
        Self {
            claim_lease,
            ..Self::testing()
        }
    }

    /// Keeps fast heartbeats while a test proves that work does not wait out
    /// the shutdown grace.
    #[cfg(test)]
    pub(crate) const fn testing_with_shutdown_grace(shutdown_grace: Duration) -> Self {
        Self {
            shutdown_grace,
            ..Self::testing()
        }
    }

    pub(crate) const fn claim_heartbeat(self) -> Duration {
        self.claim_heartbeat
    }

    pub(crate) fn claim_lease_nanos(self) -> i64 {
        duration_nanos(self.claim_lease)
    }

    pub(crate) fn shutdown_grace_nanos(self) -> i64 {
        duration_nanos(self.shutdown_grace)
    }
}

fn duration_nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).expect("fixed execution duration fits durable nanoseconds")
}

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

pub(crate) struct ActiveExecution {
    claim_id: String,
    stop: Arc<ExecutionStop>,
    stopped: AtomicBool,
    stopped_notification: Notify,
}

impl ActiveExecution {
    fn new(claim_id: String) -> Self {
        Self {
            claim_id,
            stop: Arc::new(ExecutionStop::new()),
            stopped: AtomicBool::new(false),
            stopped_notification: Notify::new(),
        }
    }

    pub(crate) fn claim_id(&self) -> &str {
        &self.claim_id
    }

    pub(crate) fn stop(&self) -> Arc<ExecutionStop> {
        self.stop.clone()
    }

    pub(crate) fn request_stop(&self) {
        self.stop.request();
    }

    pub(crate) fn mark_stopped(&self) {
        if !self.stopped.swap(true, Ordering::SeqCst) {
            self.stopped_notification.notify_waiters();
        }
    }

    pub(crate) async fn wait_stopped(&self) {
        if self.stopped.load(Ordering::SeqCst) {
            return;
        }
        let notified = self.stopped_notification.notified();
        if self.stopped.load(Ordering::SeqCst) {
            return;
        }
        notified.await;
    }
}

#[derive(Clone, Default)]
pub(crate) struct ActiveExecutions {
    executions: Arc<Mutex<HashMap<RunId, Arc<ActiveExecution>>>>,
}

impl ActiveExecutions {
    pub(crate) fn register(&self, run_id: RunId, claim_id: String) -> ExecutionRegistration {
        let active = Arc::new(ActiveExecution::new(claim_id));
        let replaced = self
            .executions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(run_id.clone(), active.clone());
        if let Some(replaced) = replaced {
            replaced.request_stop();
        }
        ExecutionRegistration {
            executions: self.clone(),
            run_id,
            active,
        }
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, run_id: &RunId) -> bool {
        self.executions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(run_id)
    }

    pub(crate) fn find(&self, run_id: &RunId, claim_id: &str) -> Option<Arc<ActiveExecution>> {
        self.executions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(run_id)
            .filter(|active| active.claim_id() == claim_id)
            .cloned()
    }

    fn remove(&self, run_id: &RunId, claim_id: &str) {
        let mut executions = self
            .executions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if executions
            .get(run_id)
            .is_some_and(|active| active.claim_id() == claim_id)
        {
            executions.remove(run_id);
        }
    }
}

pub(crate) struct ExecutionRegistration {
    executions: ActiveExecutions,
    run_id: RunId,
    active: Arc<ActiveExecution>,
}

impl ExecutionRegistration {
    pub(crate) fn active(&self) -> Arc<ActiveExecution> {
        self.active.clone()
    }
}

impl Drop for ExecutionRegistration {
    fn drop(&mut self) {
        self.active.mark_stopped();
        self.executions.remove(&self.run_id, self.active.claim_id());
    }
}
