//! Graceful shutdown (ADR 0018, Rollout): the process refuses new Runs, lets
//! its live Runs end on their own for the Shutdown Drain, then interrupts
//! the rest and waits the commit grace for them to record it. A Run whose
//! owner records nothing keeps its lease, and the next process's
//! expired-lease sweep ends it after expiry.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::time::timeout;

use crate::{Error, RunModule, ShutdownDrain};

/// Whether the process still accepts new Runs; shared by every clone of one
/// Run module.
#[derive(Clone, Default)]
pub(crate) struct Admission(Arc<AtomicBool>);

impl Admission {
    /// Refuses new Runs from now on; `false` when they were refused already.
    fn close(&self) -> bool {
        !self.0.swap(true, Ordering::SeqCst)
    }

    /// Refuses a new Run once the process is shutting down.
    pub(crate) fn check(&self) -> Result<(), Error> {
        if self.0.load(Ordering::SeqCst) {
            Err(Error::ShuttingDown)
        } else {
            Ok(())
        }
    }
}

impl RunModule {
    /// Returns the Shutdown Drain [`Self::shutdown`] waits.
    #[must_use]
    pub const fn shutdown_drain(&self) -> ShutdownDrain {
        self.shutdown_drain
    }

    /// Replaces the Shutdown Drain on a built module.
    #[cfg(test)]
    pub(crate) const fn with_shutdown_drain(mut self, drain: ShutdownDrain) -> Self {
        self.shutdown_drain = drain;
        self
    }

    /// Shuts the Run module down gracefully. Every new Run is refused with
    /// [`Error::ShuttingDown`] from now on, while the live Run Owners keep
    /// working: their Runs end on their own and their requests are served.
    /// Once every owner retired, or the Shutdown Drain is out, every owner
    /// still live is interrupted (its Run ends `interrupted`, or `timed_out`
    /// past its deadline) and given the commit grace to record that. Returns
    /// then; an owner that recorded nothing by then keeps its lease, and the
    /// next process's expired-lease sweep ends its Run.
    ///
    /// The caller reports itself not ready before this, and stops serving
    /// only after it returns. Only the first call drains; a later one
    /// returns at once.
    pub async fn shutdown(&self) {
        if !self.admission.close() {
            return;
        }
        let drain = self.shutdown_drain.duration();
        if timeout(drain, self.owners.retired()).await.is_ok() {
            return;
        }
        let live = self.owners.live();
        tracing::warn!(
            live = live.len(),
            drain_ms = drain.as_millis(),
            "the Shutdown Drain is out: interrupting the Runs still live"
        );
        for owner in &live {
            owner.request_stop();
        }
        if timeout(self.execution_policy.commit_grace(), self.owners.retired())
            .await
            .is_err()
        {
            tracing::warn!(
                live = self.owners.live().len(),
                "Runs still live after the commit grace keep their lease; \
                 the next process's expired-lease sweep ends them"
            );
        }
    }
}
