//! Graceful shutdown (ADR 0018, Rollout): the process refuses new Runs, lets
//! its live Runs end on their own for the Shutdown Drain, then interrupts
//! the rest and waits the commit grace for them to record it. A Run whose
//! owner records nothing keeps its lease, and the next process's
//! expired-lease sweep ends it after expiry.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::watch;
use tokio::time::timeout;

use crate::{Error, RunModule};

/// Whether the process still accepts new owners, and how many it admitted
/// that have not registered yet; shared by every clone of one Run module.
#[derive(Clone, Default)]
pub(crate) struct Admission(Arc<AdmissionState>);

#[derive(Default)]
struct AdmissionState {
    closed: AtomicBool,
    /// Admissions between their check and their owner's registration (or
    /// their failure): a Run being accepted and launched, or a recovered
    /// owner being registered.
    in_flight: watch::Sender<usize>,
}

impl Admission {
    /// Refuses new owners from now on; `false` when they were refused already.
    fn close(&self) -> bool {
        !self.0.closed.swap(true, Ordering::SeqCst)
    }

    /// Refuses a new Run once the process is shutting down.
    pub(crate) fn check(&self) -> Result<(), Error> {
        if self.0.closed.load(Ordering::SeqCst) {
            Err(Error::ShuttingDown)
        } else {
            Ok(())
        }
    }

    /// Admits one owner to come, held until it is registered or given up:
    /// [`RunModule::shutdown`] waits for every admission it let through
    /// before it counts the live owners. Refused once shutting down.
    pub(crate) fn admit(&self) -> Result<Admitted, Error> {
        self.0.in_flight.send_modify(|count| *count += 1);
        let admitted = Admitted(self.clone());
        // Counted before the check: a shutdown that closed admission first
        // refuses this one, and one that closes later waits for it.
        self.check()?;
        Ok(admitted)
    }

    /// Ends one admission in flight.
    fn release(&self) {
        self.0.in_flight.send_modify(|count| *count -= 1);
    }

    /// Returns once no admission is in flight.
    async fn settled(&self) {
        let mut in_flight = self.0.in_flight.subscribe();
        // The sender lives as long as `self`.
        let _settled = in_flight.wait_for(|count| *count == 0).await;
    }
}

/// One admission in flight; dropping it, once the owner is registered or
/// the admission failed, lets shutdown count the live owners.
pub(crate) struct Admitted(Admission);

impl Drop for Admitted {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl RunModule {
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
        // An admission let through before the close ends registered or
        // refused before the owners are counted.
        let drain = self.shutdown_drain.duration();
        let drained = timeout(drain, async {
            self.admission.settled().await;
            self.owners.retired().await;
        })
        .await;
        if drained.is_ok() {
            return;
        }
        tracing::warn!(
            live = self.owners.live().len(),
            drain_ms = drain.as_millis(),
            "the Shutdown Drain is out: interrupting the Runs still live"
        );
        let interrupt_live = || {
            for owner in self.owners.live() {
                owner.request_stop();
            }
        };
        interrupt_live();
        let committed = timeout(self.execution_policy.commit_grace(), async {
            // An owner registered by a late admission is interrupted too.
            self.admission.settled().await;
            interrupt_live();
            self.owners.retired().await;
        })
        .await;
        if committed.is_err() {
            tracing::warn!(
                live = self.owners.live().len(),
                "Runs still live after the commit grace keep their lease; \
                 the next process's expired-lease sweep ends them"
            );
        }
    }
}
