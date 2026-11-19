//! Graceful shutdown (ADR 0018, Rollout): the process refuses new Runs, lets
//! its live Runs end on their own for the Shutdown Drain, then interrupts
//! the rest and waits the commit grace for them to record it. A Run whose
//! owner records nothing keeps its lease, and the next process's
//! expired-lease sweep ends it after expiry.
//!
//! A Run that a Run Executor runs is never interrupted here (ADR 0027,
//! Reconnect): the drain closes the pull streams and moves each Run stream
//! once no dispatch is in flight on it, and the Run goes on on another
//! Coral Instance.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use coral_telemetry::safe;
use tokio::sync::watch;
use tokio::time::timeout;

use crate::owner::DrainStep;
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
    /// The Run streams drain instead. The pull streams close and no new
    /// stream opens. Each stream's owner starts no new dispatch, and once
    /// this Coral Instance holds no pull stream and no dispatch is in flight
    /// on the stream, it commits what it applied and ends the stream cleanly:
    /// the Run Executor reopens it on another Coral Instance. At the drain's
    /// length the rest move at once, and the Coral Instance each reopens on
    /// settles its dispatches in flight. Nothing records Interrupted.
    ///
    /// The drain starts when this is called: admission closes, the pull
    /// streams drain and the stream owners hold their dispatches before the
    /// returned future is first polled. The caller reports itself not ready
    /// before this, and stops serving only after the future completes. Only
    /// the first call drains; a later one completes at once.
    pub fn shutdown(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let first = self.admission.close();
        if first {
            if let Some(streams) = &self.pull_streams {
                streams.drain();
            }
            self.drain_owners(DrainStep::Hold);
        }
        let module = self.clone();
        async move {
            if first {
                module.drain_and_stop().await;
            }
        }
    }

    /// The rest of [`Self::shutdown`] once the drain started.
    async fn drain_and_stop(&self) {
        // An admission let through before the close ends registered or
        // refused before the owners are counted.
        let drain = self.shutdown_drain.duration();
        let drained = timeout(drain, async {
            self.admission.settled().await;
            self.drain_owners(DrainStep::Hold);
            // A stream that moved before the Run Executor's pull stream left
            // here would reopen here.
            if let Some(streams) = &self.pull_streams {
                streams.ended().await;
            }
            self.drain_owners(DrainStep::Leave);
            self.owners.retired().await;
        })
        .await;
        if drained.is_ok() {
            return;
        }
        tracing::warn!(
            coral.shutdown.live_runs = safe::number(self.owners.live().len()),
            coral.shutdown.drain_ms = safe::number(drain.as_millis()),
            "the Shutdown Drain is out: interrupting the Runs still live"
        );
        let interrupt_live = || self.drain_owners(DrainStep::LeaveNow);
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
                coral.shutdown.live_runs = safe::number(self.owners.live().len()),
                "Runs still live after the commit grace keep their lease; \
                 the next process's expired-lease sweep ends them"
            );
        }
    }

    /// Takes `step` of the drain with every owner live here.
    fn drain_owners(&self, step: DrainStep) {
        for owner in self.owners.live() {
            owner.drain(step);
        }
    }
}
