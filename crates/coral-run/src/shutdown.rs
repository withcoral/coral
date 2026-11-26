//! Graceful shutdown (ADR 0027, Reconnect): the Coral Instance refuses new
//! Runs, closes its pull streams, and moves each Run stream once no dispatch
//! is in flight on it, so the Run goes on on another Coral Instance. A Run
//! that a dialed Run Executor runs is never interrupted here. A local
//! child's stream can reopen only here, so its Run keeps dispatching and, at
//! the drain's length, ends interrupted, or timed out past its deadline. A
//! Run whose loop records nothing by then is the sweeper's.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use coral_telemetry::safe;
use tokio::sync::watch;
use tokio::time::timeout;

use crate::run_stream::run_loop::DrainStep;
use crate::{Error, RunModule};

/// Whether the process still accepts new loops, and how many it admitted
/// that have not registered yet; shared by every clone of one Run module.
#[derive(Clone, Default)]
pub(crate) struct Admission(Arc<AdmissionState>);

#[derive(Default)]
struct AdmissionState {
    closed: AtomicBool,
    /// Admissions between their check and their registration (or their
    /// failure): a Run being accepted and launched, or a reopened stream's
    /// loop being registered.
    in_flight: watch::Sender<usize>,
}

impl Admission {
    /// Refuses new loops from now on; `false` when they were refused already.
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

    /// Admits one loop to come, held until it is registered or given up:
    /// [`RunModule::shutdown`] waits for every admission it let through
    /// before it counts the live loops. Refused once shutting down.
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

/// One admission in flight; dropping it, once the loop is registered or
/// the admission failed, lets shutdown count the live loops.
pub(crate) struct Admitted(Admission);

impl Drop for Admitted {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl RunModule {
    /// Shuts the Run module down gracefully. Every new Run is refused with
    /// [`Error::ShuttingDown`] from now on. The pull streams close and no new
    /// stream opens. Each stream's loop starts no new dispatch, and once
    /// this Coral Instance holds no pull stream and no dispatch is in flight
    /// on the stream, it commits what it applied and ends the stream cleanly:
    /// the Run Executor reopens it on another Coral Instance. At the drain's
    /// length the rest move at once, and the Coral Instance each reopens on
    /// settles its dispatches in flight. A local child's stream can reopen
    /// only here, so it keeps dispatching through the drain, and at its
    /// length its Run ends interrupted.
    ///
    /// The drain starts when this is called: admission closes, the pull
    /// streams drain and the stream loops hold their dispatches before the
    /// returned future is first polled. The caller reports itself not ready
    /// before this, and stops serving only after the future completes. The
    /// drain runs on its own task, so dropping the future does not stop it,
    /// and the future raises a panic of the drain again. Only the first
    /// call drains: a later one completes at once, so the caller that stops
    /// serving awaits the first.
    pub fn shutdown(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let drained = if self.admission.close() {
            if let Some(streams) = &self.pull_streams {
                streams.drain();
            }
            self.drain_loops(DrainStep::Hold);
            let module = self.clone();
            Some(tokio::spawn(async move { module.drain_and_stop().await }))
        } else {
            None
        };
        async move {
            if let Some(drained) = drained
                && let Err(error) = drained.await
                && error.is_panic()
            {
                std::panic::resume_unwind(error.into_panic());
            }
        }
    }

    /// The rest of [`Self::shutdown`] once the drain started.
    async fn drain_and_stop(&self) {
        // An admission let through before the close ends registered or
        // refused before the loops are counted.
        let drain = self.shutdown_drain.duration();
        let drained = timeout(drain, async {
            self.admission.settled().await;
            self.drain_loops(DrainStep::Hold);
            // A stream that moved while the Run Executor's pull stream was
            // still here would reopen here.
            if let Some(streams) = &self.pull_streams {
                streams.ended().await;
            }
            self.drain_loops(DrainStep::Move);
            self.loops.retired().await;
        })
        .await;
        if drained.is_ok() {
            return;
        }
        tracing::warn!(
            coral.shutdown.live_runs = safe::number(self.loops.live().len()),
            coral.shutdown.drain_ms = safe::number(drain.as_millis()),
            "the Shutdown Drain is out: moving the Run streams and interrupting the local children's Runs"
        );
        let end_live = || self.drain_loops(DrainStep::MoveNow);
        end_live();
        let committed = timeout(self.execution_policy.commit_grace(), async {
            // A loop registered by a late admission moves or is interrupted
            // too.
            self.admission.settled().await;
            end_live();
            self.loops.retired().await;
        })
        .await;
        if committed.is_err() {
            tracing::warn!(
                coral.shutdown.live_runs = safe::number(self.loops.live().len()),
                "Run streams still live after the commit grace are left to the Run sweeper"
            );
        }
    }

    /// Takes `step` of the drain with every loop live here.
    fn drain_loops(&self, step: DrainStep) {
        for run_loop in self.loops.live() {
            run_loop.drain(step);
        }
    }
}
