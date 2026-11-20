//! The wake-up reader (ADR 0027, Outside requests and the Run's head).
//!
//! A writer other than a Run's own loop that commits on a running Run a Run
//! Executor holds moves the Run's head, and adds nothing else: the receiver
//! of a decision or a cancellation, and the sweeper. Each Run stream's loop
//! publishes the head it last wrote. One reader per Coral Instance, after the
//! content-free change signal rang, reads in one statement the heads of the
//! Runs whose loops are registered here, and wakes each loop whose Run ended
//! or moved past what it wrote. The loop rereads its Run. While no Run
//! stream's loop is registered here it reads nothing. Per-Run loops never
//! subscribe to the signal: a loop parked on a connection request hears its
//! rings through the reader. A Run Owner of an owned Run keeps a
//! subscription of its own until the owner path goes (#2911).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use coral_db::Database;
use coral_telemetry::safe;
use tokio::sync::Notify;

use crate::observation::{Cadence, Turn};
use crate::owner::{Message, OwnerHandle, Owners};
use crate::{Error, RunId, run_repository};

/// How many times the receiver of a decision or a cancellation for a Run a
/// Run Executor holds reads the Run again after another writer moved it
/// under the write, before it answers that the request is not yet recorded.
const OUTSIDE_WRITE_ATTEMPTS: usize = 8;

/// Runs `attempt`, one transaction of an outside writer of a Run a Run
/// Executor holds, until it lands or finds the Run ended: another writer
/// that moved the Run's head or claim under it means a reread and a retry.
///
/// # Errors
///
/// [`Error::RunRequestNotYetRecorded`] after [`OUTSIDE_WRITE_ATTEMPTS`]
/// attempts that another writer moved; any other error of an attempt.
pub(crate) async fn until_written<Attempt>(
    mut attempt: impl FnMut() -> Attempt,
) -> Result<(), Error>
where
    Attempt: std::future::Future<Output = Result<(), Error>>,
{
    for _attempt in 0..OUTSIDE_WRITE_ATTEMPTS {
        match attempt().await {
            Err(Error::HeadMismatch | Error::ClaimMismatch) => {}
            Err(Error::RunEnded) => return Ok(()),
            written => return written,
        }
    }
    Err(Error::RunRequestNotYetRecorded)
}

/// This Coral Instance's wake-up reader, and the loops that listen to it.
#[derive(Clone)]
pub(crate) struct RunWakes {
    database: Arc<Database>,
    woken: Arc<Notify>,
    listeners: Listeners,
    reader: Arc<OnceLock<Reader>>,
}

/// The Run stream loops here that wait for a ring of the change signal:
/// those parked on a connection request.
type Listeners = Arc<Mutex<HashMap<RunId, OwnerHandle>>>;

impl RunWakes {
    pub(crate) fn new(database: Arc<Database>) -> Self {
        Self {
            database,
            woken: Arc::default(),
            listeners: Listeners::default(),
            reader: Arc::default(),
        }
    }

    /// The loop `handle` names parked a connection wait: it hears each ring
    /// of the change signal as [`Message::Changed`], once at once as a
    /// subscription of its own would, until [`Self::unlisten`]. Its reader
    /// runs: the loop registered first.
    pub(crate) fn listen(&self, handle: &OwnerHandle) {
        self.lock().insert(handle.run_id().clone(), handle.clone());
        handle.send(Message::Changed);
    }

    /// The loop `handle` names waits for no ring any more. A loop that
    /// replaced it for the same Run keeps listening.
    pub(crate) fn unlisten(&self, handle: &OwnerHandle) {
        let mut listeners = self.lock();
        if listeners
            .get(handle.run_id())
            .is_some_and(|listening| listening.is(handle))
        {
            listeners.remove(handle.run_id());
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<RunId, OwnerHandle>> {
        self.listeners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A Run stream's loop registered in `owners`: starts this Coral
    /// Instance's reader on the first, and has it read at once.
    pub(crate) fn loop_registered(
        &self,
        owners: &Owners,
        #[cfg(test)] observer: &Arc<dyn crate::execution::ExecutionObserver>,
    ) {
        self.reader.get_or_init(|| {
            Reader::start(
                self.database.clone(),
                self.woken.clone(),
                self.listeners.clone(),
                owners.clone(),
                #[cfg(test)]
                observer.clone(),
            )
        });
        self.woken.notify_one();
    }

    /// Wakes this Coral Instance's reader when the loop of `run_id` is
    /// here: a write just committed reaches it without waiting for the
    /// signal.
    pub(crate) fn wake_for(&self, run_id: &RunId) {
        if self
            .reader
            .get()
            .is_some_and(|reader| reader.owners.contains(run_id))
        {
            self.woken.notify_one();
        }
    }
}

/// The reader; the last handle on it stops it.
struct Reader {
    task: tokio::task::JoinHandle<()>,
    owners: Owners,
}

impl Reader {
    fn start(
        database: Arc<Database>,
        woken: Arc<Notify>,
        listeners: Listeners,
        owners: Owners,
        #[cfg(test)] observer: Arc<dyn crate::execution::ExecutionObserver>,
    ) -> Self {
        let registered = owners.clone();
        let task = tokio::spawn(async move {
            let mut cadence = Cadence::default();
            loop {
                let turn = cadence.turn(&database, &woken).await;
                // A ring that a local wake beat to the turn is relayed too,
                // and the backstop stands in for a ring that never came.
                if cadence.rang() || turn == Turn::Backstop {
                    let listeners = listeners
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    for listener in listeners.values() {
                        listener.send(Message::Changed);
                    }
                }
                #[cfg(test)]
                observer.before_wake_read().await;
                let loops = owners.streams();
                // A registration wakes the next read.
                if loops.is_empty() {
                    continue;
                }
                let runs: Vec<_> = loops
                    .iter()
                    .map(|(workspace, owner)| (workspace.clone(), owner.run_id().clone()))
                    .collect();
                let heads = match run_repository::heads(&database, &runs).await {
                    Ok(heads) => heads,
                    Err(error) => {
                        tracing::warn!(
                            error.type = safe::kind(error.code()),
                            "the wake-up reader's read of its Runs' heads failed; the next read tries again"
                        );
                        continue;
                    }
                };
                for (_, owner) in &loops {
                    if heads
                        .get(owner.run_id())
                        .is_some_and(|run| owner.behind(run.head, run.running))
                    {
                        owner.send(Message::Woken);
                        #[cfg(test)]
                        observer.wake_delivered(turn);
                    }
                }
            }
        });
        Self {
            task,
            owners: registered,
        }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.task.abort();
    }
}
