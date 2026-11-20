//! The wake rows' reader (ADR 0027, Outside requests and the wake row).
//!
//! A writer other than a Run's own loop that commits on a running Run a Run
//! Executor holds raises the Run's wake row in the same transaction: the
//! receiver of a decision or a cancellation, and the sweeper. One reader per
//! Coral Instance reads the wake rows of the Runs whose stream this instance
//! holds, after the content-free change signal rang, and hands each to the
//! loop registered here for its Run. The loop rereads its Run and deletes the
//! row in its next chunk commit. A row whose Run has no loop here stays: a
//! reopen of the Run's stream deletes it. After each read, the reader deletes
//! the rows it read of Runs that ended: no write follows a Run's end. While
//! no owner is registered here it reads nothing, and a row left then goes
//! with its Run's retention. Per-Run loops never subscribe to the signal: a
//! loop parked on a connection request hears its rings through the reader.
//! A Run Owner of an owned Run keeps a subscription of its own until the
//! owner path goes (#2911).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

use coral_db::Database;
use coral_telemetry::safe;
use tokio::sync::Notify;

use crate::engine::Clock;
use crate::observation::{Cadence, Turn, late_delivery_ms};
use crate::owner::{Message, OwnerHandle, Owners};
use crate::run_repository::{self, HeldWake};
use crate::{CoralInstanceId, Error, RunId};

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

/// This Coral Instance's wake rows and their reader.
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
    /// Instance's reader on the first, and has it read at once, for a row
    /// raised before the loop was here.
    pub(crate) fn loop_registered(
        &self,
        owners: &Owners,
        instance: &CoralInstanceId,
        clock: &Arc<dyn Clock>,
        #[cfg(test)] observer: &Arc<dyn crate::execution::ExecutionObserver>,
    ) {
        self.reader.get_or_init(|| {
            Reader::start(
                self.database.clone(),
                self.woken.clone(),
                self.listeners.clone(),
                owners.clone(),
                instance.clone(),
                clock.clone(),
                #[cfg(test)]
                observer.clone(),
            )
        });
        self.woken.notify_one();
    }

    /// Wakes this Coral Instance's reader when the loop of `run_id` is
    /// here: a row just raised reaches it without waiting for the signal.
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
        instance: CoralInstanceId,
        clock: Arc<dyn Clock>,
        #[cfg(test)] observer: Arc<dyn crate::execution::ExecutionObserver>,
    ) -> Self {
        let registered = owners.clone();
        let task = tokio::spawn(async move {
            let mut cadence = Cadence::default();
            // The rows already logged as late: a row is handed again on
            // every read until its loop deletes it, and is logged once.
            let mut logged = HashSet::new();
            // The wake rows already logged as undecodable, by their Run when
            // its id decodes: each is logged once while it stays.
            let mut skipped = HashSet::new();
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
                // A registration wakes the next read.
                if owners.is_empty() {
                    continue;
                }
                let wakes = match run_repository::wakes_held_by(&database, &instance).await {
                    Ok(wakes) => {
                        let (wakes, undecodable): (Vec<_>, Vec<_>) =
                            wakes.into_iter().partition(Result::is_ok);
                        log_undecodable(&mut skipped, undecodable);
                        wakes.into_iter().filter_map(Result::ok).collect::<Vec<_>>()
                    }
                    Err(error) => {
                        tracing::warn!(
                            error.type = safe::kind(error.code()),
                            "wake row read failed; the next read tries again"
                        );
                        continue;
                    }
                };
                let now = clock.now_unix_nanos().ok();
                let read: HashSet<&RunId> = wakes.iter().map(|wake| &wake.run_id).collect();
                logged.retain(|run: &RunId| read.contains(run));
                for wake in &wakes {
                    let Some(owner) = owners.get(&wake.run_id) else {
                        continue;
                    };
                    if let Some(now) = now
                        && !logged.contains(&wake.run_id)
                        && warn_if_late(now, owner.held_id(), wake)
                    {
                        logged.insert(wake.run_id.clone());
                    }
                    owner.send(Message::Woken);
                    #[cfg(test)]
                    observer.wake_delivered(turn);
                }
                let ended: Vec<HeldWake> = wakes.into_iter().filter(|wake| wake.ended).collect();
                if let Err(error) = run_repository::clear_ended_wakes(&database, &ended).await {
                    tracing::warn!(
                        error.type = safe::kind(error.code()),
                        "ended Runs' wake rows were not deleted; the next read tries again"
                    );
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

/// Logs each wake row in `undecodable` whose Run's stored row does not
/// decode, once while it stays: `skipped` holds the ones logged, and forgets
/// the ones this read did not find. The next read reads them again.
fn log_undecodable(
    skipped: &mut HashSet<Option<RunId>>,
    undecodable: Vec<Result<HeldWake, run_repository::UndecodableRun>>,
) {
    let undecodable: Vec<_> = undecodable.into_iter().filter_map(Result::err).collect();
    let read: HashSet<Option<RunId>> = undecodable
        .iter()
        .map(|row| row.id.as_ref().map(|id| id.run_id().clone()))
        .collect();
    skipped.retain(|run| read.contains(run));
    for row in undecodable {
        if skipped.insert(row.id.as_ref().map(|id| id.run_id().clone())) {
            tracing::warn!(
                coral.run.id = row.id.as_ref().map(safe::id),
                error.type = safe::kind(row.error.code()),
                "the wake reader skips a wake row whose Run's stored row does not decode"
            );
        }
    }
}

/// Logs a wake row handed to its loop late after it was raised
/// ([`late_delivery_ms`]); `true` when it logged one.
fn warn_if_late(now_unix_nanos: i64, run: &crate::types::HeldRunId, wake: &HeldWake) -> bool {
    let Some(delay_ms) = late_delivery_ms(now_unix_nanos, wake.raised_at_unix_nanos.get()) else {
        return false;
    };
    tracing::warn!(
        coral.run.id = safe::id(run),
        coral.run.request.delay_ms = safe::number(delay_ms),
        "wake row reached its Run's loop late"
    );
    true
}
