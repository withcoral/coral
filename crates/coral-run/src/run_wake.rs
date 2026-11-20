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
//! with its Run's retention. Per-Run loops never subscribe to the signal.

use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

use coral_db::Database;
use coral_telemetry::safe;
use tokio::sync::Notify;

use crate::engine::Clock;
use crate::observation::{Cadence, late_delivery_ms};
use crate::owner::{Message, Owners};
use crate::run_repository::{self, HeldWake};
use crate::{CoralInstanceId, RunId};

/// This Coral Instance's wake rows and their reader.
#[derive(Clone)]
pub(crate) struct RunWakes {
    database: Arc<Database>,
    woken: Arc<Notify>,
    reader: Arc<OnceLock<Reader>>,
}

impl RunWakes {
    pub(crate) fn new(database: Arc<Database>) -> Self {
        Self {
            database,
            woken: Arc::default(),
            reader: Arc::default(),
        }
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
            loop {
                #[cfg_attr(not(test), expect(unused_variables, reason = "tests observe it"))]
                let turn = cadence.turn(&database, &woken).await;
                #[cfg(test)]
                observer.before_wake_read().await;
                // A registration wakes the next read.
                if owners.is_empty() {
                    continue;
                }
                let wakes = match run_repository::wakes_held_by(&database, &instance).await {
                    Ok(wakes) => wakes,
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

/// Logs a wake row handed to its loop late after it was raised
/// ([`late_delivery_ms`]); `true` when it logged one.
fn warn_if_late(now_unix_nanos: i64, run: &crate::types::HeldRunId, wake: &HeldWake) -> bool {
    let Some(delay_ms) = late_delivery_ms(now_unix_nanos, wake.raised_at_unix_nanos) else {
        return false;
    };
    tracing::warn!(
        coral.run.id = safe::id(run),
        coral.run.request.delay_ms = safe::number(delay_ms),
        "wake row reached its Run's loop late"
    );
    true
}
