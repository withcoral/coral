//! The Run Inbox (ADR 0025): where an outside Run Request waits for its
//! Run's Run Owner, whichever Coral Instance received it. Only cancellations
//! take this path so far.
//!
//! [`RunInbox`] holds the implementation bootstrap chose behind the `Inbox`
//! trait. Its two halves are the requester's (submit a request and wait for
//! its answer, read from the record) and the owning Coral Instance's (the
//! stream of requests addressed to the Runs it holds). Every implementation
//! promises that requests are addressed by Run, that there is one per
//! decided thing and the first wins, that each is delivered at least once,
//! and that one is removed only by the chunk commit that records it.
//!
//! One reader per Coral Instance hands each delivered request to the Run
//! Owner registered here for its Run. A request for a Run this instance
//! holds whose owner is not registered yet (acceptance writes the claim
//! before `launch` registers the owner) stays in the Run Inbox, and the
//! registration wakes the reader to read it again.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use coral_db::{ChangeSubscription, Database, Transaction};
use coral_workspace::{Principal, WorkspaceId};
use tokio::sync::{Mutex, Notify};
use tokio::time::{Instant, sleep_until, timeout_at};

use crate::observation::OBSERVE_MIN_REREAD_GAP;
use crate::owner::{Message, Owners};
use crate::{CoralInstanceId, Error, RunId, run_repository};

/// The shortest gap between two reads of the Run Inbox.
const READ_GAP: Duration = Duration::from_millis(250);
/// The longest gap between two reads: the backstop for a change signal
/// that does not ring.
const BACKSTOP: Duration = Duration::from_secs(15);

/// One outside Run Request as the Run Inbox carries it: a cancellation of
/// its Run, by the requester.
#[derive(Debug, Clone)]
pub(crate) struct InboxRequest {
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) run_id: RunId,
    pub(crate) requester: Principal,
}

/// The row that carried one delivered request: the owner removes it by this
/// identity, and knows by it a request delivered again.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct InboxRow(pub(crate) String);

/// One request the Run Inbox delivered, with the row that carried it.
#[derive(Debug)]
pub(crate) struct Delivery {
    pub(crate) row: InboxRow,
    pub(crate) request: InboxRequest,
}

#[async_trait]
trait Inbox: Send + Sync {
    /// Puts `request` in its Run's Run Inbox at `submitted_at_unix_nanos`,
    /// wakes this Coral Instance's stream at once (so a local owner gets it
    /// without waiting for the change signal), and waits until `until` for
    /// the request to be Recorded: `true` once it is, or once the Run ended
    /// without it and the record answers; `false` past `until`.
    async fn submit(
        &self,
        request: &InboxRequest,
        submitted_at_unix_nanos: i64,
        until: Instant,
    ) -> Result<bool, Error>;

    /// The next requests addressed to the Runs `instance` holds, in insert
    /// order, once there may be new ones.
    async fn receive(&self, instance: &CoralInstanceId) -> Vec<Delivery>;

    /// Wakes the stream at once.
    fn wake(&self);

    /// Removes the Run's `rows` inside the chunk commit that records them.
    async fn remove(
        &self,
        transaction: &mut Transaction<'_>,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
        rows: &[InboxRow],
    ) -> Result<(), Error>;
}

/// Where outside Run Requests wait for their Run Owner (ADR 0025), as
/// bootstrap chose to keep them.
#[derive(Clone)]
pub struct RunInbox {
    inbox: Arc<dyn Inbox>,
    reader: Arc<OnceLock<Reader>>,
}

impl RunInbox {
    /// The Run Inbox as rows in `database`, read after its change signal
    /// rings.
    #[must_use]
    pub fn table(database: Arc<Database>) -> Self {
        Self {
            inbox: Arc::new(TableInbox {
                database,
                woken: Notify::new(),
                cadence: Mutex::default(),
            }),
            reader: Arc::default(),
        }
    }

    pub(crate) async fn submit(
        &self,
        request: &InboxRequest,
        submitted_at_unix_nanos: i64,
        until: Instant,
    ) -> Result<bool, Error> {
        self.inbox
            .submit(request, submitted_at_unix_nanos, until)
            .await
    }

    pub(crate) async fn remove(
        &self,
        transaction: &mut Transaction<'_>,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
        rows: &[InboxRow],
    ) -> Result<(), Error> {
        if rows.is_empty() {
            return Ok(());
        }
        self.inbox
            .remove(transaction, workspace_id, run_id, rows)
            .await
    }

    /// An owner registered in `owners`: starts this Coral Instance's reader
    /// on the first, and wakes it when it holds a request no owner here took.
    pub(crate) fn owner_registered(
        &self,
        owners: &Owners,
        instance: &CoralInstanceId,
        #[cfg(test)] observer: &Arc<dyn crate::execution::ExecutionObserver>,
    ) {
        let reader = self.reader.get_or_init(|| {
            Reader::start(
                self.inbox.clone(),
                owners.clone(),
                instance.clone(),
                #[cfg(test)]
                observer.clone(),
            )
        });
        if reader.holding.load(Ordering::SeqCst) {
            self.inbox.wake();
        }
    }
}

/// This Coral Instance's Run Inbox reader; the last handle on it stops it.
struct Reader {
    task: tokio::task::JoinHandle<()>,
    /// The last read found a request whose Run has no owner here yet.
    holding: Arc<AtomicBool>,
}

impl Reader {
    fn start(
        inbox: Arc<dyn Inbox>,
        owners: Owners,
        instance: CoralInstanceId,
        #[cfg(test)] observer: Arc<dyn crate::execution::ExecutionObserver>,
    ) -> Self {
        let holding = Arc::new(AtomicBool::new(false));
        let held = holding.clone();
        let task = tokio::spawn(async move {
            loop {
                let deliveries = inbox.receive(&instance).await;
                // Set before looking for the owners, so an owner that
                // registers meanwhile wakes the next read.
                held.store(true, Ordering::SeqCst);
                let mut holds = false;
                for Delivery { row, request } in deliveries {
                    let Some(owner) = owners.get(&request.run_id) else {
                        holds = true;
                        continue;
                    };
                    owner.send(Message::Cancel {
                        requester: request.requester,
                        row,
                    });
                    #[cfg(test)]
                    observer.inbox_delivered();
                }
                held.store(holds, Ordering::SeqCst);
            }
        });
        Self { task, holding }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The Run Inbox as rows in the Coral database.
struct TableInbox {
    database: Arc<Database>,
    woken: Notify,
    cadence: Mutex<Cadence>,
}

/// When the stream reads: on subscribe; after a ring of the change signal
/// (the first after an outage included), at most once per gap; at once on
/// a wake; and at the backstop.
#[derive(Default)]
struct Cadence {
    changes: Option<ChangeSubscription>,
    last_read: Option<Instant>,
}

impl Cadence {
    async fn turn(&mut self, database: &Database, woken: &Notify) {
        if let Some(last_read) = self.last_read {
            let changes = &mut self.changes;
            tokio::select! {
                () = async {
                    rung(changes).await;
                    sleep_until(last_read + READ_GAP).await;
                } => {}
                () = woken.notified() => {}
                () = sleep_until(last_read + BACKSTOP) => {}
            }
        }
        if self.changes.is_none() {
            self.changes = database.subscribe_changes().ok();
        }
        self.last_read = Some(Instant::now());
    }
}

#[async_trait]
impl Inbox for TableInbox {
    async fn submit(
        &self,
        request: &InboxRequest,
        submitted_at_unix_nanos: i64,
        until: Instant,
    ) -> Result<bool, Error> {
        run_repository::insert_cancellation(&self.database, request, submitted_at_unix_nanos)
            .await?;
        self.wake();
        let database = &self.database;
        read_until(database, until, move || async move {
            run_repository::cancellation_waits(database, &request.workspace_id, &request.run_id)
                .await
                .map(|waits| !waits)
        })
        .await
    }

    async fn receive(&self, instance: &CoralInstanceId) -> Vec<Delivery> {
        let mut cadence = self.cadence.lock().await;
        loop {
            cadence.turn(&self.database, &self.woken).await;
            match run_repository::held_by(&self.database, instance).await {
                Ok(deliveries) => return deliveries,
                Err(error) => {
                    tracing::warn!(%error, "Run Inbox read failed; the next read tries again");
                }
            }
        }
    }

    fn wake(&self) {
        self.woken.notify_one();
    }

    async fn remove(
        &self,
        transaction: &mut Transaction<'_>,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
        rows: &[InboxRow],
    ) -> Result<(), Error> {
        run_repository::delete_inbox_rows(transaction, workspace_id, run_id, rows).await
    }
}

/// Reads `settled` again after every ring of the change signal, at most once
/// per reread gap, until it holds, or until `until`, when it reads it one
/// last time.
pub(crate) async fn read_until<Read>(
    database: &Database,
    until: Instant,
    mut settled: impl FnMut() -> Read,
) -> Result<bool, Error>
where
    Read: Future<Output = Result<bool, Error>>,
{
    let mut changes = database.subscribe_changes().ok();
    loop {
        let read_at = Instant::now();
        if settled().await? {
            return Ok(true);
        }
        let next = async {
            rung(&mut changes).await;
            sleep_until(read_at + OBSERVE_MIN_REREAD_GAP).await;
        };
        if timeout_at(until, next).await.is_err() {
            return settled().await;
        }
    }
}

/// Returns once the change signal rings, or an outage of it begins (the
/// next call then waits for its return); never without a subscription.
async fn rung(changes: &mut Option<ChangeSubscription>) {
    match changes {
        Some(changes) => {
            let _rang_or_lost = changes.changed().await;
        }
        None => std::future::pending().await,
    }
}
