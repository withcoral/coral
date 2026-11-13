//! The Run Inbox (ADR 0025): where an outside Run Request waits for its
//! Run's Run Owner, whichever Coral Instance received it: a cancellation, or
//! a decision on one of the Run's Approval Requests.
//!
//! [`RunInbox`] holds the implementation bootstrap chose behind the `Inbox`
//! trait. Its two halves are the requester's (submit a request and wait for
//! its answer, read from the record) and the owning Coral Instance's (the
//! stream of requests addressed to the Runs it holds, and the expired-lease
//! sweep's read of one Run's requests for the owner it recreates). Every
//! implementation promises that requests are addressed by Run, that there is
//! one per decided thing and the first wins, that each is delivered at least
//! once, and that one is removed only by the chunk commit that records it.
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
use coral_telemetry::safe;
use coral_workspace::{Principal, WorkspaceId};
use tokio::sync::{Mutex, Notify};
use tokio::time::{Instant, sleep_until};

use crate::engine::Clock;
use crate::observation::read_until;
use crate::owner::{DecisionRequest, Message, Owners};
use crate::types::HeldRunId;
use crate::{CoralInstanceId, Error, RunId, run_repository};

/// The shortest gap between two reads of the Run Inbox.
const READ_GAP: Duration = Duration::from_millis(250);
/// The longest gap between two reads: the backstop for a change signal
/// that does not ring.
const BACKSTOP: Duration = Duration::from_secs(15);
/// A request that reaches its Run Owner later than this after it was
/// submitted is logged, so a slow delivery can be told from a slow owner.
const LATE_DELIVERY: Duration = Duration::from_secs(1);

/// One outside Run Request as the Run Inbox carries it, addressed to its Run.
#[derive(Debug, Clone)]
pub(crate) struct InboxRequest {
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) run_id: RunId,
    pub(crate) request: OutsideRequest,
}

/// What an outside Run Request asks the Run Owner to decide.
#[derive(Debug, Clone)]
pub(crate) enum OutsideRequest {
    /// The Run's cancellation, by this requester.
    Cancel(Principal),
    /// A decision on one Approval Request of the Run.
    Decide(DecisionRequest),
}

impl OutsideRequest {
    /// The request's kind, for logs.
    pub(crate) const fn kind(&self) -> &'static str {
        match self {
            Self::Cancel(_) => "cancel",
            Self::Decide(_) => "decision",
        }
    }
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
    /// When the requester put it in the Run Inbox, on the Run Module's clock.
    pub(crate) submitted_at_unix_nanos: i64,
}

impl Delivery {
    /// The message that hands the request to its Run Owner.
    pub(crate) fn into_message(self) -> Message {
        let Self { row, request, .. } = self;
        match request.request {
            OutsideRequest::Cancel(requester) => Message::Cancel { requester, row },
            OutsideRequest::Decide(request) => Message::Decide { request, row },
        }
    }
}

#[async_trait]
trait Inbox: Send + Sync {
    /// Puts `request` in its Run's Run Inbox at `submitted_at_unix_nanos`,
    /// in `transaction`, which checked the requester's access: nothing when
    /// the Run is not running, a cancellation's Run has its termination
    /// recorded, or a request for the same decided thing waits already.
    async fn submit(
        &self,
        transaction: &mut Transaction<'_>,
        request: &InboxRequest,
        submitted_at_unix_nanos: i64,
    ) -> Result<(), Error>;

    /// Waits until `until` for the submitted `request` to be Recorded:
    /// `true` once it is, or once nothing waits for it (the record answers);
    /// `false` past `until`.
    async fn recorded(&self, request: &InboxRequest, until: Instant) -> Result<bool, Error>;

    /// The next requests addressed to the Runs `instance` holds, in insert
    /// order, once there may be new ones.
    async fn receive(&self, instance: &CoralInstanceId) -> Vec<Delivery>;

    /// The requests waiting for the running Run `run_id`, in insert order,
    /// read in `transaction`: the expired-lease sweep's, for the owner it
    /// recreates.
    async fn waiting(
        &self,
        transaction: &mut Transaction<'_>,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
    ) -> Result<Vec<Delivery>, Error>;

    /// Wakes the stream at once: the owner of a request just submitted is
    /// local, and gets it without waiting for the change signal.
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
        transaction: &mut Transaction<'_>,
        request: &InboxRequest,
        submitted_at_unix_nanos: i64,
    ) -> Result<(), Error> {
        self.inbox
            .submit(transaction, request, submitted_at_unix_nanos)
            .await
    }

    /// Waits until `until` for the submitted `request` to be Recorded. A
    /// wait that runs out is logged: with the Run Inbox reader's late-delivery
    /// log, or its absence, it says whether delivery or the owner was slow.
    pub(crate) async fn recorded(
        &self,
        request: &InboxRequest,
        until: Instant,
    ) -> Result<bool, Error> {
        let waiting = Instant::now();
        let recorded = self.inbox.recorded(request, until).await?;
        if !recorded {
            let waited = waiting.elapsed().as_millis();
            tracing::warn!(
                coral.workspace.id = safe::id(&request.workspace_id),
                coral.run.request.kind = safe::kind(request.request.kind()),
                coral.run.request.delay_ms =
                    safe::number(i64::try_from(waited).unwrap_or(i64::MAX)),
                "Run Owner did not record an outside request within its requester's wait"
            );
        }
        Ok(recorded)
    }

    /// The requests waiting for the running Run `run_id`, in insert order,
    /// read in `transaction`.
    pub(crate) async fn waiting(
        &self,
        transaction: &mut Transaction<'_>,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
    ) -> Result<Vec<Delivery>, Error> {
        self.inbox.waiting(transaction, workspace_id, run_id).await
    }

    /// Wakes this Coral Instance's reader when the owner of `run_id` is
    /// here: it gets a request just submitted without waiting for the
    /// change signal.
    pub(crate) fn wake_for(&self, run_id: &RunId) {
        if self
            .reader
            .get()
            .is_some_and(|reader| reader.owners.contains(run_id))
        {
            self.inbox.wake();
        }
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
        clock: &Arc<dyn Clock>,
        #[cfg(test)] observer: &Arc<dyn crate::execution::ExecutionObserver>,
    ) {
        let reader = self.reader.get_or_init(|| {
            Reader::start(
                self.inbox.clone(),
                owners.clone(),
                instance.clone(),
                clock.clone(),
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
    /// The Run Owners it hands requests to.
    owners: Owners,
}

impl Reader {
    fn start(
        inbox: Arc<dyn Inbox>,
        owners: Owners,
        instance: CoralInstanceId,
        clock: Arc<dyn Clock>,
        #[cfg(test)] observer: Arc<dyn crate::execution::ExecutionObserver>,
    ) -> Self {
        let holding = Arc::new(AtomicBool::new(false));
        let held = holding.clone();
        let registered = owners.clone();
        let task = tokio::spawn(async move {
            loop {
                let deliveries = inbox.receive(&instance).await;
                // Set before looking for the owners, so an owner that
                // registers meanwhile wakes the next read.
                held.store(true, Ordering::SeqCst);
                let mut holds = false;
                for delivery in deliveries {
                    let Some(owner) = owners.get(&delivery.request.run_id) else {
                        holds = true;
                        continue;
                    };
                    warn_if_late(clock.as_ref(), owner.held_id(), &delivery);
                    owner.send(delivery.into_message());
                    #[cfg(test)]
                    observer.inbox_delivered();
                }
                held.store(holds, Ordering::SeqCst);
            }
        });
        Self {
            task,
            holding,
            owners: registered,
        }
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
        transaction: &mut Transaction<'_>,
        request: &InboxRequest,
        submitted_at_unix_nanos: i64,
    ) -> Result<(), Error> {
        run_repository::insert_inbox_request(transaction, request, submitted_at_unix_nanos).await
    }

    async fn recorded(&self, request: &InboxRequest, until: Instant) -> Result<bool, Error> {
        let database = &self.database;
        read_until(database, until, move || async move {
            run_repository::inbox_request_waits(database, request)
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
                    tracing::warn!(
                        error.type = safe::kind(error.code()),
                        "Run Inbox read failed; the next read tries again"
                    );
                }
            }
        }
    }

    async fn waiting(
        &self,
        transaction: &mut Transaction<'_>,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
    ) -> Result<Vec<Delivery>, Error> {
        run_repository::inbox_requests_waiting_for(transaction, workspace_id, run_id).await
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

/// Logs a request that reached its Run Owner later than [`LATE_DELIVERY`]
/// after it was submitted. Its requester waits a few seconds for the record;
/// this tells a slow delivery from a slow owner when that wait runs out.
/// Between hosts the gap also holds their clocks' difference.
fn warn_if_late(clock: &dyn Clock, run: &HeldRunId, delivery: &Delivery) {
    let Ok(now) = clock.now_unix_nanos() else {
        return;
    };
    let delay_ms = now.saturating_sub(delivery.submitted_at_unix_nanos) / 1_000_000;
    if delay_ms > i64::try_from(LATE_DELIVERY.as_millis()).unwrap_or(i64::MAX) {
        tracing::warn!(
            coral.run.id = safe::id(run),
            coral.run.request.kind = safe::kind(delivery.request.request.kind()),
            coral.run.request.delay_ms = safe::number(delay_ms),
            "Run Inbox delivered a request to its Run Owner late"
        );
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
