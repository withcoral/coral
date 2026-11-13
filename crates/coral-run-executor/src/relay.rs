//! Compiles relayed through the database (ADR 0027): Coral Instances talk
//! only through it. A Coral Instance that holds no pull stream with room
//! writes a row and waits on the change signal for its answer; every Coral
//! Instance that holds one with room, itself included, takes the oldest
//! row, serves it on that stream, and writes the answer back.
//!
//! A row with no call is a probe: [`crate::CompilerClient::ready`] waits on
//! one until some Coral Instance has room, before the compile's own time
//! starts. A pickup answers no more probes from other Coral Instances than
//! it has room for calls that have not yet arrived.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use coral_db::{ChangeSubscription, Database};
use coral_telemetry::safe;
use tokio::time::Instant;

use crate::compile_wire::{ProtocolError, Read, Takes};
use crate::compiler::STOPPED;
use crate::relay_repository::{self as rows, Answered, NewRequest, Request, Status, Take, Taken};
use crate::time::{millis, nanos, unix_now};
use crate::wire::v1::compile::Call;
use crate::{CompileError, LocalRunExecutors, PullStreams, Refused, StreamId, Unavailable};

/// How long a waiter or a pickup waits for a signal before it looks again,
/// the backstop for a notification outage, and how often a waiter asks the
/// local children for room again.
const LOOK_AGAIN: Duration = Duration::from_secs(1);

/// The time a pickup leaves before a row's deadline to write the answer
/// and for its requester to read it.
const ANSWER_RESERVE: Duration = Duration::from_millis(100);

/// How long before its deadline a requester stops waiting for a row nobody
/// took, so that its caller hears no compiler took the call before its own
/// deadline ends the wait. A pickup takes no row past that point.
const UNTAKEN_RESERVE: Duration = Duration::from_millis(100);

/// How long past its deadline a row stays before a pickup deletes it. Its
/// requester deletes it first unless the requester stopped.
const ORPHAN_GRACE: Duration = Duration::from_secs(5);

/// How long a pickup counts a probe it answered for another Coral Instance
/// against its room, while that requester's call is on its way.
const PROMISE_KEPT: Duration = Duration::from_secs(1);

/// This Coral Instance's side of the relay.
#[derive(Clone)]
pub(crate) struct CompileRelay {
    database: Arc<Database>,
    instance: Arc<str>,
}

/// Who had room for a probe.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Probed {
    /// This Coral Instance: its own room, to hold.
    Here,
    /// Another Coral Instance: the calls relay.
    Elsewhere,
    /// Nobody, by the deadline.
    Nobody,
}

/// How a requester's wait ended.
enum Waited {
    Answered {
        answer: Result<Answered, ProtocolError>,
        by: Option<String>,
    },
    /// No answer: its write failed or its deadline came first. `taken`
    /// when a pickup held the row at the last read.
    Unanswered { taken: bool },
}

impl CompileRelay {
    pub(crate) fn new(database: Arc<Database>, instance: &str) -> Self {
        Self {
            database,
            instance: Arc::from(instance),
        }
    }

    /// Waits up to `until` for a Coral Instance with room to answer a probe.
    pub(crate) async fn room(
        &self,
        children: Option<&LocalRunExecutors>,
        until: Instant,
    ) -> Probed {
        match self.wait(None, children, until).await {
            Waited::Answered { answer: Ok(_), by } if by.as_deref() == Some(&*self.instance) => {
                Probed::Here
            }
            Waited::Answered { answer: Ok(_), .. } => Probed::Elsewhere,
            Waited::Answered { answer: Err(_), .. } | Waited::Unanswered { .. } => Probed::Nobody,
        }
    }

    /// Relays `call` and returns its answer by `until`. A call nobody took
    /// fails as no compiler; one taken and unanswered, as timed out.
    pub(crate) async fn call(
        &self,
        call: &Call,
        children: Option<&LocalRunExecutors>,
        until: Instant,
    ) -> Result<Read, CompileError> {
        match self.wait(Some(call), children, until).await {
            Waited::Unanswered { taken: false } => {
                Err(CompileError::Unavailable(Unavailable::NoCompiler))
            }
            Waited::Unanswered { taken: true } => Err(CompileError::TimedOut),
            Waited::Answered {
                answer: Ok(Answered::Call(answer)),
                ..
            } => {
                let read = Read::answer(answer)?;
                if !read.answers(Takes::of(call)) {
                    return Err(ProtocolError.into());
                }
                match read {
                    Read::Failure(error) => Err(error),
                    read => Ok(read),
                }
            }
            Waited::Answered {
                answer: Ok(Answered::Room) | Err(ProtocolError),
                ..
            } => Err(ProtocolError.into()),
        }
    }

    /// Writes a row for `call`, none for a probe, with the deadline
    /// `until`, and waits for its answer, asking `children` for room every
    /// [`LOOK_AGAIN`]: until [`UNTAKEN_RESERVE`] before `until` while nobody
    /// took it, and until `until` once a pickup did. No database wait runs
    /// past `until`. The row is gone when it returns.
    async fn wait(
        &self,
        call: Option<&Call>,
        children: Option<&LocalRunExecutors>,
        until: Instant,
    ) -> Waited {
        let id = uuid::Uuid::new_v4().to_string();
        // Subscribed before the write, so an answer after it is not missed.
        let mut changes = self.database.subscribe_changes().ok();
        let now = unix_now();
        let row_values = NewRequest {
            id: &id,
            requester: &self.instance,
            call,
            created_at: now,
            deadline: now.saturating_add(nanos(until.saturating_duration_since(Instant::now()))),
        };
        // Armed first: a write that lands after the wait gave up is deleted.
        let mut row = Withdraw(Some((self.database.clone(), id.clone())));
        let untaken_until = until.checked_sub(UNTAKEN_RESERVE).unwrap_or(until);
        match tokio::time::timeout_at(untaken_until, self.request(row_values)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(
                    error.type = safe::kind(error.code()),
                    "a compile could not be relayed"
                );
                return Waited::Unanswered { taken: false };
            }
            Err(_elapsed) => return Waited::Unanswered { taken: false },
        }
        let mut taken = false;
        let mut asked: Option<Instant> = None;
        loop {
            // Bounded by `until` alone: the read after the last wait runs at
            // the untaken bound and still sees a take that just landed.
            let status = tokio::time::timeout_at(until, rows::status(&self.database, &id));
            match status.await {
                Err(_elapsed) => {}
                Ok(Ok(Status::Answered { answer, by })) => {
                    row.delete().await;
                    return Waited::Answered { answer, by };
                }
                Ok(Ok(Status::Gone)) => {
                    row.disarm();
                    return Waited::Unanswered { taken };
                }
                Ok(Ok(Status::Waiting { taken: now_taken })) => taken = now_taken,
                Ok(Err(error)) => {
                    tracing::warn!(
                        error.type = safe::kind(error.code()),
                        "a relayed compile could not be read"
                    );
                }
            }
            let give_up = if taken { until } else { untaken_until };
            let left = give_up.saturating_duration_since(Instant::now());
            if left.is_zero() {
                row.delete().await;
                return Waited::Unanswered { taken };
            }
            if let Some(children) = children
                && asked.is_none_or(|asked| asked.elapsed() >= LOOK_AGAIN)
            {
                children.request_room();
                asked = Some(Instant::now());
            }
            signalled(&mut changes, &self.database, left.min(LOOK_AGAIN)).await;
        }
    }

    /// Serves the relayed compiles on `streams` while one has room.
    pub(crate) async fn serve(self, streams: PullStreams) -> Infallible {
        let mut changes = self.database.subscribe_changes().ok();
        let mut promised = Promised::default();
        loop {
            if streams.most_room().is_some() {
                self.serve_pending(&streams, &mut promised).await;
            }
            if streams.most_room().is_some() {
                // A lapsing promise frees room for a probe held back.
                let wait = promised
                    .next_lapse()
                    .map_or(LOOK_AGAIN, |lapse| lapse.min(LOOK_AGAIN));
                signalled(&mut changes, &self.database, wait).await;
            } else {
                // A row written while no stream had room is taken once one has.
                streams.room_signalled(LOOK_AGAIN).await;
            }
        }
    }

    /// Takes rows while a stream has room. Room already promised to another
    /// Coral Instance's probe takes only calls. One read first, so a pickup
    /// with nothing to take takes no write lock.
    async fn serve_pending(&self, streams: &PullStreams, promised: &mut Promised) {
        while let Some(stream) = streams.most_room() {
            promised.lapse();
            let free: u32 = streams.held().iter().map(|held| held.free).sum();
            let calls_only = usize::try_from(free).unwrap_or(usize::MAX) <= promised.len();
            let now = unix_now();
            let take = Take {
                taker: &self.instance,
                deadline_after: now.saturating_add(nanos(UNTAKEN_RESERVE)),
                calls_only,
            };
            match rows::pending(&self.database, &take, orphaned_before(now)).await {
                Ok(true) => {}
                Ok(false) => break,
                Err(error) => {
                    tracing::warn!(
                        error.type = safe::kind(error.code()),
                        "relayed compiles could not be read"
                    );
                    break;
                }
            }
            match self.take(take, orphaned_before(now)).await {
                Ok(None) => break,
                Ok(Some(taken)) => {
                    let requester = taken.requester.clone();
                    let foreign = requester != *self.instance;
                    match self.serve_one(streams, stream, taken).await {
                        Served::Probe if foreign => promised.make(requester),
                        Served::Call if foreign => promised.keep(&requester),
                        Served::Probe | Served::Call | Served::Other => {}
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        error.type = safe::kind(error.code()),
                        "a relayed compile could not be taken"
                    );
                    break;
                }
            }
        }
    }

    /// Answers a probe at once, and sends a call down `stream`, answering it
    /// once its `Compiled` arrives. A call the stream refused goes back.
    async fn serve_one(&self, streams: &PullStreams, stream: StreamId, taken: Taken) -> Served {
        let Taken {
            id,
            request,
            deadline,
            ..
        } = taken;
        let call = match request {
            Request::Room => {
                self.answer(&id, Answered::Room).await;
                return Served::Probe;
            }
            Request::Unreadable => {
                self.answer(&id, failure(ProtocolError.into())).await;
                return Served::Other;
            }
            Request::Call(call) => call,
        };
        let until = deadline.saturating_sub(nanos(ANSWER_RESERVE));
        let left = Duration::from_nanos(u64::try_from(until - unix_now()).unwrap_or(0));
        match streams.compile(stream, millis(left), call) {
            Ok(answered) => {
                let relay = self.clone();
                tokio::spawn(async move {
                    let read = match tokio::time::timeout(left, answered).await {
                        Ok(Ok(read)) => read,
                        Ok(Err(_closed)) => Read::Failure(STOPPED),
                        Err(_elapsed) => Read::Failure(CompileError::TimedOut),
                    };
                    relay.answer(&id, Answered::Call(read.into_answer())).await;
                });
                Served::Call
            }
            Err(Refused::NoRoom | Refused::Closed) => {
                if let Err(error) = self.release(&id).await {
                    tracing::warn!(
                        error.type = safe::kind(error.code()),
                        "a relayed compile could not be given back"
                    );
                }
                Served::Other
            }
            Err(Refused::TooLarge) => {
                self.answer(&id, failure(ProtocolError.into())).await;
                Served::Other
            }
        }
    }

    async fn request(&self, row: NewRequest<'_>) -> Result<(), coral_db::Error> {
        let mut transaction = self.database.begin().await?;
        rows::request(&mut transaction, row).await?;
        transaction.commit().await
    }

    /// Deletes the rows left over from before `expired`, then takes the
    /// oldest row that `take` names.
    async fn take(&self, take: Take<'_>, expired: i64) -> Result<Option<Taken>, coral_db::Error> {
        let mut transaction = self.database.begin().await?;
        rows::delete_past(&mut transaction, expired).await?;
        let taken = rows::take_oldest(&mut transaction, take).await?;
        transaction.commit().await?;
        Ok(taken)
    }

    async fn release(&self, id: &str) -> Result<(), coral_db::Error> {
        let mut transaction = self.database.begin().await?;
        rows::release(&mut transaction, id).await?;
        transaction.commit().await
    }

    async fn answer(&self, id: &str, answered: Answered) {
        let written = async {
            let mut transaction = self.database.begin().await?;
            rows::answer(&mut transaction, id, answered).await?;
            transaction.commit().await
        };
        if let Err(error) = written.await {
            tracing::warn!(
                error.type = safe::kind(error.code()),
                "a relayed compile's answer could not be written"
            );
        }
    }
}

/// What a pickup did with a row it took.
enum Served {
    /// Answered a probe.
    Probe,
    /// Sent a call down a stream.
    Call,
    /// Answered it with a failure, or gave it back.
    Other,
}

/// The probes a pickup answered for other Coral Instances whose calls have
/// not come yet: room it counts as spoken for, for [`PROMISE_KEPT`].
#[derive(Default)]
struct Promised(VecDeque<(Instant, String)>);

impl Promised {
    fn make(&mut self, requester: String) {
        self.0.push_back((Instant::now(), requester));
    }

    /// A call from `requester` came: one of its promises is kept.
    fn keep(&mut self, requester: &str) {
        if let Some(kept) = self
            .0
            .iter()
            .position(|(_, promised)| promised == requester)
        {
            self.0.remove(kept);
        }
    }

    fn lapse(&mut self) {
        self.0.retain(|(made, _)| made.elapsed() < PROMISE_KEPT);
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    /// The time until the oldest promise lapses.
    fn next_lapse(&self) -> Option<Duration> {
        self.0
            .front()
            .map(|(made, _)| PROMISE_KEPT.saturating_sub(made.elapsed()))
    }
}

/// The deadline before which a row is left over at `now`.
fn orphaned_before(now: i64) -> i64 {
    now.saturating_sub(nanos(ORPHAN_GRACE))
}

fn failure(error: CompileError) -> Answered {
    Answered::Call(Read::Failure(error).into_answer())
}

/// Waits for a change signal for at most `wait`. Without a subscription,
/// as during a notification outage, it subscribes again or sleeps.
async fn signalled(changes: &mut Option<ChangeSubscription>, database: &Database, wait: Duration) {
    if changes.is_none() {
        *changes = database.subscribe_changes().ok();
    }
    match changes {
        Some(subscription) => {
            if let Ok(Err(_outage)) = tokio::time::timeout(wait, subscription.changed()).await {
                *changes = None;
            }
        }
        None => tokio::time::sleep(wait).await,
    }
}

/// A requester's row, deleted when the requester stops waiting, even when
/// its caller drops the wait.
struct Withdraw(Option<(Arc<Database>, String)>);

impl Withdraw {
    async fn delete(&mut self) {
        if let Some((database, id)) = self.0.take() {
            withdraw(&database, &id).await;
        }
    }

    /// The row is already gone.
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for Withdraw {
    fn drop(&mut self) {
        if let Some((database, id)) = self.0.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move { withdraw(&database, &id).await });
        }
    }
}

async fn withdraw(database: &Database, id: &str) {
    let deleted = async {
        let mut transaction = database.begin().await?;
        rows::withdraw(&mut transaction, id).await?;
        transaction.commit().await
    };
    if let Err(error) = deleted.await {
        tracing::warn!(
            error.type = safe::kind(error.code()),
            "a relayed compile's row could not be deleted"
        );
    }
}

#[cfg(test)]
mod tests;
