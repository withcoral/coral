//! Compiles relayed through the database: Coral Instances talk
//! only through it. A Coral Instance that holds no pull stream with room
//! writes a row and waits on the change signal for its answer; every Coral
//! Instance that holds one with room, itself included, takes the oldest row
//! it can serve, serves it on that stream, and writes the answer back.
//!
//! A row with no call is a probe: [`crate::CompilerClient::ready`] waits on
//! one until some Coral Instance has room, before the compile's own time
//! starts. A pickup holds one unit of its room for each probe it answers
//! for another Coral Instance, which that Coral Instance's next call spends.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use coral_db::{ChangeSubscription, Database, UnixNanos};
use coral_telemetry::safe;
use tokio::time::Instant;

use crate::compile_wire::{ProtocolError, Read, Takes};
use crate::compiler::STOPPED;
use crate::relay_repository::{self as rows, Answered, NewRequest, Request, Status, Take, Taken};
use crate::time::{before, millis, unix_now};
use crate::wire::v1::compile::Call;
use crate::{CompileError, LocalRunExecutors, PullStreams, Refused, StreamId, Unavailable};

/// How long a waiter or a pickup waits for a signal before it looks again,
/// the backstop for a notification outage, and how often a waiter asks the
/// local children for room again.
const LOOK_AGAIN: Duration = Duration::from_secs(1);

/// The time a pickup leaves before a row's deadline to write the answer
/// and for its requester to read it, the [`TAKEN_RESERVE`] included.
const ANSWER_RESERVE: Duration = Duration::from_millis(100);

/// How long before its deadline a requester stops waiting for a row a pickup
/// took, so that its caller hears the taker stopped before its own deadline
/// ends the wait. A live taker answers [`ANSWER_RESERVE`] before the
/// deadline, so an answer slower than the difference reads as stopped.
const TAKEN_RESERVE: Duration = Duration::from_millis(30);

/// How long before its deadline a requester stops waiting for a row nobody
/// took, so that its caller hears no compiler took the call before its own
/// deadline ends the wait. A pickup takes no row past that point.
const UNTAKEN_RESERVE: Duration = Duration::from_millis(100);

/// How long past its deadline a row stays before a pickup deletes it. Its
/// requester deletes it first unless the requester stopped.
const ORPHAN_GRACE: Duration = Duration::from_secs(5);

/// How long a pickup holds a unit for a probe it answered for another Coral
/// Instance, while that Coral Instance's call is on its way.
const PROMISE_KEPT: Duration = Duration::from_secs(1);

/// This Coral Instance's side of the relay.
#[derive(Clone)]
pub(crate) struct CompileRelay {
    database: Arc<Database>,
    instance: Arc<str>,
    /// How long a pickup holds a unit for a probe it answered for another
    /// Coral Instance: [`PROMISE_KEPT`], except in tests.
    promise_kept: Duration,
    /// How long before a call's deadline a pickup's compile ends:
    /// [`ANSWER_RESERVE`], except in tests.
    answer_reserve: Duration,
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
            promise_kept: PROMISE_KEPT,
            answer_reserve: ANSWER_RESERVE,
        }
    }

    /// This relay, holding a unit for a probe it answered for `kept`.
    #[cfg(test)]
    pub(crate) const fn with_promise_kept(mut self, kept: Duration) -> Self {
        self.promise_kept = kept;
        self
    }

    /// This relay, ending a compile it serves `reserve` before the call's
    /// deadline.
    #[cfg(test)]
    pub(crate) const fn with_answer_reserve(mut self, reserve: Duration) -> Self {
        self.answer_reserve = reserve;
        self
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

    /// Relays `call` and returns its answer before `until`. A call nobody
    /// took fails as no compiler. A live taker answers [`ANSWER_RESERVE`]
    /// before `until`, a compile timeout included, so one taken and
    /// unanswered [`TAKEN_RESERVE`] before it fails as a stopped compiler.
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
            Waited::Unanswered { taken: true } => Err(STOPPED),
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
    /// took it, and until [`TAKEN_RESERVE`] before `until` once a pickup did.
    /// Its write and reads end by then; the delete of its row after them may
    /// take longer. The row is gone when it returns, unless that delete
    /// failed.
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
            deadline: now.saturating_add(until.saturating_duration_since(Instant::now())),
        };
        // Armed first: a write that lands after the wait gave up is deleted.
        let mut row = Withdraw(Some((self.database.clone(), id.clone())));
        let untaken_until = until.checked_sub(UNTAKEN_RESERVE).unwrap_or(until);
        let taken_until = until.checked_sub(TAKEN_RESERVE).unwrap_or(until);
        match tokio::time::timeout_at(untaken_until, self.request(row_values)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(
                    error.type = safe::kind(error.code()),
                    "a compile could not be relayed"
                );
                row.delete().await;
                return Waited::Unanswered { taken: false };
            }
            // The write may still land: the delete after it removes it.
            Err(_elapsed) => {
                row.delete().await;
                return Waited::Unanswered { taken: false };
            }
        }
        let mut taken = false;
        let mut asked: Option<Instant> = None;
        loop {
            // Bounded by the taken bound alone: the read after the last wait
            // runs at the untaken bound and still sees a take that just landed.
            let status = tokio::time::timeout_at(taken_until, rows::status(&self.database, &id));
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
            let give_up = if taken { taken_until } else { untaken_until };
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

    /// Serves the relayed compiles on `streams` while one has room, or holds
    /// room for a call it promised.
    pub(crate) async fn serve(self, streams: PullStreams) -> Infallible {
        let mut changes = self.database.subscribe_changes().ok();
        let mut promised = Promised::new(streams.clone(), self.promise_kept);
        loop {
            promised.lapse();
            if streams.most_room().is_some() || !promised.is_empty() {
                self.serve_pending(&streams, &mut promised).await;
            }
            // A lapsing promise frees its room.
            let wait = promised
                .next_lapse()
                .map_or(LOOK_AGAIN, |lapse| lapse.min(LOOK_AGAIN));
            if streams.most_room().is_some() {
                signalled(&mut changes, &self.database, wait).await;
            } else if !promised.is_empty() {
                // Its promised calls come as rows; probes wait for room.
                tokio::select! {
                    () = signalled(&mut changes, &self.database, wait) => {}
                    () = streams.room_signalled(wait) => {}
                }
            } else {
                // A row written while no stream had room is taken once one has.
                streams.room_signalled(LOOK_AGAIN).await;
            }
        }
    }

    /// Takes rows while a stream has room. With none free, it takes only the
    /// calls of the Coral Instances it holds room for. One read first, so a
    /// pickup with nothing to take takes no write lock.
    async fn serve_pending(&self, streams: &PullStreams, promised: &mut Promised) {
        loop {
            promised.lapse();
            let free = streams.most_room();
            let requesters = promised.requesters();
            let from = match free {
                Some(_) => None,
                None if !requesters.is_empty() => Some(requesters.as_slice()),
                None => return,
            };
            let now = unix_now();
            let take = Take {
                taker: &self.instance,
                deadline_after: now.saturating_add(UNTAKEN_RESERVE),
                from,
            };
            match rows::work_for(&self.database, &take, orphaned_before(now)).await {
                Ok(true) => {}
                Ok(false) => return,
                Err(error) => {
                    tracing::warn!(
                        error.type = safe::kind(error.code()),
                        "relayed compiles could not be read"
                    );
                    return;
                }
            }
            let taken = match self.take(from).await {
                Ok(Some(taken)) => taken,
                Ok(None) => return,
                Err(error) => {
                    tracing::warn!(
                        error.type = safe::kind(error.code()),
                        "a relayed compile could not be taken"
                    );
                    return;
                }
            };
            let foreign = taken.requester != *self.instance;
            match (taken.request, free) {
                (Request::Room, Some(stream)) if foreign => {
                    // Room held for the call that this answer invites.
                    if !promised.hold(taken.requester, stream) {
                        self.release(&taken.id).await;
                    } else if self.answer(&taken.id, Answered::Room).await {
                        promised.answered_newest();
                    } else {
                        promised.give_up_newest();
                        self.release(&taken.id).await;
                    }
                }
                (Request::Room, _) => {
                    self.answer(&taken.id, Answered::Room).await;
                }
                (Request::Unreadable, _) => {
                    self.answer(&taken.id, failed(ProtocolError.into())).await;
                }
                (Request::Call(call), free) => {
                    let via = match promised.spend(&taken.requester) {
                        Some(held) => Some(Via::Held(held)),
                        None => free.map(Via::Free),
                    };
                    match via {
                        Some(via) => {
                            self.send(streams, via, taken.id, call, taken.deadline)
                                .await;
                        }
                        None => self.release(&taken.id).await,
                    }
                }
            }
        }
    }

    /// Sends a taken call down a stream, and answers it once its `Compiled`
    /// arrives. A held unit that is gone falls back to free room; a call no
    /// stream takes goes back.
    async fn send(
        &self,
        streams: &PullStreams,
        via: Via,
        id: String,
        call: Call,
        deadline: UnixNanos,
    ) {
        let until = before(deadline, self.answer_reserve);
        let left = until.saturating_duration_since(unix_now());
        let time_left_ms = millis(left);
        let sent = match via {
            Via::Free(stream) => streams.compile(stream, time_left_ms, call),
            Via::Held(stream) => {
                let fallback = streams.most_room().map(|free| (free, call.clone()));
                match (streams.compile_held(stream, time_left_ms, call), fallback) {
                    (Err(Refused::NoRoom | Refused::Closed), Some((free, call))) => {
                        streams.compile(free, time_left_ms, call)
                    }
                    (sent, _) => sent,
                }
            }
        };
        match sent {
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
            }
            Err(Refused::NoRoom | Refused::Closed) => self.release(&id).await,
            Err(Refused::TooLarge) => {
                self.answer(&id, failed(ProtocolError.into())).await;
            }
        }
    }

    async fn request(&self, row: NewRequest<'_>) -> Result<(), coral_db::Error> {
        let mut transaction = self.database.begin().await?;
        rows::request(&mut transaction, row).await?;
        transaction.commit().await
    }

    /// Deletes the rows left over past their deadline and [`ORPHAN_GRACE`],
    /// then takes the oldest row its requester still waits for, only a call
    /// of one of `from` when it is set. The clock is read once the
    /// transaction has begun, so a wait to begin it cannot take a row inside
    /// its reserve.
    async fn take(&self, from: Option<&[String]>) -> Result<Option<Taken>, coral_db::Error> {
        let mut transaction = self.database.begin().await?;
        let now = unix_now();
        rows::delete_past(&mut transaction, orphaned_before(now)).await?;
        let take = Take {
            taker: &self.instance,
            deadline_after: now.saturating_add(UNTAKEN_RESERVE),
            from,
        };
        let taken = rows::take_oldest(&mut transaction, take).await?;
        transaction.commit().await?;
        Ok(taken)
    }

    /// Gives a taken row back for another pickup.
    async fn release(&self, id: &str) {
        let released = async {
            let mut transaction = self.database.begin().await?;
            rows::release(&mut transaction, id).await?;
            transaction.commit().await
        };
        if let Err(error) = released.await {
            tracing::warn!(
                error.type = safe::kind(error.code()),
                "a relayed compile could not be given back"
            );
        }
    }

    /// Writes the answer to row `id`; false when the write failed.
    async fn answer(&self, id: &str, answered: Answered) -> bool {
        let written = async {
            let mut transaction = self.database.begin().await?;
            rows::answer(&mut transaction, id, answered).await?;
            transaction.commit().await
        };
        written
            .await
            .inspect_err(|error| {
                tracing::warn!(
                    error.type = safe::kind(error.code()),
                    "a relayed compile's answer could not be written"
                );
            })
            .is_ok()
    }
}

/// The stream a taken call goes down.
enum Via {
    /// Its free room.
    Free(StreamId),
    /// A unit held for this call's Coral Instance.
    Held(StreamId),
}

/// The units a pickup holds for probes it answered for other Coral
/// Instances, until their calls come or the promise lapses. Another pickup
/// with free room may still take a promised Coral Instance's call; the unit
/// held for it then lapses unused. Dropped, it frees every unit it holds.
struct Promised {
    streams: PullStreams,
    kept: Duration,
    promises: VecDeque<Promise>,
}

struct Promise {
    made: Instant,
    requester: String,
    stream: StreamId,
}

impl Promised {
    const fn new(streams: PullStreams, kept: Duration) -> Self {
        Self {
            streams,
            kept,
            promises: VecDeque::new(),
        }
    }

    /// Holds a unit of `stream` for `requester`'s call, unless another
    /// caller took the room first.
    fn hold(&mut self, requester: String, stream: StreamId) -> bool {
        if self.streams.hold(stream).is_err() {
            return false;
        }
        self.promises.push_back(Promise {
            made: Instant::now(),
            requester,
            stream,
        });
        true
    }

    /// Starts the promise held last from now: its probe's answer is written.
    fn answered_newest(&mut self) {
        if let Some(promise) = self.promises.back_mut() {
            promise.made = Instant::now();
        }
    }

    /// Frees the unit held last: its probe's answer was not written.
    fn give_up_newest(&mut self) {
        if let Some(promise) = self.promises.pop_back() {
            self.streams.let_go(promise.stream);
        }
    }

    /// The stream holding a unit for `requester`'s call, which the call
    /// spends.
    fn spend(&mut self, requester: &str) -> Option<StreamId> {
        let kept = self
            .promises
            .iter()
            .position(|promise| promise.requester == requester)?;
        self.promises.remove(kept).map(|promise| promise.stream)
    }

    /// Frees the units of the promises that lapsed.
    fn lapse(&mut self) {
        while let Some(promise) = self.promises.front()
            && promise.made.elapsed() >= self.kept
        {
            self.streams.let_go(promise.stream);
            self.promises.pop_front();
        }
    }

    fn is_empty(&self) -> bool {
        self.promises.is_empty()
    }

    fn requesters(&self) -> Vec<String> {
        let mut requesters: Vec<String> = self
            .promises
            .iter()
            .map(|promise| promise.requester.clone())
            .collect();
        requesters.sort_unstable();
        requesters.dedup();
        requesters
    }

    /// The time until the oldest promise lapses.
    fn next_lapse(&self) -> Option<Duration> {
        self.promises
            .front()
            .map(|promise| self.kept.saturating_sub(promise.made.elapsed()))
    }
}

impl Drop for Promised {
    fn drop(&mut self) {
        for promise in self.promises.drain(..) {
            self.streams.let_go(promise.stream);
        }
    }
}

/// The deadline before which a row is left over at `now`.
fn orphaned_before(now: UnixNanos) -> UnixNanos {
    before(now, ORPHAN_GRACE)
}

fn failed(error: CompileError) -> Answered {
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
    /// Deletes the row. Dropped before the delete lands, it deletes it again
    /// from a task of its own.
    async fn delete(&mut self) {
        if let Some((database, id)) = &self.0 {
            withdraw(database, id).await;
            self.0 = None;
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
