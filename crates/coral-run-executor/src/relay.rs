//! Compiles relayed through the database (ADR 0027): Coral Instances talk
//! only through it. A Coral Instance that holds no pull stream with room
//! writes a row and waits on the change signal for its answer; every Coral
//! Instance that holds one with room, itself included, takes the oldest
//! row, serves it on that stream, and writes the answer back.
//!
//! A row with no call is a probe: [`crate::CompilerClient::ready`] waits on
//! one until some Coral Instance has room, before the compile's own time
//! starts. A pickup answers it at once and holds nothing for it.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use coral_db::{ChangeSubscription, Database};
use coral_telemetry::safe;
use tokio::time::Instant;

use crate::compile_wire::{ProtocolError, Read, Takes};
use crate::compiler::STOPPED;
use crate::pull::millis;
use crate::relay_repository::{self as rows, Answered, Request, Status, Taken};
use crate::wire::v1::compile::Call;
use crate::{CompileError, LocalRunExecutors, PullStreams, Refused, StreamId, Unavailable};

/// How long a waiter or a pickup waits for a signal before it looks again,
/// the backstop for a notification outage, and how often a waiter asks the
/// local children for room again.
const LOOK_AGAIN: Duration = Duration::from_secs(1);

/// The time a pickup leaves before a row's deadline to write the answer
/// and for its requester to read it.
const ANSWER_RESERVE: Duration = Duration::from_millis(100);

/// This Coral Instance's side of the relay.
#[derive(Clone)]
pub(crate) struct CompileRelay {
    database: Arc<Database>,
    instance: Arc<str>,
}

/// How a requester's wait ended.
enum Waited {
    Answered(Result<Answered, ProtocolError>),
    /// Its deadline came first; `taken` when a pickup had sent it.
    Unanswered {
        taken: bool,
    },
}

impl CompileRelay {
    pub(crate) fn new(database: Arc<Database>, instance: &str) -> Self {
        Self {
            database,
            instance: Arc::from(instance),
        }
    }

    /// Whether a Coral Instance with room answered a probe by `until`.
    pub(crate) async fn room(&self, children: Option<&LocalRunExecutors>, until: Instant) -> bool {
        matches!(
            self.wait(None, children, until).await,
            Waited::Answered(Ok(_))
        )
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
            Waited::Answered(Ok(Answered::Call(answer))) => {
                let read = Read::answer(answer)?;
                if !read.answers(Takes::of(call)) {
                    return Err(ProtocolError.into());
                }
                match read {
                    Read::Failure(error) => Err(error),
                    read => Ok(read),
                }
            }
            Waited::Answered(Ok(Answered::Room) | Err(ProtocolError)) => Err(ProtocolError.into()),
        }
    }

    /// Writes a row for `call`, none for a probe, with the deadline
    /// `until`, and waits that long for its answer, asking `children` for
    /// room every [`LOOK_AGAIN`]. The row is gone when it returns.
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
        let deadline = now.saturating_add(nanos(until.saturating_duration_since(Instant::now())));
        let written = rows::request(&self.database, &id, &self.instance, call, now, deadline);
        if let Err(error) = written.await {
            tracing::warn!(
                error.type = safe::kind(error.code()),
                "a compile could not be relayed"
            );
            return Waited::Unanswered { taken: false };
        }
        let mut row = Withdraw(Some((self.database.clone(), id.clone())));
        let mut taken = false;
        let mut asked: Option<Instant> = None;
        loop {
            match rows::status(&self.database, &id).await {
                Ok(Status::Answered(answer)) => {
                    row.now().await;
                    return Waited::Answered(answer);
                }
                Ok(Status::Gone) => {
                    row.disarm();
                    return Waited::Unanswered { taken };
                }
                Ok(Status::Waiting { taken: now_taken }) => taken = now_taken,
                Err(error) => tracing::warn!(
                    error.type = safe::kind(error.code()),
                    "a relayed compile could not be read"
                ),
            }
            let left = until.saturating_duration_since(Instant::now());
            if left.is_zero() {
                row.now().await;
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
        loop {
            if streams.most_room().is_some() && self.pending().await {
                while let Some(stream) = streams.most_room() {
                    let reserve = nanos(ANSWER_RESERVE);
                    let taken =
                        rows::take_oldest(&self.database, &self.instance, unix_now(), reserve);
                    match taken.await {
                        Ok(Some(taken)) => self.serve_one(&streams, stream, taken).await,
                        Ok(None) => break,
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
            if streams.most_room().is_some() {
                signalled(&mut changes, &self.database, LOOK_AGAIN).await;
            } else {
                // A row written while no stream had room is taken once one has.
                streams.room_signalled(LOOK_AGAIN).await;
            }
        }
    }

    async fn pending(&self) -> bool {
        rows::pending(&self.database, unix_now())
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(
                    error.type = safe::kind(error.code()),
                    "relayed compiles could not be read"
                );
                false
            })
    }

    /// Answers a probe at once, and sends a call down `stream`, answering it
    /// once its `Compiled` arrives. A call the stream refused goes back.
    async fn serve_one(&self, streams: &PullStreams, stream: StreamId, taken: Taken) {
        let Taken {
            id,
            request,
            deadline,
        } = taken;
        let call = match request {
            Request::Room => return self.answer(&id, Answered::Room).await,
            Request::Unreadable => return self.answer(&id, failure(ProtocolError.into())).await,
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
            }
            Err(Refused::NoRoom | Refused::Closed) => {
                if let Err(error) = rows::release(&self.database, &id).await {
                    tracing::warn!(
                        error.type = safe::kind(error.code()),
                        "a relayed compile could not be given back"
                    );
                }
            }
            Err(Refused::TooLarge) => self.answer(&id, failure(ProtocolError.into())).await,
        }
    }

    async fn answer(&self, id: &str, answered: Answered) {
        if let Err(error) = rows::answer(&self.database, id, answered).await {
            tracing::warn!(
                error.type = safe::kind(error.code()),
                "a relayed compile's answer could not be written"
            );
        }
    }
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
    async fn now(&mut self) {
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
    if let Err(error) = rows::withdraw(database, id).await {
        tracing::warn!(
            error.type = safe::kind(error.code()),
            "a relayed compile's row could not be deleted"
        );
    }
}

/// Now, in Unix nanoseconds, as rows and leases record deadlines across
/// Coral Instances.
pub(crate) fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, nanos)
}

pub(crate) fn nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests;
