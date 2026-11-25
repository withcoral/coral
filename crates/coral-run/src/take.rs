//! The take loop (ADR 0027): one per Coral Instance, for all its pull
//! streams. Each take claims the oldest waiting Runs for the stream with the
//! most room and sends them to its Run Executor as one `Taken`.

use std::convert::Infallible;
use std::time::Duration;

use coral_db::Transaction;
use coral_run_executor::wire::v1::TakenRun;
use coral_run_executor::{LocalRunExecutors, PullStreams};
use coral_telemetry::safe;
use tokio::time::Instant;

use crate::observation::rung;
use crate::run_repository::{self, Claimed, Taker};
use crate::{Error, RunModule};

/// The most Runs one take claims.
pub(crate) const TAKE_MAX: u32 = 16;

/// One pass per backstop while any stream has room, though nothing rang.
const BACKSTOP: Duration = Duration::from_secs(1);

/// When the next pass is due.
#[derive(Debug)]
pub(crate) struct Pace {
    backstop: Duration,
    /// The backstop after the last pass; none once the queue signal or new
    /// room rang since it, or before the first.
    next: Option<Instant>,
}

impl Pace {
    pub(crate) const fn new(backstop: Duration) -> Self {
        Self {
            backstop,
            next: None,
        }
    }

    pub(crate) const fn ring(&mut self) {
        self.next = None;
    }

    /// When the next pass is due, seen at `now`: none while no stream has
    /// room, `now` once something rang, and otherwise one backstop after
    /// the last pass.
    pub(crate) fn due(&self, room: bool, now: Instant) -> Option<Instant> {
        room.then(|| self.next.unwrap_or(now))
    }

    pub(crate) fn passed(&mut self, at: Instant) {
        self.next = Some(at + self.backstop);
    }
}

impl RunModule {
    /// Takes waiting Runs for `streams`, this Coral Instance's pull streams,
    /// for as long as it is polled (ADR 0027). Nothing starts a taken Run
    /// until its Run Executor opens its Run stream. While Runs wait and no
    /// stream has room, it asks `children`, the local child supervisor, for
    /// room, which keeps the limit and the back-off.
    pub async fn take_runs(
        &self,
        streams: PullStreams,
        children: Option<LocalRunExecutors>,
    ) -> Infallible {
        self.take_loop(streams, children, BACKSTOP).await
    }

    pub(crate) async fn take_loop(
        &self,
        streams: PullStreams,
        children: Option<LocalRunExecutors>,
        backstop: Duration,
    ) -> Infallible {
        // The queue signal, which rings only when a Run starts to wait, so
        // the loop reads the queue only then, at the backstop, and on new
        // room.
        let mut queue = None;
        let mut pace = Pace::new(backstop);
        // The queue changed since the children were last asked for room.
        let mut rang_since_ask = true;
        loop {
            if queue.is_none() {
                queue = self.database.subscribe_signal(run_repository::QUEUE).ok();
                // A Run may have started to wait while no subscription
                // listened: read the queue as if it rang.
                rang_since_ask = true;
                if queue.is_some() {
                    pace.ring();
                }
            }
            let now = Instant::now();
            match pace.due(streams.has_room(), now) {
                Some(due) if due <= now => {
                    pace.passed(now);
                    self.take_while_waiting(&streams).await;
                    // A take rings nothing, but a Run it left waiting asks
                    // the children for room.
                    rang_since_ask = true;
                }
                Some(due) => tokio::select! {
                    () = tokio::time::sleep_until(due) => {}
                    () = rung(&mut queue) => pace.ring(),
                },
                // New room is an ask, as the queue signal is. While no
                // stream has room, a Run that waits asks the children for
                // room, at most once a backstop and only after the queue
                // changed: an acceptance and a put-back ring the queue
                // signal, and a pass takes from it. The supervisor keeps
                // the ask until room opens. A compile, which comes before
                // every acceptance, has asked for the first child already.
                None => {
                    if let Some(children) = &children
                        && std::mem::take(&mut rang_since_ask)
                    {
                        match run_repository::any_waiting(&self.database).await {
                            Ok(true) => children.request_room(),
                            Ok(false) => {}
                            Err(error) => {
                                // The next backstop reads again.
                                rang_since_ask = true;
                                tracing::warn!(
                                    error.type = safe::kind(error.code()),
                                    "waiting Runs could not be read"
                                );
                            }
                        }
                    }
                    let room = streams.room_signalled(backstop);
                    tokio::pin!(room);
                    loop {
                        tokio::select! {
                            () = &mut room => break,
                            () = rung(&mut queue) => rang_since_ask = true,
                        }
                    }
                    pace.ring();
                }
            }
        }
    }

    /// One pass: takes while Runs wait and a stream has room.
    async fn take_while_waiting(&self, streams: &PullStreams) {
        loop {
            match self.take_once(streams).await {
                Ok(true) => {}
                Ok(false) => return,
                Err(error) => {
                    tracing::warn!(
                        error.type = safe::kind(error.code()),
                        "waiting Runs could not be taken"
                    );
                    return;
                }
            }
        }
    }

    /// One take for the stream with the most room. `false` once no stream
    /// has room or fewer Runs waited than it claimed for.
    async fn take_once(&self, streams: &PullStreams) -> Result<bool, Error> {
        let Some(stream) = streams.with_most_room() else {
            return Ok(false);
        };
        // A read first, so a take with nothing to claim takes no write lock.
        if !run_repository::any_waiting(&self.database).await? {
            return Ok(false);
        }
        let units = stream.free.min(TAKE_MAX);
        // Begun before the hold, so no room is held while the take waits
        // for the writer lock.
        let transaction = self.database.begin().await?;
        // The room moved since the look: look again.
        if streams.hold_units(stream.stream, units).is_err() {
            return Ok(true);
        }
        let taker = Taker {
            run_executor_id: &stream.run_executor_id,
            run_executor_kind: stream.kind,
        };
        let claimed = match self.claim(transaction, &taker, units).await {
            Ok(claimed) => claimed,
            Err(error) => {
                // Frees the held room; a stream that ended has none to free.
                let _ended = streams.take_held(stream.stream, units, Vec::new());
                return Err(error);
            }
        };
        let full = claimed.len() == usize::try_from(units).unwrap_or(usize::MAX);
        let runs = claimed
            .into_iter()
            .map(|Claimed { run_id, claim_id }| TakenRun { run_id, claim_id })
            .collect();
        if streams.take_held(stream.stream, units, runs).is_err() {
            // Its taken Runs go back to waiting once their take is old.
            tracing::info!("a pull stream ended before its taken Runs were sent");
            return Ok(true);
        }
        Ok(full)
    }

    async fn claim(
        &self,
        mut transaction: Transaction<'_>,
        taker: &Taker<'_>,
        units: u32,
    ) -> Result<Vec<Claimed>, Error> {
        let claimed =
            run_repository::take_oldest(&mut transaction, units, taker, self.now()?).await?;
        transaction.commit().await?;
        Ok(claimed)
    }
}
