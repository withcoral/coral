//! The take loop (ADR 0027): one per Coral Instance, for all its pull
//! streams. Each take claims the oldest waiting Runs for the stream with the
//! most room and sends them to its Run Executor as one `Taken`.

use std::convert::Infallible;
use std::time::Duration;

use coral_db::Transaction;
use coral_run_executor::PullStreams;
use coral_run_executor::wire::v1::TakenRun;
use coral_telemetry::safe;
use tokio::time::Instant;

use crate::run_inbox::rung;
use crate::run_repository::{self, Claimed, Taker};
use crate::{Error, RunModule};

/// The most Runs one take claims.
pub(crate) const TAKE_MAX: u32 = 16;

/// How often the loop takes.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Timing {
    /// At most one pass per spacing, however often the change signal rings.
    pub(crate) spacing: Duration,
    /// One pass per backstop while any stream has room, though nothing rang.
    pub(crate) backstop: Duration,
}

impl Timing {
    const PRODUCTION: Self = Self {
        spacing: Duration::from_millis(50),
        backstop: Duration::from_secs(1),
    };
}

/// When the next pass is due.
#[derive(Debug)]
pub(crate) struct Pace {
    timing: Timing,
    /// The change signal or new room rang since the last pass.
    rang: bool,
    last_pass: Option<Instant>,
}

impl Pace {
    pub(crate) const fn new(timing: Timing) -> Self {
        Self {
            timing,
            rang: true,
            last_pass: None,
        }
    }

    pub(crate) const fn ring(&mut self) {
        self.rang = true;
    }

    /// When the next pass is due, seen at `now`: none while no stream has
    /// room, one spacing after the last pass once something rang, and one
    /// backstop after it in any case.
    pub(crate) fn due(&self, room: bool, now: Instant) -> Option<Instant> {
        if !room {
            return None;
        }
        let wait = if self.rang {
            self.timing.spacing.min(self.timing.backstop)
        } else {
            self.timing.backstop
        };
        Some(self.last_pass.map_or(now, |last| last + wait))
    }

    pub(crate) const fn passed(&mut self, at: Instant) {
        self.rang = false;
        self.last_pass = Some(at);
    }
}

impl RunModule {
    /// Takes waiting Runs for `streams`, this Coral Instance's pull streams,
    /// for as long as it is polled (ADR 0027). Nothing starts a taken Run
    /// until its Run Executor opens its Run stream.
    pub async fn take_runs(&self, streams: PullStreams) -> Infallible {
        self.take_loop(streams, Timing::PRODUCTION).await
    }

    pub(crate) async fn take_loop(&self, streams: PullStreams, timing: Timing) -> Infallible {
        let mut changes = None;
        let mut pace = Pace::new(timing);
        loop {
            if changes.is_none() {
                changes = self.database.subscribe_changes().ok();
            }
            let now = Instant::now();
            match pace.due(streams.has_room(), now) {
                Some(due) if due <= now => {
                    pace.passed(now);
                    self.take_while_waiting(&streams).await;
                }
                Some(due) => tokio::select! {
                    () = tokio::time::sleep_until(due) => {}
                    () = rung(&mut changes) => pace.ring(),
                },
                // New room is an ask, as the change signal is. A change
                // while no stream has room waits in the subscription.
                None => {
                    streams.room_signalled(timing.backstop).await;
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
