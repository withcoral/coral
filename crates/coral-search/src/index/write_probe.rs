//! A test's view of each index write, and its hold on the clock that the
//! write's two limits run on.
//!
//! A test that shortens a limit races it against the database: on a loaded
//! host the wait, the write or the failed state's record runs past a limit
//! that the test meant it to keep. With a probe, the writer wait limit and
//! the write limit pass when the test advances the probe's clock and at no
//! other time. The test learns from the probe where the write is, and can
//! keep it there. The index's other work, such as a read or the record of a
//! failed state, keeps its limit on the real clock, so a probing test leaves
//! the limits long.
//!
//! Timing only: a probed write runs the path that production runs.

use std::future::Future;
use std::time::Duration;

use tokio::sync::watch;

/// A point in one index write. A write reaches them in this order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum WritePoint {
    /// It built its rows and has not asked for the writer.
    BuiltRows,
    /// It waits to begin and to take its Workspace's lock. The writer wait
    /// limit runs.
    WaitingForWriter,
    /// It holds its Workspace's lock. The write limit runs.
    HoldsLock,
    /// It committed, or it failed.
    Ended,
}

pub(crate) struct WriteProbe {
    /// The last point that a write reached.
    reached: watch::Sender<Option<WritePoint>>,
    /// The point where a write stays until the test releases it.
    held: watch::Sender<Option<WritePoint>>,
    /// The time on the clock of the limits. Only the test advances it.
    now: watch::Sender<Duration>,
}

impl WriteProbe {
    pub(super) fn new() -> Self {
        Self {
            reached: watch::Sender::new(None),
            held: watch::Sender::new(None),
            now: watch::Sender::new(Duration::ZERO),
        }
    }

    /// Waits until a write is at `point` or past it.
    pub(crate) async fn reached(&self, point: WritePoint) {
        let mut reached = self.reached.subscribe();
        // The probe holds the sender, so the channel stays open.
        drop(reached.wait_for(|at| *at >= Some(point)).await);
    }

    /// Keeps each write at `point` until [`Self::release`].
    pub(crate) fn hold_at(&self, point: WritePoint) {
        self.held.send_replace(Some(point));
    }

    pub(crate) fn release(&self) {
        self.held.send_replace(None);
    }

    /// Moves the clock of the limits forward. A limit that this passes ends
    /// its work, even if the work could finish at the same moment.
    pub(crate) fn advance(&self, by: Duration) {
        self.now.send_modify(|now| *now += by);
    }

    /// Records that a write is at `point`, and stays while the test holds
    /// writes there.
    pub(super) async fn at(&self, point: WritePoint) {
        self.reached.send_replace(Some(point));
        let mut held = self.held.subscribe();
        drop(held.wait_for(|held| *held != Some(point)).await);
    }

    /// Records [`WritePoint::Ended`] when the write drops the result.
    pub(super) const fn ends(&self) -> WriteEnd<'_> {
        WriteEnd(self)
    }

    /// Runs `work` until `limit` passes on the probe's clock. Answers `None`
    /// when the limit passes first.
    pub(super) async fn before_limit<T>(
        &self,
        limit: Duration,
        work: impl Future<Output = T>,
    ) -> Option<T> {
        let mut now = self.now.subscribe();
        let deadline = now.borrow().saturating_add(limit);
        tokio::select! {
            biased;
            _passed = now.wait_for(|now| *now >= deadline) => None,
            value = work => Some(value),
        }
    }
}

pub(super) struct WriteEnd<'a>(&'a WriteProbe);

impl Drop for WriteEnd<'_> {
    fn drop(&mut self) {
        self.0.reached.send_replace(Some(WritePoint::Ended));
    }
}
