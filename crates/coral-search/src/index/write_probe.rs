//! A test's view of each index write, and its hold on the clock that the
//! write's two limits run on.
//!
//! A test that shortens a limit races it against the database: on a loaded
//! host the wait, the write or the failed state's record runs past a limit
//! that the test meant it to keep. With a probe, the writer wait limit and
//! the write limit pass when the test advances the probe's clock and at no
//! other time. The test learns from the probe where a write is, by the
//! write's number, and can keep it there. The index's other work, such as a read or the record of a
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

/// How far one write got.
#[derive(Debug, Clone, Copy, Default)]
struct Progress {
    /// The last point the write was at before it ended.
    at: Option<WritePoint>,
    ended: bool,
}

pub(crate) struct WriteProbe {
    /// Each write the probe has seen, in the order they started. An index
    /// writes one entry at a time, under its mutex, so the last one is the
    /// write that runs now.
    writes: watch::Sender<Vec<Progress>>,
    /// The point where a write stays until the test releases it.
    held: watch::Sender<Option<WritePoint>>,
    /// The time on the clock of the limits. Only the test advances it.
    now: watch::Sender<Duration>,
}

impl WriteProbe {
    pub(super) fn new() -> Self {
        Self {
            writes: watch::Sender::new(Vec::new()),
            held: watch::Sender::new(None),
            now: watch::Sender::new(Duration::ZERO),
        }
    }

    /// Waits until the write of number `write` is at `point` or was there
    /// before. Writes count from one, in the order they start, so the end of
    /// one write says nothing of the next.
    ///
    /// # Panics
    ///
    /// Panics when that write ended and never was at `point`: a write that
    /// fails early skips the points after its failure. Panics when `write`
    /// is zero, which names no write and would wait forever.
    pub(crate) async fn reached(&self, write: usize, point: WritePoint) {
        assert!(write >= 1, "writes count from one");
        let was_there = |progress: &Progress| match point {
            WritePoint::Ended => progress.ended,
            point => progress.at >= Some(point),
        };
        let numbered = |writes: &[Progress]| {
            write
                .checked_sub(1)
                .and_then(|index| writes.get(index))
                .copied()
        };
        let mut writes = self.writes.subscribe();
        // The probe holds the sender, so the channel stays open.
        let seen = writes
            .wait_for(|writes| {
                numbered(writes).is_some_and(|progress| progress.ended || was_there(&progress))
            })
            .await
            .ok()
            .and_then(|writes| numbered(&writes));
        assert!(
            seen.is_some_and(|progress| was_there(&progress)),
            "write {write} ended and never was at {point:?}"
        );
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

    /// Records that the write that runs now is at `point`, and stays while
    /// the test holds writes there.
    pub(super) async fn at(&self, point: WritePoint) {
        self.writes.send_modify(|writes| {
            if let Some(progress) = writes.last_mut() {
                progress.at = Some(point);
            }
        });
        let mut held = self.held.subscribe();
        drop(held.wait_for(|held| *held != Some(point)).await);
    }

    /// Records that a write starts, and [`WritePoint::Ended`] when the write
    /// drops the result.
    pub(super) fn starts(&self) -> WriteEnd<'_> {
        self.writes
            .send_modify(|writes| writes.push(Progress::default()));
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
        self.0.writes.send_modify(|writes| {
            if let Some(progress) = writes.last_mut() {
                progress.ended = true;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_write_is_at_a_point_only_by_its_own_progress() {
        let probe = WriteProbe::new();
        // The first write fails as it builds its rows.
        drop(probe.starts());
        probe.reached(1, WritePoint::Ended).await;

        let _second = probe.starts();
        let second_holds_lock = probe.reached(2, WritePoint::HoldsLock);
        tokio::pin!(second_holds_lock);
        assert!(
            tokio::time::timeout(Duration::ZERO, &mut second_holds_lock)
                .await
                .is_err(),
            "the end of the first write is no point of the second"
        );
        probe.at(WritePoint::HoldsLock).await;
        second_holds_lock.await;
    }

    #[tokio::test]
    #[should_panic(expected = "write 1 ended and never was at WaitingForWriter")]
    async fn a_write_that_ended_early_was_not_at_the_points_it_skipped() {
        let probe = WriteProbe::new();
        drop(probe.starts());
        probe.reached(1, WritePoint::WaitingForWriter).await;
    }
}
