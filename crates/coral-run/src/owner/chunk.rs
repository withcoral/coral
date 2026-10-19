//! A Run Ledger chunk as the Run Owner builds it in memory: the Events it
//! applied and the Run-scoped row work that rides the same commit.

use std::fmt;

use async_trait::async_trait;
use coral_db::Transaction;

use crate::lifecycle::RunRecord;
use crate::run_repository::OwnedEvent;
use crate::{CHUNK_BYTES_CAP, CHUNK_EVENTS_CAP, Error};

use super::handle::Reply;

/// Row work items after which a chunk closes. Each held recheck and fence is
/// its own sequence of round trips inside the commit transaction, so this
/// bounds how long one commit holds the Run row and the Workspace, Provider,
/// Account and policy rows it reads, and keeps every commit well inside a
/// lease. Aligned with the Fan-out Limit (ADR 0018); slice 4 replaces the
/// per-Invocation fence with one check per chunk.
pub(crate) const CHUNK_WORK_CAP: usize = 32;

/// Row work that joins the owner's chunk commit: an Invocation row insert, a
/// held recheck, the Dispatch Fence, an Invocation's completion. It runs
/// inside the commit transaction, after the Run row is held, and learns the
/// chunk's fate once the transaction ends.
#[async_trait]
pub(crate) trait ChunkWork: Send + fmt::Debug {
    /// Does the work's row writes in `transaction` and returns the Events it
    /// appends to the chunk, in order. A domain outcome (a rejection, a wait)
    /// is a value the work keeps for [`Self::settle`]; an error fails the chunk.
    async fn run(
        &mut self,
        transaction: &mut Transaction<'_>,
        run: &RunRecord,
        now_unix_nanos: i64,
    ) -> Result<Vec<OwnedEvent>, Error>;

    /// Delivers the outcome once the chunk is recorded, or the failure that
    /// lost it before it ran or before it committed.
    fn settle(self: Box<Self>, recorded: Result<(), Error>);
}

/// One entry of a chunk, in arrival order.
pub(crate) enum ChunkItem {
    Event(OwnedEvent),
    Work(Box<dyn ChunkWork>),
}

impl fmt::Debug for ChunkItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Event(event) => formatter.debug_tuple("Event").field(event).finish(),
            Self::Work(work) => formatter.debug_tuple("Work").field(work).finish(),
        }
    }
}

/// One chunk under construction.
#[derive(Debug, Default)]
pub(crate) struct ChunkBuild {
    pub(crate) items: Vec<ChunkItem>,
    /// Program requests answered once this chunk is recorded.
    pub(crate) waiters: Vec<Reply>,
    /// Events the owner counted at apply time; row work adds its own inside
    /// the transaction.
    pub(crate) events: usize,
    /// Output bytes applied into this chunk.
    pub(crate) bytes: usize,
    /// Row work items in this chunk.
    pub(crate) works: usize,
}

impl ChunkBuild {
    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty() && self.waiters.is_empty()
    }

    /// Whether an item of `events` Events and `bytes` bytes fits beside what
    /// the chunk already holds. An empty chunk takes any item: one Event
    /// larger than the bytes cap commits alone.
    pub(crate) fn fits(&self, events: usize, bytes: usize) -> bool {
        self.items.is_empty()
            || (self.events.saturating_add(events) <= CHUNK_EVENTS_CAP
                && self.bytes.saturating_add(bytes) <= CHUNK_BYTES_CAP)
    }

    /// Whether the chunk has reached a cap and closes.
    pub(crate) fn is_full(&self) -> bool {
        self.events >= CHUNK_EVENTS_CAP
            || self.bytes >= CHUNK_BYTES_CAP
            || self.works >= CHUNK_WORK_CAP
    }

    pub(crate) fn push(&mut self, item: ChunkItem, events: usize, bytes: usize) {
        if matches!(item, ChunkItem::Work(_)) {
            self.works += 1;
        }
        self.items.push(item);
        self.events = self.events.saturating_add(events);
        self.bytes = self.bytes.saturating_add(bytes);
    }
}
