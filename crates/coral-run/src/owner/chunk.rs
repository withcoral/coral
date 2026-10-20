//! A Run Ledger chunk as the Run Owner builds it in memory: the Events it
//! applied and the Run-scoped row work that rides the same commit.

use std::fmt;

use async_trait::async_trait;
use coral_db::Transaction;

use crate::invocation::{FenceBatch, FenceRequest};
use crate::lifecycle::RunRecord;
use crate::run_repository::OwnedEvent;
use crate::{CHUNK_BYTES_CAP, CHUNK_EVENTS_CAP, Error};

use super::handle::Reply;

/// Row work items per chunk. Each one queries the database inside the
/// commit, so this keeps a commit from holding the Run row for too long.
pub(crate) const CHUNK_WORK_CAP: usize = 32;

/// Invocations one chunk's Dispatch Fence decides at most: the Fan-out
/// Limit's number. The fence reads its facts once whatever the batch holds;
/// the bound keeps its one row statement and its decisions small.
pub(crate) const FENCE_BATCH_CAP: usize = 32;

/// A database write that the owner runs inside a chunk's commit transaction,
/// such as inserting an Invocation or recording its result. After the commit,
/// the work is told whether the chunk landed.
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
    /// The chunk's Dispatch Fence: every ready Invocation that asked for it
    /// while this chunk was open, decided together after the row work. It
    /// writes no Event, so it has no place among the items.
    pub(crate) fence: Option<Box<FenceBatch>>,
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
        self.items.is_empty() && self.fence.is_none() && self.waiters.is_empty()
    }

    /// Adds `request` to the chunk's fence batch, opening it as one row work
    /// item when the chunk has none yet.
    pub(crate) fn push_fence(&mut self, request: FenceRequest) {
        if let Some(batch) = &mut self.fence {
            batch.push(request);
        } else {
            self.fence = Some(Box::new(FenceBatch::new(request)));
            self.works += 1;
        }
    }

    /// Takes the chunk's fence batch out, leaving the rest of the chunk.
    pub(crate) fn take_fence(&mut self) -> Option<Box<FenceBatch>> {
        let batch = self.fence.take();
        if batch.is_some() {
            self.works -= 1;
        }
        batch
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
            || self
                .fence
                .as_ref()
                .is_some_and(|batch| batch.len() >= FENCE_BATCH_CAP)
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
