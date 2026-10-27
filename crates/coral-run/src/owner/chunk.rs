//! A Run Ledger chunk as the Run Owner builds it in memory: the Events it
//! applied, the Run-scoped row work that rides the same commit, and the
//! termination intent and outside requests the commit records.

use std::fmt;

use async_trait::async_trait;
use coral_db::Transaction;

use crate::invocation::{FenceBatch, FenceRequest, ParkedInvocation};
use crate::lifecycle::{RequestedTermination, RunRecord};
use crate::run_inbox::InboxRow;
use crate::run_repository::OwnedEvent;
use crate::{CHUNK_BYTES_CAP, CHUNK_EVENTS_CAP, Error};

use super::handle::{Reply, RequestKey};

/// Row work items per chunk. Each one queries the database inside the
/// commit, so this keeps a commit transaction short.
pub(crate) const CHUNK_WORK_CAP: usize = 32;

/// Invocations one chunk's Dispatch Fence decides at most; a batch this
/// large closes its chunk. The fence reads its facts once whatever the batch
/// holds; the bound keeps its one row statement and its decisions small.
/// Every batched request holds a slot under the Fan-out Limit, so at the
/// default limit a batch never exceeds this.
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

    /// The Invocation this work parked on a wait, once [`Self::run`]
    /// returned `Ok`: the owner keeps it, arms the wait's deadline, and wakes
    /// it once the chunk has landed and the wait ended. `None` for work that
    /// parks nothing.
    fn take_parked(&mut self) -> Option<Box<ParkedInvocation>> {
        None
    }

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
    /// The Run Ledger head the chunk is built on, set as it goes in flight;
    /// its commit asserts it.
    pub(crate) head: u64,
    /// The entities this chunk's outside requests and the owner's own
    /// expiries decide: a later request for one is a repeat.
    pub(crate) requests: Vec<RequestKey>,
    /// The termination intent the owner applied into this chunk, written in
    /// its head update beside the `cancellation.requested` Event among the
    /// items.
    pub(crate) termination: Option<RequestedTermination>,
    /// The Run Inbox rows of the requests this chunk records, deleted in its
    /// commit.
    pub(crate) inbox_rows: Vec<InboxRow>,
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
        self.items.is_empty()
            && self.fence.is_none()
            && self.waiters.is_empty()
            && self.requests.is_empty()
            && self.termination.is_none()
            && self.inbox_rows.is_empty()
    }

    /// Whether this chunk records the request `key` names.
    pub(crate) fn records(&self, key: &RequestKey) -> bool {
        self.requests.contains(key)
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
    /// the chunk already holds. A chunk with no item and no fence batch takes
    /// any item: one Event larger than the bytes cap commits alone.
    pub(crate) fn fits(&self, events: usize, bytes: usize) -> bool {
        (self.items.is_empty() && self.fence.is_none())
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

/// What a test reads off a built chunk.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct ChunkBuildView {
    pub(crate) events: usize,
    pub(crate) works: usize,
    /// Invocations in the fence batch.
    pub(crate) fenced: usize,
    pub(crate) termination: Option<RequestedTermination>,
    /// Run Inbox rows the chunk deletes.
    pub(crate) inbox_rows: usize,
}

#[cfg(test)]
impl ChunkBuildView {
    pub(crate) fn of(chunk: &ChunkBuild) -> Self {
        Self {
            events: chunk
                .items
                .iter()
                .filter(|item| matches!(item, ChunkItem::Event(_)))
                .count(),
            works: chunk.works,
            fenced: chunk.fence.as_ref().map_or(0, |batch| batch.len()),
            termination: chunk.termination.clone(),
            inbox_rows: chunk.inbox_rows.len(),
        }
    }
}
