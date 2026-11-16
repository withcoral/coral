//! A Coral Instance's cache of the schema graphs its Tool calls resolve.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use coral_db::Transaction;
use serde_json::Value;

use crate::{Error, ProviderGenerationId, SchemaGraph, SchemaId, repository};

#[cfg(test)]
mod tests;

/// The parsed and checked schema graphs of the Provider Generations that this
/// Coral Instance resolves Tool calls against, so that a call does not read,
/// parse and check its Generation's whole graph again.
///
/// A Generation is immutable and its id is never reused, so no entry is ever
/// invalidated: an entry can go unused, but it cannot go stale. The
/// consequence is that this Coral Instance does not notice a stored graph
/// that is corrupted after it was cached here, until the entry is evicted or
/// the Coral Instance ends. Publication and Generation verification read the
/// stored graph itself, never this cache, and so keep reporting that
/// corruption.
///
/// The cache holds its byte budget of graphs, by an estimate of each graph's
/// size in memory, and whatever the budget, it holds the graph used most
/// recently. A graph that does not fit evicts the entries used longest ago
/// until the cache is within its budget or holds that graph alone. So the
/// largest Provider's graph is served from memory while it is the one in use,
/// even when it alone exceeds the budget: it evicts every other entry when it
/// arrives, and it is evicted when another graph is next loaded. A budget of
/// zero keeps only the graph used most recently.
///
/// The budget is therefore soft by one entry: the cache exceeds it only while
/// it holds that entry alone, and that entry can weigh more than the whole
/// budget. Nothing limits the weight of a graph. The limits on a Setup input
/// cap it only loosely: an `OpenAPI` document is 64 MiB at most
/// ([`OPENAPI_DOCUMENT_MAX_BYTES`](crate::OPENAPI_DOCUMENT_MAX_BYTES)), and
/// an MCP discovery snapshot 4 MiB
/// ([`MCP_DISCOVERY_SNAPSHOT_MAX_BYTES`](crate::MCP_DISCOVERY_SNAPSHOT_MAX_BYTES)).
/// As an estimate and not a bound, Microsoft Graph's document of 43 MiB makes
/// a graph of 184 MiB, so a document of 64 MiB of the same shape would make
/// one of about 276 MiB. The graphs of the Built-in, Private and Shared
/// Providers are Coral's own, and each is under 1 MiB.
///
/// Entries are keyed by Generation alone, with no Workspace. Ask only for a
/// Generation that a Workspace-scoped read in the same transaction just
/// returned, as Invocation admission does, and never for an id from outside.
///
/// A clone is another handle on the same cache.
#[derive(Clone)]
pub struct SchemaGraphCache {
    shared: Arc<Shared>,
}

struct Shared {
    budget_bytes: usize,
    entries: Mutex<Entries>,
}

#[derive(Default)]
struct Entries {
    by_generation: HashMap<ProviderGenerationId, Entry>,
    /// Each entry's Generation under the tick of its last use, oldest first.
    by_last_use: BTreeMap<u64, ProviderGenerationId>,
    last_tick: u64,
    weight_bytes: usize,
}

/// One Generation's graph. The validators compiled from it can join it here.
struct Entry {
    graph: Arc<SchemaGraph>,
    weight_bytes: usize,
    last_use: u64,
}

impl SchemaGraphCache {
    /// The budget a Coral Instance runs with, 256 MiB: room for the graphs of
    /// Microsoft Graph (184 MiB in memory, weighed at 189 MiB) and GitHub
    /// (42 MiB, weighed at 44 MiB) together, with 23 MiB over for graphs the
    /// size of Gmail's and Gemini's, which weigh 1 and 2 MiB.
    pub const DEFAULT_BUDGET_BYTES: usize = 256 * 1024 * 1024;

    /// Builds an empty cache with a budget of `budget_bytes` for the graphs
    /// it holds, by their estimated size in memory.
    #[must_use]
    pub fn new(budget_bytes: usize) -> Self {
        Self {
            shared: Arc::new(Shared {
                budget_bytes,
                entries: Mutex::default(),
            }),
        }
    }

    /// Builds an empty cache with [`Self::DEFAULT_BUDGET_BYTES`], for a test
    /// or a fixture that has no budget to choose. Bootstrap passes its budget
    /// to [`Self::new`].
    #[must_use]
    pub fn with_default_budget() -> Self {
        Self::new(Self::DEFAULT_BUDGET_BYTES)
    }

    /// Returns one Generation's schema graph: the cached one, or else the
    /// stored one, read through `transaction`, parsed, checked and then
    /// cached. A cached graph costs no query.
    ///
    /// Two concurrent loads of a Generation that is not cached may both read
    /// and parse it. Both then return the graph that reached the cache first.
    ///
    /// # Errors
    ///
    /// Returns a persistence or corrupt-package failure, and caches nothing.
    pub async fn schema_graph_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        generation_id: &ProviderGenerationId,
    ) -> Result<Arc<SchemaGraph>, Error> {
        let cached = self.entries().mark_used(generation_id);
        if let Some(graph) = cached {
            return Ok(graph);
        }
        let loaded = repository::load_schema_graph(transaction, generation_id).await?;
        Ok(self.keep_loaded(generation_id, loaded))
    }

    /// Caches the graph just loaded for `generation_id` and returns the graph
    /// to use: `loaded`, or the one that a concurrent load cached first.
    fn keep_loaded(
        &self,
        generation_id: &ProviderGenerationId,
        loaded: SchemaGraph,
    ) -> Arc<SchemaGraph> {
        // The walk of a large graph takes milliseconds, so it runs here,
        // before the lock.
        let weight_bytes = weight_bytes(&loaded);
        let (graph, released) = self.entries().keep(
            generation_id,
            Arc::new(loaded),
            weight_bytes,
            self.shared.budget_bytes,
        );
        // A graph can take milliseconds to drop, so these drop here, after
        // the statement above released the lock.
        drop(released);
        graph
    }

    /// The entries, locked. Hold them for map operations only: never across
    /// an `.await`, a parse, a walk or the drop of a graph.
    fn entries(&self) -> MutexGuard<'_, Entries> {
        // No map operation leaves the entries half-changed, so a poisoned
        // lock still guards a coherent cache.
        self.shared
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// The estimated size in memory of a parsed graph: the bytes its values hold
/// room for, by one walk of them, and 5/16 more, on a fixed base.
///
/// Measured against resident memory with glibc's allocator on x86-64, for ten
/// graphs from the Private Provider's (156 bytes stored) to Microsoft Graph's
/// (28.7 MB stored, 82,992 schemas). For the nine above 2 KB stored, resident
/// memory was 1.25 to 1.31 times the walked bytes: the rest is what the
/// allocator keeps for each block and the nodes of the graph's two maps, and
/// 5/16 covers the largest share of it. The smallest graph is nearly all the
/// first node of each map, which the base stands for.
///
/// The weight came to 1.02 to 1.13 times resident memory for those nine
/// (1.03 for Microsoft Graph's, 1.04 for GitHub's) and 1.32 times for the
/// smallest: never less. The length of the stored JSON predicts it poorly,
/// since resident memory was 5.7 to 12.1 times that length.
fn weight_bytes(graph: &SchemaGraph) -> usize {
    let held = graph
        .schemas()
        .map(|(id, schema)| {
            size_of::<SchemaId>() + id.as_str().len() + size_of::<Value>() + held_bytes(schema)
        })
        .sum::<usize>()
        + graph
            .sources()
            .map(|(source, id)| {
                size_of::<String>() + source.len() + size_of::<SchemaId>() + id.as_str().len()
            })
            .sum::<usize>();
    GRAPH_BASE_BYTES + held + held * 5 / 16
}

/// The first node of each of a graph's two maps, in round numbers.
const GRAPH_BASE_BYTES: usize = 2048;

/// The bytes of heap that `value` holds room for, by capacity: a string's
/// bytes, an array's values, an object's entries and index table.
fn held_bytes(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
        Value::String(text) => text.capacity(),
        Value::Array(items) => {
            items.capacity() * size_of::<Value>() + items.iter().map(held_bytes).sum::<usize>()
        }
        Value::Object(entries) => {
            let (slots, buckets) = object_room(entries.len());
            slots * OBJECT_SLOT_BYTES
                + buckets * OBJECT_BUCKET_BYTES
                + entries
                    .iter()
                    .map(|(key, value)| key.capacity() + held_bytes(value))
                    .sum::<usize>()
        }
    }
}

/// An object's entry: its hash, its key and its value.
const OBJECT_SLOT_BYTES: usize = size_of::<u64>() + size_of::<String>() + size_of::<Value>();

/// One bucket of an object's index table: an entry's position and a control
/// byte.
const OBJECT_BUCKET_BYTES: usize = size_of::<usize>() + 1;

/// The entry slots and the index buckets that a parsed object of `len`
/// entries holds room for. `serde_json` keeps an object in an index map that
/// reports no capacity, and parsing adds one entry at a time: the index table
/// doubles from 4 buckets, fills 7 in 8 of them (3 of the first 4), and the
/// entries are given room to match. An object of one entry holds room for
/// three, which is most of why a graph is several times its stored length.
const fn object_room(len: usize) -> (usize, usize) {
    if len == 0 {
        return (0, 0);
    }
    let mut buckets: usize = 4;
    loop {
        let slots = if buckets < 8 {
            buckets - 1
        } else {
            buckets / 8 * 7
        };
        if slots >= len {
            return (slots, buckets);
        }
        buckets *= 2;
    }
}

impl Entries {
    /// Marks the entry of `generation_id` as the most recently used and
    /// returns its graph, when the cache holds one.
    fn mark_used(&mut self, generation_id: &ProviderGenerationId) -> Option<Arc<SchemaGraph>> {
        let entry = self.by_generation.get_mut(generation_id)?;
        self.by_last_use.remove(&entry.last_use);
        self.last_tick += 1;
        entry.last_use = self.last_tick;
        self.by_last_use
            .insert(entry.last_use, generation_id.clone());
        Some(Arc::clone(&entry.graph))
    }

    /// Keeps `graph` as the most recently used entry, then evicts the entries
    /// used longest ago until the cache is within `budget_bytes` or holds
    /// `graph` alone. Keeps the entry it already holds for the Generation
    /// instead.
    ///
    /// Returns the graph to use, and the graphs the cache let go of, for the
    /// caller to drop once it has released the lock.
    fn keep(
        &mut self,
        generation_id: &ProviderGenerationId,
        graph: Arc<SchemaGraph>,
        weight_bytes: usize,
        budget_bytes: usize,
    ) -> (Arc<SchemaGraph>, Vec<Arc<SchemaGraph>>) {
        if let Some(first) = self.mark_used(generation_id) {
            return (first, vec![graph]);
        }
        self.last_tick += 1;
        self.by_last_use
            .insert(self.last_tick, generation_id.clone());
        self.by_generation.insert(
            generation_id.clone(),
            Entry {
                graph: Arc::clone(&graph),
                weight_bytes,
                last_use: self.last_tick,
            },
        );
        self.weight_bytes += weight_bytes;
        // The new entry is the last in use order, so the loop never reaches
        // it: the most recently used graph stays, whatever it weighs.
        let mut released = Vec::new();
        while self.weight_bytes > budget_bytes && self.by_generation.len() > 1 {
            let Some((_last_use, evicted)) = self.by_last_use.pop_first() else {
                break;
            };
            if let Some(entry) = self.by_generation.remove(&evicted) {
                self.weight_bytes -= entry.weight_bytes;
                released.push(entry.graph);
            }
        }
        (graph, released)
    }
}
