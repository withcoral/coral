//! A Coral Instance's cache of the schema graphs its Tool calls resolve.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use coral_db::Transaction;

use crate::{Error, ProviderGenerationId, SchemaGraph, repository};

#[cfg(test)]
mod tests;

/// The parsed and checked schema graphs of the Provider Generations that this
/// Coral Instance resolves Tool calls against, so that a call does not read,
/// parse and check its Generation's whole graph again.
///
/// A Generation is immutable and its id is never reused, so no entry is ever
/// invalidated: an entry can go unused, but it cannot go stale. The
/// consequence is that this process does not notice a stored graph that is
/// corrupted after it was cached here, until the entry is evicted or the
/// process restarts. Publication and Generation verification read the stored
/// graph itself, never this cache, and so keep reporting that corruption.
///
/// The cache holds at most its byte budget, by an estimate of each graph's
/// size in memory: twelve times the length of the JSON it is stored as. Over
/// budget, it evicts the entries used longest ago. A graph larger than the
/// whole budget is returned and not kept, and a budget of zero keeps nothing.
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

/// The bytes of memory that a parsed schema graph is weighed at for each byte
/// of the JSON it is stored as: the largest ratio measured.
///
/// Measured as resident memory with glibc's allocator on x86-64, for nine
/// graphs from 156 bytes to 4.2 MB stored. They hold 5.7 times (Gemini's,
/// 330 KB stored) to 12.1 times (a 2 KB fixture's) their stored length;
/// GitHub's, the largest, holds 10.5 times and the Built-in Provider's 9.5
/// times. The weight is an upper estimate, so the cache holds less than its
/// budget of most graphs and never much more.
const STORED_BYTE_WEIGHT: usize = 12;

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
    /// The budget a Coral Instance runs with, 256 MiB: room for five graphs
    /// the size of GitHub's (4.2 MB stored, 44 MB in memory, weighed at
    /// 50 MB), or about 65 the size of Gemini's (330 KB stored).
    pub const DEFAULT_BUDGET_BYTES: usize = 256 * 1024 * 1024;

    /// Builds an empty cache that holds at most `budget_bytes` of graphs, by
    /// their estimated size in memory.
    #[must_use]
    pub fn new(budget_bytes: usize) -> Self {
        Self {
            shared: Arc::new(Shared {
                budget_bytes,
                entries: Mutex::default(),
            }),
        }
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
        let cached = self.entries().used(generation_id);
        if let Some(graph) = cached {
            return Ok(graph);
        }
        let stored = repository::load_stored_schema_graph(transaction, generation_id).await?;
        let weight_bytes = weight_bytes(stored.stored_bytes);
        let (graph, released) = self.entries().keep(
            generation_id,
            Arc::new(stored.graph),
            weight_bytes,
            self.shared.budget_bytes,
        );
        // A graph can take milliseconds to drop, so these drop here, after
        // the statement above released the lock.
        drop(released);
        Ok(graph)
    }

    /// The entries, locked. Hold them for map operations only: never across
    /// an `.await`, a parse or the drop of a graph.
    fn entries(&self) -> MutexGuard<'_, Entries> {
        // No map operation leaves the entries half-changed, so a poisoned
        // lock still guards a coherent cache.
        self.shared
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// The estimated size in memory of a graph stored as `stored_bytes` of JSON.
const fn weight_bytes(stored_bytes: usize) -> usize {
    stored_bytes.saturating_mul(STORED_BYTE_WEIGHT)
}

impl Entries {
    /// The cached graph of `generation_id`, now the most recently used.
    fn used(&mut self, generation_id: &ProviderGenerationId) -> Option<Arc<SchemaGraph>> {
        let entry = self.by_generation.get_mut(generation_id)?;
        self.by_last_use.remove(&entry.last_use);
        self.last_tick += 1;
        entry.last_use = self.last_tick;
        self.by_last_use
            .insert(entry.last_use, generation_id.clone());
        Some(Arc::clone(&entry.graph))
    }

    /// Keeps `graph` as the most recently used entry and evicts the entries
    /// used longest ago until the cache is within `budget_bytes`. Keeps the
    /// entry it already holds for the Generation instead, and nothing when
    /// `graph` alone is over the budget.
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
        if let Some(first) = self.used(generation_id) {
            return (first, vec![graph]);
        }
        let mut released = Vec::new();
        if weight_bytes > budget_bytes {
            return (graph, released);
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
        // The new entry fits the budget and is the last in use order, so the
        // cache is within budget before this reaches it.
        while self.weight_bytes > budget_bytes {
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
