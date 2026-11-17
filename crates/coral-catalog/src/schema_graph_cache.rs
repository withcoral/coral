//! A Coral Instance's cache of the schema graphs its Tool calls resolve.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use coral_db::Transaction;
use serde_json::Value;
use tokio::sync::{OwnedMutexGuard, oneshot};

use crate::repository::StoredSchemaGraph;
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
/// The budget does not count the validators that the cached graphs compile
/// for their Tools' responses. The cache keeps those in a store with a budget
/// of its own, 128 MiB for all of them, so a validator never evicts a graph
/// and a graph never evicts a validator. A graph's validators leave the store
/// when the graph is dropped: once the cache has evicted it and the last Tool
/// call that uses it has ended.
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
    /// The turn to load each Generation that a caller loads or waits to load.
    turns: Mutex<HashMap<ProviderGenerationId, Turn>>,
    #[cfg(test)]
    observer: Option<Arc<dyn LoadObserver>>,
}

/// One Generation's turn to load, and the count of the callers that hold it
/// or wait for it.
struct Turn {
    lock: Arc<tokio::sync::Mutex<()>>,
    callers: usize,
}

/// A caller's turn to load one Generation. No other caller loads it until
/// this is dropped.
struct LoadTurn {
    // Dropped in this order: the turn passes on, then the caller leaves.
    _held: OwnedMutexGuard<()>,
    _place: Place,
}

/// A caller's place among those that hold or wait for one Generation's turn.
/// The cache forgets the turn when the last of them leaves.
struct Place {
    shared: Arc<Shared>,
    generation_id: ProviderGenerationId,
}

/// Where a test meets the loads of a cache it observes. A hook signals the
/// test or holds the load for it, and changes no outcome.
#[cfg(test)]
trait LoadObserver: Send + Sync {
    /// A caller found its Generation not cached and is about to wait for its
    /// turn to load it.
    fn awaiting_turn(&self);

    /// A load's own thread is about to parse the stored graph. Blocking here
    /// holds the load, and no Tokio task.
    fn before_parse(&self);

    /// A load that parsed has given up its turn and its place. Its caller
    /// may be gone, so the caller's end does not tell a test this.
    fn after_turn(&self);
}

#[derive(Default)]
struct Entries {
    by_generation: HashMap<ProviderGenerationId, Entry>,
    /// Each entry's Generation under the tick of its last use, oldest first.
    by_last_use: BTreeMap<u64, ProviderGenerationId>,
    last_tick: u64,
    weight_bytes: usize,
    /// The store that every graph kept here compiles its validators into. It
    /// has a budget and a lock of its own: this lock guards only the handle,
    /// which each load clones.
    validators: crate::validators::ValidatorStore,
}

/// One Generation's graph. The validators compiled from it are in the
/// cache's store, and this weight does not count them.
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
                turns: Mutex::default(),
                #[cfg(test)]
                observer: None,
            }),
        }
    }

    /// Builds an empty cache whose loads `observer` sees.
    #[cfg(test)]
    fn observed(budget_bytes: usize, observer: Arc<dyn LoadObserver>) -> Self {
        Self {
            shared: Arc::new(Shared {
                budget_bytes,
                entries: Mutex::default(),
                turns: Mutex::default(),
                observer: Some(observer),
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
    /// One load of a Generation runs at a time. A caller that finds a load
    /// under way waits for it and then uses the graph it cached, so
    /// concurrent first calls read and parse the Generation once. A waiter
    /// loads it again only when the cache evicted that graph, to make room
    /// for another Generation's graph, before the waiter's turn. A load that
    /// fails reaches its own caller alone: the next caller in turn loads
    /// through its own transaction. So does the next caller when the one
    /// whose turn it was goes away before it has read the row; once the row
    /// is read, the load finishes and caches its graph without its caller.
    ///
    /// A waiting caller keeps `transaction` open for as long as the load it
    /// waits for takes. Pass a read snapshot, as a Tool call does: it holds a
    /// connection and no lock, where a write transaction would hold
    /// `SQLite`'s one writer for that long.
    ///
    /// The parse and the check run on a thread of their own, which lives for
    /// that load: not on the caller's task, whose worker would run no other
    /// task meanwhile, and not on the runtime's blocking pool, which running
    /// Programs fill.
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
        let turn = self.load_turn(generation_id).await;
        // The load that had the turn before this one cached the graph,
        // unless it failed.
        let cached = self.entries().mark_used(generation_id);
        if let Some(graph) = cached {
            return Ok(graph);
        }
        let stored = repository::load_stored_schema_graph(transaction, generation_id).await?;
        self.parse_and_keep(generation_id.clone(), stored, turn)
            .await
    }

    /// Waits for this caller's turn to load `generation_id`. A caller that
    /// goes away while it waits gives up its place.
    async fn load_turn(&self, generation_id: &ProviderGenerationId) -> LoadTurn {
        let (place, lock) = Place::take(&self.shared, generation_id);
        #[cfg(test)]
        if let Some(observer) = &self.shared.observer {
            observer.awaiting_turn();
        }
        LoadTurn {
            _held: lock.lock_owned().await,
            _place: place,
        }
    }

    /// Parses and checks `stored` on a thread of its own, caches the graph
    /// and returns the graph to use.
    ///
    /// The thread holds `turn` until the graph is cached. So the load ends
    /// the same way when its caller goes away meanwhile, and the next caller
    /// in turn finds the graph cached.
    async fn parse_and_keep(
        &self,
        generation_id: ProviderGenerationId,
        stored: StoredSchemaGraph,
        turn: LoadTurn,
    ) -> Result<Arc<SchemaGraph>, Error> {
        let (answer, answered) = oneshot::channel();
        let cache = self.clone();
        on_own_thread(move || {
            #[cfg(test)]
            if let Some(observer) = &cache.shared.observer {
                observer.before_parse();
            }
            let loaded = stored
                .parse()
                .map(|loaded| cache.keep_loaded(&generation_id, loaded));
            drop(turn);
            #[cfg(test)]
            if let Some(observer) = &cache.shared.observer {
                observer.after_turn();
            }
            // The caller may be gone. Its graph is cached all the same.
            drop(answer.send(loaded));
        });
        // The thread drops the answer unsent only when the parse panics,
        // which the panic hook reports. Nothing is cached then.
        answered.await.unwrap_or_else(|_unanswered| {
            Err(Error::CorruptData(
                "Provider schema graph could not be parsed".to_string(),
            ))
        })
    }

    /// Caches the graph just loaded for `generation_id` and returns the graph
    /// to use: `loaded`, or the one the cache already holds for it.
    fn keep_loaded(
        &self,
        generation_id: &ProviderGenerationId,
        loaded: SchemaGraph,
    ) -> Arc<SchemaGraph> {
        let validators = self.entries().validators.clone();
        let mut loaded = loaded;
        loaded.keep_validators_in(&validators);
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

impl Shared {
    /// The turns, locked. Hold them for map operations only, as the entries.
    fn turns(&self) -> MutexGuard<'_, HashMap<ProviderGenerationId, Turn>> {
        // No map operation leaves the turns half-changed, so a poisoned lock
        // still guards a coherent map.
        self.turns.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Place {
    /// Takes a place for `generation_id`, and returns it with the lock that
    /// is the Generation's turn.
    fn take(
        shared: &Arc<Shared>,
        generation_id: &ProviderGenerationId,
    ) -> (Self, Arc<tokio::sync::Mutex<()>>) {
        let mut turns = shared.turns();
        let turn = turns.entry(generation_id.clone()).or_insert_with(|| Turn {
            lock: Arc::default(),
            callers: 0,
        });
        turn.callers += 1;
        let place = Self {
            shared: Arc::clone(shared),
            generation_id: generation_id.clone(),
        };
        (place, Arc::clone(&turn.lock))
    }
}

impl Drop for Place {
    fn drop(&mut self) {
        let mut turns = self.shared.turns();
        if let Some(turn) = turns.get_mut(&self.generation_id) {
            turn.callers -= 1;
            if turn.callers == 0 {
                turns.remove(&self.generation_id);
            }
        }
    }
}

/// Runs `work`, which computes without awaiting, on a thread of its own.
///
/// Not on the caller's task: the parse and the check of the largest graph
/// take 0.3 s, and the worker that ran them would run no other task for that
/// long. Not on the runtime's blocking pool either. A running Program holds
/// one of that pool's threads while it waits for its Tool call, so a first
/// call that needed another would wait, in a full pool, for a thread that
/// only the Programs waiting beside it can free. The thread lives for one
/// load, and a Generation has one load at a time.
///
/// When the operating system gives no thread, `work` runs where it is.
fn on_own_thread<Work: FnOnce() + Send + 'static>(work: Work) {
    let (hand_over, handed) = oneshot::channel::<Work>();
    let started = std::thread::Builder::new()
        .name("coral-schemas".to_owned())
        .spawn(move || {
            if let Ok(work) = handed.blocking_recv() {
                work();
            }
        });
    // Detached: the thread ends with its load.
    let unsent = match started {
        Ok(_detached) => hand_over.send(work).err(),
        Err(_no_thread) => Some(work),
    };
    if let Some(work) = unsent {
        work();
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
pub(crate) fn held_bytes(value: &Value) -> usize {
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
