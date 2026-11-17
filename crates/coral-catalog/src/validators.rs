//! The validators compiled from schema graphs, and the store that keeps
//! them for a Coral Instance.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use jsonschema::Validator;
use serde_json::Value;

use crate::SchemaId;
use crate::schema_graph_cache::held_bytes;
use crate::types::SchemaValidationDirection;

#[cfg(test)]
mod tests;

/// The validators compiled from the schema graphs that one Coral Instance
/// resolves Tool calls against, so that a Tool call does not bundle and
/// compile its response schema again.
///
/// A graph never changes, so a validator never goes stale. The store keeps
/// one for each schema root of each graph in each direction, built on first
/// use, until its graph is dropped or the store must make room.
///
/// The store keeps its byte budget of validators, by an estimate of each
/// one's size in memory, whatever graphs they belong to. To stay within it,
/// it lets go of the validators used longest ago, of any graph: a Tool in
/// steady use keeps its validator while those of an idle Provider leave. A
/// validator that weighs more than the whole budget is not kept: each call
/// of its Tool builds it again, as every call did before there was a store.
/// So the budget is strict.
///
/// The budget is the store's own. The byte budget of a
/// [`SchemaGraphCache`](crate::SchemaGraphCache) counts graphs alone, so a
/// validator never evicts a graph and a graph never evicts a validator.
///
/// A schema that does not compile is kept as that outcome, at the weight of
/// its entry alone, so it is not compiled again either.
///
/// A clone is another handle on the same store.
#[derive(Clone)]
pub(crate) struct ValidatorStore {
    shared: Arc<Shared>,
}

struct Shared {
    budget_bytes: usize,
    held: RwLock<Held>,
    /// The identity given to the graph that joined last.
    last_graph: AtomicU64,
}

/// One graph among those that keep their validators in a store. Two graphs
/// may name different schemas by one root identity, so a root alone does not
/// name a validator.
type GraphId = u64;

/// One schema root of a graph in one direction.
type Root = (SchemaValidationDirection, SchemaId);

/// A compiled validator, or `None` for a schema that does not compile.
type Compiled = Option<Arc<Validator>>;

#[derive(Default)]
struct Held {
    by_graph: HashMap<GraphId, HashMap<Root, Kept>>,
    /// Each validator's graph and root, oldest first, under the tick it was
    /// last ordered at: the tick of its last use then. A use since has not
    /// moved it. The store orders it again when it next looks for the
    /// validator used longest ago.
    by_order: BTreeMap<u64, (GraphId, Root)>,
    /// The tick of the last use of any validator. No two uses share a tick.
    last_tick: AtomicU64,
    weight_bytes: usize,
}

struct Kept {
    compiled: Compiled,
    weight_bytes: usize,
    /// The tick of the last use. A use writes it under the shared lock.
    last_use: AtomicU64,
    /// The tick this validator is under in the use order.
    ordered_at: u64,
}

/// What a schema graph holds of its validators: its place in a store, or
/// nothing.
///
/// A graph that a [`SchemaGraphCache`](crate::SchemaGraphCache) keeps has a
/// place in that cache's store, and gives it up when it is dropped: its
/// validators leave the store with it. Every other graph has no place: one
/// that publication materializes or verifies, one read without the cache,
/// and a copy of any graph. It keeps nothing and builds a validator on each
/// use, so no validator is held outside a store's budget.
#[derive(Default)]
pub(crate) struct Validators {
    /// Boxed so that a graph stays small inside the values that hold one.
    place: Option<Box<Place>>,
}

/// One graph's place in a store.
struct Place {
    store: ValidatorStore,
    graph: GraphId,
}

impl ValidatorStore {
    /// The validators a Coral Instance keeps, 128 MiB by weight: half of
    /// what its schema graphs may weigh. It is room for 866 of the
    /// validators of GitHub's 916 distinct response schemas, which weigh
    /// 135 MiB together, or for those of 138 Microsoft Graph Tools of median
    /// weight, 60 of mean weight, or five as heavy as the heaviest, which
    /// weighs 22 MiB. Gmail's 63 weigh 2.3 MiB together.
    ///
    /// A weight is 1.2 to 1.5 times the bytes allocated (see
    /// [`weight_bytes`]), so a full store holds 85 to 105 MiB of them.
    /// Unbounded, the validators of Microsoft Graph's 10,335 distinct
    /// response schemas weigh 22 GiB.
    pub(crate) const BUDGET_BYTES: usize = 128 * 1024 * 1024;

    /// Builds an empty store with a budget of `budget_bytes` for the
    /// validators it keeps, by their estimated size in memory.
    pub(crate) fn new(budget_bytes: usize) -> Self {
        Self {
            shared: Arc::new(Shared {
                budget_bytes,
                held: RwLock::default(),
                last_graph: AtomicU64::new(0),
            }),
        }
    }

    /// Returns the validator of `root` of `graph` in `direction`: the one
    /// kept, or else the one compiled from the schema that `bundle` builds,
    /// which is then kept. `None` when `bundle` builds no schema or the
    /// schema does not compile.
    ///
    /// No lock is held across `bundle` or the compilation, so two concurrent
    /// first uses of one root may both build its validator. Both then use
    /// the one that was kept first, and the other is dropped.
    fn validator(
        &self,
        graph: GraphId,
        root: &SchemaId,
        direction: SchemaValidationDirection,
        bundle: impl FnOnce() -> Option<Value>,
    ) -> Compiled {
        let root = (direction, root.clone());
        let kept = self.read().mark_used(graph, &root);
        if let Some(compiled) = kept {
            return compiled;
        }
        let schema = bundle()?;
        let compiled = compile(&schema);
        let weight_bytes = weight_bytes(compiled.is_some().then_some(&schema));
        // The bundle of a large schema takes milliseconds to drop, so it
        // drops here, before the lock.
        drop(schema);
        let (compiled, released) = self.write().keep(
            graph,
            root,
            compiled,
            weight_bytes,
            self.shared.budget_bytes,
        );
        // So does a validator, so these drop here, after the statement above
        // released the lock.
        drop(released);
        compiled
    }

    /// Lets go of every validator of `graph`.
    fn forget(&self, graph: GraphId) {
        let released = self.write().forget(graph);
        // Dropped here, after the statement above released the lock.
        drop(released);
    }

    /// The validators, locked for a use of one that is kept. Every response
    /// check of the Coral Instance takes this lock, and any number of them
    /// hold it at once: a use changes no map, only the tick of its
    /// validator's last use.
    fn read(&self) -> RwLockReadGuard<'_, Held> {
        // No map operation leaves the validators half-changed, so a poisoned
        // lock still guards a coherent store.
        self.shared
            .held
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The validators, locked for a change to what is kept. No check uses a
    /// kept validator meanwhile, so hold it for map operations only: never
    /// across a bundle, a compilation or the drop of a validator.
    fn write(&self) -> RwLockWriteGuard<'_, Held> {
        self.shared
            .held
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// A store with [`ValidatorStore::BUDGET_BYTES`].
impl Default for ValidatorStore {
    fn default() -> Self {
        Self::new(Self::BUDGET_BYTES)
    }
}

impl Validators {
    /// Gives the graph a place in `store`, where it keeps its validators
    /// from now on.
    pub(crate) fn keep_in(&mut self, store: &ValidatorStore) {
        let graph = store.shared.last_graph.fetch_add(1, Ordering::Relaxed) + 1;
        self.place = Some(Box::new(Place {
            store: store.clone(),
            graph,
        }));
    }

    /// Returns the validator of `root` in `direction`, compiled from the
    /// schema that `bundle` builds: the one the graph's store keeps, or one
    /// built for this use when the graph has no place in a store. `None`
    /// when `bundle` builds no schema or the schema does not compile.
    pub(crate) fn validator(
        &self,
        root: &SchemaId,
        direction: SchemaValidationDirection,
        bundle: impl FnOnce() -> Option<Value>,
    ) -> Option<Arc<Validator>> {
        match &self.place {
            Some(place) => place.store.validator(place.graph, root, direction, bundle),
            None => compile(&bundle()?),
        }
    }
}

/// A copy of a graph has no place in a store: the place is its graph's own,
/// and is given up once.
impl Clone for Validators {
    fn clone(&self) -> Self {
        Self::default()
    }
}

/// A graph's validators follow from its schemas, so they never tell two
/// graphs apart.
impl PartialEq for Validators {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for Validators {}

impl fmt::Debug for Validators {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Validators").finish_non_exhaustive()
    }
}

/// The graph is gone, so its validators leave the store.
impl Drop for Place {
    fn drop(&mut self) {
        self.store.forget(self.graph);
    }
}

fn compile(schema: &Value) -> Compiled {
    jsonschema::validator_for(schema).ok().map(Arc::new)
}

/// The estimated size in memory of the validator compiled from `schema`, a
/// bundled schema: 5/2 of the bytes the bundle's values hold room for, by
/// one walk of them, on a fixed base. `None` weighs the outcome kept for a
/// schema that does not compile: the base alone.
///
/// Measured against the allocator's own count with glibc on x86-64, for the
/// validators of 1,394 response schemas: all 63 of Gmail's, all 916 of
/// GitHub's and 415 of Microsoft Graph's, from 1 KB to 16 MB each. The
/// weight came to 1.2 times the bytes allocated for Gmail's validators
/// together and for GitHub's, and 1.5 times for Microsoft Graph's.
///
/// It is an estimate and not a bound. It was under for 8 of the 1,394: by
/// at most 5% for three of GitHub's, and by up to half for five of
/// Microsoft Graph's, each below 75 KB, where one compiled `pattern`
/// outweighs the rest of a small schema.
fn weight_bytes(schema: Option<&Value>) -> usize {
    KEPT_BASE_BYTES + schema.map_or(0, |schema| held_bytes(schema) * 5 / 2)
}

/// What a kept outcome holds besides its validator: its root in both maps
/// and its entry, in round numbers.
const KEPT_BASE_BYTES: usize = 256;

impl Held {
    /// The tick of a use that happens now.
    fn tick(&self) -> u64 {
        self.last_tick.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Marks the validator of `root` of `graph` as the most recently used
    /// and returns what is kept for it, when anything is.
    fn mark_used(&self, graph: GraphId, root: &Root) -> Option<Compiled> {
        let kept = self.by_graph.get(&graph)?.get(root)?;
        // Two uses at once take their ticks and write them in either order,
        // so each writes only a later tick: a use never looks older than
        // the one before it.
        kept.last_use.fetch_max(self.tick(), Ordering::Relaxed);
        Some(kept.compiled.clone())
    }

    /// Keeps `compiled` as the most recently used validator, then lets go of
    /// the ones used longest ago, of any graph, until the rest are within
    /// `budget_bytes`. Keeps what it already holds for the root instead, and
    /// keeps nothing that alone weighs more than `budget_bytes`.
    ///
    /// Returns the validator to use, and the outcomes that are not kept, for
    /// the caller to drop once it has released the lock.
    fn keep(
        &mut self,
        graph: GraphId,
        root: Root,
        compiled: Compiled,
        weight_bytes: usize,
        budget_bytes: usize,
    ) -> (Compiled, Vec<Compiled>) {
        if let Some(first) = self.mark_used(graph, &root) {
            return (first, vec![compiled]);
        }
        if weight_bytes > budget_bytes {
            return (compiled, Vec::new());
        }
        let now = self.tick();
        self.by_order.insert(now, (graph, root.clone()));
        self.by_graph.entry(graph).or_default().insert(
            root,
            Kept {
                compiled: compiled.clone(),
                weight_bytes,
                last_use: AtomicU64::new(now),
                ordered_at: now,
            },
        );
        self.weight_bytes += weight_bytes;
        // The new validator is the last in use order and within the budget
        // alone, so the loop ends before it reaches it.
        let mut released = Vec::new();
        while self.weight_bytes > budget_bytes {
            let Some(least_recent) = self.take_least_recently_used() else {
                break;
            };
            released.push(least_recent);
        }
        (compiled, released)
    }

    /// Lets go of the validator used longest ago and returns it, or `None`
    /// when the store is empty.
    ///
    /// The first validator in the use order was used longest ago only if no
    /// use has moved its tick since it was ordered. One that was used since
    /// is ordered again, at the tick of that use, and the search goes on:
    /// every validator is in the order at or before its last use, so the
    /// first one found in place is the one used longest ago.
    ///
    /// So a use costs no change to the order, and the exclusive lock pays
    /// for it here: the first call after many uses orders each validator
    /// used since again, in about 0.1 ms for every thousand.
    fn take_least_recently_used(&mut self) -> Option<Compiled> {
        loop {
            let (ordered_at, (graph, root)) = self.by_order.pop_first()?;
            let Some(roots) = self.by_graph.get_mut(&graph) else {
                continue;
            };
            let Some(kept) = roots.get_mut(&root) else {
                continue;
            };
            let last_use = *kept.last_use.get_mut();
            if last_use != ordered_at {
                kept.ordered_at = last_use;
                self.by_order.insert(last_use, (graph, root));
                continue;
            }
            let kept = roots.remove(&root)?;
            if roots.is_empty() {
                self.by_graph.remove(&graph);
            }
            self.weight_bytes -= kept.weight_bytes;
            return Some(kept.compiled);
        }
    }

    /// Lets go of every validator of `graph`, and returns them for the
    /// caller to drop once it has released the lock.
    fn forget(&mut self, graph: GraphId) -> Vec<Compiled> {
        let roots = self.by_graph.remove(&graph).unwrap_or_default();
        roots
            .into_values()
            .map(|kept| {
                self.by_order.remove(&kept.ordered_at);
                self.weight_bytes -= kept.weight_bytes;
                kept.compiled
            })
            .collect()
    }
}
