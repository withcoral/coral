//! The validators a schema graph has compiled from its schema roots.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use jsonschema::Validator;
use serde_json::Value;

use crate::SchemaId;
use crate::schema_graph_cache::held_bytes;
use crate::types::SchemaValidationDirection;

#[cfg(test)]
mod tests;

/// The validators that one schema graph has compiled, so that a Tool call
/// does not bundle and compile its response schema again.
///
/// A graph never changes, so a validator never goes stale. A graph builds
/// one on the first use of a schema root in a direction and keeps it until
/// the graph is dropped, or until it must make room. So the validators of a
/// graph in a [`SchemaGraphCache`](crate::SchemaGraphCache) leave with it
/// when the cache evicts it.
///
/// A graph keeps [`Self::BUDGET_BYTES`] of validators, by an estimate of each
/// one's size in memory, and lets go of the ones used longest ago to stay
/// within it. A validator that weighs more than the whole budget is not kept:
/// each call of its Tool builds it again, as every call did before a graph
/// kept any. The weight of a graph in a `SchemaGraphCache` does not count
/// them, so a cache of `n` graphs can hold `n` times this budget on top of
/// its own.
///
/// A schema that does not compile is kept as that outcome, at the weight of
/// its entry alone, so it is not compiled again either.
#[derive(Default)]
pub(crate) struct Validators {
    /// Boxed so that a graph stays small inside the values that hold one.
    held: Box<Mutex<Held>>,
}

/// One schema root in one direction.
type Key = (SchemaValidationDirection, SchemaId);

/// A compiled validator, or `None` for a schema that does not compile.
type Compiled = Option<Arc<Validator>>;

#[derive(Default)]
struct Held {
    by_key: HashMap<Key, Kept>,
    /// Each validator's key under the tick of its last use, oldest first.
    by_last_use: BTreeMap<u64, Key>,
    last_tick: u64,
    weight_bytes: usize,
}

struct Kept {
    compiled: Compiled,
    weight_bytes: usize,
    last_use: u64,
}

impl Validators {
    /// The validators one graph keeps, 32 MiB by weight: the smallest power
    /// of two that holds the validator of any one Microsoft Graph Tool, the
    /// largest of which weighs 22 MiB. It is room for the validators of 35
    /// Microsoft Graph Tools of median weight or 15 of mean weight, and of
    /// 200 GitHub Tools of mean weight. Gmail's 63 weigh 2.3 MiB together.
    ///
    /// Unbounded, the validators of GitHub's 916 distinct response schemas
    /// weigh 135 MiB, three times its graph, and those of Microsoft Graph's
    /// 10,335 weigh 22 GiB.
    pub(crate) const BUDGET_BYTES: usize = 32 * 1024 * 1024;

    /// Returns the validator of `root` in `direction`: the one this graph
    /// keeps, or else the one compiled from the schema that `bundle` builds,
    /// which is then kept. `None` when `bundle` builds no schema or the
    /// schema does not compile.
    ///
    /// No lock is held across `bundle` or the compilation, so two concurrent
    /// first uses of one root may both build its validator. Both then use
    /// the one that was kept first, and the other is dropped.
    pub(crate) fn validator(
        &self,
        root: &SchemaId,
        direction: SchemaValidationDirection,
        bundle: impl FnOnce() -> Option<Value>,
    ) -> Option<Arc<Validator>> {
        let key = (direction, root.clone());
        let kept = self.held().mark_used(&key);
        if let Some(compiled) = kept {
            return compiled;
        }
        let schema = bundle()?;
        let compiled = jsonschema::validator_for(&schema).ok().map(Arc::new);
        let weight_bytes = weight_bytes(compiled.is_some().then_some(&schema));
        // The bundle of a large schema takes milliseconds to drop, so it
        // drops here, before the lock.
        drop(schema);
        let (compiled, released) =
            self.held()
                .keep(key, compiled, weight_bytes, Self::BUDGET_BYTES);
        // So does a validator, so these drop here, after the statement above
        // released the lock.
        drop(released);
        compiled
    }

    /// The validators, locked. Hold them for map operations only: never
    /// across a bundle, a compilation or the drop of a validator.
    fn held(&self) -> MutexGuard<'_, Held> {
        // No map operation leaves the validators half-changed, so a poisoned
        // lock still guards a coherent set.
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A copy of a graph has compiled nothing yet.
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

/// What a kept outcome holds besides its validator: its key in both maps and
/// its entry, in round numbers.
const KEPT_BASE_BYTES: usize = 256;

impl Held {
    /// Marks the validator of `key` as the most recently used and returns
    /// what is kept for it, when anything is.
    fn mark_used(&mut self, key: &Key) -> Option<Compiled> {
        let kept = self.by_key.get_mut(key)?;
        self.last_tick += 1;
        if let Some(key) = self.by_last_use.remove(&kept.last_use) {
            self.by_last_use.insert(self.last_tick, key);
        }
        kept.last_use = self.last_tick;
        Some(kept.compiled.clone())
    }

    /// Keeps `compiled` as the most recently used validator, then lets go of
    /// the ones used longest ago until the rest are within `budget_bytes`.
    /// Keeps what it already holds for `key` instead, and keeps nothing that
    /// alone weighs more than `budget_bytes`.
    ///
    /// Returns the validator to use, and the outcomes that are not kept, for
    /// the caller to drop once it has released the lock.
    fn keep(
        &mut self,
        key: Key,
        compiled: Compiled,
        weight_bytes: usize,
        budget_bytes: usize,
    ) -> (Compiled, Vec<Compiled>) {
        if let Some(first) = self.mark_used(&key) {
            return (first, vec![compiled]);
        }
        if weight_bytes > budget_bytes {
            return (compiled, Vec::new());
        }
        self.last_tick += 1;
        self.by_last_use.insert(self.last_tick, key.clone());
        self.by_key.insert(
            key,
            Kept {
                compiled: compiled.clone(),
                weight_bytes,
                last_use: self.last_tick,
            },
        );
        self.weight_bytes += weight_bytes;
        // The new validator is the last in use order and within the budget
        // alone, so the loop ends before it reaches it.
        let mut released = Vec::new();
        while self.weight_bytes > budget_bytes {
            let Some((_last_use, evicted)) = self.by_last_use.pop_first() else {
                break;
            };
            if let Some(kept) = self.by_key.remove(&evicted) {
                self.weight_bytes -= kept.weight_bytes;
                released.push(kept.compiled);
            }
        }
        (compiled, released)
    }
}
