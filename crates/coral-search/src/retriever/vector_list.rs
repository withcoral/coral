//! What a retriever does with its vector list before the engine sees it
//! (ADR 0035, "The vector list"). It reads the list in its own savepoint.
//! It drops each vector that the Embedder made from other text than the
//! Search Entity holds now. Then it merges the list with its lexical list
//! by reciprocal rank. Nothing here reads the rows of one kind, so each
//! retriever uses it with its own entries.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;

use coral_db::Transaction;
use coral_embed::{MANIFEST, Merge};
use coral_telemetry::safe;
use coral_workspace::WorkspaceId;

use super::{QueryError, RetrievalError, repository};
use crate::EntityKind;

/// The vector list that `read` gives, read in its own savepoint. `None`
/// when `read` fails: the savepoint rolls back, so the snapshot stays
/// usable, the retriever of `kind` ranks by words alone, and the page does
/// not say so. Only a failure to open, keep or undo the savepoint is the
/// retriever's error.
pub(crate) async fn vector_side<T>(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    kind: EntityKind,
    read: impl AsyncFnOnce(&mut Transaction<'_>) -> Result<VectorList<T>, RetrievalError>,
) -> Result<Option<VectorList<T>>, QueryError> {
    repository::begin_vector_query(transaction)
        .await
        .map_err(QueryError::retrieval)?;
    match read(transaction).await {
        Ok(list) => {
            repository::end_vector_query(transaction)
                .await
                .map_err(QueryError::retrieval)?;
            Ok(Some(list))
        }
        Err(error) => {
            repository::abandon_vector_query(transaction)
                .await
                .map_err(QueryError::retrieval)?;
            tracing::warn!(
                coral.search.retriever = safe::kind(kind.as_str()),
                coral.workspace.id = safe::id(workspace_id),
                error.type = safe::kind(error.code()),
                "a retriever's vector query failed; it ranks by words alone"
            );
            Ok(None)
        }
    }
}

/// One entry of the vector list as the vector query found it: the Search
/// Entity's key and the source stamp the vector was made from.
pub(crate) struct NearestVector {
    pub(crate) doc_id: String,
    pub(crate) source_indexed_at_unix_nanos: i64,
}

/// One Search Entity's row as it stands now: its source stamp, and the
/// retriever's entry for it.
pub(crate) struct CurrentRow<T> {
    pub(crate) indexed_at_unix_nanos: i64,
    pub(crate) entry: T,
}

/// The entries of `nearest`, in order, whose Search Entity is in `rows`, by
/// key, with the stamp its vector was made from. A vector of old text, or of
/// a row that is gone, leaves the list, so that Entity ranks by its words
/// alone.
pub(crate) fn keep_current<T>(
    nearest: Vec<NearestVector>,
    mut rows: HashMap<String, CurrentRow<T>>,
) -> Vec<T> {
    nearest
        .into_iter()
        .filter_map(|vector| match rows.remove(&vector.doc_id) {
            Some(row) if row.indexed_at_unix_nanos == vector.source_indexed_at_unix_nanos => {
                Some(row.entry)
            }
            Some(_) | None => None,
        })
        .collect()
}

/// A retriever's vector list, best first, and whether the window cut it
/// short.
pub(crate) struct VectorList<T> {
    pub(crate) entries: Vec<T>,
    pub(crate) has_more: bool,
}

/// One entry of a retriever's window and its rank in the vector list, from
/// 1.
pub(crate) struct Merged<T> {
    pub(crate) entry: T,
    vector_rank: Option<usize>,
}

impl<T> Merged<T> {
    /// Whether the entry is strong by its place in the vector list: among
    /// the manifest's strong count at its top. The count is a constant, so
    /// the strong entries are the same for every limit. A retriever's own
    /// strong rule makes an entry strong too, wherever it came from.
    pub(crate) fn strong_by_vector(&self) -> bool {
        self.vector_rank
            .is_some_and(|rank| rank <= MANIFEST.strong_count)
    }
}

/// A retriever's window from `lexical`, its ranked statement's rows best
/// first with one past the window, and `vector`, its vector list when the
/// vector side answered: the two merged by the manifest's weights, or
/// `lexical` alone, cut to `window`. Also whether more ranked past it.
pub(crate) fn merged_window<T, K: Eq + Hash>(
    mut lexical: Vec<T>,
    vector: Option<VectorList<T>>,
    key: impl Fn(&T) -> K,
    window: usize,
) -> (Vec<Merged<T>>, bool) {
    let mut has_more = lexical.len() > window;
    lexical.truncate(window);
    let merged = match vector {
        Some(vector) => {
            has_more |= vector.has_more;
            let merged = merge(lexical, vector.entries, key, MANIFEST.merge, window + 1);
            has_more |= merged.len() > window;
            merged
        }
        None => lexical
            .into_iter()
            .map(|entry| Merged {
                entry,
                vector_rank: None,
            })
            .collect(),
    };
    (merged.into_iter().take(window).collect(), has_more)
}

/// Merges `lexical` and `vector`, each best first, by reciprocal rank, and
/// cuts the result to `window`. An entry scores `weight / (k + rank)` from
/// each list it is in, rank from 1. An entry of both lists, by `key`, is
/// one entry, and the lexical one is kept. Equal scores keep the order in
/// which the lists first hold the entries: the lexical list's by its rank,
/// then the vector list's own by its rank, as the bench harness merged. Each
/// list breaks its own ties by a stable key, so the order is the same for
/// every request while the index is unchanged.
fn merge<T, K: Eq + Hash>(
    lexical: Vec<T>,
    vector: Vec<T>,
    key: impl Fn(&T) -> K,
    weights: Merge,
    window: usize,
) -> Vec<Merged<T>> {
    let mut slots = Vec::with_capacity(lexical.len() + vector.len());
    let mut positions = HashMap::new();
    for (rank, entry) in (1..).zip(lexical) {
        positions.entry(key(&entry)).or_insert_with(|| {
            slots.push(Slot {
                entry,
                lexical_rank: Some(rank),
                vector_rank: None,
            });
            slots.len() - 1
        });
    }
    for (rank, entry) in (1..).zip(vector) {
        if let Some(slot) = positions
            .get(&key(&entry))
            .and_then(|position| slots.get_mut(*position))
        {
            slot.vector_rank.get_or_insert(rank);
        } else {
            positions.insert(key(&entry), slots.len());
            slots.push(Slot {
                entry,
                lexical_rank: None,
                vector_rank: Some(rank),
            });
        }
    }
    // The sort is stable, so equal scores keep the order above.
    slots.sort_by(|left, right| left.score(weights).cmp(&right.score(weights)).reverse());
    slots
        .into_iter()
        .take(window)
        .map(|slot| Merged {
            entry: slot.entry,
            vector_rank: slot.vector_rank,
        })
        .collect()
}

struct Slot<T> {
    entry: T,
    lexical_rank: Option<usize>,
    vector_rank: Option<usize>,
}

impl<T> Slot<T> {
    fn score(&self, weights: Merge) -> Score {
        [
            (self.lexical_rank, weights.lexical_weight),
            (self.vector_rank, weights.vector_weight),
        ]
        .into_iter()
        .filter_map(|(rank, weight)| {
            rank.map(|rank| Score {
                numerator: u128::from(weight),
                denominator: u128::from(weights.k) + rank as u128,
            })
        })
        .fold(Score::ZERO, Score::add)
    }
}

/// A reciprocal rank score as an exact fraction, so two entries of equal
/// score compare equal and keep their order.
#[derive(Clone, Copy)]
struct Score {
    numerator: u128,
    denominator: u128,
}

impl Score {
    const ZERO: Self = Self {
        numerator: 0,
        denominator: 1,
    };

    const fn add(self, other: Self) -> Self {
        Self {
            numerator: self.numerator * other.denominator + other.numerator * self.denominator,
            denominator: self.denominator * other.denominator,
        }
    }

    fn cmp(&self, other: &Self) -> Ordering {
        (self.numerator * other.denominator).cmp(&(other.numerator * self.denominator))
    }
}
