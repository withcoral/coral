//! The rerank step: it runs between the retrievers and `fuse`, and sets the
//! final mark of each Authored Tool in the windows (ADR 0044).
//!
//! An Authored Tool that its retriever's own rules do not make strong is
//! strong by distance when it is strictly nearer to the query than the
//! nearest Provider Tool of the same request. The Provider Tool window
//! holds only the Tools inside the search's Provider filter, so the
//! comparison is inside it too. Both distances come from one query vector
//! and one model, so no tuned limit enters. Every other Authored Tool is
//! last: it follows every weak entry.
//!
//! The step changes marks only. It does not reorder a window and does not
//! drop an entry. It reads the kind of an entry directly, the one place in
//! the engine that does.

use crate::retriever::{Distance, EntityRef, Mark, WindowEntry};

/// What the rerank step did, for the search span.
pub(crate) struct Reranked {
    /// How many Authored Tools the step made strong by distance.
    pub(crate) strong_by_distance: usize,
    /// Whether a Provider Tool of the windows had a distance.
    pub(crate) provider_tool_distance: bool,
}

/// Sets the final mark of each Authored Tool of `windows`.
pub(crate) fn rerank(windows: &mut [(usize, Vec<WindowEntry>)]) -> Reranked {
    let nearest = nearest_provider_tool(windows);
    let mut strong_by_distance = 0;
    for entry in windows.iter_mut().flat_map(|(_retriever, window)| window) {
        if !matches!(entry.entity, EntityRef::AuthoredTool(_)) || entry.mark == Mark::Strong {
            continue;
        }
        entry.mark = if nearer(entry.distance, nearest) {
            strong_by_distance += 1;
            Mark::Strong
        } else {
            Mark::Last
        };
    }
    Reranked {
        strong_by_distance,
        provider_tool_distance: nearest.is_some(),
    }
}

/// The distance of the nearest Provider Tool of `windows`, if any Provider
/// Tool entry has one. It has none when no Provider Tool in scope has a
/// current vector, the Provider Tool vector query failed, or the request
/// has no query vector. The Provider Tool window holds the first entry of
/// its vector list: at the manifest's merge weights and rank constant, at
/// most 62 entries rank before it, fewer than the window holds. A lower
/// weight for the vector list could push it out of the window.
fn nearest_provider_tool(windows: &[(usize, Vec<WindowEntry>)]) -> Option<Distance> {
    windows
        .iter()
        .flat_map(|(_retriever, window)| window)
        .filter(|entry| matches!(entry.entity, EntityRef::Tool(_)))
        .filter_map(|entry| entry.distance)
        .reduce(|nearest, distance| {
            if distance < nearest {
                distance
            } else {
                nearest
            }
        })
}

/// Whether `distance` is strictly nearer than `nearest`, the nearest
/// Provider Tool's. Without either distance it is not.
fn nearer(distance: Option<Distance>, nearest: Option<Distance>) -> bool {
    matches!((distance, nearest), (Some(distance), Some(nearest)) if distance < nearest)
}
