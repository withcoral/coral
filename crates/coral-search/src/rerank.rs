//! The rerank step: it runs between the retrievers and `fuse`, and decides
//! the final mark of each Authored Tool in the windows.
//!
//! An Authored Tool that its retriever's own rule does not make strong is
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
    pub(crate) tool_distance: bool,
}

/// Gives each Authored Tool of `windows` its final mark.
pub(crate) fn rerank(windows: &mut [(usize, Vec<WindowEntry>)]) -> Reranked {
    let nearest_tool = nearest_tool(windows);
    let mut strong_by_distance = 0;
    for entry in windows.iter_mut().flat_map(|(_retriever, window)| window) {
        if !matches!(entry.entity, EntityRef::AuthoredTool(_)) || entry.mark == Mark::Strong {
            continue;
        }
        entry.mark = if nearer(entry.distance, nearest_tool) {
            strong_by_distance += 1;
            Mark::Strong
        } else {
            Mark::Last
        };
    }
    Reranked {
        strong_by_distance,
        tool_distance: nearest_tool.is_some(),
    }
}

/// The distance of the nearest Provider Tool of `windows`, if any Provider
/// Tool entry has one. It has none when no Provider Tool in scope has a
/// current vector, the Provider Tool vector query failed, or the request
/// has no query vector.
fn nearest_tool(windows: &[(usize, Vec<WindowEntry>)]) -> Option<Distance> {
    windows
        .iter()
        .flat_map(|(_retriever, window)| window)
        .filter(|entry| matches!(entry.entity, EntityRef::Tool(_)))
        .filter_map(|entry| entry.distance)
        .min_by(|left, right| left.0.total_cmp(&right.0))
}

/// Whether `distance` is strictly nearer than `nearest_tool`. Without either
/// distance it is not.
fn nearer(distance: Option<Distance>, nearest_tool: Option<Distance>) -> bool {
    matches!((distance, nearest_tool), (Some(distance), Some(tool)) if distance.0 < tool.0)
}
