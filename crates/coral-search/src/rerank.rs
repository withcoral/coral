//! The rerank step: it runs between the retrievers and `fuse`, and sets the
//! final mark of each Authored Tool in the windows (ADR 0044).
//!
//! An Authored Tool that a phrase does not make strong is strong when:
//!
//! - a rare content word of the query starts a word of its path, and it is
//!   no farther from the query than the Provider Tool of rank
//!   [`RARE_WORD_PROVIDER_TOOL_RANK`] by distance, or the farthest Provider
//!   Tool with a distance when fewer have one. The distance condition holds
//!   only when both distances exist: without its own distance, or without
//!   any Provider Tool distance, the rare word decides alone, as before the
//!   step;
//! - or it is strictly nearer to the query than the nearest Provider Tool.
//!
//! Every other Authored Tool is last: it follows every weak entry. The
//! Provider Tool distances are those of the Provider Tool retriever's
//! vector list, before the merge cut it to the window, which can lose the
//! nearest entries; the list holds only the Tools inside the search's
//! Provider filter, so each comparison is inside it too. The distances come
//! from one query vector and one model, so no limit on a distance's value
//! enters; the one tuned number is the rank of the rare-word rule.
//!
//! The step changes marks only. It does not reorder a window and does not
//! drop an entry. It reads the kind of an entry directly, the one place in
//! the engine that does.

use crate::EntityKind;
use crate::retriever::{Distance, EntityRef, Mark, WindowEntry};

/// A rare path word makes an Authored Tool strong only when it is no
/// farther than the Provider Tool of this rank by distance. On the #698
/// bench a rare word alone put an Authored Tool in the first 3 places of
/// half the Provider pages; at this rank, of 4 to 10% (#3427).
const RARE_WORD_PROVIDER_TOOL_RANK: usize = 10;

/// What the rerank step did, for the search span: how many Authored Tools
/// each rule made strong, an entry under the first rule that holds, and
/// whether the Provider Tool vector list held a distance.
pub(crate) struct Reranked {
    pub(crate) strong_by_phrase: usize,
    pub(crate) strong_by_rare_path_word: usize,
    pub(crate) strong_by_distance: usize,
    pub(crate) provider_tool_distance: bool,
}

/// Sets the final mark of each Authored Tool of `windows`. `vector_lists`
/// holds each retriever's vector list distances, by kind.
pub(crate) fn rerank(
    windows: &mut [(usize, Vec<WindowEntry>)],
    vector_lists: &[(EntityKind, Vec<Distance>)],
) -> Reranked {
    let provider_tools = ProviderToolDistances::of(vector_lists);
    let mut reranked = Reranked {
        strong_by_phrase: 0,
        strong_by_rare_path_word: 0,
        strong_by_distance: 0,
        provider_tool_distance: provider_tools.nearest.is_some(),
    };
    for entry in windows.iter_mut().flat_map(|(_retriever, window)| window) {
        if !matches!(entry.entity, EntityRef::AuthoredTool(_)) {
            continue;
        }
        entry.mark = if entry.mark == Mark::Strong {
            reranked.strong_by_phrase += 1;
            Mark::Strong
        } else if entry.rare_path_word && provider_tools.admit_rare_word(entry.distance) {
            reranked.strong_by_rare_path_word += 1;
            Mark::Strong
        } else if provider_tools.nearer(entry.distance) {
            reranked.strong_by_distance += 1;
            Mark::Strong
        } else {
            Mark::Last
        };
    }
    reranked
}

/// The two Provider Tool distances the rules compare with. Both are `None`
/// when the Provider Tool vector list is empty: no Provider Tool in scope
/// has a current vector, the Provider Tool vector query failed, the request
/// has no query vector, or the Provider Tool retriever failed.
struct ProviderToolDistances {
    nearest: Option<Distance>,
    /// The distance of the Provider Tool of rank
    /// [`RARE_WORD_PROVIDER_TOOL_RANK`], or of the farthest one with a
    /// distance when fewer have one.
    rare_word_limit: Option<Distance>,
}

impl ProviderToolDistances {
    /// The distances of the Provider Tool vector list of `vector_lists`.
    fn of(vector_lists: &[(EntityKind, Vec<Distance>)]) -> Self {
        let mut distances = vector_lists
            .iter()
            .filter(|(kind, _distances)| *kind == EntityKind::ProviderTool)
            .flat_map(|(_kind, distances)| distances.iter().copied())
            .collect::<Vec<_>>();
        distances.sort_by(|left, right| left.order(*right));
        Self {
            nearest: distances.first().copied(),
            rare_word_limit: distances
                .get(RARE_WORD_PROVIDER_TOOL_RANK - 1)
                .or(distances.last())
                .copied(),
        }
    }

    /// Whether a rare path word makes an Authored Tool at `distance` strong:
    /// when it is no farther than the limit, or when either distance is
    /// missing.
    fn admit_rare_word(&self, distance: Option<Distance>) -> bool {
        match (distance, self.rare_word_limit) {
            (Some(distance), Some(limit)) => distance <= limit,
            _ => true,
        }
    }

    /// Whether `distance` is strictly nearer than the nearest Provider
    /// Tool's. Without either distance it is not.
    fn nearer(&self, distance: Option<Distance>) -> bool {
        matches!((distance, self.nearest), (Some(distance), Some(nearest)) if distance < nearest)
    }
}
