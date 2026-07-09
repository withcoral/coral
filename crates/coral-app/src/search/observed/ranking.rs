//! Observed-values provider ranking.

use crate::search::observed::sqlite_projection::ObservedValuesSearchHit;

const OBSERVED_VALUE_BASE_SCORE: u32 = 10_000;

pub(crate) fn observed_candidate_score(hit: &ObservedValuesSearchHit, index: usize) -> u32 {
    let observation_boost = u32::try_from(hit.observation_count.min(1_000)).unwrap_or(1_000);
    let rank_penalty = u32::try_from(index).unwrap_or(u32::MAX);
    OBSERVED_VALUE_BASE_SCORE
        .saturating_add(observation_boost)
        .saturating_sub(rank_penalty)
}

#[cfg(test)]
mod tests {
    use super::{OBSERVED_VALUE_BASE_SCORE, observed_candidate_score};
    use crate::search::observed::sqlite_projection::ObservedValuesSearchHit;
    use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;

    #[test]
    fn observed_score_includes_capped_observation_count_boost() {
        let hit = observed_hit_with_count(1_250);

        assert_eq!(
            observed_candidate_score(&hit, 0),
            OBSERVED_VALUE_BASE_SCORE + 1_000
        );
    }

    #[test]
    fn observed_score_preserves_store_rank_order_by_index() {
        let hit = observed_hit_with_count(7);

        let first_score = observed_candidate_score(&hit, 0);
        let second_score = observed_candidate_score(&hit, 1);
        let third_score = observed_candidate_score(&hit, 2);

        assert!(first_score > second_score);
        assert!(second_score > third_score);
    }

    fn observed_hit_with_count(observation_count: u64) -> ObservedValuesSearchHit {
        ObservedValuesSearchHit {
            source_name: "github".to_string(),
            source_scope_id: "workspace".to_string(),
            surface_kind: ObservedValuesSurfaceKind::Table,
            surface_name: "issues".to_string(),
            column_name: "title".to_string(),
            value_key: "payment-outage".to_string(),
            display_value: "Payment outage".to_string(),
            last_observed_at: "2026-07-09T12:00:00.000Z".to_string(),
            observation_count,
        }
    }
}
