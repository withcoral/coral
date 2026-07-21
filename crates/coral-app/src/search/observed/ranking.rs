//! Observed-values provider ranking.

use std::collections::VecDeque;

use crate::search::observed::sqlite_projection::ObservedValuesSearchHit;
use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;

const OBSERVED_VALUE_BASE_SCORE: u32 = 10_000;

pub(crate) fn observed_candidate_score(_hit: &ObservedValuesSearchHit, index: usize) -> u32 {
    let rank_penalty = u32::try_from(index).unwrap_or(u32::MAX);
    OBSERVED_VALUE_BASE_SCORE.saturating_sub(rank_penalty)
}

pub(crate) fn diversify_observed_hits(
    hits: Vec<ObservedValuesSearchHit>,
    limit: usize,
) -> Vec<ObservedValuesSearchHit> {
    if hits.len() <= 1 || limit == 0 {
        return hits.into_iter().take(limit).collect();
    }
    let mut groups: Vec<(ObservedDiversityKey, VecDeque<ObservedValuesSearchHit>)> = Vec::new();
    for hit in hits {
        let key = observed_diversity_key(&hit);
        if let Some((_, group_hits)) = groups.iter_mut().find(|(group_key, _)| *group_key == key) {
            group_hits.push_back(hit);
        } else {
            let mut group_hits = VecDeque::new();
            group_hits.push_back(hit);
            groups.push((key, group_hits));
        }
    }

    let mut diversified = Vec::new();
    while diversified.len() < limit && !groups.is_empty() {
        for (_, group_hits) in &mut groups {
            if let Some(hit) = group_hits.pop_front() {
                diversified.push(hit);
                if diversified.len() == limit {
                    break;
                }
            }
        }
        groups.retain(|(_, group_hits)| !group_hits.is_empty());
    }
    diversified
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedDiversityKey {
    source_name: String,
    surface_kind: ObservedValuesSurfaceKind,
    surface_name: String,
    column_name: String,
}

fn observed_diversity_key(hit: &ObservedValuesSearchHit) -> ObservedDiversityKey {
    ObservedDiversityKey {
        source_name: hit.source_name.clone(),
        surface_kind: hit.surface_kind,
        surface_name: hit.surface_name.clone(),
        column_name: hit.column_name.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::{OBSERVED_VALUE_BASE_SCORE, diversify_observed_hits, observed_candidate_score};
    use crate::search::observed::sqlite_projection::ObservedValuesSearchHit;
    use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;

    #[test]
    fn observed_score_ignores_observation_count_boost() {
        let hit = observed_hit_with_count(1_250);

        assert_eq!(observed_candidate_score(&hit, 0), OBSERVED_VALUE_BASE_SCORE);
    }

    #[test]
    fn observed_candidate_score_preserves_rank_over_observation_count() {
        let low_count_first = observed_hit("github", "issues", "title", "Payment issue", 1);
        let high_count_second = observed_hit("github", "pulls", "title", "Payment pull", 1_000);

        assert!(
            observed_candidate_score(&low_count_first, 0)
                > observed_candidate_score(&high_count_second, 1)
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

    #[test]
    fn diversify_observed_hits_interleaves_groups() {
        let hits = vec![
            observed_hit("github", "issues", "title", "Payment alpha", 1),
            observed_hit("github", "issues", "title", "Payment beta", 1),
            observed_hit("github", "pulls", "title", "Payment pull", 1),
            observed_hit("github", "issues", "title", "Payment gamma", 1),
        ];

        let diversified = diversify_observed_hits(hits, 3);

        assert_eq!(
            display_values(&diversified),
            ["Payment alpha", "Payment pull", "Payment beta"]
        );
    }

    #[test]
    fn diversify_observed_hits_preserves_order_within_group() {
        let hits = vec![
            observed_hit("github", "issues", "title", "Payment alpha", 1),
            observed_hit("github", "pulls", "title", "Payment pull", 1),
            observed_hit("github", "issues", "title", "Payment beta", 1),
            observed_hit("github", "pulls", "title", "Payment review", 1),
            observed_hit("github", "issues", "title", "Payment gamma", 1),
        ];

        let diversified = diversify_observed_hits(hits, 5);
        let issue_values = diversified
            .iter()
            .filter(|hit| hit.surface_name == "issues")
            .map(|hit| hit.display_value.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            issue_values,
            ["Payment alpha", "Payment beta", "Payment gamma"]
        );
    }

    #[test]
    fn diversify_observed_hits_handles_zero_limit_and_single_hit_passthrough() {
        let hit = observed_hit("github", "issues", "title", "Payment alpha", 1);

        assert!(diversify_observed_hits(vec![hit.clone()], 0).is_empty());
        assert_eq!(diversify_observed_hits(vec![hit], 10).len(), 1);
    }

    fn observed_hit_with_count(observation_count: u64) -> ObservedValuesSearchHit {
        observed_hit(
            "github",
            "issues",
            "title",
            "Payment outage",
            observation_count,
        )
    }

    fn observed_hit(
        source_name: &str,
        surface_name: &str,
        column_name: &str,
        display_value: &str,
        observation_count: u64,
    ) -> ObservedValuesSearchHit {
        ObservedValuesSearchHit {
            source_name: source_name.to_string(),
            source_scope_id: "workspace".to_string(),
            surface_kind: ObservedValuesSurfaceKind::Table,
            surface_name: surface_name.to_string(),
            column_name: column_name.to_string(),
            value_key: display_value.to_ascii_lowercase().replace(' ', "-"),
            display_value: display_value.to_string(),
            last_observed_at: "2026-07-09T12:00:00.000Z".to_string(),
            observation_count,
        }
    }

    fn display_values(hits: &[ObservedValuesSearchHit]) -> Vec<&str> {
        hits.iter().map(|hit| hit.display_value.as_str()).collect()
    }
}
