//! Request-wide candidate ordering across Universal Search providers.

use std::cmp::Reverse;

use crate::search::provider::ProviderSearchOutcome;
use crate::search::result::{SearchCandidate, SearchProviderKind};

// Fixed-point scale preserves reciprocal-rank precision in the existing u32
// score field.
const RECIPROCAL_RANK_SCORE_SCALE: u64 = 1_000_000_000;
// Standard RRF smoothing offset; changing it requires an explicit ranking
// decision.
const RECIPROCAL_RANK_SMOOTHING_OFFSET: u64 = 60;

pub(crate) fn order_candidates(outcomes: &mut [ProviderSearchOutcome]) -> Vec<SearchCandidate> {
    if outcomes.iter().any(|outcome| {
        outcome.status.provider == SearchProviderKind::NativeFanout
            && !outcome.candidates.is_empty()
    }) {
        reciprocal_rank_order(outcomes)
    } else {
        legacy_score_order(outcomes)
    }
}

fn legacy_score_order(outcomes: &mut [ProviderSearchOutcome]) -> Vec<SearchCandidate> {
    let mut candidates = outcomes
        .iter_mut()
        .flat_map(|outcome| std::mem::take(&mut outcome.candidates))
        .collect::<Vec<_>>();
    candidates.sort();
    candidates
}

fn reciprocal_rank_order(outcomes: &mut [ProviderSearchOutcome]) -> Vec<SearchCandidate> {
    let mut candidates = Vec::new();
    for outcome in outcomes {
        for (provider_rank, mut candidate) in std::mem::take(&mut outcome.candidates)
            .into_iter()
            .enumerate()
        {
            candidate.score = reciprocal_rank_score(provider_rank);
            candidates.push(candidate);
        }
    }
    candidates.sort_by(|left, right| {
        (
            Reverse(left.score),
            provider_order(left.provider),
            left.type_order(),
            left.key.as_str(),
        )
            .cmp(&(
                Reverse(right.score),
                provider_order(right.provider),
                right.type_order(),
                right.key.as_str(),
            ))
    });
    candidates
}

fn reciprocal_rank_score(provider_rank: usize) -> u32 {
    let provider_rank = u64::try_from(provider_rank).unwrap_or(u64::MAX);
    let denominator = RECIPROCAL_RANK_SMOOTHING_OFFSET
        .saturating_add(provider_rank)
        .saturating_add(1);
    u32::try_from(RECIPROCAL_RANK_SCORE_SCALE / denominator).unwrap_or(u32::MAX)
}

fn provider_order(provider: SearchProviderKind) -> u8 {
    match provider {
        SearchProviderKind::CatalogMetadata => 0,
        SearchProviderKind::ObservedValues => 1,
        SearchProviderKind::NativeFanout => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::{order_candidates, reciprocal_rank_score};
    use crate::search::provider::ProviderSearchOutcome;
    use crate::search::result::{
        ObservedValueResult, ProviderStatus, SearchCandidate, SearchPayload, SearchProviderKind,
        SearchProviderState, SearchSurfaceKind,
    };

    #[test]
    fn local_only_candidates_keep_legacy_scores_and_order() {
        let mut outcomes = vec![outcome(
            SearchProviderKind::CatalogMetadata,
            vec![
                candidate(SearchProviderKind::CatalogMetadata, "low", 1),
                candidate(SearchProviderKind::CatalogMetadata, "high", 9),
            ],
        )];

        let ordered = order_candidates(&mut outcomes);

        assert_eq!(candidate_keys(&ordered), ["high", "low"]);
        assert_eq!(ordered.first().expect("high candidate").score, 9);
        assert_eq!(ordered.get(1).expect("low candidate").score, 1);
    }

    #[test]
    fn empty_native_outcome_keeps_legacy_scores_and_order() {
        let mut native = outcome(SearchProviderKind::NativeFanout, Vec::new());
        native.status.state = SearchProviderState::Empty;
        let mut outcomes = vec![
            outcome(
                SearchProviderKind::CatalogMetadata,
                vec![candidate(SearchProviderKind::CatalogMetadata, "catalog", 9)],
            ),
            outcome(
                SearchProviderKind::ObservedValues,
                vec![candidate(
                    SearchProviderKind::ObservedValues,
                    "observed",
                    11,
                )],
            ),
            native,
        ];

        let ordered = order_candidates(&mut outcomes);

        assert_eq!(candidate_keys(&ordered), ["observed", "catalog"]);
        assert_eq!(ordered.first().expect("observed candidate").score, 11);
        assert_eq!(ordered.get(1).expect("catalog candidate").score, 9);
    }

    #[test]
    fn three_provider_fusion_uses_exact_rank_formula_and_provider_ties() {
        let mut outcomes = vec![
            outcome(
                SearchProviderKind::CatalogMetadata,
                vec![
                    candidate(SearchProviderKind::CatalogMetadata, "catalog-0", 99),
                    candidate(SearchProviderKind::CatalogMetadata, "catalog-1", 98),
                ],
            ),
            outcome(
                SearchProviderKind::ObservedValues,
                vec![
                    candidate(SearchProviderKind::ObservedValues, "observed-0", 7),
                    candidate(SearchProviderKind::ObservedValues, "observed-1", 6),
                ],
            ),
            outcome(
                SearchProviderKind::NativeFanout,
                vec![
                    candidate(SearchProviderKind::NativeFanout, "native-0", 1),
                    candidate(SearchProviderKind::NativeFanout, "native-1", 0),
                ],
            ),
        ];

        let ordered = order_candidates(&mut outcomes);

        assert_eq!(
            candidate_keys(&ordered),
            [
                "catalog-0",
                "observed-0",
                "native-0",
                "catalog-1",
                "observed-1",
                "native-1",
            ]
        );
        assert_eq!(
            ordered.first().expect("first rank candidate").score,
            reciprocal_rank_score(0)
        );
        assert_eq!(
            ordered.get(3).expect("second rank candidate").score,
            reciprocal_rank_score(1)
        );
    }

    #[test]
    fn equal_keys_from_different_providers_remain_distinct() {
        let mut outcomes = vec![
            outcome(
                SearchProviderKind::CatalogMetadata,
                vec![candidate(
                    SearchProviderKind::CatalogMetadata,
                    "provider-scoped-key",
                    99,
                )],
            ),
            outcome(
                SearchProviderKind::NativeFanout,
                vec![candidate(
                    SearchProviderKind::NativeFanout,
                    "provider-scoped-key",
                    1,
                )],
            ),
        ];

        let ordered = order_candidates(&mut outcomes);

        assert_eq!(
            ordered
                .iter()
                .map(|candidate| (candidate.provider, candidate.key.as_str()))
                .collect::<Vec<_>>(),
            [
                (SearchProviderKind::CatalogMetadata, "provider-scoped-key"),
                (SearchProviderKind::NativeFanout, "provider-scoped-key"),
            ]
        );
    }

    fn outcome(
        provider: SearchProviderKind,
        candidates: Vec<SearchCandidate>,
    ) -> ProviderSearchOutcome {
        ProviderSearchOutcome {
            candidates,
            status: ProviderStatus {
                provider,
                state: SearchProviderState::ResultsFound,
                note: String::new(),
                coverage: None,
            },
        }
    }

    fn candidate(provider: SearchProviderKind, key: &str, score: u32) -> SearchCandidate {
        SearchCandidate {
            key: key.to_string(),
            score,
            provider,
            payload: SearchPayload::ObservedValue(ObservedValueResult {
                value: key.to_string(),
                schema_name: "github".to_string(),
                surface_name: "issues".to_string(),
                column_name: "title".to_string(),
                surface_kind: SearchSurfaceKind::Table,
                field_path: "title".to_string(),
                observed_count: 1,
                last_observed_at: "2026-07-16T00:00:00Z".to_string(),
            }),
        }
    }

    fn candidate_keys(candidates: &[SearchCandidate]) -> Vec<&str> {
        candidates
            .iter()
            .map(|candidate| candidate.key.as_str())
            .collect()
    }
}
