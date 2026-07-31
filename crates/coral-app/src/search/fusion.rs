//! Request-wide fusion across Universal Search retrievers.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use crate::search::result::{MatchEvidence, Ranking, SearchSurfaceId};

// Standard RRF smoothing offset; changing it requires an explicit ranking
// decision.
const RECIPROCAL_RANK_SMOOTHING_OFFSET: u64 = 60;
// Fixed-point scale preserves reciprocal-rank precision in integer arithmetic.
const RECIPROCAL_RANK_SCORE_SCALE: u64 = 1_000_000_000;

#[derive(Debug, Clone)]
pub(crate) struct FusedEntry {
    pub(crate) id: SearchSurfaceId,
    pub(crate) evidence: MatchEvidence,
    pub(crate) score: u64,
}

/// Fuses every retriever's ranked list into one ordering.
///
/// Reciprocal rank fusion sums `1 / (k + rank)` over the lists an entry appears
/// in, so an entry corroborated by several retrievers outranks one found by a
/// single retriever. Only positions cross this boundary — no retriever's score
/// is compared against another's, which is what lets name matching, field
/// matching, and value matching share one ordering without a common scale.
pub(crate) fn fuse(rankings: Vec<Ranking>) -> Vec<FusedEntry> {
    let mut fused = BTreeMap::<SearchSurfaceId, FusedEntry>::new();
    for ranking in rankings {
        // A retriever that emits the same entry twice must not be paid twice,
        // and the duplicate must not consume a rank position either.
        let mut counted = BTreeSet::new();
        let mut rank = 0_usize;
        for entry_match in ranking.matches {
            if !counted.insert(entry_match.id.clone()) {
                continue;
            }
            let contribution = reciprocal_rank_score(rank);
            rank = rank.saturating_add(1);
            if let Some(existing) = fused.get_mut(&entry_match.id) {
                existing.score = existing.score.saturating_add(contribution);
                existing.evidence.merge(entry_match.evidence);
            } else {
                fused.insert(
                    entry_match.id.clone(),
                    FusedEntry {
                        id: entry_match.id,
                        evidence: entry_match.evidence,
                        score: contribution,
                    },
                );
            }
        }
    }

    let mut fused = fused.into_values().collect::<Vec<_>>();
    fused.sort_by(|left, right| {
        (Reverse(left.score), &left.id).cmp(&(Reverse(right.score), &right.id))
    });
    fused
}

fn reciprocal_rank_score(rank: usize) -> u64 {
    let rank = u64::try_from(rank).unwrap_or(u64::MAX);
    let denominator = RECIPROCAL_RANK_SMOOTHING_OFFSET
        .saturating_add(rank)
        .saturating_add(1);
    RECIPROCAL_RANK_SCORE_SCALE / denominator
}

#[cfg(test)]
mod tests {
    use super::{FusedEntry, fuse, reciprocal_rank_score};
    use crate::search::result::{
        FieldRef, FieldRole, FieldValues, MatchEvidence, Ranking, RetrieverId, SearchSurfaceId,
        SearchSurfaceKind, SurfaceMatch,
    };

    #[test]
    fn entries_found_by_two_retrievers_outrank_entries_found_by_one() {
        let fused = fuse(vec![
            ranking(RetrieverId::CatalogEntries, &["shared", "entries_only"]),
            ranking(RetrieverId::CatalogFields, &["shared", "fields_only"]),
        ]);

        assert_eq!(names(&fused), ["shared", "entries_only", "fields_only"]);
        assert_eq!(
            fused.first().expect("shared entry").score,
            reciprocal_rank_score(0).saturating_mul(2)
        );
    }

    #[test]
    fn evidence_from_every_retriever_accumulates_on_one_entry() {
        let mut field_evidence = MatchEvidence::default();
        field_evidence.matched_fields.push(FieldRef {
            name: "job_id".to_string(),
            role: FieldRole::Column,
        });
        let mut value_evidence = MatchEvidence::default();
        value_evidence.matching_values.push(FieldValues {
            field: "owner".to_string(),
            values: vec!["acme".to_string()],
        });

        let fused = fuse(vec![
            Ranking {
                retriever: RetrieverId::CatalogFields,
                matches: vec![SurfaceMatch {
                    id: id("repo_action_jobs"),
                    evidence: field_evidence,
                }],
            },
            Ranking {
                retriever: RetrieverId::ObservedValues,
                matches: vec![SurfaceMatch {
                    id: id("repo_action_jobs"),
                    evidence: value_evidence,
                }],
            },
        ]);

        assert_eq!(fused.len(), 1);
        let entry = fused.first().expect("entry");
        assert_eq!(entry.evidence.matched_fields.len(), 1);
        assert_eq!(entry.evidence.matching_values.len(), 1);
    }

    #[test]
    fn a_retriever_repeating_an_entry_is_not_paid_twice() {
        let fused = fuse(vec![ranking(
            RetrieverId::ObservedValues,
            &["repeated", "repeated", "other"],
        )]);

        assert_eq!(names(&fused), ["repeated", "other"]);
        assert_eq!(
            fused.first().expect("repeated entry").score,
            reciprocal_rank_score(0)
        );
        assert_eq!(
            fused.get(1).expect("other entry").score,
            reciprocal_rank_score(1),
            "a duplicate must not consume a rank position"
        );
    }

    #[test]
    fn a_table_and_a_function_sharing_a_name_stay_distinct() {
        let fused = fuse(vec![Ranking {
            retriever: RetrieverId::CatalogEntries,
            matches: vec![
                SurfaceMatch {
                    id: surface_id("search", SearchSurfaceKind::Table),
                    evidence: MatchEvidence::default(),
                },
                SurfaceMatch {
                    id: surface_id("search", SearchSurfaceKind::TableFunction),
                    evidence: MatchEvidence::default(),
                },
            ],
        }]);

        assert_eq!(fused.len(), 2);
    }

    #[test]
    fn ordering_does_not_depend_on_which_retriever_ran_first() {
        let forward = fuse(vec![
            ranking(RetrieverId::CatalogEntries, &["a", "b"]),
            ranking(RetrieverId::ObservedValues, &["b", "c"]),
        ]);
        let reversed = fuse(vec![
            ranking(RetrieverId::ObservedValues, &["b", "c"]),
            ranking(RetrieverId::CatalogEntries, &["a", "b"]),
        ]);

        assert_eq!(names(&forward), names(&reversed));
    }

    fn ranking(retriever: RetrieverId, names: &[&str]) -> Ranking {
        Ranking {
            retriever,
            matches: names
                .iter()
                .map(|name| SurfaceMatch {
                    id: id(name),
                    evidence: MatchEvidence::default(),
                })
                .collect(),
        }
    }

    fn id(name: &str) -> SearchSurfaceId {
        surface_id(name, SearchSurfaceKind::Table)
    }

    fn surface_id(name: &str, kind: SearchSurfaceKind) -> SearchSurfaceId {
        SearchSurfaceId {
            schema_name: "github".to_string(),
            name: name.to_string(),
            kind,
        }
    }

    fn names(fused: &[FusedEntry]) -> Vec<String> {
        fused.iter().map(|entry| entry.id.name.clone()).collect()
    }
}
