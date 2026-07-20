//! Observed-values Universal Search provider.

use std::time::Duration;

use crate::bootstrap::AppError;
use crate::search::observed::ObservedValuesRetrievalPolicy;
use crate::search::observed::ranking;
use crate::search::observed::sqlite_projection::{
    ObservedValuesDrainBudget, ObservedValuesDrainResult, ObservedValuesSearchHit,
    ObservedValuesSearchHits,
};
use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;
use crate::search::observed::sqlite_store::SqliteObservedValuesStore;
use crate::search::provider::ProviderSearchOutcome;
use crate::search::result::{
    ObservedValueResult, ProviderCoverage, ProviderStatus, SearchCandidate, SearchPayload,
    SearchProviderKind, SearchProviderState, SearchRequest, SearchSurfaceKind,
};
use crate::search::sqlite_store::SqliteSearchError;
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

const OBSERVED_PROVIDER_RETRIEVAL_MULTIPLIER: usize = 5;
const OBSERVED_PROVIDER_MIN_RETRIEVAL_LIMIT: usize = 25;
const OBSERVED_DRAIN_BEFORE_SEARCH_MAX_JOBS: usize = 128;
const OBSERVED_DRAIN_BEFORE_SEARCH_MS: u64 = 50;

#[derive(Debug, Clone)]
pub(crate) struct ObservedValuesProvider {
    store: SqliteObservedValuesStore,
}

impl ObservedValuesProvider {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self {
            store: SqliteObservedValuesStore::new(layout),
        }
    }

    pub(crate) fn search(
        &self,
        request: &SearchRequest,
        policy: Result<&ObservedValuesRetrievalPolicy, &AppError>,
    ) -> ProviderSearchOutcome {
        let policy = match policy {
            Ok(policy) => policy,
            Err(error) => return observed_policy_error_outcome(error),
        };
        let (drain, drain_error) = self.drain_before_search(&request.workspace_name);
        let retrieval_limit = usize::try_from(request.limit)
            .unwrap_or(usize::MAX)
            .saturating_mul(OBSERVED_PROVIDER_RETRIEVAL_MULTIPLIER)
            .max(OBSERVED_PROVIDER_MIN_RETRIEVAL_LIMIT);
        let hits = match self.store.search(
            &request.workspace_name,
            &request.terms,
            retrieval_limit,
            policy,
        ) {
            Ok(hits) => hits,
            Err(error) => return observed_error_outcome(&error),
        };

        observed_search_outcome(
            hits,
            &drain,
            drain_error.as_deref(),
            policy,
            retrieval_limit,
        )
    }

    pub(crate) fn drain_queue(
        &self,
        workspace_name: &WorkspaceName,
        budget: ObservedValuesDrainBudget,
    ) -> Result<ObservedValuesDrainResult, SqliteSearchError> {
        let result = self.store.drain_queue(workspace_name, budget)?;
        log_storage_drops(workspace_name, &result);
        log_drain_budget_exhaustion(workspace_name, &result);
        Ok(result)
    }

    fn drain_before_search(
        &self,
        workspace_name: &WorkspaceName,
    ) -> (ObservedValuesDrainResult, Option<String>) {
        let pending_queue_depth = match self.store.pending_queue_job_count(workspace_name) {
            Ok(0) => return (ObservedValuesDrainResult::default(), None),
            Ok(count) => u32::try_from(count).unwrap_or(u32::MAX),
            Err(error) => {
                tracing::debug!(
                    workspace = %workspace_name,
                    error = ?error,
                    "failed to read observed-value queue depth before search"
                );
                return (
                    ObservedValuesDrainResult::default(),
                    Some(format!("queue depth check failed: {error}")),
                );
            }
        };
        match self.store.drain_queue(
            workspace_name,
            ObservedValuesDrainBudget::new(
                OBSERVED_DRAIN_BEFORE_SEARCH_MAX_JOBS,
                Duration::from_millis(OBSERVED_DRAIN_BEFORE_SEARCH_MS),
            ),
        ) {
            Ok(drain) => {
                log_storage_drops(workspace_name, &drain);
                log_drain_budget_exhaustion(workspace_name, &drain);
                (drain, None)
            }
            Err(error) => {
                tracing::debug!(
                    workspace = %workspace_name,
                    error = ?error,
                    "serving observed-value search from existing projection after queue drain failure"
                );
                (
                    ObservedValuesDrainResult {
                        remaining_queue_depth: pending_queue_depth,
                        ..ObservedValuesDrainResult::default()
                    },
                    Some(format!("queue drain failed: {error}")),
                )
            }
        }
    }
}

fn observed_search_outcome(
    hits: ObservedValuesSearchHits,
    drain: &ObservedValuesDrainResult,
    drain_error: Option<&str>,
    policy: &ObservedValuesRetrievalPolicy,
    retrieval_limit: usize,
) -> ProviderSearchOutcome {
    let has_more = hits.retrieval_limited;
    let failed_source_count = policy.failed_source_count();
    let diversified_hits = ranking::diversify_observed_hits(hits.hits, retrieval_limit);
    let candidates = diversified_hits
        .into_iter()
        .enumerate()
        .map(|(index, hit)| observed_candidate(hit, index))
        .collect::<Vec<_>>();
    let state = if has_more
        || drain.budget_exhausted
        || drain.failed_jobs > 0
        || drain.remaining_queue_depth > 0
        || drain.storage_jobs_dropped > 0
        || drain_error.is_some()
        || policy.has_load_failures()
    {
        SearchProviderState::Partial
    } else if candidates.is_empty() {
        SearchProviderState::Empty
    } else {
        SearchProviderState::ResultsFound
    };
    let note = observed_provider_note(state, candidates.len(), drain, drain_error, policy);
    ProviderSearchOutcome {
        status: ProviderStatus {
            provider: SearchProviderKind::ObservedValues,
            state,
            note,
            coverage: Some(ProviderCoverage {
                eligible_units: hits.value_count,
                searched_units: hits.value_count,
                failed_units: drain
                    .failed_jobs
                    .saturating_add(drain.storage_jobs_dropped)
                    .saturating_add(u32::from(drain_error.is_some()))
                    .saturating_add(failed_source_count),
                returned_count: u32::try_from(candidates.len()).unwrap_or(u32::MAX),
                has_more,
                budget_exhausted: drain.budget_exhausted,
                timed_out: false,
                stale_index: drain.remaining_queue_depth > 0
                    || drain.failed_jobs > 0
                    || drain.storage_jobs_dropped > 0
                    || drain_error.is_some()
                    || policy.has_load_failures(),
            }),
        },
        candidates,
    }
}

/// Converts store-ordered hits into cross-provider candidates.
///
/// The observed-values provider must not rerank hits. It only translates the
/// store order into a cross-provider score so engine-level merging preserves the
/// retrieval order selected by observed storage.
fn observed_candidate(hit: ObservedValuesSearchHit, index: usize) -> SearchCandidate {
    let surface_kind = surface_kind(hit.surface_kind);
    SearchCandidate {
        key: format!(
            "observed:{}:{}:{}:{}:{}:{}",
            hit.source_name,
            hit.source_scope_id,
            hit.surface_kind.as_str(),
            hit.surface_name,
            hit.column_name,
            hit.value_key
        ),
        score: ranking::observed_candidate_score(&hit, index),
        provider: SearchProviderKind::ObservedValues,
        payload: SearchPayload::ObservedValue(ObservedValueResult {
            value: hit.display_value,
            schema_name: hit.source_name,
            surface_name: hit.surface_name,
            column_name: hit.column_name.clone(),
            surface_kind,
            field_path: hit.column_name,
            observed_count: hit.observation_count,
            last_observed_at: hit.last_observed_at,
        }),
    }
}

fn surface_kind(kind: ObservedValuesSurfaceKind) -> SearchSurfaceKind {
    match kind {
        ObservedValuesSurfaceKind::Table => SearchSurfaceKind::Table,
        ObservedValuesSurfaceKind::Function => SearchSurfaceKind::TableFunction,
    }
}

fn observed_provider_note(
    state: SearchProviderState,
    candidate_count: usize,
    drain: &ObservedValuesDrainResult,
    drain_error: Option<&str>,
    policy: &ObservedValuesRetrievalPolicy,
) -> String {
    match state {
        SearchProviderState::ResultsFound => {
            format!("found {candidate_count} observed value search result(s)")
        }
        SearchProviderState::Empty => "no observed value search results found".to_string(),
        SearchProviderState::Partial => observed_partial_note(drain, drain_error, policy),
        SearchProviderState::Error
        | SearchProviderState::NotEnabled
        | SearchProviderState::Skipped => "observed value search did not run".to_string(),
    }
}

fn observed_partial_note(
    drain: &ObservedValuesDrainResult,
    drain_error: Option<&str>,
    policy: &ObservedValuesRetrievalPolicy,
) -> String {
    let mut causes = Vec::new();
    if let Some(error) = drain_error {
        causes.push(format!(
            "observed value search used cached local memory; {error}"
        ));
    }
    if let Some(policy_note) = observed_policy_load_failure_note(policy) {
        causes.push(policy_note);
    }
    if drain.storage_jobs_dropped > 0 {
        causes.push(format!(
            "observed value search omitted {} queued observation job(s) to preserve storage headroom",
            drain.storage_jobs_dropped
        ));
    }
    if drain.failed_jobs > 0 {
        let disposition = if drain.remaining_queue_depth > 0 {
            "left for retry"
        } else {
            "dead-lettered"
        };
        causes.push(format!(
            "observed value search encountered {} failed queue job(s) {disposition}",
            drain.failed_jobs
        ));
    } else if drain.remaining_queue_depth > 0 && !drain.budget_exhausted {
        causes.push(format!(
            "observed value search used partial local memory; {} queue job(s) remain",
            drain.remaining_queue_depth
        ));
    }
    if drain.budget_exhausted {
        causes.push(format!(
            "observed value search used partial local memory; {} queue job(s) remain",
            drain.remaining_queue_depth
        ));
    }
    if causes.is_empty() {
        "partial observed value results were returned".to_string()
    } else {
        causes.join("; ")
    }
}

fn observed_policy_load_failure_note(policy: &ObservedValuesRetrievalPolicy) -> Option<String> {
    if !policy.has_load_failures() {
        return None;
    }
    let owner_source_names = policy.failed_owner_source_names().join(", ");
    Some(format!(
        "observed value search skipped {} source(s) whose live scopes could not be loaded: {owner_source_names}",
        policy.failed_source_count()
    ))
}

fn observed_error_outcome(error: &SqliteSearchError) -> ProviderSearchOutcome {
    ProviderSearchOutcome {
        candidates: Vec::new(),
        status: ProviderStatus {
            provider: SearchProviderKind::ObservedValues,
            state: SearchProviderState::Error,
            note: format!("observed value search failed: {error}"),
            coverage: Some(ProviderCoverage {
                failed_units: 1,
                ..ProviderCoverage::default()
            }),
        },
    }
}

fn observed_policy_error_outcome(error: &AppError) -> ProviderSearchOutcome {
    ProviderSearchOutcome {
        candidates: Vec::new(),
        status: ProviderStatus {
            provider: SearchProviderKind::ObservedValues,
            state: SearchProviderState::Error,
            note: format!("observed value search could not load live source scope: {error}"),
            coverage: Some(ProviderCoverage {
                failed_units: 1,
                ..ProviderCoverage::default()
            }),
        },
    }
}

fn log_storage_drops(workspace_name: &WorkspaceName, result: &ObservedValuesDrainResult) {
    if result.storage_jobs_dropped > 0 {
        tracing::debug!(
            workspace = %workspace_name,
            storage_jobs_dropped = result.storage_jobs_dropped,
            remaining_queue_depth = result.remaining_queue_depth,
            storage_limit_reached = result.storage_limit_reached,
            "dropped best-effort observed-value jobs to preserve storage headroom"
        );
    }
}

fn log_drain_budget_exhaustion(workspace_name: &WorkspaceName, result: &ObservedValuesDrainResult) {
    if result.budget_exhausted {
        tracing::debug!(
            workspace = %workspace_name,
            remaining_queue_depth = result.remaining_queue_depth,
            queue_jobs_processed = result.queue_jobs_processed,
            stale_jobs_skipped = result.stale_jobs_skipped,
            failed_jobs = result.failed_jobs,
            storage_jobs_dropped = result.storage_jobs_dropped,
            stale_rows_purged = result.stale_rows_purged,
            evicted_rows = result.evicted_rows,
            storage_limit_reached = result.storage_limit_reached,
            "observed-value drain stopped at a cooperative limit"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::observed_search_outcome;
    use crate::search::observed::sqlite_projection::{
        ObservedValuesDrainResult, ObservedValuesSearchHits,
    };
    use crate::search::observed::{
        ObservedValuesLiveScopeLoadFailure, ObservedValuesRetrievalPolicy,
    };
    use crate::search::result::SearchProviderState;

    #[test]
    fn storage_drops_make_observed_coverage_partial_and_stale() {
        let drain = ObservedValuesDrainResult {
            storage_jobs_dropped: 1,
            ..ObservedValuesDrainResult::default()
        };
        let policy = ObservedValuesRetrievalPolicy::new(Vec::new(), 365);

        let outcome = observed_search_outcome(
            ObservedValuesSearchHits::default(),
            &drain,
            None,
            &policy,
            25,
        );

        assert!(outcome.candidates.is_empty());
        assert_eq!(outcome.status.state, SearchProviderState::Partial);
        assert!(
            outcome
                .status
                .note
                .contains("omitted 1 queued observation job")
        );
        let coverage = outcome.status.coverage.expect("observed coverage");
        assert_eq!(coverage.failed_units, 1);
        assert!(coverage.stale_index);
    }

    #[test]
    fn partial_note_reports_storage_drops_with_other_failures() {
        let drain = ObservedValuesDrainResult {
            storage_jobs_dropped: 2,
            ..ObservedValuesDrainResult::default()
        };
        let policy = ObservedValuesRetrievalPolicy::with_load_failures(
            Vec::new(),
            vec![ObservedValuesLiveScopeLoadFailure {
                owner_source_name: "github".to_string(),
                message: "manifest parse failed".to_string(),
            }],
            365,
        );

        let outcome = observed_search_outcome(
            ObservedValuesSearchHits::default(),
            &drain,
            Some("queue drain failed: database busy"),
            &policy,
            25,
        );

        assert_eq!(outcome.status.state, SearchProviderState::Partial);
        assert!(
            outcome
                .status
                .note
                .contains("queue drain failed: database busy")
        );
        assert!(outcome.status.note.contains("skipped 1 source"));
        assert!(
            outcome
                .status
                .note
                .contains("omitted 2 queued observation job")
        );
    }

    #[test]
    fn failed_or_pending_queue_jobs_make_observed_coverage_partial_and_stale() {
        let policy = ObservedValuesRetrievalPolicy::new(Vec::new(), 365);
        let drains = [
            ObservedValuesDrainResult {
                failed_jobs: 1,
                ..ObservedValuesDrainResult::default()
            },
            ObservedValuesDrainResult {
                remaining_queue_depth: 1,
                ..ObservedValuesDrainResult::default()
            },
        ];

        for drain in drains {
            let outcome = observed_search_outcome(
                ObservedValuesSearchHits::default(),
                &drain,
                None,
                &policy,
                25,
            );

            assert_eq!(outcome.status.state, SearchProviderState::Partial);
            assert!(
                outcome
                    .status
                    .coverage
                    .expect("observed coverage")
                    .stale_index
            );
        }
    }
}
