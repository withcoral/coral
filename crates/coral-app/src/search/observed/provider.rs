//! Observed-values Universal Search provider.

use std::time::Duration;

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

    pub(crate) fn search(&self, request: &SearchRequest) -> ProviderSearchOutcome {
        let (drain, drain_error) = self.drain_before_search(&request.workspace_name);
        let retrieval_limit = usize::try_from(request.limit)
            .unwrap_or(usize::MAX)
            .saturating_mul(OBSERVED_PROVIDER_RETRIEVAL_MULTIPLIER)
            .max(OBSERVED_PROVIDER_MIN_RETRIEVAL_LIMIT);
        let hits = match self
            .store
            .search(&request.workspace_name, &request.terms, retrieval_limit)
        {
            Ok(hits) => hits,
            Err(error) => return observed_error_outcome(&error),
        };

        observed_search_outcome(hits, &drain, drain_error.as_deref())
    }

    pub(crate) fn drain_queue(
        &self,
        workspace_name: &WorkspaceName,
        budget: ObservedValuesDrainBudget,
    ) -> Result<ObservedValuesDrainResult, SqliteSearchError> {
        let result = self.store.drain_queue(workspace_name, budget)?;
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
) -> ProviderSearchOutcome {
    let has_more = hits.retrieval_limited;
    let candidates = hits
        .hits
        .into_iter()
        .enumerate()
        .map(|(index, hit)| observed_candidate(hit, index))
        .collect::<Vec<_>>();
    let state = if has_more || drain.budget_exhausted || drain_error.is_some() {
        SearchProviderState::Partial
    } else if candidates.is_empty() {
        SearchProviderState::Empty
    } else {
        SearchProviderState::ResultsFound
    };
    let note = observed_provider_note(state, candidates.len(), drain, drain_error);
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
                    .saturating_add(u32::from(drain_error.is_some())),
                returned_count: u32::try_from(candidates.len()).unwrap_or(u32::MAX),
                has_more,
                budget_exhausted: drain.budget_exhausted,
                timed_out: false,
                stale_index: drain.remaining_queue_depth > 0
                    || drain.failed_jobs > 0
                    || drain_error.is_some(),
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
) -> String {
    match (state, drain_error) {
        (SearchProviderState::ResultsFound, _) => {
            format!("found {candidate_count} observed value search result(s)")
        }
        (SearchProviderState::Empty, _) => "no observed value search results found".to_string(),
        (SearchProviderState::Partial, Some(error)) => {
            format!("observed value search used cached local memory; {error}")
        }
        (SearchProviderState::Partial, _) if drain.budget_exhausted => format!(
            "observed value search used partial local memory; {} queue job(s) remain",
            drain.remaining_queue_depth
        ),
        (SearchProviderState::Partial, _) => {
            "partial observed value results were returned".to_string()
        }
        (
            SearchProviderState::Error
            | SearchProviderState::NotEnabled
            | SearchProviderState::Skipped,
            _,
        ) => "observed value search did not run".to_string(),
    }
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

fn log_drain_budget_exhaustion(workspace_name: &WorkspaceName, result: &ObservedValuesDrainResult) {
    if result.budget_exhausted {
        tracing::debug!(
            workspace = %workspace_name,
            remaining_queue_depth = result.remaining_queue_depth,
            queue_jobs_processed = result.queue_jobs_processed,
            stale_jobs_skipped = result.stale_jobs_skipped,
            failed_jobs = result.failed_jobs,
            "observed-value queue drain budget expired"
        );
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::TransactionBehavior;
    use tempfile::tempdir;

    use super::ObservedValuesProvider;
    use crate::search::observed::sqlite_projection::ObservedValuesDrainBudget;
    use crate::search::observed::sqlite_queue::{
        ObservedValuesQueueJob, ObservedValuesSurfaceKind,
    };
    use crate::search::observed::sqlite_store::SqliteObservedValuesStore;
    use crate::search::result::{
        SearchPayload, SearchProviderKind, SearchProviderState, SearchRequest, SearchSurfaceKind,
    };
    use crate::search::sqlite_store::SqliteSearchStore;
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;

    #[test]
    fn observed_provider_drains_queue_and_returns_observed_value_hits() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let generation = store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        store
            .enqueue_if_current(&workspace, &test_job(), generation)
            .expect("enqueue observed value");
        let provider = ObservedValuesProvider::new(layout);
        let request = SearchRequest::new(workspace, "payment", 10).expect("valid search request");

        let outcome = provider.search(&request);

        assert_eq!(outcome.status.provider, SearchProviderKind::ObservedValues);
        assert_eq!(outcome.status.state, SearchProviderState::ResultsFound);
        assert_eq!(outcome.candidates.len(), 1);
        let candidate = outcome.candidates.first().expect("observed candidate");
        let SearchPayload::ObservedValue(observed) = &candidate.payload else {
            panic!("expected observed value payload");
        };
        assert_eq!(observed.value, "Payment outage");
        assert_eq!(observed.schema_name, "github");
        assert_eq!(observed.surface_name, "issues");
        assert_eq!(observed.column_name, "title");
        assert_eq!(observed.surface_kind, SearchSurfaceKind::Table);
        assert_eq!(observed.observed_count, 1);
    }

    #[test]
    fn observed_provider_searches_existing_projection_when_drain_lock_is_busy() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let generation = store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with("scope-1", "Payment outage"),
                generation,
            )
            .expect("enqueue first observed value");
        store
            .drain_queue(
                &workspace,
                ObservedValuesDrainBudget::new(10, std::time::Duration::from_secs(1)),
            )
            .expect("drain first observed value");
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with("scope-2", "Payment backlog"),
                generation,
            )
            .expect("enqueue pending observed value");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let mut connection = backing.connect_for_test().expect("connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("hold write lock");
        let provider = ObservedValuesProvider::new(layout);
        let request = SearchRequest::new(workspace, "payment", 10).expect("valid search request");

        let outcome = provider.search(&request);
        drop(transaction);

        assert_eq!(outcome.status.provider, SearchProviderKind::ObservedValues);
        assert_eq!(outcome.status.state, SearchProviderState::Partial);
        let coverage = outcome.status.coverage.expect("coverage");
        assert_eq!(coverage.failed_units, 1);
        assert!(coverage.stale_index);
        assert_eq!(outcome.candidates.len(), 1);
        let candidate = outcome.candidates.first().expect("observed candidate");
        let SearchPayload::ObservedValue(observed) = &candidate.payload else {
            panic!("expected observed value payload");
        };
        assert_eq!(observed.value, "Payment outage");
    }

    fn test_job() -> ObservedValuesQueueJob {
        test_job_with("scope", "Payment outage")
    }

    fn test_job_with(source_scope_id: &str, display_value: &str) -> ObservedValuesQueueJob {
        ObservedValuesQueueJob {
            owner_source_name: "github".to_string(),
            source_name: "github".to_string(),
            source_scope_id: source_scope_id.to_string(),
            surface_kind: ObservedValuesSurfaceKind::Table,
            surface_name: "issues".to_string(),
            payload_json: format!(
                r#"{{"values":[{{"column_name":"title","display_value":"{display_value}","search_text":"{}","value_key":"{}"}}]}}"#,
                display_value.to_ascii_lowercase(),
                display_value.to_ascii_lowercase().replace(' ', "-")
            ),
        }
    }
}
