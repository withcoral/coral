//! Observed-values Universal Search provider.

use std::time::Duration;

use crate::bootstrap::AppError;
use crate::search::maintenance::{
    ObservedClearMaintenanceResult, ObservedDrainMaintenanceResult,
    ObservedRebuildMaintenanceResult, SearchClearTarget, SearchDataScope, SearchMaintenanceDetail,
    SearchMaintenanceResult, SearchMaintenanceState, SearchProviderClearOutcome,
    SearchProviderClearRequest, SearchProviderRebuildRequest, SearchStorageCleanupResult,
};
use crate::search::observed::ObservedValuesRetrievalPolicy;
use crate::search::observed::ranking;
use crate::search::observed::sqlite_projection::{
    ObservedValuesDrainBudget, ObservedValuesDrainResult, ObservedValuesSearchHit,
    ObservedValuesSearchHits,
};
use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;
use crate::search::observed::sqlite_store::{ObservedValuesClearResult, SqliteObservedValuesStore};
use crate::search::provider::ProviderSearchOutcome;
use crate::search::result::{
    ObservedValueResult, ProviderCoverage, ProviderStatus, SearchCandidate, SearchPayload,
    SearchProviderKind, SearchProviderState, SearchRequest, SearchSurfaceKind,
};
use crate::search::sqlite_store::SqliteSearchCompactionResult;
use crate::search::sqlite_store::SqliteSearchError;
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

const OBSERVED_PROVIDER_RETRIEVAL_MULTIPLIER: usize = 5;
const OBSERVED_PROVIDER_MIN_RETRIEVAL_LIMIT: usize = 25;
const OBSERVED_DRAIN_BEFORE_SEARCH_MAX_JOBS: usize = 128;
const OBSERVED_DRAIN_BEFORE_SEARCH_MS: u64 = 50;
const OBSERVED_REBUILD_DRAIN_MAX_JOBS: usize = 10_000;
const OBSERVED_REBUILD_DRAIN_MS: u64 = 1_000;

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
    ) -> Result<SearchMaintenanceResult, crate::search::result::SearchManagerError> {
        let result = self
            .store
            .drain_queue(workspace_name, budget)
            .map_err(|error| observed_sqlite_app_error(&error))?;
        log_drain_maintenance(workspace_name, &result);
        Ok(observed_drain_provider_result(&result))
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
                log_drain_maintenance(workspace_name, &drain);
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

    pub(crate) fn rebuild_index(
        &self,
        request: SearchProviderRebuildRequest<'_>,
        policy: &ObservedValuesRetrievalPolicy,
    ) -> Result<SearchMaintenanceResult, crate::search::result::SearchManagerError> {
        let drain = self
            .store
            .drain_queue(
                request.workspace_name,
                ObservedValuesDrainBudget::new(
                    OBSERVED_REBUILD_DRAIN_MAX_JOBS,
                    Duration::from_millis(OBSERVED_REBUILD_DRAIN_MS),
                ),
            )
            .map_err(|error| observed_sqlite_app_error(&error))?;
        log_drain_maintenance(request.workspace_name, &drain);
        let result = self
            .store
            .rebuild_fts(request.workspace_name, policy)
            .map_err(|error| observed_sqlite_app_error(&error))?;
        Ok(observed_rebuild_provider_result(
            &drain,
            result.canonical_rows_scanned,
            result.fts_rows_rebuilt,
        ))
    }

    pub(crate) fn clear_data(
        &self,
        request: SearchProviderClearRequest<'_>,
    ) -> Result<SearchProviderClearOutcome, crate::search::result::SearchManagerError> {
        if !matches!(
            request.scope,
            SearchDataScope::ObservedValues | SearchDataScope::All
        ) {
            return Err(AppError::InvalidInput(
                "observed-value search provider supports observed-values or all clear scope"
                    .to_string(),
            )
            .into());
        }
        let result = match request.target {
            SearchClearTarget::Workspace => self
                .store
                .clear_workspace(request.workspace_name)
                .map_err(|error| observed_sqlite_app_error(&error))?,
            SearchClearTarget::Source(source_name) => self
                .store
                .clear_source(request.workspace_name, source_name)
                .map_err(|error| observed_sqlite_app_error(&error))?,
        };
        Ok(SearchProviderClearOutcome {
            result: observed_clear_provider_result(result),
            storage_cleanup: if request.compact_after_clear {
                let compaction = self
                    .store
                    .compact_after_clear(request.workspace_name)
                    .map_err(|error| observed_sqlite_app_error(&error))?;
                Some(observed_storage_cleanup_result(&compaction))
            } else {
                None
            },
        })
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
                    .saturating_add(u32::from(drain_error.is_some()))
                    .saturating_add(failed_source_count),
                returned_count: u32::try_from(candidates.len()).unwrap_or(u32::MAX),
                has_more,
                budget_exhausted: drain.budget_exhausted,
                timed_out: false,
                stale_index: drain.remaining_queue_depth > 0
                    || drain.failed_jobs > 0
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
    let load_failure_note = observed_policy_load_failure_note(policy);
    match (state, drain_error, load_failure_note.as_deref()) {
        (SearchProviderState::ResultsFound, _, _) => {
            format!("found {candidate_count} observed value search result(s)")
        }
        (SearchProviderState::Empty, _, _) => "no observed value search results found".to_string(),
        (SearchProviderState::Partial, Some(error), Some(policy_note)) => {
            format!("observed value search used cached local memory; {error}; {policy_note}")
        }
        (SearchProviderState::Partial, Some(error), _) => {
            format!("observed value search used cached local memory; {error}")
        }
        (SearchProviderState::Partial, _, Some(policy_note)) => policy_note.to_string(),
        (SearchProviderState::Partial, _, _) if drain.budget_exhausted => format!(
            "observed value search used partial local memory; {} queue job(s) remain",
            drain.remaining_queue_depth
        ),
        (SearchProviderState::Partial, _, _) => {
            "partial observed value results were returned".to_string()
        }
        (
            SearchProviderState::Error
            | SearchProviderState::NotEnabled
            | SearchProviderState::Skipped,
            _,
            _,
        ) => "observed value search did not run".to_string(),
    }
}

fn observed_policy_load_failure_note(policy: &ObservedValuesRetrievalPolicy) -> Option<String> {
    if !policy.has_load_failures() {
        return None;
    }
    let source_names = policy.failed_source_names().join(", ");
    Some(format!(
        "observed value search skipped {} source(s) whose live scopes could not be loaded: {source_names}",
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

fn observed_drain_provider_result(result: &ObservedValuesDrainResult) -> SearchMaintenanceResult {
    let state = if observed_drain_is_partial(result) {
        SearchMaintenanceState::Partial
    } else if observed_drain_did_no_work(result) {
        SearchMaintenanceState::Noop
    } else {
        SearchMaintenanceState::Completed
    };
    SearchMaintenanceResult {
        provider: SearchProviderKind::ObservedValues,
        state,
        note: observed_drain_note(result),
        detail: Some(SearchMaintenanceDetail::ObservedDrain(
            ObservedDrainMaintenanceResult {
                queue_jobs_processed: result.queue_jobs_processed,
                stale_jobs_skipped: result.stale_jobs_skipped,
                failed_jobs: result.failed_jobs,
                canonical_rows_upserted: result.canonical_rows_upserted,
                fts_rows_written: result.fts_rows_written,
                remaining_queue_depth: result.remaining_queue_depth,
                budget_exhausted: result.budget_exhausted,
                stale_rows_purged: result.stale_rows_purged,
                evicted_rows: result.evicted_rows,
                storage_limit_reached: result.storage_limit_reached,
            },
        )),
    }
}

fn observed_rebuild_provider_result(
    drain: &ObservedValuesDrainResult,
    canonical_rows_scanned: u32,
    fts_rows_rebuilt: u32,
) -> SearchMaintenanceResult {
    SearchMaintenanceResult {
        provider: SearchProviderKind::ObservedValues,
        state: if observed_drain_is_partial(drain) {
            SearchMaintenanceState::Partial
        } else {
            SearchMaintenanceState::Completed
        },
        note: observed_rebuild_note(drain, canonical_rows_scanned),
        detail: Some(SearchMaintenanceDetail::ObservedRebuild(
            ObservedRebuildMaintenanceResult {
                canonical_rows_scanned,
                fts_rows_rebuilt,
            },
        )),
    }
}

fn observed_drain_is_partial(result: &ObservedValuesDrainResult) -> bool {
    result.budget_exhausted
        || result.remaining_queue_depth > 0
        || result.failed_jobs > 0
        || result.storage_limit_reached
}

fn observed_drain_did_no_work(result: &ObservedValuesDrainResult) -> bool {
    result.queue_jobs_processed == 0
        && result.stale_jobs_skipped == 0
        && result.stale_rows_purged == 0
        && result.evicted_rows == 0
}

pub(crate) fn observed_clear_provider_result(
    result: ObservedValuesClearResult,
) -> SearchMaintenanceResult {
    let deleted_total = result
        .values
        .saturating_add(result.fts_rows)
        .saturating_add(result.queue_jobs);
    SearchMaintenanceResult {
        provider: SearchProviderKind::ObservedValues,
        state: if deleted_total == 0 {
            SearchMaintenanceState::Noop
        } else {
            SearchMaintenanceState::Completed
        },
        note: "cleared observed-value search data".to_string(),
        detail: Some(SearchMaintenanceDetail::ObservedClear(
            ObservedClearMaintenanceResult {
                values: result.values,
                fts_rows: result.fts_rows,
                queue_jobs: result.queue_jobs,
            },
        )),
    }
}

fn observed_drain_note(result: &ObservedValuesDrainResult) -> String {
    let note = if result.budget_exhausted {
        format!(
            "drained {} observed-value queue job(s); {} remain",
            result.queue_jobs_processed, result.remaining_queue_depth
        )
    } else if result.failed_jobs > 0 && result.remaining_queue_depth > 0 {
        format!(
            "drained observed-value queue with {} failed job(s) left for retry",
            result.failed_jobs
        )
    } else if result.failed_jobs > 0 {
        format!(
            "drained observed-value queue with {} failed job(s) dead-lettered",
            result.failed_jobs
        )
    } else {
        format!(
            "drained {} observed-value queue job(s)",
            result.queue_jobs_processed
        )
    };
    if result.stale_rows_purged == 0 && result.evicted_rows == 0 && !result.storage_limit_reached {
        return note;
    }
    format!(
        "{note}; purged {} stale row(s), evicted {} row(s), storage limit reached {}",
        result.stale_rows_purged, result.evicted_rows, result.storage_limit_reached
    )
}

fn observed_rebuild_note(drain: &ObservedValuesDrainResult, canonical_rows_scanned: u32) -> String {
    let drained_jobs = drain
        .queue_jobs_processed
        .saturating_add(drain.stale_jobs_skipped)
        .saturating_add(drain.failed_jobs);
    if drained_jobs == 0 && drain.remaining_queue_depth == 0 {
        return format!(
            "rebuilt observed-value FTS projection from {canonical_rows_scanned} row(s)"
        );
    }
    format!(
        "drained {} observed-value queue job(s), then rebuilt observed-value FTS projection from {} row(s); {} remain",
        drained_jobs, canonical_rows_scanned, drain.remaining_queue_depth
    )
}

fn log_drain_maintenance(workspace_name: &WorkspaceName, result: &ObservedValuesDrainResult) {
    if result.budget_exhausted
        || result.storage_limit_reached
        || result.stale_rows_purged > 0
        || result.evicted_rows > 0
    {
        tracing::debug!(
            workspace = %workspace_name,
            remaining_queue_depth = result.remaining_queue_depth,
            queue_jobs_processed = result.queue_jobs_processed,
            stale_jobs_skipped = result.stale_jobs_skipped,
            failed_jobs = result.failed_jobs,
            stale_rows_purged = result.stale_rows_purged,
            evicted_rows = result.evicted_rows,
            storage_limit_reached = result.storage_limit_reached,
            budget_exhausted = result.budget_exhausted,
            "observed-value queue drain ran storage maintenance"
        );
    }
}

fn observed_storage_cleanup_result(
    result: &SqliteSearchCompactionResult,
) -> SearchStorageCleanupResult {
    let (state, note) = match (
        result.wal_checkpoint_truncate_completed,
        result.vacuum_completed,
    ) {
        (true, true) => (
            SearchMaintenanceState::Completed,
            "local search storage cleanup completed",
        ),
        (true, false) | (false, true) => (
            SearchMaintenanceState::Partial,
            "local search storage cleanup partially completed",
        ),
        (false, false) => (
            SearchMaintenanceState::Failed,
            "local search storage cleanup did not complete",
        ),
    };
    if state != SearchMaintenanceState::Completed {
        tracing::warn!(
            wal_checkpoint_truncate_completed = result.wal_checkpoint_truncate_completed,
            vacuum_completed = result.vacuum_completed,
            detail = %result.note,
            "local search storage cleanup did not fully complete"
        );
    }
    SearchStorageCleanupResult {
        state,
        note: note.to_string(),
    }
}

fn observed_sqlite_app_error(error: &SqliteSearchError) -> AppError {
    if error.is_lock_contention() {
        AppError::Unavailable(format!("search maintenance storage is busy: {error}"))
    } else if error.is_storage_exhaustion() {
        AppError::ResourceExhausted(format!("search maintenance storage is exhausted: {error}"))
    } else if matches!(
        error,
        SqliteSearchError::UnsupportedCapability { .. }
            | SqliteSearchError::UnsupportedSchemaVersion { .. }
    ) {
        AppError::FailedPrecondition(format!("search maintenance is not supported: {error}"))
    } else {
        AppError::Internal(format!("search maintenance storage failed: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::TransactionBehavior;
    use tempfile::tempdir;

    use super::{
        ObservedValuesProvider, observed_clear_provider_result, observed_drain_provider_result,
        observed_rebuild_provider_result, observed_search_outcome,
    };
    use crate::bootstrap::AppError;
    use crate::search::maintenance::{
        ObservedClearMaintenanceResult, SearchMaintenanceDetail, SearchMaintenanceState,
    };
    use crate::search::observed::sqlite_projection::{
        ObservedValuesDrainBudget, ObservedValuesDrainResult, ObservedValuesSearchHit,
        ObservedValuesSearchHits,
    };
    use crate::search::observed::sqlite_queue::{
        ObservedValuesQueueJob, ObservedValuesSurfaceKind,
    };
    use crate::search::observed::sqlite_store::{
        ObservedValuesClearResult, SqliteObservedValuesStore,
    };
    use crate::search::observed::{
        ObservedValuesLiveScope, ObservedValuesLiveScopeLoadFailure, ObservedValuesRetrievalPolicy,
    };
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
            .current_generations(&workspace, "github")
            .expect("generation");
        store
            .enqueue_source_scan(&workspace, &test_job(), generation)
            .expect("enqueue observed value");
        let provider = ObservedValuesProvider::new(layout);
        let request = SearchRequest::new(workspace, "payment", 10).expect("valid search request");

        let policy = test_policy(&["scope"]);

        let outcome = provider.search(&request, Ok(&policy));

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
            .current_generations(&workspace, "github")
            .expect("generation");
        store
            .enqueue_source_scan(
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
            .enqueue_source_scan(
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

        let policy = test_policy(&["scope-1", "scope-2"]);

        let outcome = provider.search(&request, Ok(&policy));
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

    #[test]
    fn observed_provider_fails_closed_when_live_scope_policy_is_unavailable() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let provider = ObservedValuesProvider::new(layout);
        let request = SearchRequest::new(WorkspaceName::default(), "payment", 10).expect("request");
        let error = AppError::FailedPrecondition("missing source scope".to_string());

        let outcome = provider.search(&request, Err(&error));

        assert_eq!(outcome.status.provider, SearchProviderKind::ObservedValues);
        assert_eq!(outcome.status.state, SearchProviderState::Error);
        assert!(outcome.candidates.is_empty());
        assert!(
            outcome
                .status
                .note
                .contains("could not load live source scope"),
            "unexpected note: {}",
            outcome.status.note
        );
    }

    #[test]
    fn observed_provider_degrades_when_one_live_scope_source_fails() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let generation = store
            .current_generations(&workspace, "github")
            .expect("generation");
        store
            .enqueue_source_scan(&workspace, &test_job(), generation)
            .expect("enqueue observed value");
        let provider = ObservedValuesProvider::new(layout);
        let request = SearchRequest::new(workspace, "payment", 10).expect("valid search request");
        let policy = ObservedValuesRetrievalPolicy::with_load_failures(
            test_live_scopes(&["scope"]),
            vec![ObservedValuesLiveScopeLoadFailure {
                source_name: "jira".to_string(),
                message: "manifest parse failed".to_string(),
            }],
            365,
        );

        let outcome = provider.search(&request, Ok(&policy));

        assert_eq!(outcome.status.provider, SearchProviderKind::ObservedValues);
        assert_eq!(outcome.status.state, SearchProviderState::Partial);
        let coverage = outcome.status.coverage.expect("coverage");
        assert_eq!(coverage.failed_units, 1);
        assert!(coverage.stale_index);
        assert_eq!(outcome.candidates.len(), 1);
        assert!(
            outcome.status.note.contains("skipped 1 source")
                && outcome.status.note.contains("jira"),
            "unexpected note: {}",
            outcome.status.note
        );
    }

    #[test]
    fn observed_provider_diversifies_observed_values_by_surface_and_column() {
        let policy = test_policy(&["scope"]);
        let hits = ObservedValuesSearchHits {
            hits: vec![
                test_hit("issues", "Payment alpha", 1),
                test_hit("issues", "Payment beta", 1),
                test_hit("pulls", "Payment pull", 1),
                test_hit("issues", "Payment gamma", 1),
            ],
            value_count: 4,
            retrieval_limited: true,
        };

        let outcome = observed_search_outcome(
            hits,
            &ObservedValuesDrainResult::default(),
            None,
            &policy,
            3,
        );

        assert_eq!(
            observed_values(&outcome.candidates),
            ["Payment alpha", "Payment pull", "Payment beta",]
        );
    }

    #[test]
    fn observed_drain_budget_exhaustion_reports_partial_not_empty() {
        let result = ObservedValuesDrainResult {
            remaining_queue_depth: 1,
            budget_exhausted: true,
            ..ObservedValuesDrainResult::default()
        };

        let provider_result = observed_drain_provider_result(&result);

        assert_eq!(provider_result.state, SearchMaintenanceState::Partial);
    }

    #[test]
    fn observed_drain_reports_storage_governance() {
        let result = ObservedValuesDrainResult {
            stale_rows_purged: 2,
            evicted_rows: 3,
            storage_limit_reached: true,
            ..ObservedValuesDrainResult::default()
        };

        let provider_result = observed_drain_provider_result(&result);

        assert_eq!(provider_result.state, SearchMaintenanceState::Partial);
        let Some(SearchMaintenanceDetail::ObservedDrain(detail)) = provider_result.detail else {
            panic!("expected observed drain detail");
        };
        assert_eq!(detail.stale_rows_purged, 2);
        assert_eq!(detail.evicted_rows, 3);
        assert!(detail.storage_limit_reached);
    }

    #[test]
    fn observed_drain_noop_accounts_for_governance_work() {
        let noop = observed_drain_provider_result(&ObservedValuesDrainResult::default());
        let purged = observed_drain_provider_result(&ObservedValuesDrainResult {
            stale_rows_purged: 1,
            ..ObservedValuesDrainResult::default()
        });
        let evicted = observed_drain_provider_result(&ObservedValuesDrainResult {
            evicted_rows: 1,
            ..ObservedValuesDrainResult::default()
        });

        assert_eq!(noop.state, SearchMaintenanceState::Noop);
        assert_eq!(purged.state, SearchMaintenanceState::Completed);
        assert_eq!(evicted.state, SearchMaintenanceState::Completed);
    }

    #[test]
    fn observed_rebuild_failed_jobs_report_partial() {
        let drain = ObservedValuesDrainResult {
            failed_jobs: 1,
            remaining_queue_depth: 0,
            ..ObservedValuesDrainResult::default()
        };

        let provider_result = observed_rebuild_provider_result(&drain, 3, 3);

        assert_eq!(provider_result.state, SearchMaintenanceState::Partial);
    }

    #[test]
    fn observed_rebuild_with_no_rows_still_reports_completed() {
        let provider_result =
            observed_rebuild_provider_result(&ObservedValuesDrainResult::default(), 0, 0);

        assert_eq!(provider_result.state, SearchMaintenanceState::Completed);
    }

    #[test]
    fn observed_clear_reports_noop_only_when_nothing_was_deleted() {
        let noop = observed_clear_provider_result(ObservedValuesClearResult {
            values: 0,
            fts_rows: 0,
            queue_jobs: 0,
        });
        let completed = observed_clear_provider_result(ObservedValuesClearResult {
            values: 1,
            fts_rows: 0,
            queue_jobs: 0,
        });

        assert_eq!(noop.state, SearchMaintenanceState::Noop);
        assert_eq!(completed.state, SearchMaintenanceState::Completed);
        assert!(matches!(
            completed.detail,
            Some(SearchMaintenanceDetail::ObservedClear(
                ObservedClearMaintenanceResult { values: 1, .. }
            ))
        ));
    }

    fn test_job() -> ObservedValuesQueueJob {
        test_job_with("scope", "Payment outage")
    }

    fn test_job_with(source_scope_id: &str, display_value: &str) -> ObservedValuesQueueJob {
        ObservedValuesQueueJob {
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

    fn test_policy(scopes: &[&str]) -> ObservedValuesRetrievalPolicy {
        ObservedValuesRetrievalPolicy::new(test_live_scopes(scopes), 365)
    }

    fn test_live_scopes(scopes: &[&str]) -> Vec<ObservedValuesLiveScope> {
        scopes
            .iter()
            .map(|scope| ObservedValuesLiveScope {
                source_name: "github".to_string(),
                source_scope_id: (*scope).to_string(),
                surface_kind: ObservedValuesSurfaceKind::Table,
                surface_name: "issues".to_string(),
            })
            .collect()
    }

    fn test_hit(
        surface_name: &str,
        display_value: &str,
        observation_count: u64,
    ) -> ObservedValuesSearchHit {
        ObservedValuesSearchHit {
            source_name: "github".to_string(),
            source_scope_id: format!("{surface_name}-scope"),
            surface_kind: ObservedValuesSurfaceKind::Table,
            surface_name: surface_name.to_string(),
            column_name: "title".to_string(),
            value_key: display_value.to_ascii_lowercase().replace(' ', "-"),
            display_value: display_value.to_string(),
            last_observed_at: "2026-07-09T00:00:00.000Z".to_string(),
            observation_count,
        }
    }

    fn observed_values(candidates: &[crate::search::result::SearchCandidate]) -> Vec<&str> {
        candidates
            .iter()
            .map(|candidate| {
                let SearchPayload::ObservedValue(observed) = &candidate.payload else {
                    panic!("expected observed value payload");
                };
                observed.value.as_str()
            })
            .collect()
    }
}
