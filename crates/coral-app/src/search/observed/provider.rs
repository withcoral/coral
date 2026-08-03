//! Observed-values Universal Search provider.

use std::time::Duration;

use crate::bootstrap::AppError;
use crate::search::catalog::provider::resolve_surface_id;
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
use crate::search::provider::{
    LocalSearchWriteCoordinator, PreparedRetrievers, ProviderFailure, ProviderSearchOutcome,
    Retriever, RetrieverError, RetrieverOutcome, SearchExecutionContext, SearchProvider,
};
use crate::search::result::{
    FieldValues, MatchEvidence, ProviderCoverage, ProviderStatus, Ranking, RetrieverId,
    SearchProviderKind, SearchProviderState, SearchRequest, SearchSurfaceId, SearchSurfaceKind,
    SurfaceMatch,
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
    write_coordinator: LocalSearchWriteCoordinator,
}

impl ObservedValuesProvider {
    pub(crate) fn with_write_coordinator(
        layout: AppStateLayout,
        write_coordinator: LocalSearchWriteCoordinator,
    ) -> Self {
        Self {
            store: SqliteObservedValuesStore::new(layout),
            write_coordinator,
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
        self.write_coordinator.run(&request.workspace_name, || {
            self.search_with_policy(request, policy)
        })
    }

    fn search_with_policy(
        &self,
        request: &SearchRequest,
        policy: &ObservedValuesRetrievalPolicy,
    ) -> ProviderSearchOutcome {
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
            &request.terms,
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
        log_storage_drops(workspace_name, &result);
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
                log_storage_drops(workspace_name, &drain);
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
        log_storage_drops(request.workspace_name, &drain);
        log_drain_maintenance(request.workspace_name, &drain);
        let result = self
            .store
            .rebuild_fts(request.workspace_name, policy)
            .map_err(|error| observed_sqlite_app_error(&error))?;
        Ok(observed_rebuild_provider_result(
            &drain,
            policy,
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
                .clear_workspace_and_advance_epoch(request.workspace_name)
                .map_err(|error| observed_sqlite_app_error(&error))?,
            SearchClearTarget::Source(owner_source_name) => self
                .store
                .clear_source_and_advance_epoch(request.workspace_name, owner_source_name.as_str())
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
    terms: &[String],
    drain: &ObservedValuesDrainResult,
    drain_error: Option<&str>,
    policy: &ObservedValuesRetrievalPolicy,
    retrieval_limit: usize,
) -> ProviderSearchOutcome {
    let has_more = hits.retrieval_limited;
    let failed_source_count = policy.failed_source_count();
    let diversified_hits = ranking::diversify_observed_hits(hits.hits, retrieval_limit);
    let candidates = observed_entry_matches(diversified_hits, terms);
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
        rankings: vec![Ranking {
            retriever: RetrieverId::ObservedValues,
            matches: candidates,
        }],
    }
}

/// Converts store-ordered hits into entry matches, preserving store order.
///
/// Only values the query names outright become evidence. A fuzzy substring hit
/// would let a common value like `open` elect every entry with a status column,
/// and under reciprocal-rank fusion a rank-0 value match ties with the best
/// catalog match — so the store's substring recall is deliberately not promoted
/// to entry evidence here.
fn observed_entry_matches(
    hits: Vec<ObservedValuesSearchHit>,
    terms: &[String],
) -> Vec<SurfaceMatch> {
    let mut matches = Vec::<SurfaceMatch>::new();
    for hit in hits {
        if !query_names_value(terms, &hit.display_value) {
            continue;
        }
        let id = SearchSurfaceId {
            catalog_name: None,
            schema_name: hit.source_name,
            name: hit.surface_name,
            kind: surface_kind(hit.surface_kind),
        };
        let values = FieldValues {
            field: hit.column_name,
            values: vec![hit.display_value],
        };
        // Store order is the ranking; an entry keeps the position of its best
        // value and accumulates the rest as evidence.
        if let Some(existing) = matches.iter_mut().find(|existing| existing.id == id) {
            existing.evidence.merge(MatchEvidence {
                matched_fields: Vec::new(),
                matching_values: vec![values],
            });
        } else {
            matches.push(SurfaceMatch {
                id,
                evidence: MatchEvidence {
                    matched_fields: Vec::new(),
                    matching_values: vec![values],
                },
            });
        }
    }
    matches
}

/// True when the observed value appears as a whole query term.
///
/// Compares the displayed value, not `value_key` — that column is a content
/// hash, so matching against it silently accepts nothing.
fn query_names_value(terms: &[String], display_value: &str) -> bool {
    let value = display_value.trim().to_lowercase();
    !value.is_empty() && terms.iter().any(|term| term == &value)
}

fn surface_kind(kind: ObservedValuesSurfaceKind) -> SearchSurfaceKind {
    match kind {
        ObservedValuesSurfaceKind::Table => SearchSurfaceKind::Table,
        ObservedValuesSurfaceKind::Function => SearchSurfaceKind::TableFunction,
    }
}

impl SearchProvider for ObservedValuesProvider {
    fn kind(&self) -> SearchProviderKind {
        SearchProviderKind::ObservedValues
    }

    /// Observed values have a single retriever, and its work is inseparable
    /// from preparation: the queue drain that decides coverage also decides
    /// what the search can see. So retrieval happens here and the retriever
    /// carries the result.
    fn retrievers(
        &self,
        context: &SearchExecutionContext,
    ) -> Result<PreparedRetrievers, ProviderFailure> {
        let policy = context
            .observed_values_policy
            .as_ref()
            .ok_or_else(|| ProviderFailure {
                state: SearchProviderState::Error,
                note: "observed value search is enabled without a retrieval policy".to_string(),
                coverage: None,
            })?
            .as_ref()
            .map_err(|error| ProviderFailure {
                state: SearchProviderState::Error,
                note: format!("observed value retrieval policy is unavailable: {error}"),
                coverage: None,
            })?;
        let outcome = self.search(&context.request, Ok(policy));
        if let Some(failure) = observed_outcome_failure(&outcome.status) {
            return Err(failure);
        }
        let degraded = (outcome.status.state == SearchProviderState::Partial)
            .then(|| outcome.status.note.clone());
        let mut matches = outcome
            .rankings
            .into_iter()
            .flat_map(|ranking| ranking.matches)
            .collect::<Vec<_>>();
        if let Ok(resolution) = context.catalog_resolution.as_ref() {
            for entry_match in &mut matches {
                if let Some(id) = resolve_surface_id(&resolution.catalog, &entry_match.id) {
                    entry_match.id = id;
                }
            }
        }
        Ok(PreparedRetrievers {
            retrievers: vec![Box::new(RetrievedValues { matches })],
            coverage: outcome.status.coverage,
            degraded,
        })
    }
}

fn observed_outcome_failure(status: &ProviderStatus) -> Option<ProviderFailure> {
    (status.state == SearchProviderState::Error).then(|| ProviderFailure {
        state: status.state,
        note: status.note.clone(),
        coverage: status.coverage.clone(),
    })
}

/// Carries values already retrieved during preparation.
struct RetrievedValues {
    matches: Vec<SurfaceMatch>,
}

impl Retriever for RetrievedValues {
    fn id(&self) -> RetrieverId {
        RetrieverId::ObservedValues
    }

    fn retrieve(&self, _request: &SearchRequest) -> Result<RetrieverOutcome, RetrieverError> {
        Ok(RetrieverOutcome {
            matches: self.matches.clone(),
            // Observed retrieval already reported its own limit through coverage.
            retrieval_limited: false,
        })
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
        rankings: Vec::new(),
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
        rankings: Vec::new(),
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
            observed_drain_maintenance_result(result),
        )),
    }
}

fn observed_drain_maintenance_result(
    result: &ObservedValuesDrainResult,
) -> ObservedDrainMaintenanceResult {
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
        storage_jobs_dropped: result.storage_jobs_dropped,
    }
}

fn observed_rebuild_provider_result(
    drain: &ObservedValuesDrainResult,
    policy: &ObservedValuesRetrievalPolicy,
    canonical_rows_scanned: u32,
    fts_rows_rebuilt: u32,
) -> SearchMaintenanceResult {
    SearchMaintenanceResult {
        provider: SearchProviderKind::ObservedValues,
        state: if observed_drain_is_partial(drain) || policy.has_load_failures() {
            SearchMaintenanceState::Partial
        } else {
            SearchMaintenanceState::Completed
        },
        note: observed_rebuild_note(drain, policy, canonical_rows_scanned),
        detail: Some(SearchMaintenanceDetail::ObservedRebuild(
            ObservedRebuildMaintenanceResult {
                canonical_rows_scanned,
                fts_rows_rebuilt,
                drain: observed_drain_maintenance_result(drain),
            },
        )),
    }
}

fn observed_drain_is_partial(result: &ObservedValuesDrainResult) -> bool {
    result.budget_exhausted
        || result.remaining_queue_depth > 0
        || result.failed_jobs > 0
        || result.storage_jobs_dropped > 0
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
    let mut note = format!(
        "drained {} observed-value queue job(s)",
        result.queue_jobs_processed
    );
    let mut causes = Vec::new();
    if result.failed_jobs > 0 {
        let disposition = if result.remaining_queue_depth > 0 {
            "left for retry"
        } else {
            "dead-lettered"
        };
        causes.push(format!(
            "{} failed job(s) {disposition}",
            result.failed_jobs
        ));
    } else if result.remaining_queue_depth > 0 && !result.budget_exhausted {
        causes.push(format!(
            "{} queue job(s) remain",
            result.remaining_queue_depth
        ));
    }
    if result.storage_jobs_dropped > 0 {
        causes.push(format!(
            "{} queued observation job(s) omitted to preserve storage headroom",
            result.storage_jobs_dropped
        ));
    }
    if result.stale_rows_purged > 0 {
        causes.push(format!(
            "purged {} stale observed-value row(s)",
            result.stale_rows_purged
        ));
    }
    if result.evicted_rows > 0 {
        causes.push(format!(
            "evicted {} observed-value row(s)",
            result.evicted_rows
        ));
    }
    if result.storage_limit_reached {
        causes.push("storage limit remains reached".to_string());
    }
    if result.budget_exhausted {
        causes.push(format!(
            "cooperative budget exhausted with {} queue job(s) remaining",
            result.remaining_queue_depth
        ));
    }
    if !causes.is_empty() {
        note.push_str("; ");
        note.push_str(&causes.join("; "));
    }
    note
}

fn observed_rebuild_note(
    drain: &ObservedValuesDrainResult,
    policy: &ObservedValuesRetrievalPolicy,
    canonical_rows_scanned: u32,
) -> String {
    let attempted_jobs = drain
        .queue_jobs_processed
        .saturating_add(drain.stale_jobs_skipped)
        .saturating_add(drain.failed_jobs)
        .saturating_add(drain.storage_jobs_dropped);
    let mut note = if attempted_jobs == 0 {
        format!("rebuilt observed-value FTS projection from {canonical_rows_scanned} row(s)")
    } else {
        format!(
            "attempted {attempted_jobs} observed-value queue job(s), then rebuilt observed-value FTS projection from {canonical_rows_scanned} row(s)"
        )
    };
    let drain_note = observed_drain_note(drain);
    if observed_drain_is_partial(drain) {
        let (_, partial_detail) = drain_note
            .split_once("; ")
            .unwrap_or((drain_note.as_str(), drain_note.as_str()));
        note.push_str("; ");
        note.push_str(partial_detail);
    }
    if policy.has_load_failures() {
        note.push_str("; skipped ");
        note.push_str(&policy.failed_source_count().to_string());
        note.push_str(" owner source(s) whose live scopes could not be loaded: ");
        note.push_str(&policy.failed_owner_source_names().join(", "));
    }
    note
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
            storage_jobs_dropped = result.storage_jobs_dropped,
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
mod entry_match_tests {
    use super::{observed_entry_matches, observed_outcome_failure, query_names_value};
    use crate::hash::sha256_hex;
    use crate::search::observed::sqlite_projection::ObservedValuesSearchHit;
    use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;
    use crate::search::result::{ProviderStatus, SearchProviderKind, SearchProviderState};

    #[test]
    fn a_value_the_query_names_becomes_evidence_on_its_entry() {
        // `value_key` is a content hash. Matching the query against it instead
        // of the displayed value silently drops every observed value, which is
        // invisible unless a test uses a realistic hash here.
        let hits = vec![
            hit("linear", "issues", "identifier", "UI-520"),
            hit("linear", "issues", "priority_label", "No priority"),
            hit("linear", "teams", "key", "PLATFORM"),
        ];
        let terms = vec!["linear".to_string(), "ui-520".to_string()];

        let matches = observed_entry_matches(hits, &terms);

        assert_eq!(matches.len(), 1, "only the named value elects its entry");
        let entry = matches.first().expect("one match");
        assert_eq!(entry.id.name, "issues");
        assert_eq!(
            entry.evidence.matching_values.first().map(|values| (
                values.field.as_str(),
                values.values.first().map(String::as_str)
            )),
            Some(("identifier", Some("UI-520")))
        );
    }

    #[test]
    fn value_matching_is_case_insensitive_and_ignores_the_content_hash() {
        assert!(query_names_value(&["ui-520".to_string()], "UI-520"));
        assert!(query_names_value(
            &["no priority".to_string()],
            "No priority"
        ));
        assert!(!query_names_value(&["ui".to_string()], "UI-520"));
        assert!(!query_names_value(&[sha256_hex(b"UI-520")], "UI-520"));
    }

    #[test]
    fn observed_storage_error_remains_a_provider_failure() {
        let failure = observed_outcome_failure(&ProviderStatus {
            provider: SearchProviderKind::ObservedValues,
            state: SearchProviderState::Error,
            note: "observed value storage is unavailable".to_string(),
            coverage: None,
        })
        .expect("error status must stop provider preparation");

        assert_eq!(failure.state, SearchProviderState::Error);
        assert_eq!(failure.note, "observed value storage is unavailable");
    }

    fn hit(
        source_name: &str,
        surface_name: &str,
        column_name: &str,
        display_value: &str,
    ) -> ObservedValuesSearchHit {
        ObservedValuesSearchHit {
            source_name: source_name.to_string(),
            source_scope_id: "workspace".to_string(),
            surface_kind: ObservedValuesSurfaceKind::Table,
            surface_name: surface_name.to_string(),
            column_name: column_name.to_string(),
            value_key: sha256_hex(display_value.as_bytes()),
            display_value: display_value.to_string(),
            last_observed_at: "2026-07-09T12:00:00.000Z".to_string(),
            observation_count: 1,
        }
    }
}
