//! Bounded, failure-isolated provider-native Universal Search fanout.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use coral_engine::{QueryCancellationToken, QueryExecutionControls, QueryExecutionFailureKind};
use tokio::task;
use tokio::time::Instant;

use crate::query::manager::QueryManager;
use crate::query::{
    ExecuteSelectedTableFunction, SelectedTableFunctionExecution,
    SelectedTableFunctionExecutionError,
};
use crate::search::provider::{
    ProviderSearchFuture, ProviderSearchOutcome, SearchExecutionContext, SearchProvider,
};
use crate::search::result::{
    NativeSearchDiagnostic, NativeSearchDiagnosticReason, ProviderCoverage, ProviderStatus,
    SearchCandidate, SearchPayload, SearchProviderKind,
};
use crate::sources::universal_search::{
    ResolvedUniversalSearchRoute, UniversalSearchResolution, UniversalSearchResolutionOrigin,
};
use crate::workspaces::WorkspaceName;

use super::dedupe::{cap_request, deduplicate};
use super::diagnostics::{
    NativeCallFailure, NativeProviderSummary, bound_diagnostics, explicit_denial, failed_call,
    provider_state, resolution_failure, skipped_route, successful_call,
};
use super::normalize::normalize_batches;
use super::{MAX_RESULTS_PER_FUNCTION, NativeCandidate};

const GLOBAL_FANOUT_BUDGET: Duration = Duration::from_millis(750);
const PER_CALL_BUDGET: Duration = Duration::from_millis(600);
const MINIMUM_START_BUDGET: Duration = Duration::from_millis(100);
const CANCELLATION_CLEANUP_GRACE: Duration = Duration::from_millis(25);
const MAX_SELECTED_FUNCTIONS: usize = 4;

type SelectedFunctionFuture = Pin<
    Box<
        dyn Future<
                Output = Result<
                    SelectedTableFunctionExecution,
                    SelectedTableFunctionExecutionError,
                >,
            > + Send
            + 'static,
    >,
>;

trait SelectedFunctionExecutor: Send + Sync {
    fn execute(&self, command: ExecuteSelectedTableFunction) -> SelectedFunctionFuture;
}

impl SelectedFunctionExecutor for QueryManager {
    fn execute(&self, command: ExecuteSelectedTableFunction) -> SelectedFunctionFuture {
        let manager = self.clone();
        Box::pin(async move { manager.execute_selected_table_function(command).await })
    }
}

/// Executes only source-authorised routes under the request-wide native budget.
#[derive(Clone)]
pub(crate) struct NativeFanoutProvider {
    executor: Arc<dyn SelectedFunctionExecutor>,
    limits: NativeFanoutLimits,
}

pub(crate) struct NativeFanoutRegistration {
    pub(crate) provider: Arc<dyn SearchProvider>,
}

#[derive(Debug, Clone, Copy)]
struct NativeFanoutLimits {
    global_budget: Duration,
    per_call_budget: Duration,
    minimum_start_budget: Duration,
    cleanup_grace: Duration,
}

impl Default for NativeFanoutLimits {
    fn default() -> Self {
        Self {
            global_budget: GLOBAL_FANOUT_BUDGET,
            per_call_budget: PER_CALL_BUDGET,
            minimum_start_budget: MINIMUM_START_BUDGET,
            cleanup_grace: CANCELLATION_CLEANUP_GRACE,
        }
    }
}

impl NativeFanoutProvider {
    #[expect(
        dead_code,
        reason = "the follow-up feature gate installs this registration in production"
    )]
    pub(crate) fn registration(executor: QueryManager) -> NativeFanoutRegistration {
        let provider: Arc<dyn SearchProvider> = Arc::new(Self {
            executor: Arc::new(executor),
            limits: NativeFanoutLimits::default(),
        });
        NativeFanoutRegistration { provider }
    }

    #[cfg(test)]
    fn for_test(executor: Arc<dyn SelectedFunctionExecutor>, limits: NativeFanoutLimits) -> Self {
        Self { executor, limits }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the bounded wave, accounting, and diagnostic ordering remain auditable together"
    )]
    async fn search_native(&self, context: Arc<SearchExecutionContext>) -> ProviderSearchOutcome {
        let global_deadline = context
            .request_started_at
            .checked_add(self.limits.global_budget)
            .unwrap_or(context.request_started_at);
        if Instant::now() >= global_deadline {
            return empty_outcome(
                0,
                true,
                Vec::new(),
                "native fanout budget expired before route setup completed",
            );
        }
        let reports = match context.catalog_resolution.as_ref() {
            Ok(resolution) => &resolution.universal_search_resolutions,
            Err(_error) => {
                tracing::error!("failed to resolve native Universal Search routes");
                return provider_failure_outcome();
            }
        };
        let inventory = RouteInventory::from_reports(reports);
        let wave_cancellation = QueryCancellationToken::new();
        let eligible_count = inventory.eligible_count;
        let mut resolution_diagnostics = inventory.resolution_diagnostics;
        let already_omitted = inventory.already_omitted_diagnostics;
        let mut unattempted_diagnostics = inventory
            .unselected
            .iter()
            .map(|route| {
                skipped_route(
                    &route.route,
                    NativeSearchDiagnosticReason::FanoutLimitReached,
                )
            })
            .collect::<Vec<_>>();
        unattempted_diagnostics.sort_by_key(resolution_diagnostic_sort_key);

        if inventory.selected.is_empty() {
            let bounded = bound_diagnostics(
                Vec::new(),
                unattempted_diagnostics,
                resolution_diagnostics,
                already_omitted,
            );
            return outcome_from_parts(
                Vec::new(),
                ProviderCoverage {
                    eligible_units: u32_count(eligible_count),
                    ..ProviderCoverage::default()
                },
                NativeProviderSummary {
                    attempted_calls: 0,
                    successful_calls: 0,
                    failed_calls: 0,
                    safe_candidate_count: 0,
                    omitted_by_budget_or_cap: false,
                },
                bounded,
            );
        }

        let remaining = global_deadline.saturating_duration_since(Instant::now());
        if remaining < self.limits.minimum_start_budget {
            let mut selected_skips = inventory
                .selected
                .iter()
                .map(|route| {
                    skipped_route(
                        &route.route,
                        NativeSearchDiagnosticReason::InsufficientBudget,
                    )
                })
                .collect::<Vec<_>>();
            selected_skips.append(&mut unattempted_diagnostics);
            selected_skips.sort_by_key(resolution_diagnostic_sort_key);
            let bounded = bound_diagnostics(
                Vec::new(),
                selected_skips,
                resolution_diagnostics,
                already_omitted,
            );
            return outcome_from_parts(
                Vec::new(),
                ProviderCoverage {
                    eligible_units: u32_count(eligible_count),
                    budget_exhausted: true,
                    ..ProviderCoverage::default()
                },
                NativeProviderSummary {
                    attempted_calls: 0,
                    successful_calls: 0,
                    failed_calls: 0,
                    safe_candidate_count: 0,
                    omitted_by_budget_or_cap: true,
                },
                bounded,
            );
        }

        let requested_rows = usize::try_from(context.request.limit)
            .unwrap_or(usize::MAX)
            .min(MAX_RESULTS_PER_FUNCTION);
        let mut tasks = Vec::with_capacity(inventory.selected.len());
        for selected in inventory.selected {
            let spawned_at = Instant::now();
            let per_call_deadline = spawned_at
                .checked_add(self.limits.per_call_budget)
                .unwrap_or(global_deadline)
                .min(global_deadline);
            let executor = Arc::clone(&self.executor);
            let workspace_name = context.request.workspace_name.clone();
            let query = context.request.query.clone();
            let route = selected.route.clone();
            let controls = QueryExecutionControls::for_fanout(
                per_call_deadline,
                wave_cancellation.child_token(),
            );
            let command = ExecuteSelectedTableFunction {
                workspace_name: workspace_name.clone(),
                route: route.clone(),
                query,
                controls: controls.clone(),
                row_limit: requested_rows,
                search_origin: context.search_origin,
            };
            let cleanup_grace = self.limits.cleanup_grace;
            let minimum_start_budget = self.limits.minimum_start_budget;
            let order = selected.order;
            let task_controls = controls.clone();
            let deadline_state = CallDeadlineState::new(if per_call_deadline == global_deadline {
                TimeoutScope::Global
            } else {
                TimeoutScope::Call
            });
            let task_deadline_state = deadline_state.clone();
            let task = tokio::spawn(async move {
                run_selected_call(
                    executor,
                    command,
                    workspace_name,
                    route,
                    order,
                    task_controls,
                    per_call_deadline,
                    global_deadline,
                    minimum_start_budget,
                    cleanup_grace,
                    task_deadline_state,
                )
                .await
            });
            tasks.push(CallTask {
                selected,
                controls,
                spawned_at,
                deadline_state,
                task,
            });
        }

        let mut attempts = collect_attempts(
            tasks,
            &wave_cancellation,
            global_deadline,
            self.limits.cleanup_grace,
        )
        .await;
        attempts.sort_by_key(|attempt| attempt.selected.order);

        let mut attempted_diagnostics = Vec::with_capacity(attempts.len());
        let mut skipped_diagnostics = Vec::new();
        let mut ordered_candidates = Vec::new();
        let mut successful_calls = 0_usize;
        let mut failed_calls = 0_usize;
        let mut searched_calls = 0_usize;
        let mut returned_count = 0_usize;
        let mut has_more = false;
        let mut timed_out = false;
        let mut time_budget_exhausted = false;
        let mut omitted_for_start_budget = false;
        for attempt in attempts {
            searched_calls = searched_calls.saturating_add(usize::from(attempt.upstream_started));
            match attempt.outcome {
                AttemptOutcome::Success {
                    mut candidates,
                    raw_row_count,
                    continuation,
                } => {
                    successful_calls = successful_calls.saturating_add(1);
                    let safe_candidate_count = candidates.len();
                    returned_count = returned_count.saturating_add(safe_candidate_count);
                    has_more |= continuation;
                    attempted_diagnostics.push(successful_call(
                        &attempt.selected.route,
                        attempt.elapsed,
                        raw_row_count,
                        safe_candidate_count,
                        continuation,
                    ));
                    candidates = deduplicate(candidates);
                    ordered_candidates.extend(candidates.into_iter().map(|candidate| {
                        OrderedNativeCandidate {
                            selected_order: attempt.selected.order,
                            candidate,
                        }
                    }));
                }
                AttemptOutcome::Failure(failure) => {
                    failed_calls = failed_calls.saturating_add(1);
                    timed_out |= attempt.timeout_scope.is_some();
                    time_budget_exhausted |= attempt.timeout_scope.is_some();
                    attempted_diagnostics.push(failed_call(
                        &attempt.selected.route,
                        attempt.elapsed,
                        failure,
                        attempt.timeout_scope.is_some(),
                        attempt.cleanup_settled,
                    ));
                }
                AttemptOutcome::Skipped(reason) => {
                    omitted_for_start_budget = true;
                    skipped_diagnostics.push(skipped_route(&attempt.selected.route, reason));
                }
            }
        }
        skipped_diagnostics.append(&mut unattempted_diagnostics);
        attempted_diagnostics.sort_by_key(resolution_diagnostic_sort_key);
        skipped_diagnostics.sort_by_key(resolution_diagnostic_sort_key);

        ordered_candidates.sort_by(|left, right| {
            (
                left.candidate.rank_input.provider_ordinal(),
                left.selected_order,
                candidate_sort_key(&left.candidate),
            )
                .cmp(&(
                    right.candidate.rank_input.provider_ordinal(),
                    right.selected_order,
                    candidate_sort_key(&right.candidate),
                ))
        });
        let candidates = ordered_candidates
            .into_iter()
            .map(|ordered| ordered.candidate)
            .collect::<Vec<_>>();
        let candidates = cap_request(candidates)
            .into_iter()
            .map(native_search_candidate)
            .collect::<Vec<_>>();
        let omitted_by_cap = eligible_count > MAX_SELECTED_FUNCTIONS;
        let bounded = bound_diagnostics(
            attempted_diagnostics,
            skipped_diagnostics,
            std::mem::take(&mut resolution_diagnostics),
            already_omitted,
        );
        outcome_from_parts(
            candidates,
            ProviderCoverage {
                eligible_units: u32_count(eligible_count),
                searched_units: u32_count(searched_calls),
                failed_units: u32_count(failed_calls),
                returned_count: u32_count(returned_count),
                has_more,
                budget_exhausted: omitted_by_cap
                    || omitted_for_start_budget
                    || time_budget_exhausted,
                timed_out,
                stale_index: false,
            },
            NativeProviderSummary {
                attempted_calls: successful_calls.saturating_add(failed_calls),
                successful_calls,
                failed_calls,
                safe_candidate_count: returned_count,
                omitted_by_budget_or_cap: omitted_by_cap
                    || omitted_for_start_budget
                    || time_budget_exhausted,
            },
            bounded,
        )
    }
}

impl SearchProvider for NativeFanoutProvider {
    fn kind(&self) -> SearchProviderKind {
        SearchProviderKind::NativeFanout
    }

    fn search(&self, context: Arc<SearchExecutionContext>) -> ProviderSearchFuture {
        let provider = self.clone();
        Box::pin(async move { provider.search_native(context).await })
    }
}

#[derive(Debug, Clone)]
struct SelectedRoute {
    order: usize,
    route: ResolvedUniversalSearchRoute,
}

struct RouteInventory {
    selected: Vec<SelectedRoute>,
    unselected: Vec<SelectedRoute>,
    eligible_count: usize,
    resolution_diagnostics: Vec<NativeSearchDiagnostic>,
    already_omitted_diagnostics: usize,
}

impl RouteInventory {
    fn from_reports(reports: &[UniversalSearchResolution]) -> Self {
        let CollectedResolutionReports {
            routes_by_source,
            diagnostics: resolution_diagnostics,
            omitted_diagnostic_count: already_omitted_diagnostics,
        } = collect_resolution_reports(reports);

        let all = routes_by_source
            .into_iter()
            .flat_map(|(source_name, mut routes)| {
                routes.sort_by_key(route_sort_key);
                routes
                    .into_iter()
                    .map(move |route| (source_name.clone(), route))
            })
            .enumerate()
            .map(|(order, (source_name, route))| (source_name, SelectedRoute { order, route }))
            .collect::<Vec<_>>();
        let eligible_count = all.len();
        let mut selected_indices = Vec::new();
        let mut selected_sources = BTreeSet::new();
        for (index, (source_name, _route)) in all.iter().enumerate() {
            if selected_indices.len() == MAX_SELECTED_FUNCTIONS {
                break;
            }
            if selected_sources.insert(source_name.clone()) {
                selected_indices.push(index);
            }
        }
        for index in 0..all.len() {
            if selected_indices.len() == MAX_SELECTED_FUNCTIONS {
                break;
            }
            if !selected_indices.contains(&index) {
                selected_indices.push(index);
            }
        }
        let mut slots = all
            .into_iter()
            .map(|(_source_name, route)| Some(route))
            .collect::<Vec<_>>();
        let selected = selected_indices
            .into_iter()
            .enumerate()
            .map(|(selection_order, index)| {
                let mut selected = slots
                    .get_mut(index)
                    .and_then(Option::take)
                    .expect("selected route index is unique and in bounds");
                selected.order = selection_order;
                selected
            })
            .collect();
        let unselected = slots.into_iter().flatten().collect();
        Self {
            selected,
            unselected,
            eligible_count,
            resolution_diagnostics,
            already_omitted_diagnostics,
        }
    }
}

struct CollectedResolutionReports {
    routes_by_source: BTreeMap<String, Vec<ResolvedUniversalSearchRoute>>,
    diagnostics: Vec<NativeSearchDiagnostic>,
    omitted_diagnostic_count: usize,
}

fn collect_resolution_reports(reports: &[UniversalSearchResolution]) -> CollectedResolutionReports {
    let mut reports = reports.to_vec();
    reports.sort_by(|left, right| left.source_name.cmp(&right.source_name));
    let mut routes_by_source = BTreeMap::<String, Vec<ResolvedUniversalSearchRoute>>::new();
    let mut diagnostics = Vec::new();
    let mut omitted_diagnostic_count = 0_usize;
    for mut report in reports {
        report.eligible_routes.sort_by_key(route_sort_key);
        routes_by_source
            .entry(report.source_name.clone())
            .or_default()
            .extend(report.eligible_routes);
        report.explicit_denials.sort_by_key(|denial| {
            (
                denial.authored_route_id.clone(),
                denial
                    .locator
                    .as_ref()
                    .map(|locator| locator.schema_name.clone())
                    .unwrap_or_default(),
                denial
                    .locator
                    .as_ref()
                    .map(|locator| locator.function_name.clone())
                    .unwrap_or_default(),
            )
        });
        diagnostics.extend(report.explicit_denials.iter().map(explicit_denial));
        report.diagnostics.sort_by_key(|diagnostic| {
            (
                diagnostic.authored_route_id.clone().unwrap_or_default(),
                diagnostic
                    .locator
                    .as_ref()
                    .map(|locator| locator.schema_name.clone())
                    .unwrap_or_default(),
                diagnostic
                    .locator
                    .as_ref()
                    .map(|locator| locator.function_name.clone())
                    .unwrap_or_default(),
                resolution_reason_order(diagnostic.reason),
            )
        });
        diagnostics.extend(report.diagnostics.iter().map(resolution_failure));
        omitted_diagnostic_count =
            omitted_diagnostic_count.saturating_add(report.omitted_diagnostic_count);
    }
    diagnostics.sort_by_key(resolution_diagnostic_sort_key);
    CollectedResolutionReports {
        routes_by_source,
        diagnostics,
        omitted_diagnostic_count,
    }
}

fn resolution_diagnostic_sort_key(
    diagnostic: &NativeSearchDiagnostic,
) -> (String, u8, String, String, u8) {
    (
        diagnostic.source_name.clone(),
        u8::from(diagnostic.authored_route_id.is_none()),
        diagnostic.authored_route_id.clone().unwrap_or_default(),
        diagnostic.function_name.clone().unwrap_or_default(),
        native_reason_order(diagnostic.reason),
    )
}

fn native_reason_order(reason: NativeSearchDiagnosticReason) -> u8 {
    match reason {
        NativeSearchDiagnosticReason::Unspecified => 0,
        NativeSearchDiagnosticReason::NotAuthorized => 1,
        NativeSearchDiagnosticReason::AmbiguousRoute => 2,
        NativeSearchDiagnosticReason::InvalidSearchLimits => 3,
        NativeSearchDiagnosticReason::QueryInputUnmappable => 4,
        NativeSearchDiagnosticReason::MissingArgumentDefault => 5,
        NativeSearchDiagnosticReason::RouteStale => 6,
        NativeSearchDiagnosticReason::UnsafeOperation => 7,
        NativeSearchDiagnosticReason::NoSafeDisplayFields => 8,
        NativeSearchDiagnosticReason::FanoutLimitReached => 9,
        NativeSearchDiagnosticReason::InsufficientBudget => 10,
        NativeSearchDiagnosticReason::GlobalBudgetExhausted => 11,
        NativeSearchDiagnosticReason::CallTimeout => 12,
        NativeSearchDiagnosticReason::Cancelled => 13,
        NativeSearchDiagnosticReason::RateLimited => 14,
        NativeSearchDiagnosticReason::AuthOrPermissionFailed => 15,
        NativeSearchDiagnosticReason::UpstreamUnavailable => 16,
        NativeSearchDiagnosticReason::InvalidResponse => 17,
        NativeSearchDiagnosticReason::ExecutionFailed => 18,
        NativeSearchDiagnosticReason::UnsupportedCancellation => 19,
        NativeSearchDiagnosticReason::InternalError => 20,
    }
}

fn route_sort_key(route: &ResolvedUniversalSearchRoute) -> (u8, String, String, String, String) {
    let origin = match route.origin {
        UniversalSearchResolutionOrigin::Explicit => 0,
        UniversalSearchResolutionOrigin::Inferred => 1,
    };
    (
        origin,
        route.authored_route_id.clone().unwrap_or_default(),
        String::new(),
        route.target.operation_id.clone(),
        format!(
            "{}.{}",
            route.locator.schema_name, route.locator.function_name
        ),
    )
}

fn resolution_reason_order(
    reason: crate::sources::universal_search::UniversalSearchResolutionReason,
) -> u8 {
    use crate::sources::universal_search::UniversalSearchResolutionReason;

    match reason {
        UniversalSearchResolutionReason::AmbiguousRoute => 0,
        UniversalSearchResolutionReason::InvalidSearchLimits => 1,
        UniversalSearchResolutionReason::MissingArgumentDefault => 2,
        UniversalSearchResolutionReason::QueryInputUnmappable => 3,
        UniversalSearchResolutionReason::RouteStale => 4,
        UniversalSearchResolutionReason::UnsafeOperation => 5,
    }
}

struct OrderedNativeCandidate {
    selected_order: usize,
    candidate: NativeCandidate,
}

struct CallTask {
    selected: SelectedRoute,
    controls: QueryExecutionControls,
    spawned_at: Instant,
    deadline_state: CallDeadlineState,
    task: task::JoinHandle<AttemptResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeoutScope {
    Call,
    Global,
}

#[derive(Clone)]
struct CallDeadlineState {
    fired: Arc<AtomicBool>,
    scope: TimeoutScope,
}

impl CallDeadlineState {
    fn new(scope: TimeoutScope) -> Self {
        Self {
            fired: Arc::new(AtomicBool::new(false)),
            scope,
        }
    }

    fn mark_fired(&self) {
        self.fired.store(true, Ordering::SeqCst);
    }

    fn fired_scope(&self) -> Option<TimeoutScope> {
        self.fired.load(Ordering::SeqCst).then_some(self.scope)
    }
}

struct AttemptResult {
    selected: SelectedRoute,
    elapsed: Duration,
    upstream_started: bool,
    timeout_scope: Option<TimeoutScope>,
    cleanup_settled: bool,
    outcome: AttemptOutcome,
}

enum AttemptOutcome {
    Success {
        candidates: Vec<NativeCandidate>,
        raw_row_count: usize,
        continuation: bool,
    },
    Failure(NativeCallFailure),
    Skipped(NativeSearchDiagnosticReason),
}

impl AttemptResult {
    fn failure(selected: SelectedRoute, elapsed: Duration, failure: NativeCallFailure) -> Self {
        Self {
            selected,
            elapsed,
            upstream_started: false,
            timeout_scope: None,
            cleanup_settled: true,
            outcome: AttemptOutcome::Failure(failure),
        }
    }

    fn skipped(selected: SelectedRoute, reason: NativeSearchDiagnosticReason) -> Self {
        Self {
            selected,
            elapsed: Duration::ZERO,
            upstream_started: false,
            timeout_scope: None,
            cleanup_settled: true,
            outcome: AttemptOutcome::Skipped(reason),
        }
    }

    fn with_timeout(mut self, scope: TimeoutScope, cleanup_settled: bool) -> Self {
        self.timeout_scope = Some(scope);
        self.cleanup_settled = cleanup_settled;
        self
    }
}

async fn collect_attempts(
    mut tasks: Vec<CallTask>,
    wave_cancellation: &QueryCancellationToken,
    global_deadline: Instant,
    cleanup_grace: Duration,
) -> Vec<AttemptResult> {
    let mut attempts = Vec::with_capacity(tasks.len());
    let global_cutoff = tokio::time::sleep_until(global_deadline);
    tokio::pin!(global_cutoff);
    while !tasks.is_empty() {
        let mut call = tasks.remove(0);
        let joined = tokio::select! {
            biased;
            () = &mut global_cutoff => None,
            joined = &mut call.task => Some(joined),
        };
        let Some(joined) = joined else {
            let mut pending = vec![call];
            pending.append(&mut tasks);
            let mut unfinished = Vec::with_capacity(pending.len());
            for mut pending_call in pending {
                if pending_call.task.is_finished() {
                    let joined = (&mut pending_call.task).await;
                    attempts.push(attempt_from_join(pending_call, joined));
                } else {
                    unfinished.push(pending_call);
                }
            }
            wave_cancellation.cancel();
            collect_after_global_cutoff(&mut attempts, unfinished, global_deadline, cleanup_grace)
                .await;
            return attempts;
        };
        attempts.push(attempt_from_join(call, joined));
    }
    attempts
}

async fn collect_after_global_cutoff(
    attempts: &mut Vec<AttemptResult>,
    pending: Vec<CallTask>,
    global_deadline: Instant,
    cleanup_grace: Duration,
) {
    let cleanup_deadline = global_deadline
        .checked_add(cleanup_grace)
        .unwrap_or(global_deadline);
    for mut call in pending {
        match tokio::time::timeout_at(cleanup_deadline, &mut call.task).await {
            Ok(joined) => attempts.push(force_global_timeout(attempt_from_join(call, joined))),
            Err(_elapsed) => {
                call.task.abort();
                let upstream_started = call.controls.upstream_started();
                let mut attempt = AttemptResult::failure(
                    call.selected,
                    call.spawned_at.elapsed(),
                    NativeCallFailure::UnsupportedCancellation,
                )
                .with_timeout(TimeoutScope::Global, false);
                attempt.upstream_started = upstream_started;
                attempts.push(attempt);
            }
        }
    }
}

fn force_global_timeout(mut attempt: AttemptResult) -> AttemptResult {
    attempt.timeout_scope = Some(TimeoutScope::Global);
    if attempt.cleanup_settled {
        attempt.outcome = AttemptOutcome::Failure(NativeCallFailure::GlobalBudgetExhausted);
    } else {
        attempt.outcome = AttemptOutcome::Failure(NativeCallFailure::UnsupportedCancellation);
    }
    attempt
}

fn attempt_from_join(
    call: CallTask,
    joined: Result<AttemptResult, task::JoinError>,
) -> AttemptResult {
    let upstream_started = call.controls.upstream_started();
    let mut attempt = match joined {
        Ok(attempt) => attempt,
        Err(error) => {
            tracing::error!(
                source = %call.selected.route.source_name,
                route_id = call.selected.route.authored_route_id.as_deref().unwrap_or(""),
                cancelled = error.is_cancelled(),
                panicked = error.is_panic(),
                "native fanout call task failed"
            );
            if let Some(scope) = call.deadline_state.fired_scope() {
                let failure = match scope {
                    TimeoutScope::Call => NativeCallFailure::CallTimeout,
                    TimeoutScope::Global => NativeCallFailure::GlobalBudgetExhausted,
                };
                AttemptResult::failure(call.selected, call.spawned_at.elapsed(), failure)
                    .with_timeout(scope, true)
            } else {
                AttemptResult::failure(
                    call.selected,
                    call.spawned_at.elapsed(),
                    NativeCallFailure::Internal,
                )
            }
        }
    };
    attempt.upstream_started = upstream_started;
    attempt
}

#[expect(
    clippy::too_many_arguments,
    reason = "one task receives the complete immutable execution and stop contract"
)]
async fn run_selected_call(
    executor: Arc<dyn SelectedFunctionExecutor>,
    command: ExecuteSelectedTableFunction,
    workspace_name: WorkspaceName,
    route: ResolvedUniversalSearchRoute,
    selected_order: usize,
    controls: QueryExecutionControls,
    deadline: Instant,
    global_deadline: Instant,
    minimum_start_budget: Duration,
    cleanup_grace: Duration,
    deadline_state: CallDeadlineState,
) -> AttemptResult {
    let started_at = Instant::now();
    let selected = SelectedRoute {
        order: selected_order,
        route: route.clone(),
    };
    if global_deadline.saturating_duration_since(Instant::now()) < minimum_start_budget {
        return AttemptResult::skipped(selected, NativeSearchDiagnosticReason::InsufficientBudget);
    }
    let mut execution = executor.execute(command);
    let deadline_elapsed = tokio::time::sleep_until(deadline);
    tokio::pin!(deadline_elapsed);
    let result = tokio::select! {
        biased;
        () = &mut deadline_elapsed => None,
        result = &mut execution => Some(result),
    };
    if let Some(result) = result {
        return finish_selected_call(
            selected,
            &workspace_name,
            started_at,
            result,
            deadline,
            global_deadline,
        );
    }

    deadline_state.mark_fired();
    controls.cancellation().cancel();
    let timeout_scope = deadline_state.scope;
    let cleanup_deadline = deadline.checked_add(cleanup_grace).unwrap_or(deadline);
    let cleanup_settled = tokio::time::timeout_at(cleanup_deadline, &mut execution)
        .await
        .is_ok();
    let failure = if cleanup_settled {
        match timeout_scope {
            TimeoutScope::Call => NativeCallFailure::CallTimeout,
            TimeoutScope::Global => NativeCallFailure::GlobalBudgetExhausted,
        }
    } else {
        NativeCallFailure::UnsupportedCancellation
    };
    AttemptResult::failure(selected, started_at.elapsed(), failure)
        .with_timeout(timeout_scope, cleanup_settled)
}

fn finish_selected_call(
    selected: SelectedRoute,
    workspace_name: &WorkspaceName,
    started_at: Instant,
    result: Result<SelectedTableFunctionExecution, SelectedTableFunctionExecutionError>,
    deadline: Instant,
    global_deadline: Instant,
) -> AttemptResult {
    match result {
        Ok(execution) => {
            let raw_row_count = execution.execution.row_count();
            let candidates = normalize_batches(
                workspace_name,
                &selected.route,
                execution.execution.batches(),
            );
            let elapsed = started_at.elapsed();
            // Controlled HTTP/MCP execution caps decoded responses at 64 KiB
            // and this path reads at most five rows, so normalization work is
            // bounded. Still reject the batch when that bounded work crosses
            // the absolute call/global deadline.
            if Instant::now() >= deadline {
                let timeout_scope = if deadline == global_deadline {
                    TimeoutScope::Global
                } else {
                    TimeoutScope::Call
                };
                let failure = match timeout_scope {
                    TimeoutScope::Call => NativeCallFailure::CallTimeout,
                    TimeoutScope::Global => NativeCallFailure::GlobalBudgetExhausted,
                };
                return AttemptResult::failure(selected, elapsed, failure)
                    .with_timeout(timeout_scope, true);
            }
            AttemptResult {
                selected,
                elapsed,
                upstream_started: false,
                timeout_scope: None,
                cleanup_settled: true,
                outcome: AttemptOutcome::Success {
                    candidates,
                    raw_row_count,
                    continuation: execution.has_more,
                },
            }
        }
        Err(error) => {
            let elapsed = started_at.elapsed();
            let timeout_scope = matches!(
                error.kind,
                crate::query::SelectedTableFunctionFailureKind::Execution(
                    QueryExecutionFailureKind::Timeout
                )
            )
            .then(|| {
                if Instant::now() >= deadline && deadline == global_deadline {
                    TimeoutScope::Global
                } else {
                    TimeoutScope::Call
                }
            });
            let attempt =
                AttemptResult::failure(selected, elapsed, NativeCallFailure::Selected(error.kind));
            if let Some(scope) = timeout_scope {
                attempt.with_timeout(scope, true)
            } else {
                attempt
            }
        }
    }
}

fn candidate_sort_key(candidate: &NativeCandidate) -> String {
    hex_key(candidate.sort_key.as_bytes())
}

fn native_search_candidate(candidate: NativeCandidate) -> SearchCandidate {
    SearchCandidate {
        key: candidate_sort_key(&candidate),
        score: 0,
        provider: SearchProviderKind::NativeFanout,
        payload: SearchPayload::NativeResult(candidate.result),
    }
}

fn hex_key(bytes: [u8; 32]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(hex_digit(byte >> 4));
        output.push(hex_digit(byte & 0x0f));
    }
    output
}

fn hex_digit(nibble: u8) -> char {
    char::from(if nibble < 10 {
        b'0' + nibble
    } else {
        b'a' + (nibble - 10)
    })
}

fn outcome_from_parts(
    candidates: Vec<SearchCandidate>,
    coverage: ProviderCoverage,
    summary: NativeProviderSummary,
    diagnostics: super::diagnostics::BoundedDiagnostics,
) -> ProviderSearchOutcome {
    let state = provider_state(summary);
    ProviderSearchOutcome {
        candidates,
        status: ProviderStatus {
            provider: SearchProviderKind::NativeFanout,
            state,
            note: provider_note(state),
            coverage: Some(coverage),
            diagnostics: diagnostics.diagnostics,
            diagnostics_truncated: diagnostics.truncated,
            omitted_diagnostic_count: diagnostics.omitted_count,
        },
    }
}

fn empty_outcome(
    eligible_count: usize,
    budget_exhausted: bool,
    diagnostics: Vec<NativeSearchDiagnostic>,
    note: &str,
) -> ProviderSearchOutcome {
    ProviderSearchOutcome {
        candidates: Vec::new(),
        status: ProviderStatus {
            provider: SearchProviderKind::NativeFanout,
            state: crate::search::result::SearchProviderState::Skipped,
            note: note.to_string(),
            coverage: Some(ProviderCoverage {
                eligible_units: u32_count(eligible_count),
                budget_exhausted,
                ..ProviderCoverage::default()
            }),
            diagnostics,
            diagnostics_truncated: false,
            omitted_diagnostic_count: 0,
        },
    }
}

fn provider_failure_outcome() -> ProviderSearchOutcome {
    ProviderSearchOutcome {
        candidates: Vec::new(),
        status: ProviderStatus {
            provider: SearchProviderKind::NativeFanout,
            state: crate::search::result::SearchProviderState::Error,
            note: "native fanout route setup failed without affecting local search".to_string(),
            coverage: Some(ProviderCoverage::default()),
            diagnostics: Vec::new(),
            diagnostics_truncated: false,
            omitted_diagnostic_count: 0,
        },
    }
}

fn provider_note(state: crate::search::result::SearchProviderState) -> String {
    match state {
        crate::search::result::SearchProviderState::ResultsFound => {
            "provider-native fanout returned results".to_string()
        }
        crate::search::result::SearchProviderState::Empty => {
            "provider-native fanout completed without safe display results".to_string()
        }
        crate::search::result::SearchProviderState::Skipped => {
            "provider-native fanout had no executable work".to_string()
        }
        crate::search::result::SearchProviderState::Partial => {
            "provider-native fanout returned partial results".to_string()
        }
        crate::search::result::SearchProviderState::Error => {
            "provider-native fanout failed without affecting local search".to_string()
        }
        crate::search::result::SearchProviderState::NotEnabled => String::new(),
    }
}

fn u32_count(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests;
