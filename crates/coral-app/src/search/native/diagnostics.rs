//! Stable, bounded diagnostics and aggregate state for native fanout.

use std::time::Duration;

use coral_engine::QueryExecutionFailureKind;

use crate::query::SelectedTableFunctionFailureKind;
use crate::search::result::{
    NativeSearchDiagnostic, NativeSearchDiagnosticReason, NativeSearchDiagnosticState,
    SearchProviderState,
};
use crate::sources::universal_search::{
    ResolvedUniversalSearchDenial, ResolvedUniversalSearchRoute,
    UniversalSearchResolutionDiagnostic, UniversalSearchResolutionReason,
};

const MAX_NATIVE_DIAGNOSTICS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NativeCallFailure {
    Selected(SelectedTableFunctionFailureKind),
    GlobalBudgetExhausted,
    CallTimeout,
    UnsupportedCancellation,
    Internal,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct NativeProviderSummary {
    pub(super) attempted_calls: usize,
    pub(super) successful_calls: usize,
    pub(super) failed_calls: usize,
    pub(super) safe_candidate_count: usize,
    pub(super) omitted_by_budget_or_cap: bool,
}

#[derive(Debug)]
pub(super) struct BoundedDiagnostics {
    pub(super) diagnostics: Vec<NativeSearchDiagnostic>,
    pub(super) truncated: bool,
    pub(super) omitted_count: u32,
}

pub(super) fn provider_state(summary: NativeProviderSummary) -> SearchProviderState {
    if summary.attempted_calls == 0 {
        return SearchProviderState::Skipped;
    }
    if summary.successful_calls == 0 {
        return SearchProviderState::Error;
    }
    if summary.failed_calls > 0 || summary.omitted_by_budget_or_cap {
        return SearchProviderState::Partial;
    }
    if summary.safe_candidate_count == 0 {
        SearchProviderState::Empty
    } else {
        SearchProviderState::ResultsFound
    }
}

pub(super) fn successful_call(
    route: &ResolvedUniversalSearchRoute,
    elapsed: Duration,
    raw_row_count: usize,
    safe_candidate_count: usize,
    has_more: bool,
) -> NativeSearchDiagnostic {
    let (state, reason) = if safe_candidate_count > 0 {
        (
            NativeSearchDiagnosticState::ResultsFound,
            NativeSearchDiagnosticReason::Unspecified,
        )
    } else if raw_row_count > 0 {
        (
            NativeSearchDiagnosticState::Empty,
            NativeSearchDiagnosticReason::NoSafeDisplayFields,
        )
    } else {
        (
            NativeSearchDiagnosticState::Empty,
            NativeSearchDiagnosticReason::Unspecified,
        )
    };
    route_diagnostic(
        route,
        state,
        reason,
        elapsed,
        safe_candidate_count,
        has_more,
    )
}

pub(super) fn failed_call(
    route: &ResolvedUniversalSearchRoute,
    elapsed: Duration,
    failure: NativeCallFailure,
    timed_out: bool,
    cleanup_settled: bool,
) -> NativeSearchDiagnostic {
    if !cleanup_settled {
        return route_diagnostic(
            route,
            if timed_out {
                NativeSearchDiagnosticState::TimedOut
            } else {
                NativeSearchDiagnosticState::Error
            },
            NativeSearchDiagnosticReason::UnsupportedCancellation,
            elapsed,
            0,
            false,
        );
    }
    let (state, reason) = match failure {
        NativeCallFailure::Selected(SelectedTableFunctionFailureKind::RouteStale) => (
            NativeSearchDiagnosticState::Error,
            NativeSearchDiagnosticReason::RouteStale,
        ),
        NativeCallFailure::Selected(SelectedTableFunctionFailureKind::Execution(kind)) => {
            execution_failure(kind)
        }
        NativeCallFailure::GlobalBudgetExhausted => (
            NativeSearchDiagnosticState::TimedOut,
            NativeSearchDiagnosticReason::GlobalBudgetExhausted,
        ),
        NativeCallFailure::CallTimeout => (
            NativeSearchDiagnosticState::TimedOut,
            NativeSearchDiagnosticReason::CallTimeout,
        ),
        NativeCallFailure::UnsupportedCancellation => (
            NativeSearchDiagnosticState::Error,
            NativeSearchDiagnosticReason::UnsupportedCancellation,
        ),
        NativeCallFailure::Internal => (
            NativeSearchDiagnosticState::Error,
            NativeSearchDiagnosticReason::InternalError,
        ),
    };
    route_diagnostic(route, state, reason, elapsed, 0, false)
}

pub(super) fn skipped_route(
    route: &ResolvedUniversalSearchRoute,
    reason: NativeSearchDiagnosticReason,
) -> NativeSearchDiagnostic {
    route_diagnostic(
        route,
        NativeSearchDiagnosticState::Skipped,
        reason,
        Duration::ZERO,
        0,
        false,
    )
}

pub(super) fn explicit_denial(denial: &ResolvedUniversalSearchDenial) -> NativeSearchDiagnostic {
    NativeSearchDiagnostic {
        installed_source_name: denial.owner_source_name.clone(),
        schema_name: denial
            .locator
            .as_ref()
            .map(|locator| locator.schema_name.clone()),
        function_name: denial
            .locator
            .as_ref()
            .map(|locator| locator.function_name.clone()),
        authored_route_id: Some(denial.authored_route_id.clone()),
        state: NativeSearchDiagnosticState::Skipped,
        reason: NativeSearchDiagnosticReason::NotAuthorized,
        elapsed_ms: 0,
        safe_candidate_count: 0,
        has_more: false,
    }
}

pub(super) fn resolution_failure(
    diagnostic: &UniversalSearchResolutionDiagnostic,
) -> NativeSearchDiagnostic {
    let locator = diagnostic.locator.as_ref();
    NativeSearchDiagnostic {
        installed_source_name: diagnostic.owner_source_name.clone(),
        schema_name: locator.map(|locator| locator.schema_name.clone()),
        function_name: locator.map(|locator| locator.function_name.clone()),
        authored_route_id: diagnostic.authored_route_id.clone(),
        state: NativeSearchDiagnosticState::Skipped,
        reason: match diagnostic.reason {
            UniversalSearchResolutionReason::AmbiguousRoute => {
                NativeSearchDiagnosticReason::AmbiguousRoute
            }
            UniversalSearchResolutionReason::InvalidSearchLimits => {
                NativeSearchDiagnosticReason::InvalidSearchLimits
            }
            UniversalSearchResolutionReason::MissingArgumentDefault => {
                NativeSearchDiagnosticReason::MissingArgumentDefault
            }
            UniversalSearchResolutionReason::QueryInputUnmappable => {
                NativeSearchDiagnosticReason::QueryInputUnmappable
            }
            UniversalSearchResolutionReason::RouteStale => NativeSearchDiagnosticReason::RouteStale,
            UniversalSearchResolutionReason::UnsafeOperation => {
                NativeSearchDiagnosticReason::UnsafeOperation
            }
        },
        elapsed_ms: 0,
        safe_candidate_count: 0,
        has_more: false,
    }
}

pub(super) fn bound_diagnostics(
    attempted: Vec<NativeSearchDiagnostic>,
    unattempted: Vec<NativeSearchDiagnostic>,
    resolution: Vec<NativeSearchDiagnostic>,
    already_omitted: usize,
) -> BoundedDiagnostics {
    let mut diagnostics = Vec::with_capacity(MAX_NATIVE_DIAGNOSTICS);
    let mut locally_omitted = 0_usize;
    for diagnostic in attempted.into_iter().chain(unattempted).chain(resolution) {
        if diagnostics.len() < MAX_NATIVE_DIAGNOSTICS {
            diagnostics.push(diagnostic);
        } else {
            locally_omitted = locally_omitted.saturating_add(1);
        }
    }
    let omitted = already_omitted.saturating_add(locally_omitted);
    BoundedDiagnostics {
        diagnostics,
        truncated: omitted > 0,
        omitted_count: u32::try_from(omitted).unwrap_or(u32::MAX),
    }
}

fn route_diagnostic(
    route: &ResolvedUniversalSearchRoute,
    state: NativeSearchDiagnosticState,
    reason: NativeSearchDiagnosticReason,
    elapsed: Duration,
    safe_candidate_count: usize,
    has_more: bool,
) -> NativeSearchDiagnostic {
    NativeSearchDiagnostic {
        installed_source_name: route.owner_source_name.clone(),
        schema_name: Some(route.locator.schema_name.clone()),
        function_name: Some(route.locator.function_name.clone()),
        authored_route_id: route.authored_route_id.clone(),
        state,
        reason,
        elapsed_ms: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        safe_candidate_count: u32::try_from(safe_candidate_count).unwrap_or(u32::MAX),
        has_more,
    }
}

fn execution_failure(
    failure: QueryExecutionFailureKind,
) -> (NativeSearchDiagnosticState, NativeSearchDiagnosticReason) {
    match failure {
        QueryExecutionFailureKind::Timeout => (
            NativeSearchDiagnosticState::TimedOut,
            NativeSearchDiagnosticReason::CallTimeout,
        ),
        QueryExecutionFailureKind::Cancelled => (
            NativeSearchDiagnosticState::Cancelled,
            NativeSearchDiagnosticReason::Cancelled,
        ),
        QueryExecutionFailureKind::RateLimited => (
            NativeSearchDiagnosticState::Error,
            NativeSearchDiagnosticReason::RateLimited,
        ),
        QueryExecutionFailureKind::Authentication | QueryExecutionFailureKind::PermissionDenied => {
            (
                NativeSearchDiagnosticState::Error,
                NativeSearchDiagnosticReason::AuthOrPermissionFailed,
            )
        }
        QueryExecutionFailureKind::UpstreamUnavailable => (
            NativeSearchDiagnosticState::Error,
            NativeSearchDiagnosticReason::UpstreamUnavailable,
        ),
        QueryExecutionFailureKind::InvalidResponse => (
            NativeSearchDiagnosticState::Error,
            NativeSearchDiagnosticReason::InvalidResponse,
        ),
        _ => (
            NativeSearchDiagnosticState::Error,
            NativeSearchDiagnosticReason::ExecutionFailed,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{NativeProviderSummary, provider_state};
    use crate::search::result::SearchProviderState;

    #[test]
    fn aggregate_state_table_is_deterministic() {
        let state = |attempted, succeeded, failed, returned, omitted| {
            provider_state(NativeProviderSummary {
                attempted_calls: attempted,
                successful_calls: succeeded,
                failed_calls: failed,
                safe_candidate_count: returned,
                omitted_by_budget_or_cap: omitted,
            })
        };

        assert_eq!(state(0, 0, 0, 0, false), SearchProviderState::Skipped);
        assert_eq!(state(1, 1, 0, 0, false), SearchProviderState::Empty);
        assert_eq!(state(1, 1, 0, 1, false), SearchProviderState::ResultsFound);
        assert_eq!(state(2, 1, 1, 1, false), SearchProviderState::Partial);
        assert_eq!(state(4, 4, 0, 4, true), SearchProviderState::Partial);
        assert_eq!(state(2, 0, 2, 0, false), SearchProviderState::Error);
    }
}
