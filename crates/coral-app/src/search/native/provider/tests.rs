//! Focused bounded-fanout orchestration tests.

#![expect(
    clippy::indexing_slicing,
    reason = "focused provider fixtures assert exact bounded diagnostic and result shapes"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use coral_engine::{
    CatalogInfo, QueryExecution, QueryExecutionFailureKind, QueryExecutionProvenance,
};
use coral_spec::{ManifestDataType, SearchLimitsSpec};
use tokio::sync::Barrier;
use tokio::time::Instant;
use uuid::Uuid;

use super::super::diagnostics::{NativeCallFailure, failed_call};
use super::super::normalize::normalize_batches;
use super::{
    AttemptOutcome, AttemptResult, CallDeadlineState, CallTask, NativeFanoutLimits,
    NativeFanoutProvider, RouteInventory, SelectedFunctionExecutor, SelectedFunctionFuture,
    SelectedRoute, TimeoutScope, candidate_sort_key, collect_attempts,
};
use crate::bootstrap::AppError;
use crate::catalog::model::CatalogResolution;
use crate::query::manager::QueryManagerError;
use crate::query::{
    ExecuteSelectedTableFunction, SelectedTableFunctionExecution,
    SelectedTableFunctionExecutionError, SelectedTableFunctionFailureKind,
};
use crate::search::provider::{ObservedValuesPolicyInput, SearchExecutionContext};
use crate::search::result::{
    NativeSearchDiagnosticReason, NativeSearchDiagnosticState, SearchPayload, SearchProviderState,
    SearchRequest,
};
use crate::sources::runtime_package::RuntimeContractFingerprint;
use crate::sources::universal_search::{
    ResolvedUniversalSearchArgument, ResolvedUniversalSearchResultMapping,
    ResolvedUniversalSearchRoute, ResolvedUniversalSearchTarget, UniversalSearchFunctionLocator,
    UniversalSearchResolution, UniversalSearchResolutionDiagnostic,
    UniversalSearchResolutionOrigin, UniversalSearchResolutionReason,
};
use crate::workspaces::{WorkspaceLifecycleLock, WorkspaceName};

#[derive(Clone)]
enum Behaviour {
    Immediate {
        batches: Vec<RecordBatch>,
        has_more: bool,
    },
    Barrier(Arc<Barrier>),
    WaitForCancellation,
    NeverSettles,
    EarlyTimeout,
    PanicAfter(Duration),
    PanicAfterCancellation(Duration),
}

#[derive(Clone)]
struct ScriptedExecutor {
    behaviours: Arc<BTreeMap<String, Behaviour>>,
    starts: Arc<Mutex<Vec<String>>>,
}

impl ScriptedExecutor {
    fn same(behaviour: Behaviour) -> Self {
        Self {
            behaviours: Arc::new(BTreeMap::from([("*".to_string(), behaviour)])),
            starts: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn starts(&self) -> Vec<String> {
        self.starts.lock().expect("starts lock").clone()
    }

    fn scripted(behaviours: impl IntoIterator<Item = (String, Behaviour)>) -> Self {
        Self {
            behaviours: Arc::new(behaviours.into_iter().collect()),
            starts: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl SelectedFunctionExecutor for ScriptedExecutor {
    fn execute(&self, command: ExecuteSelectedTableFunction) -> SelectedFunctionFuture {
        let behaviour = self
            .behaviours
            .get(&command.route.locator.function_name)
            .or_else(|| self.behaviours.get("*"))
            .expect("scripted behaviour")
            .clone();
        let starts = Arc::clone(&self.starts);
        Box::pin(async move {
            let controls = command.controls.clone();
            controls.mark_upstream_started();
            starts
                .lock()
                .expect("starts lock")
                .push(command.route.locator.function_name.clone());
            match behaviour {
                Behaviour::Immediate { batches, has_more } => {
                    Ok(successful_execution(batches, has_more, &controls))
                }
                Behaviour::Barrier(barrier) => {
                    barrier.wait().await;
                    Ok(successful_execution(Vec::new(), false, &controls))
                }
                Behaviour::WaitForCancellation => {
                    controls.cancellation().cancelled().await;
                    Err(execution_error(
                        QueryExecutionFailureKind::Cancelled,
                        &controls,
                    ))
                }
                Behaviour::NeverSettles => future::pending().await,
                Behaviour::EarlyTimeout => Err(execution_error(
                    QueryExecutionFailureKind::Timeout,
                    &controls,
                )),
                Behaviour::PanicAfter(delay) => {
                    tokio::time::sleep(delay).await;
                    panic!("sanitised fake executor panic")
                }
                Behaviour::PanicAfterCancellation(delay) => {
                    controls.cancellation().cancelled().await;
                    tokio::time::sleep(delay).await;
                    panic!("sanitised cleanup panic")
                }
            }
        })
    }
}

fn successful_execution(
    batches: Vec<RecordBatch>,
    has_more: bool,
    controls: &coral_engine::QueryExecutionControls,
) -> SelectedTableFunctionExecution {
    let arrow_schema = batches
        .first()
        .map_or_else(|| Arc::new(Schema::empty()), RecordBatch::schema);
    SelectedTableFunctionExecution {
        execution: QueryExecution::new(
            arrow_schema,
            batches,
            QueryExecutionProvenance::new(
                "sensitive SQL sentinel",
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
        ),
        has_more,
        upstream_started: controls.upstream_started(),
    }
}

fn execution_error(
    kind: QueryExecutionFailureKind,
    controls: &coral_engine::QueryExecutionControls,
) -> SelectedTableFunctionExecutionError {
    SelectedTableFunctionExecutionError {
        kind: SelectedTableFunctionFailureKind::Execution(kind),
        upstream_started: controls.upstream_started(),
    }
}

fn provider(executor: ScriptedExecutor) -> NativeFanoutProvider {
    NativeFanoutProvider::for_test(Arc::new(executor), NativeFanoutLimits::default())
}

fn context(
    started_at: Instant,
    limit: u32,
    reports: Vec<UniversalSearchResolution>,
) -> Arc<SearchExecutionContext> {
    context_with_resolution(
        started_at,
        limit,
        Ok(CatalogResolution {
            catalog: CatalogInfo {
                tables: Vec::new(),
                table_functions: Vec::new(),
            },
            failed_source_names: BTreeSet::new(),
            runtime_schema_owners: BTreeMap::new(),
            universal_search_resolutions: reports,
        }),
    )
}

fn context_with_resolution(
    started_at: Instant,
    limit: u32,
    catalog_resolution: Result<CatalogResolution, QueryManagerError>,
) -> Arc<SearchExecutionContext> {
    let workspace = WorkspaceName::default();
    let lifecycle = WorkspaceLifecycleLock::default();
    let lifecycle_lease = lifecycle.snapshot_for_test(&workspace);
    Arc::new(SearchExecutionContext::new(
        started_at,
        lifecycle_lease,
        Some("2026-07-20T00:00:00.000Z".to_string()),
        Uuid::from_u128(99),
        SearchRequest::new(workspace, "payment", limit).expect("request"),
        catalog_resolution,
        ObservedValuesPolicyInput::Disabled,
    ))
}

fn report(source: &str, routes: Vec<ResolvedUniversalSearchRoute>) -> UniversalSearchResolution {
    UniversalSearchResolution {
        owner_source_name: source.to_string(),
        eligible_routes: routes,
        explicit_denials: Vec::new(),
        diagnostics: Vec::new(),
        diagnostics_truncated: false,
        omitted_diagnostic_count: 0,
    }
}

fn resolution_diagnostic(
    source: &str,
    route_id: Option<&str>,
    locator: Option<(&str, &str)>,
    reason: UniversalSearchResolutionReason,
) -> UniversalSearchResolutionDiagnostic {
    UniversalSearchResolutionDiagnostic {
        owner_source_name: source.to_string(),
        authored_route_id: route_id.map(str::to_string),
        locator: locator.map(
            |(schema_name, function_name)| UniversalSearchFunctionLocator {
                schema_name: schema_name.to_string(),
                function_name: function_name.to_string(),
            },
        ),
        reason,
    }
}

fn route(
    source: &str,
    route_name: &str,
    origin: UniversalSearchResolutionOrigin,
) -> ResolvedUniversalSearchRoute {
    ResolvedUniversalSearchRoute {
        owner_source_name: source.to_string(),
        installation_revision: Uuid::from_u128(source.bytes().map(u128::from).sum()),
        authored_route_id: matches!(origin, UniversalSearchResolutionOrigin::Explicit)
            .then(|| route_name.to_string()),
        target: ResolvedUniversalSearchTarget::V3 {
            function_name: route_name.to_string(),
        },
        locator: UniversalSearchFunctionLocator {
            schema_name: format!("{source}_runtime"),
            function_name: route_name.to_string(),
        },
        query_argument: ResolvedUniversalSearchArgument {
            name: "query".to_string(),
            data_type: ManifestDataType::Utf8,
        },
        default_arguments: Vec::new(),
        search_limits: SearchLimitsSpec {
            default_top_k: 5,
            max_top_k: 5,
            max_calls_per_query: 1,
        },
        result: ResolvedUniversalSearchResultMapping::default(),
        origin,
        runtime_contract_fingerprint: RuntimeContractFingerprint::for_test("v1:fanout-test"),
    }
}

fn empty_success() -> Behaviour {
    Behaviour::Immediate {
        batches: Vec::new(),
        has_more: false,
    }
}

fn coverage(
    outcome: &crate::search::provider::ProviderSearchOutcome,
) -> &crate::search::result::ProviderCoverage {
    outcome.status.coverage.as_ref().expect("native coverage")
}

async fn wait_for_starts(executor: &ScriptedExecutor, count: usize) {
    for _ in 0..100 {
        if executor.starts().len() >= count {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!(
        "expected {count} executor start(s), got {:?}",
        executor.starts()
    );
}

#[test]
fn route_selection_is_deterministic_explicit_first_and_one_per_source() {
    let reports = vec![
        report(
            "zeta",
            vec![
                route(
                    "zeta",
                    "inferred",
                    UniversalSearchResolutionOrigin::Inferred,
                ),
                route(
                    "zeta",
                    "explicit",
                    UniversalSearchResolutionOrigin::Explicit,
                ),
            ],
        ),
        report(
            "alpha",
            vec![route(
                "alpha",
                "a",
                UniversalSearchResolutionOrigin::Explicit,
            )],
        ),
        report(
            "delta",
            vec![route(
                "delta",
                "d",
                UniversalSearchResolutionOrigin::Explicit,
            )],
        ),
        report(
            "beta",
            vec![route(
                "beta",
                "b",
                UniversalSearchResolutionOrigin::Explicit,
            )],
        ),
        report(
            "charlie",
            vec![route(
                "charlie",
                "c",
                UniversalSearchResolutionOrigin::Explicit,
            )],
        ),
    ];

    let inventory = RouteInventory::from_reports(&reports);
    let selected = inventory
        .selected
        .iter()
        .map(|selected| {
            (
                selected.route.owner_source_name.as_str(),
                selected.route.locator.function_name.as_str(),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(inventory.eligible_count, 6);
    assert_eq!(
        selected,
        [
            ("alpha", "a"),
            ("beta", "b"),
            ("charlie", "c"),
            ("delta", "d")
        ]
    );
    assert_eq!(inventory.unselected.len(), 2);
}

#[test]
fn route_selection_golden_is_a1_b1_a2_a3() {
    let inventory = RouteInventory::from_reports(&[
        report(
            "alpha",
            ["a3", "a1", "a2"]
                .into_iter()
                .map(|name| route("alpha", name, UniversalSearchResolutionOrigin::Explicit))
                .collect(),
        ),
        report(
            "beta",
            vec![route(
                "beta",
                "b1",
                UniversalSearchResolutionOrigin::Explicit,
            )],
        ),
    ]);

    assert_eq!(
        inventory
            .selected
            .iter()
            .map(|selected| selected.route.locator.function_name.as_str())
            .collect::<Vec<_>>(),
        ["a1", "b1", "a2", "a3"]
    );
}

#[tokio::test]
async fn native_results_round_robin_by_row_then_selected_route_before_digest() {
    fn result_batch(prefix: &str, seed: usize) -> RecordBatch {
        let first_id = format!("{prefix}-{seed}-0");
        let second_id = format!("{prefix}-{seed}-1");
        let first_title = format!("{prefix}0");
        let second_title = format!("{prefix}1");
        RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(StringArray::from(vec![
                    first_id.as_str(),
                    second_id.as_str(),
                ])) as ArrayRef,
            ),
            (
                "title",
                Arc::new(StringArray::from(vec![
                    first_title.as_str(),
                    second_title.as_str(),
                ])) as ArrayRef,
            ),
        ])
        .expect("result batch")
    }

    let route_a = route("alpha", "a", UniversalSearchResolutionOrigin::Explicit);
    let route_b = route("beta", "b", UniversalSearchResolutionOrigin::Explicit);
    let workspace = WorkspaceName::default();
    let (batch_a, batch_b) = (0..1_024)
        .find_map(|seed| {
            let batch_a = result_batch("A", seed);
            let batch_b = result_batch("B", seed);
            let a = normalize_batches(&workspace, &route_a, std::slice::from_ref(&batch_a));
            let b = normalize_batches(&workspace, &route_b, std::slice::from_ref(&batch_b));
            (candidate_sort_key(&a[0]) > candidate_sort_key(&b[0])).then_some((batch_a, batch_b))
        })
        .expect("find a digest order opposed to selected route order");
    let executor = ScriptedExecutor::scripted([
        (
            "a".to_string(),
            Behaviour::Immediate {
                batches: vec![batch_a],
                has_more: false,
            },
        ),
        (
            "b".to_string(),
            Behaviour::Immediate {
                batches: vec![batch_b],
                has_more: false,
            },
        ),
    ]);
    let reports = vec![
        report("alpha", vec![route_a]),
        report("beta", vec![route_b]),
    ];
    let outcome = provider(executor)
        .search_native(context(Instant::now(), 10, reports))
        .await;
    let titles = outcome
        .candidates
        .iter()
        .map(|candidate| match &candidate.payload {
            SearchPayload::NativeResult(result) => result.title.as_deref(),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(titles, [Some("A0"), Some("B0"), Some("A1"), Some("B1")]);
    assert!(outcome.candidates[0].key > outcome.candidates[1].key);
}

#[tokio::test]
async fn four_calls_enter_one_wave_and_a_fifth_never_starts() {
    let reports = ["a", "b", "c", "d", "e"]
        .into_iter()
        .map(|source| {
            report(
                source,
                vec![route(
                    source,
                    source,
                    UniversalSearchResolutionOrigin::Explicit,
                )],
            )
        })
        .collect::<Vec<_>>();
    let executor = ScriptedExecutor::same(Behaviour::Barrier(Arc::new(Barrier::new(4))));

    let outcome = tokio::time::timeout(
        Duration::from_secs(1),
        provider(executor.clone()).search_native(context(Instant::now(), 10, reports)),
    )
    .await
    .expect("four-way barrier completes");

    assert_eq!(executor.starts().len(), 4);
    assert!(!executor.starts().iter().any(|name| name == "e"));
    assert_eq!(outcome.status.state, SearchProviderState::Partial);
    assert_eq!(coverage(&outcome).eligible_units, 5);
    assert_eq!(coverage(&outcome).searched_units, 4);
    assert_eq!(coverage(&outcome).failed_units, 0);
    assert!(coverage(&outcome).budget_exhausted);
}

#[tokio::test]
async fn mixed_and_all_failure_states_follow_provider_state_table() {
    let reports = || {
        vec![
            report(
                "alpha",
                vec![route(
                    "alpha",
                    "a",
                    UniversalSearchResolutionOrigin::Explicit,
                )],
            ),
            report(
                "beta",
                vec![route(
                    "beta",
                    "b",
                    UniversalSearchResolutionOrigin::Explicit,
                )],
            ),
        ]
    };
    let mixed = ScriptedExecutor::scripted([
        ("a".to_string(), empty_success()),
        ("b".to_string(), Behaviour::EarlyTimeout),
    ]);
    let mixed = provider(mixed)
        .search_native(context(Instant::now(), 10, reports()))
        .await;
    assert_eq!(mixed.status.state, SearchProviderState::Partial);
    assert_eq!(coverage(&mixed).searched_units, 2);
    assert_eq!(coverage(&mixed).failed_units, 1);

    let failed = ScriptedExecutor::same(Behaviour::EarlyTimeout);
    let failed = provider(failed)
        .search_native(context(Instant::now(), 10, reports()))
        .await;
    assert_eq!(failed.status.state, SearchProviderState::Error);
    assert_eq!(coverage(&failed).searched_units, 2);
    assert_eq!(coverage(&failed).failed_units, 2);
}

#[test]
fn selected_failures_map_to_stable_public_reason_codes() {
    let route = route("alpha", "a", UniversalSearchResolutionOrigin::Explicit);
    let cases = [
        (
            NativeCallFailure::Selected(SelectedTableFunctionFailureKind::RouteStale),
            NativeSearchDiagnosticState::Error,
            NativeSearchDiagnosticReason::RouteStale,
        ),
        (
            NativeCallFailure::Selected(SelectedTableFunctionFailureKind::Execution(
                QueryExecutionFailureKind::Timeout,
            )),
            NativeSearchDiagnosticState::TimedOut,
            NativeSearchDiagnosticReason::CallTimeout,
        ),
        (
            NativeCallFailure::Selected(SelectedTableFunctionFailureKind::Execution(
                QueryExecutionFailureKind::Cancelled,
            )),
            NativeSearchDiagnosticState::Cancelled,
            NativeSearchDiagnosticReason::Cancelled,
        ),
        (
            NativeCallFailure::Selected(SelectedTableFunctionFailureKind::Execution(
                QueryExecutionFailureKind::RateLimited,
            )),
            NativeSearchDiagnosticState::Error,
            NativeSearchDiagnosticReason::RateLimited,
        ),
        (
            NativeCallFailure::Selected(SelectedTableFunctionFailureKind::Execution(
                QueryExecutionFailureKind::Authentication,
            )),
            NativeSearchDiagnosticState::Error,
            NativeSearchDiagnosticReason::AuthOrPermissionFailed,
        ),
        (
            NativeCallFailure::Selected(SelectedTableFunctionFailureKind::Execution(
                QueryExecutionFailureKind::PermissionDenied,
            )),
            NativeSearchDiagnosticState::Error,
            NativeSearchDiagnosticReason::AuthOrPermissionFailed,
        ),
        (
            NativeCallFailure::Selected(SelectedTableFunctionFailureKind::Execution(
                QueryExecutionFailureKind::UpstreamUnavailable,
            )),
            NativeSearchDiagnosticState::Error,
            NativeSearchDiagnosticReason::UpstreamUnavailable,
        ),
        (
            NativeCallFailure::Selected(SelectedTableFunctionFailureKind::Execution(
                QueryExecutionFailureKind::InvalidResponse,
            )),
            NativeSearchDiagnosticState::Error,
            NativeSearchDiagnosticReason::InvalidResponse,
        ),
        (
            NativeCallFailure::Selected(SelectedTableFunctionFailureKind::Execution(
                QueryExecutionFailureKind::Execution,
            )),
            NativeSearchDiagnosticState::Error,
            NativeSearchDiagnosticReason::ExecutionFailed,
        ),
        (
            NativeCallFailure::GlobalBudgetExhausted,
            NativeSearchDiagnosticState::TimedOut,
            NativeSearchDiagnosticReason::GlobalBudgetExhausted,
        ),
        (
            NativeCallFailure::CallTimeout,
            NativeSearchDiagnosticState::TimedOut,
            NativeSearchDiagnosticReason::CallTimeout,
        ),
        (
            NativeCallFailure::UnsupportedCancellation,
            NativeSearchDiagnosticState::Error,
            NativeSearchDiagnosticReason::UnsupportedCancellation,
        ),
        (
            NativeCallFailure::Internal,
            NativeSearchDiagnosticState::Error,
            NativeSearchDiagnosticReason::InternalError,
        ),
    ];

    for (failure, state, reason) in cases {
        let diagnostic = failed_call(&route, Duration::from_millis(7), failure, false, true);
        assert_eq!(diagnostic.state, state, "failure {failure:?}");
        assert_eq!(diagnostic.reason, reason, "failure {failure:?}");
        assert_eq!(diagnostic.elapsed_ms, 7);
    }
}

#[tokio::test]
async fn zero_one_and_four_routes_have_exact_state_and_coverage() {
    let zero = provider(ScriptedExecutor::same(empty_success()))
        .search_native(context(Instant::now(), 10, Vec::new()))
        .await;
    assert_eq!(zero.status.state, SearchProviderState::Skipped);
    assert_eq!(coverage(&zero).eligible_units, 0);
    assert_eq!(coverage(&zero).searched_units, 0);

    let one_report = vec![report(
        "alpha",
        vec![route(
            "alpha",
            "a",
            UniversalSearchResolutionOrigin::Explicit,
        )],
    )];
    let one = provider(ScriptedExecutor::same(empty_success()))
        .search_native(context(Instant::now(), 10, one_report))
        .await;
    assert_eq!(one.status.state, SearchProviderState::Empty);
    assert_eq!(coverage(&one).eligible_units, 1);
    assert_eq!(coverage(&one).searched_units, 1);

    let four_reports = ["alpha", "beta", "charlie", "delta"]
        .into_iter()
        .map(|source| {
            report(
                source,
                vec![route(
                    source,
                    source,
                    UniversalSearchResolutionOrigin::Explicit,
                )],
            )
        })
        .collect();
    let four = provider(ScriptedExecutor::same(empty_success()))
        .search_native(context(Instant::now(), 10, four_reports))
        .await;
    assert_eq!(four.status.state, SearchProviderState::Empty);
    assert_eq!(coverage(&four).eligible_units, 4);
    assert_eq!(coverage(&four).searched_units, 4);
    assert_eq!(coverage(&four).failed_units, 0);
    assert!(!coverage(&four).budget_exhausted);
}

#[tokio::test]
async fn diagnostics_are_capped_by_tier_then_explicit_source_route_order() {
    let mut alpha = report(
        "alpha",
        vec![
            route("alpha", "a2", UniversalSearchResolutionOrigin::Explicit),
            route("alpha", "i2", UniversalSearchResolutionOrigin::Inferred),
            route("alpha", "a1", UniversalSearchResolutionOrigin::Explicit),
            route("alpha", "i1", UniversalSearchResolutionOrigin::Inferred),
        ],
    );
    alpha.omitted_diagnostic_count = 3;
    alpha.diagnostics = (0..14)
        .map(|index| {
            resolution_diagnostic(
                &format!("z{index:02}"),
                Some(&format!("r{index:02}")),
                Some(("safe_schema", "safe_function")),
                UniversalSearchResolutionReason::RouteStale,
            )
        })
        .collect();
    let beta = report(
        "beta",
        vec![route(
            "beta",
            "b1",
            UniversalSearchResolutionOrigin::Explicit,
        )],
    );
    let outcome = provider(ScriptedExecutor::same(empty_success()))
        .search_native(context(Instant::now(), 10, vec![beta, alpha]))
        .await;

    assert_eq!(outcome.status.diagnostics.len(), 16);
    assert!(outcome.status.diagnostics_truncated);
    assert_eq!(outcome.status.omitted_diagnostic_count, 6);
    assert_eq!(
        outcome.status.diagnostics[..4]
            .iter()
            .map(|diagnostic| {
                (
                    diagnostic.installed_source_name.as_str(),
                    diagnostic.function_name.as_deref(),
                )
            })
            .collect::<Vec<_>>(),
        [
            ("alpha", Some("a1")),
            ("alpha", Some("a2")),
            ("alpha", Some("i1")),
            ("beta", Some("b1")),
        ]
    );
    assert_eq!(
        outcome.status.diagnostics[4].reason,
        NativeSearchDiagnosticReason::FanoutLimitReached
    );
    assert_eq!(
        outcome.status.diagnostics[4].function_name.as_deref(),
        Some("i2")
    );
    assert_eq!(outcome.status.diagnostics[5].installed_source_name, "z00");
}

#[tokio::test]
async fn resolved_diagnostic_keeps_locator_and_unresolved_diagnostic_omits_it() {
    let mut report = report("alpha", Vec::new());
    report.diagnostics = vec![
        resolution_diagnostic(
            "alpha",
            Some("resolved"),
            Some(("safe_schema", "safe_function")),
            UniversalSearchResolutionReason::UnsafeOperation,
        ),
        resolution_diagnostic(
            "alpha",
            Some("unresolved"),
            None,
            UniversalSearchResolutionReason::AmbiguousRoute,
        ),
    ];
    let outcome = provider(ScriptedExecutor::same(empty_success()))
        .search_native(context(Instant::now(), 10, vec![report]))
        .await;
    let resolved = outcome
        .status
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.authored_route_id.as_deref() == Some("resolved"))
        .expect("resolved diagnostic");
    assert_eq!(resolved.schema_name.as_deref(), Some("safe_schema"));
    assert_eq!(resolved.function_name.as_deref(), Some("safe_function"));
    let unresolved = outcome
        .status
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.authored_route_id.as_deref() == Some("unresolved"))
        .expect("unresolved diagnostic");
    assert_eq!(unresolved.schema_name, None);
    assert_eq!(unresolved.function_name, None);
}

#[tokio::test(start_paused = true)]
async fn call_timeout_fires_at_600ms_and_cancellable_work_is_released() {
    let executor = ScriptedExecutor::same(Behaviour::WaitForCancellation);
    let reports = vec![report(
        "a",
        vec![route("a", "a", UniversalSearchResolutionOrigin::Explicit)],
    )];
    let provider = provider(executor.clone());
    let task = tokio::spawn(async move {
        provider
            .search_native(context(Instant::now(), 10, reports))
            .await
    });
    wait_for_starts(&executor, 1).await;

    tokio::time::advance(Duration::from_millis(599)).await;
    assert!(!task.is_finished());
    tokio::time::advance(Duration::from_millis(1)).await;
    let outcome = task.await.expect("provider task");

    assert!(coverage(&outcome).timed_out);
    assert!(coverage(&outcome).budget_exhausted);
    assert_eq!(coverage(&outcome).searched_units, 1);
    assert_eq!(coverage(&outcome).failed_units, 1);
    assert_eq!(
        outcome.status.diagnostics[0].reason,
        NativeSearchDiagnosticReason::CallTimeout
    );
    assert_eq!(
        outcome.status.diagnostics[0].state,
        NativeSearchDiagnosticState::TimedOut
    );
}

#[tokio::test(start_paused = true)]
async fn non_settling_cleanup_preserves_timeout_facts_and_stops_at_625ms() {
    let executor = ScriptedExecutor::same(Behaviour::NeverSettles);
    let reports = vec![report(
        "a",
        vec![route("a", "a", UniversalSearchResolutionOrigin::Explicit)],
    )];
    let provider = provider(executor.clone());
    let started_at = Instant::now();
    let task = tokio::spawn(async move {
        provider
            .search_native(context(started_at, 10, reports))
            .await
    });
    wait_for_starts(&executor, 1).await;

    tokio::time::advance(Duration::from_millis(624)).await;
    assert!(!task.is_finished());
    tokio::time::advance(Duration::from_millis(1)).await;
    let outcome = task.await.expect("provider task");

    assert_eq!(
        Instant::now().duration_since(started_at),
        Duration::from_millis(625)
    );
    assert!(coverage(&outcome).timed_out);
    assert!(coverage(&outcome).budget_exhausted);
    assert_eq!(
        outcome.status.diagnostics[0].reason,
        NativeSearchDiagnosticReason::UnsupportedCancellation
    );
    assert_eq!(
        outcome.status.diagnostics[0].state,
        NativeSearchDiagnosticState::TimedOut
    );
}

#[tokio::test(start_paused = true)]
async fn request_relative_global_cutoff_wins_when_setup_consumed_200ms() {
    let executor = ScriptedExecutor::same(Behaviour::NeverSettles);
    let reports = vec![report(
        "a",
        vec![route("a", "a", UniversalSearchResolutionOrigin::Explicit)],
    )];
    let provider = provider(executor.clone());
    let request_started_at = Instant::now() - Duration::from_millis(200);
    let task = tokio::spawn(async move {
        provider
            .search_native(context(request_started_at, 10, reports))
            .await
    });
    wait_for_starts(&executor, 1).await;

    tokio::time::advance(Duration::from_millis(574)).await;
    assert!(!task.is_finished());
    tokio::time::advance(Duration::from_millis(1)).await;
    let outcome = task.await.expect("provider task");

    assert_eq!(
        Instant::now().duration_since(request_started_at),
        Duration::from_millis(775)
    );
    assert_eq!(
        outcome.status.diagnostics[0].reason,
        NativeSearchDiagnosticReason::UnsupportedCancellation
    );
    assert!(coverage(&outcome).timed_out);
    assert!(coverage(&outcome).budget_exhausted);
}

#[tokio::test(start_paused = true)]
async fn success_settling_during_global_grace_is_discarded() {
    let wave_cancellation = coral_engine::QueryCancellationToken::new();
    let global_deadline = Instant::now() + Duration::from_millis(750);
    let selected = SelectedRoute {
        order: 0,
        route: route("alpha", "a", UniversalSearchResolutionOrigin::Explicit),
    };
    let controls = coral_engine::QueryExecutionControls::for_fanout(
        global_deadline,
        wave_cancellation.child_token(),
    );
    let task_selected = selected.clone();
    let task = tokio::spawn(async move {
        tokio::time::sleep_until(global_deadline + Duration::from_millis(10)).await;
        AttemptResult {
            selected: task_selected,
            elapsed: Duration::from_millis(760),
            upstream_started: true,
            timeout_scope: None,
            cleanup_settled: true,
            outcome: AttemptOutcome::Success {
                candidates: Vec::new(),
                raw_row_count: 1,
                continuation: false,
            },
        }
    });
    let collector = tokio::spawn(async move {
        collect_attempts(
            vec![CallTask {
                selected,
                controls,
                spawned_at: Instant::now(),
                deadline_state: CallDeadlineState::new(TimeoutScope::Global),
                task,
            }],
            &wave_cancellation,
            global_deadline,
            Duration::from_millis(25),
        )
        .await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(760)).await;
    let attempts = collector.await.expect("collector task");

    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].timeout_scope, Some(TimeoutScope::Global));
    assert!(matches!(
        attempts[0].outcome,
        AttemptOutcome::Failure(super::NativeCallFailure::GlobalBudgetExhausted)
    ));
}

#[tokio::test(start_paused = true)]
async fn post_global_grace_abort_is_not_awaited() {
    let started_at = Instant::now();
    let wave_cancellation = coral_engine::QueryCancellationToken::new();
    let global_deadline = started_at + Duration::from_millis(750);
    let selected = SelectedRoute {
        order: 0,
        route: route("alpha", "a", UniversalSearchResolutionOrigin::Explicit),
    };
    let controls = coral_engine::QueryExecutionControls::for_fanout(
        global_deadline,
        wave_cancellation.child_token(),
    );
    let task = tokio::spawn(future::pending::<AttemptResult>());
    let collector = tokio::spawn(async move {
        collect_attempts(
            vec![CallTask {
                selected,
                controls,
                spawned_at: started_at,
                deadline_state: CallDeadlineState::new(TimeoutScope::Global),
                task,
            }],
            &wave_cancellation,
            global_deadline,
            Duration::from_millis(25),
        )
        .await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(750)).await;
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(25)).await;
    let attempts = collector.await.expect("collector task");

    assert_eq!(
        Instant::now().duration_since(started_at),
        Duration::from_millis(775)
    );
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].timeout_scope, Some(TimeoutScope::Global));
    assert!(!attempts[0].cleanup_settled);
    assert!(matches!(
        attempts[0].outcome,
        AttemptOutcome::Failure(NativeCallFailure::UnsupportedCancellation)
    ));
}

#[tokio::test(start_paused = true)]
async fn remaining_99ms_skips_but_exactly_100ms_starts() {
    let reports = || {
        vec![report(
            "a",
            vec![route("a", "a", UniversalSearchResolutionOrigin::Explicit)],
        )]
    };
    let skipped_executor = ScriptedExecutor::same(empty_success());
    let skipped = provider(skipped_executor.clone())
        .search_native(context(
            Instant::now() - Duration::from_millis(651),
            10,
            reports(),
        ))
        .await;
    assert!(skipped_executor.starts().is_empty());
    assert_eq!(skipped.status.state, SearchProviderState::Skipped);
    assert!(coverage(&skipped).budget_exhausted);

    let started_executor = ScriptedExecutor::same(empty_success());
    let started = provider(started_executor.clone())
        .search_native(context(
            Instant::now() - Duration::from_millis(650),
            10,
            reports(),
        ))
        .await;
    assert_eq!(started_executor.starts(), ["a"]);
    assert_eq!(coverage(&started).searched_units, 1);
}

#[tokio::test(start_paused = true)]
async fn early_source_timeout_under_global_capped_deadline_is_call_timeout() {
    let executor = ScriptedExecutor::same(Behaviour::EarlyTimeout);
    let reports = vec![report(
        "a",
        vec![route("a", "a", UniversalSearchResolutionOrigin::Explicit)],
    )];
    let outcome = provider(executor)
        .search_native(context(
            Instant::now() - Duration::from_millis(200),
            10,
            reports,
        ))
        .await;

    assert_eq!(
        outcome.status.diagnostics[0].reason,
        NativeSearchDiagnosticReason::CallTimeout
    );
    assert!(coverage(&outcome).timed_out);
    assert!(coverage(&outcome).budget_exhausted);
}

#[tokio::test(start_paused = true)]
async fn panic_after_upstream_start_keeps_exact_search_accounting() {
    let executor = ScriptedExecutor::same(Behaviour::PanicAfter(Duration::from_millis(10)));
    let reports = vec![report(
        "a",
        vec![route("a", "a", UniversalSearchResolutionOrigin::Explicit)],
    )];
    let provider = provider(executor.clone());
    let task = tokio::spawn(async move {
        provider
            .search_native(context(Instant::now(), 10, reports))
            .await
    });
    wait_for_starts(&executor, 1).await;
    tokio::time::advance(Duration::from_millis(10)).await;
    let outcome = task.await.expect("provider task");

    assert_eq!(coverage(&outcome).searched_units, 1);
    assert_eq!(coverage(&outcome).failed_units, 1);
    assert!(outcome.status.diagnostics[0].elapsed_ms >= 10);
    assert_eq!(
        outcome.status.diagnostics[0].reason,
        NativeSearchDiagnosticReason::InternalError
    );
}

#[tokio::test(start_paused = true)]
async fn panic_during_call_cleanup_preserves_timeout_classification() {
    let executor =
        ScriptedExecutor::same(Behaviour::PanicAfterCancellation(Duration::from_millis(10)));
    let reports = vec![report(
        "alpha",
        vec![route(
            "alpha",
            "a",
            UniversalSearchResolutionOrigin::Explicit,
        )],
    )];
    let provider = provider(executor.clone());
    let task = tokio::spawn(async move {
        provider
            .search_native(context(Instant::now(), 10, reports))
            .await
    });
    wait_for_starts(&executor, 1).await;
    tokio::time::advance(Duration::from_millis(600)).await;
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(10)).await;
    let outcome = task.await.expect("provider task");

    assert!(coverage(&outcome).timed_out);
    assert!(coverage(&outcome).budget_exhausted);
    assert_eq!(coverage(&outcome).searched_units, 1);
    assert_eq!(coverage(&outcome).failed_units, 1);
    assert_eq!(
        outcome.status.diagnostics[0].reason,
        NativeSearchDiagnosticReason::CallTimeout
    );
    assert_eq!(
        outcome.status.diagnostics[0].state,
        NativeSearchDiagnosticState::TimedOut
    );
}

#[tokio::test(start_paused = true)]
async fn early_panic_reaped_after_another_deadline_remains_internal() {
    let executor = ScriptedExecutor::scripted([
        ("a".to_string(), Behaviour::NeverSettles),
        (
            "b".to_string(),
            Behaviour::PanicAfter(Duration::from_millis(5)),
        ),
    ]);
    let reports = vec![
        report(
            "alpha",
            vec![route(
                "alpha",
                "a",
                UniversalSearchResolutionOrigin::Explicit,
            )],
        ),
        report(
            "beta",
            vec![route(
                "beta",
                "b",
                UniversalSearchResolutionOrigin::Explicit,
            )],
        ),
    ];
    let provider = provider(executor.clone());
    let task = tokio::spawn(async move {
        provider
            .search_native(context(Instant::now(), 10, reports))
            .await
    });
    wait_for_starts(&executor, 2).await;
    tokio::time::advance(Duration::from_millis(5)).await;
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(595)).await;
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(25)).await;
    let outcome = task.await.expect("provider task");
    let beta = outcome
        .status
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.installed_source_name == "beta")
        .expect("beta diagnostic");

    assert_eq!(beta.reason, NativeSearchDiagnosticReason::InternalError);
    assert_eq!(beta.state, NativeSearchDiagnosticState::Error);
}

#[tokio::test]
async fn exactly_five_rows_without_continuation_does_not_claim_more() {
    let batch = RecordBatch::try_from_iter([(
        "title",
        Arc::new(StringArray::from(vec![
            "one", "two", "three", "four", "five",
        ])) as ArrayRef,
    )])
    .expect("batch");
    let reports = vec![report(
        "alpha",
        vec![route(
            "alpha",
            "a",
            UniversalSearchResolutionOrigin::Explicit,
        )],
    )];
    let outcome = provider(ScriptedExecutor::same(Behaviour::Immediate {
        batches: vec![batch],
        has_more: false,
    }))
    .search_native(context(Instant::now(), 10, reports))
    .await;

    assert_eq!(outcome.candidates.len(), 5);
    assert_eq!(coverage(&outcome).returned_count, 5);
    assert!(!coverage(&outcome).has_more);
}

#[tokio::test]
async fn contentless_rows_report_no_safe_fields_without_counting_a_result() {
    let batch = RecordBatch::try_from_iter([(
        "unmapped_field",
        Arc::new(StringArray::from(vec!["safe but not displayable"])) as ArrayRef,
    )])
    .expect("batch");
    let reports = vec![report(
        "alpha",
        vec![route(
            "alpha",
            "a",
            UniversalSearchResolutionOrigin::Explicit,
        )],
    )];
    let outcome = provider(ScriptedExecutor::same(Behaviour::Immediate {
        batches: vec![batch],
        has_more: false,
    }))
    .search_native(context(Instant::now(), 10, reports))
    .await;

    assert!(outcome.candidates.is_empty());
    assert_eq!(outcome.status.state, SearchProviderState::Empty);
    assert_eq!(coverage(&outcome).returned_count, 0);
    assert_eq!(
        outcome.status.diagnostics[0].reason,
        NativeSearchDiagnosticReason::NoSafeDisplayFields
    );
}

#[tokio::test]
async fn returned_count_is_measured_before_native_deduplication() {
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(StringArray::from(vec!["same-id", "same-id"])) as ArrayRef,
        ),
        (
            "title",
            Arc::new(StringArray::from(vec!["same result", "same result"])) as ArrayRef,
        ),
    ])
    .expect("batch");
    let reports = vec![report(
        "alpha",
        vec![route(
            "alpha",
            "a",
            UniversalSearchResolutionOrigin::Explicit,
        )],
    )];
    let outcome = provider(ScriptedExecutor::same(Behaviour::Immediate {
        batches: vec![batch],
        has_more: false,
    }))
    .search_native(context(Instant::now(), 10, reports))
    .await;

    assert_eq!(coverage(&outcome).returned_count, 2);
    assert_eq!(outcome.candidates.len(), 1);
}

#[tokio::test]
async fn catalog_resolution_failure_marks_native_provider_error_without_starting_calls() {
    let executor = ScriptedExecutor::same(empty_success());
    let outcome = provider(executor.clone())
        .search_native(context_with_resolution(
            Instant::now(),
            10,
            Err(QueryManagerError::App(AppError::Unavailable(
                "catalog unavailable".to_string(),
            ))),
        ))
        .await;

    assert!(executor.starts().is_empty());
    assert!(outcome.candidates.is_empty());
    assert_eq!(outcome.status.state, SearchProviderState::Error);
    assert_eq!(coverage(&outcome).eligible_units, 0);
    assert_eq!(coverage(&outcome).searched_units, 0);
    assert_eq!(coverage(&outcome).failed_units, 0);
}

#[tokio::test]
async fn contentless_rows_are_empty_and_safe_rows_never_expose_raw_sentinels() {
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(StringArray::from(vec![
                Some("safe-1"),
                Some("sk-12345678901234567890"),
            ])) as ArrayRef,
        ),
        (
            "title",
            Arc::new(StringArray::from(vec![
                Some("Safe issue"),
                Some("Bearer secret-secret-secret"),
            ])) as ArrayRef,
        ),
        (
            "url",
            Arc::new(StringArray::from(vec![
                Some("https://example.test/issue?%74oken=raw-url-secret&safe=1"),
                Some("javascript:raw-password"),
            ])) as ArrayRef,
        ),
    ])
    .expect("batch");
    let executor = ScriptedExecutor::same(Behaviour::Immediate {
        batches: vec![batch],
        has_more: false,
    });
    let reports = vec![report(
        "a",
        vec![route("a", "a", UniversalSearchResolutionOrigin::Explicit)],
    )];
    let outcome = provider(executor)
        .search_native(context(Instant::now(), 10, reports))
        .await;

    assert_eq!(coverage(&outcome).returned_count, 1);
    assert_eq!(outcome.candidates.len(), 1);
    let SearchPayload::NativeResult(result) = &outcome.candidates[0].payload else {
        panic!("native result");
    };
    assert_eq!(result.title.as_deref(), Some("Safe issue"));
    assert_eq!(
        result.url.as_deref(),
        Some("https://example.test/issue?safe=1")
    );
    let rendered = format!("{outcome:?}");
    for sentinel in [
        "sensitive SQL sentinel",
        "raw-url-secret",
        "raw-password",
        "Bearer secret-secret-secret",
        "sk-12345678901234567890",
    ] {
        assert!(!rendered.contains(sentinel), "leaked sentinel {sentinel}");
    }
}
