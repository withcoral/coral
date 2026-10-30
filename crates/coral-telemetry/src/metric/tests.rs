use std::time::Duration;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::*;
use crate::testing::MetricCapture;

/// The stable catalog: each instrument's name, kind, unit and the only
/// attribute keys it carries. A change here is a breaking change for
/// operators' dashboards.
const CATALOG: &[(&str, &str, &str, &[&str])] = &[
    (
        "coral.request.duration",
        "histogram",
        "s",
        &["interface", "operation", "outcome"],
    ),
    (
        "coral.program.compile.duration",
        "histogram",
        "s",
        &["outcome"],
    ),
    (
        "coral.program.execution.cpu.duration",
        "histogram",
        "s",
        &[],
    ),
    ("coral.program.execution.heap.peak", "histogram", "By", &[]),
    (
        "coral.program.termination",
        "counter",
        "{program}",
        &["outcome"],
    ),
    (
        "coral.run.accepted",
        "counter",
        "{run}",
        &["principal.kind"],
    ),
    (
        "coral.run.ended",
        "counter",
        "{run}",
        &["failure.code", "run.status"],
    ),
    ("coral.run.duration", "histogram", "s", &["run.status"]),
    ("coral.run.phase.duration", "histogram", "s", &["run.phase"]),
    ("coral.run.active", "gauge", "{run}", &[]),
    ("coral.run.queue.depth", "gauge", "{run}", &[]),
    ("coral.run.recovery", "counter", "{run}", &["outcome"]),
    (
        "coral.invocation.started",
        "counter",
        "{invocation}",
        &["provider.protocol", "tool.effect"],
    ),
    (
        "coral.invocation.ended",
        "counter",
        "{invocation}",
        &[
            "failure.code",
            "invocation.result_kind",
            "provider.protocol",
            "tool.effect",
        ],
    ),
    (
        "coral.invocation.duration",
        "histogram",
        "s",
        &["invocation.result_kind", "provider.protocol", "tool.effect"],
    ),
    (
        "coral.invocation.gate.duration",
        "histogram",
        "s",
        &["gate.kind"],
    ),
    ("coral.invocation.active", "gauge", "{invocation}", &[]),
    (
        "coral.upstream.duration",
        "histogram",
        "s",
        &["outcome", "provider.protocol"],
    ),
    (
        "coral.upstream.request.size",
        "histogram",
        "By",
        &["provider.protocol"],
    ),
    (
        "coral.upstream.response.size",
        "histogram",
        "By",
        &["provider.protocol"],
    ),
    (
        "coral.persistence.transaction.duration",
        "histogram",
        "s",
        &["db.system", "outcome"],
    ),
    (
        "coral.persistence.transaction.failures",
        "counter",
        "{transaction}",
        &["db.system", "failure.code"],
    ),
    (
        "coral.persistence.pool.in_use",
        "gauge",
        "{connection}",
        &["db.system"],
    ),
    ("coral.output.size", "histogram", "By", &["output.kind"]),
    (
        "coral.output.rejections",
        "counter",
        "{output}",
        &["output.kind"],
    ),
    (
        "coral.run.claims",
        "counter",
        "{claim}",
        &["operation", "outcome"],
    ),
    (
        "coral.telemetry.dropped",
        "counter",
        "{signal}",
        &["drop.reason", "signal.kind"],
    ),
];

/// Records every instrument once, with every attribute it can carry. The
/// pool it returns is observed while it is held.
fn record_each(metrics: &Metrics) -> Option<PoolObservation> {
    metrics.request_duration(
        Interface::Mcp,
        "/mcp".to_string(),
        RequestOutcome::Ok,
        Duration::from_millis(5),
    );
    program_compiled(CompileOutcome::Ok, Duration::from_millis(30));
    program_executed(Some(Duration::from_millis(12)), 4096);
    program_terminated(TerminationReason::Canceled);
    run_accepted(PrincipalKind::Agent);
    run_ended(
        RunStatus::Failed,
        Some("program_execution_failed"),
        Duration::from_secs(2),
    );
    drop(run_phase(RunPhase::Execute));
    let active = run_active();
    let queued = run_queued();
    run_recovered(RecoveryOutcome::Ended);
    run_claimed(ClaimKind::Recover, ClaimOutcome::Won);
    let invocation = invocation_started(ProviderProtocol::Http, ToolEffect::Read);
    drop(invocation_gate(GateKind::Approval));
    invocation.end(InvocationEnd::CoralFailure, Some("account_required"));
    let _in_progress = invocation_started(ProviderProtocol::Mcp, ToolEffect::Write);
    upstream_exchanged(
        ProviderProtocol::Http,
        UpstreamOutcome::Response,
        Duration::from_millis(40),
        Some(10),
        Some(2048),
    );
    transaction_ended(
        DbSystem::Postgresql,
        TransactionOutcome::Committed,
        Duration::from_millis(3),
    );
    transaction_failed(DbSystem::Sqlite, "writer_busy");
    let pool = observe_pool_in_use(DbSystem::Sqlite, || 2);
    output_kept(OutputKind::Emission, 512);
    output_rejected(OutputKind::FinalValue);
    metrics.telemetry_dropped(SignalKind::Span, DropReason::QueueFull, 3);
    drop((active, queued));
    pool
}

#[test]
fn every_instrument_exports_with_its_unit_and_only_its_attributes() {
    let capture = MetricCapture::default();
    let pool =
        tracing::subscriber::with_default(capture.subscriber(), || record_each(&capture.metrics()));
    assert!(pool.is_some(), "the pool is observed under the layer");
    let points = capture.points();
    for (name, kind, unit, keys) in CATALOG {
        let found = points
            .iter()
            .filter(|point| point.name == *name)
            .collect::<Vec<_>>();
        assert!(!found.is_empty(), "{name} exports");
        for point in &found {
            assert_eq!((point.kind, point.unit.as_str()), (*kind, *unit), "{name}");
        }
        let found_keys = found
            .iter()
            .flat_map(|point| point.attributes.keys().map(String::as_str))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            found_keys,
            keys.iter().copied().collect(),
            "{name} carries exactly its attributes"
        );
    }
    assert_eq!(
        points
            .iter()
            .map(|point| point.name.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        CATALOG.len(),
        "no instrument outside the catalog"
    );
}

fn values(capture: &MetricCapture, name: &str) -> Vec<f64> {
    capture
        .named(name)
        .into_iter()
        .map(|point| point.value)
        .collect()
}

/// A level counts what is live and returns to zero when it drops; an
/// Invocation that drops without an end counts as a host defect.
#[test]
fn levels_follow_their_guards_and_a_dropped_invocation_is_a_host_defect() {
    let capture = MetricCapture::default();
    tracing::subscriber::with_default(capture.subscriber(), || {
        let first = run_active();
        let second = run_active();
        drop(first);
        assert_eq!(values(&capture, "coral.run.active"), [1.0]);
        drop(second);
        drop(invocation_started(
            ProviderProtocol::Http,
            ToolEffect::Write,
        ));
    });
    assert_eq!(values(&capture, "coral.run.active"), [0.0]);
    assert_eq!(values(&capture, "coral.invocation.active"), [0.0]);
    let ended = capture.named("coral.invocation.ended");
    assert_eq!(
        ended
            .iter()
            .map(|point| point
                .attributes
                .get("invocation.result_kind")
                .map(String::as_str))
            .collect::<Vec<_>>(),
        [Some("host_defect")]
    );
}

/// A pool is observed only while its observation is held, and each
/// database's connections in use are read when metrics are collected.
#[test]
fn pools_are_read_at_collection_while_observed() {
    let capture = MetricCapture::default();
    let in_use = Arc::new(std::sync::atomic::AtomicU64::new(3));
    let read = Arc::clone(&in_use);
    let observed = tracing::subscriber::with_default(capture.subscriber(), || {
        observe_pool_in_use(DbSystem::Postgresql, move || read.load(Ordering::Relaxed))
    });
    assert_eq!(values(&capture, "coral.persistence.pool.in_use"), [3.0]);
    in_use.store(0, Ordering::Relaxed);
    assert_eq!(values(&capture, "coral.persistence.pool.in_use"), [0.0]);
    drop(observed);
    assert_eq!(
        values(&capture, "coral.persistence.pool.in_use"),
        [] as [f64; 0],
        "a dropped pool reports nothing"
    );
}

/// Without the layer, domain code records nothing and does not fail.
#[test]
fn nothing_records_without_the_layer() {
    tracing::subscriber::with_default(tracing_subscriber::registry(), || {
        assert!(current().is_none(), "no catalog without the layer");
        run_accepted(PrincipalKind::User);
        drop(run_active());
    });
}
