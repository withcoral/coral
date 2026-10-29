use std::time::Duration;

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
        "coral.worker.claims",
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

/// Records every instrument once, with every attribute it can carry.
fn record_each(metrics: &Metrics) {
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
    worker_claimed(ClaimKind::Recover, ClaimOutcome::Won);
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
    pool_in_use(DbSystem::Sqlite, 2);
    output_kept(OutputKind::Emission, 512);
    output_rejected(OutputKind::FinalValue);
    metrics.telemetry_dropped(SignalKind::Span, DropReason::QueueFull, 3);
    drop((active, queued));
}

#[test]
fn every_instrument_exports_with_its_unit_and_only_its_attributes() {
    let capture = MetricCapture::default();
    tracing::subscriber::with_default(capture.subscriber(), || record_each(&capture.metrics()));
    let points = capture.points();
    for (name, kind, unit, keys) in CATALOG {
        let found = points
            .iter()
            .filter(|point| point.name == *name)
            .collect::<Vec<_>>();
        assert!(!found.is_empty(), "{name} exports");
        for point in found {
            assert_eq!((point.kind, point.unit.as_str()), (*kind, *unit), "{name}");
            let mut found_keys = point
                .attributes
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>();
            found_keys.sort_unstable();
            assert!(
                found_keys.iter().all(|key| keys.contains(key)),
                "{name} carries only {keys:?}, not {found_keys:?}"
            );
        }
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

/// Without the layer, domain code records nothing and does not fail.
#[test]
fn nothing_records_without_the_layer() {
    let capture = MetricCapture::default();
    run_accepted(PrincipalKind::User);
    drop(run_active());
    assert!(capture.points().is_empty());
}
