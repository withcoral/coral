//! The stable `coral.*` metric catalog: each instrument's name, unit and
//! bounded attributes, and the functions the owning domains record it with.
//!
//! Application bootstrap builds [`Metrics`] from its meter provider and adds
//! [`Metrics::layer`] to the tracing subscriber. Domain code records through
//! the functions here, which find the catalog through the current tracing
//! dispatcher, so they record nothing without the layer and a test scopes
//! them as it scopes spans. Each attribute is one of a bounded set: an enum
//! here, or a code Coral's own code fixes. No attribute holds an identifier,
//! a Provider name, a Tool Call Path, a client, an endpoint or an error's
//! text. Durations are seconds, sizes are bytes, active and queue values are
//! gauges, transitions are counters and distributions are histograms.
//! Metrics are never sampled.

use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use opentelemetry::metrics::{
    AsyncInstrument, Counter, Histogram, Meter, MeterProvider, ObservableGauge,
};
use opentelemetry::{InstrumentationScope, KeyValue};

mod attribute;

pub use attribute::{
    ClaimKind, ClaimOutcome, CompileOutcome, DbSystem, DropReason, GateKind, Interface,
    InvocationEnd, OutputKind, PrincipalKind, ProviderProtocol, RecoveryOutcome, RequestOutcome,
    RunPhase, RunStatus, SignalKind, TerminationReason, ToolEffect, TransactionOutcome,
    UpstreamOutcome,
};

/// Histogram bounds for a duration in seconds, from 5 ms to an hour.
const SECONDS: [f64; 18] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
    1800.0, 3600.0,
];

/// Histogram bounds for a size in bytes, from 256 B to 64 MiB.
const BYTES: [f64; 10] = [
    256.0,
    1024.0,
    4096.0,
    16384.0,
    65536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
    67_108_864.0,
];

/// The catalog's instruments, built from one meter provider.
#[derive(Debug, Clone)]
pub struct Metrics(Arc<Catalog>);

#[derive(Debug)]
struct Catalog {
    request_duration: Histogram<f64>,
    program_compile_duration: Histogram<f64>,
    program_execution_cpu_duration: Histogram<f64>,
    program_execution_heap_peak: Histogram<u64>,
    program_termination: Counter<u64>,
    run_accepted: Counter<u64>,
    run_ended: Counter<u64>,
    run_duration: Histogram<f64>,
    run_phase_duration: Histogram<f64>,
    run_active: Level,
    run_queue_depth: Level,
    run_recovery: Counter<u64>,
    invocation_started: Counter<u64>,
    invocation_ended: Counter<u64>,
    invocation_duration: Histogram<f64>,
    invocation_gate_duration: Histogram<f64>,
    invocation_active: Level,
    upstream_duration: Histogram<f64>,
    upstream_request_size: Histogram<u64>,
    upstream_response_size: Histogram<u64>,
    persistence_transaction_duration: Histogram<f64>,
    persistence_transaction_failures: Counter<u64>,
    pools: Pools,
    _persistence_pool_in_use: ObservableGauge<u64>,
    output_size: Histogram<u64>,
    output_rejections: Counter<u64>,
    worker_claims: Counter<u64>,
    telemetry_dropped: Counter<u64>,
}

/// A gauge of how many of something are live on this Coral Instance,
/// read when metrics are collected.
#[derive(Debug)]
struct Level {
    count: Arc<AtomicI64>,
    _gauge: ObservableGauge<i64>,
}

impl Level {
    fn new(
        meter: &Meter,
        name: &'static str,
        unit: &'static str,
        description: &'static str,
    ) -> Self {
        let count = Arc::new(AtomicI64::new(0));
        let observed = Arc::clone(&count);
        Self {
            _gauge: meter
                .i64_observable_gauge(name)
                .with_unit(unit)
                .with_description(description)
                .with_callback(move |gauge| gauge.observe(observed.load(Ordering::Relaxed), &[]))
                .build(),
            count,
        }
    }

    fn add(&self, delta: i64) {
        self.count.fetch_add(delta, Ordering::Relaxed);
    }
}

/// The database pools whose connections in use are read when metrics are
/// collected: each [`PoolObservation`] that is still held.
type Pools = Arc<Mutex<Vec<Weak<PoolSource>>>>;

struct PoolSource {
    system: DbSystem,
    in_use: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl fmt::Debug for PoolSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PoolSource")
            .field("system", &self.system)
            .finish_non_exhaustive()
    }
}

/// Keeps one database's pools in `coral.persistence.pool.in_use` while it
/// is held.
#[derive(Debug)]
#[must_use = "the pool is observed while this is held"]
pub struct PoolObservation(
    #[expect(dead_code, reason = "held for its registration")] Arc<PoolSource>,
);

fn observe_pools(pools: &Pools, gauge: &dyn AsyncInstrument<u64>) {
    let mut sources = pools
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    sources.retain(|source| source.strong_count() > 0);
    for system in [DbSystem::Sqlite, DbSystem::Postgresql] {
        let mut connections = None;
        for source in sources.iter().filter_map(Weak::upgrade) {
            if source.system == system {
                connections = Some(
                    connections
                        .unwrap_or(0_u64)
                        .saturating_add((source.in_use)()),
                );
            }
        }
        if let Some(connections) = connections {
            gauge.observe(connections, &[KeyValue::new("db.system", system.as_str())]);
        }
    }
}

impl Metrics {
    /// Builds every instrument from `provider`, under the `coral` scope.
    #[must_use]
    #[expect(
        clippy::too_many_lines,
        reason = "one entry for each instrument of the catalog"
    )]
    pub fn new<P: MeterProvider + ?Sized>(provider: &P) -> Self {
        let meter = provider.meter_with_scope(
            InstrumentationScope::builder("coral")
                .with_version(env!("CARGO_PKG_VERSION"))
                .build(),
        );
        let seconds = |name: &'static str, description: &'static str| {
            meter
                .f64_histogram(name)
                .with_unit("s")
                .with_description(description)
                .with_boundaries(SECONDS.to_vec())
                .build()
        };
        let bytes = |name: &'static str, description: &'static str| {
            meter
                .u64_histogram(name)
                .with_unit("By")
                .with_description(description)
                .with_boundaries(BYTES.to_vec())
                .build()
        };
        let counter = |name: &'static str, unit: &'static str, description: &'static str| {
            meter
                .u64_counter(name)
                .with_unit(unit)
                .with_description(description)
                .build()
        };
        let pools = Pools::default();
        Self(Arc::new(Catalog {
            request_duration: seconds(
                "coral.request.duration",
                "How long an inbound request took, until its response ended.",
            ),
            program_compile_duration: seconds(
                "coral.program.compile.duration",
                "How long compiling a submitted Program took.",
            ),
            program_execution_cpu_duration: seconds(
                "coral.program.execution.cpu.duration",
                "The CPU time a Program's execution thread used.",
            ),
            program_execution_heap_peak: bytes(
                "coral.program.execution.heap.peak",
                "The most heap a Program's execution used.",
            ),
            program_termination: counter(
                "coral.program.termination",
                "{program}",
                "Programs Coral stopped before they returned.",
            ),
            run_accepted: counter("coral.run.accepted", "{run}", "Program Runs accepted."),
            run_ended: counter("coral.run.ended", "{run}", "Program Runs that ended."),
            run_duration: seconds(
                "coral.run.duration",
                "How long a Program Run took, from acceptance to its end.",
            ),
            run_phase_duration: seconds(
                "coral.run.phase.duration",
                "How long one phase of a Program Run took.",
            ),
            run_active: Level::new(
                &meter,
                "coral.run.active",
                "{run}",
                "Program Runs whose Run Owner is live on this Coral Instance.",
            ),
            run_queue_depth: Level::new(
                &meter,
                "coral.run.queue.depth",
                "{run}",
                "Program Runs on this Coral Instance waiting for their start.",
            ),
            run_recovery: counter(
                "coral.run.recovery",
                "{run}",
                "Program Runs a recovered Run Owner took over.",
            ),
            invocation_started: counter(
                "coral.invocation.started",
                "{invocation}",
                "Tool Invocations admitted.",
            ),
            invocation_ended: counter(
                "coral.invocation.ended",
                "{invocation}",
                "Tool Invocations that ended.",
            ),
            invocation_duration: seconds(
                "coral.invocation.duration",
                "How long a Tool Invocation took, from admission to its end.",
            ),
            invocation_gate_duration: seconds(
                "coral.invocation.gate.duration",
                "How long a Tool Invocation spent at one gate, including its wait.",
            ),
            invocation_active: Level::new(
                &meter,
                "coral.invocation.active",
                "{invocation}",
                "Tool Invocations in progress on this Coral Instance.",
            ),
            upstream_duration: seconds(
                "coral.upstream.duration",
                "How long one request to an Upstream Service took.",
            ),
            upstream_request_size: bytes(
                "coral.upstream.request.size",
                "The body size of a request to an Upstream Service.",
            ),
            upstream_response_size: bytes(
                "coral.upstream.response.size",
                "The body size of a response from an Upstream Service.",
            ),
            persistence_transaction_duration: seconds(
                "coral.persistence.transaction.duration",
                "How long a database transaction was open.",
            ),
            persistence_transaction_failures: counter(
                "coral.persistence.transaction.failures",
                "{transaction}",
                "Database transactions that failed to begin or commit.",
            ),
            _persistence_pool_in_use: {
                let observed = Arc::clone(&pools);
                meter
                    .u64_observable_gauge("coral.persistence.pool.in_use")
                    .with_unit("{connection}")
                    .with_description("Database connections in use.")
                    .with_callback(move |gauge| observe_pools(&observed, gauge))
                    .build()
            },
            pools,
            output_size: bytes(
                "coral.output.size",
                "The size of one output a Program Run kept.",
            ),
            output_rejections: counter(
                "coral.output.rejections",
                "{output}",
                "Outputs a Program Run could not keep, because they were too large or invalid.",
            ),
            worker_claims: counter(
                "coral.worker.claims",
                "{claim}",
                "Claims a Coral Instance made to own a Program Run.",
            ),
            telemetry_dropped: counter(
                "coral.telemetry.dropped",
                "{signal}",
                "Spans, log records and metric points Coral dropped instead of exporting.",
            ),
        }))
    }

    /// The tracing layer through which domain code finds this catalog.
    #[must_use]
    pub fn layer(&self) -> MetricsLayer {
        MetricsLayer(self.clone())
    }

    /// Counts `count` signals of `signal` dropped for `reason`.
    pub fn telemetry_dropped(&self, signal: SignalKind, reason: DropReason, count: u64) {
        self.0.telemetry_dropped.add(
            count,
            &[
                KeyValue::new("signal.kind", signal.as_str()),
                KeyValue::new("drop.reason", reason.as_str()),
            ],
        );
    }

    pub(crate) fn request_duration(
        &self,
        interface: Interface,
        operation: String,
        outcome: RequestOutcome,
        elapsed: Duration,
    ) {
        self.0.request_duration.record(
            elapsed.as_secs_f64(),
            &[
                KeyValue::new("interface", interface.as_str()),
                KeyValue::new("operation", operation),
                KeyValue::new("outcome", outcome.as_str()),
            ],
        );
    }
}

/// Holds the [`Metrics`] the domain functions record to. It observes no
/// span or event.
#[derive(Debug, Clone)]
pub struct MetricsLayer(Metrics);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for MetricsLayer {}

/// The catalog of the current tracing dispatcher, when it has one.
pub(crate) fn current() -> Option<Metrics> {
    tracing::dispatcher::get_default(|dispatch| {
        dispatch
            .downcast_ref::<MetricsLayer>()
            .map(|layer| layer.0.clone())
    })
}

fn with(record: impl FnOnce(&Catalog)) {
    if let Some(metrics) = current() {
        record(&metrics.0);
    }
}

fn failure_code(code: Option<&'static str>) -> Option<KeyValue> {
    code.map(|code| KeyValue::new("failure.code", code))
}

/// Records one Program compile that took `elapsed` and ended `outcome`.
pub fn program_compiled(outcome: CompileOutcome, elapsed: Duration) {
    with(|catalog| {
        catalog.program_compile_duration.record(
            elapsed.as_secs_f64(),
            &[KeyValue::new("outcome", outcome.as_str())],
        );
    });
}

/// Records what one Program execution used: the CPU time of the thread that
/// ran it, where the platform measures it, and the most heap it held.
pub fn program_executed(cpu: Option<Duration>, heap_peak_bytes: u64) {
    with(|catalog| {
        if let Some(cpu) = cpu {
            catalog
                .program_execution_cpu_duration
                .record(cpu.as_secs_f64(), &[]);
        }
        catalog
            .program_execution_heap_peak
            .record(heap_peak_bytes, &[]);
    });
}

/// Counts a Program Coral stopped for `reason` before it returned.
pub fn program_terminated(reason: TerminationReason) {
    with(|catalog| {
        catalog
            .program_termination
            .add(1, &[KeyValue::new("outcome", reason.as_str())]);
    });
}

/// Counts a Program Run accepted for a Principal of `principal` kind.
pub fn run_accepted(principal: PrincipalKind) {
    with(|catalog| {
        catalog
            .run_accepted
            .add(1, &[KeyValue::new("principal.kind", principal.as_str())]);
    });
}

/// Records a Program Run that ended `status`, with its public failure code,
/// `elapsed` after it was accepted.
pub fn run_ended(status: RunStatus, code: Option<&'static str>, elapsed: Duration) {
    with(|catalog| {
        let status = KeyValue::new("run.status", status.as_str());
        let ended = std::iter::once(status.clone())
            .chain(failure_code(code))
            .collect::<Vec<_>>();
        catalog.run_ended.add(1, &ended);
        catalog
            .run_duration
            .record(elapsed.as_secs_f64(), &[status]);
    });
}

/// Times one phase of a Program Run until the returned timer drops.
#[must_use = "the phase is timed until the timer drops"]
pub fn run_phase(phase: RunPhase) -> Timer {
    Timer::start(Timed::RunPhase(phase))
}

/// Counts one Run Owner live on this Coral Instance until the returned level
/// drops.
#[must_use = "the Run counts as active until the level drops"]
pub fn run_active() -> Active {
    Active::start(Live::Run)
}

/// Counts one accepted Program Run waiting for its start on this Coral
/// Instance until the returned level drops.
#[must_use = "the Run counts as queued until the level drops"]
pub fn run_queued() -> Active {
    Active::start(Live::Queued)
}

/// Counts one Program Run a recovered Run Owner took over, by how its one
/// chunk ended.
pub fn run_recovered(outcome: RecoveryOutcome) {
    with(|catalog| {
        catalog
            .run_recovery
            .add(1, &[KeyValue::new("outcome", outcome.as_str())]);
    });
}

/// Counts one claim of `claim` kind to own a Program Run, by its outcome.
pub fn worker_claimed(claim: ClaimKind, outcome: ClaimOutcome) {
    with(|catalog| {
        catalog.worker_claims.add(
            1,
            &[
                KeyValue::new("operation", claim.as_str()),
                KeyValue::new("outcome", outcome.as_str()),
            ],
        );
    });
}

/// Counts a Tool Invocation admitted for a Tool of `protocol` and `effect`,
/// and measures it until [`Invocation::end`], or until it drops, which
/// records a host defect.
#[must_use = "the Invocation is measured until it ends"]
pub fn invocation_started(protocol: ProviderProtocol, effect: ToolEffect) -> Invocation {
    let metrics = current();
    let attributes = [
        KeyValue::new("provider.protocol", protocol.as_str()),
        KeyValue::new("tool.effect", effect.as_str()),
    ];
    if let Some(metrics) = &metrics {
        metrics.0.invocation_started.add(1, &attributes);
        metrics.0.invocation_active.add(1);
    }
    Invocation {
        metrics,
        attributes,
        started: Instant::now(),
    }
}

/// Times one Tool Invocation's pass through `gate`, including its wait,
/// until the returned timer drops.
#[must_use = "the gate is timed until the timer drops"]
pub fn invocation_gate(gate: GateKind) -> Timer {
    Timer::start(Timed::Gate(gate))
}

/// One admitted Tool Invocation, measured until it ends.
#[derive(Debug)]
pub struct Invocation {
    metrics: Option<Metrics>,
    attributes: [KeyValue; 2],
    started: Instant,
}

impl Invocation {
    /// Records how the Invocation ended, with its public failure code.
    pub fn end(mut self, end: InvocationEnd, code: Option<&'static str>) {
        self.finish(end, code);
    }

    fn finish(&mut self, end: InvocationEnd, code: Option<&'static str>) {
        let Some(metrics) = self.metrics.take() else {
            return;
        };
        let [protocol, effect] = self.attributes.clone();
        let result = KeyValue::new("invocation.result_kind", end.as_str());
        metrics.0.invocation_duration.record(
            self.started.elapsed().as_secs_f64(),
            &[protocol.clone(), effect.clone(), result.clone()],
        );
        let ended = [protocol, effect, result]
            .into_iter()
            .chain(failure_code(code))
            .collect::<Vec<_>>();
        metrics.0.invocation_ended.add(1, &ended);
        metrics.0.invocation_active.add(-1);
    }
}

impl Drop for Invocation {
    fn drop(&mut self) {
        self.finish(InvocationEnd::HostDefect, None);
    }
}

/// Records one request to an Upstream Service over `protocol` that took
/// `elapsed`, with its body sizes when they are known.
pub fn upstream_exchanged(
    protocol: ProviderProtocol,
    outcome: UpstreamOutcome,
    elapsed: Duration,
    request_bytes: Option<u64>,
    response_bytes: Option<u64>,
) {
    with(|catalog| {
        let protocol = || KeyValue::new("provider.protocol", protocol.as_str());
        catalog.upstream_duration.record(
            elapsed.as_secs_f64(),
            &[protocol(), KeyValue::new("outcome", outcome.as_str())],
        );
        if let Some(bytes) = request_bytes {
            catalog.upstream_request_size.record(bytes, &[protocol()]);
        }
        if let Some(bytes) = response_bytes {
            catalog.upstream_response_size.record(bytes, &[protocol()]);
        }
    });
}

/// Records a database transaction on `system` that was open for `elapsed`
/// and ended `outcome`.
pub fn transaction_ended(system: DbSystem, outcome: TransactionOutcome, elapsed: Duration) {
    with(|catalog| {
        catalog.persistence_transaction_duration.record(
            elapsed.as_secs_f64(),
            &[
                KeyValue::new("db.system", system.as_str()),
                KeyValue::new("outcome", outcome.as_str()),
            ],
        );
    });
}

/// Counts a database transaction on `system` that failed to begin or commit,
/// by its failure code.
pub fn transaction_failed(system: DbSystem, code: &'static str) {
    with(|catalog| {
        catalog.persistence_transaction_failures.add(
            1,
            &[
                KeyValue::new("db.system", system.as_str()),
                KeyValue::new("failure.code", code),
            ],
        );
    });
}

/// Observes a database's connections in use on `system`, as `in_use`
/// counts them, whenever metrics are collected, until the returned
/// observation drops. `None` without a catalog to observe them in.
pub fn observe_pool_in_use(
    system: DbSystem,
    in_use: impl Fn() -> u64 + Send + Sync + 'static,
) -> Option<PoolObservation> {
    let metrics = current()?;
    let source = Arc::new(PoolSource {
        system,
        in_use: Box::new(in_use),
    });
    metrics
        .0
        .pools
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(Arc::downgrade(&source));
    Some(PoolObservation(source))
}

/// Records the size of one output of `kind` a Program Run kept.
pub fn output_kept(kind: OutputKind, bytes: u64) {
    with(|catalog| {
        catalog
            .output_size
            .record(bytes, &[KeyValue::new("output.kind", kind.as_str())]);
    });
}

/// Counts one output of `kind` a Program Run could not keep.
pub fn output_rejected(kind: OutputKind) {
    with(|catalog| {
        catalog
            .output_rejections
            .add(1, &[KeyValue::new("output.kind", kind.as_str())]);
    });
}

/// What a [`Timer`] times.
#[derive(Debug, Clone, Copy)]
enum Timed {
    RunPhase(RunPhase),
    Gate(GateKind),
}

/// Records how long something took when it drops.
#[derive(Debug)]
pub struct Timer {
    metrics: Option<Metrics>,
    timed: Timed,
    started: Instant,
}

impl Timer {
    fn start(timed: Timed) -> Self {
        Self {
            metrics: current(),
            timed,
            started: Instant::now(),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        let elapsed = self.started.elapsed().as_secs_f64();
        match self.timed {
            Timed::RunPhase(phase) => metrics
                .0
                .run_phase_duration
                .record(elapsed, &[KeyValue::new("run.phase", phase.as_str())]),
            Timed::Gate(gate) => metrics
                .0
                .invocation_gate_duration
                .record(elapsed, &[KeyValue::new("gate.kind", gate.as_str())]),
        }
    }
}

/// What an [`Active`] counts.
#[derive(Debug, Clone, Copy)]
enum Live {
    Run,
    Queued,
}

/// Counts one live thing on its gauge until it drops.
#[derive(Debug)]
pub struct Active {
    metrics: Option<Metrics>,
    live: Live,
}

impl Active {
    fn start(live: Live) -> Self {
        let metrics = current();
        if let Some(metrics) = &metrics {
            metrics.level(live).add(1);
        }
        Self { metrics, live }
    }
}

impl Metrics {
    fn level(&self, live: Live) -> &Level {
        match live {
            Live::Run => &self.0.run_active,
            Live::Queued => &self.0.run_queue_depth,
        }
    }
}

impl Drop for Active {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.level(self.live).add(-1);
        }
    }
}

#[cfg(test)]
mod tests;
