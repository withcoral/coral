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
    AcquisitionOperation, AcquisitionOutcome, CompactionOutcome, CompileOutcome,
    CompilerBuildOutcome, CompilerBuildReason, CompilerOperation, CompilerOutcome, CompilerRole,
    DbSystem, DropReason, EmbedBatchOutcome, GateKind, Interface, InvocationEnd, ModelCallOutcome,
    OutputKind, PrincipalKind, ProviderProtocol, RequestOutcome, RunExecutorKind, RunPhase,
    RunStatus, SecretStoreRead, SignalKind, SubAgentTurnOutcome, TerminationReason, TokenType,
    ToolEffect, TransactionOutcome, UpstreamOutcome, VisibilityPath,
};

/// Histogram bounds for a duration in seconds, from 5 ms to an hour.
const SECONDS: [f64; 18] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
    1800.0, 3600.0,
];

/// Fine request CPU boundaries extend the shared duration range below 5 ms.
const CPU_FINE_SECONDS: [f64; 8] = [
    0.000_01, 0.000_025, 0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.0025,
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

/// Histogram bounds for the model calls of one Sub-agent Turn, up to its 60.
const MODEL_CALLS: [f64; 9] = [1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 30.0, 45.0, 60.0];

/// Histogram bounds for tokens, as OpenTelemetry's conventions for generative AI advise.
const TOKENS: [f64; 14] = [
    1.0,
    4.0,
    16.0,
    64.0,
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
    request_cpu_duration: Histogram<f64>,
    program_compile_duration: Histogram<f64>,
    compiler_request_duration: Histogram<f64>,
    compiler_queue_duration: Histogram<f64>,
    compiler_work_duration: Histogram<f64>,
    compiler_queue_depth: LabeledLevel<CompilerRole>,
    compiler_active: LabeledLevel<CompilerRole>,
    compiler_isolate_build_duration: Histogram<f64>,
    program_execution_cpu_duration: Histogram<f64>,
    program_execution_heap_peak: Histogram<u64>,
    program_termination: Counter<u64>,
    run_accepted: Counter<u64>,
    run_ended: Counter<u64>,
    run_duration: Histogram<f64>,
    run_phase_duration: Histogram<f64>,
    run_active: Level,
    run_queue: RunQueueGauges,
    run_executor_linked: LabeledLevel<RunExecutorKind>,
    invocation_started: Counter<u64>,
    invocation_ended: Counter<u64>,
    invocation_duration: Histogram<f64>,
    invocation_gate_duration: Histogram<f64>,
    invocation_active: Level,
    upstream_duration: Histogram<f64>,
    upstream_request_size: Histogram<u64>,
    upstream_response_size: Histogram<u64>,
    persistence_acquisition_duration: Histogram<f64>,
    persistence_acquisition_waiters: LabeledLevel<(DbSystem, AcquisitionOperation)>,
    persistence_transaction_duration: Histogram<f64>,
    persistence_transaction_failures: Counter<u64>,
    pools: Pools,
    _persistence_pool_in_use: ObservableGauge<u64>,
    search_backlogs: SearchSources,
    _search_backlog: ObservableGauge<u64>,
    _search_sample_age: ObservableGauge<f64>,
    _search_pending_age: ObservableGauge<f64>,
    _search_embed_backlog: ObservableGauge<u64>,
    _search_embed_skipped: ObservableGauge<u64>,
    search_embed_batch_duration: Histogram<f64>,
    search_publication_visibility_duration: Histogram<f64>,
    search_index_recovery_duration: Histogram<f64>,
    output_size: Histogram<u64>,
    output_rejections: Counter<u64>,
    secret_store_read_duration: Histogram<f64>,
    sub_agent_turn_ended: Counter<u64>,
    sub_agent_turn_duration: Histogram<f64>,
    sub_agent_turn_active: Level,
    sub_agent_turn_model_calls: Histogram<u64>,
    sub_agent_model_call_duration: Histogram<f64>,
    sub_agent_model_call_tokens: Histogram<u64>,
    sub_agent_compactions: Counter<u64>,
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

/// A bounded gauge family keyed by catalog enums. Attribute conversion runs
/// only during collection, not on the acquisition or compiler hot path.
#[derive(Debug)]
struct LabeledLevel<K> {
    values: Arc<Mutex<std::collections::BTreeMap<K, i64>>>,
    _gauge: ObservableGauge<i64>,
}
impl<K: Ord + Copy + Send + 'static> LabeledLevel<K> {
    fn new(
        meter: &Meter,
        name: &'static str,
        unit: &'static str,
        description: &'static str,
        attributes: fn(K) -> Vec<KeyValue>,
    ) -> Self {
        let values = Arc::new(Mutex::new(std::collections::BTreeMap::<K, i64>::new()));
        let observed = Arc::clone(&values);
        let gauge = meter
            .i64_observable_gauge(name)
            .with_unit(unit)
            .with_description(description)
            .with_callback(move |gauge| {
                let values = observed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                for (key, value) in values.iter() {
                    gauge.observe(*value, &attributes(*key));
                }
            })
            .build();
        Self {
            values,
            _gauge: gauge,
        }
    }
    fn add(&self, key: K, delta: i64) {
        let mut values = self
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *values.entry(key).or_default() += delta;
    }
}

/// Measures one caller's wait for a connection and BEGIN. Dropping it before
/// an answer records cancellation and releases the waiter, never a transaction.
#[derive(Debug)]
pub struct DatabaseAcquisition {
    metrics: Option<Metrics>,
    system: DbSystem,
    operation: AcquisitionOperation,
    outcome: AcquisitionOutcome,
    started: Instant,
}

/// Starts one caller's acquisition wait. A detached backend BEGIN may outlive a
/// canceled caller; the gauge counts callers, not detached database tasks.
#[must_use = "the acquisition counts as waiting until the guard ends"]
pub fn database_acquiring(
    system: DbSystem,
    operation: AcquisitionOperation,
) -> DatabaseAcquisition {
    let guard = DatabaseAcquisition {
        metrics: current(),
        system,
        operation,
        outcome: AcquisitionOutcome::Canceled,
        started: Instant::now(),
    };
    if let Some(metrics) = &guard.metrics {
        metrics
            .0
            .persistence_acquisition_waiters
            .add((guard.system, guard.operation), 1);
    }
    guard
}

impl DatabaseAcquisition {
    /// Ends the wait on its definite answer, successful or failed.
    pub fn finish<T, E>(mut self, result: &Result<T, E>) {
        self.outcome = if result.is_ok() {
            AcquisitionOutcome::Ready
        } else {
            AcquisitionOutcome::Failed
        };
    }
}

impl Drop for DatabaseAcquisition {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics
                .0
                .persistence_acquisition_waiters
                .add((self.system, self.operation), -1);
            metrics.0.persistence_acquisition_duration.record(
                self.started.elapsed().as_secs_f64(),
                &[
                    KeyValue::new("db.system", self.system.as_str()),
                    KeyValue::new("operation", self.operation.as_str()),
                    KeyValue::new("outcome", self.outcome.as_str()),
                ],
            );
        }
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
        let search_backlogs = SearchSources::default();
        Self(Arc::new(Catalog {
            request_cpu_duration: meter.f64_histogram("coral.request.cpu.duration")
                .with_unit("s")
                .with_description("CPU spent polling the request handler and response body, including inline-awaited work; excludes pending wait time, spawned tasks and dedicated workers. Omitted without a supported thread CPU clock.")
                .with_boundaries([CPU_FINE_SECONDS.as_slice(), SECONDS.as_slice()].concat())
                .build(),
            request_duration: seconds(
                "coral.request.duration",
                "How long an inbound request took, until its response ended.",
            ),
            compiler_request_duration: seconds(
                "coral.compiler.request.duration",
                "One compiler request from enqueue attempt to caller answer, including Private Tool authoring.",
            ),
            compiler_queue_duration: seconds(
                "coral.compiler.queue.duration",
                "Time from enqueue attempt until the worker dequeues accepted work.",
            ),
            compiler_work_duration: seconds(
                "coral.compiler.work.duration",
                "Time the compiler worker spends handling one dequeued request. It includes building an isolate only when an earlier rebuild failed.",
            ),
            compiler_isolate_build_duration: seconds(
                "coral.compiler.isolate.build.duration",
                "Time the compiler worker spends building an isolate to replace a discarded one.",
            ),
            compiler_queue_depth: LabeledLevel::new(
                &meter,
                "coral.compiler.queue.depth",
                "{request}",
                "Compiler requests waiting to be dequeued, including enqueue attempts until accepted or rejected.",
                |role: CompilerRole| vec![KeyValue::new("compiler.role", role.as_str())],
            ),
            compiler_active: LabeledLevel::new(
                &meter,
                "coral.compiler.active",
                "{request}",
                "Requests the compiler worker is handling.",
                |role: CompilerRole| vec![KeyValue::new("compiler.role", role.as_str())],
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
                "Program Runs whose Run stream is served on this Coral Instance.",
            ),
            run_queue: RunQueueGauges::new(&meter),
            run_executor_linked: LabeledLevel::new(
                &meter,
                "coral.run_executor.linked",
                "{stream}",
                "Pull streams of Run Executors that this Coral Instance holds, by the credential that admitted them. A stream that closes at its connection age counts until it ends, so a Run Executor that redials here counts twice for a moment.",
                |kind: RunExecutorKind| vec![KeyValue::new("run_executor.kind", kind.as_str())],
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
            persistence_acquisition_duration: seconds(
                "coral.persistence.acquisition.duration",
                "Time awaiting a pooled connection and BEGIN, including canceled and failed waits.",
            ),
            persistence_acquisition_waiters: LabeledLevel::new(
                &meter,
                "coral.persistence.acquisition.waiters",
                "{request}",
                "Callers awaiting a pooled connection, plus BEGIN for transaction entry points.",
                |(system, operation): (DbSystem, AcquisitionOperation)| {
                    vec![
                        KeyValue::new("db.system", system.as_str()),
                        KeyValue::new("operation", operation.as_str()),
                    ]
                },
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
            _search_backlog: {
                let sources = Arc::clone(&search_backlogs);
                meter.u64_observable_gauge("coral.search.index.backlog").with_unit("{projection}")
                    .with_description("Current projections missing or stale at the last successful read snapshot, including failed retries not yet due. Compare sample.age.")
                    .with_callback(move |gauge| {
                        let snapshots = search_snapshots(&sources);
                        if !snapshots.is_empty() {
                            gauge.observe(snapshots.iter().map(|s| s.generations).sum(), &[KeyValue::new("search.projection.kind", "provider_catalog")]);
                            gauge.observe(snapshots.iter().map(|s| s.private_tools).sum(), &[KeyValue::new("search.projection.kind", "private_tool")]);
                            gauge.observe(snapshots.iter().map(|s| s.shared_tools).sum(), &[KeyValue::new("search.projection.kind", "shared_tool")]);
                        }
                    }).build()
            },
            _search_sample_age: {
                let sources = Arc::clone(&search_backlogs);
                meter.f64_observable_gauge("coral.search.index.backlog.sample.age").with_unit("s")
                    .with_description("Age of the oldest successful backlog snapshot still reporting on this Instance.")
                    .with_callback(move |gauge| {
                        if let Some(age) = search_snapshots(&sources).iter().map(|s| s.sample_age).max() { gauge.observe(age.as_secs_f64(), &[]); }
                    }).build()
            },
            _search_pending_age: {
                let sources = Arc::clone(&search_backlogs);
                meter.f64_observable_gauge("coral.search.index.first_visibility.age").with_unit("s")
                    .with_description("Age of the oldest catalog publication never yet searchable; zero when none, absent for unknown visibility history or clock. Rebuilds are excluded.")
                    .with_callback(move |gauge| {
                        let snapshots = search_snapshots(&sources);
                        if snapshots.is_empty() { return; }
                        let Some(waiting) = snapshots.iter().map(|s| s.first_visibility.as_ref()).collect::<Option<Vec<_>>>() else { return; };
                        if waiting.iter().all(|s| s.waiting == 0) { gauge.observe(0.0, &[]); }
                        else if waiting.iter().all(|s| s.waiting == 0 || s.oldest_age.is_some())
                            && let Some(age) = waiting.iter().filter_map(|s| s.oldest_age).max() { gauge.observe(age.as_secs_f64(), &[]); }
                    }).build()
            },
            _search_embed_backlog: {
                let sources = Arc::clone(&search_backlogs);
                meter.u64_observable_gauge("coral.search.embed.backlog").with_unit("{entity}")
                    .with_description("Search Entities without a vector of the current Embedder revision made from their current text, rows on the skip list included. Absent while the vector side is off or the count is unknown.")
                    .with_callback(move |gauge| {
                        if let Some(embedders) = embedder_snapshots(&sources) {
                            gauge.observe(embedders.iter().map(|e| e.stale).sum(), &[]);
                        }
                    }).build()
            },
            _search_embed_skipped: {
                let sources = Arc::clone(&search_backlogs);
                meter.u64_observable_gauge("coral.search.embed.skipped").with_unit("{entity}")
                    .with_description("Search Entities on the Embedder's skip list: their text failed while other rows embedded. A row leaves the list when it changes or the process restarts.")
                    .with_callback(move |gauge| {
                        if let Some(embedders) = embedder_snapshots(&sources) {
                            gauge.observe(embedders.iter().map(|e| e.skipped).sum(), &[]);
                        }
                    }).build()
            },
            search_backlogs,
            search_embed_batch_duration: seconds(
                "coral.search.embed.batch.duration",
                "One write of the search Embedder: reading a batch's text, embedding it and writing its vectors.",
            ),
            search_publication_visibility_duration: seconds(
                "coral.search.publication.visibility.duration",
                "Stored catalog publication time to its first successful index commit. Initial and retried attempts are separate; rebuilds and unknown legacy history record nothing.",
            ),
            search_index_recovery_duration: seconds(
                "coral.search.index.recovery.duration",
                "Time from a previously searchable catalog becoming unavailable to its successful recovery commit. Repeated failures preserve the outage start.",
            ),
            output_size: bytes(
                "coral.output.size",
                "The size of one output a Program Run kept.",
            ),
            output_rejections: counter(
                "coral.output.rejections",
                "{output}",
                "Outputs a Program Run could not keep, because they were too large or invalid.",
            ),
            secret_store_read_duration: seconds(
                "coral.secret_store.read.duration",
                "How long one read from a Secret Store took, by its outcome.",
            ),
            sub_agent_turn_ended: counter(
                "coral.sub_agent.turn.ended",
                "{turn}",
                "Sub-agent Turns that ended.",
            ),
            sub_agent_turn_duration: seconds(
                "coral.sub_agent.turn.duration",
                "How long a Sub-agent Turn took, from its start to its end.",
            ),
            sub_agent_turn_active: Level::new(
                &meter,
                "coral.sub_agent.turn.active",
                "{turn}",
                "Sub-agent Turns running on this Coral Instance.",
            ),
            sub_agent_turn_model_calls: meter
                .u64_histogram("coral.sub_agent.turn.model_calls")
                .with_unit("{call}")
                .with_description("The model calls one Sub-agent Turn made.")
                .with_boundaries(MODEL_CALLS.to_vec())
                .build(),
            sub_agent_model_call_duration: seconds(
                "coral.sub_agent.model_call.duration",
                "How long one call to a Sub-agent's model took, its retries included.",
            ),
            sub_agent_model_call_tokens: meter
                .u64_histogram("coral.sub_agent.model_call.tokens")
                .with_unit("{token}")
                .with_description(
                    "The tokens one call to a Sub-agent's model used, by type. Input excludes cached input.",
                )
                .with_boundaries(TOKENS.to_vec())
                .build(),
            sub_agent_compactions: counter(
                "coral.sub_agent.compactions",
                "{compaction}",
                "Compactions of a Sub-agent's thread, done or skipped.",
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

    /// Records CPU spent directly polling an inbound request and response body.
    pub(crate) fn request_cpu(
        &self,
        interface: Interface,
        operation: String,
        outcome: RequestOutcome,
        elapsed: Duration,
    ) {
        self.0.request_cpu_duration.record(
            elapsed.as_secs_f64(),
            &[
                KeyValue::new("interface", interface.as_str()),
                KeyValue::new("operation", operation),
                KeyValue::new("outcome", outcome.as_str()),
            ],
        );
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

/// Whether the current dispatcher has a metric catalog. Domain code can avoid
/// optional observation work when metrics are disabled.
#[must_use]
pub fn enabled() -> bool {
    current().is_some()
}

fn with(record: impl FnOnce(&Catalog)) {
    if let Some(metrics) = current() {
        record(&metrics.0);
    }
}

fn failure_code(code: Option<&'static str>) -> Option<KeyValue> {
    code.map(|code| KeyValue::new("failure.code", code))
}

/// Records the caller's answer, separately from worker execution. A caller
/// may time out while its job remains queued; the queue counts the actual job.
pub fn compiler_requested(
    role: CompilerRole,
    operation: CompilerOperation,
    outcome: CompilerOutcome,
    elapsed: Duration,
) {
    with(|catalog| {
        catalog.compiler_request_duration.record(
            elapsed.as_secs_f64(),
            &[
                KeyValue::new("compiler.role", role.as_str()),
                KeyValue::new("operation", operation.as_str()),
                KeyValue::new("outcome", outcome.as_str()),
            ],
        );
    });
}

/// Records one build of a compiler isolate that replaces a discarded one.
/// The worker builds after the job that discarded it has answered, or, for
/// [`CompilerBuildReason::Retry`], inside the next request.
pub fn compiler_isolate_built(
    role: CompilerRole,
    reason: CompilerBuildReason,
    outcome: CompilerBuildOutcome,
    elapsed: Duration,
) {
    with(|catalog| {
        catalog.compiler_isolate_build_duration.record(
            elapsed.as_secs_f64(),
            &[
                KeyValue::new("compiler.role", role.as_str()),
                KeyValue::new("build.reason", reason.as_str()),
                KeyValue::new("outcome", outcome.as_str()),
            ],
        );
    });
}

/// Tracks a compiler job across the queue and worker thread. It records to
/// the requester's catalog, which can differ from the compiler thread's.
#[derive(Debug)]
pub struct CompilerActivity {
    metrics: Option<Metrics>,
    operation: CompilerOperation,
    role: CompilerRole,
    working: bool,
    outcome: CompilerOutcome,
    started: Instant,
}

/// Starts an enqueue attempt; a rejected job drops immediately and records no
/// queue duration. A timed-out caller's accepted job stays counted until dequeued.
#[must_use]
pub fn compiler_queued(role: CompilerRole, operation: CompilerOperation) -> CompilerActivity {
    let activity = CompilerActivity {
        metrics: current(),
        operation,
        role,
        working: false,
        outcome: CompilerOutcome::Stopped,
        started: Instant::now(),
    };
    if let Some(metrics) = &activity.metrics {
        metrics.0.compiler_queue_depth.add(role, 1);
    }
    activity
}

impl CompilerActivity {
    /// Leaves the queue and begins handling this job on the compiler worker.
    #[must_use]
    pub fn dequeued(mut self) -> Self {
        if let Some(metrics) = &self.metrics {
            metrics.0.compiler_queue_depth.add(self.role, -1);
            metrics.0.compiler_queue_duration.record(
                self.started.elapsed().as_secs_f64(),
                &[
                    KeyValue::new("compiler.role", self.role.as_str()),
                    KeyValue::new("operation", self.operation.as_str()),
                ],
            );
            metrics.0.compiler_active.add(self.role, 1);
        }
        self.working = true;
        self.started = Instant::now();
        self
    }

    /// Ends worker handling with its actual result, which can differ from a
    /// caller that already timed out.
    pub fn finish(mut self, outcome: CompilerOutcome) {
        self.outcome = outcome;
    }
}

impl Drop for CompilerActivity {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            if self.working {
                metrics.0.compiler_active.add(self.role, -1);
                metrics.0.compiler_work_duration.record(
                    self.started.elapsed().as_secs_f64(),
                    &[
                        KeyValue::new("compiler.role", self.role.as_str()),
                        KeyValue::new("operation", self.operation.as_str()),
                        KeyValue::new("outcome", self.outcome.as_str()),
                    ],
                );
            } else {
                metrics.0.compiler_queue_depth.add(self.role, -1);
            }
        }
    }
}

/// A completed visibility transition, not a rewrite of an already-complete index.
pub fn publication_visible(path: VisibilityPath, elapsed: Duration) {
    with(|catalog| {
        catalog.search_publication_visibility_duration.record(
            elapsed.as_secs_f64(),
            &[KeyValue::new("outcome", path.as_str())],
        );
    });
}

/// A completed recovery of a previously searchable Provider catalog.
pub fn index_recovered(elapsed: Duration) {
    with(|catalog| {
        catalog
            .search_index_recovery_duration
            .record(elapsed.as_secs_f64(), &[]);
    });
}

/// One write of the search Embedder that ended `outcome` after `elapsed`.
pub fn embed_batch_ended(outcome: EmbedBatchOutcome, elapsed: Duration) {
    with(|catalog| {
        catalog.search_embed_batch_duration.record(
            elapsed.as_secs_f64(),
            &[KeyValue::new("outcome", outcome.as_str())],
        );
    });
}

/// Known first-visibility work, separate from rebuild backlog.
#[derive(Clone, Copy)]
pub struct FirstVisibilityBacklog {
    /// Catalog publications whose first projection has not committed.
    pub waiting: u64,
    /// Age of the oldest such publication, absent when its clock is unknown.
    pub oldest_age: Option<Duration>,
}

/// A read-only projection snapshot. Ages advance monotonically after the
/// snapshot; the sample age lets operators distinguish backlog from stale data.
#[derive(Clone, Copy)]
pub struct SearchBacklog {
    /// Active Provider Generations without a current projection.
    pub generations: u64,
    /// Current Private Tool revisions without a current projection.
    pub private_tools: u64,
    /// Live Shared Tools whose current revision has no current projection.
    pub shared_tools: u64,
    /// None when a legacy failed projection has unknown visibility history.
    pub first_visibility: Option<FirstVisibilityBacklog>,
    /// Time since the snapshot was read successfully.
    pub sample_age: Duration,
    /// The Embedder's work; `None` while the vector side is off.
    pub embedder: Option<EmbedderBacklog>,
}

/// The search Embedder's work in one snapshot.
#[derive(Clone, Copy)]
pub struct EmbedderBacklog {
    /// Search Entities without a vector of the current Embedder revision
    /// made from their current text.
    pub stale: u64,
    /// Search Entities on the Embedder's skip list.
    pub skipped: u64,
}

type SearchSources = Arc<Mutex<Vec<Weak<SearchSource>>>>;
struct SearchSource {
    read: Box<dyn Fn() -> Option<SearchBacklog> + Send + Sync>,
}

/// Keeps one index's snapshot observable. Dropping it removes the source.
pub struct SearchObservation(
    #[expect(dead_code, reason = "keeps the weak source alive")] Arc<SearchSource>,
);

/// Registers a fast, in-memory snapshot reader. No database access or export
/// work belongs in the callback. No values are observed before a successful read.
pub fn observe_search_backlog(
    read: impl Fn() -> Option<SearchBacklog> + Send + Sync + 'static,
) -> Option<SearchObservation> {
    let metrics = current()?;
    let source = Arc::new(SearchSource {
        read: Box::new(read),
    });
    metrics
        .0
        .search_backlogs
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(Arc::downgrade(&source));
    Some(SearchObservation(source))
}

fn search_snapshots(sources: &SearchSources) -> Vec<SearchBacklog> {
    let sources = {
        let mut sources = sources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        sources.retain(|source| source.strong_count() > 0);
        sources.iter().filter_map(Weak::upgrade).collect::<Vec<_>>()
    };
    sources
        .iter()
        .map(|source| (source.read)())
        .collect::<Option<Vec<_>>>()
        .unwrap_or_default()
}

/// The Embedder parts of every live snapshot, or `None` when none has one.
fn embedder_snapshots(sources: &SearchSources) -> Option<Vec<EmbedderBacklog>> {
    let embedders = search_snapshots(sources)
        .iter()
        .filter_map(|snapshot| snapshot.embedder)
        .collect::<Vec<_>>();
    (!embedders.is_empty()).then_some(embedders)
}

impl fmt::Debug for SearchSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SearchSource")
            .finish_non_exhaustive()
    }
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
pub fn program_executed(cpu: Option<Duration>, heap_peak_bytes: usize) {
    with(|catalog| {
        if let Some(cpu) = cpu {
            catalog
                .program_execution_cpu_duration
                .record(cpu.as_secs_f64(), &[]);
        }
        catalog
            .program_execution_heap_peak
            .record(byte_size(heap_peak_bytes), &[]);
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

/// The Run queue as one read snapshot of the database found it.
#[derive(Debug, Clone, Copy)]
pub struct RunQueue {
    /// Runs accepted and not yet taken.
    pub waiting: u64,
    /// How long the oldest of them had waited; zero when none waits.
    pub waiting_age: Duration,
    /// Runs taken and not yet ended, started or not.
    pub held: u64,
}

/// The Run queue gauges, which report the last reading of the queue pass.
#[derive(Debug)]
struct RunQueueGauges {
    last: Arc<Mutex<Option<RunQueue>>>,
    _gauges: (
        ObservableGauge<u64>,
        ObservableGauge<f64>,
        ObservableGauge<u64>,
    ),
}

impl RunQueueGauges {
    fn new(meter: &Meter) -> Self {
        let last = Arc::<Mutex<Option<RunQueue>>>::default();
        let description = |what: &str| {
            format!(
                "{what}, at the last read. Every Coral Instance reads the same database: \
                 take the maximum across instances, not the sum."
            )
        };
        let read = || {
            let last = Arc::clone(&last);
            move || {
                *last
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
            }
        };
        let count = |name: &'static str, what: &str, value: fn(RunQueue) -> u64| {
            let read = read();
            meter
                .u64_observable_gauge(name)
                .with_unit("{run}")
                .with_description(description(what))
                .with_callback(move |gauge| {
                    if let Some(queue) = read() {
                        gauge.observe(value(queue), &[]);
                    }
                })
                .build()
        };
        let age = {
            let read = read();
            meter
                .f64_observable_gauge("coral.run.waiting.age")
                .with_unit("s")
                .with_description(description(
                    "How long the oldest Program Run not yet taken had waited, zero when none waits",
                ))
                .with_callback(move |gauge| {
                    if let Some(queue) = read() {
                        gauge.observe(queue.waiting_age.as_secs_f64(), &[]);
                    }
                })
                .build()
        };
        Self {
            _gauges: (
                count(
                    "coral.run.waiting",
                    "Program Runs accepted and not yet taken by a Run Executor",
                    |queue| queue.waiting,
                ),
                age,
                count(
                    "coral.run.held",
                    "Program Runs taken by a Run Executor and not yet ended, started or not",
                    |queue| queue.held,
                ),
            ),
            last,
        }
    }
}

/// Reports `queue` on the Run queue gauges until the next reading. `None`,
/// a failed read, reports nothing: the queue is unknown, not empty. The
/// gauges' callbacks read only this value, never the database.
pub fn run_queue_read(queue: Option<RunQueue>) {
    with(|catalog| {
        *catalog
            .run_queue
            .last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = queue;
    });
}

/// Counts one pull stream of a Run Executor of `kind` that this Coral
/// Instance holds until the returned level drops.
#[must_use = "the stream counts as linked until the level drops"]
pub fn run_executor_linked(kind: RunExecutorKind) -> LevelGuard {
    LevelGuard::start(Live::RunExecutor(kind))
}

/// Counts one Run stream loop live on this Coral Instance until the
/// returned level drops.
#[must_use = "the Run counts as active until the level drops"]
pub fn run_active() -> LevelGuard {
    LevelGuard::start(Live::Run)
}

/// Records one read from a Secret Store. Nothing here carries the store's name,
/// a path, a key or a value.
pub fn secret_store_read(kind: &'static str, outcome: SecretStoreRead, elapsed: Duration) {
    with(|catalog| {
        catalog.secret_store_read_duration.record(
            elapsed.as_secs_f64(),
            &[
                KeyValue::new("secret_store.kind", kind),
                KeyValue::new("outcome", outcome.as_str()),
            ],
        );
    });
}

/// Counts one Sub-agent Turn running on this Coral Instance until the
/// returned level drops.
#[must_use = "the Turn counts as running until the level drops"]
pub fn sub_agent_turn_active() -> LevelGuard {
    LevelGuard::start(Live::SubAgentTurn)
}

/// Records a Sub-agent Turn that ended `outcome`, with its failure code,
/// `elapsed` after it started and after `model_calls` model calls.
pub fn sub_agent_turn_ended(
    outcome: SubAgentTurnOutcome,
    code: Option<&'static str>,
    elapsed: Duration,
    model_calls: u64,
) {
    with(|catalog| {
        let outcome = KeyValue::new("sub_agent.turn.outcome", outcome.as_str());
        let ended = std::iter::once(outcome.clone())
            .chain(failure_code(code))
            .collect::<Vec<_>>();
        catalog.sub_agent_turn_ended.add(1, &ended);
        catalog
            .sub_agent_turn_duration
            .record(elapsed.as_secs_f64(), &[outcome]);
        catalog.sub_agent_turn_model_calls.record(model_calls, &[]);
    });
}

/// Records one call to a Sub-agent's model that took `elapsed`, its retries
/// included, and the tokens it used, by type.
pub fn sub_agent_model_called(
    outcome: ModelCallOutcome,
    elapsed: Duration,
    tokens: &[(TokenType, u64)],
) {
    with(|catalog| {
        catalog.sub_agent_model_call_duration.record(
            elapsed.as_secs_f64(),
            &[KeyValue::new("outcome", outcome.as_str())],
        );
        for (token_type, count) in tokens {
            catalog.sub_agent_model_call_tokens.record(
                *count,
                &[KeyValue::new("gen_ai.token.type", token_type.as_str())],
            );
        }
    });
}

/// Counts one compaction of a Sub-agent's thread, done or skipped.
pub fn sub_agent_compacted(outcome: CompactionOutcome) {
    with(|catalog| {
        catalog
            .sub_agent_compactions
            .add(1, &[KeyValue::new("outcome", outcome.as_str())]);
    });
}

/// Counts a Tool Invocation admitted for a Tool of `protocol` and `effect`,
/// and measures it until [`InvocationMeasure::end`], or until it drops, which
/// records a host defect.
#[must_use = "the Invocation is measured until it ends"]
pub fn invocation_started(protocol: ProviderProtocol, effect: ToolEffect) -> InvocationMeasure {
    let metrics = current();
    let attributes = [
        KeyValue::new("provider.protocol", protocol.as_str()),
        KeyValue::new("tool.effect", effect.as_str()),
    ];
    if let Some(metrics) = &metrics {
        metrics.0.invocation_started.add(1, &attributes);
        metrics.0.invocation_active.add(1);
    }
    InvocationMeasure {
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
pub struct InvocationMeasure {
    metrics: Option<Metrics>,
    attributes: [KeyValue; 2],
    started: Instant,
}

impl InvocationMeasure {
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

impl Drop for InvocationMeasure {
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
    request_bytes: Option<usize>,
    response_bytes: Option<usize>,
) {
    with(|catalog| {
        let protocol = || KeyValue::new("provider.protocol", protocol.as_str());
        catalog.upstream_duration.record(
            elapsed.as_secs_f64(),
            &[protocol(), KeyValue::new("outcome", outcome.as_str())],
        );
        if let Some(size) = request_bytes {
            catalog
                .upstream_request_size
                .record(byte_size(size), &[protocol()]);
        }
        if let Some(size) = response_bytes {
            catalog
                .upstream_response_size
                .record(byte_size(size), &[protocol()]);
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
pub fn output_kept(kind: OutputKind, size: usize) {
    with(|catalog| {
        catalog.output_size.record(
            byte_size(size),
            &[KeyValue::new("output.kind", kind.as_str())],
        );
    });
}

/// Counts one output of `kind` a Program Run could not keep.
pub fn output_rejected(kind: OutputKind) {
    outputs_rejected(kind, 1);
}

/// Counts `count` outputs of `kind` a Program Run could not keep.
pub fn outputs_rejected(kind: OutputKind, count: u64) {
    with(|catalog| {
        catalog
            .output_rejections
            .add(count, &[KeyValue::new("output.kind", kind.as_str())]);
    });
}

/// `size` as a histogram records bytes.
fn byte_size(size: usize) -> u64 {
    u64::try_from(size).unwrap_or(u64::MAX)
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

/// What a [`LevelGuard`] counts.
#[derive(Debug, Clone, Copy)]
enum Live {
    Run,
    SubAgentTurn,
    RunExecutor(RunExecutorKind),
}

/// Counts one live thing on its gauge until it drops.
#[derive(Debug)]
pub struct LevelGuard {
    metrics: Option<Metrics>,
    live: Live,
}

impl LevelGuard {
    fn start(live: Live) -> Self {
        let metrics = current();
        if let Some(metrics) = &metrics {
            metrics.add(live, 1);
        }
        Self { metrics, live }
    }
}

impl Metrics {
    fn add(&self, live: Live, delta: i64) {
        match live {
            Live::Run => self.0.run_active.add(delta),
            Live::SubAgentTurn => self.0.sub_agent_turn_active.add(delta),
            Live::RunExecutor(kind) => self.0.run_executor_linked.add(kind, delta),
        }
    }
}

impl Drop for LevelGuard {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.add(self.live, -1);
        }
    }
}

#[cfg(test)]
mod tests;
