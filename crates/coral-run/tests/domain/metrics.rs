//! The Run and Tool Invocation metrics `coral-run` records at its phase
//! boundaries. Each test records to its own capture through the subscriber
//! it sets for its thread, which every Run Owner task on its current-thread
//! runtime, and every Program thread the owner starts, inherits.
use super::*;

use coral_telemetry::testing::{MetricCapture, MetricPoint};

/// A capture every Run in the calling test records to, until the guard drops.
///
/// While any thread holds a scoped default, every other thread keeps the
/// first default it reads: a thread that read before the global span
/// capture was installed would keep none, and its spans would then join
/// parents the capture never saw. So the capture is installed first.
fn measured() -> (MetricCapture, tracing::subscriber::DefaultGuard) {
    super::trace::capture();
    let capture = MetricCapture::default();
    let guard = tracing::subscriber::set_default(capture.subscriber());
    (capture, guard)
}

/// The points of `name` as their attributes, `key=value` joined by `,`, and
/// a counter's sum or a histogram's count.
fn counts(capture: &MetricCapture, name: &str) -> Vec<(String, u64)> {
    let mut counts = capture
        .named(name)
        .into_iter()
        .map(|point: MetricPoint| {
            let attributes = point
                .attributes
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(",");
            let count = if point.kind == "histogram" {
                point.count
            } else {
                point.value.to_string().parse().expect("a whole count")
            };
            (attributes, count)
        })
        .collect::<Vec<_>>();
    counts.sort();
    counts
}

/// The value of the gauge `name`.
fn level(capture: &MetricCapture, name: &str) -> Vec<f64> {
    capture
        .named(name)
        .into_iter()
        .map(|point| point.value)
        .collect()
}

fn owned(pairs: &[(&str, u64)]) -> Vec<(String, u64)> {
    pairs
        .iter()
        .map(|(attributes, count)| ((*attributes).to_string(), *count))
        .collect()
}

/// A Run counts its acceptance, its claims, each phase and its end, and
/// leaves no Run active or queued once its owner retires.
#[tokio::test]
async fn a_run_records_its_acceptance_claims_phases_and_end() {
    let (metrics, _guard) = measured();
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let (workspace_id, principal) = fixture
        .user_scope("user:run-metrics", "Measure one Run")
        .await;
    let accepted = fixture
        .runs
        .start(&principal, &workspace_id, submission("null"))
        .await
        .expect("accept the Run");
    let run = fixture
        .terminal_record(&workspace_id, accepted.run_id())
        .await;
    assert_eq!(run.state, RunState::Succeeded);
    wait_until(|| !fixture.runs.has_owner(accepted.run_id())).await;

    assert_eq!(
        counts(&metrics, "coral.run.accepted"),
        owned(&[("principal.kind=user", 1)])
    );
    assert_eq!(
        counts(&metrics, "coral.program.compile.duration"),
        owned(&[("outcome=ok", 1)])
    );
    assert_eq!(
        counts(&metrics, "coral.run.claims"),
        owned(&[
            ("operation=accept,outcome=won", 1),
            ("operation=start,outcome=won", 1)
        ])
    );
    assert_eq!(
        counts(&metrics, "coral.run.phase.duration"),
        owned(&[
            ("run.phase=complete", 1),
            ("run.phase=execute", 1),
            ("run.phase=start", 1)
        ])
    );
    assert_eq!(
        counts(&metrics, "coral.run.ended"),
        owned(&[("run.status=succeeded", 1)])
    );
    assert_eq!(
        counts(&metrics, "coral.run.duration"),
        owned(&[("run.status=succeeded", 1)])
    );
    assert_eq!(
        counts(&metrics, "coral.output.size"),
        owned(&[("output.kind=final_value", 1)])
    );
    assert_eq!(level(&metrics, "coral.run.active").last(), Some(&0.0));
    assert_eq!(level(&metrics, "coral.run.queue.depth").last(), Some(&0.0));
}

/// A Tool Invocation counts its start and end by protocol and effect, times
/// each gate, records its result's size, and leaves none active.
#[tokio::test]
async fn a_tool_invocation_records_its_gates_result_and_end() {
    let (metrics, _guard) = measured();
    let upstream = Arc::new(ScriptedUpstream::json(
        500,
        serde_json::json!({"down": true}),
    ));
    let fixture = TestModules::with_upstream(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        upstream,
    )
    .await;
    let tool = fixture
        .user_tool("user:measured-tool", "Call weather")
        .await;
    let accepted = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("invoke-tool"),
        )
        .await
        .expect("accept the Run");
    fixture
        .terminal_record(&tool.workspace_id, accepted.run_id())
        .await;
    wait_until(|| !fixture.runs.has_owner(accepted.run_id())).await;

    assert_eq!(
        counts(&metrics, "coral.invocation.started"),
        owned(&[("provider.protocol=http,tool.effect=read", 1)])
    );
    assert_eq!(
        counts(&metrics, "coral.invocation.ended"),
        owned(&[(
            "invocation.result_kind=upstream_outcome,provider.protocol=http,tool.effect=read",
            1
        )]),
        "an HTTP 500 is a complete Upstream Outcome"
    );
    assert_eq!(
        counts(&metrics, "coral.invocation.duration"),
        owned(&[(
            "invocation.result_kind=upstream_outcome,provider.protocol=http,tool.effect=read",
            1
        )])
    );
    assert_eq!(
        counts(&metrics, "coral.invocation.gate.duration"),
        owned(&[
            ("gate.kind=account", 1),
            ("gate.kind=approval", 1),
            ("gate.kind=authority", 2)
        ]),
        "authority is checked before the account gate and again before approval"
    );
    assert!(counts(&metrics, "coral.output.size").contains(&("output.kind=result".to_string(), 1)));
    assert_eq!(
        level(&metrics, "coral.invocation.active").last(),
        Some(&0.0)
    );
}

/// A Run a recovered owner takes over counts the recovery claim it won, the
/// recovery and the interruption it wrote, with its failure code.
#[tokio::test]
async fn a_recovered_run_records_its_takeover_and_end() {
    let (metrics, _guard) = measured();
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let (workspace_id, principal) = fixture
        .user_scope("user:recovered-metrics", "Recover one Run")
        .await;
    let run_id = fixture
        .staged_run(&principal, &workspace_id, "return 1;")
        .await;
    fixture
        .hand_to(&workspace_id, &run_id, "dead-holder", None)
        .await;
    let recovering = fixture.next_process();
    assert_eq!(recovering.sweep_expired_leases().await.unwrap(), 1);
    let run = fixture.terminal_record(&workspace_id, &run_id).await;
    assert_eq!(run.state, RunState::Interrupted);
    wait_until(|| !recovering.has_owner(&run_id)).await;

    assert_eq!(
        counts(&metrics, "coral.run.recovery"),
        owned(&[("outcome=ended", 1)])
    );
    assert!(
        counts(&metrics, "coral.run.claims")
            .contains(&("operation=recover,outcome=won".to_string(), 1))
    );
    assert!(
        counts(&metrics, "coral.run.phase.duration")
            .contains(&("run.phase=recover".to_string(), 1))
    );
    assert_eq!(
        counts(&metrics, "coral.run.ended"),
        owned(&[("failure.code=interrupted,run.status=interrupted", 1)])
    );
}

/// A cancelled Run counts its Program as terminated for the cancellation,
/// and ends canceled, which carries no failure code.
#[tokio::test]
async fn a_canceled_run_records_its_program_termination() {
    let (metrics, _guard) = measured();
    let engine = Arc::new(StopAwareEngine::default());
    let fixture = TestModules::new(engine.clone()).await;
    let (workspace_id, principal) = fixture
        .user_scope("user:canceled-metrics", "Cancel one Run")
        .await;
    let accepted = fixture
        .runs
        .start(&principal, &workspace_id, submission("until-canceled"))
        .await
        .expect("accept the Run");
    wait_until(|| engine.started.load(Ordering::SeqCst)).await;
    fixture
        .runs
        .cancel(
            &principal,
            &workspace_id,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .expect("cancel the Run");
    let run = fixture
        .terminal_record(&workspace_id, accepted.run_id())
        .await;
    assert_eq!(run.state, RunState::Canceled);
    wait_until(|| !fixture.runs.has_owner(accepted.run_id())).await;

    assert_eq!(
        counts(&metrics, "coral.program.termination"),
        owned(&[("outcome=canceled", 1)])
    );
    assert_eq!(
        counts(&metrics, "coral.run.ended"),
        owned(&[("run.status=canceled", 1)])
    );
}

/// A Program's thread records what its Run keeps and rejects: each Emission
/// and Log by size, and the Emission over the Run's output budget as a
/// rejection.
#[tokio::test]
async fn program_output_records_what_the_run_keeps_and_rejects() {
    let (metrics, _guard) = measured();
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let (workspace_id, principal) = fixture
        .user_scope("user:output-metrics", "Measure output")
        .await;
    // The log keeps two UTF-8 bytes and each Emission four, so the 64th
    // Emission is over the budget.
    let runs = fixture
        .live_runs(None)
        .with_value_limits(coral_value::ValueLimits::new(5, 1024, 4096, 257).expect("limits"));
    let accepted = runs
        .start(
            &principal,
            &workspace_id,
            submission("console.log('é'); for (let i = 0; i < 64; i++) await coral.emit('é');"),
        )
        .await
        .expect("accept the Run");
    wait_for_terminal(&runs, &principal, &workspace_id, accepted.run_id()).await;

    assert_eq!(
        counts(&metrics, "coral.output.size"),
        owned(&[("output.kind=emission", 63), ("output.kind=log", 1)])
    );
    assert_eq!(
        counts(&metrics, "coral.output.rejections"),
        owned(&[("output.kind=emission", 1)])
    );
}
