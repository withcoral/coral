//! The Run and Tool Invocation metrics `coral-run` records at its phase
//! boundaries. Each test records to its own capture through the subscriber
//! it sets for its thread, which every Run stream loop task on its
//! current-thread runtime, and every thread a loop starts, inherits.
use super::*;

use super::run_streams::{Scripted, finish_terminated, take};
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

/// A Run counts its acceptance, each phase and its end, and
/// leaves no Run active once its stream's loop retires.
#[tokio::test]
async fn a_run_records_its_acceptance_phases_and_end() {
    let (metrics, _guard) = measured();
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.live(None).await;
    let (workspace_id, principal) = fixture
        .user_scope("user:run-metrics", "Measure one Run")
        .await;
    let accepted = runs
        .start(&principal, &workspace_id, submission("return null;"))
        .await
        .expect("accept the Run");
    let run = fixture
        .terminal_record(&workspace_id, accepted.run_id())
        .await;
    assert_eq!(run.state, RunState::Succeeded);
    wait_until(|| !runs.has_owner(accepted.run_id())).await;

    assert_eq!(
        counts(&metrics, "coral.run.accepted"),
        owned(&[("principal.kind=user", 1)])
    );
    assert_eq!(
        counts(&metrics, "coral.program.compile.duration"),
        owned(&[("outcome=ok", 1)])
    );
    assert_eq!(
        counts(&metrics, "coral.run.phase.duration"),
        owned(&[("run.phase=complete", 1), ("run.phase=execute", 1)])
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
}

/// Each compile failure keeps its cause for the caller and the metric, at
/// both compile stages, and a failed admission creates no Run.
#[tokio::test]
async fn a_compile_failure_keeps_its_cause_for_the_caller_and_the_metric() {
    let (metrics, _guard) = measured();
    let fixture = TestModules::new(Arc::new(FailingCompileEngine)).await;
    let (workspace_id, principal) = fixture
        .user_scope("user:compile-outcomes", "Classify compile failures")
        .await;
    let runs_before = fixture.runs.count_runs().await.expect("count Runs");

    for stage in ["analysis", "compile"] {
        for (failure, kind, rejection) in [
            (
                "rejected",
                "program_admission",
                Some("program_admission_failed"),
            ),
            ("busy", "compiler_busy", Some("program_compiler_busy")),
            (
                "timed-out",
                "compilation_timed_out",
                Some("program_compilation_timed_out"),
            ),
            ("stopped", "program_worker_stopped", None),
            (
                "no-compiler",
                "compiler_unavailable",
                Some("program_compiler_unavailable"),
            ),
            ("failed", "program_runtime_unavailable", None),
        ] {
            let error = fixture
                .runs
                .start(
                    &principal,
                    &workspace_id,
                    submission(&format!("{stage}:{failure}")),
                )
                .await
                .expect_err("the compile fails");
            assert_eq!(error.code(), kind, "{stage}:{failure}");
            assert_eq!(
                error.safe_rejection().map(|(code, _message)| code),
                rejection,
                "{stage}:{failure}"
            );
        }
    }

    assert_eq!(
        fixture.runs.count_runs().await.expect("count Runs"),
        runs_before
    );
    assert_eq!(
        counts(&metrics, "coral.program.compile.duration"),
        owned(&[
            ("outcome=rejected", 2),
            ("outcome=timed_out", 2),
            ("outcome=unavailable", 8)
        ])
    );
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
    let mut fixture = TestModules::with_upstream(v8_engine(), upstream).await;
    fixture.serve().await;
    let tool = fixture
        .user_tool("user:measured-tool", "Call weather")
        .await;
    let accepted = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission(INVOKE_WEATHER),
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

/// A cancelled Run counts its Program as terminated for the cancellation,
/// and ends canceled, which carries no failure code.
#[tokio::test]
async fn a_canceled_run_records_its_program_termination() {
    let (metrics, _guard) = measured();
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let (workspace_id, principal) = fixture
        .user_scope("user:canceled-metrics", "Cancel one Run")
        .await;
    let accepted = fixture
        .runs
        .start(&principal, &workspace_id, submission("until-canceled"))
        .await
        .expect("accept the Run");
    let claim = take(&fixture.runs).await;
    let mut stream = Scripted::open(&fixture.runs, &accepted.run_id().to_string(), &claim);
    stream.start().await;
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
    finish_terminated(&mut stream, 1).await;
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

/// Catches: an output a Run Executor rejected that Coral does not count by
/// its kind, or an unspecified kind that does not break the protocol.
#[tokio::test]
async fn a_run_executors_rejected_outputs_count_by_kind() {
    use coral_run_executor::wire::v1::{
        Finish, FinishEnd, OutputKind, OutputRejected, finish, request, run_request,
    };
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let (workspace_id, principal) = fixture
        .user_scope("user:rejected-metrics", "Reject outputs")
        .await;
    let every = [
        OutputKind::Emission,
        OutputKind::Log,
        OutputKind::FinalValue,
        OutputKind::Result,
    ];
    let counted = owned(&[
        ("output.kind=emission", 1),
        ("output.kind=final_value", 1),
        ("output.kind=log", 1),
        ("output.kind=result", 1),
    ]);
    for (sent, state, counted) in [
        (&every[..], RunState::Succeeded, counted),
        (
            &[OutputKind::Unspecified][..],
            RunState::Interrupted,
            Vec::new(),
        ),
    ] {
        let (metrics, _guard) = measured();
        let accepted = fixture
            .runs
            .start(&principal, &workspace_id, submission("return;"))
            .await
            .expect("accept the Run");
        let claim = take(&fixture.runs).await;
        let mut stream = Scripted::open(&fixture.runs, &accepted.run_id().to_string(), &claim);
        stream.start().await;
        for kind in sent {
            stream.send(run_request::Message::OutputRejected(OutputRejected {
                kind: (*kind).into(),
            }));
        }
        if state == RunState::Succeeded {
            let absent = finish::Completion::End(FinishEnd::Absent.into());
            stream.request(
                1,
                request::Kind::Finish(Finish {
                    completion: Some(absent),
                }),
            );
        }
        let run = fixture
            .terminal_record(&workspace_id, accepted.run_id())
            .await;

        assert_eq!(run.state, state, "{sent:?}");
        assert_eq!(
            counts(&metrics, "coral.output.rejections"),
            counted,
            "{sent:?}"
        );
    }
}

/// Coral records what a Run keeps, each Emission and Log by size, and
/// counts each output that the Run Executor rejected, by its kind.
#[tokio::test]
async fn program_output_records_what_the_run_keeps_and_rejects() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let (workspace_id, principal) = fixture
        .user_scope("user:output-metrics", "Measure output")
        .await;
    // An Emission may hold 1,024 bytes, and the Run's output 257: the log
    // keeps two UTF-8 bytes and each Emission four, so the 64th Emission is
    // over the budget.
    let runs =
        fixture
            .served(fixture.live_runs(None).with_value_limits(
                coral_value::ValueLimits::new(5, 1024, 4096, 257).expect("limits"),
            ))
            .await;
    for (source, kept, rejected) in [
        (
            "console.log('é'); for (let i = 0; i < 64; i++) await coral.emit('é');",
            &[("output.kind=emission", 63), ("output.kind=log", 1)][..],
            "output.kind=emission",
        ),
        (
            "try { await coral.emit('x'.repeat(2048)); } catch {}",
            &[],
            "output.kind=emission",
        ),
        (
            "try { console.log('x'.repeat(64 * 1024 + 1)); } catch {}",
            &[],
            "output.kind=log",
        ),
        ("return 'x'.repeat(300);", &[], "output.kind=final_value"),
    ] {
        let (metrics, _guard) = measured();
        let accepted = runs
            .start(&principal, &workspace_id, submission(source))
            .await
            .expect("accept the Run");
        wait_for_terminal(&runs, &principal, &workspace_id, accepted.run_id()).await;

        assert_eq!(
            counts(&metrics, "coral.output.size"),
            owned(kept),
            "{source}"
        );
        assert_eq!(
            counts(&metrics, "coral.output.rejections"),
            owned(&[(rejected, 1)]),
            "{source}"
        );
    }
}
