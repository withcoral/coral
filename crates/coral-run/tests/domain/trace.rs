//! The Run Trace: one Coral-owned trace per Run, linked to the request that
//! accepted it, that every phase joins on any Coral Instance.
use super::*;

use coral_telemetry::testing::{SpanCapture, attribute};
use opentelemetry::trace::{SpanId, TraceContextExt as _};
use opentelemetry_sdk::trace::SpanData;
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// The process-wide span capture. A subscriber set for one test's thread
/// misses a callsite that another test's thread registered first, so every
/// test here reads this one and selects its own spans.
fn capture() -> &'static SpanCapture {
    static CAPTURE: std::sync::OnceLock<SpanCapture> = std::sync::OnceLock::new();
    CAPTURE.get_or_init(|| {
        let capture = SpanCapture::beside(FailingExporter);
        tracing::subscriber::set_global_default(capture.subscriber())
            .expect("install the span capture");
        capture
    })
}

/// Every span export attempt [`FailingExporter`] refused.
static FAILED_EXPORTS: AtomicUsize = AtomicUsize::new(0);

/// An exporter whose collector is down: it refuses every span. Every Run
/// the capture sees has its spans refused here.
#[derive(Debug)]
struct FailingExporter;

impl opentelemetry_sdk::trace::SpanExporter for FailingExporter {
    fn export(
        &self,
        _batch: Vec<SpanData>,
    ) -> impl Future<Output = opentelemetry_sdk::error::OTelSdkResult> + Send {
        FAILED_EXPORTS.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Err(
            opentelemetry_sdk::error::OTelSdkError::InternalFailure("the collector is down".into()),
        ))
    }
}

/// Every finished span of the traces that hold a span of the Run `run_id`.
fn of_run(run_id: &crate::RunId) -> Vec<SpanData> {
    let spans = capture().finished();
    let traces = spans
        .iter()
        .filter(|span| attribute(span, "coral.run.id").as_deref() == Some(run_id.as_str()))
        .map(|span| span.span_context.trace_id())
        .collect::<std::collections::HashSet<_>>();
    spans
        .into_iter()
        .filter(|span| traces.contains(&span.span_context.trace_id()))
        .collect()
}

/// Every finished span named `name`.
fn named<'a>(spans: &'a [SpanData], name: &str) -> Vec<&'a SpanData> {
    spans.iter().filter(|span| span.name == name).collect()
}

/// The one finished span named `name`.
fn only<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
    let found = named(spans, name);
    assert_eq!(found.len(), 1, "one {name} span in {:?}", span_names(spans));
    let [span] = found.as_slice() else {
        unreachable!("one span checked above")
    };
    span
}

fn span_names(spans: &[SpanData]) -> Vec<String> {
    spans.iter().map(|span| span.name.to_string()).collect()
}

/// A Run accepted under a request gets its own trace. Its accept span is a
/// root, it and the request link to each other once, even when the request
/// goes on to observe the Run, its phases are the accept span's children,
/// and the Run row stores the accept span's context.
#[tokio::test]
async fn an_accepted_run_gets_its_own_trace_linked_to_the_request() {
    let capture = capture();
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let (workspace_id, principal) = fixture.user_scope("user:run-trace", "Trace one Run").await;

    let request = tracing::info_span!("request");
    let request_trace = request.context().span().span_context().trace_id();
    let accepted = async {
        let accepted = fixture
            .runs
            .start(&principal, &workspace_id, submission("return 1;"))
            .await
            .expect("accept the Run");
        fixture
            .runs
            .observe(
                &principal,
                &workspace_id,
                accepted.run_id(),
                &crate::RunObservation::initial(std::time::Duration::ZERO)
                    .expect("observation")
                    .by_acceptor(),
            )
            .await
            .expect("observe the Run");
        accepted
    }
    .instrument(request.clone())
    .await;
    drop(request);
    let run = fixture
        .terminal_record(&workspace_id, accepted.run_id())
        .await;
    assert_eq!(run.state, RunState::Succeeded);
    wait_until(|| !named(&of_run(accepted.run_id()), "coral.run.complete").is_empty()).await;

    let request = capture
        .finished()
        .into_iter()
        .find(|span| span.span_context.trace_id() == request_trace)
        .expect("request span");
    let request = &request;
    let spans = of_run(accepted.run_id());
    let accept = only(&spans, "coral.run.accept");
    let run_trace = accept.span_context.trace_id();
    assert_ne!(run_trace, request.span_context.trace_id());
    assert_eq!(accept.parent_span_id, SpanId::INVALID);
    assert_eq!(
        accept
            .links
            .iter()
            .map(|link| link.span_context.clone())
            .collect::<Vec<_>>(),
        vec![request.span_context.clone()]
    );
    assert_eq!(
        request
            .links
            .iter()
            .map(|link| (link.span_context.trace_id(), link.span_context.span_id()))
            .collect::<Vec<_>>(),
        [(run_trace, accept.span_context.span_id())]
    );
    assert_eq!(
        attribute(accept, "coral.run.id").as_deref(),
        Some(accepted.run_id().as_str())
    );
    for phase in ["coral.run.start", "coral.run.execute", "coral.run.complete"] {
        let phase = only(&spans, phase);
        assert_eq!(phase.span_context.trace_id(), run_trace, "{}", phase.name);
        assert_eq!(
            phase.parent_span_id,
            accept.span_context.span_id(),
            "{}",
            phase.name
        );
    }
    assert_eq!(
        run.trace
            .as_ref()
            .map(|trace| trace.trace_id().as_str().to_string()),
        Some(run_trace.to_string()),
        "the Run row stores the accept span's context"
    );
}

/// Another Coral Instance that recovers a Run continues its Run Trace with a
/// new span, a child of the accept span. A Run accepted before the Run Trace
/// was stored is recovered in a new trace of its own.
#[tokio::test]
async fn a_recovered_run_continues_its_trace_on_another_coral_instance() {
    for stored in [true, false] {
        capture();
        let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
        let (workspace_id, principal) = fixture
            .user_scope("user:recovered-trace", "Recover one Run")
            .await;
        let run_id = fixture
            .staged_run(&principal, &workspace_id, "return 1;")
            .await;
        if !stored {
            fixture
                .inject_fault(&format!(
                    "UPDATE program_run_records SET trace_parent = NULL WHERE id = '{run_id}'"
                ))
                .await;
        }
        fixture
            .hand_to(&workspace_id, &run_id, "dead-holder", None)
            .await;

        assert_eq!(
            fixture.next_process().sweep_expired_leases().await.unwrap(),
            1
        );
        let run = fixture.terminal_record(&workspace_id, &run_id).await;
        assert_eq!(run.state, RunState::Interrupted);
        wait_until(|| !named(&of_run(&run_id), "coral.run.recover").is_empty()).await;

        let spans = of_run(&run_id);
        let accept = only(&spans, "coral.run.accept");
        let recover = only(&spans, "coral.run.recover");
        assert_eq!(
            attribute(recover, "coral.run.status").as_deref(),
            Some("interrupted")
        );
        assert!(matches!(
            recover.status,
            opentelemetry::trace::Status::Error { .. }
        ));
        if stored {
            assert_eq!(
                recover.span_context.trace_id(),
                accept.span_context.trace_id()
            );
            assert_eq!(recover.parent_span_id, accept.span_context.span_id());
        } else {
            assert_ne!(
                recover.span_context.trace_id(),
                accept.span_context.trace_id()
            );
            assert_eq!(recover.parent_span_id, SpanId::INVALID);
        }
    }
}

/// The span status rules. The span that writes a Run's terminal state is an
/// error only for a Run that failed, timed out or was interrupted. A Tool
/// Invocation's span is an error for a Coral Failure, an `indeterminate`
/// end and a host defect, and never for a complete Upstream Outcome or a
/// Run that ended before its dispatch.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one table each of how a Run and an Invocation end"
)]
fn spans_are_errors_only_by_the_run_and_invocation_status_rules() {
    use crate::invocation::InvocationHostError;
    let runs = [
        (
            crate::RunTerminalOutcome::Succeeded {
                final_value: crate::FinalValue::Absent,
            },
            None,
        ),
        (
            crate::RunTerminalOutcome::Failed(crate::RunFailure::new(
                crate::RunFailureCode::ProgramRejected,
                "the Program threw".to_string(),
            )),
            Some("program_rejected"),
        ),
        (crate::RunTerminalOutcome::Canceled, None),
        (crate::RunTerminalOutcome::TimedOut, Some("timed_out")),
        (crate::RunTerminalOutcome::Interrupted, Some("interrupted")),
    ];
    let invocations = || {
        [
            (Ok("{}".to_string()), false, Some("upstream_outcome"), false),
            (
                Err(InvocationHostError::Rejected(
                    crate::CoralFailure::ToolDisabled.into(),
                )),
                false,
                Some("coral_failure"),
                true,
            ),
            (
                Err(InvocationHostError::Fatal(Error::ProgramHostTerminated)),
                false,
                Some("run_terminated"),
                false,
            ),
            (
                Err(InvocationHostError::Fatal(Error::ProgramHostTerminated)),
                true,
                Some("indeterminate"),
                true,
            ),
            (
                Err(InvocationHostError::Fatal(Error::OwnerLost)),
                true,
                None,
                true,
            ),
        ]
    };
    capture();
    let run_ids = runs
        .iter()
        .map(|(outcome, _)| {
            let run_id =
                crate::types::HeldRunId::fixture(crate::RunId::generate().expect("Run identifier"));
            crate::trace::phase_span(None, &run_id, crate::trace::Phase::Complete)
                .in_scope(|| crate::trace::record_run_end(outcome));
            run_id
        })
        .collect::<Vec<_>>();
    let weather =
        coral_provider::ProviderCallRoot::parse("coral.providers.weather").expect("Call Root");
    let invocation_ids = invocations()
        .into_iter()
        .map(|(ended, sent, _, _)| {
            let id = crate::InvocationId::generate();
            let span = crate::trace::invocation_span(
                &id,
                coral_provider::ResolvedCallPath::fixture(&weather, "getForecast"),
                &coral_provider::ProviderId::parse("00000000-0000-4000-8000-000000000001")
                    .expect("Provider ID"),
            );
            let (kind, code) = crate::invocation::invocation_end(&ended, sent);
            crate::trace::record_invocation_end(&span, kind, code);
            id.to_string()
        })
        .collect::<Vec<_>>();
    let spans = capture().finished();
    let with = |key: &str, value: &str| {
        spans
            .iter()
            .find(|span| attribute(span, key).as_deref() == Some(value))
            .unwrap_or_else(|| panic!("a span with {key} {value}"))
    };
    let is_error =
        |span: &SpanData| matches!(span.status, opentelemetry::trace::Status::Error { .. });
    for ((outcome, error_type), run_id) in runs.into_iter().zip(&run_ids) {
        let span = with("coral.run.id", run_id.run_id().as_str());
        assert_eq!(
            attribute(span, "coral.run.status").as_deref(),
            Some(outcome.state().as_storage_str())
        );
        assert_eq!(attribute(span, "error.type").as_deref(), error_type);
        assert_eq!(is_error(span), error_type.is_some(), "{outcome:?}");
    }
    for ((_, _, kind, error), id) in invocations().into_iter().zip(&invocation_ids) {
        let span = with("coral.invocation.id", id);
        assert_eq!(
            attribute(span, "coral.invocation.result_kind").as_deref(),
            kind
        );
        assert_eq!(is_error(span), error, "{kind:?}");
    }
    assert_eq!(
        attribute(
            with(
                "coral.invocation.id",
                invocation_ids.get(1).expect("the Coral Failure case")
            ),
            "coral.invocation.failure.code"
        )
        .as_deref(),
        Some(crate::CoralFailure::ToolDisabled.code())
    );
}

/// A Tool Invocation is a span of the Program's execute phase, admitted to
/// the end, and each step it takes on the way to its dispatch is a child.
#[tokio::test]
async fn a_tool_invocation_traces_its_gates_enforcement_and_persistence_under_the_run() {
    capture();
    let upstream = Arc::new(ScriptedUpstream::json(
        500,
        serde_json::json!({"down": true}),
    ));
    let fixture = TestModules::with_upstream(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        upstream,
    )
    .await;
    let tool = fixture.user_tool("user:traced-tool", "Call weather").await;
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
    wait_until(|| !named(&of_run(accepted.run_id()), "coral.run.complete").is_empty()).await;

    let spans = of_run(accepted.run_id());
    let execute = only(&spans, "coral.run.execute");
    let invocation = only(&spans, "coral.invocation");
    assert_eq!(
        invocation.span_context.trace_id(),
        execute.span_context.trace_id()
    );
    assert_eq!(invocation.parent_span_id, execute.span_context.span_id());
    assert_eq!(
        attribute(invocation, "coral.tool.call_path").as_deref(),
        Some(tool.call_path.as_str())
    );
    assert_eq!(
        attribute(invocation, "coral.invocation.result_kind").as_deref(),
        Some("upstream_outcome"),
        "an HTTP 500 is a complete Upstream Outcome"
    );
    assert_eq!(
        attribute(invocation, "coral.provider.protocol").as_deref(),
        Some("http")
    );
    assert_eq!(
        attribute(invocation, "coral.invocation.dispatch_started").as_deref(),
        Some("true")
    );
    assert_eq!(invocation.status, opentelemetry::trace::Status::Unset);
    let children = |name: &str| {
        named(&spans, name)
            .into_iter()
            .filter(|span| span.parent_span_id == invocation.span_context.span_id())
            .collect::<Vec<_>>()
    };
    for step in [
        "coral.invocation.plan",
        "coral.invocation.credentials",
        "coral.invocation.policy_enforcement_point",
        "coral.invocation.authorize",
        "coral.invocation.persist",
    ] {
        assert!(!children(step).is_empty(), "{step} under the Invocation");
    }
    let mut gates = children("coral.invocation.gate")
        .into_iter()
        .filter_map(|span| attribute(span, "coral.invocation.gate.kind"))
        .collect::<Vec<_>>();
    gates.sort();
    gates.dedup();
    assert_eq!(gates, ["account", "approval", "authority"]);
}

/// A Run that ends in error names its Run Trace: its terminal update and
/// detail carry the Run Trace identifier, and so does the Coral Failure the
/// Program saw, whether Coral or the Program runtime decided it. A Run that
/// succeeds names none.
#[tokio::test]
async fn a_failed_run_and_its_coral_failure_name_the_run_trace() {
    capture();
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture.live_runs(None);
    let tool = fixture
        .user_tool("user:failure-trace", "Name the trace")
        .await;
    fixture
        .policies
        .set_access_state(
            &change(tool.user.clone(), 30, '3'),
            tool.policy_target.clone(),
            ToolAccessState::Disabled,
        )
        .await
        .expect("disable the forecast");
    let mut ended = Vec::new();
    for source in [
        "await coral.providers.weather.getForecast({});",
        "await coral.providers.weather.getForecast(1n);",
        "return 1;",
    ] {
        let accepted = runs
            .start(&tool.principal, &tool.workspace_id, submission(source))
            .await
            .expect("accept the Run");
        let update = wait_for_terminal(
            &runs,
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
        )
        .await;
        let record = fixture
            .run_record(&tool.workspace_id, accepted.run_id())
            .await;
        let detail = runs
            .inspect_run(
                &tool.principal,
                &tool.workspace_id,
                accepted.run_id(),
                &crate::RunInspection::default(),
            )
            .await
            .expect("inspect the Run");
        ended.push((update, record, detail));
    }
    let (succeeded, _, succeeded_detail) = ended.pop().expect("the succeeded Run");

    for (failed, failed_record, failed_detail) in ended {
        let run_trace = failed_record.trace.expect("a stored Run Trace").trace_id();
        assert_eq!(failed.state(), RunState::Failed);
        assert_eq!(failed.trace_id(), Some(&run_trace));
        assert_eq!(failed_detail.trace_id.as_ref(), Some(&run_trace));
        let Some(RunTerminalOutcome::Failed(failure)) = failed.terminal_outcome() else {
            panic!("the rejected call fails its Run: {failed:?}");
        };
        let seen = failure
            .error()
            .and_then(|error| error.tool.as_ref())
            .and_then(|tool| tool.trace_id.as_deref());
        assert_eq!(seen, Some(run_trace.as_str()));
    }
    assert_eq!(succeeded.state(), RunState::Succeeded);
    assert_eq!(succeeded.trace_id(), None);
    assert_eq!(succeeded_detail.trace_id, None);
}

/// A later request about a Run keeps its own trace and links to the Run
/// Trace once Coral has found the Run visible to its caller: a wait, an
/// approval decision and a cancellation. A caller who may not see the Run
/// gets no link.
#[tokio::test]
async fn requests_about_a_run_link_to_its_trace_only_when_authorized() {
    use crate::approval::ApprovalDecision;
    let capture = capture();
    let held = HeldApproval::new("user:linked-requests").await;
    let (fixture, workspace_id, run_id) = (&held.fixture, &held.tool.workspace_id, &held.run);
    let run_trace = fixture
        .run_record(workspace_id, run_id)
        .await
        .trace
        .expect("a stored Run Trace")
        .trace_id();
    let (_, stranger) = fixture
        .user_scope("user:linked-stranger", "Probe another Run")
        .await;
    let request = held
        .approvals
        .list_for_run(&held.actor, workspace_id, run_id)
        .await
        .expect("list Approval Requests")
        .remove(0);

    let traced = |name: &'static str| {
        let span = tracing::info_span!("request", request.kind = name);
        let trace = span.context().span().span_context().trace_id();
        (span, trace)
    };
    let (probe, probe_trace) = traced("probe");
    let concealed = fixture
        .runs
        .observe(&stranger, workspace_id, run_id, &immediate(None, 10))
        .instrument(probe)
        .await;
    assert!(matches!(concealed, Err(Error::RunNotFound)));
    let (wait, wait_trace) = traced("wait");
    fixture
        .runs
        .observe(&held.actor, workspace_id, run_id, &immediate(None, 10))
        .instrument(wait)
        .await
        .expect("wait");
    let (decide, decide_trace) = traced("decide");
    held.approvals
        .decide(
            &held.tool.principal,
            workspace_id,
            &request.id,
            ApprovalDecision::Approve,
            None,
            &TraceId::parse(&"e".repeat(32)).unwrap(),
        )
        .instrument(decide)
        .await
        .expect("decide");
    let (cancel, cancel_trace) = traced("cancel");
    fixture
        .runs
        .cancel(&held.actor, workspace_id, run_id, &immediate(None, 10))
        .instrument(cancel)
        .await
        .expect("cancel");

    // A request's span ends when the last task that carries it does, which
    // can be a moment after the request returns.
    let requests = [probe_trace, wait_trace, decide_trace, cancel_trace];
    wait_until(|| {
        let spans = capture.finished();
        requests.iter().all(|trace| {
            spans
                .iter()
                .any(|span| span.span_context.trace_id() == *trace)
        })
    })
    .await;
    let spans = capture.finished();
    let links = |trace| {
        let span = spans
            .iter()
            .find(|span| span.span_context.trace_id() == trace)
            .expect("request span");
        span.links
            .iter()
            .map(|link| link.span_context.trace_id().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(links(probe_trace), Vec::<String>::new());
    for trace in [wait_trace, decide_trace, cancel_trace] {
        assert_eq!(links(trace), vec![run_trace.as_str().to_string()]);
    }
}

/// Telemetry is best effort: a Run whose every span fails to export ends
/// as it would without telemetry, and its Events are all recorded.
#[tokio::test]
async fn a_run_ends_the_same_when_its_spans_fail_to_export() {
    capture();
    let refused_before = FAILED_EXPORTS.load(Ordering::SeqCst);
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({"t": 21})));
    let fixture = TestModules::with_upstream(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        upstream,
    )
    .await;
    let tool = fixture.user_tool("user:export-fails", "Call weather").await;
    let accepted = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("invoke-tool"),
        )
        .await
        .expect("accept the Run");
    let ended = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;

    assert!(
        FAILED_EXPORTS.load(Ordering::SeqCst) > refused_before,
        "the Run's spans were refused"
    );
    assert!(matches!(
        ended.terminal_outcome(),
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &scripted_http_result(&serde_json::json!({"t": 21}))
    ));
    assert!(matches!(
        retained_events(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id()
        )
        .await
        .iter()
        .map(RunEvent::kind)
        .collect::<Vec<_>>()
        .as_slice(),
        [
            RunEventKind::Started,
            RunEventKind::InvocationStarted { .. },
            RunEventKind::InvocationEnded { .. },
            RunEventKind::Ended {
                state: RunState::Succeeded
            },
        ]
    ));
}

/// A cancellation that the Run's owner has not recorded yet still links to
/// the Run Trace: its caller was authorized and its request was inserted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unrecorded_cancellation_still_links_to_the_run_trace() {
    let capture = capture();
    let held = HeldApproval::new("user:unrecorded-cancel").await;
    let (fixture, workspace_id, run_id) = (&held.fixture, &held.tool.workspace_id, &held.run);
    let run_trace = fixture
        .run_record(workspace_id, run_id)
        .await
        .trace
        .expect("a stored Run Trace")
        .trace_id();
    fixture
        .hand_to(workspace_id, run_id, "dead-holder", None)
        .await;
    let (gate, release) = CommitGate::blocking();
    let next = fixture.next_process_observed(gate.clone());
    assert_eq!(next.sweep_expired_leases().await.unwrap(), 1);
    wait_until(|| gate.entered()).await;

    let cancel = tracing::info_span!("request", request.kind = "cancel");
    let cancel_trace = cancel.context().span().span_context().trace_id();
    let unrecorded = next
        .cancel(&held.actor, workspace_id, run_id, &immediate(None, 10))
        .instrument(cancel)
        .await;
    assert!(
        matches!(unrecorded, Err(Error::RunRequestNotYetRecorded)),
        "{unrecorded:?}"
    );
    release
        .send(())
        .expect("release the recovered owner's chunk");

    // The request's span ends when the last task that carries it does.
    wait_until(|| {
        capture
            .finished()
            .iter()
            .any(|span| span.span_context.trace_id() == cancel_trace)
    })
    .await;
    let spans = capture.finished();
    let cancel = spans
        .iter()
        .find(|span| span.span_context.trace_id() == cancel_trace)
        .expect("cancel span");
    assert_eq!(
        cancel
            .links
            .iter()
            .map(|link| link.span_context.trace_id().to_string())
            .collect::<Vec<_>>(),
        vec![run_trace.as_str().to_string()]
    );
}
