//! The Run Trace: one Coral-owned trace per Run, linked to the request that
//! accepted it, that every phase joins on any Coral Instance.
use super::*;

use coral_telemetry::testing::{SpanCapture, attribute};
use opentelemetry::trace::SpanId;
use opentelemetry_sdk::trace::SpanData;
use tracing::Instrument as _;

/// Every finished span named `name`.
fn named<'a>(spans: &'a [SpanData], name: &str) -> Vec<&'a SpanData> {
    spans.iter().filter(|span| span.name == name).collect()
}

/// The one finished span named `name`.
fn only<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
    let found = named(spans, name);
    assert_eq!(found.len(), 1, "one {name} span in {:?}", span_names(spans));
    found[0]
}

fn span_names(spans: &[SpanData]) -> Vec<String> {
    spans.iter().map(|span| span.name.to_string()).collect()
}

/// A Run accepted under a request gets its own trace. Its accept span is a
/// root that links to the request, its phases are the accept span's
/// children, and the Run row stores the accept span's context.
#[tokio::test]
async fn an_accepted_run_gets_its_own_trace_linked_to_the_request() {
    let capture = SpanCapture::default();
    let _subscriber = tracing::subscriber::set_default(capture.subscriber());
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let (workspace_id, principal) = fixture.user_scope("user:run-trace", "Trace one Run").await;

    let request = tracing::info_span!("request");
    let accepted = fixture
        .runs
        .start(&principal, &workspace_id, submission("return 1;"))
        .instrument(request.clone())
        .await
        .expect("accept the Run");
    drop(request);
    let run = fixture
        .terminal_record(&workspace_id, accepted.run_id())
        .await;
    assert_eq!(run.state, RunState::Succeeded);
    wait_until(|| !named(&capture.finished(), "coral.run.complete").is_empty()).await;

    let spans = capture.finished();
    let request = only(&spans, "request");
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
        attribute(accept, "run.id").as_deref(),
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
        let capture = SpanCapture::default();
        let _subscriber = tracing::subscriber::set_default(capture.subscriber());
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
        wait_until(|| !named(&capture.finished(), "coral.run.recover").is_empty()).await;

        let spans = capture.finished();
        let accept = only(&spans, "coral.run.accept");
        let recover = only(&spans, "coral.run.recover");
        assert_eq!(
            attribute(recover, "run.status").as_deref(),
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
fn spans_are_errors_only_by_the_run_and_invocation_status_rules() {
    use crate::invocation::InvocationHostError;
    let runs = [
        (RunState::Succeeded, false),
        (RunState::Failed, true),
        (RunState::Canceled, false),
        (RunState::TimedOut, true),
        (RunState::Interrupted, true),
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
    let capture = SpanCapture::default();
    tracing::subscriber::with_default(capture.subscriber(), || {
        for (state, _) in runs {
            let run_id = crate::RunId::generate().expect("Run identifier");
            crate::trace::phase_span(None, &run_id, crate::trace::Phase::Complete)
                .in_scope(|| crate::trace::record_run_end(state));
        }
        for (ended, sent, _, _) in invocations() {
            let span = crate::trace::invocation_span(
                &crate::InvocationId::generate(),
                "coral.providers.weather.getForecast",
                "provider",
            );
            let (kind, code) = crate::invocation::invocation_end(&ended, sent);
            crate::trace::record_invocation_end(&span, kind, code);
        }
    });
    let spans = capture.finished();
    let is_error =
        |span: &SpanData| matches!(span.status, opentelemetry::trace::Status::Error { .. });
    let completions = named(&spans, "coral.run.complete");
    assert_eq!(completions.len(), runs.len());
    for ((state, error), span) in runs.into_iter().zip(completions) {
        assert_eq!(
            attribute(span, "run.status").as_deref(),
            Some(state.as_storage_str())
        );
        assert_eq!(is_error(span), error, "{state:?}");
    }
    let ended = named(&spans, "coral.invocation");
    assert_eq!(ended.len(), invocations().len());
    for ((_, _, kind, error), span) in invocations().into_iter().zip(ended) {
        assert_eq!(attribute(span, "invocation.result_kind").as_deref(), kind);
        assert_eq!(is_error(span), error, "{kind:?}");
    }
    assert_eq!(
        attribute(named(&spans, "coral.invocation")[1], "failure.code").as_deref(),
        Some(crate::CoralFailure::ToolDisabled.code())
    );
}

/// A Tool Invocation is a span of the Program's execute phase, admitted to
/// the end, and each step it takes on the way to its dispatch is a child.
#[tokio::test]
async fn a_tool_invocation_traces_its_gates_fence_and_persistence_under_the_run() {
    let capture = SpanCapture::default();
    let _subscriber = tracing::subscriber::set_default(capture.subscriber());
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
    wait_until(|| !named(&capture.finished(), "coral.run.complete").is_empty()).await;

    let spans = capture.finished();
    let execute = only(&spans, "coral.run.execute");
    let invocation = only(&spans, "coral.invocation");
    assert_eq!(
        invocation.span_context.trace_id(),
        execute.span_context.trace_id()
    );
    assert_eq!(invocation.parent_span_id, execute.span_context.span_id());
    assert_eq!(
        attribute(invocation, "tool.call_path").as_deref(),
        Some(tool.call_path.as_str())
    );
    assert_eq!(
        attribute(invocation, "invocation.result_kind").as_deref(),
        Some("upstream_outcome"),
        "an HTTP 500 is a complete Upstream Outcome"
    );
    assert_eq!(
        attribute(invocation, "provider.protocol").as_deref(),
        Some("http")
    );
    assert_eq!(
        attribute(invocation, "dispatch.started").as_deref(),
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
        "coral.invocation.dispatch_fence",
        "coral.invocation.persist",
    ] {
        assert!(!children(step).is_empty(), "{step} under the Invocation");
    }
    let mut gates = children("coral.invocation.gate")
        .into_iter()
        .filter_map(|span| attribute(span, "gate.kind"))
        .collect::<Vec<_>>();
    gates.sort();
    gates.dedup();
    assert_eq!(gates, ["account", "approval", "authority"]);
}
