//! The `coral.compile` span. Spans go to the test binary's one capture, and
//! each test finds its own under a request span named for it.

use std::sync::Arc;
use std::time::Duration;

use coral_telemetry::testing::{attribute, children, process_spans};
use opentelemetry::trace::Status;
use opentelemetry_sdk::trace::SpanData;
use tracing::Instrument as _;

use super::traced_compile;
use crate::pull::tests::{answer, compile_42, next_compile};
use crate::{CompileError, CompilerClient, PullStreamCompiler, PullStreams, Unavailable};

/// The id a Run Executor declares in its `Hello`, which must be a UUID.
pub(crate) const RUN_EXECUTOR: &str = "0b7c4c1e-2f4a-4d55-9f6c-3d1f0e8b9a21";

/// The Run Executor fields of a `coral.compile` span.
const RUN_EXECUTOR_FIELDS: [&str; 3] = [
    "coral.run_executor.id",
    "coral.run_executor.kind",
    "coral.compile.relayed",
];

/// Compiles `return 42;` on `compiler` as admission does, waiting for a
/// compiler first, as one traced compile.
pub(crate) async fn traced(compiler: PullStreamCompiler) -> Result<String, CompileError> {
    let compile = async {
        let held = compiler.ready().await?;
        let client: Arc<dyn CompilerClient> = held.unwrap_or_else(|| Arc::new(compiler.clone()));
        compile_42(&*client, Duration::from_secs(5)).await
    };
    traced_compile(compile, CompileError::code).await
}

/// The one `coral.compile` span whose parent is the span named `request`,
/// once that span has finished.
pub(crate) async fn compile_span_under(request: &str) -> SpanData {
    let spans = process_spans().finished_with(request).await;
    let parent = spans
        .iter()
        .find(|span| span.name == request)
        .unwrap_or_else(|| panic!("the span {request}"));
    let [compile] = children(&spans, parent, "coral.compile")[..] else {
        panic!("one compile span under {request}");
    };
    compile.clone()
}

/// The Run Executor fields `span` carries, in [`RUN_EXECUTOR_FIELDS`] order.
pub(crate) fn run_executor_fields(span: &SpanData) -> [Option<String>; 3] {
    RUN_EXECUTOR_FIELDS.map(|field| attribute(span, field))
}

// Catches: a compile with no span, a span off the request that compiles,
// or one that does not name the Run Executor that took the compile.
#[tokio::test]
async fn a_direct_compile_is_one_span_under_its_request_that_names_its_run_executor() {
    process_spans();
    let streams = PullStreams::default();
    let (stream, mut sent) = streams.scripted(RUN_EXECUTOR, 0);
    let compiler = PullStreamCompiler::new(streams.clone(), None);
    let request = tracing::info_span!("a request that compiles directly");
    let compiling = tokio::spawn(traced(compiler).instrument(request));
    answer(&streams, stream, &mut sent).await;
    assert_eq!(compiling.await.unwrap().as_deref(), Ok("42"));

    let span = compile_span_under("a request that compiles directly").await;
    assert_eq!(
        run_executor_fields(&span),
        [
            Some(RUN_EXECUTOR.into()),
            Some("dialed".into()),
            Some("false".into())
        ]
    );
    assert_eq!(span.status, Status::Unset);
}

// Catches: a span that keeps the Run Executor of an earlier call when the
// compile's last call reached none.
#[tokio::test(start_paused = true)]
async fn a_compile_whose_last_call_no_run_executor_answered_names_none() {
    process_spans();
    let streams = PullStreams::default();
    let (stream, mut sent) = streams.scripted(RUN_EXECUTOR, 0);
    let compiler = PullStreamCompiler::new(streams.clone(), None);
    let request = tracing::info_span!("a request whose last call finds no compiler");
    let compile = async {
        let first = compile_42(&compiler, Duration::from_secs(5));
        let (first, ()) = tokio::join!(first, async {
            answer(&streams, stream, &mut sent).await;
            streams.end_scripted(stream);
        });
        assert_eq!(
            first.as_deref(),
            Ok("42"),
            "a Run Executor answers the first call"
        );
        compile_42(&compiler, Duration::from_secs(5)).await
    };
    let outcome = traced_compile(compile, CompileError::code)
        .instrument(request)
        .await;
    assert_eq!(
        outcome,
        Err(CompileError::Unavailable(Unavailable::NoCompiler))
    );

    let span = compile_span_under("a request whose last call finds no compiler").await;
    assert_eq!(run_executor_fields(&span), [None, None, None]);
}

// Catches: a span that names a Run Executor whose stream ended before it
// answered, or that loses where the call went.
#[tokio::test(start_paused = true)]
async fn a_call_whose_stream_ends_unanswered_names_no_run_executor() {
    process_spans();
    let streams = PullStreams::default();
    let (stream, mut sent) = streams.scripted(RUN_EXECUTOR, 0);
    let compiler = PullStreamCompiler::new(streams.clone(), None);
    let request = tracing::info_span!("a request whose stream ends unanswered");
    let compile = async {
        let (outcome, _number) =
            tokio::join!(compile_42(&compiler, Duration::from_secs(5)), async {
                let number = next_compile(&mut sent).await;
                streams.end_scripted(stream);
                number
            });
        outcome
    };
    let outcome = traced_compile(compile, CompileError::code)
        .instrument(request)
        .await;
    assert_eq!(
        outcome,
        Err(CompileError::Unavailable(Unavailable::Stopped))
    );

    let span = compile_span_under("a request whose stream ends unanswered").await;
    assert_eq!(
        run_executor_fields(&span),
        [None, None, Some("false".into())]
    );
}

// Catches: a compile that no Run Executor took reported as a success, or
// naming a Run Executor.
#[tokio::test(start_paused = true)]
async fn a_compile_no_run_executor_took_is_an_error_span_with_no_run_executor() {
    process_spans();
    let compiler = PullStreamCompiler::new(PullStreams::default(), None);
    let request = tracing::info_span!("a request that finds no compiler");
    let outcome = traced(compiler).instrument(request).await;
    assert_eq!(
        outcome,
        Err(CompileError::Unavailable(Unavailable::NoCompiler))
    );

    let span = compile_span_under("a request that finds no compiler").await;
    assert_eq!(run_executor_fields(&span), [None, None, None]);
    assert_eq!(span.status, Status::error(""));
    assert_eq!(
        attribute(&span, "error.type").as_deref(),
        Some("compiler_unavailable")
    );
}
