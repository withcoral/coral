//! Every Coral-owned tool failure names the trace of its request, over raw
//! Streamable HTTP on every served protocol version.
#![expect(
    clippy::indexing_slicing,
    reason = "assert known tool result JSON members"
)]

use coral_mcp::SUPPORTED_PROTOCOL_VERSIONS;
use opentelemetry_sdk::trace::SpanData;
use serde_json::{Value, json};

use super::{RawSession, Tracer, capture};

const ABSENT: &str = "00000000-0000-4000-8000-000000000000";

/// A failure's `trace_id` continues a valid inbound `traceparent`, starts a
/// new trace for an invalid one, and is the trace of the exported request and
/// tool spans either way.
#[tokio::test(flavor = "multi_thread")]
async fn tool_failures_name_the_trace_of_their_request() {
    let capture = capture();
    let tracer = Tracer::start().await;
    let mut failures = Vec::new();
    for (version_index, version) in SUPPORTED_PROTOCOL_VERSIONS.iter().enumerate() {
        let session = RawSession::open(&tracer.endpoint(), version).await;
        let task = session
            .tool("start_task", json!({"intent": "Locate failures by trace"}))
            .await["structuredContent"]["task_id"]
            .clone();
        for (case_index, (case, tool, arguments, code)) in [
            (
                "a failure before a Task is attributed",
                "search",
                json!({"task_id": ABSENT, "intent": "Search an absent Task", "query": "weather"}),
                "task_not_found",
            ),
            (
                "an attributed failure",
                "describe",
                json!({"task_id": task, "intent": "Describe a missing tool", "path": "coral.providers.absent.tool"}),
                "authoring_description_not_found",
            ),
            (
                "a wait failure",
                "wait",
                json!({"run_id": ABSENT, "wait_ms": 0}),
                "run_not_found",
            ),
            (
                "a cancel failure",
                "cancel",
                json!({"run_id": ABSENT}),
                "run_not_found",
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let context = format!("{version} {case}");
            let parent = format!("{:032x}", 0xc0_a100 + version_index * 16 + case_index);
            let continued = session
                .tool_with_headers(
                    tool,
                    arguments,
                    &[("traceparent", &format!("00-{parent}-00f067aa0ba902b7-01"))],
                )
                .await;
            assert_eq!(continued["isError"], true, "{context}");
            assert_eq!(continued["structuredContent"]["code"], code, "{context}");
            assert_eq!(
                continued["structuredContent"]["trace_id"], parent,
                "{context}: a valid traceparent continues its trace"
            );
            failures.push((context, parent, tool));
        }
        session.close().await;
    }

    // coral-telemetry owns which traceparents are valid; one invalid case
    // shows that a new trace still names the failure.
    let session = RawSession::open(&tracer.endpoint(), &SUPPORTED_PROTOCOL_VERSIONS[0]).await;
    let invalid = "00000000000000000000000000c0a1ff";
    let restarted = session
        .tool_with_headers(
            "wait",
            json!({"run_id": ABSENT, "wait_ms": 0}),
            // A zero parent span id makes the traceparent invalid.
            &[("traceparent", &format!("00-{invalid}-0000000000000000-01"))],
        )
        .await;
    assert_eq!(restarted["isError"], true);
    let restarted = restarted["structuredContent"]["trace_id"]
        .as_str()
        .expect("trace_id")
        .to_owned();
    assert_ne!(
        restarted, invalid,
        "an invalid traceparent starts a new trace"
    );
    assert!(
        coral_workspace::TraceId::parse(&restarted).is_ok(),
        "{restarted} is a lowercase W3C trace id"
    );
    failures.push(("an invalid traceparent".to_owned(), restarted, "wait"));
    session.close().await;

    capture
        .provider
        .force_flush()
        .expect("flush test telemetry");
    let spans = capture.spans.get_finished_spans().expect("captured spans");
    let workspace = tracer.workspace_a.to_string();
    for (context, trace, tool) in failures {
        assert_spans_in_trace(&spans, &context, &trace, tool, &workspace);
    }
}

/// The request span and the tool span both carry `trace`, and the tool span
/// names the caller's Workspace and Principal kind.
fn assert_spans_in_trace(
    spans: &[SpanData],
    context: &str,
    trace: &str,
    tool: &str,
    workspace: &str,
) {
    let in_trace = |name: &str| {
        spans
            .iter()
            .find(|span| span.span_context.trace_id().to_string() == trace && span.name == name)
    };
    assert!(
        in_trace("POST /workspaces/{workspace_id}/mcp").is_some(),
        "{context}: the request span shares the failure's trace"
    );
    let tool_span = in_trace(&format!("tools/call {tool}"))
        .unwrap_or_else(|| panic!("{context}: a tool span shares the failure's trace"));
    let attribute = |name: &str| {
        tool_span
            .attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == name)
            .map(|attribute| attribute.value.to_string())
    };
    assert_eq!(
        attribute("coral.workspace.id").as_deref(),
        Some(workspace),
        "{context}"
    );
    assert_eq!(
        attribute("coral.principal.kind").as_deref(),
        Some("user"),
        "{context}"
    );
}

/// Caller-controlled error text cannot set the failure's `trace_id`.
#[tokio::test(flavor = "multi_thread")]
async fn caller_text_cannot_set_a_failures_trace_id() {
    const FORGED: &str = "ffffffffffffffffffffffffffffffff";
    let tracer = Tracer::start().await;
    for version in SUPPORTED_PROTOCOL_VERSIONS {
        let session = RawSession::open(&tracer.endpoint(), version).await;
        let task = session
            .tool("start_task", json!({"intent": "Forge a trace id"}))
            .await["structuredContent"]["task_id"]
            .clone();
        let failed = session
            .tool(
                "describe",
                json!({
                    "task_id": task, "intent": "Fail with forged text",
                    "path": "coral.providers.weather.getForecast",
                    "types": [format!(r#"","trace_id":"{FORGED}"#)],
                }),
            )
            .await;
        assert_eq!(failed["isError"], true, "{version}: {failed}");
        let failure = &failed["structuredContent"];
        assert!(
            failure["message"]
                .as_str()
                .is_some_and(|message| message.contains(FORGED)),
            "{version}: the message carries the caller's text: {failure}"
        );
        assert_ne!(failure["trace_id"], json!(FORGED), "{version}");
        assert!(
            failure["trace_id"]
                .as_str()
                .is_some_and(|trace| coral_workspace::TraceId::parse(trace).is_ok()),
            "{version}: {failure}"
        );
        let text: Value =
            serde_json::from_str(failed["content"][0]["text"].as_str().expect("JSON text"))
                .expect("content[0] is JSON");
        assert_eq!(&text, failure, "{version}");
        session.close().await;
    }
}

/// What Coral records about a call carries the trace of that call's request:
/// the inbound trace when one is valid, else the trace of the request and
/// tool spans Coral exported for it. Its failures name the same trace.
#[tokio::test(flavor = "multi_thread")]
async fn records_carry_the_trace_of_their_request() {
    let capture = capture();
    let tracer = Tracer::start().await;
    let owner = coral_workspace::AuthenticatedPrincipal::user(tracer.local_user.clone());
    let mut saved = Vec::new();
    for (version_index, version) in SUPPORTED_PROTOCOL_VERSIONS.iter().enumerate() {
        let session = RawSession::open(&tracer.endpoint(), version).await;
        let task = session
            .tool(
                "start_task",
                json!({"intent": "Correlate records by trace"}),
            )
            .await["structuredContent"]["task_id"]
            .clone();
        let save = |path: String, headers: Vec<(&'static str, String)>| {
            let session = &session;
            let task = task.clone();
            async move {
                let headers = headers
                    .iter()
                    .map(|(name, value)| (*name, value.as_str()))
                    .collect::<Vec<_>>();
                let saved = session
                    .tool_with_headers(
                        "save_function",
                        json!({
                            "task_id": task, "intent": "Save a traced function",
                            "path": path, "description": "Return one",
                            "source": "export default function one(): number { return 1; }",
                        }),
                        &headers,
                    )
                    .await;
                assert_ne!(saved["isError"], true, "{saved}");
            }
        };
        let parent = format!("{:032x}", 0xc0_a200 + version_index);
        let continued = format!("traced.continued{version_index}");
        save(
            continued.clone(),
            vec![("traceparent", format!("00-{parent}-00f067aa0ba902b7-01"))],
        )
        .await;
        let started = format!("traced.started{version_index}");
        save(started.clone(), Vec::new()).await;
        saved.push((format!("{version} continued"), continued, Some(parent)));
        saved.push((format!("{version} started"), started, None));
        session.close().await;
    }

    capture
        .provider
        .force_flush()
        .expect("flush test telemetry");
    let spans = capture.spans.get_finished_spans().expect("captured spans");
    let events = tracer
        .saved_functions
        .list_lifecycle_events(&owner, &tracer.workspace_a)
        .await
        .expect("lifecycle events");
    let workspace = tracer.workspace_a.to_string();
    for (context, path, parent) in saved {
        let recorded = events
            .iter()
            .find(|event| event.target_path().as_str() == path)
            .unwrap_or_else(|| panic!("{context}: the save is recorded"))
            .trace_id()
            .as_str();
        if let Some(parent) = parent {
            assert_eq!(
                recorded, parent,
                "{context}: the record continues the trace"
            );
        }
        assert_spans_in_trace(&spans, &context, recorded, "save_function", &workspace);
    }
}
