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
    let endpoint = tracer.endpoint();
    let mut failures = Vec::new();
    for (version_index, version) in SUPPORTED_PROTOCOL_VERSIONS.iter().enumerate() {
        let session = RawSession::open(&endpoint, version).await;
        let task = session
            .tool("start_task", json!({"intent": "Locate failures by trace"}))
            .await["structuredContent"]["task_id"]
            .clone();
        for (case_index, (case, tool, arguments, code)) in [
            (
                "an attributed failure",
                "describe",
                json!({"task_id": task, "intent": "Describe a missing tool", "path": "coral.providers.absent.tool"}),
                "authoring_description_not_found",
            ),
            (
                "a failure before attribution",
                "search",
                json!({"task_id": ABSENT, "intent": "Search an absent Task", "query": "weather"}),
                "task_not_found",
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

        // A zero parent span id makes the traceparent invalid.
        let invalid = format!("{:032x}", 0xc0_a1ff + version_index);
        let restarted = session
            .tool_with_headers(
                "wait",
                json!({"run_id": ABSENT, "wait_ms": 0}),
                &[("traceparent", &format!("00-{invalid}-0000000000000000-01"))],
            )
            .await;
        assert_eq!(restarted["isError"], true, "{version}");
        let restarted = restarted["structuredContent"]["trace_id"]
            .as_str()
            .unwrap_or_else(|| panic!("{version}: trace_id"))
            .to_owned();
        assert_ne!(
            restarted, invalid,
            "{version}: an invalid traceparent starts a new trace"
        );
        assert!(
            coral_workspace::TraceId::parse(&restarted).is_ok(),
            "{version}: {restarted} is a lowercase W3C trace id"
        );
        failures.push((
            format!("{version} an invalid traceparent"),
            restarted,
            "wait",
        ));
        session.close().await;
    }

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
        in_trace("coral.mcp.request").is_some(),
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
