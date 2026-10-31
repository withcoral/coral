//! Every Coral-owned tool failure names the trace of its request, over raw
//! Streamable HTTP on every served protocol version.
#![expect(
    clippy::indexing_slicing,
    reason = "assert known tool result JSON members"
)]

use coral_mcp::SUPPORTED_PROTOCOL_VERSIONS;
use opentelemetry::trace::SpanKind;
use opentelemetry_sdk::trace::SpanData;
use serde_json::{Value, json};

use coral_telemetry::testing::attribute;

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
                json!({"task_id": task, "intent": "Describe a malformed path", "path": "not a path"}),
                "invalid_input",
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
            // Only a Task Coral found names the span: an absent one is the
            // caller's text.
            let named = arguments["task_id"]
                .as_str()
                .filter(|named| Some(*named) == task.as_str())
                .map(str::to_owned);
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
            failures.push((context, parent, tool, Some(code), named));
        }
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
    failures.push((
        "an invalid traceparent".to_owned(),
        restarted,
        "wait",
        Some("run_not_found"),
        None,
    ));

    let spans = capture.spans.finished();
    for (context, trace, tool, failure, task) in &failures {
        assert_spans_in_trace(
            &spans,
            context,
            trace,
            tool,
            &tracer,
            *failure,
            task.as_deref(),
        );
    }
}

/// The request span and the tool span both carry `trace`. The tool span is
/// an MCP server span that names `tracer`'s Workspace and authenticated
/// Principal, the Task the call names, and a tool failure's Coral code.
fn assert_spans_in_trace(
    spans: &[SpanData],
    context: &str,
    trace: &str,
    tool: &str,
    tracer: &Tracer,
    failure: Option<&str>,
    task: Option<&str>,
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
    assert_eq!(tool_span.span_kind, SpanKind::Server, "{context}");
    let attribute = |name: &str| attribute(tool_span, name);
    assert_eq!(
        attribute("coral.workspace.id"),
        Some(tracer.workspace_a.to_string()),
        "{context}"
    );
    assert_eq!(
        attribute("coral.principal.id").as_deref(),
        Some(tracer.local_user.user_id().as_str()),
        "{context}"
    );
    assert_eq!(
        attribute("coral.principal.kind").as_deref(),
        Some("user"),
        "{context}"
    );
    assert_eq!(
        attribute("error.type").as_deref(),
        failure.map(|_code| "tool_error"),
        "{context}"
    );
    assert_eq!(
        attribute("coral.mcp.failure.code").as_deref(),
        failure,
        "{context}"
    );
    assert_eq!(attribute("coral.task.id").as_deref(), task, "{context}");
}

/// A protocol error keeps its JSON-RPC code on the tool span. An unknown tool
/// is the caller's mistake, not a server fault, so the span does not fail.
#[tokio::test(flavor = "multi_thread")]
async fn protocol_errors_record_their_json_rpc_code() {
    let capture = capture();
    let tracer = Tracer::start().await;
    let session = RawSession::open(&tracer.endpoint(), &SUPPORTED_PROTOCOL_VERSIONS[0]).await;
    let trace = "00000000000000000000000000c0a300";
    let response = session
        .request(
            "tools/call",
            json!({"name": "no_such_tool", "arguments": {}}),
            Some(2),
        )
        .header("traceparent", format!("00-{trace}-00f067aa0ba902b7-01"))
        .send()
        .await
        .expect("raw request");
    let message = super::parse_message(response).await;
    assert_eq!(message["error"]["code"], -32602, "{message}");

    let spans = capture.spans.finished();
    let tool_span = spans
        .iter()
        .find(|span| span.span_context.trace_id().to_string() == trace && span.name == "tools/call")
        .expect("an unknown tool's span is named by the method alone");
    let attribute = |name: &str| attribute(tool_span, name);
    assert_eq!(
        attribute("rpc.response.status_code").as_deref(),
        Some("-32602")
    );
    assert_eq!(attribute("error.type"), None);
    assert!(!matches!(
        tool_span.status,
        opentelemetry::trace::Status::Error { .. }
    ));
    assert!(!format!("{tool_span:?}").contains("no_such_tool"));
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
        let task = task.as_str().expect("task id").to_owned();
        saved.push((
            format!("{version} continued"),
            continued,
            Some(parent),
            task.clone(),
        ));
        saved.push((format!("{version} started"), started, None, task));
    }

    let spans = capture.spans.finished();
    let events = tracer
        .saved_functions
        .list_lifecycle_events(&owner, &tracer.workspace_a)
        .await
        .expect("lifecycle events");
    for (context, path, parent, task) in saved {
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
        assert_spans_in_trace(
            &spans,
            &context,
            recorded,
            "save_function",
            &tracer,
            None,
            Some(&task),
        );
    }
}
