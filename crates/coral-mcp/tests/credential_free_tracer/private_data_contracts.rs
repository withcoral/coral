//! Private data reaches authorized readers only. One journey plants a canary
//! in everything a caller or an upstream can hand Coral, then reads every
//! place telemetry and public errors could carry it.
#![expect(
    clippy::indexing_slicing,
    reason = "assert known tool result JSON members"
)]

use coral_mcp::SUPPORTED_PROTOCOL_VERSIONS;
use coral_run::approval::ApprovalDecision;
use coral_run::{RunId, RunInspection};
use coral_telemetry::testing::attribute;
use coral_workspace::{AuthenticatedPrincipal, TraceId};
use rmcp::model::{ElicitRequestParams, ElicitationAction};
use serde_json::{Value, json};

use super::prompt_contracts::Session;
use super::{RawSession, Tracer, capture, protocol_window_ends};

/// Every planted value holds this text, so one search finds any of them.
const CANARY: &str = "c0ral-private";

/// Sent with each call: what a client says about itself, and what it would
/// wrongly put in a header.
const HEADERS: &[(&str, &str)] = &[
    ("user-agent", "client-identifier-c0ral-private"),
    ("cookie", "session=cookie-c0ral-private"),
    ("x-api-key", "header-c0ral-private"),
    ("baggage", "tenant=baggage-c0ral-private"),
];

const PROGRAM: &str = "// source-c0ral-private
console.log('log-c0ral-private');
await coral.emit('emission-c0ral-private');
return await coral.providers.weather.getPrivateRecord({token: 'argument-c0ral-private'});";

/// Programs that fail with private text of their own.
const FAILING_PROGRAMS: &[&str] = &[
    "throw new TypeError('thrown-c0ral-private');",
    "return {toJSON() { throw new Error('serialized-c0ral-private'); }};",
    "throw Object.assign(new Error('authored'), {name:'ToolError', code:'account_required', credential:'attached-c0ral-private'});",
];

/// Tool calls Coral refuses, each with private text where an identifier, a
/// name or a value belongs, and what Coral answers instead of echoing it.
fn refused_calls(task: &Value) -> [(&'static str, Value, &'static str); 7] {
    [
        (
            "search",
            json!({"task_id": "task-c0ral-private", "intent": "Search", "query": "weather"}),
            "task id is invalid",
        ),
        (
            "describe",
            json!({"task_id": task, "intent": "Describe", "path": "coral.providers.weather.c0ral-private"}),
            "Discovery input is invalid",
        ),
        (
            "wait",
            json!({"run_id": "run-c0ral-private", "wait_ms": 0}),
            "run id is invalid",
        ),
        (
            "cancel",
            json!({"run_id": "run-c0ral-private"}),
            "run id is invalid",
        ),
        (
            "end_task",
            json!({"task_id": task, "outcome": "outcome-c0ral-private"}),
            "invalid arguments for 'end_task'",
        ),
        (
            "start_task",
            json!({"intent": "Start", "key-c0ral-private": true}),
            "invalid arguments for 'start_task'",
        ),
        ("no-such-tool-c0ral-private", json!({}), "tool not found"),
    ]
}

/// Starts a session on `endpoint` as a client that identifies itself with
/// private text, and returns how Coral answers.
async fn initialize(endpoint: String, bearer: Option<&str>) -> (reqwest::StatusCode, String) {
    let request = reqwest::Client::new()
        .post(endpoint)
        .header("accept", "application/json, text/event-stream")
        .json(
            &json!({"jsonrpc":"2.0", "id":1, "method":"initialize", "params":{
                "protocolVersion":"2025-03-26", "capabilities":{},
                "clientInfo":{"name":"client-info-c0ral-private", "version":"c0ral-private"},
            }}),
        );
    let response = super::authorized(request, bearer)
        .send()
        .await
        .expect("initialize");
    let status = response.status();
    (status, response.text().await.expect("response text"))
}

/// Every answer to a working Agent credential, an unusable one, a client that
/// identifies itself with private text, and URLs that hold it.
async fn answers_to_private_sessions(tracer: &Tracer) -> String {
    use reqwest::StatusCode;
    let origin = format!("http://{}", tracer.server.local_addr());
    let organization = format!("{origin}/mcp");
    let mut answers = String::new();
    for (endpoint, bearer, expected) in [
        (
            organization.clone(),
            Some(tracer.agent_credential.as_str()),
            StatusCode::OK,
        ),
        (
            organization,
            Some("token-c0ral-private"),
            StatusCode::UNAUTHORIZED,
        ),
        (tracer.endpoint(), None, StatusCode::OK),
        (
            format!("{}?access_token=url-{CANARY}", tracer.endpoint()),
            None,
            StatusCode::NOT_FOUND,
        ),
        (
            format!("{origin}/workspaces/{CANARY}/mcp"),
            None,
            StatusCode::NOT_FOUND,
        ),
    ] {
        let (status, answer) = initialize(endpoint.clone(), bearer).await;
        assert_eq!(status, expected, "{endpoint}: {answer}");
        answers.push_str(&answer);
    }
    answers
}

/// Runs [`PROGRAM`] on a Run that waits for approval, then does what the Run
/// Status Page does through its adapter: reads the Run and approves it with a
/// comment.
async fn approve_as_the_run_status_page_does(tracer: &Tracer, session: &RawSession, task: &Value) {
    let update = session
        .tool(
            "exec",
            json!({"task_id": task, "intent": "Approve on the page", "program": PROGRAM}),
        )
        .await;
    let update = &update["structuredContent"];
    assert_eq!(update["state"], "running", "{update}");
    let run = RunId::parse(update["run_id"].as_str().expect("Run id")).expect("Run id");
    let owner = AuthenticatedPrincipal::user(tracer.local_user.clone());
    let page = tracer
        .runs
        .inspect_run(&owner, &tracer.workspace_a, &run, &RunInspection::default())
        .await
        .expect("read the Run Status Page");
    assert!(
        page.submitted_source.contains(CANARY),
        "an authorized reader sees the Run's private detail"
    );
    let requests = tracer
        .approvals
        .list_for_run(&owner, &tracer.workspace_a, &run)
        .await
        .expect("list Approval Requests");
    let request = requests.first().expect("the pending Approval Request");
    tracer
        .approvals
        .decide(
            &owner,
            &tracer.workspace_a,
            &request.id,
            ApprovalDecision::Approve,
            Some("comment-c0ral-private"),
            &TraceId::current_or_new(),
        )
        .await
        .expect("approve");
    let ended = session
        .tool("wait", json!({"run_id": update["run_id"], "wait_ms": 5000}))
        .await;
    assert_eq!(ended["structuredContent"]["state"], "succeeded", "{ended}");
}

/// Approves a Run through an elicitation whose question is the Run's intent.
async fn approve_through_elicitation(tracer: &Tracer) {
    let session = Session::open(
        &tracer.endpoint(),
        protocol_window_ends().1,
        vec![Some(ElicitationAction::Accept)],
        None,
    )
    .await;
    let task = session.start_task().await;
    let update = session
        .call(
            "exec",
            json!({
                "task_id": task, "intent": "Release elicitation-c0ral-private",
                "wait_ms": 5000, "program": PROGRAM,
            }),
        )
        .await;
    let ended = session.wait_terminal(update).await;
    assert_eq!(ended["state"], "succeeded", "{ended}");
    let prompts = session.prompts();
    let [ElicitRequestParams::FormElicitationParams { message, .. }] = prompts.as_slice() else {
        panic!("one form elicitation: {prompts:?}");
    };
    assert!(
        message.contains("elicitation-c0ral-private"),
        "the person who decides sees the intent: {message}"
    );
    session.close().await;
}

/// `text` around where it holds `private`, for a failure message.
fn around<'a>(text: &'a str, private: &str) -> &'a str {
    let at = text.find(private).unwrap_or(0);
    let start = (at.saturating_sub(160)..=at)
        .find(|index| text.is_char_boundary(*index))
        .unwrap_or(0);
    let end = (at + 160..text.len())
        .find(|index| text.is_char_boundary(*index))
        .unwrap_or(text.len());
    text.get(start..end).unwrap_or(text)
}

/// A canary in anything a caller or an upstream hands Coral stays out of
/// everything that leaves the host, whichever crates and libraries the
/// operator's filters admit, the telemetry the host keeps, public errors and
/// Administrative Lifecycle Events. No metric dimension takes a value from any
/// of them. The authorized readers still get it, so the journey did carry the
/// private data.
#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one journey through every kind of private data, then every sink"
)]
async fn private_data_stays_out_of_telemetry_public_errors_and_lifecycle_events() {
    let capture = capture();
    let tracer = Tracer::start().await;
    let session = RawSession::open(&tracer.endpoint(), &SUPPORTED_PROTOCOL_VERSIONS[0]).await;
    let task = session
        .tool_with_headers(
            "start_task",
            json!({"intent": format!("task-intent-{CANARY}")}),
            HEADERS,
        )
        .await["structuredContent"]["task_id"]
        .clone();

    let answered = session
        .tool_with_headers(
            "exec",
            json!({
                "task_id": task, "intent": format!("run-intent-{CANARY}"),
                "wait_ms": 5000, "program": PROGRAM,
            }),
            HEADERS,
        )
        .await;
    assert_eq!(
        answered["structuredContent"]["state"], "succeeded",
        "{answered}"
    );
    assert!(
        answered.to_string().contains("result-c0ral-private"),
        "the caller gets its Result: {answered}"
    );

    for program in FAILING_PROGRAMS {
        let failed = session
            .tool(
                "exec",
                json!({"task_id": task, "intent": "Fail", "wait_ms": 5000, "program": program}),
            )
            .await;
        assert_eq!(failed["structuredContent"]["state"], "failed", "{failed}");
    }

    let saved = session
        .tool(
            "save_function",
            json!({
                "task_id": task, "intent": format!("save-intent-{CANARY}"),
                "path": "diagnostics.fail",
                "description": format!("description-{CANARY}"),
                "source": format!(
                    "export default function fail() {{ throw new Error('function-{CANARY}'); }}"
                ),
            }),
        )
        .await;
    assert_ne!(saved["isError"], true, "{saved}");
    let linked = session
        .tool(
            "exec",
            json!({
                "task_id": task, "intent": "Fail in a Saved Function", "wait_ms": 5000,
                "program": "return coral.functions.diagnostics.fail();",
            }),
        )
        .await;
    assert_eq!(linked["structuredContent"]["state"], "failed", "{linked}");

    let searched = session
        .tool(
            "search",
            json!({"task_id": task, "intent": "Search", "query": format!("query-{CANARY}")}),
        )
        .await;
    assert_ne!(searched["isError"], true, "{searched}");

    let mut public_errors = answers_to_private_sessions(&tracer).await;
    for (tool, arguments, refusal) in refused_calls(&task) {
        let answer = session
            .tool_answer(tool, arguments, HEADERS)
            .await
            .to_string();
        assert!(answer.contains(refusal), "{tool}: {answer}");
        public_errors.push_str(&answer);
    }

    tracer.require_approval_of("getPrivateRecord").await;
    approve_as_the_run_status_page_does(&tracer, &session, &task).await;
    approve_through_elicitation(&tracer).await;

    let ended = session
        .tool(
            "end_task",
            json!({"task_id": task, "outcome": "success", "feedback": format!("feedback-{CANARY}")}),
        )
        .await;
    assert_eq!(ended["structuredContent"]["outcome"], "success", "{ended}");
    session.close().await;
    tracer.server.shutdown().await.expect("stop MCP server");

    let owner = AuthenticatedPrincipal::user(tracer.local_user.clone());
    let lifecycle = tracer
        .saved_functions
        .list_lifecycle_events(&owner, &tracer.workspace_a)
        .await
        .expect("lifecycle events");
    assert!(!lifecycle.is_empty(), "the save is recorded");

    // A library's own fields never leave: only Coral's spans answer for
    // theirs before the export filter.
    let spans = capture
        .spans
        .finished()
        .into_iter()
        .filter(|span| attribute(span, "target").is_some_and(|target| target.starts_with("coral")))
        .collect::<Vec<_>>();
    assert!(!spans.is_empty(), "capture is active");
    let metrics = capture.metrics.points();
    let operations = metrics
        .iter()
        .filter(|point| point.name == "coral.request.duration")
        .filter_map(|point| point.attributes.get("operation").cloned())
        .collect::<std::collections::BTreeSet<_>>();
    for tool in ["start_task", "exec", "save_function", "end_task"] {
        assert!(
            operations.contains(&format!("tools/call {tool}")),
            "each tool call's request is named by its tool: {operations:?}"
        );
    }
    let exported = capture.exported.text();
    assert!(
        exported.contains("tools/call exec"),
        "the export capture is active"
    );
    let logs = capture.logs.0.lock().expect("captured logs");
    let sinks = [
        ("what leaves the host", exported),
        ("traces", format!("{spans:?}")),
        ("metrics", format!("{metrics:?}")),
        (
            "operational logs",
            String::from_utf8_lossy(&logs).into_owned(),
        ),
        ("public errors", public_errors),
        ("Administrative Lifecycle Events", format!("{lifecycle:?}")),
    ];
    for (sink, text) in &sinks {
        for private in [CANARY, tracer.agent_credential.as_str()] {
            assert!(
                !text.contains(private),
                "{sink} holds private data: {}",
                around(text, private)
            );
        }
    }
}
