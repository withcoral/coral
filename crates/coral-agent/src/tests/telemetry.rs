use std::time::Duration;

use coral_app::SubAgentToolsFactory as _;
use coral_telemetry::testing::{MetricCapture, SpanCapture, attribute, process_spans};
use coral_test_support::EVENTUALLY;
use opentelemetry::trace::SpanId;
use serde_json::{Value, json};
use tracing::Instrument as _;
use tracing_subscriber::layer::SubscriberExt as _;
use wiremock::matchers::any;
use wiremock::{Mock, ResponseTemplate};

use super::{Lead, MESSAGE, MODEL};
use crate::SubAgentFactory;
use crate::model::Block;

const PROGRAM: &str = "return 'kestrel';";
const MODEL_TEXT: &str = "Looking for kestrels.";
const SERVER_TEXT: &str = "the server's own detail";

/// The model server's answer: `message`, stopped for `finish_reason`, under
/// another name than the config file's.
fn answer(message: &Value, finish_reason: &str) -> ResponseTemplate {
    let usage = json!({"prompt_tokens": 50, "completion_tokens": 7, "prompt_tokens_details": {"cached_tokens": 30}});
    let choice = json!({"message": message, "finish_reason": finish_reason});
    let completion = json!({"model": "served-name", "choices": [choice], "usage": usage});
    ResponseTemplate::new(200).set_body_json(completion)
}

/// What `found` returns once it returns something.
async fn eventually<T>(mut found: impl FnMut() -> Option<T>) -> T {
    let polled = async {
        loop {
            if let Some(value) = found() {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(EVENTUALLY, polled).await.unwrap()
}

// Catches: a Turn that joins the trace of the `agent` call or loses its link
// to it, a Run that loses its link to the tool call that started it, a span
// without a field that ADR 0040 lists, and a span that holds the lead's
// message, a tool's arguments or result, or the model's or its server's text.
#[tokio::test]
async fn a_turn_is_a_trace_of_its_own_with_counts_and_no_text() {
    let capture = process_spans();
    let lead = Lead::new().await;
    let (tools, task_id) = (lead.tools(), lead.task().await);
    let arguments =
        json!({"task_id": task_id.as_str(), "intent": "Find kestrels", "program": PROGRAM});
    let function = json!({"name": "exec", "arguments": arguments.to_string()});
    let exec = json!({"id": "call_1", "type": "function", "function": function});
    let calls_exec = json!({"role": "assistant", "content": MODEL_TEXT, "tool_calls": [exec]});
    Mock::given(any())
        .respond_with(answer(&calls_exec, "tool_calls"))
        .up_to_n_times(1)
        .mount(&lead.model)
        .await;
    let refusal = ResponseTemplate::new(400).set_body_string(SERVER_TEXT);
    lead.model_answers(refusal).await;

    let agent_call = tracing::info_span!("agent call");
    let (sub_agent_id, _) = lead.turn(&*tools, &task_id).instrument(agent_call).await;

    let (agent_call, turn, spans) = eventually(|| {
        let spans = capture.finished();
        let agent_call = spans.iter().find(|span| span.name == "agent call")?;
        let turn = spans.iter().find(|span| {
            let links = span.links.iter().map(|link| &link.span_context);
            span.name == "coral.sub_agent.turn" && links.eq([&agent_call.span_context])
        })?;
        Some((agent_call.clone(), turn.clone(), spans))
    })
    .await;
    let trace_id = turn.span_context.trace_id();
    assert_ne!(trace_id, agent_call.span_context.trace_id());
    assert_eq!(turn.parent_span_id, SpanId::INVALID);
    let in_turn: Vec<_> = spans
        .iter()
        .filter(|span| span.span_context.trace_id() == trace_id)
        .collect();
    let children = |name: &str| -> Vec<_> {
        let parent = turn.span_context.span_id();
        let named = in_turn.iter().copied();
        named
            .filter(|span| span.name == name && span.parent_span_id == parent)
            .collect()
    };
    let (mut model_calls, tool_calls) = (
        children("coral.sub_agent.model_call"),
        children("coral.sub_agent.tool_call"),
    );
    model_calls.sort_by_key(|span| span.start_time);
    let ([replied, refused], [exec]) = (model_calls.as_slice(), tool_calls.as_slice()) else {
        panic!("two model calls and one tool call: {model_calls:?} {tool_calls:?}");
    };
    let thread = lead.thread(&sub_agent_id).await;
    let Some([Block::ToolResult { content, .. }]) = thread.get(2).map(|m| m.content.as_slice())
    else {
        panic!("the third message is the result of exec: {thread:?}");
    };
    let result: Value = serde_json::from_str(content).unwrap();
    let run_id = result
        .get("run_id")
        .and_then(Value::as_str)
        .unwrap()
        .to_owned();
    let (workspace_id, principal_id) = (
        lead.workspace_id.to_string(),
        lead.actor.telemetry_id().to_string(),
    );
    for (span, key, value) in [
        (&turn, "coral.workspace.id", workspace_id.as_str()),
        (&turn, "coral.principal.id", &principal_id),
        (&turn, "coral.principal.kind", "user"),
        (&turn, "coral.task.id", task_id.as_str()),
        (&turn, "coral.sub_agent.id", &sub_agent_id),
        (&turn, "coral.sub_agent.turn.outcome", "failed"),
        (&turn, "error.type", "model_refused"),
        (&turn, "coral.sub_agent.turn.model_calls", "2"),
        (&turn, "coral.sub_agent.turn.tool_calls", "1"),
        (replied, "gen_ai.request.model", MODEL),
        (replied, "server.address", "127.0.0.1"),
        (replied, "gen_ai.response.finish_reasons", "tool_calls"),
        (replied, "gen_ai.usage.input_tokens", "50"),
        (replied, "gen_ai.usage.cache_read.input_tokens", "30"),
        (replied, "gen_ai.usage.output_tokens", "7"),
        (replied, "coral.sub_agent.model_call.retries", "0"),
        (refused, "http.response.status_code", "400"),
        (refused, "error.type", "model_refused"),
        (exec, "gen_ai.tool.name", "exec"),
        (exec, "coral.run.id", &run_id),
    ] {
        assert_eq!(attribute(span, key).as_deref(), Some(value), "{key}");
    }
    let accepts = spans.iter().filter(|span| span.name == "coral.run.accept");
    let mut links = accepts.map(|accept| accept.links.iter().map(|link| &link.span_context));
    assert!(links.any(|links| links.eq([&exec.span_context])));
    for span in &in_turn {
        let events = span.events.iter().flat_map(|event| &event.attributes);
        let values = span.attributes.iter().chain(events);
        for value in values.map(|kv| kv.value.to_string()) {
            for text in [MESSAGE, PROGRAM, MODEL_TEXT, SERVER_TEXT, content] {
                assert!(!value.contains(text), "{} holds {text:?}", span.name);
            }
        }
    }
}

// Catches: a Turn that records no metric, a metric attribute that holds the
// model's name, the Workspace or an ID, a compaction that is not traced or
// counted, and a failed search without its code.
#[tokio::test]
async fn a_compacting_turn_records_bounded_metrics_and_its_compaction() {
    process_spans();
    let (spans, metrics) = (SpanCapture::default(), MetricCapture::default());
    let captured = tracing_subscriber::registry().with(spans.layer());
    let _captured = tracing::subscriber::set_default(captured.with(metrics.layer()));
    let lead = Lead::new().await;
    let base_url = lead.base_url();
    let section = format!("base_url = '{base_url}'\nmodel = '{MODEL}'\ncontext_window = 48000");
    let factory = SubAgentFactory;
    let tools = factory.build(&toml::from_str(&section).unwrap(), lead.dependencies());
    super::compaction::stub(&lead, 3).await;

    lead.turn(&*tools.unwrap(), &lead.task().await).await;

    // The Turn's span finishes after its metrics are recorded.
    let ended = || {
        let running = metrics.values("coral.sub_agent.turn.active");
        let ended = metrics.named("coral.sub_agent.turn.ended");
        let traced = spans
            .finished()
            .iter()
            .any(|span| span.name == "coral.sub_agent.turn");
        (running == [0.0] && !ended.is_empty() && traced).then_some(())
    };
    eventually(ended).await;
    let mut points: Vec<String> = metrics
        .points()
        .into_iter()
        .filter(|point| point.name.starts_with("coral.sub_agent."))
        .map(|point| {
            let measure = if point.name.ends_with(".duration") {
                point.count.to_string()
            } else {
                point.value.to_string()
            };
            format!("{} {:?} {measure}", point.name, point.attributes)
        })
        .collect();
    points.sort();
    assert_eq!(
        points,
        [
            r#"coral.sub_agent.compactions {"outcome": "done"} 1"#,
            r#"coral.sub_agent.model_call.duration {"outcome": "ok"} 5"#,
            r#"coral.sub_agent.model_call.tokens {"gen_ai.token.type": "cached_input"} 0"#,
            r#"coral.sub_agent.model_call.tokens {"gen_ai.token.type": "input"} 36020"#,
            r#"coral.sub_agent.model_call.tokens {"gen_ai.token.type": "output"} 33004"#,
            "coral.sub_agent.turn.active {} 0",
            r#"coral.sub_agent.turn.duration {"sub_agent.turn.outcome": "completed"} 1"#,
            r#"coral.sub_agent.turn.ended {"sub_agent.turn.outcome": "completed"} 1"#,
            "coral.sub_agent.turn.model_calls {} 4",
        ]
    );
    let spans = spans.finished();
    let named = |name: &'static str| spans.iter().filter(move |span| span.name == name);
    let number = |span, key| attribute(span, key).unwrap().parse::<u64>().unwrap();
    let turn = named("coral.sub_agent.turn").next().unwrap();
    let [compaction] = named("coral.sub_agent.compaction").collect::<Vec<_>>()[..] else {
        panic!("one compaction");
    };
    let before = number(compaction, "coral.sub_agent.compaction.tokens_before");
    let after = number(compaction, "coral.sub_agent.compaction.tokens_after");
    assert!(after < before, "{before} -> {after}");
    assert_eq!(number(turn, "coral.sub_agent.turn.compactions"), 1);
    let failed = named("coral.sub_agent.tool_call").map(|call| attribute(call, "error.type"));
    assert_eq!(
        failed.collect::<Vec<_>>(),
        vec![Some("invalid_arguments".to_owned()); 3]
    );
}
