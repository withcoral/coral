use std::slice;
use std::time::Duration;

use coral_credentials::Secret;
use coral_outbound::{DestinationPolicy, NetworkAccess};
use reqwest::StatusCode;
use serde_json::{Map, Value, json};
use url::Url;
use wiremock::matchers::{any, body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{
    Block, Message, ModelCall, ModelClient, ModelError, Protocol, Reply, Role, ToolCall, Usage,
};
use crate::prompt::ToolText;

const MODEL: &str = "test-model";
const KEY: &str = "model-key";
const TOOLS: [ToolText; 1] = [ToolText {
    name: "exec",
    description: "Run a Program.",
    parameters: r#"{"type":"object"}"#,
}];

fn client_for(server: &MockServer, protocol: Protocol, api_key: Option<&str>) -> ModelClient {
    let http = coral_outbound::Client::new(
        DestinationPolicy::new(NetworkAccess::HttpAndHttps, NetworkAccess::Deny),
        Duration::from_secs(5),
    )
    .unwrap();
    let base_url = Url::parse(&format!("{}/v1", server.uri())).unwrap();
    let api_key = api_key.map(|key| Secret::new(key.to_owned()).unwrap());
    ModelClient {
        retry_waits: [Duration::ZERO; 3],
        ..ModelClient::new(
            http,
            protocol,
            &base_url,
            MODEL.to_owned(),
            api_key,
            None,
            Map::new(),
        )
    }
}

/// Args that set how much a model reasons on each protocol.
fn effort_args(protocol: Protocol, effort: Option<&str>) -> Map<String, Value> {
    let Some(effort) = effort else {
        return Map::new();
    };
    let args = match protocol {
        Protocol::ChatCompletions => json!({"reasoning_effort": effort}),
        Protocol::OpenAiResponses => json!({"reasoning": {"effort": effort}}),
        Protocol::Anthropic if effort == "none" => json!({"thinking": {"type": "disabled"}}),
        Protocol::Anthropic => match effort.parse::<u32>() {
            Ok(budget) => json!({"thinking": {"type": "enabled", "budget_tokens": budget}}),
            Err(_word) => {
                json!({"thinking": {"type": "adaptive"}, "output_config": {"effort": effort}})
            }
        },
    };
    args.as_object().cloned().unwrap()
}

fn client(server: &MockServer, api_key: Option<&str>) -> ModelClient {
    client_for(server, Protocol::ChatCompletions, api_key)
}

fn call(id: &str) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        name: "exec".to_owned(),
        arguments: r#"{"program":"return 1;"}"#.to_owned(),
    }
}

fn assistant(content: Vec<Block>) -> Message {
    Message {
        role: Role::Assistant,
        content,
    }
}

/// A conversation with text, tool calls and their results, and an empty assistant message.
fn conversation() -> Vec<Message> {
    vec![
        Message::user("Hello?".to_owned()),
        assistant(vec![
            Block::Text {
                text: "Looking.".to_owned(),
            },
            Block::ToolUse(call("call_1")),
        ]),
        Message::tool_results(vec![call("call_1").result("1".to_owned(), false, None)]),
        assistant(vec![Block::ToolUse(call("call_2"))]),
        Message::tool_results(vec![call("call_2").result("2".to_owned(), true, None)]),
        assistant(Vec::new()),
    ]
}

fn answer(text: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
    }))
}

fn responses_answer(text: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "model": MODEL,
        "status": "completed",
        "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": text}]}],
    }))
}

fn anthropic_answer(text: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "model": MODEL,
        "stop_reason": "end_turn",
        "content": [{"type": "text", "text": text}],
    }))
}

async fn ask(client: &ModelClient) -> Result<Reply, ModelError> {
    client.reply("Be brief.", &conversation(), &TOOLS).await
}

async fn received(server: &MockServer) -> Vec<wiremock::Request> {
    server.received_requests().await.unwrap()
}

/// The assistant message of a system, user and assistant thread, as the last request sent it.
async fn sent_reply(server: &MockServer) -> Option<Value> {
    let body: Value = received(server).await.pop()?.body_json().ok()?;
    body.pointer("/messages/2").cloned()
}

// Catches: a conversation or a tool that does not reach the server as Chat
// Completions, and a reply whose tool calls or record are lost.
#[tokio::test]
async fn the_conversation_goes_as_chat_completions_and_the_reply_comes_back_as_blocks() {
    let server = MockServer::start().await;
    let arguments = r#"{"program":"return 1;"}"#;
    let function = json!({"name": "exec", "arguments": arguments});
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_json(json!({
            "model": MODEL,
            "messages": [
                {"role": "system", "content": "Be brief."},
                {"role": "user", "content": [{"type": "text", "text": "Hello?"}]},
                {"role": "assistant", "content": "Looking.", "tool_calls": [
                    {"type": "function", "id": "call_1", "function": function},
                ]},
                {"role": "tool", "tool_call_id": "call_1", "content": "1"},
                {"role": "assistant", "content": null, "tool_calls": [
                    {"type": "function", "id": "call_2", "function": function},
                ]},
                {"role": "tool", "tool_call_id": "call_2", "content": "2"},
            ],
            "tools": [{"type": "function", "function": {
                "name": "exec",
                "description": "Run a Program.",
                "parameters": {"type": "object"},
                "strict": false,
            }}],
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "model": "served-model",
            "choices": [{"message": {"role": "assistant", "content": "Again.", "tool_calls": [
                {"id": "call_3", "type": "function", "function": function},
            ]}}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 7, "prompt_tokens_details": {"cached_tokens": 60}},
        })))
        .mount(&server)
        .await;

    let reply = ask(&client(&server, None)).await.unwrap();

    let again = Block::Text {
        text: "Again.".to_owned(),
    };
    assert_eq!(
        reply.message,
        assistant(vec![again, Block::ToolUse(call("call_3"))])
    );
    let usage = Usage {
        input_tokens: 40,
        cache_read_input_tokens: 60,
        output_tokens: 7,
    };
    let record = ModelCall {
        latency: Duration::ZERO,
        ..reply.call
    };
    assert_eq!(
        record,
        ModelCall {
            model: "served-model".to_owned(),
            latency: Duration::ZERO,
            usage: Some(usage),
        }
    );
}

// Catches: a set reasoning_effort left off a reply's or a summary's request, or the field sent
// when it is not set.
#[tokio::test]
async fn reasoning_effort_goes_on_every_request_only_when_set() {
    for (effort, expected) in [(Some("none"), Some(json!("none"))), (None, None)] {
        let server = MockServer::start().await;
        Mock::given(any())
            .respond_with(answer("Hi."))
            .mount(&server)
            .await;
        let client = ModelClient {
            request_args: effort_args(Protocol::ChatCompletions, effort),
            ..client(&server, None)
        };

        ask(&client).await.unwrap();
        let summary = client.summarize("Be brief.", &conversation(), 100).await;
        summary.unwrap();

        let sent: Vec<Option<Value>> = received(&server)
            .await
            .iter()
            .map(|request| {
                let body: Value = request.body_json().unwrap();
                body.get("reasoning_effort").cloned()
            })
            .collect();
        assert_eq!(sent, [expected.clone(), expected], "{effort:?}");
    }
}

// Catches: reasoning in either field dropped or kept without its model, an empty field taken over
// the next, reasoning not sent back to the model that wrote it, or reasoning in the lead's answer.
#[tokio::test]
async fn reasoning_is_kept_with_its_model_and_sent_back_to_it() {
    for message in [
        json!({"role": "assistant", "content": "Done.", "reasoning": "Plan."}),
        json!({"role": "assistant", "content": "Done.", "reasoning_content": "Plan."}),
        json!({"role": "assistant", "content": "Done.", "reasoning": "", "reasoning_content": "Plan."}),
    ] {
        let server = MockServer::start().await;
        let completion = json!({"choices": [{"message": message}]});
        Mock::given(any())
            .respond_with(ResponseTemplate::new(200).set_body_json(completion))
            .mount(&server)
            .await;
        let client = client(&server, None);
        let hello = Message::user("Hello?".to_owned());

        let reply = client
            .reply("Be brief.", slice::from_ref(&hello), &TOOLS)
            .await;
        let reply = reply.unwrap().message;
        let thread = [hello, reply.clone()];
        client.reply("Be brief.", &thread, &TOOLS).await.unwrap();

        let reasoning = Block::Reasoning {
            text: "Plan.".to_owned(),
            model: MODEL.to_owned(),
            protocol: Some(Protocol::ChatCompletions),
            opaque: None,
        };
        let done = Block::Text {
            text: "Done.".to_owned(),
        };
        assert_eq!(reply, assistant(vec![reasoning, done]), "{message}");
        assert_eq!(reply.text(), "Done.");
        let replayed = json!({"role": "assistant", "content": "Done.", "reasoning": "Plan."});
        assert_eq!(sent_reply(&server).await, Some(replayed), "{message}");
    }
}

// Catches: reasoning that another model wrote sent to this one, or a stored reasoning block that
// no longer reads.
#[tokio::test]
async fn reasoning_by_another_model_stays_in_the_thread_and_is_left_out_of_the_request() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(answer("Hi."))
        .mount(&server)
        .await;
    let stored = json!([
        {"type": "reasoning", "text": "Plan.", "model": "earlier-model"},
        {"type": "text", "text": "Looking."},
    ]);
    let thread = [
        Message::user("Hello?".to_owned()),
        assistant(serde_json::from_value(stored).unwrap()),
    ];

    client(&server, None)
        .reply("Be brief.", &thread, &TOOLS)
        .await
        .unwrap();

    let left_out = json!({"role": "assistant", "content": "Looking."});
    assert_eq!(sent_reply(&server).await, Some(left_out));
}

// Catches: legacy stored reasoning/tool_result blocks without `protocol`/`opaque`/`is_error`
// failing to deserialize, or legacy Chat Completions reasoning leaking into other protocols.
#[tokio::test]
async fn legacy_stored_blocks_deserialize_and_stay_scoped_to_chat_completions() {
    let stored_assistant = json!([
        {"type": "reasoning", "text": "Legacy plan.", "model": MODEL},
        {"type": "text", "text": "Looking."},
    ]);
    let stored_result = json!([
        {"type": "tool_result", "tool_use_id": "call_1", "content": "ok"},
    ]);
    let blocks: Vec<Block> = serde_json::from_value(stored_result).unwrap();
    assert_eq!(
        blocks,
        vec![Block::ToolResult {
            tool_use_id: "call_1".to_owned(),
            content: "ok".to_owned(),
            is_error: false,
            run_id: None,
        }]
    );
    let thread = [
        Message::user("Hello?".to_owned()),
        assistant(serde_json::from_value(stored_assistant).unwrap()),
    ];

    // Chat Completions replays legacy reasoning for the same model.
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(answer("Hi."))
        .mount(&server)
        .await;
    client_for(&server, Protocol::ChatCompletions, None)
        .reply("Be brief.", &thread, &TOOLS)
        .await
        .unwrap();
    assert_eq!(
        sent_reply(&server).await,
        Some(json!({"role": "assistant", "content": "Looking.", "reasoning": "Legacy plan."}))
    );

    // OpenAI Responses and Anthropic omit legacy Chat Completions reasoning.
    for (protocol, stub) in [
        (Protocol::OpenAiResponses, responses_answer("Hi.")),
        (Protocol::Anthropic, anthropic_answer("Hi.")),
    ] {
        let server = MockServer::start().await;
        Mock::given(any()).respond_with(stub).mount(&server).await;
        client_for(&server, protocol, None)
            .reply("Be brief.", &thread, &TOOLS)
            .await
            .unwrap();
        let body: Value = received(&server).await.pop().unwrap().body_json().unwrap();
        let serialized = body.to_string();
        assert!(
            !serialized.contains("Legacy plan."),
            "{protocol:?} leaked legacy reasoning: {serialized}"
        );
    }
}

fn expected_openai_responses_request() -> Value {
    let arguments = r#"{"program":"return 1;"}"#;
    json!({
        "model": MODEL,
        "store": false,
        "instructions": "Be brief.",
        "input": [
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "Hello?"}]},
            {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Looking."}]},
            {"type": "function_call", "call_id": "call_1", "name": "exec", "arguments": arguments},
            {"type": "function_call_output", "call_id": "call_1", "output": "1"},
            {"type": "function_call", "call_id": "call_2", "name": "exec", "arguments": arguments},
            {"type": "function_call_output", "call_id": "call_2", "output": "2"},
        ],
        "tools": [{
            "type": "function",
            "name": "exec",
            "description": "Run a Program.",
            "parameters": {"type": "object"},
            "strict": false,
        }],
        "reasoning": {"effort": "high"},
        "include": ["reasoning.encrypted_content"],
    })
}

// Catches: OpenAI Responses request shape, stateless `store: false`, `include`,
// function calls/outputs, usage split, and finish reason mapping.
#[tokio::test]
async fn openai_responses_encodes_conversation_and_decodes_reply_and_encrypted_reasoning() {
    let server = MockServer::start().await;
    let arguments = r#"{"program":"return 1;"}"#;
    let expected_opaque = json!({
        "type": "reasoning",
        "id": "rs_123",
        "encrypted_content": "enc_blob_abc",
        "summary": [{"type": "summary_text", "text": "Check tools first."}],
    });
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .and(header("authorization", format!("Bearer {KEY}")))
        .and(body_json(expected_openai_responses_request()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "model": "gpt-5.4-2026-03-01",
            "status": "completed",
            "output": [
                expected_opaque,
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "Running now."}],
                },
                {
                    "type": "function_call",
                    "call_id": "call_3",
                    "name": "exec",
                    "arguments": arguments,
                },
            ],
            "usage": {
                "input_tokens": 200,
                "output_tokens": 35,
                "input_tokens_details": {"cached_tokens": 150},
            },
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(responses_answer("All done."))
        .mount(&server)
        .await;

    let client = ModelClient {
        request_args: effort_args(Protocol::OpenAiResponses, Some("high")),
        ..client_for(&server, Protocol::OpenAiResponses, Some(KEY))
    };
    let reply = ask(&client).await.unwrap();

    assert_eq!(reply.finish_reason.as_deref(), Some("tool_calls"));
    assert_eq!(reply.message.text(), "Running now.");
    assert_eq!(
        reply.message,
        assistant(vec![
            Block::Reasoning {
                text: "Check tools first.".to_owned(),
                model: MODEL.to_owned(),
                protocol: Some(Protocol::OpenAiResponses),
                opaque: Some(expected_opaque.clone()),
            },
            Block::Text {
                text: "Running now.".to_owned(),
            },
            Block::ToolUse(call("call_3")),
        ])
    );
    assert_eq!(
        reply.call.usage,
        Some(Usage {
            input_tokens: 50,
            cache_read_input_tokens: 150,
            output_tokens: 35,
        })
    );

    // Next turn replays the encrypted reasoning item verbatim before assistant text and tool call.
    let next_thread = [
        Message::user("Hello?".to_owned()),
        reply.message,
        Message::tool_results(vec![call("call_3").result("ok".to_owned(), false, None)]),
    ];
    let second = client
        .reply("Be brief.", &next_thread, &TOOLS)
        .await
        .unwrap();
    assert_eq!(second.finish_reason.as_deref(), Some("stop"));
    let second_body: Value = received(&server).await.pop().unwrap().body_json().unwrap();
    assert_eq!(second_body.pointer("/input/1"), Some(&expected_opaque));
    assert_eq!(
        second_body.pointer("/input/2/type"),
        Some(&json!("message"))
    );
    assert_eq!(
        second_body.pointer("/input/3/type"),
        Some(&json!("function_call"))
    );
}

// Catches: OpenAI Responses summarize `max_output_tokens`, omitting `include` when effort is
// `"none"`, and incomplete finish-reason normalization.
#[tokio::test]
async fn openai_responses_handles_summarize_none_effort_and_incomplete_status() {
    for (status, reason, expected) in [
        ("incomplete", Some("max_output_tokens"), Some("length")),
        ("incomplete", Some("content_filter"), Some("content_filter")),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/responses"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "model": MODEL,
                "status": status,
                "incomplete_details": {"reason": reason},
                "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Summary."}]}],
            })))
            .mount(&server)
            .await;

        let client = ModelClient {
            request_args: effort_args(Protocol::OpenAiResponses, Some("none")),
            ..client_for(&server, Protocol::OpenAiResponses, None)
        };
        let reply = client
            .summarize("Summarize.", &[Message::user("Work.".to_owned())], 512)
            .await
            .unwrap();

        assert_eq!(reply.finish_reason.as_deref(), expected);
        let body: Value = received(&server).await.pop().unwrap().body_json().unwrap();
        assert_eq!(body.get("max_output_tokens"), Some(&json!(512)));
        assert_eq!(body.get("tools"), None);
        assert_eq!(body.get("include"), None);
        assert_eq!(body.get("reasoning"), Some(&json!({"effort": "none"})));
    }
}

fn expected_anthropic_request(parsed_arguments: &Value) -> Value {
    json!({
        "model": MODEL,
        "max_tokens": 16384,
        "system": [{
            "type": "text",
            "text": "Be brief.",
            "cache_control": {"type": "ephemeral"},
        }],
        "tools": [{
            "name": "exec",
            "description": "Run a Program.",
            "input_schema": {"type": "object"},
        }],
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": "Hello?"}],
            },
            {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "Looking."},
                    {"type": "tool_use", "id": "call_1", "name": "exec", "input": parsed_arguments},
                ],
            },
            {
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "call_1", "content": "1"}],
            },
            {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "call_2", "name": "exec", "input": parsed_arguments},
                ],
            },
            {
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "call_2",
                    "content": "2",
                    "is_error": true,
                    "cache_control": {"type": "ephemeral"},
                }],
            },
        ],
        "thinking": {"type": "adaptive"},
        "output_config": {"effort": "medium"},
    })
}

// Catches: Anthropic headers, prompt caching breakpoints (system + rolling last user turn),
// tool_result `is_error`, adjacent user message merging (`tool_result` before `text`), thinking
// + redacted_thinking replay order, usage normalization, and stop_reason mapping.
#[tokio::test]
async fn anthropic_encodes_caching_error_flags_merged_turns_and_replays_thinking_blocks() {
    let server = MockServer::start().await;
    let parsed_arguments = json!({"program": "return 1;"});
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(header("x-api-key", KEY))
        .and(header("anthropic-version", "2023-06-01"))
        .and(body_json(expected_anthropic_request(&parsed_arguments)))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "model": "claude-sonnet-4-6",
            "stop_reason": "tool_use",
            "content": [
                {"type": "thinking", "thinking": "Let me check.", "signature": "sig_1"},
                {"type": "redacted_thinking", "data": "opaque_redacted"},
                {"type": "thinking", "thinking": "Unsigned thought."},
                {"type": "text", "text": "Running."},
                {"type": "tool_use", "id": "call_3", "name": "exec", "input": parsed_arguments},
            ],
            "usage": {
                "input_tokens": 30,
                "cache_creation_input_tokens": 20,
                "cache_read_input_tokens": 150,
                "output_tokens": 12,
            },
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(anthropic_answer("Done."))
        .mount(&server)
        .await;

    let client = ModelClient {
        request_args: effort_args(Protocol::Anthropic, Some("medium")),
        ..client_for(&server, Protocol::Anthropic, Some(KEY))
    };
    let reply = ask(&client).await.unwrap();

    // No Authorization header is sent to Anthropic.
    let first_req = received(&server).await.remove(0);
    assert_eq!(first_req.headers.get("authorization"), None);

    assert_eq!(reply.finish_reason.as_deref(), Some("tool_calls"));
    assert_eq!(reply.message.text(), "Running.");
    assert_eq!(
        reply.call.usage,
        Some(Usage {
            input_tokens: 50,
            cache_read_input_tokens: 150,
            output_tokens: 12,
        })
    );

    // Next turn: merge adjacent user messages (tool_result + steering text),
    // replay signed & redacted thinking first, drop unsigned thinking.
    let next_thread = [
        Message::user("Hello?".to_owned()),
        reply.message,
        Message::tool_results(vec![call("call_3").result("ok".to_owned(), false, None)]),
        Message::user("Also check the total.".to_owned()),
    ];
    let second = client
        .reply("Be brief.", &next_thread, &TOOLS)
        .await
        .unwrap();
    assert_eq!(second.finish_reason.as_deref(), Some("stop"));
    let second_body: Value = received(&server).await.pop().unwrap().body_json().unwrap();
    assert_eq!(
        second_body.pointer("/messages/1"),
        Some(&json!({
            "role": "assistant",
            "content": [
                {"type": "thinking", "thinking": "Let me check.", "signature": "sig_1"},
                {"type": "redacted_thinking", "data": "opaque_redacted"},
                {"type": "text", "text": "Running."},
                {"type": "tool_use", "id": "call_3", "name": "exec", "input": parsed_arguments},
            ],
        }))
    );
    assert_eq!(
        second_body.pointer("/messages/2"),
        Some(&json!({
            "role": "user",
            "content": [
                {"type": "tool_result", "tool_use_id": "call_3", "content": "ok"},
                {
                    "type": "text",
                    "text": "Also check the total.",
                    "cache_control": {"type": "ephemeral"},
                },
            ],
        }))
    );
}

// Catches: Anthropic reasoning_effort variants (`None`, `"none"`, numeric budget, effort word)
// and stop_reason normalization (`max_tokens`, `refusal`).
#[tokio::test]
async fn anthropic_maps_reasoning_effort_variants_and_stop_reasons() {
    for (
        effort,
        max_tokens,
        stop_reason,
        expected_finish,
        expected_thinking,
        expected_output_config,
        expected_max,
    ) in [
        (None, None, "max_tokens", "length", None, None, 16_384),
        (
            Some("none"),
            None,
            "refusal",
            "content_filter",
            Some(json!({"type": "disabled"})),
            None,
            16_384,
        ),
        (
            Some("high"),
            None,
            "max_tokens",
            "length",
            Some(json!({"type": "adaptive"})),
            Some(json!({"effort": "high"})),
            16_384,
        ),
        (
            Some("20000"),
            None,
            "max_tokens",
            "length",
            Some(json!({"type": "enabled", "budget_tokens": 20_000})),
            None,
            24_096,
        ),
        (
            Some("8000"),
            Some(4096),
            "max_tokens",
            "length",
            Some(json!({"type": "enabled", "budget_tokens": 8_000})),
            None,
            12_096,
        ),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "model": MODEL,
                "stop_reason": stop_reason,
                "content": [{"type": "text", "text": "Cut."}],
            })))
            .mount(&server)
            .await;
        let client = ModelClient {
            request_args: effort_args(Protocol::Anthropic, effort),
            ..client_for(&server, Protocol::Anthropic, None)
        };
        let hello = [Message::user("Hello?".to_owned())];
        let reply = match max_tokens {
            Some(limit) => client.summarize("Be brief.", &hello, limit).await.unwrap(),
            None => client.reply("Be brief.", &hello, &TOOLS).await.unwrap(),
        };

        assert_eq!(reply.finish_reason.as_deref(), Some(expected_finish));
        let body: Value = received(&server).await.pop().unwrap().body_json().unwrap();
        assert_eq!(
            body.get("thinking"),
            expected_thinking.as_ref(),
            "{effort:?}"
        );
        assert_eq!(
            body.get("output_config"),
            expected_output_config.as_ref(),
            "{effort:?}"
        );
        assert_eq!(
            body.get("max_tokens"),
            Some(&json!(expected_max)),
            "{effort:?}"
        );
    }
}

// Catches: args left off a reply or a summary on any protocol, a field that Coral owns replaced by
// an arg, an arg that lifts the cap on a summary's tokens, and a cap on replies that args cannot set.
#[tokio::test]
async fn args_are_merged_into_every_request_where_coral_owns_the_rest() {
    let chat = json!({"choices": [{"message": {"role": "assistant", "content": "Hi."}}]});
    let responses = json!({"status": "completed", "output": [
        {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Hi."}]},
    ]});
    let anthropic =
        json!({"stop_reason": "end_turn", "content": [{"type": "text", "text": "Hi."}]});
    for (protocol, answer, cap) in [
        (Protocol::ChatCompletions, chat, "max_completion_tokens"),
        (Protocol::OpenAiResponses, responses, "max_output_tokens"),
        (Protocol::Anthropic, anthropic, "max_tokens"),
    ] {
        let server = MockServer::start().await;
        Mock::given(any())
            .respond_with(ResponseTemplate::new(200).set_body_json(answer))
            .mount(&server)
            .await;
        let args = json!({"model": "other", "speed": "fast", "max_tokens": 32_000, "top_p": 0.5});
        let client = ModelClient {
            request_args: args.as_object().cloned().unwrap(),
            ..client_for(&server, protocol, None)
        };

        ask(&client).await.unwrap();
        let summary = client.summarize("Be brief.", &conversation(), 100).await;
        summary.unwrap();

        let [reply, summary] = &received(&server).await[..] else {
            panic!("{protocol:?}: two requests");
        };
        let (reply, summary): (Value, Value) =
            (reply.body_json().unwrap(), summary.body_json().unwrap());
        for body in [&reply, &summary] {
            assert_eq!(body.get("model"), Some(&json!(MODEL)), "{protocol:?}");
            assert_eq!(body.get("speed"), Some(&json!("fast")), "{protocol:?}");
            assert_eq!(body.get("top_p"), Some(&json!(0.5)), "{protocol:?}");
        }
        assert_eq!(
            reply.get("max_tokens"),
            Some(&json!(32_000)),
            "{protocol:?}"
        );
        let caps = ["max_completion_tokens", "max_output_tokens", "max_tokens"];
        let capped = caps.map(|name| (name, summary.get(name).cloned()));
        let expected = caps.map(|name| (name, (name == cap).then(|| json!(100))));
        assert_eq!(capped, expected, "{protocol:?}");
    }
}

// Catches: a key not sent as a bearer token, or an Authorization header sent without one.
#[tokio::test]
async fn bearer_header_is_sent_only_with_a_key() {
    for (api_key, expected) in [(Some(KEY), Some(format!("Bearer {KEY}"))), (None, None)] {
        let server = MockServer::start().await;
        Mock::given(any())
            .respond_with(answer("Hi."))
            .mount(&server)
            .await;

        ask(&client(&server, api_key)).await.unwrap();

        let sent: Vec<_> = received(&server)
            .await
            .iter()
            .map(|request| {
                request
                    .headers
                    .get("authorization")
                    .map(|value| value.to_str().unwrap().to_owned())
            })
            .collect();
        assert_eq!(sent, [expected]);
    }
}

// Catches: a busy or failing server not retried, or retried more than three times.
#[tokio::test]
async fn busy_or_failing_server_is_retried_three_times() {
    let busy = ModelError::Unavailable(Some(StatusCode::SERVICE_UNAVAILABLE));
    for (status, failures, requests, expected) in [
        (429, 1, 2, Ok("Hi.".to_owned())),
        (500, 3, 4, Ok("Hi.".to_owned())),
        (503, 4, 4, Err(busy)),
    ] {
        let server = MockServer::start().await;
        Mock::given(any())
            .respond_with(ResponseTemplate::new(status))
            .up_to_n_times(failures)
            .mount(&server)
            .await;
        Mock::given(any())
            .respond_with(answer("Hi."))
            .mount(&server)
            .await;

        let reply = ask(&client(&server, None)).await;

        let text = reply.map(|reply| reply.message.text());
        assert_eq!(text, expected, "status {status}");
        assert_eq!(received(&server).await.len(), requests);
    }
}

// Catches: a server that the operator's destination policy forbids sent the request and the key, or
// the refusal shown as a server that did not answer.
#[tokio::test]
async fn a_forbidden_server_is_sent_nothing() {
    let server = MockServer::start().await;
    let closed = DestinationPolicy::default();
    let client = ModelClient {
        http: coral_outbound::Client::new(closed, Duration::from_secs(5)).unwrap(),
        ..client(&server, Some(KEY))
    };

    assert_eq!(client.probe().await, Err(ModelError::Forbidden));
    assert!(received(&server).await.is_empty());
}

// Catches: a refusal or an unreadable answer retried, or an error's text holding the answer or the key.
#[tokio::test]
async fn refusal_and_unreadable_answer_end_at_once_without_the_answer_or_key() {
    const BODY: &str = "the server's own detail";
    for protocol in [
        Protocol::ChatCompletions,
        Protocol::OpenAiResponses,
        Protocol::Anthropic,
    ] {
        for (answer, expected) in [
            (
                ResponseTemplate::new(400).set_body_string(BODY),
                ModelError::Refused(StatusCode::BAD_REQUEST),
            ),
            (
                ResponseTemplate::new(200).set_body_string(BODY),
                ModelError::Misconfigured,
            ),
            (
                ResponseTemplate::new(200).set_body_json(json!({})),
                ModelError::Misconfigured,
            ),
        ] {
            let server = MockServer::start().await;
            Mock::given(any()).respond_with(answer).mount(&server).await;

            let error = ask(&client_for(&server, protocol, Some(KEY)))
                .await
                .unwrap_err();

            assert_eq!(error, expected, "{protocol:?}");
            assert_eq!(received(&server).await.len(), 1, "{protocol:?}");
            let text = error.to_string();
            assert!(!text.contains(BODY) && !text.contains(KEY), "{text}");
        }
    }
}
