use std::time::Duration;

use coral_credentials::Secret;
use coral_outbound::{DestinationPolicy, NetworkAccess};
use reqwest::StatusCode;
use serde_json::json;
use url::Url;
use wiremock::matchers::{any, body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{Block, Message, ModelCall, ModelClient, ModelError, Reply, Role, ToolCall, Usage};
use crate::prompt::ToolText;

const MODEL: &str = "test-model";
const KEY: &str = "model-key";
const TOOLS: [ToolText; 1] = [ToolText {
    name: "exec",
    description: "Run a Program.",
    parameters: r#"{"type":"object"}"#,
}];

fn client(server: &MockServer, api_key: Option<&str>) -> ModelClient {
    let http = coral_outbound::Client::new(
        DestinationPolicy::new(NetworkAccess::HttpAndHttps, NetworkAccess::Deny),
        Duration::from_secs(5),
    )
    .unwrap();
    let base_url = Url::parse(&format!("{}/v1", server.uri())).unwrap();
    let api_key = api_key.map(|key| Secret::new(key.to_owned()).unwrap());
    ModelClient {
        retry_waits: [Duration::ZERO; 3],
        ..ModelClient::new(http, &base_url, MODEL.to_owned(), api_key)
    }
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

/// A conversation with every kind of block, and an empty assistant message.
fn conversation() -> Vec<Message> {
    vec![
        Message::user("Hello?".to_owned()),
        assistant(vec![
            Block::Text {
                text: "Looking.".to_owned(),
            },
            Block::ToolUse(call("call_1")),
        ]),
        Message::tool_results(vec![call("call_1").result("1".to_owned())]),
        assistant(vec![Block::ToolUse(call("call_2"))]),
        Message::tool_results(vec![call("call_2").result("2".to_owned())]),
        assistant(Vec::new()),
    ]
}

fn answer(text: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
    }))
}

async fn ask(client: &ModelClient) -> Result<Reply, ModelError> {
    client.reply("Be brief.", &conversation(), &TOOLS).await
}

async fn received(server: &MockServer) -> Vec<wiremock::Request> {
    server.received_requests().await.unwrap()
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
    for (status, failures, requests, expected) in [
        (429, 1, 2, Ok("Hi.".to_owned())),
        (500, 3, 4, Ok("Hi.".to_owned())),
        (503, 4, 4, Err(ModelError::Unavailable)),
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

// Catches: a refusal or an unreadable answer retried, or an error's text holding the answer or the key.
#[tokio::test]
async fn refusal_and_unreadable_answer_end_at_once_without_the_answer_or_key() {
    const BODY: &str = "the server's own detail";
    for (answer, expected) in [
        (
            ResponseTemplate::new(400).set_body_string(BODY),
            ModelError::Refused(StatusCode::BAD_REQUEST),
        ),
        (
            ResponseTemplate::new(200).set_body_string(BODY),
            ModelError::Misconfigured,
        ),
    ] {
        let server = MockServer::start().await;
        Mock::given(any()).respond_with(answer).mount(&server).await;

        let error = ask(&client(&server, Some(KEY))).await.unwrap_err();

        assert_eq!(error, expected);
        assert_eq!(received(&server).await.len(), 1);
        let text = error.to_string();
        assert!(!text.contains(BODY) && !text.contains(KEY), "{text}");
    }
}
