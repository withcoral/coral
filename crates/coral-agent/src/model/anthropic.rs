//! Anthropic Messages API wire codec (`/messages`) with prompt caching and
//! signed/redacted thinking block round-tripping.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::prompt::ToolText;

use super::{
    Block, Message, ModelCall, ModelError, Protocol, Reply, Role, ToolCall, Usage, tool_parameters,
};

/// The `anthropic-version` header required by the Messages API.
pub(super) const VERSION: &str = "2023-06-01";

/// Default `max_tokens` for reply calls (Anthropic requires the field on every request).
const DEFAULT_MAX_TOKENS: u32 = 16_384;

/// Minimum headroom above `budget_tokens` when `max_tokens` was not explicitly set.
const BUDGET_HEADROOM: u32 = 4_096;

/// Serializes an Anthropic Messages API request body.
pub(super) fn request_body(
    model: &str,
    system: &str,
    conversation: &[Message],
    tools: &[ToolText],
    max_tokens: Option<u32>,
    reasoning_effort: Option<&str>,
) -> Result<Vec<u8>, ModelError> {
    let (thinking, output_config) = reasoning_settings(reasoning_effort);
    let max_tokens = resolve_max_tokens(max_tokens, thinking.as_ref());
    let mut messages = anthropic_messages(conversation, model);
    mark_last_user_cache_breakpoint(&mut messages);
    let request = MessagesRequest {
        model,
        max_tokens,
        system: vec![SystemBlock {
            kind: "text",
            text: system,
            cache_control: CacheControl::EPHEMERAL,
        }],
        tools: tools.iter().map(AnthropicTool::from).collect(),
        messages,
        thinking,
        output_config,
    };
    serde_json::to_vec(&request).map_err(|_error| ModelError::Misconfigured)
}

/// Parses an Anthropic Messages API response body into a [`Reply`].
pub(super) fn parse_reply(
    body: &[u8],
    configured_model: &str,
    latency: Duration,
) -> Result<Reply, ModelError> {
    let response: MessagesReply =
        serde_json::from_slice(body).map_err(|_error| ModelError::Misconfigured)?;
    let mut blocks = Vec::new();
    for item in response.content {
        match item.get("type").and_then(Value::as_str) {
            Some("thinking") => {
                if let Some(block) = parse_thinking(&item, configured_model) {
                    blocks.push(block);
                }
            }
            Some("redacted_thinking") => {
                if let Some(data) = item.get("data").and_then(Value::as_str) {
                    blocks.push(Block::Reasoning {
                        text: String::new(),
                        model: configured_model.to_owned(),
                        protocol: Some(Protocol::Anthropic),
                        opaque: Some(json!({"type": "redacted_thinking", "data": data})),
                    });
                }
            }
            Some("text") => {
                if let Some(text) = item
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                {
                    blocks.push(Block::Text {
                        text: text.to_owned(),
                    });
                }
            }
            Some("tool_use") => {
                blocks.push(Block::ToolUse(parse_tool_use(item)?));
            }
            _ => {}
        }
    }
    let finish_reason = response.stop_reason.as_deref().map(normalize_stop_reason);
    Ok(Reply {
        message: Message {
            role: Role::Assistant,
            content: blocks,
        },
        finish_reason,
        call: ModelCall {
            model: response
                .model
                .unwrap_or_else(|| configured_model.to_owned()),
            latency,
            usage: response.usage.map(Usage::from),
        },
    })
}

/// Maps `reasoning_effort` to Anthropic's `thinking` and `output_config` fields:
/// - `None`: omit both so the model server's defaults apply.
/// - `"none"` / `"disabled"`: disable thinking explicitly.
/// - Unsigned integer (e.g. `"8000"`): enabled thinking with `budget_tokens`.
/// - Any other value (e.g. `"low"`, `"medium"`, `"high"`, `"max"`): adaptive
///   thinking with `output_config.effort` sent as written.
fn reasoning_settings(
    reasoning_effort: Option<&str>,
) -> (Option<Thinking>, Option<OutputConfig<'_>>) {
    let Some(effort) = reasoning_effort else {
        return (None, None);
    };
    if matches!(effort, "none" | "disabled") {
        return (Some(Thinking::Disabled), None);
    }
    if let Ok(budget_tokens) = effort.parse::<u32>() {
        return (Some(Thinking::Enabled { budget_tokens }), None);
    }
    (Some(Thinking::Adaptive), Some(OutputConfig { effort }))
}

/// Ensures `max_tokens` is strictly greater than `budget_tokens` when enabled
/// budget thinking is requested, since Anthropic rejects `max_tokens <= budget_tokens`.
fn resolve_max_tokens(max_tokens: Option<u32>, thinking: Option<&Thinking>) -> u32 {
    match (max_tokens, thinking) {
        (Some(limit), Some(Thinking::Enabled { budget_tokens })) if limit <= *budget_tokens => {
            budget_tokens.saturating_add(limit)
        }
        (Some(limit), _) => limit,
        (None, Some(Thinking::Enabled { budget_tokens })) => {
            DEFAULT_MAX_TOKENS.max(budget_tokens.saturating_add(BUDGET_HEADROOM))
        }
        (None, _) => DEFAULT_MAX_TOKENS,
    }
}

/// Converts the stored thread into alternating Anthropic `user` / `assistant`
/// messages, merging adjacent same-role messages, placing `tool_result` blocks
/// before user text, and placing signed/redacted thinking blocks before
/// assistant text and tool calls.
fn anthropic_messages(conversation: &[Message], model: &str) -> Vec<AnthropicMessage> {
    let mut messages = Vec::new();
    for message in conversation {
        match message.role {
            Role::User => push_user_message(&mut messages, &message.content),
            Role::Assistant => push_assistant_message(&mut messages, &message.content, model),
        }
    }
    messages
}

fn push_user_message(messages: &mut Vec<AnthropicMessage>, content: &[Block]) {
    let mut tool_results = Vec::new();
    let mut texts = Vec::new();
    for block in content {
        match block {
            Block::ToolResult {
                tool_use_id,
                content,
                is_error,
                ..
            } => {
                let mut item = Map::new();
                item.insert("type".to_owned(), Value::String("tool_result".to_owned()));
                item.insert("tool_use_id".to_owned(), Value::String(tool_use_id.clone()));
                item.insert("content".to_owned(), Value::String(content.clone()));
                if *is_error {
                    item.insert("is_error".to_owned(), Value::Bool(true));
                }
                tool_results.push(Value::Object(item));
            }
            Block::Text { text } if !text.is_empty() => {
                texts.push(json!({"type": "text", "text": text}));
            }
            Block::Text { .. }
            | Block::Reasoning { .. }
            | Block::ToolUse(_)
            | Block::Summary { .. } => {}
        }
    }
    if tool_results.is_empty() && texts.is_empty() {
        return;
    }
    if let Some(last) = messages.last_mut().filter(|msg| msg.role == "user") {
        // Anthropic requires every tool_result before text in a user message.
        let first_non_tool = last
            .content
            .iter()
            .position(|block| block.get("type").and_then(Value::as_str) != Some("tool_result"))
            .unwrap_or(last.content.len());
        last.content
            .splice(first_non_tool..first_non_tool, tool_results);
        last.content.extend(texts);
    } else {
        tool_results.extend(texts);
        messages.push(AnthropicMessage {
            role: "user",
            content: tool_results,
        });
    }
}

fn push_assistant_message(messages: &mut Vec<AnthropicMessage>, content: &[Block], model: &str) {
    let mut thinking_blocks = Vec::new();
    let mut other_blocks = Vec::new();
    for block in content {
        match block {
            Block::Reasoning {
                text,
                model: writer,
                protocol: Some(Protocol::Anthropic),
                opaque: Some(opaque),
            } if writer == model => {
                if let Some(replayed) = replay_thinking_block(text, opaque) {
                    thinking_blocks.push(replayed);
                }
            }
            Block::Text { text } if !text.is_empty() => {
                other_blocks.push(json!({"type": "text", "text": text}));
            }
            Block::ToolUse(call) => {
                other_blocks.push(json!({
                    "type": "tool_use",
                    "id": call.id,
                    "name": call.name,
                    "input": parse_tool_input(&call.arguments),
                }));
            }
            Block::Reasoning { .. }
            | Block::Text { .. }
            | Block::ToolResult { .. }
            | Block::Summary { .. } => {}
        }
    }
    if thinking_blocks.is_empty() && other_blocks.is_empty() {
        return;
    }
    if let Some(last) = messages.last_mut().filter(|msg| msg.role == "assistant") {
        let first_non_thinking = last
            .content
            .iter()
            .position(|block| {
                !matches!(
                    block.get("type").and_then(Value::as_str),
                    Some("thinking" | "redacted_thinking")
                )
            })
            .unwrap_or(last.content.len());
        last.content
            .splice(first_non_thinking..first_non_thinking, thinking_blocks);
        last.content.extend(other_blocks);
    } else {
        thinking_blocks.extend(other_blocks);
        messages.push(AnthropicMessage {
            role: "assistant",
            content: thinking_blocks,
        });
    }
}

fn replay_thinking_block(text: &str, opaque: &Value) -> Option<Value> {
    match opaque.get("type").and_then(Value::as_str) {
        Some("thinking") => {
            let signature = opaque
                .get("signature")
                .and_then(Value::as_str)
                .filter(|sig| !sig.is_empty())?;
            Some(json!({
                "type": "thinking",
                "thinking": text,
                "signature": signature,
            }))
        }
        Some("redacted_thinking") => {
            let data = opaque.get("data").and_then(Value::as_str)?;
            Some(json!({
                "type": "redacted_thinking",
                "data": data,
            }))
        }
        _ => None,
    }
}

/// Anthropic requires `tool_use.input` to be a JSON object. Malformed stored
/// arguments fall back to an empty object so replaying the thread stays valid.
fn parse_tool_input(arguments: &str) -> Value {
    match serde_json::from_str::<Value>(arguments) {
        Ok(Value::Object(map)) => Value::Object(map),
        _ => Value::Object(Map::new()),
    }
}

/// Places a rolling ephemeral cache breakpoint on the last block of the last
/// `user` message so each step of a Turn reads the prior conversation prefix
/// from Anthropic's prompt cache.
fn mark_last_user_cache_breakpoint(messages: &mut [AnthropicMessage]) {
    let Some(last_user) = messages.iter_mut().rev().find(|msg| msg.role == "user") else {
        return;
    };
    let Some(Value::Object(block)) = last_user.content.last_mut() else {
        return;
    };
    block.insert("cache_control".to_owned(), json!({"type": "ephemeral"}));
}

fn parse_thinking(item: &Value, configured_model: &str) -> Option<Block> {
    let text = item
        .get("thinking")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let opaque = item
        .get("signature")
        .and_then(Value::as_str)
        .filter(|sig| !sig.is_empty())
        .map(|signature| json!({"type": "thinking", "signature": signature}));
    if text.is_empty() && opaque.is_none() {
        return None;
    }
    Some(Block::Reasoning {
        text,
        model: configured_model.to_owned(),
        protocol: Some(Protocol::Anthropic),
        opaque,
    })
}

fn parse_tool_use(item: Value) -> Result<ToolCall, ModelError> {
    #[derive(Deserialize)]
    struct ToolUseBlock {
        id: String,
        name: String,
        #[serde(default)]
        input: Value,
    }
    let call: ToolUseBlock =
        serde_json::from_value(item).map_err(|_error| ModelError::Misconfigured)?;
    let arguments = match call.input {
        Value::Null => "{}".to_owned(),
        other => other.to_string(),
    };
    Ok(ToolCall {
        id: call.id,
        name: call.name,
        arguments,
    })
}

fn normalize_stop_reason(reason: &str) -> String {
    match reason {
        "end_turn" | "stop_sequence" | "pause_turn" => "stop".to_owned(),
        "tool_use" => "tool_calls".to_owned(),
        "max_tokens" => "length".to_owned(),
        "refusal" => "content_filter".to_owned(),
        other => other.to_owned(),
    }
}

#[derive(Serialize)]
struct MessagesRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    system: Vec<SystemBlock<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<AnthropicTool<'a>>,
    messages: Vec<AnthropicMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking: Option<Thinking>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_config: Option<OutputConfig<'a>>,
}

#[derive(Serialize)]
struct SystemBlock<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    text: &'a str,
    cache_control: CacheControl,
}

#[derive(Serialize)]
struct CacheControl {
    #[serde(rename = "type")]
    kind: &'static str,
}

impl CacheControl {
    const EPHEMERAL: Self = Self { kind: "ephemeral" };
}

#[derive(Serialize)]
struct AnthropicTool<'a> {
    name: &'a str,
    description: &'a str,
    input_schema: Value,
}

impl<'a> From<&'a ToolText> for AnthropicTool<'a> {
    fn from(tool: &'a ToolText) -> Self {
        Self {
            name: tool.name,
            description: tool.description,
            input_schema: tool_parameters(tool),
        }
    }
}

#[derive(Serialize)]
struct AnthropicMessage {
    role: &'static str,
    content: Vec<Value>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Thinking {
    Adaptive,
    Enabled { budget_tokens: u32 },
    Disabled,
}

#[derive(Serialize)]
struct OutputConfig<'a> {
    effort: &'a str,
}

#[derive(Deserialize)]
struct MessagesReply {
    model: Option<String>,
    content: Vec<Value>,
    stop_reason: Option<String>,
    usage: Option<AnthropicUsage>,
}

#[derive(Deserialize)]
#[expect(
    clippy::struct_field_names,
    reason = "Anthropic Messages API usage field names"
)]
struct AnthropicUsage {
    input_tokens: i64,
    #[serde(default)]
    cache_creation_input_tokens: i64,
    #[serde(default)]
    cache_read_input_tokens: i64,
    output_tokens: i64,
}

impl From<AnthropicUsage> for Usage {
    fn from(usage: AnthropicUsage) -> Self {
        // Anthropic reports `input_tokens` net of both cache creation and cache
        // read. Coral counts all non-cached-read input in `input_tokens`, and
        // cache hits in `cache_read_input_tokens`.
        Self {
            input_tokens: usage
                .input_tokens
                .saturating_add(usage.cache_creation_input_tokens),
            cache_read_input_tokens: usage.cache_read_input_tokens,
            output_tokens: usage.output_tokens,
        }
    }
}
