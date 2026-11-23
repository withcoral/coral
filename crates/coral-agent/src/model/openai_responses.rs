//! `OpenAI` Responses API wire codec (`/responses`), stateless with encrypted
//! reasoning items round-tripped verbatim.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::prompt::ToolText;

use super::{
    Block, Message, ModelCall, ModelError, Protocol, Reply, Role, ToolCall, Usage,
    reasoning_matches, tool_parameters,
};

/// Serializes an `OpenAI` Responses API request body.
pub(super) fn request_body(
    model: &str,
    system: &str,
    conversation: &[Message],
    tools: &[ToolText],
    max_tokens: Option<u32>,
    reasoning_effort: Option<&str>,
) -> Result<Vec<u8>, ModelError> {
    let include_encrypted = reasoning_effort != Some("none");
    let request = ResponsesRequest {
        model,
        store: false,
        instructions: system,
        input: input_items(conversation, model),
        tools: tools.iter().map(ResponsesTool::from).collect(),
        max_output_tokens: max_tokens,
        reasoning: reasoning_effort.map(|effort| ReasoningConfig { effort }),
        include: if include_encrypted {
            vec!["reasoning.encrypted_content"]
        } else {
            Vec::new()
        },
    };
    serde_json::to_vec(&request).map_err(|_error| ModelError::Misconfigured)
}

/// Parses an `OpenAI` Responses API response body into a [`Reply`].
pub(super) fn parse_reply(
    body: &[u8],
    configured_model: &str,
    latency: Duration,
) -> Result<Reply, ModelError> {
    let response: ResponsesReply =
        serde_json::from_slice(body).map_err(|_error| ModelError::Misconfigured)?;
    let mut blocks = Vec::new();
    let mut has_tool_calls = false;
    for item in response.output {
        match item.get("type").and_then(Value::as_str) {
            Some("reasoning") => {
                if let Some(block) = parse_reasoning(&item, configured_model) {
                    blocks.push(block);
                }
            }
            Some("message") => {
                if let Some(text) = parse_message_text(&item) {
                    blocks.push(Block::Text { text });
                }
            }
            Some("function_call") => {
                let call = parse_function_call(item)?;
                has_tool_calls = true;
                blocks.push(Block::ToolUse(call));
            }
            _ => {}
        }
    }
    let finish_reason = normalize_finish_reason(
        response.status.as_deref(),
        response
            .incomplete_details
            .as_ref()
            .and_then(|details| details.reason.as_deref()),
        has_tool_calls,
    );
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

/// Converts the stored thread into stateless Responses API `input` items.
fn input_items(conversation: &[Message], model: &str) -> Vec<Value> {
    let mut items = Vec::new();
    for message in conversation {
        match message.role {
            Role::User => {
                let mut text_parts = Vec::new();
                for block in &message.content {
                    match block {
                        Block::Text { text } => {
                            text_parts.push(json!({"type": "input_text", "text": text}));
                        }
                        Block::ToolResult {
                            tool_use_id,
                            content,
                            ..
                        } => {
                            items.push(json!({
                                "type": "function_call_output",
                                "call_id": tool_use_id,
                                "output": content,
                            }));
                        }
                        Block::Reasoning { .. } | Block::ToolUse(_) | Block::Summary { .. } => {}
                    }
                }
                if !text_parts.is_empty() {
                    items.push(json!({
                        "type": "message",
                        "role": "user",
                        "content": text_parts,
                    }));
                }
            }
            Role::Assistant => {
                let mut text_parts = Vec::new();
                for block in &message.content {
                    match block {
                        Block::Reasoning {
                            model: writer,
                            protocol,
                            opaque: Some(item),
                            ..
                        } if reasoning_matches(
                            writer,
                            *protocol,
                            Protocol::OpenAiResponses,
                            model,
                        ) =>
                        {
                            flush_assistant_text(&mut items, &mut text_parts);
                            items.push(item.clone());
                        }
                        Block::Text { text } if !text.is_empty() => {
                            text_parts.push(json!({"type": "output_text", "text": text}));
                        }
                        Block::ToolUse(call) => {
                            flush_assistant_text(&mut items, &mut text_parts);
                            items.push(json!({
                                "type": "function_call",
                                "call_id": call.id,
                                "name": call.name,
                                "arguments": call.arguments,
                            }));
                        }
                        Block::Reasoning { .. }
                        | Block::Text { .. }
                        | Block::ToolResult { .. }
                        | Block::Summary { .. } => {}
                    }
                }
                flush_assistant_text(&mut items, &mut text_parts);
            }
        }
    }
    items
}

fn flush_assistant_text(items: &mut Vec<Value>, text_parts: &mut Vec<Value>) {
    if !text_parts.is_empty() {
        items.push(json!({
            "type": "message",
            "role": "assistant",
            "content": std::mem::take(text_parts),
        }));
    }
}

fn parse_reasoning(item: &Value, configured_model: &str) -> Option<Block> {
    let summary_array = item.get("summary").and_then(Value::as_array);
    let summary_texts: Vec<&str> = summary_array
        .into_iter()
        .flatten()
        .filter_map(|part| {
            let kind = part.get("type").and_then(Value::as_str);
            if matches!(kind, None | Some("summary_text")) {
                part.get("text").and_then(Value::as_str)
            } else {
                None
            }
        })
        .filter(|text| !text.is_empty())
        .collect();
    let text = summary_texts.join("\n");
    let encrypted = item
        .get("encrypted_content")
        .and_then(Value::as_str)
        .filter(|content| !content.is_empty());
    let opaque = encrypted.map(|encrypted_content| {
        let mut replay = Map::new();
        replay.insert("type".to_owned(), Value::String("reasoning".to_owned()));
        if let Some(id) = item.get("id").and_then(Value::as_str) {
            replay.insert("id".to_owned(), Value::String(id.to_owned()));
        }
        replay.insert(
            "encrypted_content".to_owned(),
            Value::String(encrypted_content.to_owned()),
        );
        let summary = summary_array.cloned().unwrap_or_default();
        replay.insert("summary".to_owned(), Value::Array(summary));
        Value::Object(replay)
    });
    if text.is_empty() && opaque.is_none() {
        return None;
    }
    Some(Block::Reasoning {
        text,
        model: configured_model.to_owned(),
        protocol: Some(Protocol::OpenAiResponses),
        opaque,
    })
}

fn parse_message_text(item: &Value) -> Option<String> {
    let parts = item.get("content")?.as_array()?;
    let text: String = parts
        .iter()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("output_text"))
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect();
    (!text.is_empty()).then_some(text)
}

fn parse_function_call(item: Value) -> Result<ToolCall, ModelError> {
    #[derive(Deserialize)]
    struct FunctionCallItem {
        call_id: String,
        name: String,
        arguments: String,
    }
    let call: FunctionCallItem =
        serde_json::from_value(item).map_err(|_error| ModelError::Misconfigured)?;
    Ok(ToolCall {
        id: call.call_id,
        name: call.name,
        arguments: call.arguments,
    })
}

fn normalize_finish_reason(
    status: Option<&str>,
    incomplete_reason: Option<&str>,
    has_tool_calls: bool,
) -> Option<String> {
    match status? {
        "completed" if has_tool_calls => Some("tool_calls".to_owned()),
        "completed" => Some("stop".to_owned()),
        "incomplete" => match incomplete_reason {
            Some("max_output_tokens") => Some("length".to_owned()),
            Some("content_filter") => Some("content_filter".to_owned()),
            Some(other) => Some(other.to_owned()),
            None => Some("incomplete".to_owned()),
        },
        other => Some(other.to_owned()),
    }
}

#[derive(Serialize)]
struct ResponsesRequest<'a> {
    model: &'a str,
    store: bool,
    instructions: &'a str,
    input: Vec<Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<ResponsesTool<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_output_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<ReasoningConfig<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    include: Vec<&'static str>,
}

#[derive(Serialize)]
struct ReasoningConfig<'a> {
    effort: &'a str,
}

#[derive(Serialize)]
#[serde(tag = "type", rename = "function")]
struct ResponsesTool<'a> {
    name: &'a str,
    description: &'a str,
    parameters: Value,
    strict: bool,
}

impl<'a> From<&'a ToolText> for ResponsesTool<'a> {
    fn from(tool: &'a ToolText) -> Self {
        Self {
            name: tool.name,
            description: tool.description,
            parameters: tool_parameters(tool),
            strict: false,
        }
    }
}

#[derive(Deserialize)]
struct ResponsesReply {
    model: Option<String>,
    status: Option<String>,
    incomplete_details: Option<IncompleteDetails>,
    output: Vec<Value>,
    usage: Option<ResponsesUsage>,
}

#[derive(Deserialize)]
struct IncompleteDetails {
    reason: Option<String>,
}

#[derive(Deserialize)]
struct ResponsesUsage {
    input_tokens: i64,
    output_tokens: i64,
    input_tokens_details: Option<InputTokensDetails>,
}

#[derive(Deserialize)]
struct InputTokensDetails {
    cached_tokens: Option<i64>,
}

impl From<ResponsesUsage> for Usage {
    fn from(usage: ResponsesUsage) -> Self {
        let cached = usage
            .input_tokens_details
            .and_then(|details| details.cached_tokens)
            .unwrap_or_default();
        Self {
            input_tokens: usage.input_tokens.saturating_sub(cached),
            cache_read_input_tokens: cached,
            output_tokens: usage.output_tokens,
        }
    }
}
