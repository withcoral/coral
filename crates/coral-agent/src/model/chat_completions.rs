//! OpenAI-compatible Chat Completions wire codec (`/chat/completions`).

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::prompt::ToolText;

use super::{
    Block, Message, ModelCall, ModelError, Protocol, Reply, Role, ToolCall, Usage, tool_parameters,
};

/// Serializes a Chat Completions request body.
pub(super) fn request_body(
    model: &str,
    system: &str,
    conversation: &[Message],
    tools: &[ToolText],
    max_tokens: Option<u32>,
    reasoning_effort: Option<&str>,
) -> Result<Vec<u8>, ModelError> {
    let request = CompletionRequest {
        model,
        messages: chat_messages(system, conversation, model),
        tools: tools.iter().map(ChatTool::from).collect(),
        max_completion_tokens: max_tokens,
        reasoning_effort,
    };
    serde_json::to_vec(&request).map_err(|_error| ModelError::Misconfigured)
}

/// Parses a Chat Completions response body into a [`Reply`].
pub(super) fn parse_reply(
    body: &[u8],
    configured_model: &str,
    latency: Duration,
) -> Result<Reply, ModelError> {
    let completion: Completion =
        serde_json::from_slice(body).map_err(|_error| ModelError::Misconfigured)?;
    let choice = completion.choices.into_iter().next();
    let choice = choice.ok_or(ModelError::Misconfigured)?;
    Ok(Reply {
        message: choice.message.into_message(configured_model),
        finish_reason: choice.finish_reason,
        call: ModelCall {
            model: completion
                .model
                .unwrap_or_else(|| configured_model.to_owned()),
            latency,
            usage: completion.usage.map(Usage::from),
        },
    })
}

#[derive(Serialize)]
struct CompletionRequest<'a> {
    model: &'a str,
    messages: Vec<ChatMessage<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<ChatTool<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_completion_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_effort: Option<&'a str>,
}

/// The conversation as Chat Completions messages: a user's text as content parts, an assistant
/// message with only tool calls as `null` content, reasoning replayed to its authoring model, and
/// an empty assistant message left out.
fn chat_messages<'a>(
    system: &'a str,
    conversation: &'a [Message],
    model: &str,
) -> Vec<ChatMessage<'a>> {
    let mut messages = vec![ChatMessage::System { content: system }];
    for message in conversation {
        match message.role {
            Role::User => {
                let mut parts = Vec::new();
                for block in &message.content {
                    match block {
                        Block::Text { text } => parts.push(TextPart { text }),
                        Block::ToolResult {
                            tool_use_id,
                            content,
                            ..
                        } => messages.push(ChatMessage::Tool {
                            tool_call_id: tool_use_id,
                            content,
                        }),
                        Block::Reasoning { .. } | Block::ToolUse(_) | Block::Summary { .. } => {}
                    }
                }
                if !parts.is_empty() {
                    messages.push(ChatMessage::User { content: parts });
                }
            }
            Role::Assistant => {
                let text = message.text();
                let tool_calls: Vec<_> = message.tool_calls().map(ChatToolCall::from).collect();
                if !text.is_empty() || !tool_calls.is_empty() {
                    let content = (!text.is_empty()).then_some(text);
                    messages.push(ChatMessage::Assistant {
                        content,
                        reasoning: reasoning_by(message, model),
                        tool_calls,
                    });
                }
            }
        }
    }
    messages
}

/// The plain-text reasoning in `message` if `model` wrote it over Chat Completions.
fn reasoning_by<'a>(message: &'a Message, model: &str) -> Option<&'a str> {
    message.content.iter().find_map(|block| match block {
        Block::Reasoning {
            text,
            model: writer,
            protocol,
            ..
        } if writer == model
            && protocol.unwrap_or(Protocol::ChatCompletions) == Protocol::ChatCompletions
            && !text.is_empty() =>
        {
            Some(text.as_str())
        }
        _ => None,
    })
}

#[derive(Serialize)]
#[serde(tag = "role", rename_all = "lowercase")]
enum ChatMessage<'a> {
    System {
        content: &'a str,
    },
    User {
        content: Vec<TextPart<'a>>,
    },
    Assistant {
        content: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reasoning: Option<&'a str>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        tool_calls: Vec<ChatToolCall<&'a str>>,
    },
    Tool {
        tool_call_id: &'a str,
        content: &'a str,
    },
}

#[derive(Serialize)]
#[serde(tag = "type", rename = "text")]
struct TextPart<'a> {
    text: &'a str,
}

/// A tool call as Chat Completions writes it: borrowed in a request, owned in a reply.
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename = "function")]
struct ChatToolCall<S> {
    id: S,
    function: FunctionCall<S>,
}

#[derive(Serialize, Deserialize)]
struct FunctionCall<S> {
    name: S,
    arguments: S,
}

impl<'a> From<&'a ToolCall> for ChatToolCall<&'a str> {
    fn from(call: &'a ToolCall) -> Self {
        Self {
            id: &call.id,
            function: FunctionCall {
                name: &call.name,
                arguments: &call.arguments,
            },
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "type", rename = "function")]
struct ChatTool<'a> {
    function: FunctionDeclaration<'a>,
}

#[derive(Serialize)]
struct FunctionDeclaration<'a> {
    name: &'a str,
    description: &'a str,
    parameters: Value,
    strict: bool,
}

impl<'a> From<&'a ToolText> for ChatTool<'a> {
    fn from(tool: &'a ToolText) -> Self {
        Self {
            function: FunctionDeclaration {
                name: tool.name,
                description: tool.description,
                parameters: tool_parameters(tool),
                strict: false,
            },
        }
    }
}

#[derive(Deserialize)]
struct Completion {
    model: Option<String>,
    choices: Vec<Choice>,
    usage: Option<CompletionUsage>,
}

#[derive(Deserialize)]
struct Choice {
    message: ChoiceMessage,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct ChoiceMessage {
    /// `null` when the model wrote no text.
    content: Option<String>,
    /// vLLM's name for the reasoning; other servers write `reasoning_content`.
    reasoning: Option<String>,
    reasoning_content: Option<String>,
    /// Absent or `null` when the model called no tool.
    tool_calls: Option<Vec<ChatToolCall<String>>>,
}

impl ChoiceMessage {
    /// The reply as blocks, its reasoning marked as written by `model`.
    fn into_message(self, model: &str) -> Message {
        let reasoning = [self.reasoning, self.reasoning_content]
            .into_iter()
            .flatten()
            .find(|text| !text.is_empty())
            .map(|text| Block::Reasoning {
                text,
                model: model.to_owned(),
                protocol: Some(Protocol::ChatCompletions),
                opaque: None,
            });
        let text = self.content.filter(|text| !text.is_empty());
        let calls = self.tool_calls.unwrap_or_default().into_iter().map(|call| {
            Block::ToolUse(ToolCall {
                id: call.id,
                name: call.function.name,
                arguments: call.function.arguments,
            })
        });
        let text = text.map(|text| Block::Text { text });
        Message {
            role: Role::Assistant,
            content: reasoning.into_iter().chain(text).chain(calls).collect(),
        }
    }
}

#[derive(Deserialize)]
struct CompletionUsage {
    prompt_tokens: i64,
    completion_tokens: i64,
    prompt_tokens_details: Option<PromptTokensDetails>,
}

#[derive(Deserialize)]
struct PromptTokensDetails {
    cached_tokens: Option<i64>,
}

impl From<CompletionUsage> for Usage {
    fn from(usage: CompletionUsage) -> Self {
        let cached = usage
            .prompt_tokens_details
            .and_then(|details| details.cached_tokens)
            .unwrap_or_default();
        Self {
            input_tokens: usage.prompt_tokens.saturating_sub(cached),
            cache_read_input_tokens: cached,
            output_tokens: usage.completion_tokens,
        }
    }
}
