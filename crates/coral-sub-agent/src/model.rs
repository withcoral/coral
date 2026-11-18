//! The Sub-agent's model, reached through an OpenAI-compatible Chat Completions endpoint.
//! A conversation is kept in Coral's own block format and converted here on every call.

use std::time::Duration;

use coral_credentials::Secret;
use coral_outbound::{FetchError, OutboundUrlPolicyError};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::Instant;
use url::Url;

use crate::prompt::ToolText;
use crate::telemetry::ModelCallTrace;

/// A model behind an OpenAI-compatible Chat Completions endpoint.
pub(crate) struct ModelClient {
    http: coral_outbound::Client,
    completions_url: String,
    model: String,
    api_key: Option<Secret>,
    context_window: Option<u32>,
    retry_waits: [Duration; 3],
}

impl ModelClient {
    /// Reaches `model` under `base_url`, such as `https://host/v1`, through `http`.
    #[must_use]
    pub(crate) fn new(
        http: coral_outbound::Client,
        base_url: &Url,
        model: String,
        api_key: Option<Secret>,
        context_window: Option<u32>,
    ) -> Self {
        Self {
            http,
            completions_url: format!(
                "{}/chat/completions",
                base_url.as_str().trim_end_matches('/')
            ),
            model,
            api_key,
            context_window,
            retry_waits: RETRY_WAITS,
        }
    }

    /// The model's context window in tokens, when the operator set it.
    pub(crate) const fn context_window(&self) -> Option<u32> {
        self.context_window
    }

    /// Sends `system`, the conversation and the tools the model may call, and returns its reply.
    /// # Errors
    /// Returns why the model gave no reply.
    pub(crate) async fn reply(
        &self,
        system: &str,
        conversation: &[Message],
        tools: &[ToolText],
    ) -> Result<Reply, ModelError> {
        let request = CompletionRequest {
            model: &self.model,
            messages: chat_messages(system, conversation, &self.model),
            tools: tools.iter().map(ChatTool::from).collect(),
            max_completion_tokens: None,
        };
        self.complete(&request).await
    }

    /// Sends `system` and the conversation without tools, for a reply of at most `max_tokens`.
    /// # Errors
    /// Returns why the model gave no reply.
    pub(crate) async fn summarize(
        &self,
        system: &str,
        conversation: &[Message],
        max_tokens: u32,
    ) -> Result<Reply, ModelError> {
        let request = CompletionRequest {
            model: &self.model,
            messages: chat_messages(system, conversation, &self.model),
            tools: Vec::new(),
            max_completion_tokens: Some(max_tokens),
        };
        self.complete(&request).await
    }

    async fn complete(&self, request: &CompletionRequest<'_>) -> Result<Reply, ModelError> {
        let mut call = ModelCallTrace::open(&self.model, &self.completions_url);
        let mut result = self.send(request).await;
        for wait in self.retry_waits {
            if !matches!(result, Err(ModelError::Unavailable(_))) {
                break;
            }
            tokio::time::sleep(wait).await;
            call.retried();
            result = self.send(request).await;
        }
        call.ended(&result);
        result
    }

    async fn send(&self, request: &CompletionRequest<'_>) -> Result<Reply, ModelError> {
        let sent_at = Instant::now();
        let mut builder = self
            .http
            .post(&self.completions_url)
            .map_err(|_rejected| ModelError::Misconfigured)?
            .json(request);
        if let Some(key) = &self.api_key {
            builder = builder.bearer_auth(key.expose());
        }
        let response = builder
            .send()
            .await
            .map_err(|error| match FetchError::from(error) {
                FetchError::DestinationRejected => ModelError::Misconfigured,
                FetchError::Http(_) => ModelError::Unavailable(None),
            })?;
        match response.status() {
            status if status.is_success() => {}
            status if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() => {
                return Err(ModelError::Unavailable(Some(status)));
            }
            status => return Err(ModelError::Refused(status)),
        }
        let body = coral_outbound::read_bounded_body(response, BODY_LIMIT)
            .await
            .map_err(|error| match error {
                OutboundUrlPolicyError::BodyTooLarge { .. } => ModelError::Misconfigured,
                _ => ModelError::Unavailable(None),
            })?;
        let latency = sent_at.elapsed();
        let completion: Completion =
            serde_json::from_slice(&body).map_err(|_error| ModelError::Misconfigured)?;
        let choice = completion.choices.into_iter().next();
        let choice = choice.ok_or(ModelError::Misconfigured)?;
        Ok(Reply {
            message: choice.message.into_message(&self.model),
            finish_reason: choice.finish_reason,
            call: ModelCall {
                model: completion.model.unwrap_or_else(|| self.model.clone()),
                latency,
                usage: completion.usage.map(Usage::from),
            },
        })
    }
}

/// One message of a conversation, in Coral's own block format.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Message {
    pub(crate) role: Role,
    pub(crate) content: Vec<Block>,
}

impl Message {
    /// What the lead wrote.
    pub(crate) fn user(text: String) -> Self {
        Self {
            role: Role::User,
            content: vec![Block::Text { text }],
        }
    }

    /// The results of the model's tool calls, in the order it made them.
    pub(crate) fn tool_results(results: Vec<Block>) -> Self {
        Self {
            role: Role::User,
            content: results,
        }
    }

    /// The text of the message, without its tool calls.
    pub(crate) fn text(&self) -> String {
        let texts = self.content.iter().filter_map(|block| match block {
            Block::Text { text } => Some(text.as_str()),
            Block::Reasoning { .. }
            | Block::ToolUse(_)
            | Block::ToolResult { .. }
            | Block::Summary { .. } => None,
        });
        texts.collect()
    }

    /// The tool calls the model made in this message.
    pub(crate) fn tool_calls(&self) -> impl Iterator<Item = &ToolCall> {
        self.content.iter().filter_map(|block| match block {
            Block::ToolUse(call) => Some(call),
            Block::Text { .. }
            | Block::Reasoning { .. }
            | Block::ToolResult { .. }
            | Block::Summary { .. } => None,
        })
    }

    /// The reasoning in this message if `model` wrote it: a model reads back only its own.
    fn reasoning_by(&self, model: &str) -> Option<&str> {
        self.content.iter().find_map(|block| match block {
            Block::Reasoning {
                text,
                model: writer,
            } => (writer == model).then_some(text.as_str()),
            Block::Text { .. }
            | Block::ToolUse(_)
            | Block::ToolResult { .. }
            | Block::Summary { .. } => None,
        })
    }
}

/// Who wrote a message: the lead or a tool's result is the user.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Role {
    User,
    Assistant,
}

/// One part of a message. It is stored as JSON under these names.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum Block {
    Text {
        text: String,
    },
    /// What the model reasoned before it replied, and the configured `model` that wrote it.
    Reasoning {
        text: String,
        model: String,
    },
    ToolUse(ToolCall),
    ToolResult {
        tool_use_id: String,
        content: String,
    },
    /// The model's summary of the thread's older messages, which later calls read in their place.
    Summary {
        text: String,
        /// The position of the first message that the summary leaves whole.
        kept_from: usize,
    },
}

/// A tool call that the model made.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ToolCall {
    pub(crate) id: String,
    pub(crate) name: String,
    /// The arguments as the model wrote them, which may not be JSON.
    pub(crate) arguments: String,
}

impl ToolCall {
    /// The block that answers this call with `content`.
    pub(crate) fn result(&self, content: String) -> Block {
        Block::ToolResult {
            tool_use_id: self.id.clone(),
            content,
        }
    }
}

/// The model's reply, and the record of the call that produced it.
#[derive(Debug)]
pub(crate) struct Reply {
    pub(crate) message: Message,
    pub(crate) call: ModelCall,
    /// Why the model stopped, as the server wrote it.
    pub(crate) finish_reason: Option<String>,
}

/// The record of one model call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ModelCall {
    /// The model that answered, as the server named it.
    pub(crate) model: String,
    pub(crate) latency: Duration,
    /// `None` when the server reported no usage.
    pub(crate) usage: Option<Usage>,
}

/// The tokens one model call used. Cached input is counted apart from `input_tokens`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(
    clippy::struct_field_names,
    reason = "the names Coral's usage record keeps"
)]
pub(crate) struct Usage {
    pub(crate) input_tokens: i64,
    pub(crate) cache_read_input_tokens: i64,
    pub(crate) output_tokens: i64,
}

/// Why the model gave no reply. The text never holds the server's answer, the key or the conversation.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ModelError {
    /// The server could not be reached (`None`), or stayed busy or failing with this status,
    /// through every retry.
    #[error("the model server is unavailable")]
    Unavailable(Option<StatusCode>),
    /// The server refused the request.
    #[error("the model server refused the request with status {0}")]
    Refused(StatusCode),
    /// The destination policy forbids the server, or its answer is not a chat completion.
    #[error("the model settings do not lead to a Chat Completions endpoint Coral may use")]
    Misconfigured,
}

/// The waits before each retry of a send that failed or an answer of 429 or 5xx.
const RETRY_WAITS: [Duration; 3] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
];

/// The largest answer Coral reads from the model server.
const BODY_LIMIT: usize = 4 << 20;

#[derive(Serialize)]
struct CompletionRequest<'a> {
    model: &'a str,
    messages: Vec<ChatMessage<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<ChatTool<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_completion_tokens: Option<u32>,
}

/// The conversation as Chat Completions messages: a user's text as content parts, an assistant
/// message with only tool calls as `null` content, and an empty assistant message left out.
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
                        reasoning: message.reasoning_by(model),
                        tool_calls,
                    });
                }
            }
        }
    }
    messages
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
                parameters: serde_json::from_str(tool.parameters)
                    .expect("a tool's parameters are JSON, as the prompt's tests check"),
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

#[cfg(test)]
mod tests;
