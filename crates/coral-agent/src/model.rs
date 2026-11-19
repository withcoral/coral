//! The Sub-agent's model, reached through Chat Completions, `OpenAI` Responses,
//! or Anthropic Messages endpoints over Coral's guarded outbound client. A
//! conversation is kept in Coral's own block format and converted here on every
//! call.

mod anthropic;
mod chat_completions;
mod openai_responses;
mod refusal;

use std::time::Duration;

use coral_credentials::Secret;
use coral_outbound::{FetchError, OutboundUrlPolicyError};
use coral_run::RunId;
use refusal::Refusal;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::Instant;
use url::Url;

use crate::prompt::ToolText;
use crate::telemetry::ModelCallTrace;

/// The wire protocol a configured model endpoint speaks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Protocol {
    /// OpenAI-compatible Chat Completions (`/chat/completions`).
    #[default]
    ChatCompletions,
    /// `OpenAI` Responses API (`/responses`), stateless with encrypted reasoning.
    #[serde(rename = "openai_responses")]
    OpenAiResponses,
    /// Anthropic Messages API (`/messages`), with prompt caching and thinking blocks.
    Anthropic,
}

impl Protocol {
    const fn path_suffix(self) -> &'static str {
        match self {
            Self::ChatCompletions => "chat/completions",
            Self::OpenAiResponses => "responses",
            Self::Anthropic => "messages",
        }
    }
}

/// A model behind a Chat Completions, `OpenAI` Responses, or Anthropic Messages endpoint.
pub(crate) struct ModelClient {
    http: coral_outbound::Client,
    protocol: Protocol,
    endpoint_url: String,
    model: String,
    api_key: Option<Secret>,
    context_window: Option<u32>,
    /// Sent as written on every request: servers accept different values.
    reasoning_effort: Option<String>,
    retry_waits: [Duration; 3],
}

impl ModelClient {
    /// Reaches `model` under `base_url`, such as `https://host/v1`, speaking `protocol` through `http`.
    #[must_use]
    pub(crate) fn new(
        http: coral_outbound::Client,
        protocol: Protocol,
        base_url: &Url,
        model: String,
        api_key: Option<Secret>,
        context_window: Option<u32>,
        reasoning_effort: Option<String>,
    ) -> Self {
        let base = base_url.as_str().trim_end_matches('/');
        Self {
            http,
            protocol,
            endpoint_url: format!("{base}/{}", protocol.path_suffix()),
            model,
            api_key,
            context_window,
            reasoning_effort,
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
        let body = self.encode_request(system, conversation, tools, None)?;
        self.complete(&body).await
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
        let body = self.encode_request(system, conversation, &[], Some(max_tokens))?;
        self.complete(&body).await
    }

    fn encode_request(
        &self,
        system: &str,
        conversation: &[Message],
        tools: &[ToolText],
        max_tokens: Option<u32>,
    ) -> Result<Vec<u8>, ModelError> {
        let effort = self.reasoning_effort.as_deref();
        match self.protocol {
            Protocol::ChatCompletions => chat_completions::request_body(
                &self.model,
                system,
                conversation,
                tools,
                max_tokens,
                effort,
            ),
            Protocol::OpenAiResponses => openai_responses::request_body(
                &self.model,
                system,
                conversation,
                tools,
                max_tokens,
                effort,
            ),
            Protocol::Anthropic => anthropic::request_body(
                &self.model,
                system,
                conversation,
                tools,
                max_tokens,
                effort,
            ),
        }
    }

    fn decode_reply(&self, body: &[u8], latency: Duration) -> Result<Reply, ModelError> {
        match self.protocol {
            Protocol::ChatCompletions => chat_completions::parse_reply(body, &self.model, latency),
            Protocol::OpenAiResponses => openai_responses::parse_reply(body, &self.model, latency),
            Protocol::Anthropic => anthropic::parse_reply(body, &self.model, latency),
        }
    }

    async fn complete(&self, request_body: &[u8]) -> Result<Reply, ModelError> {
        let mut call = ModelCallTrace::open(&self.model, &self.endpoint_url);
        let mut result = self.send(request_body).await;
        for wait in self.retry_waits {
            if !matches!(result, Err(ModelError::Unavailable(_))) {
                break;
            }
            tokio::time::sleep(wait).await;
            call.retried();
            result = self.send(request_body).await;
        }
        call.ended(&result);
        result
    }

    async fn send(&self, request_body: &[u8]) -> Result<Reply, ModelError> {
        let sent_at = Instant::now();
        let mut builder = self
            .http
            .post(&self.endpoint_url)
            .map_err(|_rejected| ModelError::Misconfigured)?
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(request_body.to_vec());
        match self.protocol {
            Protocol::ChatCompletions | Protocol::OpenAiResponses => {
                if let Some(key) = &self.api_key {
                    builder = builder.bearer_auth(key.expose());
                }
            }
            Protocol::Anthropic => {
                builder = builder.header("anthropic-version", anthropic::VERSION);
                if let Some(key) = &self.api_key {
                    builder = builder.header("x-api-key", key.expose());
                }
            }
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
            status => {
                // A 413 is too long whatever its body. With another status, a body that
                // is missing or oversized reads as any other refusal.
                let body = coral_outbound::read_bounded_body(response, REFUSAL_BODY_LIMIT).await;
                return Err(match Refusal::read(status, &body.unwrap_or_default()) {
                    Refusal::ContextTooLong => ModelError::ContextTooLong(status),
                    Refusal::Other => ModelError::Refused(status),
                });
            }
        }
        let body = coral_outbound::read_bounded_body(response, BODY_LIMIT)
            .await
            .map_err(|error| match error {
                OutboundUrlPolicyError::BodyTooLarge { .. } => ModelError::Misconfigured,
                _ => ModelError::Unavailable(None),
            })?;
        let latency = sent_at.elapsed();
        self.decode_reply(&body, latency)
    }
}

/// Parses a tool's static JSON schema string into a [`Value`].
fn tool_parameters(tool: &ToolText) -> Value {
    serde_json::from_str(tool.parameters)
        .expect("a tool's parameters are JSON, as the prompt's tests check")
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
    /// What the model reasoned before it replied, and the configured `model`
    /// (and `protocol`) that wrote it.
    Reasoning {
        #[serde(default)]
        text: String,
        model: String,
        /// The protocol that wrote this block; absent in older Chat Completions rows.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        protocol: Option<Protocol>,
        /// Opaque protocol payload needed to replay this reasoning verbatim
        /// (e.g. `OpenAI` Responses `encrypted_content`, Anthropic thinking
        /// signature or `redacted_thinking` data).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        opaque: Option<Value>,
    },
    ToolUse(ToolCall),
    ToolResult {
        tool_use_id: String,
        content: String,
        /// Whether the tool call returned an error rather than a normal result.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        is_error: bool,
        /// The Run that an `exec` started.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run_id: Option<String>,
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
    /// The block that answers this call with `content`, whether it errored, and the Run it started.
    pub(crate) fn result(&self, content: String, is_error: bool, run_id: Option<RunId>) -> Block {
        Block::ToolResult {
            tool_use_id: self.id.clone(),
            content,
            is_error,
            run_id: run_id.map(|run_id| run_id.as_str().to_owned()),
        }
    }
}

/// The model's reply, and the record of the call that produced it.
#[derive(Debug)]
pub(crate) struct Reply {
    pub(crate) message: Message,
    pub(crate) call: ModelCall,
    /// Why the model stopped, as the server wrote it (or normalized for telemetry).
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
    /// The server refused the request because it does not fit the model's context window.
    #[error("the conversation is too long for the model: start a new agent with a narrower brief")]
    ContextTooLong(StatusCode),
    /// The destination policy forbids the server, or its answer could not be read for the configured protocol.
    #[error("the model settings do not lead to a model endpoint Coral may use")]
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

/// The most of a refusal that Coral reads to learn whether the request was too long.
const REFUSAL_BODY_LIMIT: usize = 64 << 10;

#[cfg(test)]
mod tests;
