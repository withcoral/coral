//! The model behind the agent, behind a trait so tests script it.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::toolbox::ToolSpec;

/// Who wrote one transcript message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The caller, or tool results returned to the model.
    User,
    /// The model.
    Assistant,
}

impl Role {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "user" => Some(Self::User),
            "assistant" => Some(Self::Assistant),
            _ => None,
        }
    }
}

/// One transcript message: a role and its content blocks, kept verbatim.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    /// Who wrote it.
    pub role: Role,
    /// Content blocks in the Messages API shape. Thinking blocks stay unchanged.
    pub content: Vec<Value>,
}

/// One request to the model.
#[derive(Debug, Clone)]
pub struct ModelRequest {
    /// Stable instructions. Kept first so the provider can cache them.
    pub system: String,
    /// The tools the model may call.
    pub tools: Vec<ToolSpec>,
    /// The whole thread so far, oldest first.
    pub messages: Vec<Message>,
}

/// Why the model stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// It finished its message.
    EndTurn,
    /// It wants tool results.
    ToolUse,
    /// It ran out of output tokens.
    MaxTokens,
    /// It declined the request.
    Refusal,
    /// Anything else the provider reports.
    Other,
}

/// The model's reply.
#[derive(Debug, Clone)]
pub struct ModelResponse {
    /// Content blocks, appended to the transcript unchanged.
    pub content: Vec<Value>,
    /// Why it stopped.
    pub stop_reason: StopReason,
    /// Token counts as the provider reported them, when it did.
    pub usage: Option<Value>,
}

/// A model call failure. The message is safe to show the caller.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct ModelError(pub String);

/// The model the agent runs on.
#[async_trait]
pub trait Model: Send + Sync {
    /// Returns the model's next message for the thread.
    ///
    /// # Errors
    ///
    /// Returns a safe message when the model cannot answer.
    async fn respond(&self, request: &ModelRequest) -> Result<ModelResponse, ModelError>;
}

/// Operator settings for the Anthropic Messages API, resolved at bootstrap.
#[derive(Clone)]
pub struct AnthropicSettings {
    /// API key. Never logged.
    pub api_key: String,
    /// Model identifier, such as `claude-haiku-5-5`.
    pub model: String,
    /// `output_config.effort`, such as `medium`.
    pub effort: String,
    /// Whether the model thinks before it answers. Off sends `thinking: disabled`,
    /// which models accept only at effort `high` or below.
    pub thinking: bool,
}

impl std::fmt::Debug for AnthropicSettings {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AnthropicSettings")
            .field("model", &self.model)
            .field("effort", &self.effort)
            .field("thinking", &self.thinking)
            .finish_non_exhaustive()
    }
}

const ANTHROPIC_MESSAGES_URL: &str = "https://api.anthropic.com/v1/messages";
const MAX_TOKENS: u32 = 16_000;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
const RETRIES: u32 = 3;

/// The Anthropic Messages API over Coral's guarded outbound client.
pub struct AnthropicModel {
    client: coral_outbound::Client,
    settings: AnthropicSettings,
}

impl AnthropicModel {
    /// Builds the client under the operator's destination policy.
    ///
    /// # Errors
    ///
    /// Returns an HTTP client initialization error.
    pub fn new(
        policy: coral_outbound::DestinationPolicy,
        settings: AnthropicSettings,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: coral_outbound::Client::new(policy, REQUEST_TIMEOUT)?,
            settings,
        })
    }

    fn body(&self, request: &ModelRequest) -> Value {
        let tools = request
            .tools
            .iter()
            .map(|tool| {
                json!({
                    "name": tool.name,
                    "description": tool.description,
                    "input_schema": tool.input_schema,
                })
            })
            .collect::<Vec<_>>();
        let messages = request
            .messages
            .iter()
            .map(|message| json!({"role": message.role.as_str(), "content": message.content}))
            .collect::<Vec<_>>();
        let mut body = serde_json::Map::new();
        body.insert("model".into(), json!(self.settings.model));
        body.insert("max_tokens".into(), json!(MAX_TOKENS));
        // Caches the tools, system prompt and thread prefix across steps.
        body.insert("cache_control".into(), json!({"type": "ephemeral"}));
        body.insert(
            "output_config".into(),
            json!({"effort": self.settings.effort}),
        );
        // Leaving `thinking` out keeps the model's default, adaptive thinking.
        if !self.settings.thinking {
            body.insert("thinking".into(), json!({"type": "disabled"}));
        }
        body.insert("system".into(), json!(request.system));
        body.insert("tools".into(), Value::Array(tools));
        body.insert("messages".into(), Value::Array(messages));
        Value::Object(body)
    }
}

#[async_trait]
impl Model for AnthropicModel {
    async fn respond(&self, request: &ModelRequest) -> Result<ModelResponse, ModelError> {
        let body = self.body(request);
        let mut attempt = 0;
        loop {
            attempt += 1;
            let response = self
                .client
                .post(ANTHROPIC_MESSAGES_URL)
                .map_err(|_rejected| ModelError("The model endpoint is not allowed".into()))?
                .header("x-api-key", &self.settings.api_key)
                .header("anthropic-version", "2023-06-01")
                .json(&body)
                .send()
                .await;
            let response = match response {
                Ok(response) => response,
                Err(error) if attempt <= RETRIES => {
                    tracing::warn!(%error, attempt, "model request failed; retrying");
                    tokio::time::sleep(backoff(attempt)).await;
                    continue;
                }
                Err(_error) => return Err(ModelError("The model could not be reached".into())),
            };
            let status = response.status();
            let payload: Value = response
                .json()
                .await
                .map_err(|_error| ModelError("The model returned an unreadable reply".into()))?;
            if status.is_success() {
                return parse_response(&payload);
            }
            let retryable = status.as_u16() == 429 || status.is_server_error();
            if retryable && attempt <= RETRIES {
                tracing::warn!(%status, attempt, "model request rejected; retrying");
                tokio::time::sleep(backoff(attempt)).await;
                continue;
            }
            let detail = payload
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("no detail");
            tracing::warn!(%status, detail, "model request failed");
            return Err(ModelError(format!("The model request failed ({status})")));
        }
    }
}

fn backoff(attempt: u32) -> Duration {
    Duration::from_secs(2_u64.saturating_pow(attempt))
}

fn parse_response(payload: &Value) -> Result<ModelResponse, ModelError> {
    let content = payload
        .get("content")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| ModelError("The model reply had no content".into()))?;
    let stop_reason = match payload.get("stop_reason").and_then(Value::as_str) {
        Some("end_turn" | "stop_sequence") => StopReason::EndTurn,
        Some("tool_use") => StopReason::ToolUse,
        Some("max_tokens") => StopReason::MaxTokens,
        Some("refusal") => StopReason::Refusal,
        _ => StopReason::Other,
    };
    Ok(ModelResponse {
        content,
        stop_reason,
        usage: payload.get("usage").cloned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thinking_off_disables_it_and_thinking_on_keeps_the_model_default() {
        for (thinking, expected) in [(true, None), (false, Some(json!({"type": "disabled"})))] {
            let model = AnthropicModel::new(
                coral_outbound::DestinationPolicy::default(),
                AnthropicSettings {
                    api_key: "test-key".into(),
                    model: "test-model".into(),
                    effort: "medium".into(),
                    thinking,
                },
            )
            .expect("client");
            let body = model.body(&ModelRequest {
                system: String::new(),
                tools: Vec::new(),
                messages: Vec::new(),
            });
            assert_eq!(
                body.get("thinking").cloned(),
                expected,
                "thinking = {thinking}"
            );
        }
    }
}
