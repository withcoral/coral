//! The Sub-agent's model, reached through an OpenAI-compatible Chat Completions endpoint.

use std::time::Duration;

use coral_credentials::Secret;
use coral_outbound::{FetchError, OutboundUrlPolicyError};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use url::Url;

/// A model behind an OpenAI-compatible Chat Completions endpoint.
pub(crate) struct ModelClient {
    http: coral_outbound::Client,
    completions_url: String,
    model: String,
    api_key: Option<Secret>,
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
    ) -> Self {
        Self {
            http,
            completions_url: format!(
                "{}/chat/completions",
                base_url.as_str().trim_end_matches('/')
            ),
            model,
            api_key,
            retry_waits: RETRY_WAITS,
        }
    }

    /// Sends the conversation to the model and returns the text of its reply.
    /// # Errors
    /// Returns why the model gave no reply.
    pub(crate) async fn reply(&self, conversation: &[Message]) -> Result<String, ModelError> {
        let request = CompletionRequest {
            model: &self.model,
            messages: conversation,
        };
        let mut result = self.send(&request).await;
        for wait in self.retry_waits {
            if !matches!(result, Err(ModelError::Unavailable)) {
                break;
            }
            tokio::time::sleep(wait).await;
            result = self.send(&request).await;
        }
        result
    }

    async fn send(&self, request: &CompletionRequest<'_>) -> Result<String, ModelError> {
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
                FetchError::Http(_) => ModelError::Unavailable,
            })?;
        match response.status() {
            status if status.is_success() => {}
            status if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() => {
                return Err(ModelError::Unavailable);
            }
            status => return Err(ModelError::Refused(status)),
        }
        let body = coral_outbound::read_bounded_body(response, BODY_LIMIT)
            .await
            .map_err(|error| match error {
                OutboundUrlPolicyError::BodyTooLarge { .. } => ModelError::Misconfigured,
                _ => ModelError::Unavailable,
            })?;
        let completion: Completion =
            serde_json::from_slice(&body).map_err(|_error| ModelError::Misconfigured)?;
        completion
            .choices
            .into_iter()
            .next()
            .map(|choice| choice.message.content.unwrap_or_default())
            .ok_or(ModelError::Misconfigured)
    }
}

/// Who wrote a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Role {
    /// The instructions the model follows.
    System,
    /// The one the model answers.
    User,
    /// The model.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "a Turn sends the model none of its own replies yet"
        )
    )]
    Assistant,
}

/// One message of a conversation with the model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Message {
    /// Who wrote it.
    pub(crate) role: Role,
    /// What it says.
    #[serde(rename = "content")]
    pub(crate) text: String,
}

/// Why the model gave no reply. The text never holds the server's answer, the key or the conversation.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ModelError {
    /// The server could not be reached, or stayed busy or failing, through every retry.
    #[error("the model server is unavailable")]
    Unavailable,
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
    messages: &'a [Message],
}

#[derive(Deserialize)]
struct Completion {
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    message: ChoiceMessage,
}

#[derive(Deserialize)]
struct ChoiceMessage {
    /// `null` when the model wrote no text.
    content: Option<String>,
}

#[cfg(test)]
mod tests;
