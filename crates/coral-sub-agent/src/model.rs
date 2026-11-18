//! The model behind the Sub-agent. This file is a stand-in that calls no
//! model: it replies with the conversation it was sent.

use coral_outbound::DestinationPolicy;
use coral_secret_store::Secret;
use url::Url;

/// The model that a Sub-agent's Turn calls.
#[expect(dead_code, reason = "the stand-in sends the model nothing")]
pub struct ModelClient {
    policy: DestinationPolicy,
    base_url: Url,
    model: String,
    api_key: Option<Secret>,
}

/// Who wrote one message of a conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Coral, before the conversation starts.
    System,
    /// The lead.
    User,
    /// The model.
    Assistant,
}

/// One message of the conversation that the model reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// Who wrote it.
    pub role: Role,
    /// What it says.
    pub text: String,
}

/// Why the model gave no reply.
#[derive(Debug, thiserror::Error)]
pub enum ModelError {
    /// The model server refused the request.
    #[error("The model server refused the request.")]
    Refused,
    /// The model server did not answer.
    #[error("The model server could not be reached.")]
    Unreachable,
    /// The model server answered with something other than a reply.
    #[error("The reply of the model server could not be read.")]
    UnreadableReply,
}

impl ModelClient {
    /// A client of the model `model` at `base_url`, which sends `api_key` as
    /// its bearer token under `policy`.
    #[must_use]
    pub const fn new(
        policy: DestinationPolicy,
        base_url: Url,
        model: String,
        api_key: Option<Secret>,
    ) -> Self {
        Self {
            policy,
            base_url,
            model,
            api_key,
        }
    }

    /// Sends the conversation to the model and returns the text of its reply.
    ///
    /// # Errors
    /// Returns why the model gave no reply.
    #[expect(clippy::unused_async, reason = "the stand-in sends the model nothing")]
    pub async fn reply(&self, conversation: &[Message]) -> Result<String, ModelError> {
        let texts: Vec<&str> = conversation
            .iter()
            .map(|message| message.text.as_str())
            .collect();
        Ok(texts.join("\n\n"))
    }
}
