//! Whether a model server's refusal says that the request does not fit the
//! model's context window. Nothing else in the crate reads a refusal's body.

use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::Value;

/// What a refusal says, as far as Coral acts on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Refusal {
    /// The request does not fit the model's context window.
    ContextTooLong,
    Other,
}

impl Refusal {
    /// Reads the refusal that a server sent with `status`. The status decides
    /// first, then the reason the server names, then its message.
    pub(super) fn read(status: StatusCode, body: &[u8]) -> Self {
        if status == StatusCode::PAYLOAD_TOO_LARGE {
            return Self::ContextTooLong;
        }
        // Only a rejected request can be too long. A busy or failing server
        // (429, 5xx) is never read here, whatever its message says.
        if status != StatusCode::BAD_REQUEST {
            return Self::Other;
        }
        let Ok(body) = serde_json::from_slice::<ErrorBody>(body) else {
            return Self::Other;
        };
        let (reasons, message) = match &body {
            ErrorBody::Wrapped { error } | ErrorBody::Bare(error) => (
                [error.code.as_str(), error.kind.as_str()],
                error.message.as_str().unwrap_or_default(),
            ),
            ErrorBody::Text { error } => ([None, None], error.as_str()),
        };
        let named = reasons.iter().flatten();
        let named = named
            .into_iter()
            .any(|reason| TOO_LONG_REASONS.contains(reason));
        let message = message.to_ascii_lowercase();
        let said = TOO_LONG_PHRASES
            .iter()
            .any(|phrase| message.contains(phrase));
        if named || said {
            Self::ContextTooLong
        } else {
            Self::Other
        }
    }
}

/// The error that a Chat Completions server sends with a refusal. Servers wrap
/// it in `error`, send `error` as plain text, or send the error bare.
#[derive(Deserialize)]
#[serde(untagged)]
enum ErrorBody {
    Wrapped { error: ErrorDetail },
    Text { error: String },
    Bare(ErrorDetail),
}

/// The fields of an error that Coral reads. Each is a string from some
/// servers and a number or nothing from others, so none is required.
#[derive(Deserialize)]
struct ErrorDetail {
    #[serde(default)]
    code: Value,
    #[serde(default, rename = "type")]
    kind: Value,
    #[serde(default)]
    message: Value,
}

/// The reasons, in an error's `code` or `type`, that mean the request does
/// not fit.
const TOO_LONG_REASONS: &[&str] = &[
    "context_length_exceeded",       // OpenAI
    "request_too_large",             // Anthropic
    "model_context_window_exceeded", // z.ai
];

/// The phrases, in lower case, with which a server's message says the request
/// does not fit. They come from Pi's list of overflow errors
/// (`packages/ai/src/utils/overflow.ts` in `badlogic/pi-mono`), as plain
/// phrases in place of its patterns. Add a server's phrase with its example in
/// the test beside this file.
const TOO_LONG_PHRASES: &[&str] = &[
    "prompt is too long",                     // Anthropic
    "prompt too long",                        // z.ai, Ollama
    "prompt exceeds max length",              // z.ai
    "input is too long for requested model",  // Amazon Bedrock
    "exceeds the context window",             // OpenAI
    "maximum context length",                 // vLLM, LiteLLM, OpenRouter, Mistral
    "context window exceeded",                // LiteLLM's own check
    "exceeds the maximum number of tokens",   // Google
    "maximum prompt length is",               // xAI
    "reduce the length of the messages",      // Groq
    "maximum allowed input length",           // OpenRouter, Poolside
    "longer than the model's context length", // Together AI
    "prompt token count of",                  // GitHub Copilot
    "exceeds the available context size",     // llama.cpp
    "greater than the context length",        // LM Studio
    "context window exceeds limit",           // MiniMax
    "exceeded model token limit",             // Kimi
    "but the configured context size is",     // DS4
    "range of input length should be",        // DashScope, Qwen
    "context length exceeded",                // generic
    "context_length_exceeded",                // generic, written in a message
    "too many tokens",                        // generic
    "token limit exceeded",                   // generic
];

#[cfg(test)]
mod tests {
    use super::*;

    // Catches: a server's "too long" read as a plain refusal, so that the Turn
    // fails where it could compact; and a rate limit or another bad request
    // read as "too long", so that the Turn compacts for nothing.
    #[test]
    fn a_refusal_is_too_long_only_when_the_status_the_reason_or_the_message_says_so() {
        let wrapped = |message: &str| {
            format!(
                r#"{{"error":{{"message":"{message}","type":"invalid_request_error","code":null}}}}"#
            )
        };
        let too_long = [
            (413, String::new()),
            (400, r#"{"error":{"message":"m","type":"invalid_request_error","code":"context_length_exceeded"}}"#.to_owned()),
            (400, r#"{"error":{"message":"m","type":"request_too_large"}}"#.to_owned()),
            // A bare error with a number for its code, as vLLM sends.
            (400, r#"{"object":"error","message":"This model's maximum context length is 4096 tokens. However, you requested 5000 tokens.","type":"BadRequestError","param":null,"code":400}"#.to_owned()),
            (400, r#"{"error":"prompt too long; exceeded max context length by 10 tokens"}"#.to_owned()),
            (400, wrapped("prompt is too long: 213462 tokens > 200000 maximum")),
            (400, wrapped("Requested token count exceeds the model's maximum context length of 131072 tokens")),
            (400, wrapped("the request exceeds the available context size, try increasing it")),
        ];
        for (status, body) in too_long {
            let status = StatusCode::from_u16(status).unwrap();
            assert_eq!(
                Refusal::read(status, body.as_bytes()),
                Refusal::ContextTooLong,
                "{body}"
            );
        }
        let other = [
            (
                429,
                wrapped("Too many tokens, please wait before trying again."),
            ),
            (401, wrapped("prompt is too long")),
            (400, wrapped("Unknown tool `nothing`")),
            (400, "prompt is too long".to_owned()),
            (400, r#"{"error":{"message":null,"code":400}}"#.to_owned()),
            (400, String::new()),
        ];
        for (status, body) in other {
            let status = StatusCode::from_u16(status).unwrap();
            assert_eq!(
                Refusal::read(status, body.as_bytes()),
                Refusal::Other,
                "{body}"
            );
        }
    }
}
