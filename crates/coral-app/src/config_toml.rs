//! Decoding of `config.toml` that never echoes configured values.
//!
//! Operators can put a secret under the wrong key. TOML and serde errors quote
//! the rejected value or render its source line, so every `config.toml` parser
//! decodes through [`decode`], which names the key path instead.

use serde::de::DeserializeOwned;

/// Standard serde messages that quote the rejected input.
///
/// Serde renders input only through `Unexpected` in these messages. The
/// others name keys, or come from deserializers that must not quote what
/// they reject.
const VALUE_BEARING_MESSAGES: [&str; 3] = ["invalid type: ", "invalid value: ", "unknown variant "];

/// A `config.toml` decode failure that says where the document is invalid,
/// never what it holds.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ConfigDecodeError(String);

/// Deserializes `T` from a whole `config.toml` document.
///
/// # Errors
///
/// Returns [`ConfigDecodeError`] naming the line of a syntax error or the key
/// path of a rejected value.
pub(crate) fn decode<T: DeserializeOwned>(raw: &str) -> Result<T, ConfigDecodeError> {
    let deserializer = toml::Deserializer::parse(raw).map_err(|error| {
        let line = error
            .span()
            .and_then(|span| raw.get(..span.start))
            .map_or(1, |before| before.matches('\n').count() + 1);
        ConfigDecodeError(format!("line {line}: {}", error.message()))
    })?;
    serde_path_to_error::deserialize(deserializer).map_err(|error| {
        let message = error.inner().message();
        let reason = if VALUE_BEARING_MESSAGES
            .iter()
            .any(|prefix| message.starts_with(prefix))
        {
            "unsupported value"
        } else {
            message
        };
        if error.path().iter().next().is_none() {
            ConfigDecodeError(reason.to_owned())
        } else {
            ConfigDecodeError(format!("{}: {reason}", error.path()))
        }
    })
}
