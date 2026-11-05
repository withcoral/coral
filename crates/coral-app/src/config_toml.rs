//! Decoding of `config.toml` that never echoes configured values.
//!
//! Operators can put a secret under the wrong key. TOML and serde errors quote
//! the rejected value or render its source line, so every `config.toml` parser
//! decodes through [`decode`] or [`decode_entry`], which name the key path
//! instead.

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
    serde_path_to_error::deserialize(deserializer).map_err(|error| rejected(None, &error))
}

/// Deserializes `T` from one value of a `config.toml` document, for a section
/// that names each of its entries in its own failures.
///
/// # Errors
///
/// Returns [`ConfigDecodeError`] naming `label` and the key path below it of a
/// rejected value.
pub(crate) fn decode_entry<T: DeserializeOwned>(
    label: &str,
    raw: toml::Value,
) -> Result<T, ConfigDecodeError> {
    serde_path_to_error::deserialize(raw).map_err(|error| rejected(Some(label), &error))
}

fn rejected(
    label: Option<&str>,
    error: &serde_path_to_error::Error<toml::de::Error>,
) -> ConfigDecodeError {
    let message = error.inner().message();
    let reason = if VALUE_BEARING_MESSAGES
        .iter()
        .any(|prefix| message.starts_with(prefix))
    {
        "unsupported value"
    } else {
        message
    };
    let at_root = error.path().iter().next().is_none();
    ConfigDecodeError(match (label, at_root) {
        (None, true) => reason.to_owned(),
        (None, false) => format!("{}: {reason}", error.path()),
        (Some(label), true) => format!("{label}: {reason}"),
        (Some(label), false) => format!("{label}.{}: {reason}", error.path()),
    })
}
