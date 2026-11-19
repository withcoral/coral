//! Decoding of `config.toml` that never echoes configured values.
//!
//! Operators can put a secret under the wrong key. TOML and serde errors quote
//! the rejected value or render its source line, so every `config.toml` parser
//! decodes through [`decode`] or [`Document`], which name the key path instead.

use serde::Deserialize as _;
use serde::de::DeserializeOwned;
use toml::de::{DeTable, ValueDeserializer};

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

impl ConfigDecodeError {
    /// A failure that a section's own decoder found. `message` says where
    /// the document is invalid, never what it holds.
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// Deserializes `T` from a whole `config.toml` document.
///
/// # Errors
///
/// Returns [`ConfigDecodeError`] naming the line of a syntax error or the key
/// path of a rejected value.
pub(crate) fn decode<T: DeserializeOwned>(raw: &str) -> Result<T, ConfigDecodeError> {
    Document::parse(raw)?.decode()
}

/// A parsed `config.toml`. Take out the sections that another decoder owns,
/// then decode the rest.
pub(crate) struct Document<'raw>(toml::Spanned<DeTable<'raw>>);

impl<'raw> Document<'raw> {
    /// Parses `raw`. A syntax error names its line.
    pub(crate) fn parse(raw: &'raw str) -> Result<Self, ConfigDecodeError> {
        DeTable::parse(raw).map(Self).map_err(|error| {
            let line = error
                .span()
                .and_then(|span| raw.get(..span.start))
                .map_or(1, |before| before.matches('\n').count() + 1);
            ConfigDecodeError(format!("line {line}: {}", error.message()))
        })
    }

    /// Takes the top-level section `name` out of the document, undecoded.
    /// None when the document has no such key.
    pub(crate) fn take_section(
        &mut self,
        name: &str,
    ) -> Result<Option<toml::Table>, ConfigDecodeError> {
        let Some(section) = self.0.get_mut().remove(name) else {
            return Ok(None);
        };
        toml::Table::deserialize(ValueDeserializer::from(section))
            .map(Some)
            .map_err(|_quotes_the_value| ConfigDecodeError(format!("{name}: unsupported value")))
    }

    /// Deserializes `T` from what is left of the document. A failure names
    /// the key path of the rejected value.
    pub(crate) fn decode<T: DeserializeOwned>(self) -> Result<T, ConfigDecodeError> {
        serde_path_to_error::deserialize(toml::Deserializer::from(self.0)).map_err(|error| {
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
}
