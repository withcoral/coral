use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Error;

/// Maximum UTF-8 size of one publicly returnable declaration evidence path.
pub const DECLARATION_PATH_MAX_BYTES: usize = 1024;

/// A validated callable-rooted readable JavaScript declaration path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DeclarationPath(String);

impl DeclarationPath {
    /// Starts a path at `input`.
    #[must_use]
    pub fn input() -> Self {
        Self("input".to_string())
    }

    /// Starts a path at `result`.
    #[must_use]
    pub fn result() -> Self {
        Self("result".to_string())
    }

    /// Parses one complete canonical declaration path.
    ///
    /// # Errors
    ///
    /// Returns a corrupt-data error unless the path consists only of a callable
    /// root, canonical properties, numeric properties, `[key]`, and `[]`.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.len() > DECLARATION_PATH_MAX_BYTES || !valid_path(value) {
            return Err(Error::CorruptData(
                "declaration evidence has an invalid public path".to_string(),
            ));
        }
        Ok(Self(value.to_string()))
    }

    /// Appends one property using canonical JavaScript access syntax.
    #[must_use]
    pub fn property(&self, name: &str) -> Option<Self> {
        let suffix = if safe_identifier(name) {
            format!(".{name}")
        } else {
            format!("[{}]", serde_json::to_string(name).ok()?)
        };
        self.append(&suffix)
    }

    /// Appends one TypeScript numeric-literal property.
    #[must_use]
    pub fn numeric_property(&self, literal: &str) -> Option<Self> {
        valid_numeric_literal(literal)
            .then(|| format!("[{literal}]"))
            .and_then(|suffix| self.append(&suffix))
    }

    /// Appends a readable index-signature segment.
    #[must_use]
    pub fn index(&self) -> Option<Self> {
        self.append("[key]")
    }

    /// Appends one array or tuple element segment.
    #[must_use]
    pub fn array_element(&self) -> Option<Self> {
        self.append("[]")
    }

    /// Returns the canonical public JavaScript path.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn append(&self, suffix: &str) -> Option<Self> {
        let value = format!("{}{suffix}", self.0);
        (value.len() <= DECLARATION_PATH_MAX_BYTES).then_some(Self(value))
    }
}

impl fmt::Display for DeclarationPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for DeclarationPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for DeclarationPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(D::Error::custom)
    }
}

fn valid_path(value: &str) -> bool {
    let mut rest = if let Some(rest) = value.strip_prefix("input") {
        rest
    } else if let Some(rest) = value.strip_prefix("result") {
        rest
    } else {
        return false;
    };
    while !rest.is_empty() {
        if let Some(after_dot) = rest.strip_prefix('.') {
            let end = after_dot.find(['.', '[']).unwrap_or(after_dot.len());
            let Some(segment) = after_dot.get(..end) else {
                return false;
            };
            if !safe_identifier(segment) {
                return false;
            }
            rest = after_dot.get(end..).unwrap_or_default();
            continue;
        }
        if let Some(after) = rest.strip_prefix("[]") {
            rest = after;
            continue;
        }
        if let Some(after) = rest.strip_prefix("[key]") {
            rest = after;
            continue;
        }
        let Some(after_open) = rest.strip_prefix('[') else {
            return false;
        };
        if after_open.starts_with('"') {
            let mut values = serde_json::Deserializer::from_str(after_open).into_iter::<String>();
            if !matches!(values.next(), Some(Ok(_))) {
                return false;
            }
            let offset = values.byte_offset();
            let Some(after_string) = after_open.get(offset..) else {
                return false;
            };
            let Some(after_close) = after_string.strip_prefix(']') else {
                return false;
            };
            rest = after_close;
            continue;
        }
        let Some(close) = after_open.find(']') else {
            return false;
        };
        let Some(literal) = after_open.get(..close) else {
            return false;
        };
        if !valid_numeric_literal(literal) {
            return false;
        }
        rest = after_open.get(close + 1..).unwrap_or_default();
    }
    true
}

fn valid_numeric_literal(value: &str) -> bool {
    let mut decimal_point = false;
    !value.is_empty()
        && value.bytes().all(|byte| {
            if byte == b'.' && !decimal_point {
                decimal_point = true;
                true
            } else {
                byte.is_ascii_digit()
            }
        })
        && value.bytes().any(|byte| byte.is_ascii_digit())
}

fn safe_identifier(value: &str) -> bool {
    let mut characters = value.chars();
    characters.next().is_some_and(|character| {
        matches!(character, '_' | '$') || unicode_id_start::is_id_start(character)
    }) && characters.all(|character| {
        matches!(character, '_' | '$' | '\u{200c}' | '\u{200d}')
            || unicode_id_start::is_id_continue(character)
    })
}

#[cfg(test)]
mod tests {
    use super::DeclarationPath;

    #[test]
    fn declaration_paths_build_and_parse_one_canonical_grammar() {
        let path = DeclarationPath::input()
            .property("items")
            .and_then(|path| path.array_element())
            .and_then(|path| path.property("wire-name"))
            .expect("bounded path");
        assert_eq!(path.as_str(), "input.items[][\"wire-name\"]");
        assert_eq!(DeclarationPath::parse(path.as_str()).expect("parse"), path);
        DeclarationPath::parse("result[7].value").expect("numeric property path");
        for invalid in ["bogus", "input.", "result.bad!", "input[wat]", "input..x"] {
            assert!(
                DeclarationPath::parse(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }
}
