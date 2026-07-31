//! Shared defense-in-depth filtering for untrusted search content.
//!
//! This is deliberately a narrow heuristic boundary. Callers still own their
//! surface-specific validity and size policies.

use serde_json::Value;

const SENSITIVE_FIELD_NAMES: &[&str] = &[
    "apikey",
    "authorization",
    "authtoken",
    "cookie",
    "credential",
    "password",
    "passwd",
    "privatekey",
    "refreshtoken",
    "secret",
    "session",
    "token",
];

const SENSITIVE_AUTH_SCHEMES: &[&str] = &[
    "api-key",
    "apikey",
    "basic",
    "bearer",
    "digest",
    "negotiate",
    "ntlm",
    "token",
];

/// Result of removing recognized secret-bearing content from a JSON value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::search) enum JsonSanitization {
    Unchanged,
    Changed,
    Drop,
}

/// Returns whether a field name resembles a credential-bearing field.
pub(in crate::search) fn is_sensitive_name(field_name: &str) -> bool {
    let normalized = field_name
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect::<String>()
        .to_ascii_lowercase();
    SENSITIVE_FIELD_NAMES
        .iter()
        .any(|name| normalized.contains(name))
}

/// Returns whether a value contains one of the obvious secret shapes we know.
///
/// A `false` result does not mean the value is generally non-sensitive.
pub(in crate::search) fn is_sensitive_value(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.contains("-----BEGIN ") || trimmed.contains(" PRIVATE KEY-----") {
        return true;
    }
    contains_sensitive_auth_scheme(trimmed)
        || is_sensitive_token(trimmed)
        || contains_sensitive_token(trimmed)
        || contains_sensitive_key_value_pair(trimmed)
}

/// Returns whether either side of a decoded name/value pair is sensitive.
pub(in crate::search) fn is_sensitive_pair(key: &str, value: &str) -> bool {
    is_sensitive_name(key) || is_sensitive_value(value)
}

/// Removes recognized secret-bearing fields while preserving safe JSON siblings.
pub(in crate::search) fn sanitize_json_value(value: &mut Value) -> JsonSanitization {
    match value {
        Value::Object(fields) => {
            let mut changed = false;
            fields.retain(|name, value| {
                if is_sensitive_name(name) {
                    changed = true;
                    return false;
                }
                match sanitize_json_value(value) {
                    JsonSanitization::Unchanged => true,
                    JsonSanitization::Changed => {
                        changed = true;
                        true
                    }
                    JsonSanitization::Drop => {
                        changed = true;
                        false
                    }
                }
            });
            if !changed {
                JsonSanitization::Unchanged
            } else if fields.values().any(has_display_content) {
                JsonSanitization::Changed
            } else {
                JsonSanitization::Drop
            }
        }
        Value::Array(values) => {
            let mut changed = false;
            values.retain_mut(|value| match sanitize_json_value(value) {
                JsonSanitization::Unchanged => true,
                JsonSanitization::Changed => {
                    changed = true;
                    true
                }
                JsonSanitization::Drop => {
                    changed = true;
                    false
                }
            });
            if !changed {
                JsonSanitization::Unchanged
            } else if values.iter().any(has_display_content) {
                JsonSanitization::Changed
            } else {
                JsonSanitization::Drop
            }
        }
        Value::String(value) if is_sensitive_value(value) => JsonSanitization::Drop,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            JsonSanitization::Unchanged
        }
    }
}

fn has_display_content(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(value) => !value.trim().is_empty(),
        Value::Array(values) => values.iter().any(has_display_content),
        Value::Object(fields) => fields.values().any(has_display_content),
        Value::Bool(_) | Value::Number(_) => true,
    }
}

fn is_sensitive_token(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    if (lower.starts_with("sk-") && value.len() >= 20)
        || lower.starts_with("ghp_")
        || lower.starts_with("github_pat_")
        || lower.starts_with("xoxb-")
        || lower.starts_with("xoxp-")
        || lower.starts_with("xoxa-")
        || lower.starts_with("ya29.")
    {
        return true;
    }
    looks_like_jwt(value)
}

fn contains_sensitive_token(value: &str) -> bool {
    value
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.')))
        .any(is_sensitive_token)
}

fn contains_sensitive_auth_scheme(value: &str) -> bool {
    let mut words = value.split_whitespace();
    let Some(mut previous) = words.next() else {
        return false;
    };
    for current in words {
        if is_sensitive_auth_scheme(previous) && current.len() >= 8 && is_auth_credential(current) {
            return true;
        }
        previous = current;
    }
    false
}

fn is_sensitive_auth_scheme(value: &str) -> bool {
    let scheme = value.trim_matches(|character: char| {
        character.is_ascii_punctuation() && !matches!(character, '-' | '_')
    });
    SENSITIVE_AUTH_SCHEMES
        .iter()
        .any(|candidate| scheme.eq_ignore_ascii_case(candidate))
}

fn is_auth_credential(value: &str) -> bool {
    !value.is_empty()
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(character, '-' | '_' | '.' | '~' | '+' | '/' | '=')
        })
}

fn contains_sensitive_key_value_pair(value: &str) -> bool {
    value
        .char_indices()
        .filter(|(_, character)| matches!(character, '=' | ':'))
        .filter_map(|(separator_index, _)| sensitive_key_before(value, separator_index))
        .any(is_sensitive_name)
}

fn sensitive_key_before(value: &str, separator_index: usize) -> Option<&str> {
    let prefix = value
        .get(..separator_index)?
        .trim_end_matches(|character: char| {
            character.is_whitespace() || matches!(character, '"' | '\'')
        });
    let key_start = prefix
        .char_indices()
        .rev()
        .find(|(_, character)| !is_sensitive_key_character(*character))
        .map_or(0, |(index, character)| index + character.len_utf8());
    prefix.get(key_start..).filter(|key| !key.is_empty())
}

fn is_sensitive_key_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
}

fn looks_like_jwt(value: &str) -> bool {
    let mut parts = value.split('.');
    let Some(header) = parts.next() else {
        return false;
    };
    let Some(payload) = parts.next() else {
        return false;
    };
    let Some(signature) = parts.next() else {
        return false;
    };
    if parts.next().is_some() {
        return false;
    }
    [header, payload, signature].iter().all(|part| {
        part.len() >= 8
            && part
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{JsonSanitization, is_sensitive_name, is_sensitive_value, sanitize_json_value};

    #[test]
    fn shared_policy_recognizes_names_embedded_tokens_and_nested_secrets() {
        assert!(is_sensitive_name("api_key"));
        assert!(is_sensitive_value(
            "prefix sk-123456789012345678901234 suffix"
        ));
        assert!(is_sensitive_value("Bearer coral-secret-123456"));
        assert!(is_sensitive_value(
            "prefix bearer coral-secret-123456 suffix"
        ));
        assert!(is_sensitive_value("Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="));
        assert!(!is_sensitive_value("bearer of bad news"));

        let mut value = json!({
            "name": "Ada",
            "nested": { "refresh_token": "hidden", "project": "coral" }
        });
        assert_eq!(sanitize_json_value(&mut value), JsonSanitization::Changed);
        assert_eq!(
            value,
            json!({ "name": "Ada", "nested": { "project": "coral" } })
        );
    }
}
