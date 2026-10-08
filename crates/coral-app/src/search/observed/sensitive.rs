//! Best-effort suppression of obvious secrets in observed values.
//!
//! This module recognizes credential-like field names and a limited set of
//! obvious secret token shapes. It is defense in depth, not general
//! sensitive-data detection or a data-loss-prevention boundary.

use serde_json::Value;
use url::{Url, form_urlencoded};

const SENSITIVE_COLUMN_NAMES: &[&str] = &[
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

/// Returns whether a field name resembles a credential-bearing field.
pub(super) fn is_sensitive_column(column_name: &str) -> bool {
    let normalized = column_name
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect::<String>()
        .to_ascii_lowercase();
    SENSITIVE_COLUMN_NAMES
        .iter()
        .any(|name| normalized.contains(name))
}

/// Returns whether a value contains one of the obvious secret shapes we know.
///
/// A `false` result does not mean the value is generally non-sensitive.
pub(super) fn is_sensitive_value(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.contains("-----BEGIN ") || trimmed.contains(" PRIVATE KEY-----") {
        return true;
    }
    is_sensitive_token(trimmed)
        || contains_sensitive_token(trimmed)
        || contains_sensitive_key_value_pair(trimmed)
}

/// Removes recognized secret-bearing fields while preserving safe structured content.
///
/// This is a best-effort heuristic and does not classify arbitrary sensitive data.
pub(super) fn sanitize_observed_value(value: String, max_value_bytes: usize) -> Option<String> {
    let sanitized = match sanitize_json(&value)
        .or_else(|| sanitize_url(&value))
        .or_else(|| sanitize_form(&value))
        .unwrap_or(SanitizedValue::Unchanged)
    {
        SanitizedValue::Unchanged => value,
        SanitizedValue::Changed(value) => value,
        SanitizedValue::Drop => return None,
    };

    if sanitized.len() > max_value_bytes || is_sensitive_value(&sanitized) {
        return None;
    }
    Some(sanitized)
}

enum SanitizedValue {
    Unchanged,
    Changed(String),
    Drop,
}

fn sanitize_json(value: &str) -> Option<SanitizedValue> {
    let trimmed = value.trim_start();
    if !trimmed.starts_with('{') && !trimmed.starts_with('[') {
        return None;
    }

    let mut structured_value = serde_json::from_str::<Value>(value).ok()?;
    match sanitize_json_value(&mut structured_value) {
        JsonSanitization::Unchanged => Some(SanitizedValue::Unchanged),
        JsonSanitization::Changed => Some(
            serde_json::to_string(&structured_value)
                .map_or(SanitizedValue::Drop, SanitizedValue::Changed),
        ),
        JsonSanitization::Drop => Some(SanitizedValue::Drop),
    }
}

enum JsonSanitization {
    Unchanged,
    Changed,
    Drop,
}

fn sanitize_json_value(value: &mut Value) -> JsonSanitization {
    match value {
        Value::Object(fields) => {
            let mut changed = false;
            fields.retain(|name, value| {
                if is_sensitive_column(name) {
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
            } else if fields.values().any(has_observable_content) {
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
            } else if values.iter().any(has_observable_content) {
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

fn sanitize_url(value: &str) -> Option<SanitizedValue> {
    let Ok(mut url) = Url::parse(value) else {
        return sanitize_relative_url(value);
    };
    url.query()?;

    let pairs = url.query_pairs().into_owned().collect::<Vec<_>>();
    let retained_pairs = pairs
        .iter()
        .filter(|(key, value)| !is_sensitive_pair(key, value))
        .cloned()
        .collect::<Vec<_>>();
    if retained_pairs.len() == pairs.len() {
        return Some(SanitizedValue::Unchanged);
    }

    url.set_query(None);
    if !retained_pairs.is_empty() {
        url.query_pairs_mut().extend_pairs(
            retained_pairs
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
    }
    Some(SanitizedValue::Changed(url.into()))
}

fn sanitize_relative_url(value: &str) -> Option<SanitizedValue> {
    let (path, query_and_fragment) = value.split_once('?')?;
    let (query, fragment) = query_and_fragment
        .split_once('#')
        .map_or((query_and_fragment, None), |(query, fragment)| {
            (query, Some(fragment))
        });
    let pairs = form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect::<Vec<_>>();
    let retained_pairs = pairs
        .iter()
        .filter(|(key, value)| !is_sensitive_pair(key, value))
        .cloned()
        .collect::<Vec<_>>();
    if retained_pairs.len() == pairs.len() {
        return Some(SanitizedValue::Unchanged);
    }

    let mut sanitized = path.to_string();
    if !retained_pairs.is_empty() {
        sanitized.push('?');
        let mut serializer = form_urlencoded::Serializer::new(String::new());
        serializer.extend_pairs(
            retained_pairs
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
        sanitized.push_str(&serializer.finish());
    }
    if let Some(fragment) = fragment {
        sanitized.push('#');
        sanitized.push_str(fragment);
    }
    if sanitized.is_empty() {
        Some(SanitizedValue::Drop)
    } else {
        Some(SanitizedValue::Changed(sanitized))
    }
}

fn sanitize_form(value: &str) -> Option<SanitizedValue> {
    if !value.contains('=') {
        return None;
    }
    let pairs = form_urlencoded::parse(value.as_bytes())
        .into_owned()
        .collect::<Vec<_>>();
    if pairs.is_empty() {
        return None;
    }
    let retained_pairs = pairs
        .iter()
        .filter(|(key, value)| !is_sensitive_pair(key, value))
        .cloned()
        .collect::<Vec<_>>();
    if retained_pairs.len() == pairs.len() {
        return Some(SanitizedValue::Unchanged);
    }
    if retained_pairs.is_empty() {
        return Some(SanitizedValue::Drop);
    }

    let mut serializer = form_urlencoded::Serializer::new(String::new());
    serializer.extend_pairs(
        retained_pairs
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );
    Some(SanitizedValue::Changed(serializer.finish()))
}

fn is_sensitive_pair(key: &str, value: &str) -> bool {
    is_sensitive_column(key) || is_sensitive_value(value)
}

fn has_observable_content(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(value) => !value.trim().is_empty(),
        Value::Array(values) => values.iter().any(has_observable_content),
        Value::Object(fields) => fields.values().any(has_observable_content),
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

fn contains_sensitive_key_value_pair(value: &str) -> bool {
    value
        .char_indices()
        .filter(|(_, character)| matches!(character, '=' | ':'))
        .filter_map(|(separator_index, _)| sensitive_key_before(value, separator_index))
        .any(is_sensitive_column)
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
    use serde_json::{Value, json};
    use url::Url;

    use super::sanitize_observed_value;

    #[test]
    fn sanitizes_nested_json_and_preserves_benign_siblings() {
        let value = sanitize_observed_value(
            r#"{"name":"Ada","api_key":"literal-secret","nested":{"project":"coral","refresh_token":"other-secret"},"items":[{"label":"safe","password":"hidden"},{"token":"hidden"}]}"#
                .to_string(),
            usize::MAX,
        )
        .expect("safe JSON fields should remain");
        let value: Value = serde_json::from_str(&value).expect("sanitized JSON");

        assert_eq!(
            value,
            json!({
                "name": "Ada",
                "nested": { "project": "coral" },
                "items": [{ "label": "safe" }]
            })
        );
    }

    #[test]
    fn sanitizes_decoded_url_query_keys_and_preserves_the_url() {
        let value = sanitize_observed_value(
            "https://example.test/callback?name=Ada&api%5Fkey=literal-secret&project=coral"
                .to_string(),
            usize::MAX,
        )
        .expect("URL base and safe pairs should remain");
        let url = Url::parse(&value).expect("sanitized URL");
        let pairs = url.query_pairs().into_owned().collect::<Vec<_>>();

        assert_eq!(
            pairs,
            [
                ("name".to_string(), "Ada".to_string()),
                ("project".to_string(), "coral".to_string()),
            ]
        );
        assert!(!value.contains("literal-secret"));
    }

    #[test]
    fn sanitizes_relative_url_query_keys_and_preserves_path_and_fragment() {
        let value = sanitize_observed_value(
            "/callback?name=Ada&api_key=literal-secret#details".to_string(),
            usize::MAX,
        )
        .expect("relative URL content should remain");

        assert_eq!(value, "/callback?name=Ada#details");
    }

    #[test]
    fn sanitizes_decoded_form_keys_and_preserves_safe_pairs() {
        let value = sanitize_observed_value(
            "name=Ada&api%5Fkey=literal-secret&project=coral".to_string(),
            usize::MAX,
        )
        .expect("safe form pairs should remain");

        assert_eq!(value, "name=Ada&project=coral");
    }

    #[test]
    fn drops_empty_structures_but_keeps_a_url_without_sensitive_query_pairs() {
        assert!(
            sanitize_observed_value(r#"{"api_key":"hidden"}"#.to_string(), usize::MAX).is_none()
        );
        assert!(
            sanitize_observed_value("api_key=hidden&token=hidden".to_string(), usize::MAX)
                .is_none()
        );
        assert_eq!(
            sanitize_observed_value(
                "https://example.test/callback?api_key=hidden".to_string(),
                usize::MAX,
            )
            .as_deref(),
            Some("https://example.test/callback")
        );
    }

    #[test]
    fn leaves_benign_structured_values_byte_for_byte_unchanged() {
        for value in [
            r#"{ "name": "Ada" }"#,
            "https://example.test/callback?name=Ada",
            "name=Ada&project=coral",
        ] {
            assert_eq!(
                sanitize_observed_value(value.to_string(), usize::MAX).as_deref(),
                Some(value)
            );
        }
    }

    #[test]
    fn malformed_recognizable_sensitive_assignments_fail_closed() {
        assert!(
            sanitize_observed_value(
                r#"{"name":"Ada","api_key":"literal-secret""#.to_string(),
                usize::MAX,
            )
            .is_none()
        );
    }

    #[test]
    fn rejects_reconstructed_values_that_exceed_the_value_budget() {
        let value = "safe=ééé&token=x".to_string();
        let max_value_bytes = value.len();

        assert!(sanitize_observed_value(value, max_value_bytes).is_none());
    }
}
