//! Sensitive observed-value detection.

use serde_json::Value;

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

pub(super) fn is_sensitive_value(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.contains("-----BEGIN ") || trimmed.contains(" PRIVATE KEY-----") {
        return true;
    }
    is_sensitive_token(trimmed)
        || contains_sensitive_token(trimmed)
        || contains_sensitive_key_value_pair(trimmed)
}

pub(super) fn exclude_sensitive_json_fields(value: String) -> Option<String> {
    let Ok(mut structured_value) = serde_json::from_str::<Value>(&value) else {
        return Some(value);
    };
    if !remove_sensitive_fields(&mut structured_value) {
        return Some(value);
    }
    if !has_observable_content(&structured_value) {
        return None;
    }
    serde_json::to_string(&structured_value).ok()
}

fn remove_sensitive_fields(value: &mut Value) -> bool {
    match value {
        Value::Object(fields) => {
            let original_field_count = fields.len();
            fields.retain(|name, _| !is_sensitive_column(name));
            let mut removed_field = fields.len() != original_field_count;
            for value in fields.values_mut() {
                removed_field |= remove_sensitive_fields(value);
            }
            removed_field
        }
        Value::Array(values) => {
            let mut removed_field = false;
            for value in values {
                removed_field |= remove_sensitive_fields(value);
            }
            removed_field
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
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
        .split(|ch: char| {
            matches!(
                ch,
                '?' | '&' | ';' | ',' | '"' | '\'' | '{' | '}' | '[' | ']' | ' ' | '\t' | '\n'
            )
        })
        .filter_map(|segment| segment.split_once('=').or_else(|| segment.split_once(':')))
        .any(|(key, value)| !value.trim().is_empty() && is_sensitive_column(key))
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
