//! `.env`-style material encoding helpers shared by app-owned stores.

use std::collections::BTreeMap;

/// Errors returned while parsing `.env`-style material.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct EnvFileError(String);

/// Renders key/value material as a deterministic `.env`-style file.
pub(crate) fn render_env_file(values: &BTreeMap<String, String>) -> String {
    let mut output = String::new();
    for (env_var, value) in values {
        output.push_str(env_var);
        output.push('=');
        output.push_str(&encode_env_value(value));
        output.push('\n');
    }
    output
}

/// Parses key/value material from a `.env`-style file.
pub(crate) fn parse_env_file(raw: &str) -> Result<BTreeMap<String, String>, EnvFileError> {
    let mut values = BTreeMap::new();
    for (index, line) in raw.lines().enumerate() {
        let line_number = index + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let Some((env_var, raw_value)) = line.split_once('=') else {
            return Err(EnvFileError(format!("line {line_number} is missing '='")));
        };
        let env_var = env_var.trim();
        if env_var.is_empty() {
            return Err(EnvFileError(format!(
                "line {line_number} has an empty variable name"
            )));
        }
        if values.contains_key(env_var) {
            return Err(EnvFileError(format!(
                "line {line_number} redefines '{env_var}'"
            )));
        }

        let value = decode_env_value(raw_value.trim(), line_number)?;
        values.insert(env_var.to_string(), value);
    }
    Ok(values)
}

pub(crate) fn decode_env_value(raw: &str, line_number: usize) -> Result<String, EnvFileError> {
    if let Some(inner) = raw.strip_prefix('"') {
        let Some(inner) = inner.strip_suffix('"') else {
            return Err(EnvFileError(format!(
                "line {line_number} has an unterminated quoted value"
            )));
        };
        return decode_quoted_env_value(inner, line_number);
    }

    if let Some(inner) = raw.strip_prefix('\'') {
        let Some(inner) = inner.strip_suffix('\'') else {
            return Err(EnvFileError(format!(
                "line {line_number} has an unterminated single-quoted value"
            )));
        };
        return Ok(inner.to_string());
    }

    Ok(raw.to_string())
}

fn decode_quoted_env_value(raw: &str, line_number: usize) -> Result<String, EnvFileError> {
    let mut decoded = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            decoded.push(ch);
            continue;
        }

        let Some(escaped) = chars.next() else {
            return Err(EnvFileError(format!(
                "line {line_number} ends with a dangling escape"
            )));
        };
        match escaped {
            '\\' => decoded.push('\\'),
            '"' => decoded.push('"'),
            'n' => decoded.push('\n'),
            'r' => decoded.push('\r'),
            't' => decoded.push('\t'),
            other => {
                return Err(EnvFileError(format!(
                    "line {line_number} uses unsupported escape '\\{other}'"
                )));
            }
        }
    }
    Ok(decoded)
}

pub(crate) fn encode_env_value(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/' | ':' | '@'))
    {
        return value.to_string();
    }

    let mut encoded = String::with_capacity(value.len() + 2);
    encoded.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => encoded.push_str("\\\\"),
            '"' => encoded.push_str("\\\""),
            '\n' => encoded.push_str("\\n"),
            '\r' => encoded.push_str("\\r"),
            '\t' => encoded.push_str("\\t"),
            other => encoded.push(other),
        }
    }
    encoded.push('"');
    encoded
}
