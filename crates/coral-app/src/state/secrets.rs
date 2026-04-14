//! Plaintext source-secret persistence under the app state directory.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::state::AppStateLayout;
use crate::storage::fs as storage_fs;
use crate::storage::fs::FileLock;
use crate::workspaces::WorkspaceName;

/// Errors returned by the plaintext env-file secret helpers.
#[derive(Debug, thiserror::Error)]
pub enum CredentialsError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("invalid secrets env file: {0}")]
    Parse(String),
}

#[derive(Clone)]
pub(crate) struct SecretStore {
    layout: AppStateLayout,
}

impl SecretStore {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    pub(crate) fn replace_source_secrets_for(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        secrets: &BTreeMap<String, String>,
    ) -> Result<Vec<String>, AppError> {
        let path = self.layout.secret_file(workspace_name, source_name);
        if secrets.is_empty() {
            if path.exists() {
                std::fs::remove_file(path)?;
            }
            return Ok(Vec::new());
        }
        save_file(&path, self.layout.state_lock(), secrets)?;
        Ok(secrets.keys().cloned().collect())
    }

    pub(crate) fn read_source_secrets_for(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<BTreeMap<String, String>, AppError> {
        let path = self.layout.secret_file(workspace_name, source_name);
        load_file(&path).map_err(Into::into)
    }
}

fn load_file(path: &Path) -> Result<BTreeMap<String, String>, CredentialsError> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    parse_env_file(&std::fs::read_to_string(path)?)
}

fn save_file(
    path: &Path,
    lock_path: &Path,
    values: &BTreeMap<String, String>,
) -> Result<(), CredentialsError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    storage_fs::ensure_dir(parent)?;

    let mut output = String::new();
    for (env_var, value) in values {
        output.push_str(env_var);
        output.push('=');
        output.push_str(&encode_env_value(value));
        output.push('\n');
    }

    let _lock = FileLock::exclusive(lock_path)?;
    storage_fs::write_atomic(path, output.as_bytes())?;
    Ok(())
}

fn parse_env_file(raw: &str) -> Result<BTreeMap<String, String>, CredentialsError> {
    let mut values = BTreeMap::new();
    for (index, line) in raw.lines().enumerate() {
        let line_number = index + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let Some((env_var, raw_value)) = line.split_once('=') else {
            return Err(CredentialsError::Parse(format!(
                "line {line_number} is missing '='"
            )));
        };
        let env_var = env_var.trim();
        if env_var.is_empty() {
            return Err(CredentialsError::Parse(format!(
                "line {line_number} has an empty variable name"
            )));
        }
        if values.contains_key(env_var) {
            return Err(CredentialsError::Parse(format!(
                "line {line_number} redefines '{env_var}'"
            )));
        }

        let value = decode_env_value(raw_value.trim(), line_number)?;
        values.insert(env_var.to_string(), value);
    }
    Ok(values)
}

fn decode_env_value(raw: &str, line_number: usize) -> Result<String, CredentialsError> {
    if let Some(inner) = raw.strip_prefix('"') {
        let Some(inner) = inner.strip_suffix('"') else {
            return Err(CredentialsError::Parse(format!(
                "line {line_number} has an unterminated quoted value"
            )));
        };
        return decode_quoted_env_value(inner, line_number);
    }

    if let Some(inner) = raw.strip_prefix('\'') {
        let Some(inner) = inner.strip_suffix('\'') else {
            return Err(CredentialsError::Parse(format!(
                "line {line_number} has an unterminated single-quoted value"
            )));
        };
        return Ok(inner.to_string());
    }

    Ok(raw.to_string())
}

fn decode_quoted_env_value(raw: &str, line_number: usize) -> Result<String, CredentialsError> {
    let mut decoded = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            decoded.push(ch);
            continue;
        }

        let Some(escaped) = chars.next() else {
            return Err(CredentialsError::Parse(format!(
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
                return Err(CredentialsError::Parse(format!(
                    "line {line_number} uses unsupported escape '\\{other}'"
                )));
            }
        }
    }
    Ok(decoded)
}

fn encode_env_value(value: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::{decode_env_value, encode_env_value, load_file, save_file};
    use tempfile::TempDir;

    #[test]
    fn round_trips_encoded_secret_values() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("secret.env");
        let lock_path = temp.path().join(".lock");
        let values = std::collections::BTreeMap::from([
            ("TOKEN".to_string(), "abc".to_string()),
            ("MULTI".to_string(), "hello\nworld".to_string()),
        ]);
        save_file(&path, &lock_path, &values).expect("save env file");
        assert_eq!(load_file(&path).expect("load env file"), values);
        assert_eq!(encode_env_value("hello world"), "\"hello world\"");
        assert_eq!(
            decode_env_value("\"hello\\nworld\"", 1).expect("decode"),
            "hello\nworld"
        );
    }
}
