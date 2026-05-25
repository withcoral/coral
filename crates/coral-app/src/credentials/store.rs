//! Plaintext credential material persistence under the app state directory.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use crate::bootstrap::AppError;
use crate::state::AppStateLayout;
use crate::storage::fs as storage_fs;
use crate::storage::fs::FileLock;
use crate::workspaces::WorkspaceName;

use super::{CredentialMaterialSnapshot, CredentialSetId};

/// Errors returned by the plaintext credential env-file helpers.
#[derive(Debug, thiserror::Error)]
pub enum CredentialsError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("invalid secrets env file: {0}")]
    Parse(String),
}

#[derive(Clone)]
pub(crate) struct CredentialStore {
    layout: AppStateLayout,
}

impl CredentialStore {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    pub(crate) fn replace_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        values: &BTreeMap<String, String>,
    ) -> Result<(), AppError> {
        let path = self.material_file(workspace_name, credential_set_id)?;
        tracing::trace!(%credential_set_id, "replacing credential material");
        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        save_values_unlocked(&path, values)?;
        Ok(())
    }

    pub(crate) fn read_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
    ) -> Result<BTreeMap<String, String>, AppError> {
        let path = self.material_file(workspace_name, credential_set_id)?;
        tracing::trace!(%credential_set_id, "reading credential material");
        load_file(&path).map_err(Into::into)
    }

    pub(crate) fn snapshot_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
    ) -> Result<CredentialMaterialSnapshot, AppError> {
        let path = self.material_file(workspace_name, credential_set_id)?;
        tracing::trace!(%credential_set_id, "snapshotting credential material");
        let _lock = FileLock::shared(self.layout.state_lock())?;
        snapshot_file(&path).map_err(Into::into)
    }

    pub(crate) fn restore_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        snapshot: &CredentialMaterialSnapshot,
    ) -> Result<(), AppError> {
        let path = self.material_file(workspace_name, credential_set_id)?;
        tracing::trace!(%credential_set_id, "restoring credential material");
        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        restore_snapshot_unlocked(&path, snapshot)?;
        Ok(())
    }

    pub(crate) fn remove_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
    ) -> Result<(), AppError> {
        let path = self.material_file(workspace_name, credential_set_id)?;
        tracing::trace!(%credential_set_id, "removing credential material");
        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        remove_file_if_exists_unlocked(&path)?;
        Ok(())
    }

    fn material_file(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
    ) -> Result<std::path::PathBuf, AppError> {
        let source_name = credential_set_id.source_name()?;
        Ok(self.layout.secret_file(workspace_name, &source_name))
    }
}

fn load_file(path: &Path) -> Result<BTreeMap<String, String>, CredentialsError> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    parse_env_file(&std::fs::read_to_string(path)?)
}

fn snapshot_file(path: &Path) -> Result<CredentialMaterialSnapshot, CredentialsError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(CredentialMaterialSnapshot(Some(bytes))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Ok(CredentialMaterialSnapshot(None))
        }
        Err(error) => Err(error.into()),
    }
}

fn restore_snapshot_unlocked(
    path: &Path,
    snapshot: &CredentialMaterialSnapshot,
) -> Result<(), CredentialsError> {
    match &snapshot.0 {
        Some(bytes) => {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            storage_fs::ensure_dir(parent)?;
            storage_fs::write_atomic(path, bytes)?;
        }
        None => remove_file_if_exists_unlocked(path)?,
    }
    Ok(())
}

#[cfg(test)]
fn save_file(
    path: &Path,
    lock_path: &Path,
    values: &BTreeMap<String, String>,
) -> Result<(), CredentialsError> {
    let _lock = FileLock::exclusive(lock_path)?;
    save_values_unlocked(path, values)
}

fn save_values_unlocked(
    path: &Path,
    values: &BTreeMap<String, String>,
) -> Result<(), CredentialsError> {
    if values.is_empty() {
        remove_file_if_exists_unlocked(path)?;
        return Ok(());
    }

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    storage_fs::ensure_dir(parent)?;

    let mut output = String::new();
    for (env_var, value) in values {
        output.push_str(env_var);
        output.push('=');
        output.push_str(&encode_env_value(value));
        output.push('\n');
    }

    storage_fs::write_atomic(path, output.as_bytes())?;
    Ok(())
}

fn remove_file_if_exists_unlocked(path: &Path) -> Result<(), io::Error> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
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
    use std::collections::BTreeMap;

    use super::{CredentialStore, decode_env_value, encode_env_value, load_file, save_file};
    use crate::credentials::CredentialSetId;
    use crate::sources::SourceName;
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;
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

    #[test]
    fn replace_material_does_not_parse_existing_file() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let store = CredentialStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let path = layout.secret_file(&workspace_name, &source_name);
        std::fs::create_dir_all(path.parent().expect("secret parent")).expect("secret parent dir");
        std::fs::write(&path, "BROKEN\n").expect("write malformed existing env file");

        let values = BTreeMap::from([("API_TOKEN".to_string(), "secret-token".to_string())]);
        store
            .replace_material(&workspace_name, &credential_set_id, &values)
            .expect("replace malformed material");
        assert_eq!(load_file(&path).expect("load replaced material"), values);

        std::fs::write(&path, "BROKEN\n").expect("write malformed existing env file");
        store
            .replace_material(&workspace_name, &credential_set_id, &BTreeMap::new())
            .expect("remove malformed material");
        assert!(!path.exists(), "empty replacement should remove material");
    }

    #[test]
    fn remove_material_treats_missing_files_as_success() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let store = CredentialStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let path = layout.secret_file(&workspace_name, &source_name);

        store
            .remove_material(&workspace_name, &credential_set_id)
            .expect("missing material should be removable");

        std::fs::create_dir_all(path.parent().expect("secret parent")).expect("secret parent dir");
        std::fs::write(&path, "BROKEN\n").expect("write malformed existing env file");
        store
            .remove_material(&workspace_name, &credential_set_id)
            .expect("malformed material should be removable");
        assert!(!path.exists(), "remove should delete material");

        store
            .remove_material(&workspace_name, &credential_set_id)
            .expect("second remove should still be successful");
    }

    #[test]
    fn restore_material_snapshot_preserves_raw_bytes() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let store = CredentialStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let path = layout.secret_file(&workspace_name, &source_name);
        std::fs::create_dir_all(path.parent().expect("secret parent")).expect("secret parent dir");
        std::fs::write(&path, "BROKEN\n").expect("write malformed existing env file");

        let snapshot = store
            .snapshot_material(&workspace_name, &credential_set_id)
            .expect("snapshot malformed material");
        let values = BTreeMap::from([("API_TOKEN".to_string(), "secret-token".to_string())]);
        store
            .replace_material(&workspace_name, &credential_set_id, &values)
            .expect("replace material");

        store
            .restore_material(&workspace_name, &credential_set_id, &snapshot)
            .expect("restore malformed material");
        assert_eq!(
            std::fs::read(&path).expect("restored bytes"),
            b"BROKEN\n".to_vec()
        );
    }
}
