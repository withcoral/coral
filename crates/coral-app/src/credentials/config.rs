//! Credential storage configuration loaded from `config.toml`.

use serde::Deserialize;

use crate::bootstrap::AppError;
use crate::state::AppStateLayout;

use super::CredentialStoragePreference;

#[derive(Debug, Clone, Default)]
pub(crate) struct CredentialStorageConfig {
    pub(crate) storage: CredentialStoragePreference,
    pub(crate) encryption_key_env: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct CredentialStorageConfigFile {
    #[serde(default)]
    credentials: CredentialStorageConfigSection,
}

#[derive(Debug, Deserialize, Default)]
struct CredentialStorageConfigSection {
    #[serde(default)]
    storage: CredentialStoragePreference,
    #[serde(default)]
    encryption_key_env: Option<String>,
}

impl CredentialStorageConfig {
    pub(crate) fn load(layout: &AppStateLayout) -> Result<Self, AppError> {
        if !layout.config_file().exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(layout.config_file())?;
        let file = toml::from_str::<CredentialStorageConfigFile>(&raw)?;
        Ok(Self {
            storage: file.credentials.storage,
            encryption_key_env: file.credentials.encryption_key_env,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_auto_when_section_is_absent() {
        let file = toml::from_str::<CredentialStorageConfigFile>("version = 1").expect("config");
        assert_eq!(file.credentials.storage, CredentialStoragePreference::Auto);
    }

    #[test]
    fn parses_configured_storage_preference() {
        let file = toml::from_str::<CredentialStorageConfigFile>(
            r#"
[credentials]
storage = "file"
"#,
        )
        .expect("config");
        assert_eq!(file.credentials.storage, CredentialStoragePreference::File);
    }
}
