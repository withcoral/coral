//! Credential storage configuration loaded from `config.toml`.

use serde::Deserialize;

use crate::bootstrap::AppError;
use crate::state::AppStateLayout;

use super::CredentialStoragePreference;

#[derive(Debug, Clone, Default)]
pub(crate) struct CredentialStorageConfig {
    pub(crate) storage: CredentialStoragePreference,
    pub(crate) encryption_key_env: Option<String>,
    pub(crate) encryption_key_source: CredentialEncryptionKeySource,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CredentialEncryptionKeySource {
    #[default]
    Auto,
    File,
    Keychain,
    Vault,
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
    encryption_key_env: Option<String>,
    #[serde(default)]
    encryption_key_source: CredentialEncryptionKeySource,
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
            encryption_key_source: file.credentials.encryption_key_source,
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn defaults_to_auto_when_section_is_absent() {
        let file = toml::from_str::<CredentialStorageConfigFile>("version = 1").expect("config");
        assert_eq!(file.credentials.storage, CredentialStoragePreference::Auto);
        assert_eq!(
            file.credentials.encryption_key_source,
            CredentialEncryptionKeySource::Auto,
        );
        assert_eq!(file.credentials.encryption_key_env, None);
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

    #[test]
    fn parses_encryption_key_configuration() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        std::fs::write(
            layout.config_file(),
            r#"
[credentials]
encryption_key_env = "CORAL_CREDENTIAL_KEY"
encryption_key_source = "keychain"
"#,
        )
        .expect("write config");
        let config = CredentialStorageConfig::load(&layout).expect("load config");

        assert_eq!(
            config.encryption_key_env.as_deref(),
            Some("CORAL_CREDENTIAL_KEY")
        );
        assert_eq!(
            config.encryption_key_source,
            CredentialEncryptionKeySource::Keychain
        );
    }

    #[test]
    fn rejects_unknown_encryption_key_source() {
        let error = toml::from_str::<CredentialStorageConfigFile>(
            r#"
[credentials]
encryption_key_source = "remote"
"#,
        )
        .expect_err("unknown source should fail");

        assert!(error.to_string().contains("unknown variant"));
    }
}
