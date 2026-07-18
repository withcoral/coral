//! Credential storage and envelope-encryption configuration from `config.toml`.
//!
//! `[credentials].storage` selects where credential *material* is kept. The
//! encryption keys live under `[encryption]` instead, because one key protects
//! every envelope domain — credential documents, identity-spec setup documents,
//! and identity documents — not credentials alone.

use serde::Deserialize;

use crate::bootstrap::AppError;
use crate::state::AppStateLayout;

use super::CredentialStoragePreference;

#[derive(Debug, Clone, Default)]
pub(crate) struct CredentialStorageConfig {
    pub(crate) storage: CredentialStoragePreference,
    pub(crate) encryption_key_env: Option<String>,
    pub(crate) decryption_key_envs: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct CredentialStorageConfigFile {
    #[serde(default)]
    credentials: CredentialStorageConfigSection,
    #[serde(default)]
    encryption: EnvelopeEncryptionConfigSection,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct CredentialStorageConfigSection {
    #[serde(default)]
    storage: CredentialStoragePreference,
}

/// Envelope-encryption keys, which protect identity material as well as credentials.
#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct EnvelopeEncryptionConfigSection {
    #[serde(default)]
    encryption_key_env: Option<String>,
    #[serde(default)]
    decryption_key_envs: Vec<String>,
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
            encryption_key_env: file.encryption.encryption_key_env,
            decryption_key_envs: file.encryption.decryption_key_envs,
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

    #[test]
    fn parses_encryption_key_environment_variables() {
        let file = toml::from_str::<CredentialStorageConfigFile>(
            r#"
[encryption]
encryption_key_env = "CORAL_ACTIVE_KEY"
decryption_key_envs = ["CORAL_PREVIOUS_KEY"]
"#,
        )
        .expect("config");

        assert_eq!(
            file.encryption.encryption_key_env.as_deref(),
            Some("CORAL_ACTIVE_KEY")
        );
        assert_eq!(file.encryption.decryption_key_envs, ["CORAL_PREVIOUS_KEY"]);
    }

    #[test]
    fn encryption_keys_are_rejected_under_the_credentials_section() {
        // The keys protect identity material too, so `[credentials]` must not
        // silently accept them and leave encryption unconfigured.
        toml::from_str::<CredentialStorageConfigFile>(
            r#"
[credentials]
encryption_key_env = "CORAL_ACTIVE_KEY"
"#,
        )
        .expect_err("encryption keys belong under [encryption]");
    }

    #[test]
    fn rejects_unknown_credential_fields() {
        let error = toml::from_str::<CredentialStorageConfigFile>(
            r#"
[credentials]
decryption_key_env = "CORAL_PREVIOUS_KEY"
"#,
        )
        .expect_err("misspelled key field");

        assert!(error.to_string().contains("unknown field"), "{error}");
    }
}
