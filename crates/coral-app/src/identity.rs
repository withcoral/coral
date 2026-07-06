//! Shared validation helpers for app-owned identifiers.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Identity document crypto lands before later manager and repository units consume it."
    )
)]

use std::collections::BTreeMap;

use zeroize::Zeroizing;

use crate::bootstrap::AppError;
use crate::credentials::CredentialsError;
use crate::credentials::encryption::{
    CREDENTIAL_DOCUMENT_AAD_VERSION, CREDENTIAL_DOCUMENT_ALGORITHM, CredentialKeyProvider,
    EncryptedEnvelopeDocument, encode_aad_fields, open_envelope_document, rewrap_envelope_document,
    seal_envelope_document,
};

/// Envelope algorithm identifier for encrypted identity documents.
pub(crate) const IDENTITY_DOCUMENT_ALGORITHM: &str = CREDENTIAL_DOCUMENT_ALGORITHM;
/// AAD layout version for encrypted identity documents.
pub(crate) const IDENTITY_DOCUMENT_AAD_VERSION: i64 = CREDENTIAL_DOCUMENT_AAD_VERSION;

const IDENTITY_DOCUMENT_VERSION: u32 = 1;

/// Plaintext setup-input values stored for an identity spec document.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct PlaintextIdentitySpecDocument {
    /// Plaintext identity spec document schema version.
    pub(crate) version: u32,
    /// Identity spec setup-input values serialized before envelope encryption.
    pub(crate) values: BTreeMap<String, String>,
}

/// Plaintext values stored for an identity instance document.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct PlaintextIdentityDocument {
    /// Plaintext identity document schema version.
    pub(crate) version: u32,
    /// Identity instance values serialized before envelope encryption.
    pub(crate) values: BTreeMap<String, String>,
}

pub(crate) fn parse_path_segment(kind: &str, value: &str) -> Result<String, AppError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::InvalidInput(format!("missing {kind} name")));
    }
    if trimmed.contains('/') || trimmed.contains('\\') {
        return Err(AppError::InvalidInput(format!(
            "{kind} name must not contain '/' or '\\\\'"
        )));
    }
    if trimmed == "." || trimmed == ".." {
        return Err(AppError::InvalidInput(format!(
            "{kind} name must not be '.' or '..'"
        )));
    }
    Ok(trimmed.to_string())
}

/// Build AAD for an encrypted identity-spec setup-input document.
pub(crate) fn identity_spec_document_aad(scope_kind: &str, scope_id: &str, name: &str) -> Vec<u8> {
    let aad_version = IDENTITY_DOCUMENT_AAD_VERSION.to_string();
    encode_aad_fields(
        "coral-identity-spec-document",
        &[
            aad_version.as_str(),
            scope_kind,
            scope_id,
            name,
            IDENTITY_DOCUMENT_ALGORITHM,
        ],
    )
}

/// Build AAD for an encrypted identity instance document.
pub(crate) fn identity_document_aad(owner_kind: &str, owner_key: &str, name: &str) -> Vec<u8> {
    let aad_version = IDENTITY_DOCUMENT_AAD_VERSION.to_string();
    encode_aad_fields(
        "coral-identity-document",
        &[
            aad_version.as_str(),
            owner_kind,
            owner_key,
            name,
            IDENTITY_DOCUMENT_ALGORITHM,
        ],
    )
}

/// Encrypt an identity-spec setup-input document with the shared app KEK.
pub(crate) fn encrypt_identity_spec_document(
    scope_kind: &str,
    scope_id: &str,
    name: &str,
    values: &BTreeMap<String, String>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<EncryptedEnvelopeDocument, CredentialsError> {
    let plaintext = PlaintextIdentitySpecDocument {
        version: IDENTITY_DOCUMENT_VERSION,
        values: values.clone(),
    };
    let document_bytes = Zeroizing::new(
        serde_json::to_vec(&plaintext)
            .map_err(|error| CredentialsError::Parse(error.to_string()))?,
    );
    seal_envelope_document(
        identity_spec_document_aad(scope_kind, scope_id, name),
        document_bytes,
        key_provider,
    )
}

/// Decrypt an identity-spec setup-input document with the shared app KEK.
pub(crate) fn decrypt_identity_spec_document(
    scope_kind: &str,
    scope_id: &str,
    name: &str,
    document: &EncryptedEnvelopeDocument,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<BTreeMap<String, String>, CredentialsError> {
    let plaintext = open_envelope_document(
        document,
        identity_spec_document_aad(scope_kind, scope_id, name),
        key_provider,
    )?;
    let decoded: PlaintextIdentitySpecDocument = serde_json::from_slice(&plaintext)
        .map_err(|error| CredentialsError::Parse(error.to_string()))?;
    if decoded.version != IDENTITY_DOCUMENT_VERSION {
        return Err(CredentialsError::Parse(format!(
            "unsupported identity spec document version {}",
            decoded.version
        )));
    }
    Ok(decoded.values)
}

/// Rewrap an identity-spec setup-input document when its KEK is stale.
pub(crate) fn rewrap_identity_spec_document(
    scope_kind: &str,
    scope_id: &str,
    name: &str,
    document: &EncryptedEnvelopeDocument,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<Option<EncryptedEnvelopeDocument>, CredentialsError> {
    rewrap_envelope_document(
        document,
        identity_spec_document_aad(scope_kind, scope_id, name),
        key_provider,
    )
}

/// Encrypt an identity instance document with the shared app KEK.
pub(crate) fn encrypt_identity_document(
    owner_kind: &str,
    owner_key: &str,
    name: &str,
    values: &BTreeMap<String, String>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<EncryptedEnvelopeDocument, CredentialsError> {
    let plaintext = PlaintextIdentityDocument {
        version: IDENTITY_DOCUMENT_VERSION,
        values: values.clone(),
    };
    let document_bytes = Zeroizing::new(
        serde_json::to_vec(&plaintext)
            .map_err(|error| CredentialsError::Parse(error.to_string()))?,
    );
    seal_envelope_document(
        identity_document_aad(owner_kind, owner_key, name),
        document_bytes,
        key_provider,
    )
}

/// Decrypt an identity instance document with the shared app KEK.
pub(crate) fn decrypt_identity_document(
    owner_kind: &str,
    owner_key: &str,
    name: &str,
    document: &EncryptedEnvelopeDocument,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<BTreeMap<String, String>, CredentialsError> {
    let plaintext = open_envelope_document(
        document,
        identity_document_aad(owner_kind, owner_key, name),
        key_provider,
    )?;
    let decoded: PlaintextIdentityDocument = serde_json::from_slice(&plaintext)
        .map_err(|error| CredentialsError::Parse(error.to_string()))?;
    if decoded.version != IDENTITY_DOCUMENT_VERSION {
        return Err(CredentialsError::Parse(format!(
            "unsupported identity document version {}",
            decoded.version
        )));
    }
    Ok(decoded.values)
}

/// Rewrap an identity instance document when its KEK is stale.
pub(crate) fn rewrap_identity_document(
    owner_kind: &str,
    owner_key: &str,
    name: &str,
    document: &EncryptedEnvelopeDocument,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<Option<EncryptedEnvelopeDocument>, CredentialsError> {
    rewrap_envelope_document(
        document,
        identity_document_aad(owner_kind, owner_key, name),
        key_provider,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::encryption::{
        CredentialEncryptionKey, decrypt_credential_values, encrypt_credential_values,
    };
    use crate::sources::SourceName;
    use crate::workspaces::WorkspaceName;

    #[derive(Clone)]
    struct StaticKeyProvider {
        key: CredentialEncryptionKey,
    }

    impl CredentialKeyProvider for StaticKeyProvider {
        fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
            Ok(self.key.clone())
        }

        fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
            if self.key.key_id() == key_id {
                Ok(self.key.clone())
            } else {
                Err(CredentialsError::Crypto("missing test key".to_string()))
            }
        }
    }

    #[derive(Clone)]
    struct RotatingKeyProvider {
        active: CredentialEncryptionKey,
        keys: Vec<CredentialEncryptionKey>,
    }

    impl CredentialKeyProvider for RotatingKeyProvider {
        fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
            Ok(self.active.clone())
        }

        fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
            self.keys
                .iter()
                .find(|key| key.key_id() == key_id)
                .cloned()
                .ok_or_else(|| CredentialsError::Crypto("missing test key".to_string()))
        }
    }

    #[test]
    fn rejects_empty_names() {
        let error = parse_path_segment("source", "   ").expect_err("empty name should fail");
        assert!(error.to_string().contains("missing source name"));
    }

    #[test]
    fn rejects_path_separators() {
        let error = parse_path_segment("workspace", r"bad\name").expect_err("slash should fail");
        assert!(
            error
                .to_string()
                .contains("workspace name must not contain '/' or '\\\\'")
        );
    }

    #[test]
    fn rejects_dot_segments() {
        let error = parse_path_segment("source", "..").expect_err("dot segment should fail");
        assert!(
            error
                .to_string()
                .contains("source name must not be '.' or '..'")
        );
    }

    #[test]
    fn identity_spec_document_round_trips_and_pins_metadata() {
        let provider = StaticKeyProvider {
            key: CredentialEncryptionKey::from_static_bytes_for_test([31; 32]),
        };
        let values = BTreeMap::from([("client_secret".to_string(), "secret".to_string())]);

        let encrypted =
            encrypt_identity_spec_document("workspace", "acme", "github", &values, &provider)
                .expect("encrypt identity spec document");

        assert_eq!(encrypted.algorithm, IDENTITY_DOCUMENT_ALGORITHM);
        assert_eq!(encrypted.aad_version, IDENTITY_DOCUMENT_AAD_VERSION);
        assert_eq!(
            decrypt_identity_spec_document("workspace", "acme", "github", &encrypted, &provider)
                .expect("decrypt identity spec document"),
            values
        );
    }

    #[test]
    fn identity_document_round_trips_and_pins_metadata() {
        let provider = StaticKeyProvider {
            key: CredentialEncryptionKey::from_static_bytes_for_test([32; 32]),
        };
        let values = BTreeMap::from([("refresh_token".to_string(), "secret".to_string())]);

        let encrypted =
            encrypt_identity_document("workspace", "acme", "github", &values, &provider)
                .expect("encrypt identity document");

        assert_eq!(encrypted.algorithm, IDENTITY_DOCUMENT_ALGORITHM);
        assert_eq!(encrypted.aad_version, IDENTITY_DOCUMENT_AAD_VERSION);
        assert_eq!(
            decrypt_identity_document("workspace", "acme", "github", &encrypted, &provider)
                .expect("decrypt identity document"),
            values
        );
    }

    #[test]
    fn identity_and_credential_document_domains_are_separated() {
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let source = SourceName::parse("github").expect("source");
        let provider = StaticKeyProvider {
            key: CredentialEncryptionKey::from_static_bytes_for_test([33; 32]),
        };
        let values = BTreeMap::from([("token".to_string(), "secret".to_string())]);

        let credential = encrypt_credential_values(&workspace, &source, &values, &provider)
            .expect("encrypt credential");
        assert_open_failed(
            &decrypt_identity_spec_document("workspace", "acme", "github", &credential, &provider)
                .expect_err("credential domain should not open as identity spec"),
        );
        assert_open_failed(
            &decrypt_identity_document("workspace", "acme", "github", &credential, &provider)
                .expect_err("credential domain should not open as identity document"),
        );

        let spec =
            encrypt_identity_spec_document("workspace", "acme", "github", &values, &provider)
                .expect("encrypt identity spec");
        assert_open_failed(
            &decrypt_credential_values(&workspace, &source, &spec, &provider)
                .expect_err("identity spec domain should not open as credential"),
        );
        assert_open_failed(
            &decrypt_identity_document("workspace", "acme", "github", &spec, &provider)
                .expect_err("identity spec domain should not open as identity document"),
        );

        let identity = encrypt_identity_document("workspace", "acme", "github", &values, &provider)
            .expect("encrypt identity document");
        assert_open_failed(
            &decrypt_credential_values(&workspace, &source, &identity, &provider)
                .expect_err("identity document domain should not open as credential"),
        );
        assert_open_failed(
            &decrypt_identity_spec_document("workspace", "acme", "github", &identity, &provider)
                .expect_err("identity document domain should not open as identity spec"),
        );
    }

    #[test]
    fn identity_document_aad_is_ordered_and_length_prefixed() {
        let provider = StaticKeyProvider {
            key: CredentialEncryptionKey::from_static_bytes_for_test([34; 32]),
        };
        let values = BTreeMap::from([("token".to_string(), "secret".to_string())]);

        let spec = encrypt_identity_spec_document("a:b", "c", "github", &values, &provider)
            .expect("encrypt identity spec");
        assert_open_failed(
            &decrypt_identity_spec_document("a", "b:c", "github", &spec, &provider)
                .expect_err("colon-bearing identity spec fields should stay distinct"),
        );
        assert_open_failed(
            &decrypt_identity_spec_document("c", "a:b", "github", &spec, &provider)
                .expect_err("identity spec field order should authenticate"),
        );

        let identity =
            encrypt_identity_document("owner:kind", "owner", "github", &values, &provider)
                .expect("encrypt identity document");
        assert_open_failed(
            &decrypt_identity_document("owner", "kind:owner", "github", &identity, &provider)
                .expect_err("colon-bearing identity fields should stay distinct"),
        );
        assert_open_failed(
            &decrypt_identity_document("owner", "owner:kind", "github", &identity, &provider)
                .expect_err("identity field order should authenticate"),
        );
    }

    #[test]
    fn identity_documents_rewrap_stale_keks_without_reencrypting_payloads() {
        let old_key = CredentialEncryptionKey::from_static_bytes_for_test([35; 32]);
        let new_key = CredentialEncryptionKey::from_static_bytes_for_test([36; 32]);
        let old_provider = RotatingKeyProvider {
            active: old_key.clone(),
            keys: vec![old_key.clone()],
        };
        let rotating_provider = RotatingKeyProvider {
            active: new_key.clone(),
            keys: vec![old_key, new_key.clone()],
        };
        let values = BTreeMap::from([("token".to_string(), "secret".to_string())]);

        let spec =
            encrypt_identity_spec_document("workspace", "acme", "github", &values, &old_provider)
                .expect("encrypt identity spec");
        let rewrapped_spec =
            rewrap_identity_spec_document("workspace", "acme", "github", &spec, &rotating_provider)
                .expect("rewrap identity spec")
                .expect("stale spec key should rewrap");
        assert_rewrapped_document(&spec, &rewrapped_spec, new_key.key_id());
        assert_eq!(
            decrypt_identity_spec_document(
                "workspace",
                "acme",
                "github",
                &rewrapped_spec,
                &rotating_provider,
            )
            .expect("decrypt rewrapped identity spec"),
            values
        );

        let identity =
            encrypt_identity_document("workspace", "acme", "github", &values, &old_provider)
                .expect("encrypt identity document");
        let rewrapped_identity =
            rewrap_identity_document("workspace", "acme", "github", &identity, &rotating_provider)
                .expect("rewrap identity document")
                .expect("stale identity key should rewrap");
        assert_rewrapped_document(&identity, &rewrapped_identity, new_key.key_id());
        assert_eq!(
            decrypt_identity_document(
                "workspace",
                "acme",
                "github",
                &rewrapped_identity,
                &rotating_provider,
            )
            .expect("decrypt rewrapped identity document"),
            values
        );
        assert!(
            rewrap_identity_document(
                "workspace",
                "acme",
                "github",
                &rewrapped_identity,
                &rotating_provider,
            )
            .expect("rewrap current identity document")
            .is_none(),
            "active KEK should not produce another rewrap"
        );
    }

    fn assert_rewrapped_document(
        original: &EncryptedEnvelopeDocument,
        rewrapped: &EncryptedEnvelopeDocument,
        expected_key_id: &str,
    ) {
        assert_eq!(rewrapped.key_id, expected_key_id);
        assert_eq!(rewrapped.ciphertext, original.ciphertext);
        assert_eq!(rewrapped.nonce, original.nonce);
        assert_ne!(rewrapped.wrapped_dek, original.wrapped_dek);
        assert_ne!(rewrapped.wrapped_dek_nonce, original.wrapped_dek_nonce);
    }

    fn assert_open_failed(error: &CredentialsError) {
        assert!(
            error.to_string().contains("open failed"),
            "unexpected error: {error}"
        );
    }
}
