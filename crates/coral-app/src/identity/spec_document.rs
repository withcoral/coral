//! Envelope crypto for encrypted identity-spec setup documents.
//!
//! Setup values are sealed under a domain of their own, with AAD bound to the
//! exact durable [`IdentitySpecKey`] that owns them, so a credential envelope
//! can never be replayed as an identity-spec document and no spec key can open
//! material belonging to another scope or name.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Identity-spec document crypto lands before manager consumers."
    )
)]

use std::collections::BTreeMap;

use zeroize::Zeroizing;

use crate::credentials::CredentialsError;
use crate::credentials::encryption::{
    CredentialKeyProvider, ENVELOPE_DOCUMENT_ALGORITHM, EnvelopeContext, open_envelope_document,
    rewrap_envelope_document, seal_envelope_document,
};
use crate::encrypted_document::EncryptedEnvelopeDocument;
use crate::state::db::IdentitySpecKey;

/// Envelope algorithm identifier for encrypted identity-spec setup documents.
pub(crate) const IDENTITY_SPEC_DOCUMENT_ALGORITHM: &str = ENVELOPE_DOCUMENT_ALGORITHM;
/// Authenticated binding layout for encrypted identity-spec setup documents.
pub(crate) const IDENTITY_SPEC_DOCUMENT_BINDING_VERSION: i64 = 1;

const IDENTITY_SPEC_DOCUMENT_VERSION: u32 = 1;

#[derive(serde::Serialize)]
struct PlaintextIdentitySpecDocument<'a> {
    version: u32,
    values: &'a BTreeMap<String, String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DecryptedIdentitySpecDocument {
    version: u32,
    values: BTreeMap<String, String>,
}

/// Encrypt identity-spec setup inputs with AAD bound to the exact durable spec key.
pub(crate) fn encrypt_identity_spec_document(
    key: &IdentitySpecKey,
    values: &BTreeMap<String, String>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<EncryptedEnvelopeDocument, CredentialsError> {
    let plaintext = PlaintextIdentitySpecDocument {
        version: IDENTITY_SPEC_DOCUMENT_VERSION,
        values,
    };
    let document_bytes = serde_json::to_vec(&plaintext)
        .map(Zeroizing::new)
        .map_err(|error| CredentialsError::Parse(error.to_string()))?;
    let context = identity_spec_document_context(IDENTITY_SPEC_DOCUMENT_BINDING_VERSION, key)?;
    seal_envelope_document(&context, document_bytes, key_provider)
}

/// Decrypt identity-spec setup inputs for the exact durable spec key.
pub(crate) fn decrypt_identity_spec_document(
    key: &IdentitySpecKey,
    document: &EncryptedEnvelopeDocument,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<BTreeMap<String, String>, CredentialsError> {
    let context = identity_spec_document_context(document.binding_version, key)?;
    let plaintext = open_envelope_document(&context, document, key_provider)?;
    let decoded: DecryptedIdentitySpecDocument = serde_json::from_slice(&plaintext)
        .map_err(|error| CredentialsError::Parse(error.to_string()))?;
    if decoded.version != IDENTITY_SPEC_DOCUMENT_VERSION {
        return Err(CredentialsError::Parse(format!(
            "unsupported identity spec document version {}",
            decoded.version
        )));
    }
    Ok(decoded.values)
}

#[cfg(test)]
pub(crate) fn seal_identity_spec_plaintext_for_test(
    key: &IdentitySpecKey,
    plaintext: Vec<u8>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<EncryptedEnvelopeDocument, CredentialsError> {
    let context = identity_spec_document_context(IDENTITY_SPEC_DOCUMENT_BINDING_VERSION, key)?;
    seal_envelope_document(&context, Zeroizing::new(plaintext), key_provider)
}

/// Rewrap an identity-spec setup document after authenticating its exact durable key.
pub(crate) fn rewrap_identity_spec_document(
    key: &IdentitySpecKey,
    document: &EncryptedEnvelopeDocument,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<Option<EncryptedEnvelopeDocument>, CredentialsError> {
    let context = identity_spec_document_context(document.binding_version, key)?;
    rewrap_envelope_document(&context, document, key_provider)
}

fn identity_spec_document_context(
    binding_version: i64,
    key: &IdentitySpecKey,
) -> Result<EnvelopeContext, CredentialsError> {
    if binding_version != IDENTITY_SPEC_DOCUMENT_BINDING_VERSION {
        return Err(CredentialsError::Crypto(format!(
            "unsupported identity spec binding version {binding_version}"
        )));
    }
    let (scope_kind, scope_id, name) = key.document_aad_parts();
    EnvelopeContext::new(
        "coral-identity-spec-document",
        binding_version,
        &[scope_kind, scope_id, name],
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use zeroize::Zeroizing;

    use super::{
        IDENTITY_SPEC_DOCUMENT_ALGORITHM, IDENTITY_SPEC_DOCUMENT_BINDING_VERSION,
        decrypt_identity_spec_document, encrypt_identity_spec_document,
        identity_spec_document_context, rewrap_identity_spec_document,
    };
    use crate::credentials::CredentialsError;
    use crate::credentials::encryption::test_support::{RotatingKeyProvider, StaticKeyProvider};
    use crate::credentials::encryption::{
        CredentialEncryptionKey, CredentialKeyProvider, EnvelopeContext, decrypt_credential_values,
        seal_envelope_document,
    };
    use crate::encrypted_document::EncryptedEnvelopeDocument;
    use crate::sources::SourceName;
    use crate::state::db::IdentitySpecKey;
    use crate::workspaces::WorkspaceName;

    #[test]
    fn setup_documents_round_trip_and_authenticate_exact_spec_keys() {
        let provider = static_provider(43);
        let values = secret_values();
        let global = IdentitySpecKey::global("github_oauth").expect("global key");
        let encrypted = encrypt_for_key(&global, &values, &provider);

        assert_eq!(encrypted.algorithm, IDENTITY_SPEC_DOCUMENT_ALGORITHM);
        assert_eq!(
            encrypted.binding_version,
            IDENTITY_SPEC_DOCUMENT_BINDING_VERSION
        );
        assert_eq!(decrypt_for_key(&global, &encrypted, &provider), values);

        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let wrong_scope =
            IdentitySpecKey::workspace(workspace, "github_oauth").expect("workspace key");
        assert_open_failed(
            &decrypt_for_key_result(&wrong_scope, &encrypted, &provider)
                .expect_err("workspace key must not open global material"),
        );
        let wrong_name = IdentitySpecKey::global("gitlab_oauth").expect("global key");
        assert_open_failed(
            &decrypt_for_key_result(&wrong_name, &encrypted, &provider)
                .expect_err("other spec name must not open material"),
        );

        let mut unsupported = encrypted;
        unsupported.binding_version += 1;
        let error = decrypt_for_key_result(&global, &unsupported, &provider)
            .expect_err("unknown binding version must fail");
        assert!(
            error
                .to_string()
                .contains("unsupported identity spec binding version 2")
        );
    }

    #[test]
    fn credential_and_identity_spec_document_domains_are_separate() {
        let provider = static_provider(47);
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let source = SourceName::parse("github_oauth").expect("source");
        let key = IdentitySpecKey::workspace(workspace.clone(), "github_oauth").expect("key");
        let values = secret_values();
        let credential_context = EnvelopeContext::new(
            "coral-credential-document",
            1,
            &[workspace.as_str(), source.as_str()],
        )
        .expect("credential context");
        let credential_plaintext = serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "values": &values,
        }))
        .expect("serialize credential");
        let credential = seal_envelope_document(
            &credential_context,
            Zeroizing::new(credential_plaintext),
            &provider,
        )
        .expect("encrypt credential");
        let spec = encrypt_for_key(&key, &values, &provider);

        assert_open_failed(
            &decrypt_for_key_result(&key, &credential, &provider)
                .expect_err("credential must not open as identity-spec material"),
        );
        assert_open_failed(
            &decrypt_credential_values(&workspace, &source, &spec, &provider)
                .expect_err("identity-spec material must not open as credentials"),
        );
    }

    #[test]
    fn setup_documents_reject_unknown_plaintext_versions() {
        let provider = static_provider(53);
        let key = IdentitySpecKey::global("github_oauth").expect("key");
        let plaintext = serde_json::to_vec(&serde_json::json!({
            "version": 2,
            "values": {"TOKEN": "secret"},
        }))
        .expect("serialize plaintext");
        let context = identity_spec_document_context(IDENTITY_SPEC_DOCUMENT_BINDING_VERSION, &key)
            .expect("identity spec context");
        let encrypted = seal_envelope_document(&context, Zeroizing::new(plaintext), &provider)
            .expect("seal document");

        let error = decrypt_for_key_result(&key, &encrypted, &provider)
            .expect_err("unknown plaintext version must fail");
        assert!(
            error
                .to_string()
                .contains("identity spec document version 2")
        );
    }

    #[test]
    fn setup_document_rewrap_preserves_payload_and_authenticates_current_key() {
        let old_key = CredentialEncryptionKey::from_static_bytes_for_test([59; 32]);
        let new_key = CredentialEncryptionKey::from_static_bytes_for_test([61; 32]);
        let old_provider = RotatingKeyProvider {
            active: old_key.clone(),
            keys: vec![old_key.clone()],
        };
        let rotating_provider = RotatingKeyProvider {
            active: new_key.clone(),
            keys: vec![old_key, new_key.clone()],
        };
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let key = IdentitySpecKey::workspace(workspace.clone(), "github_oauth").expect("key");
        let encrypted = encrypt_for_key(&key, &secret_values(), &old_provider);
        let rewrapped = rewrap_identity_spec_document(&key, &encrypted, &rotating_provider)
            .expect("rewrap")
            .expect("stale key must rewrap");

        assert_eq!(rewrapped.key_id, new_key.key_id());
        assert_eq!(rewrapped.ciphertext, encrypted.ciphertext);
        assert_eq!(rewrapped.nonce, encrypted.nonce);
        assert_ne!(rewrapped.wrapped_dek, encrypted.wrapped_dek);
        assert_ne!(rewrapped.wrapped_dek_nonce, encrypted.wrapped_dek_nonce);
        assert_eq!(
            decrypt_for_key(&key, &rewrapped, &rotating_provider),
            secret_values()
        );
        assert!(
            rewrap_identity_spec_document(&key, &rewrapped, &rotating_provider,)
                .expect("current rewrap")
                .is_none()
        );
        let wrong = IdentitySpecKey::workspace(workspace, "gitlab_oauth").expect("wrong key");
        assert_open_failed(
            &rewrap_identity_spec_document(&wrong, &rewrapped, &rotating_provider)
                .expect_err("same-key rewrap must authenticate exact spec key"),
        );
    }

    fn static_provider(byte: u8) -> StaticKeyProvider {
        StaticKeyProvider {
            key: CredentialEncryptionKey::from_static_bytes_for_test([byte; 32]),
        }
    }

    fn secret_values() -> BTreeMap<String, String> {
        BTreeMap::from([("TOKEN".to_string(), "secret".to_string())])
    }

    fn encrypt_for_key(
        key: &IdentitySpecKey,
        values: &BTreeMap<String, String>,
        provider: &dyn CredentialKeyProvider,
    ) -> EncryptedEnvelopeDocument {
        encrypt_identity_spec_document(key, values, provider)
            .expect("encrypt identity-spec material")
    }

    fn decrypt_for_key(
        key: &IdentitySpecKey,
        document: &EncryptedEnvelopeDocument,
        provider: &dyn CredentialKeyProvider,
    ) -> BTreeMap<String, String> {
        decrypt_for_key_result(key, document, provider).expect("decrypt identity-spec material")
    }

    fn decrypt_for_key_result(
        key: &IdentitySpecKey,
        document: &EncryptedEnvelopeDocument,
        provider: &dyn CredentialKeyProvider,
    ) -> Result<BTreeMap<String, String>, CredentialsError> {
        decrypt_identity_spec_document(key, document, provider)
    }

    fn assert_open_failed(error: &CredentialsError) {
        assert!(
            error.to_string().contains("open failed"),
            "unexpected error: {error}"
        );
    }
}
