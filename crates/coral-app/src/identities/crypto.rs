//! Exact-spec envelope encryption for identity setup documents.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "identity manager consumers land in the next stack layer"
    )
)]

use std::collections::BTreeMap;

use zeroize::Zeroizing;

use super::model::{IdentityName, IdentityOwner, IdentitySpecReference};
use crate::bootstrap::AppError;
use crate::credentials::CredentialsError;
use crate::credentials::encryption::{
    CredentialEncryptionKey, CredentialKeyProvider, ENVELOPE_DOCUMENT_ALGORITHM, EnvelopeContext,
    open_envelope_document, rewrap_envelope_document, seal_envelope_document,
};
use crate::encrypted_document::EncryptedEnvelopeDocument;

/// Envelope algorithm identifier for encrypted identity setup documents.
pub(crate) const IDENTITY_DOCUMENT_ALGORITHM: &str = ENVELOPE_DOCUMENT_ALGORITHM;
/// Authenticated binding layout for encrypted identity setup documents.
pub(crate) const IDENTITY_DOCUMENT_BINDING_VERSION: i64 = 1;

const IDENTITY_DOCUMENT_VERSION: u32 = 1;

#[derive(serde::Serialize)]
struct PlaintextIdentityDocument<'a> {
    version: u32,
    values: &'a BTreeMap<String, String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DecryptedIdentityDocument {
    version: u32,
    values: BTreeMap<String, String>,
}

/// Validated metadata authenticated with one identity setup document.
pub(super) struct IdentityDocumentBinding<'a> {
    owner: &'a IdentityOwner,
    name: &'a IdentityName,
    spec_reference: &'a IdentitySpecReference,
}

impl<'a> IdentityDocumentBinding<'a> {
    /// Bind an identity instance to a compatible exact spec reference.
    pub(super) fn new(
        owner: &'a IdentityOwner,
        name: &'a IdentityName,
        spec_reference: &'a IdentitySpecReference,
    ) -> Result<Self, AppError> {
        spec_reference.validate_for_owner(owner)?;
        Ok(Self {
            owner,
            name,
            spec_reference,
        })
    }
}

/// Encrypt identity setup values for one exact owner, instance, and resolved spec revision.
pub(super) fn encrypt_identity_document(
    binding: &IdentityDocumentBinding<'_>,
    values: &BTreeMap<String, String>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<EncryptedEnvelopeDocument, CredentialsError> {
    let plaintext = PlaintextIdentityDocument {
        version: IDENTITY_DOCUMENT_VERSION,
        values,
    };
    let document_bytes = serde_json::to_vec(&plaintext)
        .map(Zeroizing::new)
        .map_err(|error| CredentialsError::Parse(error.to_string()))?;
    let context = identity_document_context(IDENTITY_DOCUMENT_BINDING_VERSION, binding)?;
    seal_envelope_document(&context, document_bytes, key_provider)
}

/// Decrypt identity setup values for the exact authenticated identity metadata.
///
/// Takes the stored KEK the document names, so reading identity material can neither
/// create nor rotate key material.
pub(super) fn decrypt_identity_document(
    binding: &IdentityDocumentBinding<'_>,
    document: &EncryptedEnvelopeDocument,
    kek: &CredentialEncryptionKey,
) -> Result<BTreeMap<String, String>, CredentialsError> {
    let context = identity_document_context(document.binding_version, binding)?;
    let plaintext = open_envelope_document(&context, document, kek)?;
    let decoded: DecryptedIdentityDocument = serde_json::from_slice(&plaintext)
        .map_err(|error| CredentialsError::Parse(error.to_string()))?;
    if decoded.version != IDENTITY_DOCUMENT_VERSION {
        return Err(CredentialsError::Parse(format!(
            "unsupported identity document version {}",
            decoded.version
        )));
    }
    Ok(decoded.values)
}

/// Authenticate and rewrap an identity setup document whose envelope key is stale.
pub(super) fn rewrap_identity_document(
    binding: &IdentityDocumentBinding<'_>,
    document: &EncryptedEnvelopeDocument,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<Option<EncryptedEnvelopeDocument>, CredentialsError> {
    let context = identity_document_context(document.binding_version, binding)?;
    rewrap_envelope_document(&context, document, key_provider)
}

fn identity_document_context(
    binding_version: i64,
    binding: &IdentityDocumentBinding<'_>,
) -> Result<EnvelopeContext, CredentialsError> {
    if binding_version != IDENTITY_DOCUMENT_BINDING_VERSION {
        return Err(CredentialsError::Crypto(format!(
            "unsupported identity document binding version {binding_version}"
        )));
    }
    let (scope_kind, scope_id, spec_name) = binding.spec_reference.key().document_aad_parts();
    EnvelopeContext::new(
        "coral-identity-document",
        binding_version,
        &[
            binding.owner.kind(),
            binding.owner.key(),
            binding.name.as_str(),
            scope_kind,
            scope_id,
            spec_name,
            binding.spec_reference.fingerprint(),
        ],
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use zeroize::Zeroizing;

    use super::{
        IDENTITY_DOCUMENT_ALGORITHM, IDENTITY_DOCUMENT_BINDING_VERSION, IdentityDocumentBinding,
        decrypt_identity_document, encrypt_identity_document, identity_document_context,
        rewrap_identity_document,
    };
    use crate::credentials::CredentialsError;
    use crate::credentials::encryption::{
        CredentialEncryptionKey, CredentialKeyProvider, EnvelopeContext, decrypt_credential_values,
        encrypt_credential_values, seal_envelope_document,
    };
    use crate::encrypted_document::EncryptedEnvelopeDocument;
    use crate::identities::model::{
        IdentityAudience, IdentityName, IdentityOwner, IdentitySpecReference,
    };
    use crate::identity::spec_document::{
        decrypt_identity_spec_document, encrypt_identity_spec_document,
    };
    use crate::sources::SourceName;
    use crate::state::db::IdentitySpecKey;
    use crate::workspaces::WorkspaceName;

    const FINGERPRINT: &str = "identity-manifest-v1:sha256:0000000000000000000000000000000000000000000000000000000000000000";
    const OTHER_FINGERPRINT: &str = "identity-manifest-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111";

    #[derive(Clone)]
    struct StaticKeyProvider(CredentialEncryptionKey);

    impl CredentialKeyProvider for StaticKeyProvider {
        fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
            Ok(self.0.clone())
        }

        fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
            (self.0.key_id() == key_id)
                .then(|| self.0.clone())
                .ok_or_else(|| CredentialsError::Crypto("missing test key".to_string()))
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
    fn setup_documents_round_trip_and_authenticate_every_identity_binding() {
        let provider = static_provider(11);
        let owner = workspace_owner("acme");
        let name = identity_name("primary");
        let reference = global_reference(&owner, "github", FINGERPRINT);
        let binding = document_binding(&owner, &name, &reference);
        let values = secret_values();
        let encrypted = encrypt(&binding, &values, &provider);

        assert_eq!(encrypted.algorithm, IDENTITY_DOCUMENT_ALGORITHM);
        assert_eq!(encrypted.binding_version, IDENTITY_DOCUMENT_BINDING_VERSION);
        assert_eq!(decrypt(&binding, &encrypted, &provider), values);

        let wrong_kind_owner = user_owner("acme");
        let wrong_kind_reference = global_reference(&wrong_kind_owner, "github", FINGERPRINT);
        let wrong_key_owner = workspace_owner("other");
        let wrong_key_reference = global_reference(&wrong_key_owner, "github", FINGERPRINT);
        let workspace_reference = workspace_reference(&owner, "acme", "github", FINGERPRINT);
        let wrong_name = identity_name("secondary");
        let wrong_spec_name = global_reference(&owner, "gitlab", FINGERPRINT);
        let wrong_fingerprint = global_reference(&owner, "github", OTHER_FINGERPRINT);
        let wrong_cases = [
            (&wrong_kind_owner, &name, &wrong_kind_reference),
            (&wrong_key_owner, &name, &wrong_key_reference),
            (&owner, &wrong_name, &reference),
            (&owner, &name, &workspace_reference),
            (&owner, &name, &wrong_spec_name),
            (&owner, &name, &wrong_fingerprint),
        ];
        for (wrong_owner, wrong_name, wrong_reference) in wrong_cases {
            let wrong_binding = document_binding(wrong_owner, wrong_name, wrong_reference);
            assert_open_failed(
                &decrypt_with_provider(&wrong_binding, &encrypted, &provider)
                    .expect_err("changed identity binding must fail"),
            );
        }

        let mut unsupported = encrypted;
        unsupported.binding_version += 1;
        let error = decrypt_with_provider(&binding, &unsupported, &provider)
            .expect_err("unknown binding version must fail");
        assert!(
            error
                .to_string()
                .contains("unsupported identity document binding version 2")
        );
    }

    #[test]
    fn binding_rejects_a_workspace_reference_for_another_owner() {
        let owner = workspace_owner("acme");
        let other_owner = workspace_owner("other");
        let name = identity_name("primary");
        let reference = workspace_reference(&owner, "acme", "github", FINGERPRINT);

        assert!(IdentityDocumentBinding::new(&other_owner, &name, &reference).is_err());
    }

    #[test]
    fn identity_binding_is_not_colon_ambiguous() {
        let provider = static_provider(13);
        let owner = user_owner("a:b");
        let name = identity_name("c");
        let reference = global_reference(&owner, "github", FINGERPRINT);
        let binding = document_binding(&owner, &name, &reference);
        let encrypted = encrypt(&binding, &secret_values(), &provider);

        let split_owner = user_owner("a");
        let split_name = identity_name("b:c");
        let split_reference = global_reference(&split_owner, "github", FINGERPRINT);
        let split_binding = document_binding(&split_owner, &split_name, &split_reference);
        assert_open_failed(
            &decrypt_with_provider(&split_binding, &encrypted, &provider)
                .expect_err("colon-equivalent fields must not share a binding"),
        );
    }

    #[test]
    fn credential_spec_and_identity_document_domains_are_separate() {
        let provider = static_provider(17);
        let owner = workspace_owner("acme");
        let name = identity_name("primary");
        let reference = workspace_reference(&owner, "acme", "github", FINGERPRINT);
        let binding = document_binding(&owner, &name, &reference);
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let source = SourceName::parse("github").expect("source");
        let values = secret_values();
        let credential = encrypt_credential_values(&workspace, &source, &values, &provider)
            .expect("credential encryption");
        let spec = encrypt_identity_spec_document(reference.key(), &values, &provider)
            .expect("spec encryption");
        let identity = encrypt(&binding, &values, &provider);

        let credential_error = decrypt_with_provider(&binding, &credential, &provider)
            .expect_err("credential must not open as identity material");
        assert!(
            credential_error
                .to_string()
                .contains("unsupported identity document binding version 2"),
            "unexpected error: {credential_error}"
        );
        assert_open_failed(
            &decrypt_with_provider(&binding, &spec, &provider)
                .expect_err("spec material must not open as identity material"),
        );
        assert_open_failed(
            &decrypt_credential_values(&workspace, &source, &identity, &provider.0)
                .expect_err("identity material must not open as credentials"),
        );
        assert_open_failed(
            &decrypt_identity_spec_document(reference.key(), &identity, &provider.0)
                .expect_err("identity material must not open as spec material"),
        );
    }

    #[test]
    fn setup_documents_reject_legacy_aad_unknown_plaintext_and_tampering() {
        let provider = static_provider(23);
        let owner = user_owner("alice");
        let name = identity_name("github");
        let reference = global_reference(&owner, "github", FINGERPRINT);
        let binding = document_binding(&owner, &name, &reference);
        let legacy_context = EnvelopeContext::new(
            "coral-identity-document",
            IDENTITY_DOCUMENT_BINDING_VERSION,
            &[owner.kind(), owner.key(), name.as_str()],
        )
        .expect("legacy context");
        let legacy = seal_envelope_document(
            &legacy_context,
            Zeroizing::new(
                serde_json::to_vec(&serde_json::json!({"version": 1, "values": {}}))
                    .expect("legacy plaintext"),
            ),
            &provider,
        )
        .expect("legacy seal");
        assert_open_failed(
            &decrypt_with_provider(&binding, &legacy, &provider)
                .expect_err("legacy owner/name-only AAD must fail"),
        );

        let unknown = seal_plaintext(
            &binding,
            serde_json::to_vec(&serde_json::json!({"version": 2, "values": {}}))
                .expect("unknown plaintext"),
            &provider,
        );
        assert!(
            decrypt_with_provider(&binding, &unknown, &provider)
                .expect_err("unknown version must fail")
                .to_string()
                .contains("identity document version 2")
        );
        for plaintext in [
            b"{".to_vec(),
            serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "values": {},
                "unsupported": true,
            }))
            .expect("unsupported plaintext"),
        ] {
            let invalid = seal_plaintext(&binding, plaintext, &provider);
            assert!(matches!(
                decrypt_with_provider(&binding, &invalid, &provider),
                Err(CredentialsError::Parse(_))
            ));
        }

        let mut tampered = encrypt(&binding, &secret_values(), &provider);
        *tampered.ciphertext.first_mut().expect("ciphertext") ^= 1;
        assert_open_failed(
            &decrypt_with_provider(&binding, &tampered, &provider)
                .expect_err("tampered ciphertext must fail"),
        );
    }

    #[test]
    fn setup_document_rewrap_preserves_payload_and_authenticates_noop() {
        let old_key = CredentialEncryptionKey::from_static_bytes_for_test([29; 32]);
        let new_key = CredentialEncryptionKey::from_static_bytes_for_test([31; 32]);
        let old_provider = RotatingKeyProvider {
            active: old_key.clone(),
            keys: vec![old_key.clone()],
        };
        let rotating_provider = RotatingKeyProvider {
            active: new_key.clone(),
            keys: vec![old_key, new_key.clone()],
        };
        let owner = user_owner("alice");
        let name = identity_name("github");
        let reference = global_reference(&owner, "github", FINGERPRINT);
        let binding = document_binding(&owner, &name, &reference);
        let encrypted = encrypt(&binding, &secret_values(), &old_provider);
        let rewrapped = rewrap_identity_document(&binding, &encrypted, &rotating_provider)
            .expect("rewrap")
            .expect("stale key must rewrap");

        assert_eq!(rewrapped.key_id, new_key.key_id());
        assert_eq!(rewrapped.ciphertext, encrypted.ciphertext);
        assert_eq!(rewrapped.nonce, encrypted.nonce);
        assert_eq!(
            decrypt(&binding, &rewrapped, &rotating_provider),
            secret_values()
        );
        assert!(
            rewrap_identity_document(&binding, &rewrapped, &rotating_provider)
                .expect("current key")
                .is_none()
        );
        let wrong_owner = user_owner("mallory");
        let wrong_reference = global_reference(&wrong_owner, "github", FINGERPRINT);
        let wrong_binding = document_binding(&wrong_owner, &name, &wrong_reference);
        assert_open_failed(
            &rewrap_identity_document(&wrong_binding, &rewrapped, &rotating_provider)
                .expect_err("same-key rewrap must still authenticate the document"),
        );
    }

    #[test]
    fn diagnostics_do_not_expose_plaintext_values() {
        let provider = static_provider(37);
        let owner = user_owner("alice");
        let name = identity_name("github");
        let reference = global_reference(&owner, "github", FINGERPRINT);
        let binding = document_binding(&owner, &name, &reference);
        let marker = "TOP_SECRET_IDENTITY_MARKER";
        let values = BTreeMap::from([("TOKEN".to_string(), marker.to_string())]);
        let mut encrypted = encrypt(&binding, &values, &provider);

        let debug = format!("{encrypted:?}");
        assert!(debug.contains("ciphertext_len"));
        assert!(!debug.contains(&format!("{:?}", encrypted.ciphertext)));
        assert!(
            !encrypted
                .ciphertext
                .windows(marker.len())
                .any(|window| window == marker.as_bytes())
        );
        *encrypted.ciphertext.first_mut().expect("ciphertext") ^= 1;
        let error =
            decrypt_with_provider(&binding, &encrypted, &provider).expect_err("tampered document");
        assert!(!error.to_string().contains(marker));
        assert!(!format!("{error:?}").contains(marker));
    }

    fn static_provider(byte: u8) -> StaticKeyProvider {
        StaticKeyProvider(CredentialEncryptionKey::from_static_bytes_for_test(
            [byte; 32],
        ))
    }

    fn user_owner(user: &str) -> IdentityOwner {
        IdentityOwner::for_user(
            crate::identity::Principal::parse(user, crate::identity::PrincipalKind::User)
                .expect("user"),
        )
    }

    fn workspace_owner(workspace: &str) -> IdentityOwner {
        IdentityOwner::workspace(WorkspaceName::parse(workspace).expect("workspace"))
    }

    fn identity_name(name: &str) -> IdentityName {
        IdentityName::parse(name).expect("identity name")
    }

    fn global_reference(
        owner: &IdentityOwner,
        spec_name: &str,
        fingerprint: &str,
    ) -> IdentitySpecReference {
        IdentitySpecReference::new(
            owner,
            IdentitySpecKey::global(spec_name).expect("global key"),
            fingerprint,
            "github",
            "fixed_token",
            IdentityAudience::new("api.github.com", None).expect("audience"),
        )
        .expect("reference")
    }

    fn workspace_reference(
        owner: &IdentityOwner,
        workspace: &str,
        spec_name: &str,
        fingerprint: &str,
    ) -> IdentitySpecReference {
        IdentitySpecReference::new(
            owner,
            IdentitySpecKey::workspace(
                WorkspaceName::parse(workspace).expect("workspace"),
                spec_name,
            )
            .expect("workspace key"),
            fingerprint,
            "github",
            "fixed_token",
            IdentityAudience::new("api.github.com", None).expect("audience"),
        )
        .expect("reference")
    }

    fn secret_values() -> BTreeMap<String, String> {
        BTreeMap::from([("TOKEN".to_string(), "secret".to_string())])
    }

    fn document_binding<'a>(
        owner: &'a IdentityOwner,
        name: &'a IdentityName,
        reference: &'a IdentitySpecReference,
    ) -> IdentityDocumentBinding<'a> {
        IdentityDocumentBinding::new(owner, name, reference).expect("binding")
    }

    fn encrypt(
        binding: &IdentityDocumentBinding<'_>,
        values: &BTreeMap<String, String>,
        provider: &dyn CredentialKeyProvider,
    ) -> EncryptedEnvelopeDocument {
        encrypt_identity_document(binding, values, provider).expect("identity encryption")
    }

    fn decrypt(
        binding: &IdentityDocumentBinding<'_>,
        document: &EncryptedEnvelopeDocument,
        provider: &dyn CredentialKeyProvider,
    ) -> BTreeMap<String, String> {
        decrypt_with_provider(binding, document, provider).expect("identity decryption")
    }

    /// Resolve the KEK a document names, then decrypt with it.
    fn decrypt_with_provider(
        binding: &IdentityDocumentBinding<'_>,
        document: &EncryptedEnvelopeDocument,
        provider: &dyn CredentialKeyProvider,
    ) -> Result<BTreeMap<String, String>, CredentialsError> {
        let kek = provider.key(&document.key_id)?;
        decrypt_identity_document(binding, document, &kek)
    }

    fn seal_plaintext(
        binding: &IdentityDocumentBinding<'_>,
        plaintext: Vec<u8>,
        provider: &dyn CredentialKeyProvider,
    ) -> EncryptedEnvelopeDocument {
        let context = identity_document_context(IDENTITY_DOCUMENT_BINDING_VERSION, binding)
            .expect("identity document context");
        seal_envelope_document(&context, Zeroizing::new(plaintext), provider)
            .expect("seal plaintext")
    }

    fn assert_open_failed(error: &CredentialsError) {
        assert!(
            error.to_string().contains("open failed"),
            "unexpected error: {error}"
        );
    }
}
