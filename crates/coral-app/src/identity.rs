//! Shared validation helpers for app-owned identifiers.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Identity document crypto lands before manager and repository consumers."
    )
)]

use std::collections::BTreeMap;
use std::fmt;

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

/// Stable local user used by single-user local mode.
pub(crate) const LOCAL_MEMBER_ID: &str = "local";

/// Request-scoped user principal selected by the app transport layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserPrincipal {
    user_id: String,
}

impl UserPrincipal {
    /// Builds the default single-user local principal.
    #[must_use]
    pub fn local() -> Self {
        Self {
            user_id: LOCAL_MEMBER_ID.to_string(),
        }
    }

    /// Builds a principal for a validated user id.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the user id is empty, contains whitespace,
    /// contains path separators, or aliases the reserved local single-user
    /// sentinel.
    pub fn for_user(user_id: &str) -> Result<Self, AppError> {
        if user_id.chars().any(char::is_whitespace) {
            return Err(AppError::InvalidInput(
                "user id must not contain whitespace".to_string(),
            ));
        }
        let user_id = parse_path_segment("user", user_id)?;
        if user_id == LOCAL_MEMBER_ID {
            return Err(AppError::InvalidInput(format!(
                "user id '{LOCAL_MEMBER_ID}' is reserved for single-user local mode"
            )));
        }
        Ok(Self { user_id })
    }

    /// Returns the validated user id.
    #[must_use]
    pub fn user_id(&self) -> &str {
        &self.user_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UserPrincipalProviderErrorKind {
    Unauthenticated,
    Unavailable,
    Internal,
}

/// Client-safe failure reported by a request principal provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserPrincipalProviderError {
    kind: UserPrincipalProviderErrorKind,
    client_message: String,
}

impl UserPrincipalProviderError {
    fn new(
        kind: UserPrincipalProviderErrorKind,
        client_message: impl Into<String>,
        default_message: &str,
    ) -> Self {
        let client_message = client_message.into();
        let client_message = if client_message.trim().is_empty() {
            default_message.to_string()
        } else {
            client_message
        };
        Self {
            kind,
            client_message,
        }
    }

    /// Builds a provider error with a client-safe message.
    #[must_use]
    pub fn unauthenticated(client_message: impl Into<String>) -> Self {
        Self::new(
            UserPrincipalProviderErrorKind::Unauthenticated,
            client_message,
            "unauthenticated request",
        )
    }

    /// Builds a transient provider failure with a client-safe message.
    #[must_use]
    pub fn unavailable(client_message: impl Into<String>) -> Self {
        Self::new(
            UserPrincipalProviderErrorKind::Unavailable,
            client_message,
            "user principal provider unavailable",
        )
    }

    /// Builds an unexpected provider failure with a client-safe message.
    #[must_use]
    pub fn internal(client_message: impl Into<String>) -> Self {
        Self::new(
            UserPrincipalProviderErrorKind::Internal,
            client_message,
            "user principal provider failed",
        )
    }

    pub(crate) fn kind(&self) -> UserPrincipalProviderErrorKind {
        self.kind
    }

    /// Returns the client-safe failure message.
    #[must_use]
    pub fn client_message(&self) -> &str {
        &self.client_message
    }
}

impl fmt::Display for UserPrincipalProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.client_message)
    }
}

impl std::error::Error for UserPrincipalProviderError {}

/// Server-side provider for request user principals.
///
/// The OSS provider always returns [`UserPrincipal::local`]. Product runtimes
/// can install a provider that authenticates inbound metadata and returns the
/// corresponding user principal.
#[tonic::async_trait]
pub trait UserPrincipalProvider: Send + Sync + std::fmt::Debug {
    /// Returns the user principal for one inbound gRPC request.
    ///
    /// # Errors
    ///
    /// Returns [`UserPrincipalProviderError`] when transport metadata is
    /// malformed, the provider cannot authenticate the request, or principal
    /// selection fails.
    async fn principal_for_metadata(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<UserPrincipal, UserPrincipalProviderError>;
}

/// Default OSS principal provider for single-user local mode.
#[derive(Debug, Default)]
pub struct SingleUserPrincipalProvider;

#[tonic::async_trait]
impl UserPrincipalProvider for SingleUserPrincipalProvider {
    async fn principal_for_metadata(
        &self,
        _metadata: &tonic::metadata::MetadataMap,
    ) -> Result<UserPrincipal, UserPrincipalProviderError> {
        Ok(UserPrincipal::local())
    }
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
    let document_bytes = serialize_identity_document(&plaintext)?;
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
    validate_identity_document_version("identity spec", decoded.version)?;
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
    let document_bytes = serialize_identity_document(&plaintext)?;
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
    validate_identity_document_version("identity", decoded.version)?;
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

fn serialize_identity_document(
    document: &impl serde::Serialize,
) -> Result<Zeroizing<Vec<u8>>, CredentialsError> {
    serde_json::to_vec(document)
        .map(Zeroizing::new)
        .map_err(|error| CredentialsError::Parse(error.to_string()))
}

fn validate_identity_document_version(kind: &str, version: u32) -> Result<(), CredentialsError> {
    if version != IDENTITY_DOCUMENT_VERSION {
        return Err(CredentialsError::Parse(format!(
            "unsupported {kind} document version {version}"
        )));
    }
    Ok(())
}

pub(crate) async fn run_key_operation<T, F>(operation: F) -> Result<T, AppError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, AppError> + Send + 'static,
{
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || span.in_scope(operation)).await?
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
    fn user_principal_rejects_whitespace_anywhere() {
        for invalid in [" saul", "saul ", "alice bob", "alice\tbob", "alice\nbob"] {
            let error = UserPrincipal::for_user(invalid).expect_err("whitespace should fail");

            assert!(
                error
                    .to_string()
                    .contains("user id must not contain whitespace")
            );
        }
    }

    #[test]
    fn user_principal_rejects_path_segments_and_reserved_local_id() {
        for invalid in ["team/saul", r"team\saul", ".", "..", "local"] {
            UserPrincipal::for_user(invalid).expect_err("invalid user id should fail");
        }
    }

    #[test]
    fn user_principal_preserves_valid_id() {
        let principal = UserPrincipal::for_user("saul").expect("valid user");

        assert_eq!(principal.user_id(), "saul");
    }

    #[tokio::test]
    async fn single_user_provider_returns_local_principal() {
        let principal = SingleUserPrincipalProvider
            .principal_for_metadata(&tonic::metadata::MetadataMap::new())
            .await
            .expect("local principal");

        assert_eq!(principal, UserPrincipal::local());
    }

    #[test]
    fn identity_documents_round_trip_and_pin_metadata() {
        let provider = static_provider(43);
        let values = secret_values();
        let spec =
            encrypt_identity_spec_document("global", "__global__", "github", &values, &provider)
                .expect("encrypt identity spec");
        let identity = encrypt_identity_document("user", "member-1", "github", &values, &provider)
            .expect("encrypt identity");

        assert_eq!(spec.algorithm, IDENTITY_DOCUMENT_ALGORITHM);
        assert_eq!(spec.aad_version, IDENTITY_DOCUMENT_AAD_VERSION);
        assert_eq!(identity.algorithm, IDENTITY_DOCUMENT_ALGORITHM);
        assert_eq!(identity.aad_version, IDENTITY_DOCUMENT_AAD_VERSION);
        assert_eq!(
            decrypt_identity_spec_document("global", "__global__", "github", &spec, &provider,)
                .expect("decrypt identity spec"),
            values
        );
        assert_eq!(
            decrypt_identity_document("user", "member-1", "github", &identity, &provider)
                .expect("decrypt identity"),
            values
        );
    }

    #[test]
    fn credential_spec_and_identity_domains_are_pairwise_separated() {
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let source = SourceName::parse("github").expect("source");
        let provider = static_provider(47);
        let values = secret_values();
        let credential = encrypt_credential_values(&workspace, &source, &values, &provider)
            .expect("encrypt credential");
        let spec =
            encrypt_identity_spec_document("workspace", "acme", "github", &values, &provider)
                .expect("encrypt identity spec");
        let identity = encrypt_identity_document("workspace", "acme", "github", &values, &provider)
            .expect("encrypt identity");

        assert_open_failed(
            &decrypt_identity_spec_document("workspace", "acme", "github", &credential, &provider)
                .expect_err("credential must not open as an identity spec"),
        );
        assert_open_failed(
            &decrypt_identity_document("workspace", "acme", "github", &credential, &provider)
                .expect_err("credential must not open as an identity"),
        );
        assert_open_failed(
            &decrypt_credential_values(&workspace, &source, &spec, &provider)
                .expect_err("identity spec must not open as a credential"),
        );
        assert_open_failed(
            &decrypt_identity_document("workspace", "acme", "github", &spec, &provider)
                .expect_err("identity spec must not open as an identity"),
        );
        assert_open_failed(
            &decrypt_credential_values(&workspace, &source, &identity, &provider)
                .expect_err("identity must not open as a credential"),
        );
        assert_open_failed(
            &decrypt_identity_spec_document("workspace", "acme", "github", &identity, &provider)
                .expect_err("identity must not open as an identity spec"),
        );
    }

    #[test]
    fn identity_aad_authenticates_ordered_length_prefixed_fields() {
        let provider = static_provider(53);
        let values = secret_values();
        let spec = encrypt_identity_spec_document("a:b", "c", "github", &values, &provider)
            .expect("encrypt identity spec");
        assert_open_failed(
            &decrypt_identity_spec_document("a", "b:c", "github", &spec, &provider)
                .expect_err("colon-bearing spec fields must stay distinct"),
        );
        assert_open_failed(
            &decrypt_identity_spec_document("c", "a:b", "github", &spec, &provider)
                .expect_err("spec field order must authenticate"),
        );
        assert_open_failed(
            &decrypt_identity_spec_document("a:b", "c", "gitlab", &spec, &provider)
                .expect_err("spec name must authenticate"),
        );

        let identity =
            encrypt_identity_document("owner:kind", "owner", "github", &values, &provider)
                .expect("encrypt identity");
        assert_open_failed(
            &decrypt_identity_document("owner", "kind:owner", "github", &identity, &provider)
                .expect_err("colon-bearing owner fields must stay distinct"),
        );
        assert_open_failed(
            &decrypt_identity_document("owner", "owner:kind", "github", &identity, &provider)
                .expect_err("owner field order must authenticate"),
        );
        assert_open_failed(
            &decrypt_identity_document("owner:kind", "owner", "gitlab", &identity, &provider)
                .expect_err("identity name must authenticate"),
        );
    }

    #[test]
    fn identity_documents_reject_tampering_and_invalid_metadata() {
        let provider = static_provider(59);
        let document = encrypt_identity_spec_document(
            "workspace",
            "acme",
            "github",
            &secret_values(),
            &provider,
        )
        .expect("encrypt identity spec");

        let mut tampered = document.clone();
        *tampered.ciphertext.first_mut().expect("ciphertext") ^= 1;
        assert_spec_open_failed(&tampered, &provider);
        let mut tampered = document.clone();
        *tampered.nonce.first_mut().expect("nonce") ^= 1;
        assert_spec_open_failed(&tampered, &provider);
        let mut tampered = document.clone();
        *tampered.wrapped_dek.first_mut().expect("wrapped DEK") ^= 1;
        assert_spec_open_failed(&tampered, &provider);
        let mut tampered = document.clone();
        *tampered.wrapped_dek_nonce.first_mut().expect("DEK nonce") ^= 1;
        assert_spec_open_failed(&tampered, &provider);

        let mut invalid = document.clone();
        invalid.algorithm = "unsupported".to_string();
        assert_unsupported_metadata(&invalid, &provider);
        let mut invalid = document;
        invalid.aad_version += 1;
        assert_unsupported_metadata(&invalid, &provider);
    }

    #[test]
    fn identity_documents_reject_unknown_plaintext_versions() {
        let provider = static_provider(61);
        let values = secret_values();
        let spec = seal_test_document(
            &PlaintextIdentitySpecDocument {
                version: IDENTITY_DOCUMENT_VERSION + 1,
                values: values.clone(),
            },
            identity_spec_document_aad("workspace", "acme", "github"),
            &provider,
        );
        let error = decrypt_identity_spec_document("workspace", "acme", "github", &spec, &provider)
            .expect_err("unknown identity spec version must fail");
        assert!(
            error
                .to_string()
                .contains("identity spec document version 2")
        );

        let identity = seal_test_document(
            &PlaintextIdentityDocument {
                version: IDENTITY_DOCUMENT_VERSION + 1,
                values,
            },
            identity_document_aad("user", "member-1", "github"),
            &provider,
        );
        let error = decrypt_identity_document("user", "member-1", "github", &identity, &provider)
            .expect_err("unknown identity version must fail");
        assert!(error.to_string().contains("identity document version 2"));
    }

    #[test]
    fn identity_documents_rewrap_stale_keks_without_reencrypting_payloads() {
        let old_key = CredentialEncryptionKey::from_static_bytes_for_test([67; 32]);
        let new_key = CredentialEncryptionKey::from_static_bytes_for_test([71; 32]);
        let old_provider = RotatingKeyProvider {
            active: old_key.clone(),
            keys: vec![old_key.clone()],
        };
        let rotating_provider = RotatingKeyProvider {
            active: new_key.clone(),
            keys: vec![old_key, new_key.clone()],
        };
        let values = secret_values();
        let spec =
            encrypt_identity_spec_document("workspace", "acme", "github", &values, &old_provider)
                .expect("encrypt identity spec");
        let rewrapped_spec =
            rewrap_identity_spec_document("workspace", "acme", "github", &spec, &rotating_provider)
                .expect("rewrap identity spec")
                .expect("stale spec key must rewrap");
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
            encrypt_identity_document("user", "member-1", "github", &values, &old_provider)
                .expect("encrypt identity");
        let rewrapped_identity =
            rewrap_identity_document("user", "member-1", "github", &identity, &rotating_provider)
                .expect("rewrap identity")
                .expect("stale identity key must rewrap");
        assert_rewrapped_document(&identity, &rewrapped_identity, new_key.key_id());
        assert_eq!(
            decrypt_identity_document(
                "user",
                "member-1",
                "github",
                &rewrapped_identity,
                &rotating_provider,
            )
            .expect("decrypt rewrapped identity"),
            values
        );
        assert!(
            rewrap_identity_document(
                "user",
                "member-1",
                "github",
                &rewrapped_identity,
                &rotating_provider,
            )
            .expect("rewrap current identity")
            .is_none()
        );
        assert_open_failed(
            &rewrap_identity_spec_document(
                "workspace",
                "wrong",
                "github",
                &spec,
                &rotating_provider,
            )
            .expect_err("rewrap must authenticate identity fields"),
        );
    }

    fn static_provider(byte: u8) -> StaticKeyProvider {
        StaticKeyProvider {
            key: CredentialEncryptionKey::from_static_bytes_for_test([byte; 32]),
        }
    }

    fn secret_values() -> BTreeMap<String, String> {
        BTreeMap::from([("token".to_string(), "secret".to_string())])
    }

    fn seal_test_document(
        document: &impl serde::Serialize,
        aad: Vec<u8>,
        provider: &StaticKeyProvider,
    ) -> EncryptedEnvelopeDocument {
        seal_envelope_document(
            aad,
            serialize_identity_document(document).expect("serialize test document"),
            provider,
        )
        .expect("seal test document")
    }

    fn assert_spec_open_failed(document: &EncryptedEnvelopeDocument, provider: &StaticKeyProvider) {
        assert_open_failed(
            &decrypt_identity_spec_document("workspace", "acme", "github", document, provider)
                .expect_err("tampered document must fail"),
        );
    }

    fn assert_unsupported_metadata(
        document: &EncryptedEnvelopeDocument,
        provider: &StaticKeyProvider,
    ) {
        let error =
            decrypt_identity_spec_document("workspace", "acme", "github", document, provider)
                .expect_err("unsupported metadata must fail");
        assert!(error.to_string().contains("unsupported"));
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
