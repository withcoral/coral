//! Shared validation helpers for app-owned identifiers.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Identity-spec document crypto lands before manager consumers."
    )
)]

use std::collections::BTreeMap;
use std::fmt;

use zeroize::Zeroizing;

use crate::bootstrap::AppError;
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
struct DecryptedIdentitySpecDocument {
    version: u32,
    values: BTreeMap<String, String>,
}

/// Stable local principal used by single-user local mode.
pub(crate) const LOCAL_PRINCIPAL_ID: &str = "coral:local";

/// Stable, opaque identity shared by every principal kind and authority.
///
/// Providers must supply identifiers from one collision-free namespace. The
/// identifier deliberately does not expose whether the principal is a user,
/// agent, service, or another future actor kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PrincipalId(String);

impl PrincipalId {
    /// Parses a canonical principal identifier.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the identifier is empty or contains
    /// whitespace or control characters.
    pub fn parse(value: &str) -> Result<Self, AppError> {
        if value.is_empty()
            || value
                .chars()
                .any(|character| character.is_whitespace() || character.is_control())
        {
            return Err(AppError::InvalidInput(
                "principal id must be non-empty and contain no whitespace or control characters"
                    .to_string(),
            ));
        }
        if value == LOCAL_PRINCIPAL_ID {
            return Err(AppError::InvalidInput(format!(
                "principal id '{LOCAL_PRINCIPAL_ID}' is reserved for local mode"
            )));
        }
        Ok(Self(value.to_string()))
    }

    /// Returns the canonical principal identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PrincipalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Authenticated category of actor represented by a [`Principal`].
///
/// Kind is available to authorization policy, but does not itself grant a
/// permission or imply a role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrincipalKind {
    /// A human user.
    User,
    /// An autonomous or delegated agent.
    Agent,
}

/// Request-scoped principal selected by the app transport layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    id: PrincipalId,
    kind: PrincipalKind,
}

impl Principal {
    /// Builds a principal from its canonical identity and authenticated kind.
    #[must_use]
    pub const fn new(id: PrincipalId, kind: PrincipalKind) -> Self {
        Self { id, kind }
    }

    /// Parses and builds a principal with an authenticated kind.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when `id` is not a valid [`PrincipalId`].
    pub fn parse(id: &str, kind: PrincipalKind) -> Result<Self, AppError> {
        PrincipalId::parse(id).map(|id| Self::new(id, kind))
    }

    /// Builds the default local user principal.
    #[must_use]
    pub fn local() -> Self {
        Self {
            id: PrincipalId(LOCAL_PRINCIPAL_ID.to_string()),
            kind: PrincipalKind::User,
        }
    }

    /// Derives the stable Coral user identity for a federated session subject.
    ///
    /// The subject alone is the identity: `[auth.provider]` holds exactly one
    /// OIDC provider (not a list), so every subject Coral ever sees is issued
    /// by that provider and two subjects cannot collide. The `subject` argument
    /// is the raw upstream `sub` claim, with no issuer or provider prefix.
    ///
    /// The preimage is versioned because of that assumption: admitting a second
    /// provider would make the provider part of the identity, which needs a `-v2`
    /// preimage. Note what that costs — and the limit of the cost. The derivation
    /// is one-way and the upstream subject is persisted nowhere (only the
    /// short-lived in-memory authorization-code store holds it), but the subject
    /// is re-presented at every login, so a stored id is recomputable then. A
    /// version bump is therefore a lazy rekey — `UPDATE ... WHERE
    /// created_by_principal_id = <old id>` as each user next signs in — rather
    /// than dual-prefix acceptance or orphaned attribution rows. What it does
    /// cost is time: a user's rows carry the old id until they come back.
    ///
    /// The digest is stable and collision-free, but it is not opaque against a
    /// guesser: it is unkeyed, and subjects are low-entropy (emails, numeric
    /// provider ids), so anyone holding this value and a candidate list can confirm a
    /// match offline. Nothing deployment-specific enters the preimage either, so
    /// the same subject yields the same id everywhere — two databases join on it
    /// directly. That is accepted rather than fixed. The value reaches exactly one
    /// place, `created_by_principal_id` on `tasks`: it is in no proto, no log and
    /// no query attribution, so reading it takes database access, and it authorizes
    /// nothing, since any valid session token already grants full access to the
    /// instance and every source in it.
    ///
    /// It is also a placeholder. Managing authorization needs a users table — a
    /// role cannot be granted to someone the instance can neither enumerate nor
    /// show an admin — and a one-way digest is structurally opposed to that. When
    /// that table lands the identity becomes a random surrogate key with the
    /// provider's `sub` in its own column: opaque by construction rather than by
    /// keeping a key secret, updatable when an upstream subject changes, and
    /// enumerable, so users can be listed and deleted at all. Getting there is the
    /// same lazy rekey described above. Do not key this digest in the meantime —
    /// that swaps one derivation for another and pays that rekey twice.
    pub(crate) fn for_federated(subject: &str) -> Self {
        let mut identity = Vec::with_capacity(subject.len() + 32);
        identity.extend_from_slice(b"coral-federated-user-v1\0");
        identity.extend_from_slice(&(subject.len() as u64).to_be_bytes());
        identity.extend_from_slice(subject.as_bytes());
        Self {
            id: PrincipalId(format!("federated-{}", crate::hash::sha256_hex(&identity))),
            kind: PrincipalKind::User,
        }
    }

    /// Returns the stable principal identity.
    #[must_use]
    pub const fn id(&self) -> &PrincipalId {
        &self.id
    }

    /// Returns the authenticated actor kind.
    #[must_use]
    pub const fn kind(&self) -> PrincipalKind {
        self.kind
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrincipalProviderErrorKind {
    Unauthenticated,
    Unavailable,
    Internal,
}

/// Client-safe failure reported by a request principal provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrincipalProviderError {
    kind: PrincipalProviderErrorKind,
    client_message: String,
}

impl PrincipalProviderError {
    fn new(
        kind: PrincipalProviderErrorKind,
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
            PrincipalProviderErrorKind::Unauthenticated,
            client_message,
            "unauthenticated request",
        )
    }

    /// Builds a transient provider failure with a client-safe message.
    #[must_use]
    pub fn unavailable(client_message: impl Into<String>) -> Self {
        Self::new(
            PrincipalProviderErrorKind::Unavailable,
            client_message,
            "principal provider unavailable",
        )
    }

    /// Builds an unexpected provider failure with a client-safe message.
    #[must_use]
    pub fn internal(client_message: impl Into<String>) -> Self {
        Self::new(
            PrincipalProviderErrorKind::Internal,
            client_message,
            "principal provider failed",
        )
    }

    pub(crate) fn kind(&self) -> PrincipalProviderErrorKind {
        self.kind
    }

    /// Returns the client-safe failure message.
    #[must_use]
    pub fn client_message(&self) -> &str {
        &self.client_message
    }
}

impl fmt::Display for PrincipalProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.client_message)
    }
}

impl std::error::Error for PrincipalProviderError {}

/// Server-side provider for request principals.
///
/// The OSS provider always returns [`Principal::local`]. Product runtimes can
/// install a provider that authenticates inbound metadata and returns the
/// corresponding stable principal identity and actor kind. A provider must
/// classify a given [`PrincipalId`] consistently across requests.
#[tonic::async_trait]
pub trait PrincipalProvider: Send + Sync + std::fmt::Debug {
    /// Returns the principal for one inbound gRPC request.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalProviderError`] when transport metadata is
    /// malformed, the provider cannot authenticate the request, or principal
    /// selection fails.
    async fn principal_for_metadata(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<Principal, PrincipalProviderError>;
}

/// Server-side authenticator for a bearer token held outside gRPC metadata.
///
/// A served surface that already parsed the token out of its own transport —
/// the MCP HTTP `Authorization` header, say — authenticates it here instead of
/// re-encoding a gRPC [`tonic::metadata::MetadataMap`] for
/// [`PrincipalProvider`] to take apart again. The two entry points must accept
/// the same tokens, so an implementation is expected to share one verification
/// path between them.
#[tonic::async_trait]
pub trait BearerAuthenticator: Send + Sync + std::fmt::Debug {
    /// Returns the principal a bare bearer token authenticates.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalProviderError`] when the token is malformed, fails
    /// verification, or principal selection fails.
    async fn principal_for_bearer(&self, token: &str) -> Result<Principal, PrincipalProviderError>;
}

/// Default OSS principal provider for local mode.
#[derive(Debug, Default)]
pub struct LocalPrincipalProvider;

#[tonic::async_trait]
impl PrincipalProvider for LocalPrincipalProvider {
    async fn principal_for_metadata(
        &self,
        _metadata: &tonic::metadata::MetadataMap,
    ) -> Result<Principal, PrincipalProviderError> {
        Ok(Principal::local())
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
    use super::{
        LOCAL_PRINCIPAL_ID, LocalPrincipalProvider, Principal, PrincipalId, PrincipalKind,
        PrincipalProvider, parse_path_segment,
    };

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
    fn principal_id_rejects_empty_whitespace_and_control_characters() {
        for invalid in [
            "",
            " saul",
            "saul ",
            "alice bob",
            "alice\tbob",
            "alice\nbob",
            "alice\0bob",
        ] {
            let error = PrincipalId::parse(invalid).expect_err("invalid principal id");

            assert!(error.to_string().contains("principal id must be non-empty"));
        }
    }

    #[test]
    fn principal_id_rejects_reserved_local_identity() {
        PrincipalId::parse(LOCAL_PRINCIPAL_ID).expect_err("local identity must stay app-owned");
    }

    #[test]
    fn principal_preserves_canonical_opaque_id_and_explicit_kind() {
        let id = PrincipalId::parse("product:principal/saul").expect("valid principal id");
        let principal = Principal::new(id.clone(), PrincipalKind::Agent);

        assert_eq!(principal.id(), &id);
        assert_eq!(principal.id().as_str(), "product:principal/saul");
        assert_eq!(principal.kind(), PrincipalKind::Agent);
    }

    #[test]
    fn federated_principal_is_stable_and_namespaces_subject() {
        let principal = Principal::for_federated("alice");
        assert_eq!(principal, Principal::for_federated("alice"));
        assert_ne!(principal, Principal::for_federated("bob"));
        PrincipalId::parse(principal.id().as_str()).expect("generated id is canonical");
        assert_eq!(principal.kind(), PrincipalKind::User);
    }

    #[tokio::test]
    async fn local_provider_returns_local_principal() {
        let principal = LocalPrincipalProvider
            .principal_for_metadata(&tonic::metadata::MetadataMap::new())
            .await
            .expect("local principal");

        assert_eq!(principal, Principal::local());
        assert_eq!(principal.kind(), PrincipalKind::User);
    }
}

#[cfg(test)]
mod spec_document_crypto_tests {
    use std::collections::BTreeMap;

    use zeroize::Zeroizing;

    use super::{
        IDENTITY_SPEC_DOCUMENT_ALGORITHM, IDENTITY_SPEC_DOCUMENT_BINDING_VERSION,
        decrypt_identity_spec_document, encrypt_identity_spec_document,
        identity_spec_document_context, rewrap_identity_spec_document,
    };
    use crate::credentials::CredentialsError;
    use crate::credentials::encryption::{
        CredentialEncryptionKey, CredentialKeyProvider, EnvelopeContext, decrypt_credential_values,
        seal_envelope_document,
    };
    use crate::encrypted_document::EncryptedEnvelopeDocument;
    use crate::sources::SourceName;
    use crate::state::db::IdentitySpecKey;
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
