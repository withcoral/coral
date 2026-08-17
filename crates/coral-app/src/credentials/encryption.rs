//! Envelope encryption uses one key for credential data and another for that key.
//!
//! # Terminology
//! - AEAD means authenticated encryption with associated data; the AEAD used here,
//!   AES-256-GCM, is Advanced Encryption Standard with a 256-bit key in Galois/Counter Mode.
//! - A DEK (data-encryption key) encrypts one document; a longer-lived KEK
//!   (key-encryption key) wraps it. Rewrapping encrypts it with a replacement KEK.
//! - AAD (additional authenticated data) is unencrypted but authenticated context,
//!   binding documents to workspace/source and wrapped DEKs to KEK identifiers.
//! - A nonce ("number used once") is a public, per-key unique encryption input.
//! - SHA-256 (Secure Hash Algorithm, 256-bit) derives non-secret KEK identifiers;
//!   it does not encrypt data.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Credential DB runtime wiring and identity document callers land in later stack branches; this branch isolates cryptographic primitives for review."
    )
)]

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Mutex;

use async_trait::async_trait;
use base64::Engine as _;
use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom as _, SystemRandom};
use sha2::{Digest as _, Sha256};
use tracing::warn;
use zeroize::{Zeroize as _, Zeroizing};

use super::CredentialsError;
use crate::encrypted_document::EncryptedEnvelopeDocument;
use crate::sources::SourceName;
use crate::state::AppStateLayout;
use crate::storage::fs as storage_fs;
use crate::storage::fs::FileLock;
use crate::workspaces::WorkspaceName;

pub(crate) const ENVELOPE_DOCUMENT_ALGORITHM: &str = "AES-256-GCM";
pub(crate) const CREDENTIAL_DOCUMENT_BINDING_VERSION: i64 = 2;

const CREDENTIAL_DOCUMENT_VERSION: u32 = 1;
const LEGACY_CREDENTIAL_BINDING_VERSION: i64 = 1;
const KEY_FILE_VERSION: &str = "v1";
const KEY_FILE_MAX_BYTES: u64 = 4 * 1024;
const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;

static LOCAL_KEY_FILE_LOCK: Mutex<()> = Mutex::new(());

pub(crate) type EncryptedCredentialDocument = EncryptedEnvelopeDocument;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct EnvelopeEncryptionKey {
    key_id: String,
    bytes: [u8; KEY_LEN],
}

impl fmt::Debug for EnvelopeEncryptionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvelopeEncryptionKey")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

impl Drop for EnvelopeEncryptionKey {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

impl EnvelopeEncryptionKey {
    pub(crate) fn from_encoded_material(raw: &str) -> Result<Self, CredentialsError> {
        let trimmed = raw.trim();
        let Some(encoded) = trimmed.strip_prefix(&format!("{KEY_FILE_VERSION}:")) else {
            return Err(CredentialsError::Crypto(
                "unsupported credential encryption key version".to_string(),
            ));
        };
        let decoded = Zeroizing::new(
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|error| {
                    CredentialsError::Crypto(format!("invalid encryption key: {error}"))
                })?,
        );
        if decoded.len() != KEY_LEN {
            return Err(CredentialsError::Crypto(format!(
                "credential encryption key has invalid length {}",
                decoded.len()
            )));
        }
        let mut bytes = [0_u8; KEY_LEN];
        bytes.copy_from_slice(decoded.as_slice());
        Ok(Self {
            key_id: key_id_for_bytes(&bytes),
            bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn from_static_bytes_for_test(bytes: [u8; KEY_LEN]) -> Self {
        Self {
            key_id: key_id_for_bytes(&bytes),
            bytes,
        }
    }

    pub(crate) fn key_id(&self) -> &str {
        self.key_id.as_str()
    }
}

/// Resolves KEKs, and mints one when a deployment has none yet.
///
/// Async because resolving a key is I/O: [`LocalFileEnvelopeKeyProvider`] reads
/// durable key material from disk. Every envelope operation takes already-resolved
/// [`EnvelopeEncryptionKey`] values instead of a provider, so key resolution stays
/// here — awaited once by the caller — and the crypto itself stays synchronous.
///
/// `active_key` is additionally a minting capability: it creates key material when a
/// deployment has none. Only sealing and rewrapping need it, so a read path that
/// resolves by `key_id` can neither create nor rotate key material.
#[async_trait]
pub(crate) trait EnvelopeKeyProvider: Send + Sync {
    async fn active_key(&self) -> Result<EnvelopeEncryptionKey, CredentialsError>;

    async fn key(&self, key_id: &str) -> Result<EnvelopeEncryptionKey, CredentialsError>;
}

/// Versioned authenticated context that binds an encrypted envelope to its owner.
pub(crate) struct EnvelopeContext {
    binding_version: i64,
    encoded_aad: Vec<u8>,
}

impl EnvelopeContext {
    /// Build canonical AAD with the binding version first and algorithm last.
    pub(crate) fn new(
        domain: &str,
        binding_version: i64,
        fields: &[&str],
    ) -> Result<Self, CredentialsError> {
        if binding_version < 1 {
            return Err(CredentialsError::Crypto(
                "envelope binding version must be positive".to_string(),
            ));
        }
        if domain.contains('\0') {
            return Err(CredentialsError::Crypto(
                "envelope domain must not contain NUL".to_string(),
            ));
        }
        let binding_version_text = binding_version.to_string();
        let mut versioned_fields = Vec::with_capacity(fields.len() + 2);
        versioned_fields.push(binding_version_text.as_str());
        versioned_fields.extend_from_slice(fields);
        versioned_fields.push(ENVELOPE_DOCUMENT_ALGORITHM);
        Ok(Self {
            binding_version,
            encoded_aad: encode_aad_fields(domain, &versioned_fields),
        })
    }

    /// Bind a wrapped DEK to the same owner context as its encrypted payload.
    pub(crate) fn dek_aad(&self, key_id: &str) -> Vec<u8> {
        let mut aad = self.encoded_aad.clone();
        encode_aad_field(&mut aad, key_id);
        aad
    }
}

<<<<<<< HEAD
/// Resolves the envelope key from a private file scoped to this app-state directory.
||||||| parent of 1b6c0a465 (refactor(app): make envelope key resolution async)
/// Immutable configured encryption keys resolved during app bootstrap.
#[derive(Debug, Clone)]
pub(crate) struct ConfiguredEnvelopeKeyProvider {
    active_key_id: Option<String>,
    keys_by_id: BTreeMap<String, EnvelopeEncryptionKey>,
}

impl ConfiguredEnvelopeKeyProvider {
    pub(crate) fn new(
        active_key: EnvelopeEncryptionKey,
        decryption_keys: impl IntoIterator<Item = EnvelopeEncryptionKey>,
    ) -> Result<Self, CredentialsError> {
        let active_key_id = active_key.key_id().to_string();
        let mut keys_by_id = BTreeMap::from([(active_key_id.clone(), active_key)]);
        for key in decryption_keys {
            let key_id = key.key_id().to_string();
            if keys_by_id.contains_key(&key_id) {
                return Err(CredentialsError::Parse(format!(
                    "duplicate credential encryption key id '{key_id}'"
                )));
            }
            keys_by_id.insert(key_id, key);
        }
        Ok(Self {
            active_key_id: Some(active_key_id),
            keys_by_id,
        })
    }

    pub(crate) fn unavailable() -> Self {
        Self {
            active_key_id: None,
            keys_by_id: BTreeMap::new(),
        }
    }

    fn single(active_key: EnvelopeEncryptionKey) -> Self {
        let active_key_id = active_key.key_id().to_string();
        Self {
            active_key_id: Some(active_key_id.clone()),
            keys_by_id: BTreeMap::from([(active_key_id, active_key)]),
        }
    }

    fn key_if_present(&self, key_id: &str) -> Option<EnvelopeEncryptionKey> {
        self.keys_by_id.get(key_id).cloned()
    }
}

impl EnvelopeKeyProvider for ConfiguredEnvelopeKeyProvider {
    fn active_key(&self) -> Result<EnvelopeEncryptionKey, CredentialsError> {
        self.active_key_id
            .as_deref()
            .and_then(|key_id| self.key_if_present(key_id))
            .ok_or_else(configured_key_required)
    }

    fn key(&self, key_id: &str) -> Result<EnvelopeEncryptionKey, CredentialsError> {
        self.key_if_present(key_id).ok_or_else(|| {
            CredentialsError::Unavailable(format!(
                "credential encryption key '{key_id}' is unavailable"
            ))
        })
    }
}

fn configured_key_required() -> CredentialsError {
    CredentialsError::Unavailable(
        "encrypted identity inputs require a configured credential encryption key".to_string(),
    )
}

/// Resolves an explicitly supplied key or falls back to a key file scoped to
/// this app-state config directory. Callers own config and environment resolution.
=======
/// Immutable configured encryption keys resolved during app bootstrap.
#[derive(Debug, Clone)]
pub(crate) struct ConfiguredEnvelopeKeyProvider {
    active_key_id: Option<String>,
    keys_by_id: BTreeMap<String, EnvelopeEncryptionKey>,
}

impl ConfiguredEnvelopeKeyProvider {
    pub(crate) fn new(
        active_key: EnvelopeEncryptionKey,
        decryption_keys: impl IntoIterator<Item = EnvelopeEncryptionKey>,
    ) -> Result<Self, CredentialsError> {
        let active_key_id = active_key.key_id().to_string();
        let mut keys_by_id = BTreeMap::from([(active_key_id.clone(), active_key)]);
        for key in decryption_keys {
            let key_id = key.key_id().to_string();
            if keys_by_id.contains_key(&key_id) {
                return Err(CredentialsError::Parse(format!(
                    "duplicate credential encryption key id '{key_id}'"
                )));
            }
            keys_by_id.insert(key_id, key);
        }
        Ok(Self {
            active_key_id: Some(active_key_id),
            keys_by_id,
        })
    }

    pub(crate) fn unavailable() -> Self {
        Self {
            active_key_id: None,
            keys_by_id: BTreeMap::new(),
        }
    }

    fn single(active_key: EnvelopeEncryptionKey) -> Self {
        let active_key_id = active_key.key_id().to_string();
        Self {
            active_key_id: Some(active_key_id.clone()),
            keys_by_id: BTreeMap::from([(active_key_id, active_key)]),
        }
    }

    fn key_if_present(&self, key_id: &str) -> Option<EnvelopeEncryptionKey> {
        self.keys_by_id.get(key_id).cloned()
    }
}

#[async_trait]
impl EnvelopeKeyProvider for ConfiguredEnvelopeKeyProvider {
    async fn active_key(&self) -> Result<EnvelopeEncryptionKey, CredentialsError> {
        self.active_key_id
            .as_deref()
            .and_then(|key_id| self.key_if_present(key_id))
            .ok_or_else(configured_key_required)
    }

    async fn key(&self, key_id: &str) -> Result<EnvelopeEncryptionKey, CredentialsError> {
        self.key_if_present(key_id).ok_or_else(|| {
            CredentialsError::Unavailable(format!(
                "credential encryption key '{key_id}' is unavailable"
            ))
        })
    }
}

fn configured_key_required() -> CredentialsError {
    CredentialsError::Unavailable(
        "encrypted identity inputs require a configured credential encryption key".to_string(),
    )
}

/// Resolves an explicitly supplied key or falls back to a key file scoped to
/// this app-state config directory. Callers own config and environment resolution.
>>>>>>> 1b6c0a465 (refactor(app): make envelope key resolution async)
#[derive(Debug, Clone)]
pub(crate) struct LocalFileEnvelopeKeyProvider {
    path: PathBuf,
}

impl LocalFileEnvelopeKeyProvider {
    pub(crate) fn new(layout: &AppStateLayout) -> Self {
        Self {
            path: layout.envelope_encryption_key_file(),
        }
    }

    fn load_key(&self) -> Result<Option<EnvelopeEncryptionKey>, CredentialsError> {
        match std::fs::symlink_metadata(&self.path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        let raw = Zeroizing::new(storage_fs::read_to_string_private(
            &self.path,
            KEY_FILE_MAX_BYTES,
        )?);
        EnvelopeEncryptionKey::from_encoded_material(raw.as_str()).map(Some)
    }

    fn load_or_create_key(&self) -> Result<EnvelopeEncryptionKey, CredentialsError> {
        let _thread_guard = LOCAL_KEY_FILE_LOCK.lock().map_err(|_error| {
            CredentialsError::Crypto("credential encryption key lock is poisoned".to_string())
        })?;
        if let Some(parent) = self.path.parent() {
            storage_fs::ensure_private_dir_no_symlink(parent)?;
        }
        let lock_path = self.path.with_extension("key.lock");
        let _process_guard = FileLock::exclusive(&lock_path)?;

        if let Some(key) = self.load_key()? {
            return Ok(key);
        }

        let bytes = Zeroizing::new(random_array::<KEY_LEN>()?);
        let mut encoded = Zeroizing::new(format!("{KEY_FILE_VERSION}:"));
        base64::engine::general_purpose::STANDARD.encode_string(bytes.as_slice(), &mut encoded);
        encoded.push('\n');
        storage_fs::write_atomic(&self.path, encoded.as_bytes())?;
        warn!(
            path = %self.path.display(),
            "created local credential encryption key"
        );
        Ok(EnvelopeEncryptionKey {
            key_id: key_id_for_bytes(&bytes),
            bytes: *bytes,
        })
    }
}

/// Runs one key-file operation off the async runtime.
///
/// The file helpers this provider relies on are deliberately blocking: they take a
/// process-wide flock and perform symlink and permission checks that must not be
/// split across an await. Containing that here keeps the blocking inside the
/// implementation instead of pushing it onto every caller.
async fn run_key_file_operation<T, F>(operation: F) -> Result<T, CredentialsError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, CredentialsError> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| {
            CredentialsError::Crypto(format!("credential key file task failed: {error}"))
        })?
}

#[async_trait]
impl EnvelopeKeyProvider for LocalFileEnvelopeKeyProvider {
    async fn active_key(&self) -> Result<EnvelopeEncryptionKey, CredentialsError> {
<<<<<<< HEAD
||||||| parent of 1b6c0a465 (refactor(app): make envelope key resolution async)
    fn active_key(&self) -> Result<EnvelopeEncryptionKey, CredentialsError> {
        if let Some(keys) = &self.configured_keys {
            return keys.active_key();
        }
        self.load_or_create_key()
=======
        if let Some(keys) = &self.configured_keys {
            return keys.active_key().await;
        }
>>>>>>> 1b6c0a465 (refactor(app): make envelope key resolution async)
        let provider = self.clone();
        run_key_file_operation(move || provider.load_or_create_key()).await
    }

    async fn key(&self, key_id: &str) -> Result<EnvelopeEncryptionKey, CredentialsError> {
<<<<<<< HEAD
||||||| parent of 1b6c0a465 (refactor(app): make envelope key resolution async)
    fn key(&self, key_id: &str) -> Result<EnvelopeEncryptionKey, CredentialsError> {
        if let Some(key) = self
            .configured_keys
            .as_ref()
            .and_then(|keys| keys.key_if_present(key_id))
        {
            return Ok(key);
        }
        if let Some(key) = self.load_key()?
            && key.key_id == key_id
        {
            return Ok(key);
        }
        Err(CredentialsError::Unavailable(format!(
            "credential encryption key '{key_id}' is unavailable"
        )))
=======
        if let Some(key) = self
            .configured_keys
            .as_ref()
            .and_then(|keys| keys.key_if_present(key_id))
        {
            return Ok(key);
        }
>>>>>>> 1b6c0a465 (refactor(app): make envelope key resolution async)
        let provider = self.clone();
        let key_id = key_id.to_string();
        run_key_file_operation(move || {
            if let Some(key) = provider.load_key()?
                && key.key_id == key_id
            {
                return Ok(key);
            }
            Err(CredentialsError::Unavailable(format!(
                "credential encryption key '{key_id}' is unavailable"
            )))
        })
        .await
    }
}

#[derive(serde::Serialize)]
struct PlaintextCredentialDocument<'a> {
    version: u32,
    values: &'a BTreeMap<String, String>,
}

#[derive(serde::Deserialize)]
struct DecryptedCredentialDocument {
    version: u32,
    values: BTreeMap<String, String>,
}

pub(crate) fn encrypt_credential_values(
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    values: &BTreeMap<String, String>,
    active_kek: &EnvelopeEncryptionKey,
) -> Result<EncryptedCredentialDocument, CredentialsError> {
    let plaintext = PlaintextCredentialDocument {
        version: CREDENTIAL_DOCUMENT_VERSION,
        values,
    };
    let document_bytes = Zeroizing::new(
        serde_json::to_vec(&plaintext)
            .map_err(|error| CredentialsError::Parse(error.to_string()))?,
    );
    let context = credential_document_context(
        CREDENTIAL_DOCUMENT_BINDING_VERSION,
        workspace_name,
        source_name,
    )?;
    seal_envelope_document(&context, document_bytes, active_kek)
}

pub(crate) fn decrypt_credential_values(
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    document: &EncryptedCredentialDocument,
    kek: &EnvelopeEncryptionKey,
) -> Result<BTreeMap<String, String>, CredentialsError> {
    let plaintext = decrypt_credential_document_bytes(workspace_name, source_name, document, kek)?;
    let decoded: DecryptedCredentialDocument = serde_json::from_slice(&plaintext)
        .map_err(|error| CredentialsError::Parse(error.to_string()))?;
    if decoded.version != CREDENTIAL_DOCUMENT_VERSION {
        return Err(CredentialsError::Parse(format!(
            "unsupported credential document version {}",
            decoded.version
        )));
    }
    Ok(decoded.values)
}

pub(crate) fn rewrap_credential_document(
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    document: &EncryptedCredentialDocument,
    old_kek: &EnvelopeEncryptionKey,
    active_kek: &EnvelopeEncryptionKey,
) -> Result<Option<EncryptedCredentialDocument>, CredentialsError> {
    let context =
        credential_document_context(document.binding_version, workspace_name, source_name)?;
    if document.binding_version == LEGACY_CREDENTIAL_BINDING_VERSION {
        // Binding version 1 includes both historical credential AAD encodings.
        // Authenticate either shape, then reseal once into the exact v2 recipe.
        let values = decrypt_credential_values(workspace_name, source_name, document, old_kek)?;
        return encrypt_credential_values(workspace_name, source_name, &values, active_kek)
            .map(Some);
    }
    rewrap_envelope_document(&context, document, old_kek, active_kek)
}

fn decrypt_credential_document_bytes(
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    document: &EncryptedCredentialDocument,
    kek: &EnvelopeEncryptionKey,
) -> Result<Zeroizing<Vec<u8>>, CredentialsError> {
    let context =
        credential_document_context(document.binding_version, workspace_name, source_name)?;
    validate_document_metadata(document, &context)?;
    validate_unwrapping_key(document, kek)?;
    let dek = unwrap_credential_dek(document, kek, &context)?;

    let mut ciphertext = Zeroizing::new(document.ciphertext.clone());
    match open(
        &*dek,
        document.nonce.as_slice(),
        &context.encoded_aad,
        ciphertext.as_mut_slice(),
    ) {
        Ok(plaintext) => Ok(Zeroizing::new(plaintext.to_vec())),
        Err(primary_error) if document.binding_version == LEGACY_CREDENTIAL_BINDING_VERSION => {
            let mut legacy_ciphertext = Zeroizing::new(document.ciphertext.clone());
            match open(
                &*dek,
                document.nonce.as_slice(),
                &legacy_credential_document_aad(workspace_name, source_name, &document.key_id),
                legacy_ciphertext.as_mut_slice(),
            ) {
                Ok(plaintext) => Ok(Zeroizing::new(plaintext.to_vec())),
                Err(_) => Err(primary_error),
            }
        }
        Err(error) => Err(error),
    }
}

fn unwrap_credential_dek(
    document: &EncryptedCredentialDocument,
    kek: &EnvelopeEncryptionKey,
    context: &EnvelopeContext,
) -> Result<Zeroizing<[u8; KEY_LEN]>, CredentialsError> {
    match unwrap_current_dek(document, kek, context) {
        Ok(dek) => Ok(dek),
        Err(primary_error) if document.binding_version == LEGACY_CREDENTIAL_BINDING_VERSION => {
            match unwrap_dek_with_aad(
                document,
                kek,
                &legacy_length_prefixed_credential_dek_aad(&document.key_id),
            ) {
                Ok(dek) => Ok(dek),
                Err(_) => {
                    unwrap_dek_with_aad(document, kek, &legacy_credential_dek_aad(&document.key_id))
                        .map_err(|_legacy_error| primary_error)
                }
            }
        }
        Err(error) => Err(error),
    }
}

fn unwrap_current_dek(
    document: &EncryptedEnvelopeDocument,
    kek: &EnvelopeEncryptionKey,
    context: &EnvelopeContext,
) -> Result<Zeroizing<[u8; KEY_LEN]>, CredentialsError> {
    unwrap_dek_with_aad(document, kek, &context.dek_aad(&document.key_id))
}

fn unwrap_dek_with_aad(
    document: &EncryptedEnvelopeDocument,
    kek: &EnvelopeEncryptionKey,
    aad: &[u8],
) -> Result<Zeroizing<[u8; KEY_LEN]>, CredentialsError> {
    let mut dek = Zeroizing::new(document.wrapped_dek.clone());
    open(
        &kek.bytes,
        document.wrapped_dek_nonce.as_slice(),
        aad,
        dek.as_mut_slice(),
    )
    .and_then(validate_dek_plaintext)
}

fn validate_dek_plaintext(
    dek_plaintext: &[u8],
) -> Result<Zeroizing<[u8; KEY_LEN]>, CredentialsError> {
    if dek_plaintext.len() != KEY_LEN {
        return Err(CredentialsError::Crypto(format!(
            "credential document DEK has invalid length {}",
            dek_plaintext.len()
        )));
    }
    let mut dek = [0_u8; KEY_LEN];
    dek.copy_from_slice(dek_plaintext);
    Ok(Zeroizing::new(dek))
}

fn rewrap_dek(
    document: &EncryptedEnvelopeDocument,
    active_kek: &EnvelopeEncryptionKey,
    dek: &[u8; KEY_LEN],
    context: &EnvelopeContext,
) -> Result<EncryptedEnvelopeDocument, CredentialsError> {
    let wrapped_dek_nonce = random_array::<NONCE_LEN>()?;
    let mut wrapped_dek = Zeroizing::new(dek.to_vec());
    seal(
        &active_kek.bytes,
        &wrapped_dek_nonce,
        &context.dek_aad(active_kek.key_id()),
        &mut wrapped_dek,
    )?;

    EncryptedEnvelopeDocument::new(
        document.ciphertext.clone(),
        document.nonce.clone(),
        std::mem::take(&mut *wrapped_dek),
        wrapped_dek_nonce.to_vec(),
        active_kek.key_id.clone(),
        document.algorithm.clone(),
        context.binding_version,
    )
    .map_err(|error| CredentialsError::Crypto(error.to_string()))
}

fn validate_document_metadata(
    document: &EncryptedEnvelopeDocument,
    context: &EnvelopeContext,
) -> Result<(), CredentialsError> {
    document
        .validate()
        .map_err(|error| CredentialsError::Crypto(error.to_string()))?;
    if document.algorithm != ENVELOPE_DOCUMENT_ALGORITHM {
        return Err(CredentialsError::Crypto(format!(
            "unsupported envelope encryption algorithm '{}'",
            document.algorithm
        )));
    }
    if document.binding_version != context.binding_version {
        return Err(CredentialsError::Crypto(format!(
            "envelope binding version {} does not match context version {}",
            document.binding_version, context.binding_version
        )));
    }
    Ok(())
}

/// Reject a KEK that is not the one the stored document names.
///
/// The wrapped DEK already authenticates `key_id` through its AAD, so a mismatched
/// KEK cannot unwrap it. Checking first turns that into a precise error instead of a
/// bare AEAD failure. Key identifiers are non-secret digests, so naming both is safe.
fn validate_unwrapping_key(
    document: &EncryptedEnvelopeDocument,
    kek: &EnvelopeEncryptionKey,
) -> Result<(), CredentialsError> {
    if kek.key_id() != document.key_id {
        return Err(CredentialsError::Crypto(format!(
            "envelope key '{}' does not match document key '{}'",
            kek.key_id(),
            document.key_id
        )));
    }
    Ok(())
}

/// Seal serialized plaintext with a random DEK and wrap that DEK with the active KEK.
pub(crate) fn seal_envelope_document(
    context: &EnvelopeContext,
    mut document_bytes: Zeroizing<Vec<u8>>,
    kek: &EnvelopeEncryptionKey,
) -> Result<EncryptedEnvelopeDocument, CredentialsError> {
    let dek = Zeroizing::new(random_array::<KEY_LEN>()?);
    let nonce = random_array::<NONCE_LEN>()?;
    let wrapped_dek_nonce = random_array::<NONCE_LEN>()?;

    seal(&*dek, &nonce, &context.encoded_aad, &mut document_bytes)?;

    let mut wrapped_dek = Zeroizing::new(dek.to_vec());
    seal(
        &kek.bytes,
        &wrapped_dek_nonce,
        &context.dek_aad(kek.key_id()),
        &mut wrapped_dek,
    )?;

    EncryptedEnvelopeDocument::new(
        std::mem::take(&mut *document_bytes),
        nonce.to_vec(),
        std::mem::take(&mut *wrapped_dek),
        wrapped_dek_nonce.to_vec(),
        kek.key_id.clone(),
        ENVELOPE_DOCUMENT_ALGORITHM,
        context.binding_version,
    )
    .map_err(|error| CredentialsError::Crypto(error.to_string()))
}

/// Open an envelope document when its persisted and expected bindings match.
///
/// Takes the KEK the document names rather than a [`EnvelopeKeyProvider`]: opening
/// needs no authority to mint or rotate keys, and resolving the KEK in the caller
/// keeps provider failures distinguishable from authentication failures.
pub(crate) fn open_envelope_document(
    context: &EnvelopeContext,
    document: &EncryptedEnvelopeDocument,
    kek: &EnvelopeEncryptionKey,
) -> Result<Zeroizing<Vec<u8>>, CredentialsError> {
    validate_document_metadata(document, context)?;
    validate_unwrapping_key(document, kek)?;
    let dek = unwrap_current_dek(document, kek, context)?;

    let mut ciphertext = Zeroizing::new(document.ciphertext.clone());
    open(
        &*dek,
        document.nonce.as_slice(),
        &context.encoded_aad,
        ciphertext.as_mut_slice(),
    )
    .map(|plaintext| Zeroizing::new(plaintext.to_vec()))
}

/// Authenticate and rewrap an envelope document when its KEK is stale.
pub(crate) fn rewrap_envelope_document(
    context: &EnvelopeContext,
    document: &EncryptedEnvelopeDocument,
    old_kek: &EnvelopeEncryptionKey,
    active_kek: &EnvelopeEncryptionKey,
) -> Result<Option<EncryptedEnvelopeDocument>, CredentialsError> {
    validate_document_metadata(document, context)?;
    validate_unwrapping_key(document, old_kek)?;
    let dek = unwrap_current_dek(document, old_kek, context)?;
    let mut document_probe = Zeroizing::new(document.ciphertext.clone());
    open(
        &*dek,
        document.nonce.as_slice(),
        &context.encoded_aad,
        document_probe.as_mut_slice(),
    )?;
    if old_kek.key_id == active_kek.key_id {
        return Ok(None);
    }

    rewrap_dek(document, active_kek, &dek, context).map(Some)
}

fn seal(
    key_bytes: &[u8],
    nonce_bytes: &[u8; NONCE_LEN],
    aad: &[u8],
    in_out: &mut Vec<u8>,
) -> Result<(), CredentialsError> {
    let key = LessSafeKey::new(
        UnboundKey::new(&aead::AES_256_GCM, key_bytes)
            .map_err(|_error| CredentialsError::Crypto("invalid AES-256-GCM key".to_string()))?,
    );
    key.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(*nonce_bytes),
        Aad::from(aad),
        in_out,
    )
    .map_err(|_error| CredentialsError::Crypto("AES-256-GCM seal failed".to_string()))
}

fn open<'a>(
    key_bytes: &[u8],
    nonce_bytes: &[u8],
    aad: &[u8],
    in_out: &'a mut [u8],
) -> Result<&'a [u8], CredentialsError> {
    let nonce = nonce_bytes.try_into().map_err(|_error| {
        CredentialsError::Crypto("invalid AES-256-GCM nonce length".to_string())
    })?;
    let key = LessSafeKey::new(
        UnboundKey::new(&aead::AES_256_GCM, key_bytes)
            .map_err(|_error| CredentialsError::Crypto("invalid AES-256-GCM key".to_string()))?,
    );
    key.open_in_place(Nonce::assume_unique_for_key(nonce), Aad::from(aad), in_out)
        .map(|plaintext| &*plaintext)
        .map_err(|_error| CredentialsError::Crypto("AES-256-GCM open failed".to_string()))
}

fn credential_document_context(
    binding_version: i64,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
) -> Result<EnvelopeContext, CredentialsError> {
    match binding_version {
        LEGACY_CREDENTIAL_BINDING_VERSION | CREDENTIAL_DOCUMENT_BINDING_VERSION => {}
        unsupported => {
            return Err(CredentialsError::Crypto(format!(
                "unsupported credential binding version {unsupported}"
            )));
        }
    }
    EnvelopeContext::new(
        "coral-credential-document",
        binding_version,
        &[workspace_name.as_str(), source_name.as_str()],
    )
}

fn legacy_credential_document_aad(
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    key_id: &str,
) -> Vec<u8> {
    format!(
        "coral-credential-document:v{}:{}:{}:{}:{}",
        LEGACY_CREDENTIAL_BINDING_VERSION,
        workspace_name.as_str(),
        source_name.as_str(),
        ENVELOPE_DOCUMENT_ALGORITHM,
        key_id
    )
    .into_bytes()
}

fn legacy_length_prefixed_credential_dek_aad(key_id: &str) -> Vec<u8> {
    let binding_version = LEGACY_CREDENTIAL_BINDING_VERSION.to_string();
    encode_aad_fields("coral-credential-dek", &[binding_version.as_str(), key_id])
}

fn legacy_credential_dek_aad(key_id: &str) -> Vec<u8> {
    format!("coral-credential-dek:v{LEGACY_CREDENTIAL_BINDING_VERSION}:{key_id}").into_bytes()
}

fn encode_aad_fields(domain: &str, fields: &[&str]) -> Vec<u8> {
    let mut aad = Vec::new();
    aad.extend_from_slice(domain.as_bytes());
    aad.push(0);
    for field in fields {
        encode_aad_field(&mut aad, field);
    }
    aad
}

fn encode_aad_field(aad: &mut Vec<u8>, field: &str) {
    let bytes = field.as_bytes();
    aad.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    aad.extend_from_slice(bytes);
}

fn random_array<const N: usize>() -> Result<[u8; N], CredentialsError> {
    let mut bytes = [0_u8; N];
    SystemRandom::new().fill(&mut bytes).map_err(|_error| {
        CredentialsError::Crypto("secure random generation failed".to_string())
    })?;
    Ok(bytes)
}

fn key_id_for_bytes(bytes: &[u8; KEY_LEN]) -> String {
    let digest = Sha256::digest(bytes);
    let hex = format!("{digest:x}");
    format!("local-file-{}", hex.get(..16).unwrap_or(hex.as_str()))
}

/// Deterministic key providers shared by every envelope-crypto test suite.
#[cfg(test)]
pub(crate) mod test_support {
    use super::{CredentialsError, EnvelopeEncryptionKey, EnvelopeKeyProvider};
    use async_trait::async_trait;

    /// Serves one fixed key and refuses every other key identifier.
    #[derive(Clone)]
    pub(crate) struct StaticKeyProvider {
        pub(crate) key: EnvelopeEncryptionKey,
    }

    #[async_trait]
    impl EnvelopeKeyProvider for StaticKeyProvider {
        async fn active_key(&self) -> Result<EnvelopeEncryptionKey, CredentialsError> {
            Ok(self.key.clone())
        }

        async fn key(&self, key_id: &str) -> Result<EnvelopeEncryptionKey, CredentialsError> {
            if self.key.key_id() == key_id {
                Ok(self.key.clone())
            } else {
                Err(CredentialsError::Crypto("missing test key".to_string()))
            }
        }
    }

    /// Wraps with `active` while keeping every key in `keys` unwrappable.
    #[derive(Clone)]
    pub(crate) struct RotatingKeyProvider {
        pub(crate) active: EnvelopeEncryptionKey,
        pub(crate) keys: Vec<EnvelopeEncryptionKey>,
    }

    #[async_trait]
    impl EnvelopeKeyProvider for RotatingKeyProvider {
        async fn active_key(&self) -> Result<EnvelopeEncryptionKey, CredentialsError> {
            Ok(self.active.clone())
        }

        async fn key(&self, key_id: &str) -> Result<EnvelopeEncryptionKey, CredentialsError> {
            self.keys
                .iter()
                .find(|key| key.key_id() == key_id)
                .cloned()
                .ok_or_else(|| CredentialsError::Crypto("missing test key".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{EnvelopeKeyProvider, LocalFileEnvelopeKeyProvider};
    use crate::state::AppStateLayout;

    #[tokio::test]
<<<<<<< HEAD
    async fn missing_key_lookup_does_not_create_a_local_key_file() {
||||||| parent of 1b6c0a465 (refactor(app): make envelope key resolution async)
    #[test]
    fn configured_provider_selects_active_and_resolves_decryption_keys() {
        let active = EnvelopeEncryptionKey::from_static_bytes_for_test([7_u8; KEY_LEN]);
        let previous = EnvelopeEncryptionKey::from_static_bytes_for_test([8_u8; KEY_LEN]);
        let provider = ConfiguredEnvelopeKeyProvider::new(active.clone(), [previous.clone()])
            .expect("configured key ring");

        assert_eq!(provider.active_key().expect("active key"), active);
        assert_eq!(
            provider.key(previous.key_id()).expect("previous key"),
            previous
        );
        assert!(matches!(
            provider.key("missing-key"),
            Err(CredentialsError::Unavailable(_))
        ));
    }

    #[test]
    fn configured_provider_rejects_duplicate_key_material() {
        let active = EnvelopeEncryptionKey::from_static_bytes_for_test([7_u8; KEY_LEN]);

        let error = ConfiguredEnvelopeKeyProvider::new(active.clone(), [active])
            .expect_err("duplicate key id");

        assert!(matches!(error, CredentialsError::Parse(_)));
    }

    #[test]
    fn provided_key_does_not_create_a_local_key_file() {
=======
    async fn configured_provider_selects_active_and_resolves_decryption_keys() {
        let active = EnvelopeEncryptionKey::from_static_bytes_for_test([7_u8; KEY_LEN]);
        let previous = EnvelopeEncryptionKey::from_static_bytes_for_test([8_u8; KEY_LEN]);
        let provider = ConfiguredEnvelopeKeyProvider::new(active.clone(), [previous.clone()])
            .expect("configured key ring");

        assert_eq!(provider.active_key().await.expect("active key"), active);
        assert_eq!(
            provider.key(previous.key_id()).await.expect("previous key"),
            previous
        );
        assert!(matches!(
            provider.key("missing-key").await,
            Err(CredentialsError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn configured_provider_rejects_duplicate_key_material() {
        let active = EnvelopeEncryptionKey::from_static_bytes_for_test([7_u8; KEY_LEN]);

        let error = ConfiguredEnvelopeKeyProvider::new(active.clone(), [active])
            .expect_err("duplicate key id");

        assert!(matches!(error, CredentialsError::Parse(_)));
    }

    #[tokio::test]
    async fn provided_key_does_not_create_a_local_key_file() {
>>>>>>> 1b6c0a465 (refactor(app): make envelope key resolution async)
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let provider = LocalFileEnvelopeKeyProvider::new(&layout);

<<<<<<< HEAD
||||||| parent of 1b6c0a465 (refactor(app): make envelope key resolution async)
        let first = provider.active_key().expect("provided key");
        let second = provider.key(first.key_id()).expect("provided key by id");

        assert_eq!(first, second);
        assert!(!layout.envelope_encryption_key_file().exists());
    }

    #[test]
    fn missing_key_lookup_does_not_create_a_local_key_file() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let provider = LocalFileEnvelopeKeyProvider::new(&layout, None);

        let error = provider.key("missing-key").expect_err("missing key");
=======
        let first = provider.active_key().await.expect("provided key");
        let second = provider
            .key(first.key_id())
            .await
            .expect("provided key by id");

        assert_eq!(first, second);
        assert!(!layout.envelope_encryption_key_file().exists());
    }

    #[tokio::test]
    async fn missing_key_lookup_does_not_create_a_local_key_file() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let provider = LocalFileEnvelopeKeyProvider::new(&layout, None);

>>>>>>> 1b6c0a465 (refactor(app): make envelope key resolution async)
        let error = provider.key("missing-key").await.expect_err("missing key");

        assert!(error.to_string().contains("is unavailable"));
        assert!(!layout.envelope_encryption_key_file().exists());
        assert!(
            !layout
                .envelope_encryption_key_file()
                .parent()
                .unwrap()
                .exists()
        );
    }
<<<<<<< HEAD
||||||| parent of 1b6c0a465 (refactor(app): make envelope key resolution async)

    #[test]
    fn provided_key_keeps_existing_file_key_available_for_rewrap() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let file_key = LocalFileEnvelopeKeyProvider::new(&layout, None)
            .active_key()
            .expect("file key");
        let provided_key = EnvelopeEncryptionKey::from_static_bytes_for_test([9_u8; KEY_LEN]);
        let provider = LocalFileEnvelopeKeyProvider::new(&layout, Some(provided_key.clone()));

        assert_eq!(provider.active_key().expect("provided key"), provided_key);
        assert_eq!(provider.key(file_key.key_id()).expect("file key"), file_key);
    }
=======

    #[tokio::test]
    async fn provided_key_keeps_existing_file_key_available_for_rewrap() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let file_key = LocalFileEnvelopeKeyProvider::new(&layout, None)
            .active_key()
            .await
            .expect("file key");
        let provided_key = EnvelopeEncryptionKey::from_static_bytes_for_test([9_u8; KEY_LEN]);
        let provider = LocalFileEnvelopeKeyProvider::new(&layout, Some(provided_key.clone()));

        assert_eq!(
            provider.active_key().await.expect("provided key"),
            provided_key
        );
        assert_eq!(
            provider.key(file_key.key_id()).await.expect("file key"),
            file_key
        );
    }
>>>>>>> 1b6c0a465 (refactor(app): make envelope key resolution async)
}
