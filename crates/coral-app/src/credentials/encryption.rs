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
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom as _, SystemRandom};
use sha2::{Digest as _, Sha256};
use tracing::{info, warn};
use zeroize::{Zeroize as _, Zeroizing};

use super::CredentialsError;
use super::config::CredentialEncryptionKeySource;
use super::store::{CredentialConfigNamespace, KeychainCredentialBackend};
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
const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;

static LOCAL_KEY_FILE_LOCK: Mutex<()> = Mutex::new(());

pub(crate) type EncryptedCredentialDocument = EncryptedEnvelopeDocument;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CredentialEncryptionKeyOrigin {
    Provided,
    File,
    CreatedFile,
    Keychain,
    CreatedKeychain,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct CredentialEncryptionKey {
    key_id: String,
    bytes: [u8; KEY_LEN],
}

impl fmt::Debug for CredentialEncryptionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CredentialEncryptionKey")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

impl Drop for CredentialEncryptionKey {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

impl CredentialEncryptionKey {
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
/// `active_key` is a minting capability: [`LocalFileCredentialKeyProvider`] creates
/// durable key material on disk when none exists. Only sealing and rewrapping take a
/// provider; opening takes the [`CredentialEncryptionKey`] its document names, so a
/// read path cannot create or rotate key material.
pub(crate) trait CredentialKeyProvider: Send + Sync {
    fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError>;

    fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError>;
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

/// Resolves an explicitly supplied key or the caller-selected local key source.
/// Callers own configuration and environment resolution.
#[derive(Clone)]
pub(crate) struct LocalFileCredentialKeyProvider {
    provided_key: Option<CredentialEncryptionKey>,
    source: CredentialEncryptionKeySource,
    file: PlaintextFileCredentialKeyProvider,
    keychain: KeychainCredentialKeyProvider,
}

impl fmt::Debug for LocalFileCredentialKeyProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalFileCredentialKeyProvider")
            .field("has_provided_key", &self.provided_key.is_some())
            .field("source", &self.source)
            .field("file", &self.file)
            .field("keychain", &self.keychain)
            .finish()
    }
}

impl LocalFileCredentialKeyProvider {
    pub(crate) fn with_source(
        layout: &AppStateLayout,
        provided_key: Option<CredentialEncryptionKey>,
        source: CredentialEncryptionKeySource,
    ) -> Self {
        Self {
            provided_key,
            source,
            file: PlaintextFileCredentialKeyProvider::new(layout),
            keychain: KeychainCredentialKeyProvider::new(layout),
        }
    }

    #[cfg(test)]
    fn with_keychain_for_test(
        layout: &AppStateLayout,
        provided_key: Option<CredentialEncryptionKey>,
        source: CredentialEncryptionKeySource,
        keychain: KeychainCredentialKeyProvider,
    ) -> Self {
        Self {
            provided_key,
            source,
            file: PlaintextFileCredentialKeyProvider::new(layout),
            keychain,
        }
    }

    pub(crate) fn active_key_origin(
        &self,
    ) -> Result<CredentialEncryptionKeyOrigin, CredentialsError> {
        Ok(self.active_key_selection()?.1)
    }

    fn active_key_selection(
        &self,
    ) -> Result<(CredentialEncryptionKey, CredentialEncryptionKeyOrigin), CredentialsError> {
        if let Some(key) = &self.provided_key {
            return Ok((key.clone(), CredentialEncryptionKeyOrigin::Provided));
        }
        match self.source {
            CredentialEncryptionKeySource::Auto => self.auto_key_selection(),
            CredentialEncryptionKeySource::File => self.file.active_key_selection(),
            CredentialEncryptionKeySource::Keychain => self
                .keychain
                .active_key_selection()
                .map_err(configured_keychain_key_unavailable),
            CredentialEncryptionKeySource::Vault => Err(CredentialsError::Unavailable(
                "vault credential encryption key source is not implemented".to_string(),
            )),
        }
    }

    fn auto_key_selection(
        &self,
    ) -> Result<(CredentialEncryptionKey, CredentialEncryptionKeyOrigin), CredentialsError> {
        if self.file.exists()? {
            info!(
                path = %self.file.path().display(),
                "using existing plaintext credential encryption key; migrate the KEK to keychain before enabling keychain sourcing"
            );
            return self.file.active_key_selection();
        }

        match self.keychain.probe() {
            Ok(()) => self.keychain.active_key_selection(),
            Err(error) => {
                warn!(
                    detail = %error,
                    path = %self.file.path().display(),
                    "keychain unavailable; falling back to plaintext credential encryption key file; configure a keychain or set [credentials].encryption_key_source = \"keychain\" to fail closed"
                );
                self.file.active_key_selection()
            }
        }
    }
}

impl CredentialKeyProvider for LocalFileCredentialKeyProvider {
    fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
        Ok(self.active_key_selection()?.0)
    }

    fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
        if let Some(key) = &self.provided_key
            && key.key_id == key_id
        {
            return Ok(key.clone());
        }
        if let Some(key) = self.file.load_key()?
            && key.key_id == key_id
        {
            return Ok(key);
        }
        match self.keychain.load_key() {
            Ok(Some(key)) if key.key_id == key_id => return Ok(key),
            Err(error) if self.source == CredentialEncryptionKeySource::Keychain => {
                return Err(configured_keychain_key_unavailable(error));
            }
            Ok(_) | Err(_) => {}
        }
        Err(key_unavailable(key_id))
    }
}

#[derive(Debug, Clone)]
struct PlaintextFileCredentialKeyProvider {
    path: PathBuf,
}

impl PlaintextFileCredentialKeyProvider {
    fn new(layout: &AppStateLayout) -> Self {
        Self {
            path: layout.credential_encryption_key_file(),
        }
    }

    fn path(&self) -> &Path {
        self.path.as_path()
    }

    fn exists(&self) -> Result<bool, CredentialsError> {
        self.path.try_exists().map_err(Into::into)
    }

    fn load_or_create_key(
        &self,
    ) -> Result<(CredentialEncryptionKey, CredentialEncryptionKeyOrigin), CredentialsError> {
        let _thread_guard = LOCAL_KEY_FILE_LOCK.lock().map_err(|_error| {
            CredentialsError::Crypto("credential encryption key lock is poisoned".to_string())
        })?;
        if let Some(parent) = self.path.parent() {
            storage_fs::ensure_private_dir(parent)?;
        }
        let lock_path = self.path.with_extension("key.lock");
        let _process_guard = FileLock::exclusive(&lock_path)?;

        if let Some(key) = self.load_key()? {
            return Ok((key, CredentialEncryptionKeyOrigin::File));
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
        Ok((
            CredentialEncryptionKey {
                key_id: key_id_for_bytes(&bytes),
                bytes: *bytes,
            },
            CredentialEncryptionKeyOrigin::CreatedFile,
        ))
    }

    fn load_key(&self) -> Result<Option<CredentialEncryptionKey>, CredentialsError> {
        match std::fs::read_to_string(&self.path) {
            Ok(raw) => {
                let raw = Zeroizing::new(raw);
                CredentialEncryptionKey::from_encoded_material(raw.as_str()).map(Some)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn active_key_selection(
        &self,
    ) -> Result<(CredentialEncryptionKey, CredentialEncryptionKeyOrigin), CredentialsError> {
        self.load_or_create_key()
    }
}

impl CredentialKeyProvider for PlaintextFileCredentialKeyProvider {
    fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
        Ok(self.active_key_selection()?.0)
    }

    fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
        self.load_key()?
            .ok_or_else(|| key_unavailable(key_id))
            .and_then(|key| verify_key_id(key, key_id))
    }
}

#[derive(Clone)]
pub(crate) struct KeychainCredentialKeyProvider {
    keychain: Arc<dyn CredentialEncryptionKeychain>,
    lock_path: PathBuf,
}

impl fmt::Debug for KeychainCredentialKeyProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeychainCredentialKeyProvider")
            .finish_non_exhaustive()
    }
}

impl KeychainCredentialKeyProvider {
    pub(crate) fn new(layout: &AppStateLayout) -> Self {
        let namespace = CredentialConfigNamespace::from_layout(layout);
        let backend = KeychainCredentialBackend::new(namespace.clone());
        Self {
            keychain: Arc::new(NativeCredentialEncryptionKeychain {
                backend,
                entry: CredentialEncryptionKeychainEntry::from_namespace(&namespace),
            }),
            lock_path: layout
                .credential_encryption_key_file()
                .with_extension("keychain.lock"),
        }
    }

    #[cfg(test)]
    fn with_keychain_for_test(
        layout: &AppStateLayout,
        keychain: Arc<dyn CredentialEncryptionKeychain>,
    ) -> Self {
        Self {
            keychain,
            lock_path: layout
                .credential_encryption_key_file()
                .with_extension("keychain.lock"),
        }
    }

    fn probe(&self) -> Result<(), CredentialsError> {
        self.keychain.probe()
    }

    fn load_key(&self) -> Result<Option<CredentialEncryptionKey>, CredentialsError> {
        if let Some(raw) = self.keychain.read_key_material()? {
            return CredentialEncryptionKey::from_encoded_material(raw.as_str()).map(Some);
        }
        Ok(None)
    }

    fn load_or_create_key(
        &self,
    ) -> Result<(CredentialEncryptionKey, CredentialEncryptionKeyOrigin), CredentialsError> {
        if let Some(key) = self.load_key()? {
            return Ok((key, CredentialEncryptionKeyOrigin::Keychain));
        }
        if let Some(parent) = self.lock_path.parent() {
            storage_fs::ensure_private_dir(parent)?;
        }
        let _guard = FileLock::exclusive(&self.lock_path)?;
        if let Some(key) = self.load_key()? {
            return Ok((key, CredentialEncryptionKeyOrigin::Keychain));
        }
        let bytes = Zeroizing::new(random_array::<KEY_LEN>()?);
        let mut encoded = Zeroizing::new(format!("{KEY_FILE_VERSION}:"));
        base64::engine::general_purpose::STANDARD.encode_string(bytes.as_slice(), &mut encoded);
        self.keychain.write_key_material(encoded)?;
        info!("created keychain credential encryption key");
        Ok((
            CredentialEncryptionKey {
                key_id: key_id_for_bytes(&bytes),
                bytes: *bytes,
            },
            CredentialEncryptionKeyOrigin::CreatedKeychain,
        ))
    }

    fn active_key_selection(
        &self,
    ) -> Result<(CredentialEncryptionKey, CredentialEncryptionKeyOrigin), CredentialsError> {
        self.load_or_create_key()
    }
}

impl CredentialKeyProvider for KeychainCredentialKeyProvider {
    fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
        Ok(self.active_key_selection()?.0)
    }

    fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
        self.load_key()?
            .ok_or_else(|| key_unavailable(key_id))
            .and_then(|key| verify_key_id(key, key_id))
    }
}

trait CredentialEncryptionKeychain: Send + Sync {
    fn probe(&self) -> Result<(), CredentialsError>;

    fn read_key_material(&self) -> Result<Option<Zeroizing<String>>, CredentialsError>;

    fn write_key_material(&self, material: Zeroizing<String>) -> Result<(), CredentialsError>;
}

#[derive(Clone)]
struct NativeCredentialEncryptionKeychain {
    backend: KeychainCredentialBackend,
    entry: CredentialEncryptionKeychainEntry,
}

impl CredentialEncryptionKeychain for NativeCredentialEncryptionKeychain {
    fn probe(&self) -> Result<(), CredentialsError> {
        self.backend.run_native(|backend| backend.probe_native())
    }

    fn read_key_material(&self) -> Result<Option<Zeroizing<String>>, CredentialsError> {
        let entry = self.entry.clone();
        self.backend.run_native(move |backend| {
            backend.probe_native()?;
            match backend
                .entry_for(&entry.service, &entry.account)?
                .get_password()
            {
                Ok(value) => Ok(Some(Zeroizing::new(value))),
                Err(keyring_core::Error::NoEntry) => Ok(None),
                Err(error) => Err(CredentialsError::Unavailable(error.to_string())),
            }
        })
    }

    fn write_key_material(&self, material: Zeroizing<String>) -> Result<(), CredentialsError> {
        let entry = self.entry.clone();
        self.backend.run_native(move |backend| {
            backend.probe_native()?;
            backend
                .entry_for(&entry.service, &entry.account)?
                .set_password(material.as_str())
                .map_err(|error| CredentialsError::Unavailable(error.to_string()))
        })
    }
}

#[derive(Clone)]
struct CredentialEncryptionKeychainEntry {
    service: String,
    account: String,
}

impl CredentialEncryptionKeychainEntry {
    fn from_namespace(config_namespace: &CredentialConfigNamespace) -> Self {
        Self {
            service: format!(
                "com.withcoral.coral/{}/credential-encryption-key",
                config_namespace.as_str()
            ),
            account: "active".to_string(),
        }
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
    key_provider: &dyn CredentialKeyProvider,
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
    seal_envelope_document(&context, document_bytes, key_provider)
}

pub(crate) fn decrypt_credential_values(
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    document: &EncryptedCredentialDocument,
    kek: &CredentialEncryptionKey,
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
    key_provider: &dyn CredentialKeyProvider,
) -> Result<Option<EncryptedCredentialDocument>, CredentialsError> {
    let context =
        credential_document_context(document.binding_version, workspace_name, source_name)?;
    if document.binding_version == LEGACY_CREDENTIAL_BINDING_VERSION {
        // Binding version 1 includes both historical credential AAD encodings.
        // Authenticate either shape, then reseal once into the exact v2 recipe.
        let kek = key_provider.key(&document.key_id)?;
        let values = decrypt_credential_values(workspace_name, source_name, document, &kek)?;
        return encrypt_credential_values(workspace_name, source_name, &values, key_provider)
            .map(Some);
    }
    rewrap_envelope_document(&context, document, key_provider)
}

fn decrypt_credential_document_bytes(
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    document: &EncryptedCredentialDocument,
    kek: &CredentialEncryptionKey,
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
    kek: &CredentialEncryptionKey,
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
    kek: &CredentialEncryptionKey,
    context: &EnvelopeContext,
) -> Result<Zeroizing<[u8; KEY_LEN]>, CredentialsError> {
    unwrap_dek_with_aad(document, kek, &context.dek_aad(&document.key_id))
}

fn unwrap_dek_with_aad(
    document: &EncryptedEnvelopeDocument,
    kek: &CredentialEncryptionKey,
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
    active_kek: &CredentialEncryptionKey,
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
    kek: &CredentialEncryptionKey,
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
    key_provider: &dyn CredentialKeyProvider,
) -> Result<EncryptedEnvelopeDocument, CredentialsError> {
    let kek = key_provider.active_key()?;
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
/// Takes the KEK the document names rather than a [`CredentialKeyProvider`]: opening
/// needs no authority to mint or rotate keys, and resolving the KEK in the caller
/// keeps provider failures distinguishable from authentication failures.
pub(crate) fn open_envelope_document(
    context: &EnvelopeContext,
    document: &EncryptedEnvelopeDocument,
    kek: &CredentialEncryptionKey,
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
    key_provider: &dyn CredentialKeyProvider,
) -> Result<Option<EncryptedEnvelopeDocument>, CredentialsError> {
    validate_document_metadata(document, context)?;
    let old_kek = key_provider.key(&document.key_id)?;
    let active_kek = key_provider.active_key()?;
    let dek = unwrap_current_dek(document, &old_kek, context)?;
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

    rewrap_dek(document, &active_kek, &dek, context).map(Some)
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

fn verify_key_id(
    key: CredentialEncryptionKey,
    key_id: &str,
) -> Result<CredentialEncryptionKey, CredentialsError> {
    if key.key_id == key_id {
        Ok(key)
    } else {
        Err(key_unavailable(key_id))
    }
}

fn key_unavailable(key_id: &str) -> CredentialsError {
    CredentialsError::Crypto(format!(
        "credential encryption key '{key_id}' is unavailable"
    ))
}

fn configured_keychain_key_unavailable(error: CredentialsError) -> CredentialsError {
    match error {
        CredentialsError::Unavailable(detail) => CredentialsError::Unavailable(format!(
            "credential encryption key source is configured for keychain, but keychain is unavailable: {detail}"
        )),
        error => error,
    }
}

fn key_id_for_bytes(bytes: &[u8; KEY_LEN]) -> String {
    let digest = Sha256::digest(bytes);
    let hex = format!("{digest:x}");
    format!("local-file-{}", hex.get(..16).unwrap_or(hex.as_str()))
}

/// Deterministic key providers shared by every envelope-crypto test suite.
#[cfg(test)]
pub(crate) mod test_support {
    use super::{CredentialEncryptionKey, CredentialKeyProvider, CredentialsError};

    /// Serves one fixed key and refuses every other key identifier.
    #[derive(Clone)]
    pub(crate) struct StaticKeyProvider {
        pub(crate) key: CredentialEncryptionKey,
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

    /// Wraps with `active` while keeping every key in `keys` unwrappable.
    #[derive(Clone)]
    pub(crate) struct RotatingKeyProvider {
        pub(crate) active: CredentialEncryptionKey,
        pub(crate) keys: Vec<CredentialEncryptionKey>,
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
}
#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use base64::Engine as _;
    use tempfile::{TempDir, tempdir};

    use super::*;
    use crate::state::AppStateLayout;

    #[test]
    fn provided_key_does_not_create_a_local_key_file() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let encoded = format!(
            "{KEY_FILE_VERSION}:{}",
            base64::engine::general_purpose::STANDARD.encode([7_u8; KEY_LEN])
        );
        let key = CredentialEncryptionKey::from_encoded_material(&encoded).expect("encoded key");
        let provider = LocalFileCredentialKeyProvider::with_source(
            &layout,
            Some(key),
            CredentialEncryptionKeySource::Auto,
        );

        let first = provider.active_key().expect("provided key");
        let second = provider.key(first.key_id()).expect("provided key by id");

        assert_eq!(first, second);
        assert_eq!(
            provider.active_key_origin().expect("provided origin"),
            CredentialEncryptionKeyOrigin::Provided
        );
        assert!(!layout.credential_encryption_key_file().exists());
    }

    #[test]
    fn missing_key_lookup_does_not_create_a_local_key_file() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let keychain = FakeKeychain::available();
        let provider = LocalFileCredentialKeyProvider::with_keychain_for_test(
            &layout,
            None,
            CredentialEncryptionKeySource::Auto,
            KeychainCredentialKeyProvider::with_keychain_for_test(&layout, keychain.clone()),
        );

        let error = provider.key("missing-key").expect_err("missing key");

        assert!(error.to_string().contains("is unavailable"));
        assert!(!layout.credential_encryption_key_file().exists());
        assert!(keychain.material().is_none());
    }

    #[test]
    fn provided_key_keeps_existing_file_key_available_for_rewrap() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let file_key = LocalFileCredentialKeyProvider::with_source(
            &layout,
            None,
            CredentialEncryptionKeySource::File,
        )
        .active_key()
        .expect("file key");
        let provided_key = CredentialEncryptionKey::from_static_bytes_for_test([9_u8; KEY_LEN]);
        let provider = LocalFileCredentialKeyProvider::with_source(
            &layout,
            Some(provided_key.clone()),
            CredentialEncryptionKeySource::File,
        );

        assert_eq!(provider.active_key().expect("provided key"), provided_key);
        assert_eq!(provider.key(file_key.key_id()).expect("file key"), file_key);
    }

    #[test]
    fn keychain_key_provider_stores_and_reuses_versioned_key() {
        let (_temp, layout) = temp_layout();
        let keychain = FakeKeychain::available();
        let provider =
            KeychainCredentialKeyProvider::with_keychain_for_test(&layout, keychain.clone());

        let first = provider.active_key().expect("first key");
        let second = provider.active_key().expect("second key");

        assert_eq!(first, second);
        assert!(
            keychain
                .material()
                .expect("stored key")
                .starts_with(&format!("{KEY_FILE_VERSION}:")),
            "keychain key material should use the shared versioned encoding"
        );
    }

    #[test]
    fn key_lookup_does_not_create_missing_key_material() {
        let (_temp, layout) = temp_layout();
        let keychain = FakeKeychain::available();
        let keychain_provider =
            KeychainCredentialKeyProvider::with_keychain_for_test(&layout, keychain.clone());

        keychain_provider
            .key("missing-key")
            .expect_err("missing keychain key should fail");

        assert!(keychain.material().is_none(), "lookup must not create KEK");
        LocalFileCredentialKeyProvider::with_source(
            &layout,
            None,
            CredentialEncryptionKeySource::File,
        )
        .key("missing-key")
        .expect_err("missing file key should fail");
        assert!(
            !layout.credential_encryption_key_file().exists(),
            "file lookup must not create KEK"
        );
    }

    #[test]
    #[ignore = "uses the native OS keychain; run manually on hosts with keychain access"]
    fn native_keychain_key_provider_round_trips_key_material() {
        if !matches!(
            crate::bootstrap::AppEnvironment::env_var("CORAL_RUN_NATIVE_KEYCHAIN_TESTS"),
            Ok(Some(_))
        ) {
            return;
        }
        let (_temp, layout) = temp_layout();
        let first = KeychainCredentialKeyProvider::new(&layout)
            .active_key()
            .expect("native keychain should create KEK");
        let second = KeychainCredentialKeyProvider::new(&layout)
            .key(first.key_id())
            .expect("native keychain should read KEK by id");

        assert_eq!(first, second);
    }

    #[test]
    fn auto_prefers_keychain_without_creating_file_key() {
        let (_temp, layout) = temp_layout();
        let keychain = FakeKeychain::available();
        let provider = LocalFileCredentialKeyProvider::with_keychain_for_test(
            &layout,
            None,
            CredentialEncryptionKeySource::Auto,
            KeychainCredentialKeyProvider::with_keychain_for_test(&layout, keychain.clone()),
        );

        assert_eq!(
            provider
                .active_key_origin()
                .expect("created keychain origin"),
            CredentialEncryptionKeyOrigin::CreatedKeychain
        );
        assert_eq!(
            provider
                .active_key_origin()
                .expect("existing keychain origin"),
            CredentialEncryptionKeyOrigin::Keychain
        );

        assert!(keychain.material().is_some(), "auto should write keychain");
        assert!(
            !layout.credential_encryption_key_file().exists(),
            "auto keychain success should not create plaintext key material"
        );
    }

    #[test]
    fn auto_falls_back_to_file_with_loud_warning_when_keychain_unavailable() {
        let (_temp, layout) = temp_layout();
        let keychain = FakeKeychain::unavailable();
        let provider = LocalFileCredentialKeyProvider::with_keychain_for_test(
            &layout,
            None,
            CredentialEncryptionKeySource::Auto,
            KeychainCredentialKeyProvider::with_keychain_for_test(&layout, keychain),
        );

        assert_eq!(
            provider.active_key_origin().expect("created file origin"),
            CredentialEncryptionKeyOrigin::CreatedFile
        );
        assert_eq!(
            provider.active_key_origin().expect("existing file origin"),
            CredentialEncryptionKeyOrigin::File
        );

        assert!(
            layout.credential_encryption_key_file().exists(),
            "unavailable keychain should fall back to the plaintext key file"
        );
    }

    #[test]
    fn auto_keeps_existing_file_key_even_when_keychain_available() {
        let (_temp, layout) = temp_layout();
        let file_key = LocalFileCredentialKeyProvider::with_source(
            &layout,
            None,
            CredentialEncryptionKeySource::File,
        )
        .active_key()
        .expect("file key");
        let keychain = FakeKeychain::available();
        let provider = LocalFileCredentialKeyProvider::with_keychain_for_test(
            &layout,
            None,
            CredentialEncryptionKeySource::Auto,
            KeychainCredentialKeyProvider::with_keychain_for_test(&layout, keychain.clone()),
        );

        let selected = provider.active_key().expect("auto key");

        assert_eq!(selected, file_key);
        assert_eq!(
            provider.active_key_origin().expect("existing file origin"),
            CredentialEncryptionKeyOrigin::File
        );
        assert!(
            keychain.material().is_none(),
            "existing file key should keep auto mode from silently switching to keychain"
        );
    }

    #[test]
    fn explicit_keychain_source_fails_closed_when_unavailable() {
        let (_temp, layout) = temp_layout();
        let provider = LocalFileCredentialKeyProvider::with_keychain_for_test(
            &layout,
            None,
            CredentialEncryptionKeySource::Keychain,
            KeychainCredentialKeyProvider::with_keychain_for_test(
                &layout,
                FakeKeychain::unavailable(),
            ),
        );

        let error = provider
            .active_key()
            .expect_err("explicit keychain should not fall back");

        assert!(
            matches!(error, CredentialsError::Unavailable(_)),
            "unexpected error: {error:#}"
        );
        assert!(
            !layout.credential_encryption_key_file().exists(),
            "explicit keychain failure must not create a plaintext key"
        );
    }

    fn temp_layout() -> (TempDir, AppStateLayout) {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        (temp, layout)
    }

    struct FakeKeychain {
        available: bool,
        material: Mutex<Option<String>>,
    }

    impl FakeKeychain {
        fn available() -> Arc<Self> {
            Arc::new(Self {
                available: true,
                material: Mutex::new(None),
            })
        }

        fn unavailable() -> Arc<Self> {
            Arc::new(Self {
                available: false,
                material: Mutex::new(None),
            })
        }

        fn material(&self) -> Option<String> {
            self.material.lock().expect("material lock").clone()
        }
    }

    impl CredentialEncryptionKeychain for FakeKeychain {
        fn probe(&self) -> Result<(), CredentialsError> {
            if self.available {
                Ok(())
            } else {
                Err(CredentialsError::Unavailable(
                    "fake keychain unavailable".to_string(),
                ))
            }
        }

        fn read_key_material(&self) -> Result<Option<Zeroizing<String>>, CredentialsError> {
            self.probe()?;
            Ok(self.material().map(Zeroizing::new))
        }

        fn write_key_material(&self, material: Zeroizing<String>) -> Result<(), CredentialsError> {
            self.probe()?;
            *self.material.lock().map_err(|error| {
                CredentialsError::Unavailable(format!("fake keychain lock poisoned: {error}"))
            })? = Some(material.as_str().to_string());
            Ok(())
        }
    }
}
