//! Application-level envelope encryption for DB-backed credential material.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Credential DB runtime wiring lands in a later stack branch; this branch isolates cryptographic primitives for review."
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
use super::store::{CredentialConfigNamespace, KeychainCredentialBackend};
use crate::bootstrap;
use crate::sources::SourceName;
use crate::state::AppStateLayout;
use crate::storage::fs as storage_fs;
use crate::storage::fs::FileLock;
use crate::workspaces::WorkspaceName;

pub(crate) const CREDENTIAL_DOCUMENT_ALGORITHM: &str = "AES-256-GCM";
pub(crate) const CREDENTIAL_DOCUMENT_AAD_VERSION: i64 = 1;

const CREDENTIAL_DOCUMENT_VERSION: u32 = 1;
const KEY_FILE_VERSION: &str = "v1";
const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;

static LOCAL_KEY_FILE_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CredentialEncryptionKeySource {
    Auto,
    File,
    Keychain,
    Vault,
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

pub(crate) trait CredentialKeyProvider: Send + Sync {
    fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError>;

    fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError>;
}

/// Resolves credential encryption keys for DB-backed credential documents.
///
/// The historical type name is kept for the upstack runtime wiring. In auto
/// mode this provider prefers the OS keychain and uses the plaintext key file
/// only when keychain storage is unavailable or an existing file key would
/// otherwise strand encrypted rows. Shared-Postgres deployments MUST provision
/// the same KEK on every server via `[credentials].encryption_key_env` or by
/// pre-seeding each host's keychain with the same key material.
#[derive(Clone)]
pub(crate) struct LocalFileCredentialKeyProvider {
    config_file: PathBuf,
    file: PlaintextFileCredentialKeyProvider,
    keychain: KeychainCredentialKeyProvider,
}

impl fmt::Debug for LocalFileCredentialKeyProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalFileCredentialKeyProvider")
            .field("config_file", &self.config_file)
            .field("file", &self.file)
            .field("keychain", &self.keychain)
            .finish()
    }
}

impl LocalFileCredentialKeyProvider {
    pub(crate) fn new(layout: &AppStateLayout) -> Self {
        Self {
            config_file: layout.config_file().to_path_buf(),
            file: PlaintextFileCredentialKeyProvider::new(layout),
            keychain: KeychainCredentialKeyProvider::new(layout),
        }
    }

    #[cfg(test)]
    fn with_keychain_for_test(
        layout: &AppStateLayout,
        keychain: KeychainCredentialKeyProvider,
    ) -> Self {
        Self {
            config_file: layout.config_file().to_path_buf(),
            file: PlaintextFileCredentialKeyProvider::new(layout),
            keychain,
        }
    }

    fn load_configured_key(&self) -> Result<Option<CredentialEncryptionKey>, CredentialsError> {
        let Some(env_name) = configured_key_env(&self.config_file)? else {
            return Ok(None);
        };
        let raw = bootstrap::env_var(&env_name).ok_or_else(|| {
            CredentialsError::Crypto(format!(
                "credential encryption key environment variable `{env_name}` is not set"
            ))
        })?;
        decode_key_material(&raw).map(Some).map_err(|error| {
            CredentialsError::Crypto(format!(
                "invalid credential encryption key from environment variable `{env_name}`: {error}"
            ))
        })
    }

    fn configured_source(&self) -> Result<CredentialEncryptionKeySource, CredentialsError> {
        configured_key_source(&self.config_file)
    }

    fn auto_key(&self, key_id: Option<&str>) -> Result<CredentialEncryptionKey, CredentialsError> {
        if self.file.exists()? {
            info!(
                path = %self.file.path().display(),
                "using existing plaintext credential encryption key; migrate the KEK to keychain before enabling keychain sourcing"
            );
            return self.file.resolve_key(key_id);
        }

        match self.keychain.probe() {
            Ok(()) => self.keychain.resolve_key(key_id),
            Err(error) => {
                warn!(
                    detail = %error,
                    path = %self.file.path().display(),
                    "keychain unavailable; falling back to plaintext credential encryption key file; configure a keychain or set [credentials].encryption_key_source = \"keychain\" to fail closed"
                );
                self.file.resolve_key(key_id)
            }
        }
    }

    fn explicit_source_key(
        &self,
        source: CredentialEncryptionKeySource,
        key_id: Option<&str>,
    ) -> Result<CredentialEncryptionKey, CredentialsError> {
        match source {
            CredentialEncryptionKeySource::Auto => self.auto_key(key_id),
            CredentialEncryptionKeySource::File => self.file.resolve_key(key_id),
            CredentialEncryptionKeySource::Keychain => self
                .keychain
                .resolve_key(key_id)
                .map_err(configured_keychain_key_unavailable),
            CredentialEncryptionKeySource::Vault => Err(CredentialsError::Unavailable(
                "vault credential encryption key source is not implemented".to_string(),
            )),
        }
    }
}

impl CredentialKeyProvider for LocalFileCredentialKeyProvider {
    fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
        if let Some(key) = self.load_configured_key()? {
            return Ok(key);
        }
        self.explicit_source_key(self.configured_source()?, None)
    }

    fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
        if let Some(key) = self.load_configured_key()? {
            return verify_key_id(key, key_id);
        }
        self.explicit_source_key(self.configured_source()?, Some(key_id))
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

    fn load_or_create_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
        let _thread_guard = LOCAL_KEY_FILE_LOCK.lock().map_err(|_error| {
            CredentialsError::Crypto("credential encryption key lock is poisoned".to_string())
        })?;
        if let Some(parent) = self.path.parent() {
            storage_fs::ensure_private_dir(parent)?;
        }
        let lock_path = self.path.with_extension("key.lock");
        let _process_guard = FileLock::exclusive(&lock_path)?;

        match std::fs::read_to_string(&self.path) {
            Ok(raw) => decode_key_material(&raw),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let bytes = random_array::<KEY_LEN>()?;
                let encoded = format!(
                    "{KEY_FILE_VERSION}:{}\n",
                    base64::engine::general_purpose::STANDARD.encode(bytes)
                );
                storage_fs::write_atomic(&self.path, encoded.as_bytes())?;
                warn!(
                    path = %self.path.display(),
                    "created plaintext credential encryption key; shared-Postgres deployments must provision the same KEK on every server"
                );
                Ok(CredentialEncryptionKey {
                    key_id: key_id_for_bytes(&bytes),
                    bytes,
                })
            }
            Err(error) => Err(error.into()),
        }
    }

    fn load_key(&self) -> Result<Option<CredentialEncryptionKey>, CredentialsError> {
        match std::fs::read_to_string(&self.path) {
            Ok(raw) => decode_key_material(&raw).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn resolve_key(
        &self,
        key_id: Option<&str>,
    ) -> Result<CredentialEncryptionKey, CredentialsError> {
        match key_id {
            Some(key_id) => self
                .load_key()?
                .ok_or_else(|| key_unavailable(key_id))
                .and_then(|key| verify_key_id(key, key_id)),
            None => self.load_or_create_key(),
        }
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
            return decode_key_material(&raw).map(Some);
        }
        Ok(None)
    }

    fn load_or_create_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
        if let Some(key) = self.load_key()? {
            return Ok(key);
        }
        if let Some(parent) = self.lock_path.parent() {
            storage_fs::ensure_private_dir(parent)?;
        }
        let _guard = FileLock::exclusive(&self.lock_path)?;
        if let Some(key) = self.load_key()? {
            return Ok(key);
        }
        let bytes = random_array::<KEY_LEN>()?;
        let encoded = encode_key_material(&bytes);
        self.keychain.write_key_material(&encoded)?;
        info!("created keychain credential encryption key");
        Ok(CredentialEncryptionKey {
            key_id: key_id_for_bytes(&bytes),
            bytes,
        })
    }

    fn resolve_key(
        &self,
        key_id: Option<&str>,
    ) -> Result<CredentialEncryptionKey, CredentialsError> {
        match key_id {
            Some(key_id) => self
                .load_key()?
                .ok_or_else(|| key_unavailable(key_id))
                .and_then(|key| verify_key_id(key, key_id)),
            None => self.load_or_create_key(),
        }
    }
}

impl CredentialKeyProvider for KeychainCredentialKeyProvider {
    fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
        self.resolve_key(None)
    }

    fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
        self.resolve_key(Some(key_id))
    }
}

trait CredentialEncryptionKeychain: Send + Sync {
    fn probe(&self) -> Result<(), CredentialsError>;

    fn read_key_material(&self) -> Result<Option<String>, CredentialsError>;

    fn write_key_material(&self, material: &str) -> Result<(), CredentialsError>;
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

    fn read_key_material(&self) -> Result<Option<String>, CredentialsError> {
        let entry = self.entry.clone();
        self.backend.run_native(move |backend| {
            backend.probe_native()?;
            match backend
                .entry_for(&entry.service, &entry.account)?
                .get_password()
            {
                Ok(value) => Ok(Some(value)),
                Err(keyring_core::Error::NoEntry) => Ok(None),
                Err(error) => Err(CredentialsError::Unavailable(error.to_string())),
            }
        })
    }

    fn write_key_material(&self, material: &str) -> Result<(), CredentialsError> {
        let entry = self.entry.clone();
        let material = material.to_string();
        self.backend.run_native(move |backend| {
            backend.probe_native()?;
            backend
                .entry_for(&entry.service, &entry.account)?
                .set_password(&material)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EncryptedCredentialDocument {
    pub(crate) ciphertext: Vec<u8>,
    pub(crate) nonce: Vec<u8>,
    pub(crate) wrapped_dek: Vec<u8>,
    pub(crate) wrapped_dek_nonce: Vec<u8>,
    pub(crate) key_id: String,
    pub(crate) algorithm: String,
    pub(crate) aad_version: i64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PlaintextCredentialDocument {
    version: u32,
    values: BTreeMap<String, String>,
}

pub(crate) fn encrypt_credential_values(
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    values: &BTreeMap<String, String>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<EncryptedCredentialDocument, CredentialsError> {
    let kek = key_provider.active_key()?;
    let dek = Zeroizing::new(random_array::<KEY_LEN>()?);
    let nonce = random_array::<NONCE_LEN>()?;
    let wrapped_dek_nonce = random_array::<NONCE_LEN>()?;

    let plaintext = PlaintextCredentialDocument {
        version: CREDENTIAL_DOCUMENT_VERSION,
        values: values.clone(),
    };
    let mut document_bytes = Zeroizing::new(
        serde_json::to_vec(&plaintext)
            .map_err(|error| CredentialsError::Parse(error.to_string()))?,
    );
    seal(
        &*dek,
        &nonce,
        credential_document_aad(workspace_name, source_name),
        &mut document_bytes,
    )?;

    let mut wrapped_dek = Zeroizing::new(dek.to_vec());
    seal(
        &kek.bytes,
        &wrapped_dek_nonce,
        credential_dek_aad(kek.key_id()),
        &mut wrapped_dek,
    )?;

    Ok(EncryptedCredentialDocument {
        ciphertext: std::mem::take(&mut *document_bytes),
        nonce: nonce.to_vec(),
        wrapped_dek: std::mem::take(&mut *wrapped_dek),
        wrapped_dek_nonce: wrapped_dek_nonce.to_vec(),
        key_id: kek.key_id.clone(),
        algorithm: CREDENTIAL_DOCUMENT_ALGORITHM.to_string(),
        aad_version: CREDENTIAL_DOCUMENT_AAD_VERSION,
    })
}

pub(crate) fn decrypt_credential_values(
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    document: &EncryptedCredentialDocument,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<BTreeMap<String, String>, CredentialsError> {
    let plaintext =
        decrypt_credential_document_bytes(workspace_name, source_name, document, key_provider)?;
    let decoded: PlaintextCredentialDocument = serde_json::from_slice(&plaintext)
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
    validate_document_metadata(document)?;
    let old_kek = key_provider.key(&document.key_id)?;
    let active_kek = key_provider.active_key()?;
    if old_kek.key_id == active_kek.key_id {
        return Ok(None);
    }

    let dek = unwrap_dek(document, &old_kek)?;
    let mut document_probe = Zeroizing::new(document.ciphertext.clone());
    if open(
        &*dek,
        document.nonce.as_slice(),
        credential_document_aad(workspace_name, source_name),
        document_probe.as_mut_slice(),
    )
    .is_err()
    {
        // Documents written before this rotation-safe envelope shape bound the
        // payload AAD to the KEK id. Re-encrypt those once so future rotations
        // only need to rewrap the DEK.
        let values =
            decrypt_credential_values(workspace_name, source_name, document, key_provider)?;
        return encrypt_credential_values(workspace_name, source_name, &values, key_provider)
            .map(Some);
    }

    let wrapped_dek_nonce = random_array::<NONCE_LEN>()?;
    let mut wrapped_dek = Zeroizing::new(dek.to_vec());
    seal(
        &active_kek.bytes,
        &wrapped_dek_nonce,
        credential_dek_aad(active_kek.key_id()),
        &mut wrapped_dek,
    )?;

    Ok(Some(EncryptedCredentialDocument {
        ciphertext: document.ciphertext.clone(),
        nonce: document.nonce.clone(),
        wrapped_dek: std::mem::take(&mut *wrapped_dek),
        wrapped_dek_nonce: wrapped_dek_nonce.to_vec(),
        key_id: active_kek.key_id.clone(),
        algorithm: document.algorithm.clone(),
        aad_version: document.aad_version,
    }))
}

fn decrypt_credential_document_bytes(
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    document: &EncryptedCredentialDocument,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<Zeroizing<Vec<u8>>, CredentialsError> {
    validate_document_metadata(document)?;
    let kek = key_provider.key(&document.key_id)?;
    let dek = unwrap_dek(document, &kek)?;

    let mut ciphertext = Zeroizing::new(document.ciphertext.clone());
    match open(
        &*dek,
        document.nonce.as_slice(),
        credential_document_aad(workspace_name, source_name),
        ciphertext.as_mut_slice(),
    ) {
        Ok(plaintext) => Ok(Zeroizing::new(plaintext.to_vec())),
        Err(primary_error) => {
            let mut legacy_ciphertext = Zeroizing::new(document.ciphertext.clone());
            match open(
                &*dek,
                document.nonce.as_slice(),
                legacy_credential_document_aad(workspace_name, source_name, &document.key_id),
                legacy_ciphertext.as_mut_slice(),
            ) {
                Ok(plaintext) => Ok(Zeroizing::new(plaintext.to_vec())),
                Err(_) => Err(primary_error),
            }
        }
    }
}

fn unwrap_dek(
    document: &EncryptedCredentialDocument,
    kek: &CredentialEncryptionKey,
) -> Result<Zeroizing<[u8; KEY_LEN]>, CredentialsError> {
    let mut dek = Zeroizing::new(document.wrapped_dek.clone());
    match open(
        &kek.bytes,
        document.wrapped_dek_nonce.as_slice(),
        credential_dek_aad(&document.key_id),
        dek.as_mut_slice(),
    ) {
        Ok(dek_plaintext) => validate_dek_plaintext(dek_plaintext),
        Err(primary_error) => {
            let mut legacy_dek = Zeroizing::new(document.wrapped_dek.clone());
            match open(
                &kek.bytes,
                document.wrapped_dek_nonce.as_slice(),
                legacy_credential_dek_aad(&document.key_id),
                legacy_dek.as_mut_slice(),
            ) {
                Ok(dek_plaintext) => validate_dek_plaintext(dek_plaintext),
                Err(_) => Err(primary_error),
            }
        }
    }
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

fn validate_document_metadata(
    document: &EncryptedCredentialDocument,
) -> Result<(), CredentialsError> {
    if document.algorithm != CREDENTIAL_DOCUMENT_ALGORITHM {
        return Err(CredentialsError::Crypto(format!(
            "unsupported credential encryption algorithm '{}'",
            document.algorithm
        )));
    }
    if document.aad_version != CREDENTIAL_DOCUMENT_AAD_VERSION {
        return Err(CredentialsError::Crypto(format!(
            "unsupported credential AAD version {}",
            document.aad_version
        )));
    }
    Ok(())
}

fn seal(
    key_bytes: &[u8],
    nonce_bytes: &[u8; NONCE_LEN],
    aad: Vec<u8>,
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
    aad: Vec<u8>,
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

fn credential_document_aad(workspace_name: &WorkspaceName, source_name: &SourceName) -> Vec<u8> {
    let aad_version = CREDENTIAL_DOCUMENT_AAD_VERSION.to_string();
    encode_aad_fields(
        "coral-credential-document",
        &[
            aad_version.as_str(),
            workspace_name.as_str(),
            source_name.as_str(),
            CREDENTIAL_DOCUMENT_ALGORITHM,
        ],
    )
}

fn legacy_credential_document_aad(
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    key_id: &str,
) -> Vec<u8> {
    format!(
        "coral-credential-document:v{}:{}:{}:{}:{}",
        CREDENTIAL_DOCUMENT_AAD_VERSION,
        workspace_name.as_str(),
        source_name.as_str(),
        CREDENTIAL_DOCUMENT_ALGORITHM,
        key_id
    )
    .into_bytes()
}

fn credential_dek_aad(key_id: &str) -> Vec<u8> {
    let aad_version = CREDENTIAL_DOCUMENT_AAD_VERSION.to_string();
    encode_aad_fields("coral-credential-dek", &[aad_version.as_str(), key_id])
}

fn legacy_credential_dek_aad(key_id: &str) -> Vec<u8> {
    format!("coral-credential-dek:v{CREDENTIAL_DOCUMENT_AAD_VERSION}:{key_id}").into_bytes()
}

fn encode_aad_fields(domain: &str, fields: &[&str]) -> Vec<u8> {
    let mut aad = Vec::new();
    aad.extend_from_slice(domain.as_bytes());
    aad.push(0);
    for field in fields {
        let bytes = field.as_bytes();
        aad.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        aad.extend_from_slice(bytes);
    }
    aad
}

fn random_array<const N: usize>() -> Result<[u8; N], CredentialsError> {
    let mut bytes = [0_u8; N];
    SystemRandom::new().fill(&mut bytes).map_err(|_error| {
        CredentialsError::Crypto("secure random generation failed".to_string())
    })?;
    Ok(bytes)
}

fn configured_key_env(config_file: &Path) -> Result<Option<String>, CredentialsError> {
    if !config_file.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(config_file)?;
    let config: toml::Value =
        toml::from_str(&raw).map_err(|error| CredentialsError::Parse(error.to_string()))?;
    let Some(value) = config
        .get("credentials")
        .and_then(|credentials| credentials.get("encryption_key_env"))
    else {
        return Ok(None);
    };
    value
        .as_str()
        .map(|env| Some(env.to_string()))
        .ok_or_else(|| {
            CredentialsError::Parse("[credentials].encryption_key_env must be a string".to_string())
        })
}

fn configured_key_source(
    config_file: &Path,
) -> Result<CredentialEncryptionKeySource, CredentialsError> {
    if !config_file.exists() {
        return Ok(CredentialEncryptionKeySource::Auto);
    }
    let raw = std::fs::read_to_string(config_file)?;
    let config: toml::Value =
        toml::from_str(&raw).map_err(|error| CredentialsError::Parse(error.to_string()))?;
    let Some(value) = config
        .get("credentials")
        .and_then(|credentials| credentials.get("encryption_key_source"))
    else {
        return Ok(CredentialEncryptionKeySource::Auto);
    };
    match value.as_str() {
        Some("auto") => Ok(CredentialEncryptionKeySource::Auto),
        Some("file") => Ok(CredentialEncryptionKeySource::File),
        Some("keychain") => Ok(CredentialEncryptionKeySource::Keychain),
        Some("vault") => Ok(CredentialEncryptionKeySource::Vault),
        Some(source) => Err(CredentialsError::Parse(format!(
            "unsupported [credentials].encryption_key_source '{source}'"
        ))),
        None => Err(CredentialsError::Parse(
            "[credentials].encryption_key_source must be a string".to_string(),
        )),
    }
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

fn encode_key_material(bytes: &[u8; KEY_LEN]) -> String {
    format!(
        "{KEY_FILE_VERSION}:{}",
        base64::engine::general_purpose::STANDARD.encode(bytes)
    )
}

fn decode_key_material(raw: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
    let trimmed = raw.trim();
    let Some(encoded) = trimmed.strip_prefix(&format!("{KEY_FILE_VERSION}:")) else {
        return Err(CredentialsError::Crypto(
            "unsupported credential encryption key version".to_string(),
        ));
    };
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| CredentialsError::Crypto(format!("invalid encryption key: {error}")))?;
    let bytes: [u8; KEY_LEN] = decoded.try_into().map_err(|decoded: Vec<u8>| {
        CredentialsError::Crypto(format!(
            "credential encryption key has invalid length {}",
            decoded.len()
        ))
    })?;
    Ok(CredentialEncryptionKey {
        key_id: key_id_for_bytes(&bytes),
        bytes,
    })
}

fn key_id_for_bytes(bytes: &[u8; KEY_LEN]) -> String {
    let digest = Sha256::digest(bytes);
    let hex = format!("{digest:x}");
    format!("local-file-{}", hex.get(..16).unwrap_or(hex.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex};

    use tempfile::TempDir;

    impl CredentialKeyProvider for CredentialEncryptionKey {
        fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
            Ok(self.clone())
        }

        fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
            if self.key_id() == key_id {
                Ok(self.clone())
            } else {
                Err(CredentialsError::Crypto(format!(
                    "credential encryption key '{key_id}' is unavailable"
                )))
            }
        }
    }

    #[test]
    fn encrypt_decrypt_authenticates_context_and_redacts_debug() {
        let ws = WorkspaceName::parse("acme").expect("workspace");
        let src = SourceName::parse("github").expect("source");
        let provider = CredentialEncryptionKey::from_static_bytes_for_test([7; KEY_LEN]);
        let values = BTreeMap::from([("token".to_string(), "s3cr3t".to_string())]);

        let document =
            encrypt_credential_values(&ws, &src, &values, &provider).expect("encrypt credentials");
        assert_eq!(
            decrypt_credential_values(&ws, &src, &document, &provider).expect("decrypt"),
            values
        );

        let mut tampered = document.clone();
        *tampered.ciphertext.first_mut().expect("ciphertext byte") ^= 1;
        decrypt_credential_values(&ws, &src, &tampered, &provider)
            .expect_err("tampered ciphertext should fail");
        let mut tampered = document.clone();
        *tampered.wrapped_dek.first_mut().expect("wrapped DEK byte") ^= 1;
        decrypt_credential_values(&ws, &src, &tampered, &provider)
            .expect_err("tampered wrapped DEK should fail");
        let other_ws = WorkspaceName::parse("other").expect("workspace");
        decrypt_credential_values(&other_ws, &src, &document, &provider)
            .expect_err("wrong workspace should fail");
        let other_src = SourceName::parse("slack").expect("source");
        decrypt_credential_values(&ws, &other_src, &document, &provider)
            .expect_err("wrong source should fail");
        let mismatch = CredentialEncryptionKey::from_static_bytes_for_test([8; KEY_LEN]);
        decrypt_credential_values(&ws, &src, &document, &mismatch)
            .expect_err("wrong key should fail");

        let debug = format!("{provider:?}");
        assert!(debug.contains(provider.key_id()));
        assert!(!debug.contains("bytes"));
        assert!(!debug.contains("[7, 7"));
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
        write_key_source_config(&layout, "file");
        LocalFileCredentialKeyProvider::new(&layout)
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
            KeychainCredentialKeyProvider::with_keychain_for_test(&layout, keychain.clone()),
        );

        provider.active_key().expect("keychain key");

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
            KeychainCredentialKeyProvider::with_keychain_for_test(&layout, keychain),
        );

        provider.active_key().expect("fallback file key");

        assert!(
            layout.credential_encryption_key_file().exists(),
            "unavailable keychain should fall back to the plaintext key file"
        );
    }

    #[test]
    fn auto_keeps_existing_file_key_even_when_keychain_available() {
        let (_temp, layout) = temp_layout();
        write_key_source_config(&layout, "file");
        let file_key = LocalFileCredentialKeyProvider::new(&layout)
            .active_key()
            .expect("file key");
        std::fs::remove_file(layout.config_file()).expect("remove explicit file config");
        let keychain = FakeKeychain::available();
        let provider = LocalFileCredentialKeyProvider::with_keychain_for_test(
            &layout,
            KeychainCredentialKeyProvider::with_keychain_for_test(&layout, keychain.clone()),
        );

        let selected = provider.active_key().expect("auto key");

        assert_eq!(selected, file_key);
        assert!(
            keychain.material().is_none(),
            "existing file key should keep auto mode from silently switching to keychain"
        );
    }

    #[test]
    fn explicit_keychain_source_fails_closed_when_unavailable() {
        let (_temp, layout) = temp_layout();
        write_key_source_config(&layout, "keychain");
        let provider = LocalFileCredentialKeyProvider::with_keychain_for_test(
            &layout,
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

    fn write_key_source_config(layout: &AppStateLayout, source: &str) {
        std::fs::write(
            layout.config_file(),
            format!("version = 1\n\n[credentials]\nencryption_key_source = \"{source}\"\n"),
        )
        .expect("write config");
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

        fn read_key_material(&self) -> Result<Option<String>, CredentialsError> {
            self.probe()?;
            Ok(self.material())
        }

        fn write_key_material(&self, material: &str) -> Result<(), CredentialsError> {
            self.probe()?;
            *self.material.lock().map_err(|error| {
                CredentialsError::Unavailable(format!("fake keychain lock poisoned: {error}"))
            })? = Some(material.to_string());
            Ok(())
        }
    }
}
