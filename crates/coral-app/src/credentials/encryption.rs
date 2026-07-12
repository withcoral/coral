//! Application-level envelope encryption for DB-backed credential material.

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

use base64::Engine as _;
use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom as _, SystemRandom};
use sha2::{Digest as _, Sha256};
use tracing::warn;
use zeroize::{Zeroize as _, Zeroizing};

use super::CredentialsError;
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

pub(crate) trait CredentialKeyProvider: Send + Sync {
    fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError>;

    fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError>;
}

/// Resolves an explicitly supplied key or falls back to a key file scoped to
/// this app-state config directory. Callers own config and environment resolution.
#[derive(Debug, Clone)]
pub(crate) struct LocalFileCredentialKeyProvider {
    path: PathBuf,
    provided_key: Option<CredentialEncryptionKey>,
    allow_local_file_fallback: bool,
}

impl LocalFileCredentialKeyProvider {
    pub(crate) fn new(
        layout: &AppStateLayout,
        provided_key: Option<CredentialEncryptionKey>,
    ) -> Self {
        Self::from_layout(layout, provided_key, true)
    }

    pub(crate) fn configured_key_only(
        layout: &AppStateLayout,
        provided_key: Option<CredentialEncryptionKey>,
    ) -> Self {
        Self::from_layout(layout, provided_key, false)
    }

    fn from_layout(
        layout: &AppStateLayout,
        provided_key: Option<CredentialEncryptionKey>,
        allow_local_file_fallback: bool,
    ) -> Self {
        Self {
            path: layout.credential_encryption_key_file(),
            provided_key,
            allow_local_file_fallback,
        }
    }

    fn load_key(&self) -> Result<Option<CredentialEncryptionKey>, CredentialsError> {
        match storage_fs::read_to_string_private(&self.path) {
            Ok(raw) => {
                let raw = Zeroizing::new(raw);
                CredentialEncryptionKey::from_encoded_material(raw.as_str()).map(Some)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
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
        Ok(CredentialEncryptionKey {
            key_id: key_id_for_bytes(&bytes),
            bytes: *bytes,
        })
    }
}

impl CredentialKeyProvider for LocalFileCredentialKeyProvider {
    fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
        if let Some(key) = &self.provided_key {
            return Ok(key.clone());
        }
        if self.allow_local_file_fallback {
            self.load_or_create_key()
        } else {
            Err(configured_key_required())
        }
    }

    fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
        if let Some(key) = &self.provided_key
            && key.key_id == key_id
        {
            return Ok(key.clone());
        }
        if self.allow_local_file_fallback {
            if let Some(key) = self.load_key()?
                && key.key_id == key_id
            {
                return Ok(key);
            }
        } else if self.provided_key.is_none() {
            return Err(configured_key_required());
        }
        Err(CredentialsError::Crypto(format!(
            "credential encryption key '{key_id}' is unavailable"
        )))
    }
}

pub(crate) fn configured_key_required() -> CredentialsError {
    CredentialsError::Unavailable(
        "Postgres identity material requires [credentials].encryption_key_env to provide a shared credential encryption key".to_string(),
    )
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

/// Shared envelope-encrypted document layout for credential and identity data.
pub(crate) type EncryptedEnvelopeDocument = EncryptedCredentialDocument;

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
    seal_envelope_document(
        credential_document_aad(workspace_name, source_name),
        document_bytes,
        key_provider,
    )
}

pub(crate) fn decrypt_credential_values(
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    document: &EncryptedCredentialDocument,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<BTreeMap<String, String>, CredentialsError> {
    let plaintext =
        decrypt_credential_document_bytes(workspace_name, source_name, document, key_provider)?;
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
    validate_document_metadata(document, CREDENTIAL_DOCUMENT_AAD_VERSION)?;
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

    rewrap_dek(document, &active_kek, &dek, CREDENTIAL_DOCUMENT_AAD_VERSION)
}

fn decrypt_credential_document_bytes(
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    document: &EncryptedCredentialDocument,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<Zeroizing<Vec<u8>>, CredentialsError> {
    validate_document_metadata(document, CREDENTIAL_DOCUMENT_AAD_VERSION)?;
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
    match unwrap_current_dek(document, kek, CREDENTIAL_DOCUMENT_AAD_VERSION) {
        Ok(dek) => Ok(dek),
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

fn unwrap_current_dek(
    document: &EncryptedEnvelopeDocument,
    kek: &CredentialEncryptionKey,
    aad_version: i64,
) -> Result<Zeroizing<[u8; KEY_LEN]>, CredentialsError> {
    let mut dek = Zeroizing::new(document.wrapped_dek.clone());
    open(
        &kek.bytes,
        document.wrapped_dek_nonce.as_slice(),
        envelope_dek_aad(aad_version, &document.key_id),
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
    aad_version: i64,
) -> Result<Option<EncryptedEnvelopeDocument>, CredentialsError> {
    let wrapped_dek_nonce = random_array::<NONCE_LEN>()?;
    let mut wrapped_dek = Zeroizing::new(dek.to_vec());
    seal(
        &active_kek.bytes,
        &wrapped_dek_nonce,
        envelope_dek_aad(aad_version, active_kek.key_id()),
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

fn validate_document_metadata(
    document: &EncryptedCredentialDocument,
    expected_aad_version: i64,
) -> Result<(), CredentialsError> {
    if document.algorithm != CREDENTIAL_DOCUMENT_ALGORITHM {
        return Err(CredentialsError::Crypto(format!(
            "unsupported credential encryption algorithm '{}'",
            document.algorithm
        )));
    }
    if document.aad_version != expected_aad_version {
        return Err(CredentialsError::Crypto(format!(
            "unsupported credential AAD version {}",
            document.aad_version
        )));
    }
    Ok(())
}

/// Seal serialized plaintext with a random DEK and wrap that DEK with the active KEK.
pub(crate) fn seal_envelope_document(
    document_aad: Vec<u8>,
    document_bytes: Zeroizing<Vec<u8>>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<EncryptedEnvelopeDocument, CredentialsError> {
    seal_envelope_document_with_aad_version(
        CREDENTIAL_DOCUMENT_AAD_VERSION,
        document_aad,
        document_bytes,
        key_provider,
    )
}

/// Seal serialized plaintext under a caller-selected envelope AAD version.
pub(crate) fn seal_envelope_document_with_aad_version(
    aad_version: i64,
    document_aad: Vec<u8>,
    mut document_bytes: Zeroizing<Vec<u8>>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<EncryptedEnvelopeDocument, CredentialsError> {
    let kek = key_provider.active_key()?;
    let dek = Zeroizing::new(random_array::<KEY_LEN>()?);
    let nonce = random_array::<NONCE_LEN>()?;
    let wrapped_dek_nonce = random_array::<NONCE_LEN>()?;

    seal(&*dek, &nonce, document_aad, &mut document_bytes)?;

    let mut wrapped_dek = Zeroizing::new(dek.to_vec());
    seal(
        &kek.bytes,
        &wrapped_dek_nonce,
        envelope_dek_aad(aad_version, kek.key_id()),
        &mut wrapped_dek,
    )?;

    Ok(EncryptedCredentialDocument {
        ciphertext: std::mem::take(&mut *document_bytes),
        nonce: nonce.to_vec(),
        wrapped_dek: std::mem::take(&mut *wrapped_dek),
        wrapped_dek_nonce: wrapped_dek_nonce.to_vec(),
        key_id: kek.key_id.clone(),
        algorithm: CREDENTIAL_DOCUMENT_ALGORITHM.to_string(),
        aad_version,
    })
}

/// Open a current-format envelope document with the supplied payload AAD.
pub(crate) fn open_envelope_document(
    document: &EncryptedEnvelopeDocument,
    document_aad: Vec<u8>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<Zeroizing<Vec<u8>>, CredentialsError> {
    open_envelope_document_with_aad_version(
        CREDENTIAL_DOCUMENT_AAD_VERSION,
        document,
        document_aad,
        key_provider,
    )
}

/// Open an envelope document only when its AAD version matches the caller's expectation.
pub(crate) fn open_envelope_document_with_aad_version(
    expected_aad_version: i64,
    document: &EncryptedEnvelopeDocument,
    document_aad: Vec<u8>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<Zeroizing<Vec<u8>>, CredentialsError> {
    validate_document_metadata(document, expected_aad_version)?;
    let kek = key_provider.key(&document.key_id)?;
    let dek = unwrap_current_dek(document, &kek, expected_aad_version)?;

    let mut ciphertext = Zeroizing::new(document.ciphertext.clone());
    open(
        &*dek,
        document.nonce.as_slice(),
        document_aad,
        ciphertext.as_mut_slice(),
    )
    .map(|plaintext| Zeroizing::new(plaintext.to_vec()))
}

/// Rewrap a current-format envelope document when its KEK is stale.
pub(crate) fn rewrap_envelope_document(
    document: &EncryptedEnvelopeDocument,
    document_aad: Vec<u8>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<Option<EncryptedEnvelopeDocument>, CredentialsError> {
    rewrap_envelope_document_inner(
        CREDENTIAL_DOCUMENT_AAD_VERSION,
        document,
        document_aad,
        key_provider,
        false,
    )
}

/// Rewrap an envelope document only when its AAD version matches the caller's expectation.
pub(crate) fn rewrap_envelope_document_with_aad_version(
    expected_aad_version: i64,
    document: &EncryptedEnvelopeDocument,
    document_aad: Vec<u8>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<Option<EncryptedEnvelopeDocument>, CredentialsError> {
    rewrap_envelope_document_inner(
        expected_aad_version,
        document,
        document_aad,
        key_provider,
        true,
    )
}

fn rewrap_envelope_document_inner(
    expected_aad_version: i64,
    document: &EncryptedEnvelopeDocument,
    document_aad: Vec<u8>,
    key_provider: &dyn CredentialKeyProvider,
    authenticate_current: bool,
) -> Result<Option<EncryptedEnvelopeDocument>, CredentialsError> {
    validate_document_metadata(document, expected_aad_version)?;
    let old_kek = key_provider.key(&document.key_id)?;
    let active_kek = key_provider.active_key()?;
    if !authenticate_current && old_kek.key_id == active_kek.key_id {
        return Ok(None);
    }

    let dek = unwrap_current_dek(document, &old_kek, expected_aad_version)?;
    let mut document_probe = Zeroizing::new(document.ciphertext.clone());
    open(
        &*dek,
        document.nonce.as_slice(),
        document_aad,
        document_probe.as_mut_slice(),
    )?;

    if old_kek.key_id == active_kek.key_id {
        return Ok(None);
    }

    rewrap_dek(document, &active_kek, &dek, expected_aad_version)
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

fn envelope_dek_aad(aad_version: i64, key_id: &str) -> Vec<u8> {
    let aad_version = aad_version.to_string();
    encode_aad_fields("coral-credential-dek", &[aad_version.as_str(), key_id])
}

fn legacy_credential_dek_aad(key_id: &str) -> Vec<u8> {
    format!("coral-credential-dek:v{CREDENTIAL_DOCUMENT_AAD_VERSION}:{key_id}").into_bytes()
}

/// Encode an AAD domain and ordered fields using length-prefixed values.
pub(crate) fn encode_aad_fields(domain: &str, fields: &[&str]) -> Vec<u8> {
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

fn key_id_for_bytes(bytes: &[u8; KEY_LEN]) -> String {
    let digest = Sha256::digest(bytes);
    let hex = format!("{digest:x}");
    format!("local-file-{}", hex.get(..16).unwrap_or(hex.as_str()))
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use tempfile::tempdir;

    use super::{
        CredentialEncryptionKey, CredentialKeyProvider, KEY_FILE_VERSION, KEY_LEN,
        LocalFileCredentialKeyProvider,
    };
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
        let provider = LocalFileCredentialKeyProvider::new(&layout, Some(key));

        let first = provider.active_key().expect("provided key");
        let second = provider.key(first.key_id()).expect("provided key by id");

        assert_eq!(first, second);
        assert!(!layout.credential_encryption_key_file().exists());
    }

    #[test]
    fn missing_key_lookup_does_not_create_a_local_key_file() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let provider = LocalFileCredentialKeyProvider::new(&layout, None);

        let error = provider.key("missing-key").expect_err("missing key");

        assert!(error.to_string().contains("is unavailable"));
        assert!(!layout.credential_encryption_key_file().exists());
    }

    #[test]
    fn provided_key_keeps_existing_file_key_available_for_rewrap() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let file_key = LocalFileCredentialKeyProvider::new(&layout, None)
            .active_key()
            .expect("file key");
        let provided_key = CredentialEncryptionKey::from_static_bytes_for_test([9_u8; KEY_LEN]);
        let provider = LocalFileCredentialKeyProvider::new(&layout, Some(provided_key.clone()));

        assert_eq!(provider.active_key().expect("provided key"), provided_key);
        assert_eq!(provider.key(file_key.key_id()).expect("file key"), file_key);
    }
}
