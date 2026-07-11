use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};
use std::thread;

use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey};
use tempfile::tempdir;
use zeroize::Zeroizing;

use super::CredentialsError;
use super::encryption::{
    CREDENTIAL_DOCUMENT_AAD_VERSION, CREDENTIAL_DOCUMENT_ALGORITHM, CredentialEncryptionKey,
    CredentialKeyProvider, LocalFileCredentialKeyProvider, decrypt_credential_values,
    encrypt_credential_values, open_envelope_document, rewrap_credential_document,
    rewrap_envelope_document, seal_envelope_document,
};
use crate::sources::SourceName;
use crate::state::AppStateLayout;
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
fn encrypt_decrypt_authenticates_context_and_redacts_key_debug() {
    let workspace = WorkspaceName::parse("acme").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let provider = StaticKeyProvider {
        key: CredentialEncryptionKey::from_static_bytes_for_test([7; 32]),
    };
    let values = BTreeMap::from([("token".to_string(), "s3cr3t".to_string())]);

    let document = encrypt_credential_values(&workspace, &source, &values, &provider)
        .expect("encrypt credentials");
    assert_eq!(
        decrypt_credential_values(&workspace, &source, &document, &provider).expect("decrypt"),
        values
    );

    let mut tampered = document.clone();
    *tampered.ciphertext.first_mut().expect("ciphertext byte") ^= 1;
    decrypt_credential_values(&workspace, &source, &tampered, &provider)
        .expect_err("tampered ciphertext should fail");
    let mut tampered = document.clone();
    *tampered.wrapped_dek.first_mut().expect("wrapped DEK byte") ^= 1;
    decrypt_credential_values(&workspace, &source, &tampered, &provider)
        .expect_err("tampered wrapped DEK should fail");
    let other_workspace = WorkspaceName::parse("other").expect("workspace");
    decrypt_credential_values(&other_workspace, &source, &document, &provider)
        .expect_err("wrong workspace should fail");
    let other_source = SourceName::parse("slack").expect("source");
    decrypt_credential_values(&workspace, &other_source, &document, &provider)
        .expect_err("wrong source should fail");
    let mismatch = StaticKeyProvider {
        key: CredentialEncryptionKey::from_static_bytes_for_test([8; 32]),
    };
    decrypt_credential_values(&workspace, &source, &document, &mismatch)
        .expect_err("wrong key should fail");

    let debug = format!("{:?}", provider.key);
    assert!(debug.contains(provider.key.key_id()));
    assert!(!debug.contains("bytes"));
    assert!(!debug.contains("[7, 7"));
}

#[test]
fn credential_document_aad_disambiguates_colon_bearing_identities() {
    let workspace = WorkspaceName::parse("a:b").expect("workspace");
    let source = SourceName::parse("c").expect("source");
    let replay_workspace = WorkspaceName::parse("a").expect("workspace");
    let replay_source = SourceName::parse("b:c").expect("source");
    let provider = StaticKeyProvider {
        key: CredentialEncryptionKey::from_static_bytes_for_test([10; 32]),
    };
    let encrypted = encrypt_credential_values(
        &workspace,
        &source,
        &BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]),
        &provider,
    )
    .expect("encrypt");

    let error = decrypt_credential_values(&replay_workspace, &replay_source, &encrypted, &provider)
        .expect_err("ambiguous colon-delimited identity should not decrypt");

    assert!(error.to_string().contains("open failed"));
}

#[test]
fn credential_document_rejects_tampered_nonces() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let provider = StaticKeyProvider {
        key: CredentialEncryptionKey::from_static_bytes_for_test([11; 32]),
    };
    let encrypted = encrypt_credential_values(
        &workspace,
        &source,
        &BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]),
        &provider,
    )
    .expect("encrypt");

    let mut tampered = encrypted.clone();
    *tampered.nonce.first_mut().expect("payload nonce") ^= 0x01;
    assert_open_failed(
        &decrypt_credential_values(&workspace, &source, &tampered, &provider)
            .expect_err("tampered payload nonce should fail authentication"),
    );

    let mut tampered = encrypted;
    *tampered
        .wrapped_dek_nonce
        .first_mut()
        .expect("wrapped DEK nonce") ^= 0x01;
    assert_open_failed(
        &decrypt_credential_values(&workspace, &source, &tampered, &provider)
            .expect_err("tampered wrapped DEK nonce should fail authentication"),
    );
}

#[test]
fn credential_document_rejects_key_id_aad_mismatch_even_when_key_resolves() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let original_key_bytes = [12; 32];
    let mutated_key_bytes = [14; 32];
    let original_key = CredentialEncryptionKey::from_static_bytes_for_test(original_key_bytes);
    let mutated_key = CredentialEncryptionKey::from_static_bytes_for_test(mutated_key_bytes);
    let provider = RotatingKeyProvider {
        active: original_key.clone(),
        keys: vec![original_key, mutated_key.clone()],
    };
    let mut encrypted = encrypt_credential_values(
        &workspace,
        &source,
        &BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]),
        &provider,
    )
    .expect("encrypt");
    let original_key_id = encrypted.key_id.clone();
    let dek = open_for_test(
        &original_key_bytes,
        encrypted.wrapped_dek_nonce.as_slice(),
        current_dek_aad_for_test(&original_key_id),
        &encrypted.wrapped_dek,
    );
    let mismatched_nonce = [6; 12];
    encrypted.wrapped_dek = seal_for_test(
        &mutated_key_bytes,
        mismatched_nonce,
        current_dek_aad_for_test(&original_key_id),
        &dek,
    );
    encrypted.wrapped_dek_nonce = mismatched_nonce.to_vec();
    encrypted.key_id = mutated_key.key_id().to_string();
    provider
        .key(&encrypted.key_id)
        .expect("mutated key id should resolve");

    assert_open_failed(
        &decrypt_credential_values(&workspace, &source, &encrypted, &provider)
            .expect_err("mismatched key-id AAD should fail authentication"),
    );
}

#[test]
fn credential_document_rejects_aad_version_mismatch() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let provider = StaticKeyProvider {
        key: CredentialEncryptionKey::from_static_bytes_for_test([15; 32]),
    };
    let mut encrypted = encrypt_credential_values(
        &workspace,
        &source,
        &BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]),
        &provider,
    )
    .expect("encrypt");
    encrypted.aad_version = CREDENTIAL_DOCUMENT_AAD_VERSION + 1;

    let error = decrypt_credential_values(&workspace, &source, &encrypted, &provider)
        .expect_err("unsupported AAD version should fail");

    assert!(
        error
            .to_string()
            .contains("unsupported credential AAD version"),
        "unexpected error: {error}"
    );
}

#[test]
fn decrypt_accepts_legacy_colon_delimited_dek_aad() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let key_bytes = [17; 32];
    let provider = StaticKeyProvider {
        key: CredentialEncryptionKey::from_static_bytes_for_test(key_bytes),
    };
    let values = BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]);
    let mut encrypted =
        encrypt_credential_values(&workspace, &source, &values, &provider).expect("encrypt");
    let key_id = encrypted.key_id.clone();
    let dek = open_for_test(
        &key_bytes,
        encrypted.wrapped_dek_nonce.as_slice(),
        current_dek_aad_for_test(&key_id),
        &encrypted.wrapped_dek,
    );
    let legacy_nonce = [3; 12];
    encrypted.wrapped_dek = seal_for_test(
        &key_bytes,
        legacy_nonce,
        legacy_dek_aad_for_test(&key_id),
        &dek,
    );
    encrypted.wrapped_dek_nonce = legacy_nonce.to_vec();

    assert_eq!(
        decrypt_credential_values(&workspace, &source, &encrypted, &provider)
            .expect("legacy DEK AAD should decrypt"),
        values
    );
}

#[test]
fn credential_document_rewrap_changes_kek_without_reencrypting_payload() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let old_key_bytes = [19; 32];
    let new_key_bytes = [23; 32];
    let old_key = CredentialEncryptionKey::from_static_bytes_for_test(old_key_bytes);
    let new_key = CredentialEncryptionKey::from_static_bytes_for_test(new_key_bytes);
    let old_provider = RotatingKeyProvider {
        active: old_key.clone(),
        keys: vec![old_key.clone()],
    };
    let rotating_provider = RotatingKeyProvider {
        active: new_key.clone(),
        keys: vec![old_key, new_key.clone()],
    };
    let values = BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]);
    let encrypted =
        encrypt_credential_values(&workspace, &source, &values, &old_provider).expect("encrypt");

    let rewrapped = rewrap_credential_document(&workspace, &source, &encrypted, &rotating_provider)
        .expect("rewrap")
        .expect("stale key should rewrap");

    assert_eq!(rewrapped.key_id, new_key.key_id());
    assert_eq!(rewrapped.ciphertext, encrypted.ciphertext);
    assert_eq!(rewrapped.nonce, encrypted.nonce);
    assert_ne!(rewrapped.wrapped_dek, encrypted.wrapped_dek);
    assert_ne!(rewrapped.wrapped_dek_nonce, encrypted.wrapped_dek_nonce);
    assert!(
        try_open_for_test(
            &old_key_bytes,
            rewrapped.wrapped_dek_nonce.as_slice(),
            current_dek_aad_for_test(&rewrapped.key_id),
            &rewrapped.wrapped_dek,
        )
        .is_err(),
        "old KEK should not unwrap the rewrapped DEK"
    );
    assert_eq!(
        decrypt_credential_values(&workspace, &source, &rewrapped, &rotating_provider)
            .expect("decrypt rewrapped"),
        values
    );
}

#[test]
fn credential_document_rewrap_migrates_legacy_payload_aad() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let old_key_bytes = [29; 32];
    let old_key = CredentialEncryptionKey::from_static_bytes_for_test(old_key_bytes);
    let new_key = CredentialEncryptionKey::from_static_bytes_for_test([31; 32]);
    let old_provider = RotatingKeyProvider {
        active: old_key.clone(),
        keys: vec![old_key.clone()],
    };
    let rotating_provider = RotatingKeyProvider {
        active: new_key.clone(),
        keys: vec![old_key, new_key.clone()],
    };
    let values = BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]);
    let mut legacy =
        encrypt_credential_values(&workspace, &source, &values, &old_provider).expect("encrypt");
    let key_id = legacy.key_id.clone();
    let dek: [u8; 32] = open_for_test(
        &old_key_bytes,
        legacy.wrapped_dek_nonce.as_slice(),
        current_dek_aad_for_test(&key_id),
        &legacy.wrapped_dek,
    )
    .try_into()
    .expect("DEK length");
    let plaintext = serde_json::to_vec(&serde_json::json!({
        "version": 1,
        "values": values,
    }))
    .expect("serialize legacy document");
    let legacy_nonce = [5; 12];
    legacy.ciphertext = seal_for_test(
        &dek,
        legacy_nonce,
        legacy_document_aad_for_test(&workspace, &source, &key_id),
        &plaintext,
    );
    legacy.nonce = legacy_nonce.to_vec();

    assert_eq!(
        decrypt_credential_values(&workspace, &source, &legacy, &old_provider)
            .expect("legacy payload AAD should decrypt"),
        values
    );
    let migrated = rewrap_credential_document(&workspace, &source, &legacy, &rotating_provider)
        .expect("rewrap legacy document")
        .expect("stale key should migrate");
    assert_eq!(migrated.key_id, new_key.key_id());
    assert_ne!(migrated.ciphertext, legacy.ciphertext);
    assert_ne!(migrated.nonce, legacy.nonce);
    assert_eq!(
        decrypt_credential_values(&workspace, &source, &migrated, &rotating_provider)
            .expect("decrypt migrated document"),
        values
    );
}

#[test]
fn shared_envelope_helpers_round_trip_and_rewrap_current_documents() {
    let old_key = CredentialEncryptionKey::from_static_bytes_for_test([37; 32]);
    let new_key = CredentialEncryptionKey::from_static_bytes_for_test([41; 32]);
    let old_provider = RotatingKeyProvider {
        active: old_key.clone(),
        keys: vec![old_key.clone()],
    };
    let rotating_provider = RotatingKeyProvider {
        active: new_key.clone(),
        keys: vec![old_key, new_key.clone()],
    };
    let aad = b"test-envelope-aad".to_vec();
    let plaintext = b"envelope payload".to_vec();

    let encrypted = seal_envelope_document(
        aad.clone(),
        Zeroizing::new(plaintext.clone()),
        &old_provider,
    )
    .expect("seal envelope");
    assert_eq!(
        open_envelope_document(&encrypted, aad.clone(), &old_provider)
            .expect("open envelope")
            .as_slice(),
        plaintext
    );
    assert_open_failed(
        &open_envelope_document(&encrypted, b"wrong-aad".to_vec(), &old_provider)
            .expect_err("wrong AAD should fail authentication"),
    );

    let rewrapped = rewrap_envelope_document(&encrypted, aad.clone(), &rotating_provider)
        .expect("rewrap envelope")
        .expect("stale key should rewrap");
    assert_eq!(rewrapped.key_id, new_key.key_id());
    assert_eq!(rewrapped.ciphertext, encrypted.ciphertext);
    assert_eq!(rewrapped.nonce, encrypted.nonce);
    assert_ne!(rewrapped.wrapped_dek, encrypted.wrapped_dek);
    assert_ne!(rewrapped.wrapped_dek_nonce, encrypted.wrapped_dek_nonce);
    assert_eq!(
        open_envelope_document(&rewrapped, aad.clone(), &rotating_provider)
            .expect("open rewrapped envelope")
            .as_slice(),
        plaintext
    );
    assert!(
        rewrap_envelope_document(&rewrapped, aad, &rotating_provider)
            .expect("rewrap current envelope")
            .is_none(),
        "active KEK should not produce another rewrap"
    );
}

#[test]
fn local_file_key_provider_creates_and_reuses_private_key_file() {
    let temp = tempdir().expect("temp dir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
    layout.ensure().expect("ensure layout");
    let provider = local_file_key_provider(&layout);

    let first = provider.active_key().expect("first key");
    let second = provider.active_key().expect("second key");

    assert_eq!(first, second);
    assert!(
        layout.credential_encryption_key_file().exists(),
        "provider should create durable key material outside the DB"
    );
}

#[test]
fn local_file_key_provider_serializes_concurrent_first_use_creation() {
    let temp = tempdir().expect("temp dir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
    layout.ensure().expect("ensure layout");
    let provider = local_file_key_provider(&layout);
    let thread_count = 32;
    let barrier = Arc::new(Barrier::new(thread_count));

    let handles: Vec<_> = (0..thread_count)
        .map(|_| {
            let provider = provider.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                provider.active_key().map(|key| key.key_id().to_string())
            })
        })
        .collect();

    let key_ids: Vec<_> = handles
        .into_iter()
        .map(|handle| {
            handle
                .join()
                .expect("thread should not panic")
                .expect("key")
        })
        .collect();
    let first_key_id = key_ids.first().expect("key id");

    assert!(
        key_ids.iter().all(|key_id| key_id == first_key_id),
        "all concurrent first-use callers should observe the single persisted key"
    );
    assert_eq!(
        local_file_key_provider(&layout)
            .active_key()
            .expect("persisted key")
            .key_id(),
        first_key_id
    );
}

#[test]
fn decrypt_rejects_unknown_key_id() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let provider = StaticKeyProvider {
        key: CredentialEncryptionKey::from_static_bytes_for_test([13; 32]),
    };
    let mut encrypted = encrypt_credential_values(
        &workspace,
        &source,
        &BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]),
        &provider,
    )
    .expect("encrypt");
    encrypted.key_id = "missing-key".to_string();

    let error = decrypt_credential_values(&workspace, &source, &encrypted, &provider)
        .expect_err("missing KEK should fail");

    assert!(error.to_string().contains("missing test key"));
}

fn assert_open_failed(error: &CredentialsError) {
    assert!(
        error.to_string().contains("open failed"),
        "unexpected error: {error}"
    );
}

fn local_file_key_provider(layout: &AppStateLayout) -> LocalFileCredentialKeyProvider {
    LocalFileCredentialKeyProvider::new(layout, None)
}

fn current_dek_aad_for_test(key_id: &str) -> Vec<u8> {
    let aad_version = CREDENTIAL_DOCUMENT_AAD_VERSION.to_string();
    encode_aad_fields_for_test("coral-credential-dek", &[aad_version.as_str(), key_id])
}

fn legacy_dek_aad_for_test(key_id: &str) -> Vec<u8> {
    format!("coral-credential-dek:v{CREDENTIAL_DOCUMENT_AAD_VERSION}:{key_id}").into_bytes()
}

fn legacy_document_aad_for_test(
    workspace: &WorkspaceName,
    source: &SourceName,
    key_id: &str,
) -> Vec<u8> {
    format!(
        "coral-credential-document:v{}:{}:{}:{}:{}",
        CREDENTIAL_DOCUMENT_AAD_VERSION,
        workspace.as_str(),
        source.as_str(),
        CREDENTIAL_DOCUMENT_ALGORITHM,
        key_id
    )
    .into_bytes()
}

fn encode_aad_fields_for_test(domain: &str, fields: &[&str]) -> Vec<u8> {
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

fn seal_for_test(
    key_bytes: &[u8; 32],
    nonce_bytes: [u8; 12],
    aad: Vec<u8>,
    plaintext: &[u8],
) -> Vec<u8> {
    let key = LessSafeKey::new(UnboundKey::new(&aead::AES_256_GCM, key_bytes).expect("test key"));
    let mut in_out = plaintext.to_vec();
    key.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(nonce_bytes),
        Aad::from(aad),
        &mut in_out,
    )
    .expect("seal");
    in_out
}

fn open_for_test(
    key_bytes: &[u8; 32],
    nonce_bytes: &[u8],
    aad: Vec<u8>,
    ciphertext: &[u8],
) -> Vec<u8> {
    let key = LessSafeKey::new(UnboundKey::new(&aead::AES_256_GCM, key_bytes).expect("test key"));
    let mut in_out = ciphertext.to_vec();
    key.open_in_place(
        Nonce::assume_unique_for_key(nonce_bytes.try_into().expect("nonce")),
        Aad::from(aad),
        &mut in_out,
    )
    .expect("open")
    .to_vec()
}

fn try_open_for_test(
    key_bytes: &[u8; 32],
    nonce_bytes: &[u8],
    aad: Vec<u8>,
    ciphertext: &[u8],
) -> Result<Vec<u8>, ()> {
    let key =
        LessSafeKey::new(UnboundKey::new(&aead::AES_256_GCM, key_bytes).map_err(|_error| ())?);
    let mut in_out = ciphertext.to_vec();
    let nonce = nonce_bytes.try_into().map_err(|_error| ())?;
    key.open_in_place(
        Nonce::assume_unique_for_key(nonce),
        Aad::from(aad),
        &mut in_out,
    )
    .map(|plaintext| plaintext.to_vec())
    .map_err(|_error| ())
}
