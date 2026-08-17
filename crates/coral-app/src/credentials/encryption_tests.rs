use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};
use std::thread;

use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey};
use tempfile::tempdir;
use zeroize::Zeroizing;

use super::CredentialsError;
use super::encryption::test_support::{RotatingKeyProvider, StaticKeyProvider};
use super::encryption::{
    CREDENTIAL_DOCUMENT_BINDING_VERSION, ENVELOPE_DOCUMENT_ALGORITHM, EnvelopeContext,
    EnvelopeEncryptionKey, EnvelopeKeyProvider, LocalFileEnvelopeKeyProvider,
    decrypt_credential_values, encrypt_credential_values, open_envelope_document,
    rewrap_credential_document, rewrap_envelope_document, seal_envelope_document,
};
use crate::encrypted_document::EncryptedEnvelopeDocument;
use crate::sources::SourceName;
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

#[test]
fn encrypt_decrypt_authenticates_context_and_redacts_key_debug() {
    let workspace = WorkspaceName::parse("acme").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let provider = StaticKeyProvider {
        key: EnvelopeEncryptionKey::from_static_bytes_for_test([7; 32]),
    };
    let values = BTreeMap::from([("token".to_string(), "s3cr3t".to_string())]);

    let document = encrypt_credential_values(&workspace, &source, &values, &provider)
        .expect("encrypt credentials");
    assert_eq!(
        document.binding_version,
        CREDENTIAL_DOCUMENT_BINDING_VERSION
    );
    assert_eq!(
        decrypt_values_with_provider(&workspace, &source, &document, &provider).expect("decrypt"),
        values
    );

    let mut tampered = document.clone();
    *tampered.ciphertext.first_mut().expect("ciphertext byte") ^= 1;
    decrypt_values_with_provider(&workspace, &source, &tampered, &provider)
        .expect_err("tampered ciphertext should fail");
    let mut tampered = document.clone();
    *tampered.wrapped_dek.first_mut().expect("wrapped DEK byte") ^= 1;
    decrypt_values_with_provider(&workspace, &source, &tampered, &provider)
        .expect_err("tampered wrapped DEK should fail");
    let other_workspace = WorkspaceName::parse("other").expect("workspace");
    decrypt_values_with_provider(&other_workspace, &source, &document, &provider)
        .expect_err("wrong workspace should fail");
    let other_source = SourceName::parse("slack").expect("source");
    decrypt_values_with_provider(&workspace, &other_source, &document, &provider)
        .expect_err("wrong source should fail");
    let mismatch = StaticKeyProvider {
        key: EnvelopeEncryptionKey::from_static_bytes_for_test([8; 32]),
    };
    decrypt_values_with_provider(&workspace, &source, &document, &mismatch)
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
        key: EnvelopeEncryptionKey::from_static_bytes_for_test([10; 32]),
    };
    let encrypted = encrypt_credential_values(
        &workspace,
        &source,
        &BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]),
        &provider,
    )
    .expect("encrypt");

    let error =
        decrypt_values_with_provider(&replay_workspace, &replay_source, &encrypted, &provider)
            .expect_err("ambiguous colon-delimited identity should not decrypt");

    assert!(error.to_string().contains("open failed"));
}

#[test]
fn credential_document_rejects_tampered_nonces() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let provider = StaticKeyProvider {
        key: EnvelopeEncryptionKey::from_static_bytes_for_test([11; 32]),
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
        &decrypt_values_with_provider(&workspace, &source, &tampered, &provider)
            .expect_err("tampered payload nonce should fail authentication"),
    );

    let mut tampered = encrypted;
    *tampered
        .wrapped_dek_nonce
        .first_mut()
        .expect("wrapped DEK nonce") ^= 0x01;
    assert_open_failed(
        &decrypt_values_with_provider(&workspace, &source, &tampered, &provider)
            .expect_err("tampered wrapped DEK nonce should fail authentication"),
    );
}

#[test]
fn credential_document_rejects_key_id_aad_mismatch_even_when_key_resolves() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let original_key_bytes = [12; 32];
    let mutated_key_bytes = [14; 32];
    let original_key = EnvelopeEncryptionKey::from_static_bytes_for_test(original_key_bytes);
    let mutated_key = EnvelopeEncryptionKey::from_static_bytes_for_test(mutated_key_bytes);
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
        credential_dek_aad_for_test(
            &workspace,
            &source,
            encrypted.binding_version,
            &original_key_id,
        ),
        &encrypted.wrapped_dek,
    );
    let mismatched_nonce = [6; 12];
    encrypted.wrapped_dek = seal_for_test(
        &mutated_key_bytes,
        mismatched_nonce,
        credential_dek_aad_for_test(
            &workspace,
            &source,
            encrypted.binding_version,
            &original_key_id,
        ),
        &dek,
    );
    encrypted.wrapped_dek_nonce = mismatched_nonce.to_vec();
    encrypted.key_id = mutated_key.key_id().to_string();
    provider
        .key(&encrypted.key_id)
        .expect("mutated key id should resolve");

    assert_open_failed(
        &decrypt_values_with_provider(&workspace, &source, &encrypted, &provider)
            .expect_err("mismatched key-id AAD should fail authentication"),
    );
}

#[test]
fn credential_document_rejects_binding_version_mismatch() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let provider = StaticKeyProvider {
        key: EnvelopeEncryptionKey::from_static_bytes_for_test([15; 32]),
    };
    let mut encrypted = encrypt_credential_values(
        &workspace,
        &source,
        &BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]),
        &provider,
    )
    .expect("encrypt");
    encrypted.binding_version = CREDENTIAL_DOCUMENT_BINDING_VERSION + 1;

    let error = decrypt_values_with_provider(&workspace, &source, &encrypted, &provider)
        .expect_err("unsupported binding version should fail");

    assert!(
        error
            .to_string()
            .contains("unsupported credential binding version"),
        "unexpected error: {error}"
    );
}

#[test]
fn decrypt_accepts_legacy_colon_delimited_dek_aad() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let key_bytes = [17; 32];
    let provider = StaticKeyProvider {
        key: EnvelopeEncryptionKey::from_static_bytes_for_test(key_bytes),
    };
    let values = BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]);
    let mut encrypted = encrypt_credential_v1_for_test(&workspace, &source, &values, &provider);
    let key_id = encrypted.key_id.clone();
    let dek = open_for_test(
        &key_bytes,
        encrypted.wrapped_dek_nonce.as_slice(),
        credential_dek_aad_for_test(&workspace, &source, encrypted.binding_version, &key_id),
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
        decrypt_values_with_provider(&workspace, &source, &encrypted, &provider)
            .expect("legacy DEK AAD should decrypt"),
        values
    );
}

#[test]
fn decrypt_accepts_legacy_length_prefixed_dek_aad() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let key_bytes = [18; 32];
    let provider = StaticKeyProvider {
        key: EnvelopeEncryptionKey::from_static_bytes_for_test(key_bytes),
    };
    let values = BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]);
    let mut encrypted = encrypt_credential_v1_for_test(&workspace, &source, &values, &provider);
    let key_id = encrypted.key_id.clone();
    let dek = open_for_test(
        &key_bytes,
        encrypted.wrapped_dek_nonce.as_slice(),
        credential_dek_aad_for_test(&workspace, &source, encrypted.binding_version, &key_id),
        &encrypted.wrapped_dek,
    );
    let legacy_nonce = [4; 12];
    encrypted.wrapped_dek = seal_for_test(
        &key_bytes,
        legacy_nonce,
        legacy_length_prefixed_dek_aad_for_test(&key_id),
        &dek,
    );
    encrypted.wrapped_dek_nonce = legacy_nonce.to_vec();

    assert_eq!(
        decrypt_values_with_provider(&workspace, &source, &encrypted, &provider)
            .expect("legacy length-prefixed DEK AAD should decrypt"),
        values
    );
}

#[test]
fn credential_document_rewrap_changes_kek_without_reencrypting_payload() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let old_key_bytes = [19; 32];
    let new_key_bytes = [23; 32];
    let old_key = EnvelopeEncryptionKey::from_static_bytes_for_test(old_key_bytes);
    let new_key = EnvelopeEncryptionKey::from_static_bytes_for_test(new_key_bytes);
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
            credential_dek_aad_for_test(
                &workspace,
                &source,
                rewrapped.binding_version,
                &rewrapped.key_id,
            ),
            &rewrapped.wrapped_dek,
        )
        .is_err(),
        "old KEK should not unwrap the rewrapped DEK"
    );
    assert_eq!(
        decrypt_values_with_provider(&workspace, &source, &rewrapped, &rotating_provider)
            .expect("decrypt rewrapped"),
        values
    );
}

#[test]
fn credential_document_same_key_rewrap_authenticates_payload_context() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let other_workspace = WorkspaceName::parse("other").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let provider = StaticKeyProvider {
        key: EnvelopeEncryptionKey::from_static_bytes_for_test([27; 32]),
    };
    let encrypted = encrypt_credential_values(
        &workspace,
        &source,
        &BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]),
        &provider,
    )
    .expect("encrypt");

    assert_open_failed(
        &rewrap_credential_document(&other_workspace, &source, &encrypted, &provider)
            .expect_err("same-key rewrap must authenticate context"),
    );
    let mut tampered = encrypted;
    *tampered.ciphertext.first_mut().expect("ciphertext") ^= 1;
    assert_open_failed(
        &rewrap_credential_document(&workspace, &source, &tampered, &provider)
            .expect_err("same-key rewrap must authenticate ciphertext"),
    );
}

#[test]
fn credential_v1_rewrap_migrates_to_v2_with_same_key() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let provider = StaticKeyProvider {
        key: EnvelopeEncryptionKey::from_static_bytes_for_test([28; 32]),
    };
    let values = BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]);
    let v1 = encrypt_credential_v1_for_test(&workspace, &source, &values, &provider);

    let migrated = rewrap_credential_document(&workspace, &source, &v1, &provider)
        .expect("rewrap v1")
        .expect("v1 must be resealed");

    assert_eq!(
        migrated.binding_version,
        CREDENTIAL_DOCUMENT_BINDING_VERSION
    );
    assert_eq!(migrated.key_id, v1.key_id);
    assert_ne!(migrated.ciphertext, v1.ciphertext);
    assert_eq!(
        decrypt_values_with_provider(&workspace, &source, &migrated, &provider)
            .expect("decrypt migrated credential"),
        values
    );
}

#[test]
fn credential_document_rewrap_migrates_legacy_payload_aad() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let old_key_bytes = [29; 32];
    let old_key = EnvelopeEncryptionKey::from_static_bytes_for_test(old_key_bytes);
    let new_key = EnvelopeEncryptionKey::from_static_bytes_for_test([31; 32]);
    let old_provider = RotatingKeyProvider {
        active: old_key.clone(),
        keys: vec![old_key.clone()],
    };
    let rotating_provider = RotatingKeyProvider {
        active: new_key.clone(),
        keys: vec![old_key, new_key.clone()],
    };
    let values = BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]);
    let mut legacy = encrypt_credential_v1_for_test(&workspace, &source, &values, &old_provider);
    let key_id = legacy.key_id.clone();
    let dek: [u8; 32] = open_for_test(
        &old_key_bytes,
        legacy.wrapped_dek_nonce.as_slice(),
        credential_dek_aad_for_test(&workspace, &source, legacy.binding_version, &key_id),
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

    let migrated = rewrap_credential_document(&workspace, &source, &legacy, &rotating_provider)
        .expect("rewrap legacy document")
        .expect("stale key should migrate");
    assert_eq!(migrated.key_id, new_key.key_id());
    assert_eq!(
        migrated.binding_version,
        CREDENTIAL_DOCUMENT_BINDING_VERSION
    );
    assert_ne!(migrated.ciphertext, legacy.ciphertext);
    assert_ne!(migrated.nonce, legacy.nonce);
    assert_eq!(
        decrypt_values_with_provider(&workspace, &source, &migrated, &rotating_provider)
            .expect("decrypt migrated document"),
        values
    );
}

#[test]
fn envelope_context_rejects_nul_in_domain() {
    let Err(error) = EnvelopeContext::new("test-envelope\0other", 1, &["workspace"]) else {
        panic!("NUL-containing envelope domain should fail");
    };
    assert!(error.to_string().contains("must not contain NUL"));
}

#[test]
fn wrapped_dek_authenticates_full_envelope_context() {
    const BINDING_VERSION: i64 = 7;

    let old_key_bytes = [37; 32];
    let provider = StaticKeyProvider {
        key: EnvelopeEncryptionKey::from_static_bytes_for_test(old_key_bytes),
    };
    let context =
        EnvelopeContext::new("test-envelope", BINDING_VERSION, &["workspace", "document"])
            .expect("envelope context");
    let wrong_context = EnvelopeContext::new(
        "test-envelope",
        BINDING_VERSION,
        &["other-workspace", "document"],
    )
    .expect("wrong envelope context");
    let encrypted = seal_envelope_document(
        &context,
        Zeroizing::new(b"envelope payload".to_vec()),
        &provider,
    )
    .expect("seal envelope");

    assert_eq!(
        open_for_test(
            &old_key_bytes,
            encrypted.wrapped_dek_nonce.as_slice(),
            context.dek_aad(&encrypted.key_id),
            &encrypted.wrapped_dek,
        )
        .len(),
        32
    );
    assert!(
        try_open_for_test(
            &old_key_bytes,
            encrypted.wrapped_dek_nonce.as_slice(),
            wrong_context.dek_aad(&encrypted.key_id),
            &encrypted.wrapped_dek,
        )
        .is_err(),
        "wrapped DEK must authenticate the full envelope context"
    );
}

#[test]
fn shared_envelope_context_authenticates_open_and_rewrap() {
    const BINDING_VERSION: i64 = 7;

    let old_key = EnvelopeEncryptionKey::from_static_bytes_for_test([37; 32]);
    let new_key = EnvelopeEncryptionKey::from_static_bytes_for_test([41; 32]);
    let old_provider = RotatingKeyProvider {
        active: old_key.clone(),
        keys: vec![old_key.clone()],
    };
    let rotating_provider = RotatingKeyProvider {
        active: new_key.clone(),
        keys: vec![old_key, new_key.clone()],
    };
    let context =
        EnvelopeContext::new("test-envelope", BINDING_VERSION, &["workspace", "document"])
            .expect("envelope context");
    let plaintext = b"envelope payload".to_vec();

    let encrypted =
        seal_envelope_document(&context, Zeroizing::new(plaintext.clone()), &old_provider)
            .expect("seal envelope");
    assert_eq!(encrypted.binding_version, BINDING_VERSION);
    assert_eq!(
        open_with_provider(&context, &encrypted, &old_provider)
            .expect("open envelope")
            .as_slice(),
        plaintext
    );
    let wrong_context = EnvelopeContext::new(
        "test-envelope",
        BINDING_VERSION,
        &["other-workspace", "document"],
    )
    .expect("wrong envelope context");
    assert_open_failed(
        &open_with_provider(&wrong_context, &encrypted, &old_provider)
            .expect_err("wrong binding should fail authentication"),
    );
    let wrong_version =
        EnvelopeContext::new("test-envelope", 1, &["workspace", "document"]).expect("context");
    assert!(
        open_with_provider(&wrong_version, &encrypted, &old_provider)
            .expect_err("wrong expected binding version should fail")
            .to_string()
            .contains("binding version 7 does not match context version 1")
    );
    let Err(error) = EnvelopeContext::new("test-envelope", 0, &["workspace"]) else {
        panic!("nonpositive binding version should fail");
    };
    assert!(error.to_string().contains("must be positive"));
    let mut unsupported = encrypted.clone();
    unsupported.algorithm = "unsupported".to_string();
    assert!(
        open_with_provider(&context, &unsupported, &old_provider)
            .expect_err("unsupported algorithm should fail")
            .to_string()
            .contains("unsupported envelope encryption algorithm")
    );
    let mut rebound = encrypted.clone();
    rebound.binding_version += 1;
    let rebound_context = EnvelopeContext::new(
        "test-envelope",
        rebound.binding_version,
        &["workspace", "document"],
    )
    .expect("rebound context");
    assert_open_failed(
        &open_with_provider(&rebound_context, &rebound, &old_provider)
            .expect_err("stored version must also bind the wrapped DEK"),
    );

    let rewrapped = rewrap_envelope_document(&context, &encrypted, &rotating_provider)
        .expect("rewrap envelope")
        .expect("stale key should rewrap");
    assert_eq!(rewrapped.key_id, new_key.key_id());
    assert_eq!(rewrapped.ciphertext, encrypted.ciphertext);
    assert_eq!(rewrapped.nonce, encrypted.nonce);
    assert_ne!(rewrapped.wrapped_dek, encrypted.wrapped_dek);
    assert_ne!(rewrapped.wrapped_dek_nonce, encrypted.wrapped_dek_nonce);
    assert_eq!(
        open_with_provider(&context, &rewrapped, &rotating_provider)
            .expect("open rewrapped envelope")
            .as_slice(),
        plaintext
    );
    assert!(
        rewrap_envelope_document(&context, &rewrapped, &rotating_provider)
            .expect("rewrap current envelope")
            .is_none()
    );
    assert_open_failed(
        &rewrap_envelope_document(&wrong_context, &rewrapped, &rotating_provider)
            .expect_err("same-key rewrap must still authenticate binding"),
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
        layout.envelope_encryption_key_file().exists(),
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
        key: EnvelopeEncryptionKey::from_static_bytes_for_test([13; 32]),
    };
    let mut encrypted = encrypt_credential_values(
        &workspace,
        &source,
        &BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]),
        &provider,
    )
    .expect("encrypt");
    encrypted.key_id = "missing-key".to_string();

    let error = decrypt_values_with_provider(&workspace, &source, &encrypted, &provider)
        .expect_err("missing KEK should fail");

    assert!(error.to_string().contains("missing test key"));
}

#[test]
fn open_rejects_a_key_the_document_does_not_name() {
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let source = SourceName::parse("github").expect("source");
    let stored_key = EnvelopeEncryptionKey::from_static_bytes_for_test([43; 32]);
    let other_key = EnvelopeEncryptionKey::from_static_bytes_for_test([47; 32]);
    let provider = StaticKeyProvider {
        key: stored_key.clone(),
    };
    let values = BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]);
    let encrypted =
        encrypt_credential_values(&workspace, &source, &values, &provider).expect("encrypt");

    let error = decrypt_credential_values(&workspace, &source, &encrypted, &other_key)
        .expect_err("a key the document does not name must be rejected");

    assert!(
        error.to_string().contains("does not match document key"),
        "unexpected error: {error}"
    );
    assert_eq!(
        decrypt_credential_values(&workspace, &source, &encrypted, &stored_key).expect("decrypt"),
        values
    );
}

fn assert_open_failed(error: &CredentialsError) {
    assert!(
        error.to_string().contains("open failed"),
        "unexpected error: {error}"
    );
}

/// Resolve the KEK a document names, then decrypt with it.
fn decrypt_values_with_provider(
    workspace: &WorkspaceName,
    source: &SourceName,
    document: &EncryptedEnvelopeDocument,
    key_provider: &dyn EnvelopeKeyProvider,
) -> Result<BTreeMap<String, String>, CredentialsError> {
    let kek = key_provider.key(&document.key_id)?;
    decrypt_credential_values(workspace, source, document, &kek)
}

/// Resolve the KEK a document names, then open it.
fn open_with_provider(
    context: &EnvelopeContext,
    document: &EncryptedEnvelopeDocument,
    key_provider: &dyn EnvelopeKeyProvider,
) -> Result<Zeroizing<Vec<u8>>, CredentialsError> {
    let kek = key_provider.key(&document.key_id)?;
    open_envelope_document(context, document, &kek)
}

fn local_file_key_provider(layout: &AppStateLayout) -> LocalFileEnvelopeKeyProvider {
    LocalFileEnvelopeKeyProvider::new(layout, None)
}

fn encrypt_credential_v1_for_test(
    workspace: &WorkspaceName,
    source: &SourceName,
    values: &BTreeMap<String, String>,
    key_provider: &dyn EnvelopeKeyProvider,
) -> super::encryption::EncryptedCredentialDocument {
    let context = EnvelopeContext::new(
        "coral-credential-document",
        1,
        &[workspace.as_str(), source.as_str()],
    )
    .expect("v1 credential context");
    let plaintext = serde_json::to_vec(&serde_json::json!({
        "version": 1,
        "values": values,
    }))
    .expect("serialize v1 credential document");
    seal_envelope_document(&context, Zeroizing::new(plaintext), key_provider)
        .expect("encrypt v1 credential document")
}

fn credential_dek_aad_for_test(
    workspace: &WorkspaceName,
    source: &SourceName,
    binding_version: i64,
    key_id: &str,
) -> Vec<u8> {
    let binding_version = binding_version.to_string();
    encode_aad_fields_for_test(
        "coral-credential-document",
        &[
            binding_version.as_str(),
            workspace.as_str(),
            source.as_str(),
            ENVELOPE_DOCUMENT_ALGORITHM,
            key_id,
        ],
    )
}

fn legacy_length_prefixed_dek_aad_for_test(key_id: &str) -> Vec<u8> {
    encode_aad_fields_for_test("coral-credential-dek", &["1", key_id])
}

fn legacy_dek_aad_for_test(key_id: &str) -> Vec<u8> {
    format!("coral-credential-dek:v1:{key_id}").into_bytes()
}

fn legacy_document_aad_for_test(
    workspace: &WorkspaceName,
    source: &SourceName,
    key_id: &str,
) -> Vec<u8> {
    format!(
        "coral-credential-document:v{}:{}:{}:{}:{}",
        1,
        workspace.as_str(),
        source.as_str(),
        ENVELOPE_DOCUMENT_ALGORITHM,
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
