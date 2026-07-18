use std::sync::Arc;
use std::time::Duration;

use coral_spec::parse_identity_manifest_yaml;

use super::IdentityManager;
use crate::bootstrap::AppError;
use crate::credentials::CredentialsError;
use crate::credentials::encryption::{CredentialEncryptionKey, CredentialKeyProvider};
use crate::identities::crypto::{IdentityDocumentBinding, decrypt_identity_document};
use crate::identities::model::{IdentityName, IdentityOwner};
use crate::identity::{Principal, PrincipalKind};
use crate::identity_specs::identity_spec_fingerprint;
use crate::state::db::{
    CoralDb, DbRepos, IdentityDocumentRecord, IdentityRecord, IdentitySpecKey,
    set_identity_document_version,
};

struct TestKeyProvider(Vec<CredentialEncryptionKey>);

impl CredentialKeyProvider for TestKeyProvider {
    fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
        self.0
            .last()
            .cloned()
            .ok_or_else(|| CredentialsError::Unavailable("missing test key".to_string()))
    }

    fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
        self.0
            .iter()
            .find(|key| key.key_id() == key_id)
            .cloned()
            .ok_or_else(|| CredentialsError::Unavailable("missing test key".to_string()))
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "shared SQLite/Postgres manager contract"
)]
pub(crate) async fn assert_user_global_fixed_token_create_contract(db: &Arc<CoralDb>) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fixed_a = format!("fixeda_{suffix}");
    let fixed_b = format!("fixedb_{suffix}");
    let oauth = format!("oauth_{suffix}");
    let race = format!("race_{suffix}");
    let missing = format!("missing_{suffix}");
    for (name, yaml) in [
        (&fixed_a, fixed_manifest(&fixed_a, "a")),
        (&fixed_b, fixed_manifest(&fixed_b, "b")),
        (&oauth, oauth_manifest(&oauth)),
        (&race, fixed_manifest(&race, "before")),
    ] {
        put_spec(db, name, &yaml).await;
    }

    let principal =
        Principal::parse(&format!("user-{suffix}"), PrincipalKind::User).expect("principal");
    let owner = IdentityOwner::for_user(principal.clone());
    let unavailable = Arc::new(TestKeyProvider(Vec::new()));
    let unavailable_manager = IdentityManager::new(db.clone(), unavailable);
    for (name, spec, token, expected) in [
        ("blank", fixed_a.as_str(), " \t ", "invalid"),
        ("missing", missing.as_str(), "token", "missing"),
        ("oauth", oauth.as_str(), "token", "invalid"),
        ("no_key", fixed_a.as_str(), "token", "credentials"),
    ] {
        let identity_name = format!("{name}_{suffix}");
        let error = unavailable_manager
            .create_or_replace_user_fixed_token(&principal, &identity_name, spec, token.to_string())
            .await
            .expect_err("invalid creation must fail");
        match expected {
            "missing" => assert!(matches!(
                error,
                AppError::IdentitySpecNotFound { scope, .. } if scope == "global"
            )),
            "credentials" => assert!(matches!(error, AppError::Credentials(_))),
            _ => assert!(matches!(error, AppError::InvalidInput(_))),
        }
        assert_eq!(load_pair(db, &owner, &identity_name).await, (None, None));
    }

    let old_key = CredentialEncryptionKey::from_static_bytes_for_test([61; 32]);
    let old_provider = Arc::new(TestKeyProvider(vec![old_key.clone()]));
    let manager = IdentityManager::new(db.clone(), old_provider.clone());
    let identity = format!("primary_{suffix}");
    let created = manager
        .create_or_replace_user_fixed_token(
            &principal,
            &identity,
            &fixed_a,
            "  alpha-token  ".to_string(),
        )
        .await
        .expect("create fixed-token identity");
    assert_reference(&created, &fixed_a, "a");
    let created_pair = load_pair(db, &owner, &identity).await;
    assert_eq!(created_pair.0.as_ref(), Some(&created));
    let created_document = created_pair.1.as_ref().expect("created document");
    assert_eq!(created_document.document_version, 1);
    assert_eq!(created_document.envelope.key_id, old_key.key_id());
    assert_material(
        &created,
        created_document,
        "alpha-token",
        old_provider.as_ref(),
    );

    let new_key = CredentialEncryptionKey::from_static_bytes_for_test([62; 32]);
    let rotated_provider = Arc::new(TestKeyProvider(vec![old_key, new_key.clone()]));
    let rotated = IdentityManager::new(db.clone(), rotated_provider.clone());
    let replaced = rotated
        .create_or_replace_user_fixed_token(
            &principal,
            &identity,
            &fixed_b,
            " beta-token ".to_string(),
        )
        .await
        .expect("replace fixed-token identity");
    assert_reference(&replaced, &fixed_b, "b");
    let replaced_pair = load_pair(db, &owner, &identity).await;
    let replaced_document = replaced_pair.1.as_ref().expect("replaced document");
    assert_eq!(
        replaced.created_at_unix_nanos,
        created.created_at_unix_nanos
    );
    assert_eq!(replaced_document.document_version, 2);
    assert_eq!(
        replaced_document.created_at_unix_nanos,
        created_document.created_at_unix_nanos
    );
    assert_eq!(replaced_document.envelope.key_id, new_key.key_id());
    assert_material(
        &replaced,
        replaced_document,
        "beta-token",
        rotated_provider.as_ref(),
    );

    let identity_name = IdentityName::parse(&identity).expect("identity name");
    set_identity_document_version(db, &owner, &identity_name, i64::MAX).await;
    let before_failure = load_pair(db, &owner, &identity).await;
    let error = rotated
        .create_or_replace_user_fixed_token(
            &principal,
            &identity,
            &fixed_a,
            "rollback-token".to_string(),
        )
        .await
        .expect_err("exhausted document version must fail");
    assert!(
        matches!(error, AppError::FailedPrecondition(detail) if detail.contains("version is exhausted"))
    );
    assert_eq!(load_pair(db, &owner, &identity).await, before_failure);

    let conflict_name = format!("conflict_{suffix}");
    let conflict_gate = Arc::new(tokio::sync::Barrier::new(2));
    let left = rotated
        .clone()
        .with_before_upsert_gate(conflict_gate.clone());
    let right = rotated.clone().with_before_upsert_gate(conflict_gate);
    let (left, right) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            left.create_or_replace_user_fixed_token(
                &principal,
                &conflict_name,
                &fixed_a,
                "left-token".to_string(),
            ),
            right.create_or_replace_user_fixed_token(
                &principal,
                &conflict_name,
                &fixed_b,
                "right-token".to_string(),
            ),
        )
    })
    .await
    .expect("conflicting writes must not deadlock");
    let left = left.expect("left write");
    let right = right.expect("right write");
    assert_reference(&left, &fixed_a, "a");
    assert_reference(&right, &fixed_b, "b");
    let conflict = load_pair(db, &owner, &conflict_name).await;
    let conflict_identity = conflict.0.as_ref().expect("conflict identity");
    let conflict_document = conflict.1.as_ref().expect("conflict document");
    assert_eq!(conflict_document.document_version, 2);
    let (winning, winning_token) = match conflict_identity.spec_reference.key().name() {
        name if name == fixed_a.as_str() => (&left, "left-token"),
        name if name == fixed_b.as_str() => (&right, "right-token"),
        name => panic!("unexpected winning spec {name}"),
    };
    assert_eq!(conflict_identity, winning);
    assert_material(
        conflict_identity,
        conflict_document,
        winning_token,
        rotated_provider.as_ref(),
    );

    let prepared = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let gated = rotated
        .clone()
        .with_before_write_gate(prepared.clone(), resume.clone());
    let raced_name = format!("raced_{suffix}");
    let raced = tokio::time::timeout(Duration::from_secs(10), async {
        let create = gated.create_or_replace_user_fixed_token(
            &principal,
            &raced_name,
            &race,
            "race-token".to_string(),
        );
        let replace = async {
            prepared.wait().await;
            put_spec(db, &race, &fixed_manifest(&race, "after")).await;
            resume.wait().await;
        };
        tokio::join!(create, replace).0
    })
    .await
    .expect("spec replacement race must not deadlock")
    .expect("raced create");
    assert_reference(&raced, &race, "after");
    let raced_pair = load_pair(db, &owner, &raced_name).await;
    assert_eq!(raced_pair.0.as_ref(), Some(&raced));
    assert_eq!(
        raced_pair
            .1
            .as_ref()
            .expect("raced document")
            .document_version,
        1
    );
    assert_material(
        &raced,
        raced_pair.1.as_ref().expect("raced document"),
        "race-token",
        rotated_provider.as_ref(),
    );

    let mut cleanup = db.begin().await.expect("begin identity cleanup");
    let conflict_identity_name = IdentityName::parse(&conflict_name).expect("conflict name");
    let raced_identity_name = IdentityName::parse(&raced_name).expect("raced name");
    for name in [
        &identity_name,
        &conflict_identity_name,
        &raced_identity_name,
    ] {
        assert!(
            cleanup
                .identities()
                .delete(&owner, name)
                .await
                .expect("delete test identity")
        );
    }
    for spec_name in [&fixed_a, &fixed_b, &oauth, &race] {
        assert!(
            cleanup
                .identity_specs()
                .delete(&IdentitySpecKey::global(spec_name).expect("cleanup spec key"))
                .await
                .expect("delete test identity spec")
        );
    }
    cleanup.commit().await.expect("commit identity cleanup");
}

async fn put_spec(db: &CoralDb, name: &str, yaml: &str) {
    let manifest = parse_identity_manifest_yaml(yaml).expect("identity manifest");
    let key = IdentitySpecKey::global(name).expect("global spec key");
    let mut tx = db.begin().await.expect("begin spec write");
    tx.identity_specs()
        .upsert(&key, &manifest, yaml, 1)
        .await
        .expect("upsert identity spec");
    tx.commit().await.expect("commit identity spec");
}

async fn load_pair(
    db: &CoralDb,
    owner: &IdentityOwner,
    identity_name: &str,
) -> (Option<IdentityRecord>, Option<IdentityDocumentRecord>) {
    let name = IdentityName::parse(identity_name).expect("identity name");
    let mut session = db;
    let identity = session
        .identities()
        .get(owner, &name)
        .await
        .expect("identity");
    let document = session
        .identity_documents()
        .get(owner, &name)
        .await
        .expect("identity document");
    (identity, document)
}

fn assert_reference(record: &IdentityRecord, spec_name: &str, revision: &str) {
    let manifest = parse_identity_manifest_yaml(&fixed_manifest(spec_name, revision))
        .expect("expected manifest");
    assert_eq!(
        record.spec_reference.key(),
        &IdentitySpecKey::global(spec_name).expect("global spec key")
    );
    assert_eq!(
        record.spec_reference.fingerprint(),
        identity_spec_fingerprint(&manifest).expect("fingerprint")
    );
    assert_eq!(record.spec_reference.issuer(), format!("issuer_{revision}"));
    assert_eq!(record.spec_reference.identity_type(), "fixed_token");
}

fn assert_material(
    identity: &IdentityRecord,
    document: &IdentityDocumentRecord,
    token: &str,
    provider: &dyn CredentialKeyProvider,
) {
    let binding =
        IdentityDocumentBinding::new(&document.owner, &document.name, &identity.spec_reference)
            .expect("document binding");
    let kek = provider
        .key(&document.envelope.key_id)
        .expect("stored envelope key");
    let values =
        decrypt_identity_document(&binding, &document.envelope, &kek).expect("decrypt token");
    assert_eq!(values.get("TOKEN").map(String::as_str), Some(token));
    assert!(
        !document
            .envelope
            .ciphertext
            .windows(token.len())
            .any(|window| window == token.as_bytes())
    );
}

fn fixed_manifest(name: &str, revision: &str) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: {revision}\ndescription: {revision}\nissuer: issuer_{revision}\ntype: fixed_token\naudience: {{host: api.example.com}}\n"
    )
}

fn oauth_manifest(name: &str) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: oauth\ndescription: oauth\nissuer: oauth_issuer\ntype: oauth\naudience: {{host: api.example.com}}\noauth:\n  method:\n    flow: {{type: device_code}}\n    endpoints: {{device_authorization_url: 'https://provider.example.com/device', token_url: 'https://provider.example.com/token'}}\n    client: {{id: {{default: demo-client}}}}\n"
    )
}
