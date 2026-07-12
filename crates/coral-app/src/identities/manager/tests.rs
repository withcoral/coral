use std::sync::Arc;
use std::time::Duration;

use coral_spec::parse_identity_manifest_yaml;
use tempfile::tempdir;

use super::IdentityManager;
use crate::bootstrap::AppError;
use crate::credentials::CredentialsError;
use crate::credentials::encryption::{
    CredentialEncryptionKey, CredentialKeyProvider, EncryptedEnvelopeDocument,
};
use crate::identities::model::{IdentityName, IdentityOwner};
use crate::identity::{UserPrincipal, decrypt_identity_document};
use crate::identity_specs::identity_spec_fingerprint;
use crate::state::db::{
    CoralDb, DbRepos, IdentityDocumentRecord, IdentityRecord, IdentitySpecKey, IdentitySpecWrite,
    ResolvedDatabaseConfig,
};
use crate::workspaces::WorkspaceName;

struct TestKeyProvider(Vec<CredentialEncryptionKey>);

impl CredentialKeyProvider for TestKeyProvider {
    fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
        self.0
            .last()
            .cloned()
            .ok_or_else(|| CredentialsError::Crypto("missing test key".to_string()))
    }

    fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
        self.0
            .iter()
            .find(|key| key.key_id() == key_id)
            .cloned()
            .ok_or_else(|| CredentialsError::Crypto("missing test key".to_string()))
    }
}

#[tokio::test]
async fn sqlite_user_global_fixed_token_manager_contract() {
    let temp = tempdir().expect("temp dir");
    let db = Arc::new(
        CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite"),
    );
    db.migrate().await.expect("migrate sqlite");
    assert_user_global_fixed_token_manager_contract(&db).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "shared SQLite/Postgres manager contract"
)]
pub(crate) async fn assert_user_global_fixed_token_manager_contract(db: &Arc<CoralDb>) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let spec_a = format!("fixed_a_{suffix}");
    let spec_b = format!("fixed_b_{suffix}");
    let oauth = format!("oauth_{suffix}");
    let workspace_only = format!("workspace_only_{suffix}");
    let race = format!("race_{suffix}");
    let workspace = WorkspaceName::parse(&format!("work{suffix}")).expect("workspace");
    let workspace_key =
        IdentitySpecKey::workspace(workspace.clone(), &workspace_only).expect("workspace key");
    let mut tx = db.begin().await.expect("begin seed");
    tx.workspaces()
        .ensure(workspace.as_str(), 1)
        .await
        .expect("seed workspace");
    tx.commit().await.expect("commit workspace");
    for (key, yaml) in [
        (
            IdentitySpecKey::global(&spec_a).unwrap(),
            fixed_manifest(&spec_a, "a"),
        ),
        (
            IdentitySpecKey::global(&spec_b).unwrap(),
            fixed_manifest(&spec_b, "b"),
        ),
        (
            IdentitySpecKey::global(&oauth).unwrap(),
            oauth_manifest(&oauth),
        ),
        (workspace_key, fixed_manifest(&workspace_only, "workspace")),
        (
            IdentitySpecKey::global(&race).unwrap(),
            fixed_manifest(&race, "before"),
        ),
    ] {
        put_spec(db, &key, &yaml).await;
    }

    let old_key = CredentialEncryptionKey::from_static_bytes_for_test([71; 32]);
    let old_provider = Arc::new(TestKeyProvider(vec![old_key.clone()]));
    let manager = IdentityManager::new(db.clone(), old_provider.clone());
    let principal_a = UserPrincipal::for_user(&format!("a-{suffix}")).unwrap();
    let principal_b = UserPrincipal::for_user(&format!("b-{suffix}")).unwrap();
    let owner_a = IdentityOwner::for_user(principal_a.clone());
    let owner_b = IdentityOwner::for_user(principal_b.clone());
    let identity = format!("shared-{suffix}");
    assert!(matches!(
        manager
            .create_or_replace_user_fixed_token(&principal_a, "missing", &workspace_only, "x".into())
            .await,
        Err(AppError::IdentitySpecNotFound { scope, .. }) if scope == "global"
    ));
    assert!(matches!(
        manager
            .create_or_replace_user_fixed_token(&principal_a, "wrong", &oauth, "x".into())
            .await,
        Err(AppError::InvalidInput(_))
    ));
    assert!(manager.list_for_owner(&owner_a).await.unwrap().is_empty());

    let created_a = manager
        .create_or_replace_user_fixed_token(
            &principal_a,
            &identity,
            &spec_a,
            "  alpha-token  ".into(),
        )
        .await
        .expect("create owner A");
    let created_b = manager
        .create_or_replace_user_fixed_token(&principal_b, &identity, &spec_a, " beta-token ".into())
        .await
        .expect("create owner B");
    assert_reference(&created_a, &spec_a, "a");
    assert_reference(&created_b, &spec_a, "a");
    let before_a = load_pair(db, &owner_a, &identity).await;
    let before_b = load_pair(db, &owner_b, &identity).await;
    assert_material(
        before_a.1.as_ref().unwrap(),
        &owner_a,
        "alpha-token",
        old_provider.as_ref(),
    );
    assert_material(
        before_b.1.as_ref().unwrap(),
        &owner_b,
        "beta-token",
        old_provider.as_ref(),
    );
    assert!(matches!(
        manager
            .create_or_replace_user_fixed_token(&principal_a, &identity, &spec_b, " \t ".into())
            .await,
        Err(AppError::InvalidInput(_))
    ));
    assert!(load_pair(db, &owner_a, &identity).await == before_a);

    let new_key = CredentialEncryptionKey::from_static_bytes_for_test([72; 32]);
    let rotated_provider = Arc::new(TestKeyProvider(vec![old_key, new_key.clone()]));
    let rotated = IdentityManager::new(db.clone(), rotated_provider.clone());
    let replaced = rotated
        .create_or_replace_user_fixed_token(
            &principal_a,
            &identity,
            &spec_b,
            " gamma-token ".into(),
        )
        .await
        .expect("replace owner A");
    assert_reference(&replaced, &spec_b, "b");
    let after_a = load_pair(db, &owner_a, &identity).await;
    let after_document = after_a.1.as_ref().unwrap();
    assert_eq!(
        replaced.created_at_unix_nanos,
        created_a.created_at_unix_nanos
    );
    assert_eq!(after_document.document_version, 2);
    assert_eq!(
        after_document.created_at_unix_nanos,
        before_a.1.unwrap().created_at_unix_nanos
    );
    assert_eq!(after_document.key_id, new_key.key_id());
    assert_material(
        after_document,
        &owner_a,
        "gamma-token",
        rotated_provider.as_ref(),
    );
    assert!(load_pair(db, &owner_b, &identity).await == before_b);

    let conflict_name = format!("conflict-{suffix}");
    let conflict_gate = Arc::new(tokio::sync::Barrier::new(2));
    let left = rotated
        .clone()
        .with_before_upsert_gate(conflict_gate.clone());
    let right = rotated.clone().with_before_upsert_gate(conflict_gate);
    let (left, right) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            left.create_or_replace_user_fixed_token(
                &principal_a,
                &conflict_name,
                &spec_a,
                "left-token".into(),
            ),
            right.create_or_replace_user_fixed_token(
                &principal_a,
                &conflict_name,
                &spec_b,
                "right-token".into(),
            ),
        )
    })
    .await
    .expect("conflicting writes must not deadlock");
    let left = left.expect("left conflicting write");
    let right = right.expect("right conflicting write");
    assert_reference(&left, &spec_a, "a");
    assert_reference(&right, &spec_b, "b");
    let conflict = load_pair(db, &owner_a, &conflict_name).await;
    let conflict_record = conflict.0.as_ref().unwrap();
    assert_eq!(conflict.1.as_ref().unwrap().document_version, 2);
    let expected_token = match conflict_record.spec_reference.key().name() {
        name if name == spec_a => "left-token",
        name if name == spec_b => "right-token",
        name => panic!("unexpected winning spec {name}"),
    };
    assert_material(
        conflict.1.as_ref().unwrap(),
        &owner_a,
        expected_token,
        rotated_provider.as_ref(),
    );

    delete_spec(db, &IdentitySpecKey::global(&spec_a).unwrap()).await;
    assert_eq!(rotated.get(&owner_b, &identity).await.unwrap(), created_b);
    assert_eq!(rotated.list_for_owner(&owner_b).await.unwrap().len(), 1);
    rotated
        .delete(&owner_b, &identity)
        .await
        .expect("delete orphan");
    assert!(load_pair(db, &owner_b, &identity).await == (None, None));
    assert!(matches!(
        rotated.get(&owner_b, &identity).await,
        Err(AppError::IdentityNotFound(_))
    ));
    assert!(matches!(
        rotated.delete(&owner_b, &identity).await,
        Err(AppError::IdentityNotFound(_))
    ));
    assert_eq!(rotated.get(&owner_a, &identity).await.unwrap(), replaced);

    let selected = Arc::new(tokio::sync::Barrier::new(2));
    let resume = Arc::new(tokio::sync::Barrier::new(2));
    let gated = rotated
        .clone()
        .with_before_write_gate(selected.clone(), resume.clone());
    let raced_name = format!("raced-{suffix}");
    let race_result = tokio::time::timeout(Duration::from_secs(10), async {
        let create = gated.create_or_replace_user_fixed_token(
            &principal_a,
            &raced_name,
            &race,
            " race-token ".into(),
        );
        let replace = async {
            selected.wait().await;
            put_spec(
                db,
                &IdentitySpecKey::global(&race).unwrap(),
                &fixed_manifest(&race, "after"),
            )
            .await;
            resume.wait().await;
        };
        let (created, ()) = tokio::join!(create, replace);
        created
    })
    .await
    .expect("race must not deadlock")
    .expect("race create");
    assert_reference(&race_result, &race, "after");
    let raced = load_pair(db, &owner_a, &raced_name).await;
    assert_eq!(raced.0.unwrap(), race_result);
    assert_eq!(raced.1.as_ref().unwrap().document_version, 1);
    assert_material(
        raced.1.as_ref().unwrap(),
        &owner_a,
        "race-token",
        rotated_provider.as_ref(),
    );
}

async fn put_spec(db: &Arc<CoralDb>, key: &IdentitySpecKey, yaml: &str) {
    let manifest = parse_identity_manifest_yaml(yaml).expect("valid identity manifest");
    let write = IdentitySpecWrite::new(
        manifest.version,
        manifest.description,
        manifest.issuer,
        manifest.identity_type.label(),
        yaml,
    )
    .expect("valid identity write");
    let mut tx = db.begin().await.expect("begin spec write");
    tx.identity_specs()
        .upsert(key, &write, 1)
        .await
        .expect("write spec");
    tx.commit().await.expect("commit spec");
}

async fn delete_spec(db: &Arc<CoralDb>, key: &IdentitySpecKey) {
    let mut tx = db.begin().await.unwrap();
    assert!(tx.identity_specs().delete(key).await.unwrap());
    tx.commit().await.unwrap();
}

async fn load_pair(
    db: &Arc<CoralDb>,
    owner: &IdentityOwner,
    name: &str,
) -> (Option<IdentityRecord>, Option<IdentityDocumentRecord>) {
    let name = IdentityName::parse(name).unwrap();
    let mut db = db.as_ref();
    let record = db.identities().load_optional(owner, &name).await.unwrap();
    let document = db
        .identity_documents()
        .load_optional(owner, &name)
        .await
        .unwrap();
    (record, document)
}

fn assert_reference(record: &IdentityRecord, spec_name: &str, label: &str) {
    let manifest = parse_identity_manifest_yaml(&fixed_manifest(spec_name, label)).unwrap();
    assert_eq!(
        record.spec_reference.key(),
        &IdentitySpecKey::global(spec_name).unwrap()
    );
    assert_eq!(
        record.spec_reference.fingerprint(),
        identity_spec_fingerprint(&manifest).unwrap()
    );
    assert_eq!(record.spec_reference.issuer(), format!("issuer_{label}"));
    assert_eq!(record.spec_reference.identity_type(), "fixed_token");
}

fn assert_material(
    document: &IdentityDocumentRecord,
    owner: &IdentityOwner,
    token: &str,
    key_provider: &dyn CredentialKeyProvider,
) {
    let envelope = EncryptedEnvelopeDocument {
        ciphertext: document.ciphertext.clone(),
        nonce: document.nonce.clone(),
        wrapped_dek: document.wrapped_dek.clone(),
        wrapped_dek_nonce: document.wrapped_dek_nonce.clone(),
        key_id: document.key_id.clone(),
        algorithm: document.algorithm.clone(),
        aad_version: document.aad_version,
    };
    let values = decrypt_identity_document(
        owner.kind(),
        owner.key(),
        document.name.as_str(),
        &envelope,
        key_provider,
    )
    .expect("decrypt identity material");
    assert_eq!(
        values,
        std::collections::BTreeMap::from([("TOKEN".to_string(), token.to_string())])
    );
    assert!(
        !document
            .ciphertext
            .windows(token.len())
            .any(|window| window == token.as_bytes())
    );
}

fn fixed_manifest(name: &str, label: &str) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: {label}\ndescription: {label}\nissuer: issuer_{label}\ntype: fixed_token\n"
    )
}

fn oauth_manifest(name: &str) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: oauth\ndescription: oauth\nissuer: oauth_issuer\ntype: oauth\noauth:\n  method:\n    flow:\n      type: authorization_code\n      pkce: disabled\n    redirect_uri: http://127.0.0.1:53682/oauth/callback\n    endpoints:\n      authorization_url: https://provider.example.com/authorize\n      token_url: https://provider.example.com/token\n    client:\n      id:\n        default: client\n"
    )
}
