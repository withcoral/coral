use std::collections::BTreeMap;

use tempfile::tempdir;

use crate::identities::model::{IdentityName, IdentityOwner, IdentitySpecReference};
use crate::identity::UserPrincipal;
use crate::state::db::{
    CoralDb, CoralTx, DbRepos, IdentityDocumentRecord, IdentityDocumentWrite,
    IdentityOAuthRefreshClaim, IdentityRecord, IdentitySpecKey, IdentitySpecWrite,
    ResolvedDatabaseConfig,
};
use crate::workspaces::WorkspaceName;

#[tokio::test]
async fn identity_repository_contract_holds_against_sqlite() {
    let temp = tempdir().expect("temp dir");
    let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
        path: temp.path().join("coral.sqlite"),
    })
    .await
    .expect("open sqlite");
    db.migrate().await.expect("migrate sqlite");
    assert_identity_repository_contract(&db).await;
}

#[expect(clippy::too_many_lines, reason = "Shared backend contract fixture.")]
pub(in crate::state::db) async fn assert_identity_repository_contract(db: &CoralDb) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let workspace = parsed_workspace(&format!("owner{suffix}"));
    let other_workspace = parsed_workspace(&format!("other{suffix}"));
    let user = IdentityOwner::for_user(
        UserPrincipal::for_user(workspace.as_str()).expect("matching user principal"),
    );
    let workspace_owner = IdentityOwner::workspace(workspace.clone());
    let other_owner = IdentityOwner::workspace(other_workspace.clone());
    let shared_name = identity_name(&format!("shared{suffix}"));
    let fallback_name = identity_name(&format!("fallback{suffix}"));
    let spec_name = format!("spec{suffix}");
    let replacement_spec_name = format!("replacement{suffix}");
    let global_key = IdentitySpecKey::global(&spec_name).expect("global spec key");
    let workspace_key =
        IdentitySpecKey::workspace(workspace.clone(), &spec_name).expect("workspace spec key");
    let replacement_key =
        IdentitySpecKey::global(&replacement_spec_name).expect("replacement spec key");
    let user_ref = reference(&user, global_key.clone(), "f1");
    let workspace_ref = reference(&workspace_owner, workspace_key.clone(), "f1");
    let other_ref = reference(&other_owner, global_key.clone(), "f2");
    let fallback_ref = reference(&workspace_owner, global_key.clone(), "f1");
    let replacement_identity_ref = IdentitySpecReference::new(
        &workspace_owner,
        replacement_key.clone(),
        "f3",
        "replacement-issuer",
        "oauth",
    )
    .expect("replacement identity reference");

    let mut tx = db.begin().await.expect("begin seed tx");
    for workspace in [&workspace, &other_workspace] {
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("ensure workspace");
    }
    tx.identity_specs()
        .upsert(&global_key, &spec("global"), 2)
        .await
        .expect("seed global spec");
    tx.identity_specs()
        .upsert(&workspace_key, &spec("workspace"), 3)
        .await
        .expect("seed workspace spec");
    tx.identity_specs()
        .upsert(&replacement_key, &spec("replacement"), 4)
        .await
        .expect("seed replacement spec");
    let user_metadata = metadata([("scope", "user"), ("token_type", "Bearer")]);
    let workspace_metadata = metadata([("scope", "workspace"), ("token_type", "Bearer")]);
    let other_metadata = metadata([("scope", "other"), ("token_type", "DPoP")]);
    let mut user_row =
        seed_identity_with_metadata(&mut tx, &user, &shared_name, &user_ref, &user_metadata, 10)
            .await;
    let mut workspace_row = seed_identity_with_metadata(
        &mut tx,
        &workspace_owner,
        &shared_name,
        &workspace_ref,
        &workspace_metadata,
        11,
    )
    .await;
    let other_row = seed_identity_with_metadata(
        &mut tx,
        &other_owner,
        &shared_name,
        &other_ref,
        &other_metadata,
        12,
    )
    .await;
    let fallback_row =
        seed_identity(&mut tx, &workspace_owner, &fallback_name, &fallback_ref, 13).await;
    let user_document = seed_document(&mut tx, &user, &shared_name, "user", 20).await;
    let workspace_document =
        seed_document(&mut tx, &workspace_owner, &shared_name, "workspace", 21).await;
    let other_document = seed_document(&mut tx, &other_owner, &shared_name, "other", 22).await;
    let fallback_document =
        seed_document(&mut tx, &workspace_owner, &fallback_name, "fallback", 23).await;
    tx.commit().await.expect("commit seed tx");

    assert_identity(db, &user, &shared_name, &user_row).await;
    assert_identity(db, &workspace_owner, &shared_name, &workspace_row).await;
    assert_identity(db, &other_owner, &shared_name, &other_row).await;
    assert_identity(db, &workspace_owner, &fallback_name, &fallback_row).await;
    assert_document(db, &user, &shared_name, Some(&user_document)).await;
    assert_document(
        db,
        &workspace_owner,
        &shared_name,
        Some(&workspace_document),
    )
    .await;
    assert_document(db, &other_owner, &shared_name, Some(&other_document)).await;
    assert_document(
        db,
        &workspace_owner,
        &fallback_name,
        Some(&fallback_document),
    )
    .await;
    assert_owner_names(db, &user, [&shared_name]).await;
    assert_owner_names(db, &workspace_owner, [&fallback_name, &shared_name]).await;
    assert_owner_names(db, &other_owner, [&shared_name]).await;
    assert_counts(db, &global_key, [("f1", 2), ("f2", 1), ("missing", 0)], 3).await;
    assert_counts(db, &workspace_key, [("f1", 1)], 1).await;
    assert_counts(db, &replacement_key, [("f3", 0)], 0).await;

    let claim = IdentityOAuthRefreshClaim::new(uuid::Uuid::new_v4(), 40)
        .expect("valid OAuth refresh claim");
    let competing_claim =
        IdentityOAuthRefreshClaim::new(uuid::Uuid::new_v4(), 41).expect("valid competing claim");
    let mut tx = db.begin().await.expect("begin refresh claim tx");
    assert!(
        tx.identities()
            .try_claim_oauth_refresh(&workspace_owner, &shared_name, &claim)
            .await
            .expect("claim identity OAuth refresh")
    );
    assert!(
        !tx.identities()
            .try_claim_oauth_refresh(&workspace_owner, &shared_name, &competing_claim)
            .await
            .expect("do not replace a live claim")
    );
    assert!(
        tx.identities()
            .has_oauth_refresh_claimed_dependents(&workspace_key)
            .await
            .expect("count claimed dependents")
    );
    tx.commit().await.expect("commit refresh claim");
    assert_eq!(
        load_refresh_claim(db, &workspace_owner, &shared_name).await,
        Some(claim.clone())
    );
    assert_eq!(load_refresh_claim(db, &user, &shared_name).await, None);

    let mut tx = db.begin().await.expect("begin refresh claim expiry tx");
    assert!(
        !tx.identities()
            .expire_oauth_refresh_claim(&workspace_owner, &shared_name, competing_claim.id(), 14,)
            .await
            .expect("wrong claimant cannot expire")
    );
    assert!(
        tx.identities()
            .expire_oauth_refresh_claim(&workspace_owner, &shared_name, claim.id(), 14)
            .await
            .expect("claimant expires its claim")
    );
    assert!(
        !tx.identities()
            .try_claim_oauth_refresh(&workspace_owner, &shared_name, &competing_claim)
            .await
            .expect("expired claims are never stolen")
    );
    tx.commit().await.expect("commit refresh claim expiry");
    assert_eq!(
        load_refresh_claim(db, &workspace_owner, &shared_name)
            .await
            .expect("expired claim remains durable")
            .deadline_unix_nanos(),
        14
    );

    let replacement_metadata = metadata([("token_type", "DPoP")]);
    let expected_replaced_identity = IdentityRecord {
        owner: workspace_owner.clone(),
        name: shared_name.clone(),
        spec_reference: replacement_identity_ref.clone(),
        safe_metadata: replacement_metadata.clone(),
        created_at_unix_nanos: 11,
        updated_at_unix_nanos: 30,
    };
    let mut tx = db.begin().await.expect("begin identity replacement tx");
    let replaced_identity = tx
        .identities()
        .upsert(
            &workspace_owner,
            &shared_name,
            &replacement_identity_ref,
            &replacement_metadata,
            30,
        )
        .await
        .expect("replace identity reference");
    assert_eq!(replaced_identity, expected_replaced_identity);
    let regressed_identity = tx
        .identities()
        .upsert(
            &workspace_owner,
            &shared_name,
            &replacement_identity_ref,
            &replacement_metadata,
            5,
        )
        .await
        .expect("replace identity under regressed clock");
    assert_eq!(regressed_identity, expected_replaced_identity);
    tx.commit().await.expect("commit identity replacement tx");
    assert_eq!(
        load_refresh_claim(db, &workspace_owner, &shared_name).await,
        None,
        "explicit replacement must clear refresh coordination"
    );
    assert_identity(
        db,
        &workspace_owner,
        &shared_name,
        &expected_replaced_identity,
    )
    .await;
    assert_document(
        db,
        &workspace_owner,
        &shared_name,
        Some(&workspace_document),
    )
    .await;
    assert_counts(db, &global_key, [("f1", 2), ("f2", 1)], 3).await;
    assert_counts(db, &workspace_key, [("f1", 0)], 0).await;
    assert_counts(db, &replacement_key, [("f3", 1)], 1).await;

    let mut tx = db.begin().await.expect("begin identity restore tx");
    let restored_identity = tx
        .identities()
        .upsert(
            &workspace_owner,
            &shared_name,
            &workspace_ref,
            &BTreeMap::new(),
            31,
        )
        .await
        .expect("restore workspace identity reference");
    workspace_row = IdentityRecord {
        owner: workspace_owner.clone(),
        name: shared_name.clone(),
        spec_reference: workspace_ref.clone(),
        safe_metadata: BTreeMap::new(),
        created_at_unix_nanos: 11,
        updated_at_unix_nanos: 31,
    };
    assert_eq!(restored_identity, workspace_row);
    tx.commit().await.expect("commit identity restore tx");
    assert_identity(db, &workspace_owner, &shared_name, &workspace_row).await;
    assert_counts(db, &global_key, [("f1", 2), ("f2", 1)], 3).await;
    assert_counts(db, &workspace_key, [("f1", 1)], 1).await;
    assert_counts(db, &replacement_key, [("f3", 0)], 0).await;

    let replacement_write = document("user-replacement");
    let updated_user_metadata = metadata([
        ("access_token_expires_at", "2030-01-02T03:04:05Z"),
        ("token_type", "Bearer"),
    ]);
    let mut tx = db.begin().await.expect("begin replacement tx");
    user_row = tx
        .identities()
        .upsert(&user, &shared_name, &user_ref, &updated_user_metadata, 30)
        .await
        .expect("replace user safe metadata");
    let replaced = tx
        .identity_documents()
        .upsert(&user, &shared_name, &replacement_write, 30)
        .await
        .expect("replace user document");
    assert_eq!(
        replaced,
        expected_document(&user, &shared_name, "user-replacement", 2, 20, 30)
    );
    let replacement = tx
        .identity_documents()
        .upsert(&user, &shared_name, &replacement_write, 5)
        .await
        .expect("replace under regressed clock");
    tx.commit().await.expect("commit replacement tx");
    assert_identity(db, &user, &shared_name, &user_row).await;
    assert_eq!(
        replacement,
        expected_document(&user, &shared_name, "user-replacement", 3, 20, 30)
    );
    assert_document(db, &user, &shared_name, Some(&replacement)).await;
    assert_document(
        db,
        &workspace_owner,
        &shared_name,
        Some(&workspace_document),
    )
    .await;
    assert_document(db, &other_owner, &shared_name, Some(&other_document)).await;
    assert_document(
        db,
        &workspace_owner,
        &fallback_name,
        Some(&fallback_document),
    )
    .await;

    let mut tx = db.begin().await.expect("begin spec orphan tx");
    assert!(
        tx.identity_specs()
            .delete(&global_key)
            .await
            .expect("delete global spec")
    );
    assert!(
        tx.identity_specs()
            .delete(&workspace_key)
            .await
            .expect("delete workspace spec")
    );
    assert!(
        tx.identity_specs()
            .delete(&replacement_key)
            .await
            .expect("delete replacement spec")
    );
    tx.commit().await.expect("commit spec orphan tx");
    assert_identity(db, &user, &shared_name, &user_row).await;
    assert_identity(db, &workspace_owner, &shared_name, &workspace_row).await;
    assert_identity(db, &other_owner, &shared_name, &other_row).await;
    assert_identity(db, &workspace_owner, &fallback_name, &fallback_row).await;
    assert_document(db, &user, &shared_name, Some(&replacement)).await;
    assert_document(
        db,
        &workspace_owner,
        &shared_name,
        Some(&workspace_document),
    )
    .await;
    assert_document(db, &other_owner, &shared_name, Some(&other_document)).await;
    assert_document(
        db,
        &workspace_owner,
        &fallback_name,
        Some(&fallback_document),
    )
    .await;
    assert_counts(db, &global_key, [("f1", 2), ("f2", 1)], 3).await;
    assert_counts(db, &workspace_key, [("f1", 1)], 1).await;

    let mut tx = db.begin().await.expect("begin identity cascade tx");
    assert!(
        tx.identities()
            .delete(&user, &shared_name)
            .await
            .expect("delete user identity")
    );
    tx.commit().await.expect("commit identity cascade tx");
    assert_identity_absent(db, &user, &shared_name).await;
    assert_document(db, &user, &shared_name, None).await;
    assert_identity(db, &workspace_owner, &shared_name, &workspace_row).await;
    assert_document(
        db,
        &workspace_owner,
        &shared_name,
        Some(&workspace_document),
    )
    .await;
    assert_counts(db, &global_key, [("f1", 1), ("f2", 1)], 2).await;

    let mut tx = db.begin().await.expect("begin same-key user restore tx");
    let restored_user_row = seed_identity(&mut tx, &user, &shared_name, &user_ref, 31).await;
    let restored_user_document =
        seed_document(&mut tx, &user, &shared_name, "user-restored", 32).await;
    tx.commit().await.expect("commit same-key user restore tx");
    assert_counts(db, &global_key, [("f1", 2), ("f2", 1)], 3).await;

    let mut tx = db.begin().await.expect("begin workspace cascade tx");
    tx.workspaces()
        .delete(workspace.as_str())
        .await
        .expect("delete workspace");
    tx.commit().await.expect("commit workspace cascade tx");
    assert_identity_absent(db, &workspace_owner, &shared_name).await;
    assert_identity_absent(db, &workspace_owner, &fallback_name).await;
    assert_document(db, &workspace_owner, &shared_name, None).await;
    assert_document(db, &workspace_owner, &fallback_name, None).await;
    assert_identity(db, &user, &shared_name, &restored_user_row).await;
    assert_document(db, &user, &shared_name, Some(&restored_user_document)).await;
    assert_identity(db, &other_owner, &shared_name, &other_row).await;
    assert_document(db, &other_owner, &shared_name, Some(&other_document)).await;
    assert_counts(db, &global_key, [("f1", 1), ("f2", 1)], 2).await;
    assert_counts(db, &workspace_key, [("f1", 0)], 0).await;

    let mut tx = db.begin().await.expect("begin cleanup tx");
    tx.identities()
        .delete(&user, &shared_name)
        .await
        .expect("delete user identity");
    tx.identities()
        .delete(&other_owner, &shared_name)
        .await
        .expect("delete other identity");
    tx.workspaces()
        .delete(other_workspace.as_str())
        .await
        .expect("delete other workspace");
    tx.commit().await.expect("commit cleanup tx");
}

pub(super) async fn seed_identity(
    tx: &mut CoralTx<'_>,
    owner: &IdentityOwner,
    name: &IdentityName,
    reference: &IdentitySpecReference,
    now: i64,
) -> IdentityRecord {
    seed_identity_with_metadata(tx, owner, name, reference, &BTreeMap::new(), now).await
}

pub(super) async fn seed_identity_with_metadata(
    tx: &mut CoralTx<'_>,
    owner: &IdentityOwner,
    name: &IdentityName,
    reference: &IdentitySpecReference,
    safe_metadata: &BTreeMap<String, String>,
    now: i64,
) -> IdentityRecord {
    let record = tx
        .identities()
        .upsert(owner, name, reference, safe_metadata, now)
        .await
        .expect("seed identity");
    assert_eq!(
        record,
        IdentityRecord {
            owner: owner.clone(),
            name: name.clone(),
            spec_reference: reference.clone(),
            safe_metadata: safe_metadata.clone(),
            created_at_unix_nanos: now,
            updated_at_unix_nanos: now,
        }
    );
    record
}

pub(super) fn metadata<const N: usize>(entries: [(&str, &str); N]) -> BTreeMap<String, String> {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

pub(super) async fn seed_document(
    tx: &mut CoralTx<'_>,
    owner: &IdentityOwner,
    name: &IdentityName,
    label: &str,
    now: i64,
) -> IdentityDocumentRecord {
    let record = tx
        .identity_documents()
        .upsert(owner, name, &document(label), now)
        .await
        .expect("seed document");
    assert_eq!(record, expected_document(owner, name, label, 1, now, now));
    record
}

pub(super) fn expected_document(
    owner: &IdentityOwner,
    name: &IdentityName,
    label: &str,
    document_version: i64,
    created_at_unix_nanos: i64,
    updated_at_unix_nanos: i64,
) -> IdentityDocumentRecord {
    IdentityDocumentRecord {
        owner: owner.clone(),
        name: name.clone(),
        document_version,
        ciphertext: format!("cipher-{label}").into_bytes(),
        nonce: format!("nonce-{label}").into_bytes(),
        wrapped_dek: format!("wrapped-{label}").into_bytes(),
        wrapped_dek_nonce: format!("wrapped-nonce-{label}").into_bytes(),
        key_id: format!("key-{label}"),
        algorithm: format!("algorithm-{label}"),
        aad_version: 1,
        created_at_unix_nanos,
        updated_at_unix_nanos,
    }
}

pub(super) async fn assert_identity(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    expected: &IdentityRecord,
) {
    let mut session = db;
    assert_eq!(
        session
            .identities()
            .load_optional(owner, name)
            .await
            .expect("load identity")
            .as_ref(),
        Some(expected)
    );
}

async fn load_refresh_claim(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
) -> Option<IdentityOAuthRefreshClaim> {
    let mut session = db;
    session
        .identities()
        .load_oauth_refresh_claim(owner, name)
        .await
        .expect("load identity OAuth refresh claim")
}

pub(super) async fn assert_identity_absent(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
) {
    let mut session = db;
    assert!(
        session
            .identities()
            .load_optional(owner, name)
            .await
            .expect("load absent identity")
            .is_none()
    );
}

pub(super) async fn assert_document(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    expected: Option<&IdentityDocumentRecord>,
) {
    let mut session = db;
    assert_eq!(
        session
            .identity_documents()
            .load_optional(owner, name)
            .await
            .expect("load identity document")
            .as_ref(),
        expected
    );
}

async fn assert_owner_names<const N: usize>(
    db: &CoralDb,
    owner: &IdentityOwner,
    expected: [&IdentityName; N],
) {
    let mut session = db;
    let names = session
        .identities()
        .list_for_owner(owner)
        .await
        .expect("list owner identities")
        .into_iter()
        .map(|record| record.name)
        .collect::<Vec<_>>();
    assert_eq!(names.iter().collect::<Vec<_>>(), expected);
}

async fn assert_counts<const N: usize>(
    db: &CoralDb,
    key: &IdentitySpecKey,
    expected: [(&str, u64); N],
    total: u64,
) {
    let mut session = db;
    assert_eq!(
        session
            .identities()
            .count_dependents(key)
            .await
            .expect("count dependents"),
        total
    );
    for (fingerprint, count) in expected {
        assert_eq!(
            session
                .identities()
                .count_exact_dependents(key, fingerprint)
                .await
                .expect("count exact dependents"),
            count
        );
    }
}

pub(super) fn parsed_workspace(value: &str) -> WorkspaceName {
    WorkspaceName::parse(value).expect("workspace")
}

pub(super) fn identity_name(value: &str) -> IdentityName {
    IdentityName::parse(value).expect("identity name")
}

pub(super) fn reference(
    owner: &IdentityOwner,
    key: IdentitySpecKey,
    fingerprint: &str,
) -> IdentitySpecReference {
    IdentitySpecReference::new(owner, key, fingerprint, "issuer", "fixed_token")
        .expect("identity reference")
}

fn spec(label: &str) -> IdentitySpecWrite {
    IdentitySpecWrite::new(
        format!("v-{label}"),
        format!("description-{label}"),
        format!("issuer-{label}"),
        "fixed_token",
        format!("kind: fixed_token\nname: {label}\n"),
    )
    .expect("identity spec write")
}

pub(super) fn document(label: &str) -> IdentityDocumentWrite {
    IdentityDocumentWrite::new(
        format!("cipher-{label}").into_bytes(),
        format!("nonce-{label}").into_bytes(),
        format!("wrapped-{label}").into_bytes(),
        format!("wrapped-nonce-{label}").into_bytes(),
        format!("key-{label}"),
        format!("algorithm-{label}"),
        1,
    )
    .expect("identity document write")
}
