use sea_query::{Expr, ExprTrait, Query};
use tempfile::tempdir;

use super::identity_documents::IdentityDocumentRecord;
use crate::bootstrap::AppError;
use crate::encrypted_document::EncryptedEnvelopeDocument;
use crate::identities::model::{IdentityName, IdentityOwner, IdentitySpecReference};
use crate::identity::{Principal, PrincipalKind};
use crate::state::db::schema::IdentityDocuments;
use crate::state::db::{CoralDb, CoralTx, DbRepos, IdentitySpecKey, ResolvedDatabaseConfig};
use crate::workspaces::WorkspaceName;

pub(crate) async fn set_identity_document_version(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    version: i64,
) {
    let mut tx = db.begin().await.expect("begin document version update");
    tx.execute(
        Query::update()
            .table(IdentityDocuments::Table)
            .value(IdentityDocuments::DocumentVersion, version)
            .and_where(Expr::col(IdentityDocuments::OwnerKind).eq(owner.kind()))
            .and_where(Expr::col(IdentityDocuments::OwnerKey).eq(owner.key()))
            .and_where(Expr::col(IdentityDocuments::Name).eq(name.as_str()))
            .to_owned(),
    )
    .await
    .expect("set identity document version");
    tx.commit().await.expect("commit document version update");
}

#[tokio::test(flavor = "current_thread")]
async fn identity_document_repository_contract_holds_against_sqlite() {
    let temp = tempdir().expect("temp dir");
    let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
        path: temp.path().join("coral.sqlite"),
    })
    .await
    .expect("open sqlite");
    db.migrate().await.expect("migrate sqlite");
    assert_identity_document_repository_contract(&db).await;
}

#[expect(clippy::too_many_lines, reason = "shared backend contract fixture")]
async fn assert_identity_document_repository_contract(db: &CoralDb) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let workspace = parsed_workspace(&format!("document{suffix}"));
    let other_workspace = parsed_workspace(&format!("documentb{suffix}"));
    let user = IdentityOwner::for_user(
        Principal::parse(workspace.as_str(), PrincipalKind::User).expect("matching user principal"),
    );
    let workspace_owner = IdentityOwner::workspace(workspace.clone());
    let other_workspace_owner = IdentityOwner::workspace(other_workspace.clone());
    let shared = identity_name(&format!("shared{suffix}"));
    let other = identity_name(&format!("other{suffix}"));
    let rolled_back = identity_name(&format!("rollback{suffix}"));
    let missing = identity_name(&format!("missing{suffix}"));
    let spec_key =
        IdentitySpecKey::global(&format!("documentspec{suffix}")).expect("identity spec key");
    let user_reference = reference(&user, spec_key.clone());
    let workspace_reference = reference(&workspace_owner, spec_key.clone());
    let other_workspace_reference = reference(&other_workspace_owner, spec_key);

    let mut tx = db.begin().await.expect("begin parent seed");
    tx.workspaces()
        .ensure(workspace.as_str(), 1)
        .await
        .expect("ensure document workspace");
    tx.workspaces()
        .ensure(other_workspace.as_str(), 1)
        .await
        .expect("ensure other document workspace");
    seed_identity(&mut tx, &user, &shared, &user_reference, 2).await;
    seed_identity(&mut tx, &workspace_owner, &shared, &workspace_reference, 3).await;
    seed_identity(
        &mut tx,
        &other_workspace_owner,
        &shared,
        &other_workspace_reference,
        4,
    )
    .await;
    seed_identity(&mut tx, &user, &other, &user_reference, 5).await;
    seed_identity(&mut tx, &user, &rolled_back, &user_reference, 6).await;
    for (owner, name) in [
        (&user, &shared),
        (&workspace_owner, &shared),
        (&other_workspace_owner, &shared),
        (&user, &other),
        (&user, &rolled_back),
        (&user, &missing),
    ] {
        assert!(
            tx.identity_documents()
                .get(owner, name)
                .await
                .expect("load absent document")
                .is_none()
        );
    }
    let mut invalid = document(1);
    invalid.ciphertext.clear();
    assert!(matches!(
        tx.identity_documents()
            .upsert(&user, &shared, &invalid, 9)
            .await,
        Err(AppError::InvalidInput(_))
    ));
    let user_first = tx
        .identity_documents()
        .upsert(&user, &shared, &document(1), 10)
        .await
        .expect("insert user document");
    assert_eq!(user_first, expected_record(&user, &shared, 1, 1, 10, 10));
    let workspace_first = tx
        .identity_documents()
        .upsert(&workspace_owner, &shared, &document(20), 11)
        .await
        .expect("insert workspace document");
    assert_eq!(
        workspace_first,
        expected_record(&workspace_owner, &shared, 1, 20, 11, 11)
    );
    let other_workspace_first = tx
        .identity_documents()
        .upsert(&other_workspace_owner, &shared, &document(30), 12)
        .await
        .expect("insert other workspace document");
    assert_eq!(
        other_workspace_first,
        expected_record(&other_workspace_owner, &shared, 1, 30, 12, 12)
    );
    let other_first = tx
        .identity_documents()
        .upsert(&user, &other, &document(35), 13)
        .await
        .expect("insert second user document");
    assert_eq!(other_first, expected_record(&user, &other, 1, 35, 13, 13));
    tx.commit().await.expect("commit document seed");

    assert_document(db, &user, &shared, &user_first).await;
    assert_document(db, &workspace_owner, &shared, &workspace_first).await;
    assert_document(db, &other_workspace_owner, &shared, &other_workspace_first).await;
    assert_document(db, &user, &other, &other_first).await;
    assert_document_absent(db, &user, &rolled_back).await;

    let mut tx = db.begin().await.expect("begin replacement");
    let replaced = tx
        .identity_documents()
        .upsert(&workspace_owner, &shared, &document(40), 30)
        .await
        .expect("replace document");
    let expected_replaced = expected_record(&workspace_owner, &shared, 2, 40, 11, 30);
    assert_eq!(replaced, expected_replaced);
    let regressed = tx
        .identity_documents()
        .upsert(&workspace_owner, &shared, &document(50), 5)
        .await
        .expect("replace under regressed clock");
    let expected_regressed = expected_record(&workspace_owner, &shared, 3, 50, 11, 30);
    assert_eq!(regressed, expected_regressed);
    assert!(matches!(
        tx.identity_documents()
            .upsert(&workspace_owner, &shared, &document(60), -1)
            .await,
        Err(AppError::InvalidInput(_))
    ));
    assert_eq!(
        tx.identity_documents()
            .get(&workspace_owner, &shared)
            .await
            .expect("reload after rejected write"),
        Some(expected_regressed.clone())
    );
    tx.commit().await.expect("commit replacement");
    assert_document(db, &workspace_owner, &shared, &expected_regressed).await;

    let mut tx = db.begin().await.expect("begin rollback");
    assert_eq!(
        tx.identity_documents()
            .upsert(&workspace_owner, &shared, &document(70), 40)
            .await
            .expect("replace rolled-back document"),
        expected_record(&workspace_owner, &shared, 4, 70, 11, 40)
    );
    assert_eq!(
        tx.identity_documents()
            .upsert(&user, &other, &document(80), 40)
            .await
            .expect("replace second rolled-back document"),
        expected_record(&user, &other, 2, 80, 13, 40)
    );
    tx.identity_documents()
        .upsert(&user, &rolled_back, &document(90), 40)
        .await
        .expect("insert rolled-back document");
    assert!(
        tx.identities()
            .delete(&user, &other)
            .await
            .expect("delete rolled-back parent identity")
    );
    assert!(
        tx.identity_documents()
            .get(&user, &other)
            .await
            .expect("load rolled-back cascade")
            .is_none()
    );
    tx.rollback().await.expect("rollback document writes");
    assert_document(db, &workspace_owner, &shared, &expected_regressed).await;
    assert_document(db, &user, &other, &other_first).await;
    assert_document_absent(db, &user, &rolled_back).await;

    let mut tx = db.begin().await.expect("begin direct identity cascade");
    assert!(
        tx.identities()
            .delete(&user, &shared)
            .await
            .expect("delete user identity")
    );
    assert!(
        tx.identity_documents()
            .get(&user, &shared)
            .await
            .expect("load direct cascade")
            .is_none()
    );
    assert_eq!(
        tx.identity_documents()
            .get(&workspace_owner, &shared)
            .await
            .expect("load colliding workspace document"),
        Some(expected_regressed.clone())
    );
    assert_eq!(
        tx.identity_documents()
            .get(&other_workspace_owner, &shared)
            .await
            .expect("load other workspace document"),
        Some(other_workspace_first.clone())
    );
    tx.commit().await.expect("commit direct cascade");
    assert_document_absent(db, &user, &shared).await;
    assert_document(db, &workspace_owner, &shared, &expected_regressed).await;
    assert_document(db, &other_workspace_owner, &shared, &other_workspace_first).await;

    let mut tx = db.begin().await.expect("begin workspace cascade");
    tx.workspaces()
        .delete(workspace.as_str())
        .await
        .expect("delete workspace");
    tx.commit().await.expect("commit workspace cascade");
    assert_document_absent(db, &workspace_owner, &shared).await;
    assert_document(db, &other_workspace_owner, &shared, &other_workspace_first).await;
    assert_document(db, &user, &other, &other_first).await;

    let mut tx = db.begin().await.expect("begin cleanup");
    tx.workspaces()
        .delete(other_workspace.as_str())
        .await
        .expect("delete other workspace");
    for name in [&other, &rolled_back] {
        tx.identities()
            .delete(&user, name)
            .await
            .expect("delete remaining user identity");
    }
    tx.commit().await.expect("commit cleanup");
}

pub(super) async fn seed_identity(
    tx: &mut CoralTx<'_>,
    owner: &IdentityOwner,
    name: &IdentityName,
    reference: &IdentitySpecReference,
    now: i64,
) {
    tx.identities()
        .upsert(owner, name, reference, now)
        .await
        .expect("seed identity document parent");
}

pub(super) async fn assert_document(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    expected: &IdentityDocumentRecord,
) {
    let mut session = db;
    assert_eq!(
        session
            .identity_documents()
            .get(owner, name)
            .await
            .expect("get identity document")
            .as_ref(),
        Some(expected)
    );
}

pub(super) async fn assert_document_absent(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
) {
    let mut session = db;
    assert!(
        session
            .identity_documents()
            .get(owner, name)
            .await
            .expect("get absent identity document")
            .is_none()
    );
}

pub(super) fn expected_record(
    owner: &IdentityOwner,
    name: &IdentityName,
    version: i64,
    seed: u8,
    created_at_unix_nanos: i64,
    updated_at_unix_nanos: i64,
) -> IdentityDocumentRecord {
    IdentityDocumentRecord {
        owner: owner.clone(),
        name: name.clone(),
        document_version: version,
        envelope: document(seed),
        created_at_unix_nanos,
        updated_at_unix_nanos,
    }
}

pub(super) fn document(seed: u8) -> EncryptedEnvelopeDocument {
    EncryptedEnvelopeDocument::new(
        vec![seed; 3],
        vec![seed + 1; 2],
        vec![seed + 2; 4],
        vec![seed + 3; 2],
        format!("key-{seed}"),
        format!("algorithm-{seed}"),
        i64::from(seed) + 1,
    )
    .expect("valid identity document")
}

pub(super) fn reference(owner: &IdentityOwner, key: IdentitySpecKey) -> IdentitySpecReference {
    IdentitySpecReference::new(owner, key, "fingerprint", "issuer", "fixed_token")
        .expect("identity spec reference")
}

pub(super) fn parsed_workspace(value: &str) -> WorkspaceName {
    WorkspaceName::parse(value).expect("workspace")
}

pub(super) fn identity_name(value: &str) -> IdentityName {
    IdentityName::parse(value).expect("identity name")
}
