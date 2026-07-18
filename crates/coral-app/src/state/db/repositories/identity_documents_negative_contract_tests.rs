use std::time::Duration;

use sea_query::{Expr, ExprTrait, Query, SimpleExpr};
use tempfile::tempdir;

use super::identity_documents::IdentityDocumentRecord;
use super::identity_documents_contract_tests::{
    assert_document, assert_document_absent, document, expected_record, identity_name,
    parsed_workspace, reference, seed_identity,
};
use crate::bootstrap::AppError;
use crate::identities::model::{IdentityName, IdentityOwner};
use crate::identity::UserPrincipal;
use crate::state::db::schema::IdentityDocuments;
use crate::state::db::{CoralDb, DbError, DbRepos, IdentitySpecKey, ResolvedDatabaseConfig};

#[tokio::test(flavor = "current_thread")]
async fn identity_document_repository_negative_contract_holds_against_sqlite() {
    let temp = tempdir().expect("temp dir");
    let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
        path: temp.path().join("coral.sqlite"),
    })
    .await
    .expect("open sqlite");
    db.migrate().await.expect("migrate sqlite");
    assert_identity_document_repository_negative_contract(&db).await;
}

async fn assert_identity_document_repository_negative_contract(db: &CoralDb) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let workspace = parsed_workspace(&format!("documentnegative{suffix}"));
    let owner = IdentityOwner::workspace(workspace.clone());
    let colliding_user = IdentityOwner::for_user(
        UserPrincipal::for_user(workspace.as_str()).expect("matching user principal"),
    );
    let material_name = identity_name(&format!("material{suffix}"));
    let concurrent_name = identity_name(&format!("concurrent{suffix}"));
    let orphan_name = identity_name(&format!("orphan{suffix}"));
    let spec_key =
        IdentitySpecKey::global(&format!("documentnegative{suffix}")).expect("identity spec key");
    let spec_reference = reference(&owner, spec_key.clone());
    let user_reference = reference(&colliding_user, spec_key);

    let mut tx = db.begin().await.expect("begin negative contract seed");
    tx.workspaces()
        .ensure(workspace.as_str(), 1)
        .await
        .expect("ensure negative contract workspace");
    seed_identity(&mut tx, &owner, &material_name, &spec_reference, 2).await;
    seed_identity(&mut tx, &owner, &concurrent_name, &spec_reference, 3).await;
    seed_identity(&mut tx, &colliding_user, &orphan_name, &user_reference, 4).await;
    let original = tx
        .identity_documents()
        .upsert(&owner, &material_name, &document(1), 10)
        .await
        .expect("seed corruptible document");
    let concurrent_original = tx
        .identity_documents()
        .upsert(&owner, &concurrent_name, &document(20), 80)
        .await
        .expect("seed concurrent document");
    tx.commit().await.expect("commit negative contract seed");

    assert_missing_parent_is_typed_and_nonmutating(db, &owner, &orphan_name).await;
    assert_document_corruption_matrix(db, &owner, &material_name, &original).await;
    assert_document(db, &owner, &material_name, &original).await;
    assert_foreign_key_violation_rolls_back_prior_write(db, &owner, &material_name, &original)
        .await;
    assert_max_version_is_typed_and_nonmutating(db, &owner, &material_name, &original).await;
    assert_concurrent_document_versions(db, &owner, &concurrent_name, &concurrent_original).await;

    let mut tx = db.begin().await.expect("begin negative contract cleanup");
    assert!(
        tx.identities()
            .delete(&colliding_user, &orphan_name)
            .await
            .expect("delete colliding user identity")
    );
    tx.workspaces()
        .delete(workspace.as_str())
        .await
        .expect("delete negative contract workspace");
    tx.commit().await.expect("commit negative contract cleanup");
}

async fn assert_missing_parent_is_typed_and_nonmutating(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
) {
    let mut tx = db.begin().await.expect("begin orphan document write");
    assert!(matches!(
        tx.identity_documents()
            .upsert(owner, name, &document(30), 20)
            .await,
        Err(AppError::Database(_))
    ));
    tx.rollback().await.expect("rollback failed orphan write");
    assert_document_absent(db, owner, name).await;
}

async fn assert_document_corruption_matrix(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    original: &IdentityDocumentRecord,
) {
    assert_corrupt_document(
        db,
        owner,
        name,
        IdentityDocuments::DocumentVersion,
        zero_i64(),
        original,
    )
    .await;
    for column in [
        IdentityDocuments::Ciphertext,
        IdentityDocuments::Nonce,
        IdentityDocuments::WrappedDek,
        IdentityDocuments::WrappedDekNonce,
    ] {
        assert_corrupt_document(
            db,
            owner,
            name,
            column,
            Expr::val(Vec::<u8>::new()),
            original,
        )
        .await;
    }
    for (column, value) in [
        (IdentityDocuments::KeyId, ""),
        (IdentityDocuments::KeyId, " "),
        (IdentityDocuments::Algorithm, ""),
        (IdentityDocuments::Algorithm, " "),
    ] {
        assert_corrupt_document(db, owner, name, column, Expr::val(value), original).await;
    }
    assert_corrupt_document(
        db,
        owner,
        name,
        IdentityDocuments::BindingVersion,
        zero_i64(),
        original,
    )
    .await;
    assert_corrupt_document(
        db,
        owner,
        name,
        IdentityDocuments::CreatedAtUnixNanos,
        Expr::val(-1_i64),
        original,
    )
    .await;
    assert_corrupt_document(
        db,
        owner,
        name,
        IdentityDocuments::UpdatedAtUnixNanos,
        zero_i64(),
        original,
    )
    .await;
}

async fn assert_corrupt_document(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    column: IdentityDocuments,
    value: SimpleExpr,
    original: &IdentityDocumentRecord,
) {
    let mut tx = db
        .begin()
        .await
        .expect("begin corrupt document transaction");
    let affected = tx
        .execute_affected(
            Query::update()
                .table(IdentityDocuments::Table)
                .value(column, value)
                .and_where(document_where(owner, name))
                .to_owned(),
        )
        .await
        .expect("corrupt identity document");
    assert_eq!(affected, 1);
    assert!(matches!(
        tx.identity_documents().get(owner, name).await,
        Err(DbError::CorruptData(_))
    ));
    tx.rollback().await.expect("restore identity document");
    assert_document(db, owner, name, original).await;
}

async fn assert_foreign_key_violation_rolls_back_prior_write(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    original: &IdentityDocumentRecord,
) {
    let mut tx = db.begin().await.expect("begin foreign-key violation");
    let staged = tx
        .identity_documents()
        .upsert(owner, name, &document(35), 35)
        .await
        .expect("stage document before foreign-key violation");
    let expected_staged = expected_record(owner, name, 2, 35, original.created_at_unix_nanos, 35);
    assert_eq!(staged, expected_staged);
    assert_eq!(
        tx.identity_documents()
            .get(owner, name)
            .await
            .expect("reload staged document"),
        Some(expected_staged)
    );
    let error = tx
        .execute(
            Query::update()
                .table(IdentityDocuments::Table)
                .value(IdentityDocuments::Name, "missing_parent")
                .and_where(document_where(owner, name))
                .to_owned(),
        )
        .await
        .expect_err("orphaning a document must fail");
    let DbError::Sqlx(sqlx::Error::Database(database)) = error else {
        panic!("expected typed foreign-key error, got {error}");
    };
    assert!(database.is_foreign_key_violation());
    tx.rollback()
        .await
        .expect("rollback failed foreign-key transaction");
    assert_document(db, owner, name, original).await;
}

async fn assert_max_version_is_typed_and_nonmutating(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    original: &IdentityDocumentRecord,
) {
    let mut tx = db.begin().await.expect("begin max-version transaction");
    tx.execute(
        Query::update()
            .table(IdentityDocuments::Table)
            .value(IdentityDocuments::DocumentVersion, i64::MAX)
            .and_where(document_where(owner, name))
            .to_owned(),
    )
    .await
    .expect("set max document version");
    let before = tx
        .identity_documents()
        .get(owner, name)
        .await
        .expect("load max-version document")
        .expect("max-version document");
    assert_eq!(before.document_version, i64::MAX);
    let error = tx
        .identity_documents()
        .upsert(owner, name, &document(40), 70)
        .await
        .expect_err("max version must not wrap");
    assert!(matches!(
        &error,
        AppError::FailedPrecondition(detail) if detail.contains("version is exhausted")
    ));
    assert_eq!(
        tx.identity_documents()
            .get(owner, name)
            .await
            .expect("reload max-version document"),
        Some(before)
    );
    tx.rollback().await.expect("rollback max document version");
    assert_document(db, owner, name, original).await;
}

async fn assert_concurrent_document_versions(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    original: &IdentityDocumentRecord,
) {
    let barrier = tokio::sync::Barrier::new(2);
    let (left, right) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            replace_document(db, owner, name, 60, 81, &barrier),
            replace_document(db, owner, name, 70, 82, &barrier)
        )
    })
    .await
    .expect("concurrent identity document updates timed out");
    let mut versions = [left.1.document_version, right.1.document_version];
    versions.sort_unstable();
    assert_eq!(versions, [2, 3]);
    for (result, submitted_at) in [(&left, 81), (&right, 82)] {
        let updated_at = if result.1.document_version == 2 {
            submitted_at
        } else {
            82
        };
        assert_eq!(
            result.1,
            expected_record(
                owner,
                name,
                result.1.document_version,
                result.0,
                original.created_at_unix_nanos,
                updated_at,
            )
        );
    }
    let winner = if left.1.document_version == 3 {
        &left
    } else {
        &right
    };
    let expected = expected_record(owner, name, 3, winner.0, original.created_at_unix_nanos, 82);
    assert_eq!(winner.1, expected);
    assert_document(db, owner, name, &expected).await;
}

async fn replace_document(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    seed: u8,
    now_unix_nanos: i64,
    barrier: &tokio::sync::Barrier,
) -> (u8, IdentityDocumentRecord) {
    let mut tx = db.begin().await.expect("begin concurrent document update");
    barrier.wait().await;
    let record = tx
        .identity_documents()
        .upsert(owner, name, &document(seed), now_unix_nanos)
        .await
        .expect("replace identity document");
    tx.commit()
        .await
        .expect("commit concurrent document update");
    (seed, record)
}

fn document_where(owner: &IdentityOwner, name: &IdentityName) -> SimpleExpr {
    Expr::col(IdentityDocuments::OwnerKind)
        .eq(owner.kind())
        .and(Expr::col(IdentityDocuments::OwnerKey).eq(owner.key()))
        .and(Expr::col(IdentityDocuments::Name).eq(name.as_str()))
}

fn zero_i64() -> SimpleExpr {
    Expr::val(0_i64)
}
