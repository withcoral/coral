use std::time::Duration;

use sea_query::{Expr, ExprTrait, Query, SimpleExpr, UpdateStatement};
use tempfile::tempdir;

use super::identities_contract_tests::{
    assert_document, assert_identity, assert_identity_absent, document, expected_document,
    identity_name, parsed_workspace, reference, seed_document, seed_identity,
};
use crate::bootstrap::AppError;
use crate::identities::model::{IdentityName, IdentityOwner, IdentitySpecReference};
use crate::state::db::schema::{Identities, IdentityDocuments};
use crate::state::db::{
    CoralDb, DbError, DbRepos, IdentityDocumentRecord, IdentitySpecKey, ResolvedDatabaseConfig,
};
use crate::workspaces::WorkspaceName;

#[tokio::test]
async fn identity_repository_negative_contract_holds_against_sqlite() {
    let temp = tempdir().expect("temp dir");
    let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
        path: temp.path().join("coral.sqlite"),
    })
    .await
    .expect("open sqlite");
    db.migrate().await.expect("migrate sqlite");
    assert_identity_repository_negative_contract(&db).await;
}

pub(in crate::state::db) async fn assert_identity_repository_negative_contract(db: &CoralDb) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let workspace = parsed_workspace(&format!("negative{suffix}"));
    let owner = IdentityOwner::workspace(workspace.clone());
    let spec_key = IdentitySpecKey::workspace(workspace.clone(), &format!("spec{suffix}"))
        .expect("workspace spec key");
    let spec_reference = reference(&owner, spec_key, "fingerprint");
    let corrupt_name = identity_name(&format!("corrupt{suffix}"));
    let constraint_name = identity_name(&format!("constraint{suffix}"));
    let material_name = identity_name(&format!("material{suffix}"));

    let mut tx = db.begin().await.expect("begin seed tx");
    tx.workspaces()
        .ensure(workspace.as_str(), 1)
        .await
        .expect("ensure workspace");
    let original_identity =
        seed_identity(&mut tx, &owner, &corrupt_name, &spec_reference, 10).await;
    seed_identity(&mut tx, &owner, &constraint_name, &spec_reference, 11).await;
    seed_identity(&mut tx, &owner, &material_name, &spec_reference, 12).await;
    let original_document = seed_document(&mut tx, &owner, &material_name, "material", 12).await;
    tx.commit().await.expect("commit seed tx");

    assert_foreign_keys_and_rollback(db, &suffix, &workspace, &owner).await;
    assert_rollback_invisibility(db, &suffix, &owner, &spec_reference).await;

    assert_corrupt_identity_name(
        db,
        &owner,
        &corrupt_name,
        Expr::val(format!(" {} ", corrupt_name.as_str())),
    )
    .await;
    for value in ["", " non_normalized "] {
        assert_corrupt_identity(
            db,
            &owner,
            &corrupt_name,
            Identities::IdentitySpecName,
            Expr::val(value),
        )
        .await;
    }
    for column in [
        Identities::IdentitySpecFingerprint,
        Identities::Issuer,
        Identities::IdentityType,
    ] {
        assert_corrupt_identity(db, &owner, &corrupt_name, column, Expr::val("")).await;
    }
    assert_corrupt_identity(
        db,
        &owner,
        &corrupt_name,
        Identities::IdentitySpecFingerprint,
        Expr::val(" "),
    )
    .await;
    assert_corrupt_identity(
        db,
        &owner,
        &corrupt_name,
        Identities::CreatedAtUnixNanos,
        Expr::val(-1_i64),
    )
    .await;
    assert_corrupt_identity(
        db,
        &owner,
        &corrupt_name,
        Identities::UpdatedAtUnixNanos,
        zero_i64(),
    )
    .await;
    assert_identity(db, &owner, &corrupt_name, &original_identity).await;

    assert_document_corruption_matrix(db, &owner, &material_name).await;
    assert_document(db, &owner, &material_name, Some(&original_document)).await;
    assert_constraint_violations(db, &suffix, &owner, &constraint_name, &material_name).await;
    assert_max_version_is_typed_and_nonmutating(db, &owner, &material_name, &original_document)
        .await;
    let concurrent_name = identity_name(&format!("concurrent{suffix}"));
    assert_concurrent_document_versions(db, &owner, &concurrent_name, &spec_reference).await;

    let mut tx = db.begin().await.expect("begin cleanup tx");
    tx.workspaces()
        .delete(workspace.as_str())
        .await
        .expect("delete workspace");
    tx.commit().await.expect("commit cleanup tx");
}

async fn assert_foreign_keys_and_rollback(
    db: &CoralDb,
    suffix: &str,
    workspace: &WorkspaceName,
    owner: &IdentityOwner,
) {
    let missing_workspace = parsed_workspace(&format!("missing{suffix}"));
    let missing_owner = IdentityOwner::workspace(missing_workspace.clone());
    let missing_name = identity_name(&format!("missing{suffix}"));
    let missing_key = IdentitySpecKey::workspace(missing_workspace, &format!("spec{suffix}"))
        .expect("missing workspace spec key");
    let missing_reference = reference(&missing_owner, missing_key, "missing");
    let mut tx = db.begin().await.expect("begin missing workspace tx");
    assert!(matches!(
        tx.identities()
            .upsert(&missing_owner, &missing_name, &missing_reference, 20)
            .await,
        Err(AppError::Database(_))
    ));
    tx.rollback().await.expect("rollback failed Postgres tx");
    assert_identity_absent(db, &missing_owner, &missing_name).await;

    let orphan_name = identity_name(&format!("orphan{suffix}"));
    let mut tx = db.begin().await.expect("begin orphan document tx");
    assert!(matches!(
        tx.identity_documents()
            .upsert(owner, &orphan_name, &document("orphan"), 21)
            .await,
        Err(AppError::Database(_))
    ));
    tx.rollback().await.expect("rollback failed Postgres tx");
    assert_document(db, owner, &orphan_name, None).await;

    let mut session = db;
    assert!(
        session
            .workspaces()
            .get(workspace.as_str())
            .await
            .expect("load surviving workspace")
            .is_some()
    );
}

async fn assert_rollback_invisibility(
    db: &CoralDb,
    suffix: &str,
    owner: &IdentityOwner,
    spec_reference: &IdentitySpecReference,
) {
    let rollback_name = identity_name(&format!("rollback{suffix}"));
    let mut tx = db.begin().await.expect("begin rollback pair tx");
    seed_identity(&mut tx, owner, &rollback_name, spec_reference, 30).await;
    seed_document(&mut tx, owner, &rollback_name, "rollback", 30).await;
    tx.rollback().await.expect("rollback identity pair");
    assert_identity_absent(db, owner, &rollback_name).await;
    assert_document(db, owner, &rollback_name, None).await;
}

async fn assert_document_corruption_matrix(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
) {
    assert_corrupt_document(
        db,
        owner,
        name,
        IdentityDocuments::DocumentVersion,
        zero_i64(),
    )
    .await;
    for column in [
        IdentityDocuments::Ciphertext,
        IdentityDocuments::Nonce,
        IdentityDocuments::WrappedDek,
        IdentityDocuments::WrappedDekNonce,
    ] {
        assert_corrupt_document(db, owner, name, column, Expr::val(Vec::<u8>::new())).await;
    }
    for (column, value) in [
        (IdentityDocuments::KeyId, ""),
        (IdentityDocuments::KeyId, " "),
        (IdentityDocuments::Algorithm, ""),
        (IdentityDocuments::Algorithm, " "),
    ] {
        assert_corrupt_document(db, owner, name, column, Expr::val(value)).await;
    }
    assert_corrupt_document(db, owner, name, IdentityDocuments::AadVersion, zero_i64()).await;
    assert_corrupt_document(
        db,
        owner,
        name,
        IdentityDocuments::CreatedAtUnixNanos,
        Expr::val(-1_i64),
    )
    .await;
    assert_corrupt_document(
        db,
        owner,
        name,
        IdentityDocuments::UpdatedAtUnixNanos,
        zero_i64(),
    )
    .await;
}

async fn assert_corrupt_identity(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    column: Identities,
    value: SimpleExpr,
) {
    let mut tx = db.begin().await.expect("begin corrupt identity tx");
    tx.execute(
        Query::update()
            .table(Identities::Table)
            .value(column, value)
            .and_where(identity_where(owner, name))
            .to_owned(),
    )
    .await
    .expect("corrupt identity");
    assert!(matches!(
        tx.identities().load_optional(owner, name).await,
        Err(DbError::CorruptData(_))
    ));
    assert!(matches!(
        tx.identities().list_for_owner(owner).await,
        Err(DbError::CorruptData(_))
    ));
    tx.rollback().await.expect("restore identity");
}

async fn assert_corrupt_identity_name(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    value: SimpleExpr,
) {
    let mut tx = db.begin().await.expect("begin corrupt identity name tx");
    tx.execute(
        Query::update()
            .table(Identities::Table)
            .value(Identities::Name, value)
            .and_where(identity_where(owner, name))
            .to_owned(),
    )
    .await
    .expect("corrupt identity name");
    assert!(matches!(
        tx.identities().list_for_owner(owner).await,
        Err(DbError::CorruptData(_))
    ));
    tx.rollback().await.expect("restore identity name");
}

async fn assert_corrupt_document(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    column: IdentityDocuments,
    value: SimpleExpr,
) {
    let mut tx = db.begin().await.expect("begin corrupt document tx");
    tx.execute(
        Query::update()
            .table(IdentityDocuments::Table)
            .value(column, value)
            .and_where(document_where(owner, name))
            .to_owned(),
    )
    .await
    .expect("corrupt document");
    assert!(matches!(
        tx.identity_documents().load_optional(owner, name).await,
        Err(DbError::CorruptData(_))
    ));
    tx.rollback().await.expect("restore document");
}

async fn assert_constraint_violations(
    db: &CoralDb,
    suffix: &str,
    owner: &IdentityOwner,
    constraint_name: &IdentityName,
    material_name: &IdentityName,
) {
    assert_violation(
        db,
        Query::update()
            .table(Identities::Table)
            .value(Identities::WorkspaceId, Option::<String>::None)
            .and_where(identity_where(owner, constraint_name))
            .to_owned(),
        Violation::Check,
    )
    .await;
    assert_violation(
        db,
        Query::update()
            .table(Identities::Table)
            .value(Identities::IdentitySpecScopeId, format!("other{suffix}"))
            .and_where(identity_where(owner, constraint_name))
            .to_owned(),
        Violation::Check,
    )
    .await;
    let missing_workspace = format!("missing_fk{suffix}");
    assert_violation(
        db,
        Query::update()
            .table(Identities::Table)
            .value(Identities::OwnerKey, missing_workspace.clone())
            .value(Identities::WorkspaceId, missing_workspace.clone())
            .value(Identities::IdentitySpecScopeId, missing_workspace)
            .and_where(identity_where(owner, constraint_name))
            .to_owned(),
        Violation::ForeignKey,
    )
    .await;
    assert_violation(
        db,
        Query::update()
            .table(IdentityDocuments::Table)
            .value(IdentityDocuments::Name, format!("orphan{suffix}"))
            .and_where(document_where(owner, material_name))
            .to_owned(),
        Violation::ForeignKey,
    )
    .await;
}

async fn assert_max_version_is_typed_and_nonmutating(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    original: &IdentityDocumentRecord,
) {
    let mut tx = db.begin().await.expect("begin max-version tx");
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
        .load_optional(owner, name)
        .await
        .expect("load max-version document")
        .expect("max-version document");
    let error = tx
        .identity_documents()
        .upsert(owner, name, &document("overflow"), 70)
        .await
        .expect_err("max version must not wrap");
    assert!(matches!(error, AppError::FailedPrecondition(_)));
    let after = tx
        .identity_documents()
        .load_optional(owner, name)
        .await
        .expect("reload max-version document")
        .expect("max-version document remains");
    assert_eq!(after, before);
    tx.rollback().await.expect("rollback max version");
    assert_document(db, owner, name, Some(original)).await;
}

enum Violation {
    Check,
    ForeignKey,
}

async fn assert_violation(db: &CoralDb, statement: UpdateStatement, expected: Violation) {
    let mut tx = db.begin().await.expect("begin violation tx");
    let error = tx
        .execute(statement)
        .await
        .expect_err("constraint must reject");
    let database = match error {
        DbError::Sqlx(sqlx::Error::Database(database)) => database,
        other => panic!("expected typed database error, got {other}"),
    };
    assert!(match expected {
        Violation::Check => database.is_check_violation(),
        Violation::ForeignKey => database.is_foreign_key_violation(),
    });
    tx.rollback().await.expect("rollback failed Postgres tx");
}

async fn assert_concurrent_document_versions(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    spec_reference: &IdentitySpecReference,
) {
    let mut tx = db.begin().await.expect("begin concurrency seed tx");
    seed_identity(&mut tx, owner, name, spec_reference, 80).await;
    seed_document(&mut tx, owner, name, "concurrent", 80).await;
    tx.commit().await.expect("commit concurrency seed");

    let barrier = tokio::sync::Barrier::new(2);
    let (left, right) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            replace_document(db, owner, name, "left", 81, &barrier),
            replace_document(db, owner, name, "right", 82, &barrier)
        )
    })
    .await
    .expect("concurrent identity document updates timed out");
    let mut versions = [left.1.document_version, right.1.document_version];
    versions.sort_unstable();
    assert_eq!(versions, [2, 3]);
    let winner = if left.1.document_version == 3 {
        &left
    } else {
        &right
    };
    let mut session = db;
    let persisted = session
        .identity_documents()
        .load_optional(owner, name)
        .await
        .expect("load concurrent document")
        .expect("concurrent document exists");
    assert_eq!(persisted, winner.1);
    assert_eq!(
        persisted,
        expected_document(owner, name, winner.0, 3, 80, 82)
    );
}

async fn replace_document(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    label: &'static str,
    now: i64,
    barrier: &tokio::sync::Barrier,
) -> (&'static str, IdentityDocumentRecord) {
    let mut tx = db.begin().await.expect("begin document update tx");
    barrier.wait().await;
    let record = tx
        .identity_documents()
        .upsert(owner, name, &document(label), now)
        .await
        .expect("replace identity document");
    tx.commit().await.expect("commit document update");
    (label, record)
}

fn identity_where(owner: &IdentityOwner, name: &IdentityName) -> SimpleExpr {
    Expr::col(Identities::OwnerKind)
        .eq(owner.kind())
        .and(Expr::col(Identities::OwnerKey).eq(owner.key()))
        .and(Expr::col(Identities::Name).eq(name.as_str()))
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
