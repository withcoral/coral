use sea_query::{Expr, ExprTrait, Query, SimpleExpr, UpdateStatement};
use tempfile::tempdir;

use super::identity_specs::{IdentitySpecDocumentWrite, IdentitySpecKey, IdentitySpecWrite};
use super::identity_specs_contract_tests::{document, spec};
use crate::bootstrap::AppError;
use crate::state::db::schema::{IdentitySpecDocuments, IdentitySpecs};
use crate::state::db::{CoralDb, DbError, DbRepos, ResolvedDatabaseConfig};
use crate::workspaces::WorkspaceName;

#[tokio::test]
async fn identity_spec_negative_contract_holds_against_sqlite() {
    let temp = tempdir().expect("temp dir");
    let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
        path: temp.path().join("coral.sqlite"),
    })
    .await
    .expect("open sqlite");
    db.migrate().await.expect("migrate sqlite");
    assert_identity_spec_negative_contract(&db).await;
}

#[test]
fn identity_spec_writes_reject_every_invalid_required_field() {
    const CIPHERTEXT: &[u8] = b"ciphertext";
    const NONCE: &[u8] = b"nonce";
    const WRAPPED_DEK: &[u8] = b"wrapped-dek";
    const WRAPPED_NONCE: &[u8] = b"wrapped-nonce";

    for (version, issuer, identity_type, manifest_yaml) in [
        ("", "issuer", "type", "manifest"),
        (" ", "issuer", "type", "manifest"),
        ("version", "", "type", "manifest"),
        ("version", "issuer", "", "manifest"),
        ("version", "issuer", "type", ""),
    ] {
        assert!(matches!(
            IdentitySpecWrite::new(version, "description", issuer, identity_type, manifest_yaml),
            Err(AppError::InvalidInput(_))
        ));
    }
    assert_invalid_document(b"", NONCE, WRAPPED_DEK, WRAPPED_NONCE, "key", "algo", 1);
    assert_invalid_document(
        CIPHERTEXT,
        b"",
        WRAPPED_DEK,
        WRAPPED_NONCE,
        "key",
        "algo",
        1,
    );
    assert_invalid_document(CIPHERTEXT, NONCE, b"", WRAPPED_NONCE, "key", "algo", 1);
    assert_invalid_document(CIPHERTEXT, NONCE, WRAPPED_DEK, b"", "key", "algo", 1);
    assert_invalid_document(CIPHERTEXT, NONCE, WRAPPED_DEK, WRAPPED_NONCE, "", "algo", 1);
    assert_invalid_document(
        CIPHERTEXT,
        NONCE,
        WRAPPED_DEK,
        WRAPPED_NONCE,
        " ",
        "algo",
        1,
    );
    assert_invalid_document(CIPHERTEXT, NONCE, WRAPPED_DEK, WRAPPED_NONCE, "key", "", 1);
    assert_invalid_document(CIPHERTEXT, NONCE, WRAPPED_DEK, WRAPPED_NONCE, "key", " ", 1);
    assert_invalid_document(
        CIPHERTEXT,
        NONCE,
        WRAPPED_DEK,
        WRAPPED_NONCE,
        "key",
        "algo",
        0,
    );
}

#[expect(clippy::too_many_lines, reason = "shared backend negative contract")]
pub(in crate::state::db) async fn assert_identity_spec_negative_contract(db: &CoralDb) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let workspace = WorkspaceName::parse(&format!("negative{suffix}")).expect("workspace");
    let key = IdentitySpecKey::workspace(workspace.clone(), &format!("corrupt_{suffix}"))
        .expect("corrupt key");
    let constraint_key =
        IdentitySpecKey::workspace(workspace.clone(), &format!("constraint_{suffix}"))
            .expect("constraint key");

    let mut tx = db.begin().await.expect("begin seed tx");
    tx.workspaces()
        .ensure(workspace.as_str(), 1)
        .await
        .expect("ensure workspace");
    let spec = spec("negative");
    for key in [&key, &constraint_key] {
        tx.identity_specs()
            .upsert(key, &spec, 1)
            .await
            .expect("seed spec");
    }
    tx.identity_spec_documents()
        .upsert(&key, &document("negative"), 1)
        .await
        .expect("seed document");
    tx.commit().await.expect("commit seed");

    let document = document("negative");
    let mut tx = db.begin().await.expect("begin invalid timestamp tx");
    assert!(matches!(
        tx.identity_specs().upsert(&key, &spec, -1).await,
        Err(AppError::InvalidInput(_))
    ));
    assert!(matches!(
        tx.identity_spec_documents()
            .upsert(&key, &document, -1)
            .await,
        Err(AppError::InvalidInput(_))
    ));
    tx.rollback().await.expect("rollback invalid timestamps");

    for column in [
        IdentitySpecs::Version,
        IdentitySpecs::Issuer,
        IdentitySpecs::IdentityType,
        IdentitySpecs::ManifestYaml,
    ] {
        assert_corrupt_spec(db, &key, column, Expr::val("")).await;
    }
    assert_corrupt_spec(db, &key, IdentitySpecs::Version, Expr::val(" ")).await;
    assert_corrupt_spec(
        db,
        &key,
        IdentitySpecs::CreatedAtUnixNanos,
        Expr::val(-1_i64),
    )
    .await;
    assert_corrupt_spec(db, &key, IdentitySpecs::UpdatedAtUnixNanos, zero_i64()).await;
    assert_corrupt_document(db, &key, IdentitySpecDocuments::DocumentVersion, zero_i64()).await;
    for column in [
        IdentitySpecDocuments::Ciphertext,
        IdentitySpecDocuments::Nonce,
        IdentitySpecDocuments::WrappedDek,
        IdentitySpecDocuments::WrappedDekNonce,
    ] {
        assert_corrupt_document(db, &key, column, Expr::val(Vec::<u8>::new())).await;
    }
    for column in [
        IdentitySpecDocuments::KeyId,
        IdentitySpecDocuments::Algorithm,
    ] {
        assert_corrupt_document(db, &key, column, Expr::val("")).await;
    }
    assert_corrupt_document(db, &key, IdentitySpecDocuments::KeyId, Expr::val(" ")).await;
    assert_corrupt_document(db, &key, IdentitySpecDocuments::Algorithm, Expr::val(" ")).await;
    assert_corrupt_document(db, &key, IdentitySpecDocuments::AadVersion, zero_i64()).await;
    assert_corrupt_document(
        db,
        &key,
        IdentitySpecDocuments::CreatedAtUnixNanos,
        Expr::val(-1_i64),
    )
    .await;
    assert_corrupt_document(
        db,
        &key,
        IdentitySpecDocuments::UpdatedAtUnixNanos,
        zero_i64(),
    )
    .await;
    let mut session = db;
    session
        .identity_specs()
        .load_optional(&key)
        .await
        .expect("read restored spec")
        .expect("spec restored");
    session
        .identity_spec_documents()
        .load_optional(&key)
        .await
        .expect("read restored document")
        .expect("document restored");

    assert_violation(
        db,
        Query::update()
            .table(IdentitySpecs::Table)
            .value(IdentitySpecs::ScopeKind, "global")
            .and_where(spec_where(&constraint_key))
            .to_owned(),
        Violation::Check,
    )
    .await;
    assert_violation(
        db,
        Query::update()
            .table(IdentitySpecs::Table)
            .value(IdentitySpecs::WorkspaceId, Option::<String>::None)
            .and_where(spec_where(&constraint_key))
            .to_owned(),
        Violation::Check,
    )
    .await;
    let missing_workspace = format!("missing{suffix}");
    assert_violation(
        db,
        Query::update()
            .table(IdentitySpecs::Table)
            .value(IdentitySpecs::ScopeId, missing_workspace.clone())
            .value(IdentitySpecs::WorkspaceId, missing_workspace)
            .and_where(spec_where(&constraint_key))
            .to_owned(),
        Violation::ForeignKey,
    )
    .await;
    assert_violation(
        db,
        Query::update()
            .table(IdentitySpecDocuments::Table)
            .value(IdentitySpecDocuments::Name, format!("orphan_{suffix}"))
            .and_where(document_where(&key))
            .to_owned(),
        Violation::ForeignKey,
    )
    .await;

    let mut tx = db.begin().await.expect("begin cleanup");
    for key in [&key, &constraint_key] {
        assert!(tx.identity_specs().delete(key).await.expect("delete spec"));
    }
    tx.workspaces()
        .delete(workspace.as_str())
        .await
        .expect("delete workspace");
    tx.commit().await.expect("commit cleanup");
}

async fn assert_corrupt_spec(
    db: &CoralDb,
    key: &IdentitySpecKey,
    column: IdentitySpecs,
    value: SimpleExpr,
) {
    let mut tx = db.begin().await.expect("begin corrupt spec tx");
    tx.execute(
        Query::update()
            .table(IdentitySpecs::Table)
            .value(column, value)
            .and_where(spec_where(key))
            .to_owned(),
    )
    .await
    .expect("corrupt spec");
    assert!(matches!(
        tx.identity_specs().load_optional(key).await,
        Err(DbError::CorruptData(_))
    ));
    tx.rollback().await.expect("restore spec");
}

async fn assert_corrupt_document(
    db: &CoralDb,
    key: &IdentitySpecKey,
    column: IdentitySpecDocuments,
    value: SimpleExpr,
) {
    let mut tx = db.begin().await.expect("begin corrupt document tx");
    tx.execute(
        Query::update()
            .table(IdentitySpecDocuments::Table)
            .value(column, value)
            .and_where(document_where(key))
            .to_owned(),
    )
    .await
    .expect("corrupt document");
    assert!(matches!(
        tx.identity_spec_documents().load_optional(key).await,
        Err(DbError::CorruptData(_))
    ));
    tx.rollback().await.expect("restore document");
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

fn spec_where(key: &IdentitySpecKey) -> SimpleExpr {
    Expr::col(IdentitySpecs::ScopeKind)
        .eq(key.scope().kind())
        .and(Expr::col(IdentitySpecs::ScopeId).eq(key.scope().scope_id()))
        .and(Expr::col(IdentitySpecs::Name).eq(key.name()))
}

fn document_where(key: &IdentitySpecKey) -> SimpleExpr {
    Expr::col(IdentitySpecDocuments::ScopeKind)
        .eq(key.scope().kind())
        .and(Expr::col(IdentitySpecDocuments::ScopeId).eq(key.scope().scope_id()))
        .and(Expr::col(IdentitySpecDocuments::Name).eq(key.name()))
}

fn zero_i64() -> SimpleExpr {
    Expr::val(0_i64)
}

fn assert_invalid_document(
    ciphertext: &[u8],
    nonce: &[u8],
    wrapped_dek: &[u8],
    wrapped_dek_nonce: &[u8],
    key_id: &str,
    algorithm: &str,
    aad_version: i64,
) {
    assert!(matches!(
        IdentitySpecDocumentWrite::new(
            ciphertext.to_vec(),
            nonce.to_vec(),
            wrapped_dek.to_vec(),
            wrapped_dek_nonce.to_vec(),
            key_id,
            algorithm,
            aad_version,
        ),
        Err(AppError::InvalidInput(_))
    ));
}
