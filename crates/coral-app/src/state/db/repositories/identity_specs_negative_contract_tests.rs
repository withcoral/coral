use sea_query::{Expr, ExprTrait, Query, SimpleExpr, UpdateStatement};
use tempfile::tempdir;

use super::identity_specs::{IdentitySpecId, IdentitySpecKey};
use super::identity_specs_contract_tests::{document, upsert_spec};
use crate::bootstrap;
use crate::state::db::schema::{IdentitySpecDocuments, IdentitySpecs};
use crate::state::db::{CoralDb, DbError, DbRepos, ResolvedDatabaseConfig};
use crate::workspaces::WorkspaceName;

#[tokio::test]
async fn identity_spec_corruption_contract_holds_against_sqlite() {
    let temp = tempdir().expect("temp dir");
    let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
        path: temp.path().join("coral.sqlite"),
    })
    .await
    .expect("open sqlite");
    db.migrate().await.expect("migrate sqlite");
    assert_identity_spec_corruption_contract(&db).await;
}

#[tokio::test]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared corruption contract against Postgres"]
async fn identity_spec_corruption_contract_on_postgres() {
    let Some(url) = postgres_test_url() else {
        return;
    };
    let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
        .await
        .expect("open postgres");
    db.migrate().await.expect("migrate postgres");
    assert_identity_spec_corruption_contract(&db).await;
}

#[expect(clippy::too_many_lines, reason = "shared backend corruption contract")]
async fn assert_identity_spec_corruption_contract(db: &CoralDb) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let workspace = WorkspaceName::parse(&format!("negative{suffix}")).expect("workspace");
    let key = scoped_key(&workspace, &format!("corrupt_{suffix}"));
    let constraint_key = scoped_key(&workspace, &format!("constraint_{suffix}"));
    let global_key = IdentitySpecKey::global(&format!("global_{suffix}")).expect("global key");
    let global_constraint_key =
        IdentitySpecKey::global(&format!("global_constraint_{suffix}")).expect("global key");

    let mut tx = db.begin().await.expect("begin seed transaction");
    tx.workspaces()
        .ensure(workspace.as_str(), 1)
        .await
        .expect("ensure workspace");
    let mut records = Vec::new();
    for key in [&key, &constraint_key, &global_key, &global_constraint_key] {
        records.push(
            upsert_spec(&mut tx, key, "negative", 1)
                .await
                .expect("seed spec"),
        );
    }
    let identity_spec_id = records.first().expect("seeded identity spec").id.clone();
    tx.identity_spec_documents()
        .upsert(&identity_spec_id, &document("negative"), 1)
        .await
        .expect("seed document");
    tx.commit().await.expect("commit seed");

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

    assert_corrupt_document(
        db,
        &identity_spec_id,
        IdentitySpecDocuments::DocumentVersion,
        zero_i64(),
    )
    .await;
    for column in [
        IdentitySpecDocuments::Ciphertext,
        IdentitySpecDocuments::Nonce,
        IdentitySpecDocuments::WrappedDek,
        IdentitySpecDocuments::WrappedDekNonce,
    ] {
        assert_corrupt_document(db, &identity_spec_id, column, Expr::val(Vec::<u8>::new())).await;
    }
    for (column, value) in [
        (IdentitySpecDocuments::KeyId, ""),
        (IdentitySpecDocuments::KeyId, " "),
        (IdentitySpecDocuments::Algorithm, ""),
        (IdentitySpecDocuments::Algorithm, " "),
    ] {
        assert_corrupt_document(db, &identity_spec_id, column, Expr::val(value)).await;
    }
    assert_corrupt_document(
        db,
        &identity_spec_id,
        IdentitySpecDocuments::BindingVersion,
        zero_i64(),
    )
    .await;
    assert_corrupt_document(
        db,
        &identity_spec_id,
        IdentitySpecDocuments::CreatedAtUnixNanos,
        Expr::val(-1_i64),
    )
    .await;
    assert_corrupt_document(
        db,
        &identity_spec_id,
        IdentitySpecDocuments::UpdatedAtUnixNanos,
        zero_i64(),
    )
    .await;

    let mut session = db;
    session
        .identity_specs()
        .get(&key)
        .await
        .expect("read restored spec")
        .expect("spec restored");
    session
        .identity_spec_documents()
        .get(&identity_spec_id)
        .await
        .expect("read restored document")
        .expect("document restored");

    assert_corrupt_spec(
        db,
        &constraint_key,
        IdentitySpecs::Id,
        Expr::val("not-a-uuid"),
    )
    .await;
    assert_violation(
        db,
        Query::update()
            .table(IdentitySpecs::Table)
            .value(IdentitySpecs::Name, key.name())
            .and_where(spec_where(&constraint_key))
            .to_owned(),
        Violation::Unique,
    )
    .await;
    assert_violation(
        db,
        Query::update()
            .table(IdentitySpecs::Table)
            .value(IdentitySpecs::Name, global_key.name())
            .and_where(spec_where(&global_constraint_key))
            .to_owned(),
        Violation::Unique,
    )
    .await;
    let missing_workspace = format!("missing{suffix}");
    assert_violation(
        db,
        Query::update()
            .table(IdentitySpecs::Table)
            .value(IdentitySpecs::WorkspaceId, missing_workspace)
            .and_where(spec_where(&constraint_key))
            .to_owned(),
        Violation::ForeignKey,
    )
    .await;
    let orphan_id = IdentitySpecId::new();
    assert_violation(
        db,
        Query::update()
            .table(IdentitySpecDocuments::Table)
            .value(IdentitySpecDocuments::IdentitySpecId, orphan_id.as_str())
            .and_where(document_where(&identity_spec_id))
            .to_owned(),
        Violation::ForeignKey,
    )
    .await;

    let mut tx = db.begin().await.expect("begin cleanup");
    for key in [&key, &constraint_key, &global_key, &global_constraint_key] {
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
    let mut tx = db.begin().await.expect("begin corrupt spec transaction");
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
        tx.identity_specs().get(key).await,
        Err(DbError::CorruptData(_))
    ));
    tx.rollback().await.expect("restore spec");
}

async fn assert_corrupt_document(
    db: &CoralDb,
    identity_spec_id: &IdentitySpecId,
    column: IdentitySpecDocuments,
    value: SimpleExpr,
) {
    let mut tx = db
        .begin()
        .await
        .expect("begin corrupt document transaction");
    tx.execute(
        Query::update()
            .table(IdentitySpecDocuments::Table)
            .value(column, value)
            .and_where(document_where(identity_spec_id))
            .to_owned(),
    )
    .await
    .expect("corrupt document");
    assert!(matches!(
        tx.identity_spec_documents().get(identity_spec_id).await,
        Err(DbError::CorruptData(_))
    ));
    tx.rollback().await.expect("restore document");
}

enum Violation {
    ForeignKey,
    Unique,
}

async fn assert_violation(db: &CoralDb, statement: UpdateStatement, expected: Violation) {
    let mut tx = db
        .begin()
        .await
        .expect("begin constraint violation transaction");
    let error = tx
        .execute(statement)
        .await
        .expect_err("constraint must reject");
    let database = match error {
        DbError::Sqlx(sqlx::Error::Database(database)) => database,
        other => panic!("expected typed database error, got {other}"),
    };
    assert!(match expected {
        Violation::ForeignKey => database.is_foreign_key_violation(),
        Violation::Unique => database.is_unique_violation(),
    });
    tx.rollback().await.expect("rollback failed transaction");
}

fn spec_where(key: &IdentitySpecKey) -> SimpleExpr {
    let scope = match key.scope().workspace_id() {
        None => Expr::col(IdentitySpecs::WorkspaceId).is_null(),
        Some(workspace_id) => Expr::col(IdentitySpecs::WorkspaceId).eq(workspace_id),
    };
    scope.and(Expr::col(IdentitySpecs::Name).eq(key.name()))
}

fn document_where(identity_spec_id: &IdentitySpecId) -> SimpleExpr {
    Expr::col(IdentitySpecDocuments::IdentitySpecId).eq(identity_spec_id.as_str())
}

fn zero_i64() -> SimpleExpr {
    Expr::val(0_i64)
}

fn scoped_key(workspace: &WorkspaceName, name: &str) -> IdentitySpecKey {
    IdentitySpecKey::workspace(workspace.clone(), name).expect("workspace key")
}

fn postgres_test_url() -> Option<String> {
    bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
        .expect("read CORAL_TEST_POSTGRES_URL")
        .filter(|value| !value.is_empty())
}
