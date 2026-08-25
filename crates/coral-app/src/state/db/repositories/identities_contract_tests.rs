use std::sync::Arc;

use sea_query::{Expr, ExprTrait, Query, SimpleExpr};
use tempfile::tempdir;

use super::identities::IdentityRecord;
use crate::bootstrap::AppError;
use crate::identities::model::{IdentityName, IdentityOwner, IdentitySpecReference};
use crate::identity::{Principal, PrincipalKind};
use crate::state::db::schema::Identities;
use crate::state::db::{
    CoralDb, CoralTx, DbError, DbRepos, IdentitySpecKey, ResolvedDatabaseConfig,
};
use crate::workspaces::WorkspaceName;

#[tokio::test(flavor = "current_thread")]
async fn identity_repository_contract_holds_against_sqlite() {
    let temp = tempdir().expect("temp dir");
    let db = Arc::new(
        CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite"),
    );
    db.migrate().await.expect("migrate sqlite");
    assert_identity_repository_contract(&db).await;
    assert_identity_repository_corruption_contract(&db).await;
    Box::pin(
        crate::identities::manager::tests::assert_user_global_fixed_token_create_contract(&db),
    )
    .await;
    Box::pin(crate::identities::manager::tests::assert_workspace_fixed_token_create_contract(&db))
        .await;
}

#[expect(clippy::too_many_lines, reason = "shared backend contract fixture")]
async fn assert_identity_repository_contract(db: &CoralDb) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let workspace = parsed_workspace(&format!("owner{suffix}"));
    let other_workspace = parsed_workspace(&format!("other{suffix}"));
    let reserved_workspace = parsed_workspace("__global__");
    let user = IdentityOwner::for_user(
        Principal::parse(workspace.as_str(), PrincipalKind::User).expect("matching user principal"),
    );
    let workspace_owner = IdentityOwner::workspace(workspace.clone());
    let other_owner = IdentityOwner::workspace(other_workspace.clone());
    let reserved_owner = IdentityOwner::workspace(reserved_workspace.clone());
    let shared = identity_name(&format!("shared{suffix}"));
    let fallback = identity_name(&format!("fallback{suffix}"));
    let scoped = identity_name(&format!("scoped{suffix}"));
    let reserved = identity_name(&format!("reserved{suffix}"));
    let rolled_back = identity_name(&format!("rollback{suffix}"));
    let spec_name = format!("sharedspec{suffix}");
    let global_key = IdentitySpecKey::global(&spec_name).expect("global key");
    let workspace_key =
        IdentitySpecKey::workspace(workspace.clone(), &spec_name).expect("workspace key");
    let other_workspace_key =
        IdentitySpecKey::workspace(other_workspace.clone(), &spec_name).expect("workspace key");
    let reserved_key =
        IdentitySpecKey::workspace(reserved_workspace.clone(), &spec_name).expect("reserved key");
    let replacement_key =
        IdentitySpecKey::global(&format!("replacement{suffix}")).expect("replacement key");
    let user_ref = reference(&user, global_key.clone(), "f1");
    let workspace_ref = reference(&workspace_owner, workspace_key.clone(), "f1");
    let other_ref = reference(&other_owner, global_key.clone(), "f2");
    let fallback_ref = reference(&workspace_owner, global_key.clone(), "f1");
    let other_workspace_ref = reference(&other_owner, other_workspace_key.clone(), "f4");
    let reserved_ref = reference(&reserved_owner, reserved_key.clone(), "f5");
    let replacement_ref = IdentitySpecReference::new(
        &workspace_owner,
        replacement_key.clone(),
        "f3",
        "replacement-issuer",
        "oauth",
    )
    .expect("replacement reference");

    let mut tx = db.begin().await.expect("begin seed transaction");
    for workspace in [&workspace, &other_workspace, &reserved_workspace] {
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("ensure workspace");
    }
    let user_row = seed_identity(&mut tx, &user, &shared, &user_ref, 10).await;
    let workspace_row = seed_identity(&mut tx, &workspace_owner, &shared, &workspace_ref, 11).await;
    let other_row = seed_identity(&mut tx, &other_owner, &shared, &other_ref, 12).await;
    let fallback_row = seed_identity(&mut tx, &workspace_owner, &fallback, &fallback_ref, 13).await;
    let other_scoped_row =
        seed_identity(&mut tx, &other_owner, &scoped, &other_workspace_ref, 14).await;
    let reserved_row = seed_identity(&mut tx, &reserved_owner, &reserved, &reserved_ref, 15).await;
    assert!(matches!(
        tx.identities().upsert(&user, &shared, &user_ref, -1).await,
        Err(AppError::InvalidInput(_))
    ));
    assert!(matches!(
        tx.identities()
            .upsert(&user, &shared, &workspace_ref, 20)
            .await,
        Err(AppError::InvalidInput(_))
    ));
    tx.commit().await.expect("commit seed transaction");

    assert_identity(db, &user, &shared, &user_row).await;
    assert_identity(db, &workspace_owner, &shared, &workspace_row).await;
    assert_identity(db, &other_owner, &shared, &other_row).await;
    assert_identity(db, &workspace_owner, &fallback, &fallback_row).await;
    assert_identity(db, &other_owner, &scoped, &other_scoped_row).await;
    assert_identity(db, &reserved_owner, &reserved, &reserved_row).await;
    assert_owner_names(db, &user, [&shared]).await;
    assert_owner_names(db, &workspace_owner, [&fallback, &shared]).await;
    assert_owner_names(db, &other_owner, [&scoped, &shared]).await;
    assert_owner_names(db, &reserved_owner, [&reserved]).await;
    assert_counts(db, &global_key, [("f1", 2), ("f2", 1), ("missing", 0)], 3).await;
    assert_counts(db, &workspace_key, [("f1", 1)], 1).await;
    assert_counts(db, &other_workspace_key, [("f4", 1)], 1).await;
    assert_counts(db, &reserved_key, [("f5", 1)], 1).await;
    assert_counts(db, &replacement_key, [("f3", 0)], 0).await;

    let expected_replacement = expected_record(&workspace_owner, &shared, &replacement_ref, 11, 30);
    let mut tx = db.begin().await.expect("begin replacement transaction");
    let replaced = tx
        .identities()
        .upsert(&workspace_owner, &shared, &replacement_ref, 30)
        .await
        .expect("replace identity");
    assert_eq!(replaced, expected_replacement);
    let regressed = tx
        .identities()
        .upsert(&workspace_owner, &shared, &replacement_ref, 5)
        .await
        .expect("replace under regressed clock");
    assert_eq!(regressed, expected_replacement);
    tx.commit().await.expect("commit replacement transaction");
    assert_identity(db, &workspace_owner, &shared, &expected_replacement).await;
    assert_counts(db, &global_key, [("f1", 2), ("f2", 1)], 3).await;
    assert_counts(db, &workspace_key, [("f1", 0)], 0).await;
    assert_counts(db, &replacement_key, [("f3", 1)], 1).await;

    let rollback_ref = reference(&user, global_key.clone(), "rolled-back");
    let mut tx = db.begin().await.expect("begin rollback transaction");
    tx.identities()
        .upsert(&user, &shared, &rollback_ref, 40)
        .await
        .expect("update rolled-back identity");
    assert!(
        tx.identities()
            .delete(&other_owner, &shared)
            .await
            .expect("delete rolled-back identity")
    );
    seed_identity(&mut tx, &user, &rolled_back, &user_ref, 40).await;
    tx.rollback().await.expect("rollback identity mutations");
    assert_identity(db, &user, &shared, &user_row).await;
    assert_identity(db, &other_owner, &shared, &other_row).await;
    assert_identity_absent(db, &user, &rolled_back).await;
    assert_counts(db, &global_key, [("f1", 2), ("f2", 1)], 3).await;

    let mut tx = db.begin().await.expect("begin exact delete transaction");
    assert!(
        tx.identities()
            .delete(&user, &shared)
            .await
            .expect("delete user")
    );
    assert!(
        !tx.identities()
            .delete(&user, &shared)
            .await
            .expect("delete missing")
    );
    tx.commit().await.expect("commit exact delete");
    assert_identity_absent(db, &user, &shared).await;
    assert_identity(db, &workspace_owner, &shared, &expected_replacement).await;
    assert_counts(db, &global_key, [("f1", 1), ("f2", 1)], 2).await;

    let mut tx = db.begin().await.expect("begin workspace cascade");
    tx.workspaces()
        .delete(workspace.as_str())
        .await
        .expect("delete workspace");
    tx.commit().await.expect("commit workspace cascade");
    assert_identity_absent(db, &workspace_owner, &shared).await;
    assert_identity_absent(db, &workspace_owner, &fallback).await;
    assert_identity(db, &other_owner, &shared, &other_row).await;
    assert_counts(db, &global_key, [("f1", 0), ("f2", 1)], 1).await;
    assert_counts(db, &replacement_key, [("f3", 0)], 0).await;

    assert_missing_workspace_rejected(db, &suffix, &global_key).await;
    let mut tx = db.begin().await.expect("begin cleanup");
    for workspace in [&other_workspace, &reserved_workspace] {
        tx.workspaces()
            .delete(workspace.as_str())
            .await
            .expect("delete remaining workspace");
    }
    tx.commit().await.expect("commit cleanup");
}

async fn assert_missing_workspace_rejected(db: &CoralDb, suffix: &str, key: &IdentitySpecKey) {
    let missing_workspace = parsed_workspace(&format!("missing{suffix}"));
    let owner = IdentityOwner::workspace(missing_workspace);
    let name = identity_name(&format!("missing{suffix}"));
    let reference = reference(&owner, key.clone(), "missing");
    let mut tx = db
        .begin()
        .await
        .expect("begin missing workspace transaction");
    assert!(matches!(
        tx.identities().upsert(&owner, &name, &reference, 50).await,
        Err(AppError::Database(_))
    ));
    tx.rollback().await.expect("rollback failed transaction");
    assert_identity_absent(db, &owner, &name).await;
}

async fn assert_identity_repository_corruption_contract(db: &CoralDb) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let workspace = parsed_workspace(&format!("corrupt{suffix}"));
    let owner = IdentityOwner::workspace(workspace.clone());
    let name = identity_name(&format!("identity{suffix}"));
    let key = IdentitySpecKey::global(&format!("spec{suffix}")).expect("global key");
    let reference = reference(&owner, key, "fingerprint");
    let mut tx = db.begin().await.expect("begin corruption seed");
    tx.workspaces()
        .ensure(workspace.as_str(), 1)
        .await
        .expect("ensure corruption workspace");
    seed_identity(&mut tx, &owner, &name, &reference, 10).await;
    tx.commit().await.expect("commit corruption seed");

    assert_corrupt_identity(
        db,
        &owner,
        &name,
        Identities::Name,
        Expr::val(format!(" bad{suffix}")),
        CorruptRead::List,
    )
    .await;
    for (column, value) in [
        (Identities::IdentitySpecName, Expr::val(" bad")),
        (Identities::IdentitySpecFingerprint, Expr::val("")),
        (Identities::Issuer, Expr::val(" ")),
        (Identities::IdentityType, Expr::val("unknown")),
        (Identities::CreatedAtUnixNanos, Expr::val(-1_i64)),
        (Identities::UpdatedAtUnixNanos, Expr::val(9_i64)),
    ] {
        assert_corrupt_identity(db, &owner, &name, column, value, CorruptRead::Get).await;
    }

    assert_identity(
        db,
        &owner,
        &name,
        &expected_record(&owner, &name, &reference, 10, 10),
    )
    .await;
    let mut tx = db.begin().await.expect("begin corruption cleanup");
    tx.workspaces()
        .delete(workspace.as_str())
        .await
        .expect("delete corruption workspace");
    tx.commit().await.expect("commit corruption cleanup");
}

#[derive(Clone, Copy)]
enum CorruptRead {
    Get,
    List,
}

async fn assert_corrupt_identity(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    column: Identities,
    value: SimpleExpr,
    read: CorruptRead,
) {
    let mut tx = db
        .begin()
        .await
        .expect("begin corrupt identity transaction");
    tx.execute(
        Query::update()
            .table(Identities::Table)
            .value(column, value)
            .and_where(identity_where(owner, name))
            .to_owned(),
    )
    .await
    .expect("corrupt identity");
    let error = match read {
        CorruptRead::Get => tx
            .identities()
            .get(owner, name)
            .await
            .expect_err("corrupt get"),
        CorruptRead::List => tx.identities().list(owner).await.expect_err("corrupt list"),
    };
    assert!(matches!(error, DbError::CorruptData(_)));
    tx.rollback().await.expect("restore identity");
}

fn identity_where(owner: &IdentityOwner, name: &IdentityName) -> SimpleExpr {
    Expr::col(Identities::OwnerKind)
        .eq(owner.kind())
        .and(Expr::col(Identities::OwnerKey).eq(owner.key()))
        .and(Expr::col(Identities::Name).eq(name.as_str()))
}

async fn seed_identity(
    tx: &mut CoralTx<'_>,
    owner: &IdentityOwner,
    name: &IdentityName,
    reference: &IdentitySpecReference,
    now: i64,
) -> IdentityRecord {
    let record = tx
        .identities()
        .upsert(owner, name, reference, now)
        .await
        .expect("seed identity");
    assert_eq!(record, expected_record(owner, name, reference, now, now));
    record
}

fn expected_record(
    owner: &IdentityOwner,
    name: &IdentityName,
    reference: &IdentitySpecReference,
    created_at_unix_nanos: i64,
    updated_at_unix_nanos: i64,
) -> IdentityRecord {
    IdentityRecord {
        owner: owner.clone(),
        name: name.clone(),
        spec_reference: reference.clone(),
        created_at_unix_nanos,
        updated_at_unix_nanos,
    }
}

async fn assert_identity(
    db: &CoralDb,
    owner: &IdentityOwner,
    name: &IdentityName,
    expected: &IdentityRecord,
) {
    let mut session = db;
    assert_eq!(
        session
            .identities()
            .get(owner, name)
            .await
            .expect("get identity")
            .as_ref(),
        Some(expected)
    );
}

async fn assert_identity_absent(db: &CoralDb, owner: &IdentityOwner, name: &IdentityName) {
    let mut session = db;
    assert!(
        session
            .identities()
            .get(owner, name)
            .await
            .expect("get identity")
            .is_none()
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
        .list(owner)
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
            .expect("count"),
        total
    );
    for (fingerprint, count) in expected {
        assert_eq!(
            session
                .identities()
                .count_exact_dependents(key, fingerprint)
                .await
                .expect("count exact"),
            count
        );
    }
}

fn parsed_workspace(value: &str) -> WorkspaceName {
    WorkspaceName::parse(value).expect("workspace")
}

fn identity_name(value: &str) -> IdentityName {
    IdentityName::parse(value).expect("identity name")
}

fn reference(
    owner: &IdentityOwner,
    key: IdentitySpecKey,
    fingerprint: &str,
) -> IdentitySpecReference {
    IdentitySpecReference::new(owner, key, fingerprint, "issuer", "fixed_token")
        .expect("identity reference")
}
