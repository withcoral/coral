use std::time::Duration;

use sea_query::{Expr, ExprTrait, Query};
use tempfile::tempdir;

use super::identity_specs::{
    IdentitySpecDocumentRecord, IdentitySpecDocumentWrite, IdentitySpecKey, IdentitySpecRecord,
    IdentitySpecWrite,
};
use crate::bootstrap::AppError;
use crate::state::db::schema::IdentitySpecDocuments;
use crate::state::db::{CoralDb, CoralTx, DbRepos, ResolvedDatabaseConfig};
use crate::workspaces::WorkspaceName;

#[tokio::test]
async fn identity_spec_write_contract_holds_against_sqlite() {
    let temp = tempdir().expect("temp dir");
    let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
        path: temp.path().join("coral.sqlite"),
    })
    .await
    .expect("open sqlite");
    db.migrate().await.expect("migrate sqlite");
    assert_identity_spec_write_contract(&db).await;
}

#[expect(clippy::too_many_lines, reason = "shared backend contract fixture")]
pub(in crate::state::db) async fn assert_identity_spec_write_contract(db: &CoralDb) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let workspace = parsed_workspace(&format!("identity{suffix}"));
    let alternate = parsed_workspace(&format!("alternate{suffix}"));
    let reserved = parsed_workspace("__global__");
    let name = format!("shared_{suffix}");
    let global_key = IdentitySpecKey::global(&name).expect("global key");
    let workspace_key = scoped_key(&workspace, &name);
    let alternate_key = scoped_key(&alternate, &name);
    let reserved_key = scoped_key(&reserved, &name);

    let mut tx = db.begin().await.expect("begin seed tx");
    for workspace in [&workspace, &alternate, &reserved] {
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("ensure workspace");
    }
    seed_pair(&mut tx, &global_key, "global", 10).await;
    tx.identity_specs()
        .upsert(&global_key, &spec("replacement"), 5)
        .await
        .expect("replace spec with older timestamp");
    let replacement_write = document("replacement");
    let replacement_document = tx
        .identity_spec_documents()
        .upsert(&global_key, &replacement_write, 5)
        .await
        .expect("replace document with older timestamp");
    for debug in [
        format!("{replacement_document:?}"),
        format!("{replacement_write:?}"),
    ] {
        for secret in ["cipher-", "key-", "algo-", "["] {
            assert!(!debug.contains(secret), "Debug leaked {secret}: {debug}");
        }
    }
    seed_pair(&mut tx, &workspace_key, "workspace", 20).await;
    seed_pair(&mut tx, &alternate_key, "alternate", 30).await;
    seed_pair(&mut tx, &reserved_key, "reserved", 40).await;
    tx.commit().await.expect("commit seed tx");

    for (key, label, now, document_version) in [
        (&global_key, "replacement", 10, 2),
        (&workspace_key, "workspace", 20, 1),
        (&alternate_key, "alternate", 30, 1),
        (&reserved_key, "reserved", 40, 1),
    ] {
        assert_pair(db, key, label, now, document_version).await;
    }
    let mut tx = db.begin().await.expect("begin delete tx");
    assert!(
        tx.identity_spec_documents()
            .delete(&alternate_key)
            .await
            .expect("delete document")
    );
    assert!(
        !tx.identity_spec_documents()
            .delete(&alternate_key)
            .await
            .expect("repeat document delete")
    );
    assert!(
        tx.identity_specs()
            .load_optional(&alternate_key)
            .await
            .expect("load surviving spec")
            .is_some()
    );
    tx.identity_spec_documents()
        .upsert(&alternate_key, &document("alternate"), 31)
        .await
        .expect("restore alternate document");
    assert!(
        tx.identity_specs()
            .delete(&workspace_key)
            .await
            .expect("delete exact spec")
    );
    assert!(
        !tx.identity_specs()
            .delete(&workspace_key)
            .await
            .expect("repeat exact spec delete")
    );
    assert_document_presence(
        &mut tx,
        [
            (&global_key, true),
            (&workspace_key, false),
            (&alternate_key, true),
            (&reserved_key, true),
        ],
    )
    .await;
    tx.commit().await.expect("commit exact delete");

    let mut tx = db.begin().await.expect("begin workspace cascade tx");
    tx.workspaces()
        .delete(alternate.as_str())
        .await
        .expect("delete alternate workspace");
    tx.commit().await.expect("commit workspace cascade");
    assert_absent(db, &alternate_key).await;
    assert_pair(db, &global_key, "replacement", 10, 2).await;
    assert_pair(db, &reserved_key, "reserved", 40, 1).await;

    assert_foreign_keys_and_rollback(db, &suffix, &reserved).await;
    assert_rollback_invisibility(db, &suffix).await;
    assert_max_version_is_typed_and_nonmutating(db, &reserved_key).await;
    let concurrent_key =
        IdentitySpecKey::global(&format!("concurrent_{suffix}")).expect("concurrency key");
    assert_concurrent_document_versions(db, &concurrent_key).await;

    let mut tx = db.begin().await.expect("begin cleanup tx");
    for key in [&global_key, &reserved_key, &concurrent_key] {
        assert!(
            tx.identity_specs()
                .delete(key)
                .await
                .expect("delete remaining spec")
        );
    }
    for workspace in [&workspace, &reserved] {
        tx.workspaces()
            .delete(workspace.as_str())
            .await
            .expect("delete workspace");
    }
    tx.commit().await.expect("commit cleanup");
}

async fn assert_foreign_keys_and_rollback(
    db: &CoralDb,
    suffix: &str,
    existing_workspace: &WorkspaceName,
) {
    let missing_workspace = parsed_workspace(&format!("missing{suffix}"));
    let missing_spec = scoped_key(&missing_workspace, &format!("missing_{suffix}"));
    let mut tx = db.begin().await.expect("begin missing workspace tx");
    assert!(matches!(
        tx.identity_specs()
            .upsert(&missing_spec, &spec("missing"), 50)
            .await,
        Err(AppError::Database(_))
    ));
    tx.rollback().await.expect("rollback failed Postgres tx");
    assert_absent(db, &missing_spec).await;

    let orphan = scoped_key(existing_workspace, &format!("orphan_{suffix}"));
    let mut tx = db.begin().await.expect("begin orphan document tx");
    assert!(matches!(
        tx.identity_spec_documents()
            .upsert(&orphan, &document("orphan"), 51)
            .await,
        Err(AppError::Database(_))
    ));
    tx.rollback().await.expect("rollback failed Postgres tx");
    assert_absent(db, &orphan).await;
}

async fn assert_rollback_invisibility(db: &CoralDb, suffix: &str) {
    let key = IdentitySpecKey::global(&format!("rollback_{suffix}")).expect("rollback key");
    let mut tx = db.begin().await.expect("begin rollback tx");
    seed_pair(&mut tx, &key, "rollback", 60).await;
    tx.rollback().await.expect("rollback pair");
    assert_absent(db, &key).await;
}

async fn assert_max_version_is_typed_and_nonmutating(db: &CoralDb, key: &IdentitySpecKey) {
    let mut tx = db.begin().await.expect("begin max-version tx");
    tx.execute(
        Query::update()
            .table(IdentitySpecDocuments::Table)
            .value(IdentitySpecDocuments::DocumentVersion, i64::MAX)
            .and_where(document_key_where(key))
            .to_owned(),
    )
    .await
    .expect("set max version");
    let before = tx
        .identity_spec_documents()
        .load_optional(key)
        .await
        .expect("load max-version document")
        .expect("max-version document");
    let error = tx
        .identity_spec_documents()
        .upsert(key, &document("overflow"), 70)
        .await
        .expect_err("max version must not wrap");
    assert!(matches!(error, AppError::FailedPrecondition(_)));
    let after = tx
        .identity_spec_documents()
        .load_optional(key)
        .await
        .expect("reload max-version document")
        .expect("max-version document remains");
    assert_eq!(after, before);
    tx.rollback().await.expect("rollback max version");
}

async fn assert_concurrent_document_versions(db: &CoralDb, key: &IdentitySpecKey) {
    let mut tx = db.begin().await.expect("begin concurrency seed");
    seed_pair(&mut tx, key, "concurrent", 80).await;
    tx.commit().await.expect("commit concurrency seed");
    let barrier = tokio::sync::Barrier::new(2);
    let (left, right) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            replace_document(db, key, "left", 81, &barrier),
            replace_document(db, key, "right", 82, &barrier)
        )
    })
    .await
    .expect("concurrent document updates timed out");
    let mut versions = [left, right];
    versions.sort_unstable();
    assert_eq!(versions, [2, 3]);
}

async fn replace_document(
    db: &CoralDb,
    key: &IdentitySpecKey,
    label: &str,
    now: i64,
    barrier: &tokio::sync::Barrier,
) -> i64 {
    let mut tx = db.begin().await.expect("begin document update");
    barrier.wait().await;
    let version = tx
        .identity_spec_documents()
        .upsert(key, &document(label), now)
        .await
        .expect("update document")
        .document_version;
    tx.commit().await.expect("commit document update");
    version
}

async fn seed_pair(
    tx: &mut CoralTx<'_>,
    key: &IdentitySpecKey,
    label: &str,
    now: i64,
) -> (IdentitySpecRecord, IdentitySpecDocumentRecord) {
    let spec = tx
        .identity_specs()
        .upsert(key, &spec(label), now)
        .await
        .expect("upsert spec");
    let document = tx
        .identity_spec_documents()
        .upsert(key, &document(label), now)
        .await
        .expect("upsert document");
    (spec, document)
}

async fn assert_pair(
    db: &CoralDb,
    key: &IdentitySpecKey,
    label: &str,
    now: i64,
    document_version: i64,
) {
    let mut session = db;
    let spec = session
        .identity_specs()
        .load_optional(key)
        .await
        .expect("load spec")
        .expect("spec exists");
    let document = session
        .identity_spec_documents()
        .load_optional(key)
        .await
        .expect("load document")
        .expect("document exists");
    assert_eq!(
        spec,
        IdentitySpecRecord {
            key: key.clone(),
            version: format!("v-{label}"),
            description: format!("description-{label}"),
            issuer: format!("issuer-{label}"),
            identity_type: format!("type-{label}"),
            manifest_yaml: format!("kind: identity\nname: {label}\n"),
            created_at_unix_nanos: now,
            updated_at_unix_nanos: now,
        }
    );
    assert_eq!(
        document,
        IdentitySpecDocumentRecord {
            key: key.clone(),
            document_version,
            ciphertext: format!("cipher-{label}").into_bytes(),
            nonce: format!("nonce-{label}").into_bytes(),
            wrapped_dek: format!("wrapped-{label}").into_bytes(),
            wrapped_dek_nonce: format!("wrapped-nonce-{label}").into_bytes(),
            key_id: format!("key-{label}"),
            algorithm: format!("algo-{label}"),
            aad_version: 99,
            created_at_unix_nanos: now,
            updated_at_unix_nanos: now,
        }
    );
}

async fn assert_absent(db: &CoralDb, key: &IdentitySpecKey) {
    let mut session = db;
    assert!(
        session
            .identity_specs()
            .load_optional(key)
            .await
            .expect("load absent spec")
            .is_none()
    );
    assert!(
        session
            .identity_spec_documents()
            .load_optional(key)
            .await
            .expect("load absent document")
            .is_none()
    );
}

async fn assert_document_presence<const N: usize>(
    tx: &mut CoralTx<'_>,
    expected: [(&IdentitySpecKey, bool); N],
) {
    for (key, present) in expected {
        assert_eq!(
            tx.identity_spec_documents()
                .load_optional(key)
                .await
                .expect("load document")
                .is_some(),
            present
        );
    }
}

fn spec(label: &str) -> IdentitySpecWrite {
    IdentitySpecWrite::new(
        format!("v-{label}"),
        format!("description-{label}"),
        format!("issuer-{label}"),
        format!("type-{label}"),
        format!("kind: identity\nname: {label}\n"),
    )
    .expect("valid spec")
}

fn document(label: &str) -> IdentitySpecDocumentWrite {
    IdentitySpecDocumentWrite::new(
        format!("cipher-{label}").into_bytes(),
        format!("nonce-{label}").into_bytes(),
        format!("wrapped-{label}").into_bytes(),
        format!("wrapped-nonce-{label}").into_bytes(),
        format!("key-{label}"),
        format!("algo-{label}"),
        99,
    )
    .expect("valid opaque document")
}

fn document_key_where(key: &IdentitySpecKey) -> sea_query::SimpleExpr {
    Expr::col(IdentitySpecDocuments::ScopeKind)
        .eq(key.scope().kind())
        .and(Expr::col(IdentitySpecDocuments::ScopeId).eq(key.scope().scope_id()))
        .and(Expr::col(IdentitySpecDocuments::Name).eq(key.name()))
}

fn parsed_workspace(name: &str) -> WorkspaceName {
    WorkspaceName::parse(name).expect("valid workspace")
}

fn scoped_key(workspace: &WorkspaceName, name: &str) -> IdentitySpecKey {
    IdentitySpecKey::workspace(workspace.clone(), name).expect("workspace key")
}
